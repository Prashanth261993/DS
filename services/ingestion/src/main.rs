//! FluxTape ingestion service.
//!
//! Phase 1 / Step B: connect to the Coinbase trade feed, normalize each trade,
//! and publish it to the Redpanda `trades` topic (keyed by symbol) through a
//! bounded channel that provides backpressure. Step C adds metrics + reconnect.

mod coinbase;
mod producer;
mod trade;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use trade::Trade;

/// Symbols to ingest. Coinbase product ids use BASE-QUOTE form.
const PRODUCTS: &[&str] = &["BTC-USD", "ETH-USD", "SOL-USD"];

/// Bounded channel capacity between the WS reader and the producer task.
/// Larger = absorbs longer bursts but more memory/latency (lesson 03).
const CHANNEL_CAPACITY: usize = 10_000;

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env (KAFKA_BROKERS, etc.) for local dev; ignore if absent.
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".to_string());
    info!(%brokers, "FluxTape ingestion service starting (Step B: producing to Kafka)");

    // The bounded channel: WS reader -> producer task. This is backpressure point #1.
    let (tx, rx) = mpsc::channel::<Trade>(CHANNEL_CAPACITY);

    // Spawn the producer task; it drains `rx` and publishes to Redpanda.
    let kafka = producer::build_producer(&brokers)?;
    let producer_handle = tokio::spawn(producer::run_producer(kafka, rx));

    // Run the feed, sending into `tx`. When this returns (e.g. disconnect),
    // `tx` is dropped, the channel closes, and the producer task finishes.
    let feed_result = coinbase::run_feed(PRODUCTS, tx).await;
    if let Err(e) = &feed_result {
        tracing::error!(error = %e, "feed ended with error");
    }

    // Wait for the producer to drain remaining buffered trades.
    if let Err(e) = producer_handle.await {
        tracing::error!(error = %e, "producer task join error");
    }

    feed_result
}
