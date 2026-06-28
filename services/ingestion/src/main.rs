//! FluxTape ingestion service.
//!
//! Phase 1 / Step C: connect to the Coinbase trade feed, normalize each trade,
//! and publish it to the Redpanda `trades` topic (keyed by symbol) through a
//! bounded channel that provides backpressure. Adds reconnect-with-backoff and
//! Prometheus metrics exposed on /metrics.

mod coinbase;
mod producer;
mod trade;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use trade::Trade;

/// Symbols to ingest. Coinbase product ids use BASE-QUOTE form.
const PRODUCTS: &[&str] = &["BTC-USD", "ETH-USD", "SOL-USD"];

/// Bounded channel capacity between the WS reader and the producer task.
/// Larger = absorbs longer bursts but more memory/latency (lesson 03).
const CHANNEL_CAPACITY: usize = 10_000;

/// Address the Prometheus /metrics endpoint listens on.
const METRICS_ADDR: ([u8; 4], u16) = ([0, 0, 0, 0], 9100);

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env (KAFKA_BROKERS, etc.) for local dev; ignore if absent.
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // Install the Prometheus exporter; it serves GET /metrics on METRICS_ADDR.
    let addr = SocketAddr::from(METRICS_ADDR);
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .context("install Prometheus metrics exporter")?;
    info!(%addr, "metrics endpoint listening at /metrics");

    register_metrics();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".to_string());
    info!(%brokers, "FluxTape ingestion service starting (Step C)");

    // The bounded channel: WS reader -> producer task. This is backpressure point #1.
    let (tx, rx) = mpsc::channel::<Trade>(CHANNEL_CAPACITY);

    // Spawn the producer task; it drains `rx` and publishes to Redpanda.
    let kafka = producer::build_producer(&brokers)?;
    let producer_handle = tokio::spawn(producer::run_producer(kafka, rx));

    // Run the reconnect supervisor until Ctrl+C. The supervisor owns `tx`; when
    // this select drops it (on shutdown), the channel closes and the producer
    // drains its remaining buffered trades before exiting.
    tokio::select! {
        _ = coinbase::run_with_reconnect(PRODUCTS, tx) => {
            info!("feed supervisor ended");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C; shutting down");
        }
    }

    // Wait for the producer to drain remaining buffered trades.
    if let Err(e) = producer_handle.await {
        tracing::error!(error = %e, "producer task join error");
    }

    Ok(())
}

/// Describe and zero-initialize all metrics at startup so every series exists
/// from t0 (avoids "No data" panels and makes rate() correct from the start).
fn register_metrics() {
    describe_counter!("ingestion_trades_received_total", "Trades received from the feed");
    describe_counter!("ingestion_trades_published_total", "Trades published to Kafka");
    describe_counter!("ingestion_publish_errors_total", "Failed Kafka deliveries");
    describe_counter!("ingestion_ws_reconnects_total", "WebSocket reconnect attempts");
    describe_gauge!("ingestion_channel_depth", "Current depth of the WS->producer channel");
    describe_gauge!("ingestion_ws_connected", "1 if the feed is connected, else 0");
    describe_histogram!("ingestion_publish_latency_seconds", "Kafka publish latency in seconds");

    counter!("ingestion_trades_received_total").increment(0);
    counter!("ingestion_trades_published_total").increment(0);
    counter!("ingestion_publish_errors_total").increment(0);
    counter!("ingestion_ws_reconnects_total").increment(0);
    gauge!("ingestion_channel_depth").set(0.0);
    gauge!("ingestion_ws_connected").set(0.0);
}
