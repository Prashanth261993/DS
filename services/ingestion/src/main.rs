//! FluxTape ingestion service.
//!
//! Phase 1 / Step A: connect to the Coinbase trade feed, normalize each trade,
//! and print it. Subsequent steps add the Kafka producer, backpressure, metrics,
//! and reconnect-with-backoff.

mod coinbase;
mod trade;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Symbols to ingest. Coinbase product ids use BASE-QUOTE form.
const PRODUCTS: &[&str] = &["BTC-USD", "ETH-USD", "SOL-USD"];

#[tokio::main]
async fn main() -> Result<()> {
    // Structured logging. Control verbosity with RUST_LOG (default: info).
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    info!("FluxTape ingestion service starting (Step A: print-only)");

    coinbase::run_feed(PRODUCTS, |trade| {
        info!(
            symbol = %trade.symbol,
            price = trade.price,
            qty = trade.quantity,
            side = ?trade.side,
            id = trade.trade_id,
            t = trade.event_time_ms,
            "trade"
        );
    })
    .await?;

    Ok(())
}
