//! Kafka producer: drains normalized trades from the bounded channel and
//! publishes them to the `trades` topic, keyed by symbol.
//!
//! See docs/lessons/12-kafka-producer-internals.md and 03-backpressure.md.

use anyhow::{Context, Result};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

use crate::trade::Trade;

/// The topic all trades are published to. Keyed by symbol so each symbol's
/// trades stay ordered within one partition (lesson 02).
pub const TOPIC: &str = "trades";

/// Build a FutureProducer. `acks=all` (librdkafka default) is kept for durability
/// since this is financial data.
pub fn build_producer(brokers: &str) -> Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        // How long librdkafka keeps retrying a message before reporting failure.
        .set("message.timeout.ms", "5000")
        .create()
        .context("failed to create Kafka producer")
}

/// Consume trades from `rx` and publish each to Kafka. Runs until the channel is
/// closed (all senders dropped), then returns.
pub async fn run_producer(producer: FutureProducer, mut rx: Receiver<Trade>) {
    let mut produced: u64 = 0;

    while let Some(trade) = rx.recv().await {
        let payload = match serde_json::to_vec(&trade) {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(error = %e, "failed to serialize trade; skipping");
                continue;
            }
        };
        // Key = symbol decides the partition (ordering per symbol, parallel across).
        let key = trade.symbol;

        let record = FutureRecord::to(TOPIC).key(&key).payload(&payload);

        // Timeout::Never => if librdkafka's internal queue is full, AWAIT space
        // instead of erroring. This is backpressure point #2 (lesson 03).
        match producer.send(record, Timeout::Never).await {
            Ok((partition, offset)) => {
                produced += 1;
                if produced % 500 == 0 {
                    info!(produced, partition, offset, "published trades (running total)");
                }
            }
            Err((e, _msg)) => error!(error = %e, symbol = %key, "failed to deliver trade"),
        }
    }

    info!(produced, "producer task ended (channel closed)");
}
