//! Coinbase Exchange WebSocket feed handling.
//!
//! Docs: https://docs.cdp.coinbase.com/exchange/docs/websocket-overview
//! We subscribe to the `matches` channel, which emits one message per trade.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use metrics::{counter, gauge};
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::trade::{Side, Trade};

const COINBASE_WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";

/// Messages we care about from Coinbase. `#[serde(tag = "type")]` selects the
/// variant from the JSON "type" field; everything else falls into `Other`.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CoinbaseMessage {
    /// Acknowledgement of our subscribe request.
    Subscriptions,
    /// The most recent trade, sent once right after subscribing.
    LastMatch(MatchMsg),
    /// A live trade.
    Match(MatchMsg),
    /// Server error (e.g. bad subscription).
    Error { message: String },
    /// Anything else (heartbeats, etc.) — ignored.
    #[serde(other)]
    Other,
}

/// A Coinbase trade ("match"). Numeric fields arrive as strings.
#[derive(Debug, Deserialize)]
struct MatchMsg {
    trade_id: i64,
    product_id: String,
    price: String,
    size: String,
    side: String,
    /// RFC3339 timestamp, e.g. "2026-06-27T12:34:56.789Z".
    time: String,
}

impl MatchMsg {
    /// Convert the exchange message into our canonical `Trade`.
    fn into_trade(self) -> Result<Trade> {
        let price = self.price.parse::<f64>().context("parse price")?;
        let quantity = self.size.parse::<f64>().context("parse size")?;
        let event_time_ms = chrono::DateTime::parse_from_rfc3339(&self.time)
            .context("parse time")?
            .timestamp_millis();
        let side = match self.side.as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            other => {
                warn!(side = other, "unknown side, defaulting to buy");
                Side::Buy
            }
        };
        Ok(Trade {
            symbol: self.product_id,
            price,
            quantity,
            trade_id: self.trade_id,
            event_time_ms,
            side,
        })
    }
}

/// Build the Coinbase subscribe message for the given products.
fn subscribe_message(products: &[&str]) -> String {
    serde_json::json!({
        "type": "subscribe",
        "product_ids": products,
        "channels": ["matches"],
    })
    .to_string()
}

/// Supervise the feed: connect, stream, and on any disconnect/error reconnect
/// with exponential backoff + full jitter (lesson 13). Loops forever; the
/// channel `tx` is reused across reconnects so the downstream producer never
/// restarts. Returns only if the trade receiver is dropped.
pub async fn run_with_reconnect(products: &[&str], tx: Sender<Trade>) {
    const BASE: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(30);
    /// A connection healthy for at least this long resets the backoff.
    const HEALTHY: Duration = Duration::from_secs(60);

    let mut backoff = BASE;
    loop {
        let started = Instant::now();
        match connect_and_stream(products, &tx).await {
            Ok(()) => warn!("feed connection closed; will reconnect"),
            Err(e) => warn!(error = %e, "feed connection error; will reconnect"),
        }
        gauge!("ingestion_ws_connected").set(0.0);

        // The receiver was dropped (shutdown) — stop supervising.
        if tx.is_closed() {
            info!("trade receiver closed; stopping feed supervisor");
            return;
        }

        counter!("ingestion_ws_reconnects_total").increment(1);

        // Reset backoff if the connection stayed healthy for a while.
        if started.elapsed() >= HEALTHY {
            backoff = BASE;
        }

        // Full jitter: sleep a random duration in [0, backoff].
        let wait = backoff.mul_f64(rand::random::<f64>());
        warn!(?wait, ?backoff, "backing off before reconnect");
        sleep(wait).await;

        // Exponential increase, capped.
        backoff = (backoff * 2).min(MAX);
    }
}

/// Connect once, subscribe, and stream trades, sending each into `tx`.
///
/// Sending on a bounded channel is where backpressure originates: if the
/// producer task can't keep up and the channel fills, `tx.send().await`
/// suspends this read loop, which in turn stops draining the TCP socket and
/// propagates backpressure to the exchange (lesson 03).
async fn connect_and_stream(products: &[&str], tx: &Sender<Trade>) -> Result<()> {
    info!(url = COINBASE_WS_URL, ?products, "connecting to Coinbase feed");
    let (ws_stream, _resp) = connect_async(COINBASE_WS_URL)
        .await
        .context("websocket connect failed")?;
    let (mut write, mut read) = ws_stream.split();

    write
        .send(Message::Text(subscribe_message(products)))
        .await
        .context("send subscribe")?;
    gauge!("ingestion_ws_connected").set(1.0);
    info!("subscribed; streaming trades");

    while let Some(frame) = read.next().await {
        match frame.context("websocket read error")? {
            Message::Text(txt) => {
                // Parse leniently: unknown message shapes shouldn't kill the stream.
                match serde_json::from_str::<CoinbaseMessage>(&txt) {
                    Ok(CoinbaseMessage::Match(m) | CoinbaseMessage::LastMatch(m)) => {
                        match m.into_trade() {
                            Ok(trade) => {
                                counter!("ingestion_trades_received_total").increment(1);
                                // Backpressure point #1: awaits if the channel is full.
                                if tx.send(trade).await.is_err() {
                                    warn!("trade receiver dropped; stopping feed");
                                    break;
                                }
                                // Live channel depth = backpressure signal (lesson 09).
                                gauge!("ingestion_channel_depth").increment(1.0);
                            }
                            Err(e) => warn!(error = %e, "failed to convert match"),
                        }
                    }
                    Ok(CoinbaseMessage::Subscriptions) => {
                        debug!("subscription confirmed");
                    }
                    Ok(CoinbaseMessage::Error { message }) => {
                        warn!(message, "coinbase error message");
                    }
                    Ok(CoinbaseMessage::Other) => {}
                    Err(e) => debug!(error = %e, raw = %txt, "unparsed message"),
                }
            }
            // Respond to server pings to keep the connection alive.
            Message::Ping(payload) => {
                write.send(Message::Pong(payload)).await.ok();
            }
            Message::Close(frame) => {
                warn!(?frame, "server closed connection");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
