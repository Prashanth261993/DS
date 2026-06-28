//! The canonical trade model used across FluxTape.
//!
//! We normalize every exchange's wire format into THIS struct so the rest of the
//! system never depends on exchange-specific JSON. `event_time_ms` is the
//! exchange's event time (not our receive time) — the basis for windowing later.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    /// Normalized symbol, e.g. "BTC-USD". Used as the Kafka partition key.
    pub symbol: String,
    pub price: f64,
    pub quantity: f64,
    /// Exchange-assigned trade id — becomes our idempotency key in Phase 3.
    pub trade_id: i64,
    /// Event time in epoch milliseconds (when the trade happened on the exchange).
    pub event_time_ms: i64,
    pub side: Side,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}
