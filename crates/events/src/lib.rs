//! Events emitted by the matching engine. They are serialized as JSON on the
//! wire so any consumer (or a human with `rpk topic consume`) can read them.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderStatus {
    Resting,
    Filled,
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    pub symbol: String,
    /// Monotonic per gateway instance; lets consumers detect gaps.
    pub sequence: u64,
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub taker_side: Side,
    pub price: i64,
    pub quantity: u64,
    pub timestamp_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderAccepted {
    pub symbol: String,
    pub sequence: u64,
    pub order_id: u64,
    pub side: Side,
    /// `None` for market orders.
    pub price: Option<i64>,
    pub quantity: u64,
    pub remaining: u64,
    pub status: OrderStatus,
    pub timestamp_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderCancelled {
    pub symbol: String,
    pub sequence: u64,
    pub order_id: u64,
    pub quantity: u64,
    pub timestamp_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketEvent {
    Trade(Trade),
    OrderAccepted(OrderAccepted),
    OrderCancelled(OrderCancelled),
}

impl MarketEvent {
    pub fn symbol(&self) -> &str {
        match self {
            MarketEvent::Trade(e) => &e.symbol,
            MarketEvent::OrderAccepted(e) => &e.symbol,
            MarketEvent::OrderCancelled(e) => &e.symbol,
        }
    }

    pub fn sequence(&self) -> u64 {
        match self {
            MarketEvent::Trade(e) => e.sequence,
            MarketEvent::OrderAccepted(e) => e.sequence,
            MarketEvent::OrderCancelled(e) => e.sequence,
        }
    }

    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("event serialization is infallible")
    }

    pub fn from_json(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip_is_tagged() {
        let event = MarketEvent::Trade(Trade {
            symbol: "BTC-USD".into(),
            sequence: 7,
            maker_order_id: 1,
            taker_order_id: 2,
            taker_side: Side::Buy,
            price: 100,
            quantity: 3,
            timestamp_ns: 42,
        });
        let json = event.to_json();
        assert!(std::str::from_utf8(&json).unwrap().starts_with(r#"{"type":"trade""#));
        assert_eq!(MarketEvent::from_json(&json).unwrap(), event);
    }
}
