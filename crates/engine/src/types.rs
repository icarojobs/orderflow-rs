/// Price expressed in ticks. Using integers avoids floating point drift.
pub type Price = i64;
/// Quantity expressed in lots.
pub type Qty = u64;
pub type OrderId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderType {
    Limit { price: Price },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderRequest {
    pub side: Side,
    pub order_type: OrderType,
    pub qty: Qty,
}

impl OrderRequest {
    pub fn limit(side: Side, price: Price, qty: Qty) -> Self {
        Self { side, order_type: OrderType::Limit { price }, qty }
    }
}

/// A single match between a resting (maker) order and an incoming (taker) order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fill {
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub price: Price,
    pub qty: Qty,
    pub taker_side: Side,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderStatus {
    /// The remainder is resting on the book (it may have been partially filled).
    Resting,
    Filled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Execution {
    pub order_id: OrderId,
    pub fills: Vec<Fill>,
    /// Quantity left after matching. For resting orders this is what sits on the book.
    pub remaining: Qty,
    pub status: OrderStatus,
}

impl Execution {
    pub fn filled_qty(&self) -> Qty {
        self.fills.iter().map(|f| f.qty).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    #[error("order quantity must be greater than zero")]
    ZeroQuantity,
    #[error("limit price must be greater than zero, got {0}")]
    InvalidPrice(Price),
}
