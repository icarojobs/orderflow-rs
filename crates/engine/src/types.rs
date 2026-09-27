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
    Limit {
        price: Price,
    },
    /// Executes against whatever liquidity exists; the unfilled remainder expires.
    Market,
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

    pub fn market(side: Side, qty: Qty) -> Self {
        Self { side, order_type: OrderType::Market, qty }
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
    /// Market order that could not be fully filled; the remainder was discarded.
    Expired,
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
    #[error("order {0} is not resting on the book")]
    UnknownOrder(OrderId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cancelled {
    pub order_id: OrderId,
    pub side: Side,
    pub price: Price,
    /// Quantity that was still resting when the order was removed.
    pub qty: Qty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelView {
    pub price: Price,
    pub qty: Qty,
    pub orders: usize,
}

/// Aggregated view of the top of the book, best prices first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Depth {
    pub bids: Vec<LevelView>,
    pub asks: Vec<LevelView>,
}
