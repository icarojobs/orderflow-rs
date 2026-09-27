//! Pure, single-threaded limit order book.
//!
//! The book knows nothing about networking, clocks or persistence: callers feed
//! it commands and get back deterministic results. That keeps the hot path free
//! of locks and makes the matching rules easy to test in isolation.

mod book;
mod types;

pub use book::OrderBook;
pub use types::{
    Cancelled, Depth, EngineError, Execution, Fill, LevelView, OrderId, OrderRequest, OrderStatus, OrderType,
    Price, Qty, Side,
};
