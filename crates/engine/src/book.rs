use std::collections::{BTreeMap, HashMap};

use slab::Slab;

use crate::types::{
    EngineError, Execution, Fill, OrderId, OrderRequest, OrderStatus, OrderType, Price, Qty, Side,
};

/// Resting order stored in the slab. Orders at the same price form an intrusive
/// doubly linked list, so FIFO pops and cancels are O(1) once the level is found.
#[derive(Debug)]
struct Node {
    id: OrderId,
    qty: Qty,
    prev: Option<usize>,
    next: Option<usize>,
}

#[derive(Debug, Default)]
struct Level {
    head: Option<usize>,
    tail: Option<usize>,
    qty: Qty,
    len: usize,
}

/// Limit order book for a single instrument with price-time priority.
#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<Price, Level>,
    asks: BTreeMap<Price, Level>,
    orders: Slab<Node>,
    index: HashMap<OrderId, usize>,
    next_id: OrderId,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and matches an incoming order, resting any remainder.
    pub fn submit(&mut self, req: OrderRequest) -> Result<Execution, EngineError> {
        if req.qty == 0 {
            return Err(EngineError::ZeroQuantity);
        }
        let OrderType::Limit { price } = req.order_type;
        if price <= 0 {
            return Err(EngineError::InvalidPrice(price));
        }

        self.next_id += 1;
        let id = self.next_id;
        let mut fills = Vec::new();
        let remaining = self.match_incoming(id, req.side, Some(price), req.qty, &mut fills);

        let status = if remaining == 0 {
            OrderStatus::Filled
        } else {
            self.rest(id, req.side, price, remaining);
            OrderStatus::Resting
        };

        Ok(Execution { order_id: id, fills, remaining, status })
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.keys().next().copied()
    }

    /// Number of resting orders.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Total resting quantity at a price on the given side.
    pub fn qty_at(&self, side: Side, price: Price) -> Qty {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        levels.get(&price).map_or(0, |l| l.qty)
    }

    /// Walks the opposite side from the best price, consuming liquidity until the
    /// order is filled or the limit price is no longer marketable.
    fn match_incoming(
        &mut self,
        taker_id: OrderId,
        side: Side,
        limit: Option<Price>,
        mut qty: Qty,
        fills: &mut Vec<Fill>,
    ) -> Qty {
        let orders = &mut self.orders;
        let index = &mut self.index;
        let opposite = match side {
            Side::Buy => &mut self.asks,
            Side::Sell => &mut self.bids,
        };

        while qty > 0 {
            let entry = match side {
                Side::Buy => opposite.first_entry(),
                Side::Sell => opposite.last_entry(),
            };
            let Some(mut entry) = entry else { break };
            let price = *entry.key();
            let crosses = match (side, limit) {
                (_, None) => true,
                (Side::Buy, Some(limit)) => price <= limit,
                (Side::Sell, Some(limit)) => price >= limit,
            };
            if !crosses {
                break;
            }

            let level = entry.get_mut();
            while qty > 0 {
                let Some(idx) = level.head else { break };
                let maker = &mut orders[idx];
                let traded = qty.min(maker.qty);
                maker.qty -= traded;
                level.qty -= traded;
                qty -= traded;
                fills.push(Fill {
                    maker_order_id: maker.id,
                    taker_order_id: taker_id,
                    price,
                    qty: traded,
                    taker_side: side,
                });

                if maker.qty == 0 {
                    let next = maker.next;
                    let maker_id = maker.id;
                    orders.remove(idx);
                    index.remove(&maker_id);
                    level.head = next;
                    level.len -= 1;
                    match next {
                        Some(n) => orders[n].prev = None,
                        None => level.tail = None,
                    }
                }
            }

            if level.head.is_none() {
                entry.remove();
            }
        }

        qty
    }

    fn rest(&mut self, id: OrderId, side: Side, price: Price, qty: Qty) {
        let idx = self.orders.insert(Node { id, qty, prev: None, next: None });
        let level = match side {
            Side::Buy => self.bids.entry(price).or_default(),
            Side::Sell => self.asks.entry(price).or_default(),
        };
        match level.tail {
            Some(tail) => {
                self.orders[tail].next = Some(idx);
                self.orders[idx].prev = Some(tail);
            }
            None => level.head = Some(idx),
        }
        level.tail = Some(idx);
        level.qty += qty;
        level.len += 1;
        self.index.insert(id, idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(book: &mut OrderBook, side: Side, price: Price, qty: Qty) -> Execution {
        book.submit(OrderRequest::limit(side, price, qty)).unwrap()
    }

    #[test]
    fn non_crossing_orders_rest() {
        let mut book = OrderBook::new();
        let bid = limit(&mut book, Side::Buy, 99, 10);
        let ask = limit(&mut book, Side::Sell, 101, 5);

        assert_eq!(bid.status, OrderStatus::Resting);
        assert!(ask.fills.is_empty());
        assert_eq!(book.best_bid(), Some(99));
        assert_eq!(book.best_ask(), Some(101));
        assert_eq!(book.len(), 2);
    }

    #[test]
    fn crossing_order_trades_at_maker_price() {
        let mut book = OrderBook::new();
        let ask = limit(&mut book, Side::Sell, 100, 5);
        let buy = limit(&mut book, Side::Buy, 105, 5);

        assert_eq!(buy.status, OrderStatus::Filled);
        assert_eq!(
            buy.fills,
            vec![Fill {
                maker_order_id: ask.order_id,
                taker_order_id: buy.order_id,
                price: 100,
                qty: 5,
                taker_side: Side::Buy,
            }]
        );
        assert!(book.is_empty());
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn partial_fill_rests_remainder() {
        let mut book = OrderBook::new();
        limit(&mut book, Side::Sell, 100, 3);
        let buy = limit(&mut book, Side::Buy, 100, 10);

        assert_eq!(buy.status, OrderStatus::Resting);
        assert_eq!(buy.filled_qty(), 3);
        assert_eq!(buy.remaining, 7);
        assert_eq!(book.best_bid(), Some(100));
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.qty_at(Side::Buy, 100), 7);
    }

    #[test]
    fn partial_fill_leaves_maker_on_book() {
        let mut book = OrderBook::new();
        let ask = limit(&mut book, Side::Sell, 100, 10);
        limit(&mut book, Side::Buy, 100, 4);

        assert_eq!(book.qty_at(Side::Sell, 100), 6);
        let next = limit(&mut book, Side::Buy, 100, 6);
        assert_eq!(next.fills[0].maker_order_id, ask.order_id);
        assert!(book.is_empty());
    }

    #[test]
    fn time_priority_within_level() {
        let mut book = OrderBook::new();
        let first = limit(&mut book, Side::Sell, 100, 2);
        let second = limit(&mut book, Side::Sell, 100, 2);
        let third = limit(&mut book, Side::Sell, 100, 2);

        let buy = limit(&mut book, Side::Buy, 100, 5);
        let makers: Vec<_> = buy.fills.iter().map(|f| (f.maker_order_id, f.qty)).collect();
        assert_eq!(makers, vec![(first.order_id, 2), (second.order_id, 2), (third.order_id, 1)]);
        assert_eq!(book.qty_at(Side::Sell, 100), 1);
    }

    #[test]
    fn price_priority_across_levels() {
        let mut book = OrderBook::new();
        limit(&mut book, Side::Buy, 98, 1);
        limit(&mut book, Side::Buy, 100, 1);
        limit(&mut book, Side::Buy, 99, 1);

        let sell = limit(&mut book, Side::Sell, 97, 3);
        let prices: Vec<_> = sell.fills.iter().map(|f| f.price).collect();
        assert_eq!(prices, vec![100, 99, 98]);
    }

    #[test]
    fn limit_price_stops_the_sweep() {
        let mut book = OrderBook::new();
        limit(&mut book, Side::Sell, 100, 1);
        limit(&mut book, Side::Sell, 102, 1);

        let buy = limit(&mut book, Side::Buy, 101, 2);
        assert_eq!(buy.filled_qty(), 1);
        assert_eq!(book.best_bid(), Some(101));
        assert_eq!(book.best_ask(), Some(102));
    }

    #[test]
    fn rejects_invalid_orders() {
        let mut book = OrderBook::new();
        assert_eq!(book.submit(OrderRequest::limit(Side::Buy, 100, 0)), Err(EngineError::ZeroQuantity));
        assert_eq!(book.submit(OrderRequest::limit(Side::Buy, 0, 1)), Err(EngineError::InvalidPrice(0)));
    }
}
