//! Property tests: random command streams must never break the book's invariants.

use std::collections::HashMap;

use engine::{OrderBook, OrderId, OrderRequest, OrderStatus, Price, Qty, Side};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Limit { side: Side, price: Price, qty: Qty },
    Market { side: Side, qty: Qty },
    Cancel { pick: usize },
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (side(), 90..=110i64, 1..=50u64).prop_map(|(side, price, qty)| Op::Limit { side, price, qty }),
        1 => (side(), 1..=120u64).prop_map(|(side, qty)| Op::Market { side, qty }),
        2 => any::<usize>().prop_map(|pick| Op::Cancel { pick }),
    ]
}

#[derive(Default)]
struct Ledger {
    submitted: u128,
    traded: u128,
    cancelled: u128,
    expired: u128,
}

fn resting_qty(book: &OrderBook) -> (u128, usize) {
    let depth = book.depth(usize::MAX);
    depth.bids.iter().chain(&depth.asks).fold((0, 0), |(q, n), l| (q + l.qty as u128, n + l.orders))
}

fn check_book(book: &OrderBook, ledger: &Ledger, model: &HashMap<OrderId, Qty>) -> Result<(), TestCaseError> {
    if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
        prop_assert!(bid < ask, "crossed book: bid {bid} >= ask {ask}");
    }

    let (resting, orders) = resting_qty(book);
    prop_assert_eq!(orders, book.len());
    prop_assert_eq!(orders, model.len());
    prop_assert_eq!(resting, model.values().map(|&q| q as u128).sum::<u128>());
    prop_assert_eq!(
        ledger.submitted,
        2 * ledger.traded + resting + ledger.cancelled + ledger.expired,
        "quantity is neither created nor destroyed"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn random_flow_preserves_invariants(ops in prop::collection::vec(op(), 1..300)) {
        let mut book = OrderBook::new();
        let mut ledger = Ledger::default();
        // Reference model: remaining quantity of every order we believe is resting.
        let mut model: HashMap<OrderId, Qty> = HashMap::new();
        let mut known: Vec<OrderId> = Vec::new();

        for op in ops {
            match op {
                Op::Limit { side, price, qty } => {
                    ledger.submitted += qty as u128;
                    let exec = book.submit(OrderRequest::limit(side, price, qty)).unwrap();
                    let mut last = None;
                    for fill in &exec.fills {
                        prop_assert!(fill.qty > 0);
                        match side {
                            Side::Buy => prop_assert!(fill.price <= price),
                            Side::Sell => prop_assert!(fill.price >= price),
                        }
                        if let Some(prev) = last {
                            // Best prices are consumed first.
                            match side {
                                Side::Buy => prop_assert!(fill.price >= prev),
                                Side::Sell => prop_assert!(fill.price <= prev),
                            }
                        }
                        last = Some(fill.price);
                        let maker = model.get_mut(&fill.maker_order_id).expect("maker must be resting");
                        *maker -= fill.qty;
                        if *maker == 0 {
                            model.remove(&fill.maker_order_id);
                        }
                        ledger.traded += fill.qty as u128;
                    }
                    prop_assert_eq!(exec.filled_qty() + exec.remaining, qty);
                    match exec.status {
                        OrderStatus::Resting => {
                            prop_assert!(exec.remaining > 0);
                            model.insert(exec.order_id, exec.remaining);
                        }
                        OrderStatus::Filled => prop_assert_eq!(exec.remaining, 0),
                        OrderStatus::Expired => prop_assert!(false, "limit orders never expire"),
                    }
                    known.push(exec.order_id);
                }
                Op::Market { side, qty } => {
                    ledger.submitted += qty as u128;
                    let exec = book.submit(OrderRequest::market(side, qty)).unwrap();
                    for fill in &exec.fills {
                        let maker = model.get_mut(&fill.maker_order_id).expect("maker must be resting");
                        *maker -= fill.qty;
                        if *maker == 0 {
                            model.remove(&fill.maker_order_id);
                        }
                        ledger.traded += fill.qty as u128;
                    }
                    prop_assert!(!book.contains(exec.order_id));
                    prop_assert_eq!(exec.filled_qty() + exec.remaining, qty);
                    ledger.expired += exec.remaining as u128;
                }
                Op::Cancel { pick } => {
                    if known.is_empty() {
                        continue;
                    }
                    let id = known[pick % known.len()];
                    match (book.cancel(id), model.remove(&id)) {
                        (Ok(c), Some(expected)) => {
                            prop_assert_eq!(c.qty, expected);
                            ledger.cancelled += c.qty as u128;
                        }
                        (Err(_), None) => {}
                        (got, expected) => prop_assert!(false, "cancel {id}: book {got:?}, model {expected:?}"),
                    }
                }
            }
            check_book(&book, &ledger, &model)?;
        }
    }
}
