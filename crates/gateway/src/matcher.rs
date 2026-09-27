//! Single-writer matching loop.
//!
//! Every order book is owned by one Tokio task. gRPC handlers never touch the
//! books directly: they send a [`Command`] over a bounded mpsc channel and await
//! the reply on a oneshot. No locks on the hot path, and the order in which
//! commands leave the channel *is* the global sequence of the venue.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use engine::{
    Cancelled, Depth, EngineError, Execution, OrderBook, OrderRequest, OrderStatus, OrderType, Side,
};
use events::MarketEvent;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

/// Max commands drained from the queue per wakeup.
const BATCH: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum MatchError {
    #[error("unknown symbol {0:?}")]
    UnknownSymbol(String),
    #[error(transparent)]
    Rejected(#[from] EngineError),
    #[error("matching engine is shutting down")]
    Unavailable,
}

enum Command {
    Place { symbol: String, req: OrderRequest, reply: oneshot::Sender<Result<Execution, MatchError>> },
    Cancel { symbol: String, order_id: u64, reply: oneshot::Sender<Result<Cancelled, MatchError>> },
    Depth { symbol: String, levels: usize, reply: oneshot::Sender<Result<Depth, MatchError>> },
}

/// Cheap, cloneable handle used by request handlers.
#[derive(Clone)]
pub struct MatcherHandle {
    tx: mpsc::Sender<Command>,
    events: broadcast::Sender<Arc<MarketEvent>>,
}

impl MatcherHandle {
    pub async fn place(&self, symbol: String, req: OrderRequest) -> Result<Execution, MatchError> {
        self.call(|reply| Command::Place { symbol, req, reply }).await
    }

    pub async fn cancel(&self, symbol: String, order_id: u64) -> Result<Cancelled, MatchError> {
        self.call(|reply| Command::Cancel { symbol, order_id, reply }).await
    }

    pub async fn depth(&self, symbol: String, levels: usize) -> Result<Depth, MatchError> {
        self.call(|reply| Command::Depth { symbol, levels, reply }).await
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<MarketEvent>> {
        self.events.subscribe()
    }

    async fn call<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, MatchError>>) -> Command,
    ) -> Result<T, MatchError> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(make(reply)).await.map_err(|_| MatchError::Unavailable)?;
        rx.await.map_err(|_| MatchError::Unavailable)?
    }
}

/// Spawns the matching task. It stops once every [`MatcherHandle`] is dropped
/// and the queue has been drained.
pub fn spawn(symbols: &[String], queue_capacity: usize) -> (MatcherHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(queue_capacity);
    let (events, _) = broadcast::channel(16_384);
    let matcher = Matcher {
        books: symbols.iter().map(|s| (s.clone(), OrderBook::new())).collect(),
        sequence: 0,
        events: events.clone(),
    };
    let task = tokio::spawn(matcher.run(rx));
    (MatcherHandle { tx, events }, task)
}

struct Matcher {
    books: HashMap<String, OrderBook>,
    sequence: u64,
    events: broadcast::Sender<Arc<MarketEvent>>,
}

impl Matcher {
    async fn run(mut self, mut rx: mpsc::Receiver<Command>) {
        let mut batch = Vec::with_capacity(BATCH);
        while rx.recv_many(&mut batch, BATCH).await > 0 {
            for cmd in batch.drain(..) {
                self.handle(cmd);
            }
        }
        tracing::info!(sequence = self.sequence, "matching loop drained and stopped");
    }

    fn handle(&mut self, cmd: Command) {
        match cmd {
            Command::Place { symbol, req, reply } => {
                let _ = reply.send(self.place(symbol, req));
            }
            Command::Cancel { symbol, order_id, reply } => {
                let _ = reply.send(self.cancel(symbol, order_id));
            }
            Command::Depth { symbol, levels, reply } => {
                let result = self.book(&symbol).map(|book| book.depth(levels));
                let _ = reply.send(result);
            }
        }
    }

    fn place(&mut self, symbol: String, req: OrderRequest) -> Result<Execution, MatchError> {
        let exec = self.book(&symbol)?.submit(req)?;
        let now = now_ns();

        for fill in &exec.fills {
            let sequence = self.next_sequence();
            self.publish(MarketEvent::Trade(events::Trade {
                symbol: symbol.clone(),
                sequence,
                maker_order_id: fill.maker_order_id,
                taker_order_id: fill.taker_order_id,
                taker_side: side(fill.taker_side),
                price: fill.price,
                quantity: fill.qty,
                timestamp_ns: now,
            }));
        }

        let sequence = self.next_sequence();
        self.publish(MarketEvent::OrderAccepted(events::OrderAccepted {
            symbol,
            sequence,
            order_id: exec.order_id,
            side: side(req.side),
            price: match req.order_type {
                OrderType::Limit { price } => Some(price),
                OrderType::Market => None,
            },
            quantity: req.qty,
            remaining: exec.remaining,
            status: match exec.status {
                OrderStatus::Resting => events::OrderStatus::Resting,
                OrderStatus::Filled => events::OrderStatus::Filled,
                OrderStatus::Expired => events::OrderStatus::Expired,
            },
            timestamp_ns: now,
        }));

        Ok(exec)
    }

    fn cancel(&mut self, symbol: String, order_id: u64) -> Result<Cancelled, MatchError> {
        let cancelled = self.book(&symbol)?.cancel(order_id)?;
        let sequence = self.next_sequence();
        self.publish(MarketEvent::OrderCancelled(events::OrderCancelled {
            symbol,
            sequence,
            order_id,
            quantity: cancelled.qty,
            timestamp_ns: now_ns(),
        }));
        Ok(cancelled)
    }

    fn book(&mut self, symbol: &str) -> Result<&mut OrderBook, MatchError> {
        self.books.get_mut(symbol).ok_or_else(|| MatchError::UnknownSymbol(symbol.to_string()))
    }

    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    fn publish(&self, event: MarketEvent) {
        // No subscribers is fine; the event is simply not observed.
        let _ = self.events.send(Arc::new(event));
    }
}

fn side(side: Side) -> events::Side {
    match side {
        Side::Buy => events::Side::Buy,
        Side::Sell => events::Side::Sell,
    }
}

fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as i64)
}
