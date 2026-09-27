//! WebSocket fan-out.
//!
//! One broadcast channel feeds every client. Events are serialized once by the
//! consumer and shared as `Arc`, so adding clients costs a pointer copy, not a
//! re-encode. A client that cannot keep up gets `Lagged` and skips ahead; it
//! never slows the consumer or the other clients.

use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use axum::routing::get;
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct Update {
    pub symbol: Arc<str>,
    pub json: Utf8Bytes,
}

#[derive(Clone)]
pub struct Hub {
    tx: broadcast::Sender<Update>,
}

impl Hub {
    pub fn new(capacity: usize) -> Self {
        Self { tx: broadcast::channel(capacity).0 }
    }

    pub fn publish(&self, update: Update) {
        let _ = self.tx.send(update);
    }

    pub fn clients(&self) -> usize {
        self.tx.receiver_count()
    }
}

#[derive(Clone)]
struct WsState {
    hub: Hub,
    stop: CancellationToken,
}

#[derive(Debug, Deserialize)]
struct Filter {
    symbol: Option<String>,
}

/// `GET /ws[?symbol=BTC-USD]` streams events as JSON text frames.
pub fn router(hub: Hub, stop: CancellationToken) -> Router {
    Router::new().route("/ws", get(upgrade)).with_state(WsState { hub, stop })
}

async fn upgrade(
    ws: WebSocketUpgrade,
    Query(filter): Query<Filter>,
    State(state): State<WsState>,
) -> Response {
    let rx = state.hub.tx.subscribe();
    ws.on_upgrade(move |socket| client(socket, rx, filter.symbol, state.stop))
}

async fn client(
    mut socket: WebSocket,
    mut rx: broadcast::Receiver<Update>,
    symbol: Option<String>,
    stop: CancellationToken,
) {
    metrics::gauge!("md_ws_clients").increment(1);
    loop {
        tokio::select! {
            update = rx.recv() => match update {
                Ok(update) => {
                    if symbol.as_deref().is_some_and(|s| s != &*update.symbol) {
                        continue;
                    }
                    if socket.send(Message::Text(update.json)).await.is_err() {
                        break;
                    }
                }
                Err(RecvError::Lagged(skipped)) => {
                    metrics::counter!("md_ws_lagged_total").increment(skipped);
                    tracing::warn!(skipped, "slow WebSocket client skipped events");
                }
                Err(RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
            () = stop.cancelled() => {
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
        }
    }
    metrics::gauge!("md_ws_clients").decrement(1);
}
