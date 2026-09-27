use std::pin::Pin;
use std::time::Instant;

use engine::{EngineError, LevelView, OrderRequest};
use events::MarketEvent;
use futures_util::{Stream, StreamExt};
use proto::order_gateway_server::OrderGateway;
use proto::{
    CancelOrderRequest, CancelOrderResponse, Fill, GetBookRequest, GetBookResponse, Level, OrderStatus,
    OrderType, PlaceOrderRequest, PlaceOrderResponse, Side, StreamTradesRequest, Trade,
};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::matcher::{MatchError, MatcherHandle};

const MAX_DEPTH: usize = 1_000;

pub struct GatewayService {
    matcher: MatcherHandle,
    stop: CancellationToken,
    default_depth: usize,
}

impl GatewayService {
    pub fn new(matcher: MatcherHandle, stop: CancellationToken, default_depth: usize) -> Self {
        Self { matcher, stop, default_depth }
    }
}

type TradeStream = Pin<Box<dyn Stream<Item = Result<Trade, Status>> + Send>>;

#[tonic::async_trait]
impl OrderGateway for GatewayService {
    async fn place_order(
        &self,
        request: Request<PlaceOrderRequest>,
    ) -> Result<Response<PlaceOrderResponse>, Status> {
        let req = request.into_inner();
        let side = match Side::try_from(req.side) {
            Ok(Side::Buy) => engine::Side::Buy,
            Ok(Side::Sell) => engine::Side::Sell,
            _ => return Err(Status::invalid_argument("side must be BUY or SELL")),
        };
        let order = match OrderType::try_from(req.order_type) {
            Ok(OrderType::Limit) => OrderRequest::limit(side, req.price, req.quantity),
            Ok(OrderType::Market) => OrderRequest::market(side, req.quantity),
            _ => return Err(Status::invalid_argument("order_type must be LIMIT or MARKET")),
        };

        let started = Instant::now();
        let exec = self.matcher.place(req.symbol, order).await;
        metrics::histogram!("orderflow_place_order_duration_seconds").record(started.elapsed());
        let exec = exec?;
        let status = match exec.status {
            engine::OrderStatus::Resting => OrderStatus::Resting,
            engine::OrderStatus::Filled => OrderStatus::Filled,
            engine::OrderStatus::Expired => OrderStatus::Expired,
        };
        Ok(Response::new(PlaceOrderResponse {
            order_id: exec.order_id,
            status: status.into(),
            filled_quantity: exec.filled_qty(),
            remaining_quantity: exec.remaining,
            fills: exec
                .fills
                .iter()
                .map(|f| Fill { maker_order_id: f.maker_order_id, price: f.price, quantity: f.qty })
                .collect(),
        }))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();
        let cancelled = self.matcher.cancel(req.symbol, req.order_id).await?;
        Ok(Response::new(CancelOrderResponse {
            order_id: cancelled.order_id,
            cancelled_quantity: cancelled.qty,
        }))
    }

    async fn get_book(&self, request: Request<GetBookRequest>) -> Result<Response<GetBookResponse>, Status> {
        let req = request.into_inner();
        let levels = match req.depth as usize {
            0 => self.default_depth,
            n => n.min(MAX_DEPTH),
        };
        let depth = self.matcher.depth(req.symbol.clone(), levels).await?;
        let level = |l: &LevelView| Level { price: l.price, quantity: l.qty, orders: l.orders as u32 };
        Ok(Response::new(GetBookResponse {
            symbol: req.symbol,
            bids: depth.bids.iter().map(level).collect(),
            asks: depth.asks.iter().map(level).collect(),
        }))
    }

    type StreamTradesStream = TradeStream;

    async fn stream_trades(
        &self,
        request: Request<StreamTradesRequest>,
    ) -> Result<Response<Self::StreamTradesStream>, Status> {
        let symbol = request.into_inner().symbol;
        let stream = BroadcastStream::new(self.matcher.subscribe())
            .filter_map(move |item| {
                let trade = match item {
                    Ok(event) => match &*event {
                        MarketEvent::Trade(t) if symbol.is_empty() || t.symbol == symbol => {
                            Some(Ok(trade_to_proto(t)))
                        }
                        _ => None,
                    },
                    Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "trade stream subscriber lagged");
                        None
                    }
                };
                std::future::ready(trade)
            })
            // Long-lived streams must end when the server stops, otherwise it waits forever.
            .take_until(self.stop.clone().cancelled_owned());
        Ok(Response::new(Box::pin(stream)))
    }
}

fn trade_to_proto(t: &events::Trade) -> Trade {
    let taker_side = match t.taker_side {
        events::Side::Buy => Side::Buy,
        events::Side::Sell => Side::Sell,
    };
    Trade {
        symbol: t.symbol.clone(),
        sequence: t.sequence,
        maker_order_id: t.maker_order_id,
        taker_order_id: t.taker_order_id,
        taker_side: taker_side.into(),
        price: t.price,
        quantity: t.quantity,
        timestamp_unix_nanos: t.timestamp_ns,
    }
}

impl From<MatchError> for Status {
    fn from(err: MatchError) -> Self {
        match &err {
            MatchError::UnknownSymbol(_) => Status::not_found(err.to_string()),
            MatchError::Rejected(EngineError::UnknownOrder(_)) => Status::not_found(err.to_string()),
            MatchError::Rejected(_) => Status::invalid_argument(err.to_string()),
            MatchError::Unavailable => Status::unavailable(err.to_string()),
        }
    }
}
