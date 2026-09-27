use std::time::Duration;

use gateway::config::Config;
use proto::order_gateway_client::OrderGatewayClient;
use proto::{
    CancelOrderRequest, GetBookRequest, OrderStatus, OrderType, PlaceOrderRequest, Side, StreamTradesRequest,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tonic::transport::Channel;

struct TestGateway {
    client: OrderGatewayClient<Channel>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn start() -> TestGateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config =
        Config { grpc_addr: addr, symbols: vec!["BTC-USD".into()], queue_capacity: 1024, default_depth: 10 };
    let stop = CancellationToken::new();
    let task = tokio::spawn(gateway::serve(config, listener, stop.clone()));
    let client = OrderGatewayClient::connect(format!("http://{addr}")).await.unwrap();
    TestGateway { client, stop, task }
}

fn limit(side: Side, price: i64, quantity: u64) -> PlaceOrderRequest {
    PlaceOrderRequest {
        symbol: "BTC-USD".into(),
        side: side.into(),
        order_type: OrderType::Limit.into(),
        price,
        quantity,
    }
}

#[tokio::test]
async fn place_match_stream_and_stop() {
    let TestGateway { mut client, stop, task } = start().await;

    let mut trades =
        client.stream_trades(StreamTradesRequest { symbol: "BTC-USD".into() }).await.unwrap().into_inner();

    let ask = client.place_order(limit(Side::Sell, 100, 5)).await.unwrap().into_inner();
    assert_eq!(ask.status(), OrderStatus::Resting);

    let book = client.get_book(GetBookRequest { symbol: "BTC-USD".into(), depth: 0 }).await.unwrap();
    assert_eq!(book.into_inner().asks[0].quantity, 5);

    let buy = client.place_order(limit(Side::Buy, 101, 3)).await.unwrap().into_inner();
    assert_eq!(buy.status(), OrderStatus::Filled);
    assert_eq!(buy.fills[0].price, 100);

    let trade =
        tokio::time::timeout(Duration::from_secs(2), trades.message()).await.unwrap().unwrap().unwrap();
    assert_eq!((trade.maker_order_id, trade.taker_order_id, trade.quantity), (ask.order_id, buy.order_id, 3));

    let cancel = client
        .cancel_order(CancelOrderRequest { symbol: "BTC-USD".into(), order_id: ask.order_id })
        .await
        .unwrap();
    assert_eq!(cancel.into_inner().cancelled_quantity, 2);

    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), task).await.expect("graceful stop").unwrap().unwrap();
}

#[tokio::test]
async fn invalid_requests_map_to_grpc_codes() {
    let TestGateway { mut client, stop, .. } = start().await;

    let mut unknown = limit(Side::Buy, 100, 1);
    unknown.symbol = "DOGE-USD".into();
    assert_eq!(client.place_order(unknown).await.unwrap_err().code(), Code::NotFound);

    let zero = limit(Side::Buy, 100, 0);
    assert_eq!(client.place_order(zero).await.unwrap_err().code(), Code::InvalidArgument);

    let no_side = limit(Side::Unspecified, 100, 1);
    assert_eq!(client.place_order(no_side).await.unwrap_err().code(), Code::InvalidArgument);

    let missing = CancelOrderRequest { symbol: "BTC-USD".into(), order_id: 999 };
    assert_eq!(client.cancel_order(missing).await.unwrap_err().code(), Code::NotFound);

    stop.cancel();
}
