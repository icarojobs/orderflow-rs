use std::time::Duration;

use futures_util::StreamExt;
use market_data::hub::{Hub, Update};
use market_data::{Config, consumer::StartFrom};
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn fans_out_filtered_updates_and_closes_on_stop() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Config { http_addr: addr, kafka: None, start: StartFrom::Latest, buffer: 64 };
    let metrics = telemetry::prometheus_recorder().unwrap().handle();
    let stop = CancellationToken::new();
    let hub = Hub::new(64);
    let server = tokio::spawn(market_data::serve(config, listener, metrics, stop.clone(), hub.clone()));

    let (mut all, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
    let (mut btc, _) = connect_async(format!("ws://{addr}/ws?symbol=BTC-USD")).await.unwrap();
    while hub.clients() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    hub.publish(Update { symbol: "ETH-USD".into(), json: r#"{"n":1}"#.into() });
    hub.publish(Update { symbol: "BTC-USD".into(), json: r#"{"n":2}"#.into() });

    let next = |msg: Option<Result<Message, _>>| msg.unwrap().unwrap().into_text().unwrap().to_string();
    assert_eq!(next(all.next().await), r#"{"n":1}"#);
    assert_eq!(next(all.next().await), r#"{"n":2}"#);
    assert_eq!(next(btc.next().await), r#"{"n":2}"#, "symbol filter skips ETH");

    stop.cancel();
    assert!(matches!(all.next().await, Some(Ok(Message::Close(_))) | None));
    tokio::time::timeout(Duration::from_secs(5), server).await.unwrap().unwrap().unwrap();
}
