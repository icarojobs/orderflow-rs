use market_data::Config;
use market_data::hub::Hub;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let addr = format!("127.0.0.1:{}", config.http_addr.port());
        return telemetry::probe(&addr, "/healthz").await;
    }

    let _tracing = telemetry::init_tracing("market-data")?;
    let metrics = telemetry::install_prometheus()?;

    let stop = CancellationToken::new();
    tokio::spawn(telemetry::stop_signal(stop.clone()));

    let listener = TcpListener::bind(config.http_addr).await?;
    let hub = Hub::new(config.buffer);
    market_data::serve(config, listener, metrics, stop, hub).await
}
