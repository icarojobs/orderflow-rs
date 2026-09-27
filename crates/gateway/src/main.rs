use gateway::Listeners;
use gateway::config::Config;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let addr = format!("127.0.0.1:{}", config.http_addr.port());
        return telemetry::probe(&addr, "/readyz").await;
    }

    telemetry::init_tracing();
    let metrics = telemetry::install_prometheus()?;

    let stop = CancellationToken::new();
    tokio::spawn(telemetry::stop_signal(stop.clone()));

    let listeners = Listeners {
        grpc: TcpListener::bind(config.grpc_addr).await?,
        http: TcpListener::bind(config.http_addr).await?,
    };
    gateway::serve(config, listeners, metrics, stop).await
}
