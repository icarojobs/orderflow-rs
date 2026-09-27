use gateway::Listeners;
use gateway::config::Config;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing();
    let metrics = telemetry::install_prometheus()?;

    let config = Config::from_env()?;
    let stop = CancellationToken::new();
    tokio::spawn(telemetry::stop_signal(stop.clone()));

    let listeners = Listeners {
        grpc: TcpListener::bind(config.grpc_addr).await?,
        http: TcpListener::bind(config.http_addr).await?,
    };
    gateway::serve(config, listeners, metrics, stop).await
}
