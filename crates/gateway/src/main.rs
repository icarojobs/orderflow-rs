use gateway::config::Config;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .json()
        .init();

    let config = Config::from_env()?;
    let stop = CancellationToken::new();
    tokio::spawn(gateway::stop_signal(stop.clone()));

    let grpc = TcpListener::bind(config.grpc_addr).await?;
    gateway::serve(config, grpc, stop).await
}
