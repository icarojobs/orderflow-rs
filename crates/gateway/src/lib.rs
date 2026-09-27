pub mod config;
pub mod grpc;
pub mod matcher;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::grpc::GatewayService;

/// Runs the gateway until `stop` is cancelled, then drains the matching queue.
pub async fn serve(config: Config, grpc: TcpListener, stop: CancellationToken) -> anyhow::Result<()> {
    let (matcher, matcher_task) = matcher::spawn(&config.symbols, config.queue_capacity);
    let service = GatewayService::new(matcher, stop.clone(), config.default_depth);

    tracing::info!(addr = %grpc.local_addr()?, symbols = ?config.symbols, "gRPC server listening");
    tonic::transport::Server::builder()
        .add_service(proto::order_gateway_server::OrderGatewayServer::new(service))
        .serve_with_incoming_shutdown(TcpListenerStream::new(grpc), stop.cancelled_owned())
        .await?;

    // The server owned the last handles; once it returns the matcher drains and exits.
    matcher_task.await?;
    tracing::info!("gateway stopped");
    Ok(())
}

/// Resolves on Ctrl+C or SIGTERM and cancels `token`.
pub async fn stop_signal(token: CancellationToken) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => {
                tracing::error!(%err, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("termination signal received, draining");
    token.cancel();
}
