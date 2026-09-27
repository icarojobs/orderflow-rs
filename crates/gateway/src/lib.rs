pub mod config;
pub mod grpc;
pub mod matcher;

use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::grpc::GatewayService;

pub struct Listeners {
    pub grpc: TcpListener,
    pub http: TcpListener,
}

/// Runs the gateway until `stop` is cancelled, then drains the matching queue.
pub async fn serve(
    config: Config,
    listeners: Listeners,
    metrics: PrometheusHandle,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    let (matcher, matcher_task) = matcher::spawn(&config.symbols, config.queue_capacity);

    let probe = matcher.clone();
    let ops = telemetry::ops_router(metrics, move || probe.is_alive());
    let http = tokio::spawn(telemetry::serve_http(listeners.http, ops, stop.clone()));

    let service = GatewayService::new(matcher, stop.clone(), config.default_depth);
    tracing::info!(addr = %listeners.grpc.local_addr()?, symbols = ?config.symbols, "gRPC server listening");
    tonic::transport::Server::builder()
        .add_service(proto::order_gateway_server::OrderGatewayServer::new(service))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listeners.grpc), stop.cancelled_owned())
        .await?;

    // The ops server holds the last matcher handle through the readiness probe.
    http.await??;
    matcher_task.await?;
    tracing::info!("gateway stopped");
    Ok(())
}
