pub mod config;
pub mod grpc;
pub mod matcher;
pub mod publisher;

use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
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
    let (sink, publisher_task) = match config.kafka.clone() {
        Some(kafka) => {
            let (sink, task) =
                publisher::spawn(kafka, config.publisher_capacity, config.publisher_batch, stop.clone());
            (Some(sink), Some(task))
        }
        None => {
            tracing::warn!("KAFKA_BROKERS not set, events will not be published");
            (None, None)
        }
    };
    let (matcher, matcher_task) = matcher::spawn(&config.symbols, config.queue_capacity, sink);

    let probe = matcher.clone();
    let ops = telemetry::ops_router(metrics, move || probe.is_alive());
    let http = tokio::spawn(telemetry::serve_http(listeners.http, ops, stop.clone()));

    let service = GatewayService::new(matcher, stop.clone(), config.default_depth);
    // `serve_with_incoming` skips the builder's socket options, so set TCP_NODELAY
    // here. Without it Nagle plus delayed ACKs add ~40ms to small responses.
    let grpc_addr = listeners.grpc.local_addr()?;
    let incoming = TcpListenerStream::new(listeners.grpc).map(|conn| {
        let conn = conn?;
        conn.set_nodelay(true)?;
        Ok::<_, std::io::Error>(conn)
    });
    tracing::info!(addr = %grpc_addr, symbols = ?config.symbols, "gRPC server listening");
    tonic::transport::Server::builder()
        .add_service(proto::order_gateway_server::OrderGatewayServer::new(service))
        .serve_with_incoming_shutdown(incoming, stop.cancelled_owned())
        .await?;

    // The ops server holds the last matcher handle through the readiness probe.
    http.await??;
    matcher_task.await?;
    // With the matcher gone the sink is closed; the publisher flushes what is left.
    if let Some(task) = publisher_task {
        task.await?;
    }
    tracing::info!("gateway stopped");
    Ok(())
}
