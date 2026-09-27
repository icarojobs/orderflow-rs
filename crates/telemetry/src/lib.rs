//! Cross-cutting pieces every service needs: logs, metrics, an ops HTTP
//! endpoint and signal handling.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// Latency buckets from 10µs to 1s, applied to every `*_seconds` histogram.
const LATENCY_BUCKETS: &[f64] = &[
    0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1,
    0.25, 0.5, 1.0,
];

/// JSON logs filtered by `RUST_LOG` (defaults to `info`).
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .json()
        .init();
}

/// Builds a Prometheus recorder without installing it globally (useful in tests).
pub fn prometheus_recorder() -> anyhow::Result<metrics_exporter_prometheus::PrometheusRecorder> {
    Ok(PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), LATENCY_BUCKETS)?
        .build_recorder())
}

/// Installs the global Prometheus recorder and returns a handle to render it.
pub fn install_prometheus() -> anyhow::Result<PrometheusHandle> {
    let recorder = prometheus_recorder()?;
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)?;
    Ok(handle)
}

type Readiness = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone)]
struct OpsState {
    metrics: PrometheusHandle,
    ready: Readiness,
}

/// `/healthz` (liveness), `/readyz` (readiness) and `/metrics` (Prometheus text format).
pub fn ops_router(metrics: PrometheusHandle, ready: impl Fn() -> bool + Send + Sync + 'static) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(|State(s): State<OpsState>| async move { s.metrics.render() }))
        .with_state(OpsState { metrics, ready: Arc::new(ready) })
}

async fn readyz(State(state): State<OpsState>) -> (StatusCode, &'static str) {
    if (state.ready)() { (StatusCode::OK, "ready") } else { (StatusCode::SERVICE_UNAVAILABLE, "not ready") }
}

/// Serves `router` until `stop` is cancelled.
pub async fn serve_http(
    listener: TcpListener,
    router: Router,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(addr = %listener.local_addr()?, "ops HTTP server listening");
    axum::serve(listener, router).with_graceful_shutdown(stop.cancelled_owned()).await?;
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
