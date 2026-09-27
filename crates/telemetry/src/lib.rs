//! Cross-cutting pieces every service needs: logs, metrics, an ops HTTP
//! endpoint and signal handling.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

/// Latency buckets from 10µs to 1s, applied to every `*_seconds` histogram.
const LATENCY_BUCKETS: &[f64] = &[
    0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1,
    0.25, 0.5, 1.0,
];

/// Flushes pending spans when dropped. Keep it alive for the whole `main`.
#[must_use = "dropping the guard stops span export"]
pub struct TracingGuard {
    provider: Option<SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("failed to flush spans: {err}");
        }
    }
}

/// JSON logs filtered by `RUST_LOG` (defaults to `info`). When
/// `OTEL_EXPORTER_OTLP_ENDPOINT` is set, spans are also exported over OTLP/gRPC.
pub fn init_tracing(service_name: &'static str) -> anyhow::Result<TracingGuard> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt = tracing_subscriber::fmt::layer().json();

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok().filter(|e| !e.is_empty());
    let provider = match endpoint {
        Some(endpoint) => {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .context("building OTLP exporter")?;
            Some(
                SdkTracerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(Resource::builder().with_service_name(service_name).build())
                    .build(),
            )
        }
        None => None,
    };
    let otel = provider.as_ref().map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer(service_name)));

    tracing_subscriber::registry().with(filter).with(fmt).with(otel).init();
    if provider.is_some() {
        tracing::info!("exporting spans over OTLP");
    }
    Ok(TracingGuard { provider })
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

/// Tiny HTTP GET used as the container health check: distroless images ship no curl.
pub async fn probe(addr: &str, path: &str) -> anyhow::Result<()> {
    let limit = Duration::from_secs(2);
    let mut stream = tokio::time::timeout(limit, TcpStream::connect(addr)).await??;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    tokio::time::timeout(limit, stream.read_to_end(&mut response)).await??;
    let status = response.split(|&b| b == b'\r').next().unwrap_or_default();
    anyhow::ensure!(
        status.starts_with(b"HTTP/1.1 200"),
        "{path} returned {}",
        String::from_utf8_lossy(status)
    );
    Ok(())
}

/// Reads and parses an environment variable, falling back to `default` when unset.
pub fn env_or<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(raw) => raw.parse().with_context(|| format!("invalid value for {name}: {raw:?}")),
        Err(_) => Ok(default),
    }
}
