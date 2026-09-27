pub mod consumer;
pub mod hub;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::bail;
use events::kafka::KafkaConfig;
use metrics_exporter_prometheus::PrometheusHandle;
use telemetry::env_or;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::consumer::{Consumer, StartFrom};
use crate::hub::Hub;

#[derive(Clone, Debug)]
pub struct Config {
    pub http_addr: SocketAddr,
    pub kafka: Option<KafkaConfig>,
    pub start: StartFrom,
    /// Per-client buffer before a slow WebSocket client starts skipping events.
    pub buffer: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let start = match env_or("MD_START_FROM", "latest".to_string())?.as_str() {
            "latest" => StartFrom::Latest,
            "earliest" => StartFrom::Earliest,
            other => bail!("MD_START_FROM must be `latest` or `earliest`, got {other:?}"),
        };
        Ok(Self {
            http_addr: env_or("MD_HTTP_ADDR", "0.0.0.0:8081".parse()?)?,
            kafka: KafkaConfig::from_env(),
            start,
            buffer: env_or("MD_BUFFER", 4_096)?,
        })
    }
}

/// Runs the consumer and the HTTP/WebSocket server until `stop` is cancelled.
/// Returns the hub so tests can inject updates without a broker.
pub async fn serve(
    config: Config,
    listener: TcpListener,
    metrics: PrometheusHandle,
    stop: CancellationToken,
    hub: Hub,
) -> anyhow::Result<()> {
    let connected = Arc::new(AtomicBool::new(config.kafka.is_none()));
    let consumer = match config.kafka.clone() {
        Some(kafka) => {
            let consumer = Consumer {
                config: kafka,
                start: config.start,
                hub: hub.clone(),
                connected: connected.clone(),
            };
            Some(tokio::spawn(consumer.run(stop.clone())))
        }
        None => {
            tracing::warn!("KAFKA_BROKERS not set, only the WebSocket hub is running");
            None
        }
    };

    let ready = move || connected.load(Ordering::Relaxed);
    let router = hub::router(hub, stop.clone()).merge(telemetry::ops_router(metrics, ready));
    telemetry::serve_http(listener, router, stop).await?;

    if let Some(consumer) = consumer {
        consumer.await?;
    }
    tracing::info!("market-data stopped");
    Ok(())
}
