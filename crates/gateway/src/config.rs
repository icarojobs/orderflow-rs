use std::net::SocketAddr;

use events::kafka::KafkaConfig;
use telemetry::env_or;

#[derive(Clone, Debug)]
pub struct Config {
    pub grpc_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pub symbols: Vec<String>,
    /// Capacity of the command queue in front of the matching task.
    pub queue_capacity: usize,
    pub default_depth: usize,
    /// `None` disables event publishing (handy for tests and local runs).
    pub kafka: Option<KafkaConfig>,
    pub publisher_capacity: usize,
    pub publisher_batch: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let symbols: Vec<String> = env_or("GATEWAY_SYMBOLS", "BTC-USD,ETH-USD".to_string())?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        anyhow::ensure!(!symbols.is_empty(), "GATEWAY_SYMBOLS must list at least one symbol");

        Ok(Self {
            grpc_addr: env_or("GATEWAY_GRPC_ADDR", "0.0.0.0:50051".parse()?)?,
            http_addr: env_or("GATEWAY_HTTP_ADDR", "0.0.0.0:8080".parse()?)?,
            symbols,
            queue_capacity: env_or("GATEWAY_QUEUE_CAPACITY", 65_536)?,
            default_depth: env_or("GATEWAY_DEFAULT_DEPTH", 10)?,
            kafka: KafkaConfig::from_env(),
            publisher_capacity: env_or("GATEWAY_PUBLISHER_CAPACITY", 65_536)?,
            publisher_batch: env_or("GATEWAY_PUBLISHER_BATCH", 1_024)?,
        })
    }
}
