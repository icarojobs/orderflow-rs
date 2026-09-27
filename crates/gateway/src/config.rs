use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::Context;

#[derive(Clone, Debug)]
pub struct Config {
    pub grpc_addr: SocketAddr,
    pub symbols: Vec<String>,
    /// Capacity of the command queue in front of the matching task.
    pub queue_capacity: usize,
    pub default_depth: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let symbols: Vec<String> = var_or("GATEWAY_SYMBOLS", "BTC-USD,ETH-USD".to_string())?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        anyhow::ensure!(!symbols.is_empty(), "GATEWAY_SYMBOLS must list at least one symbol");

        Ok(Self {
            grpc_addr: var_or("GATEWAY_GRPC_ADDR", "0.0.0.0:50051".parse()?)?,
            symbols,
            queue_capacity: var_or("GATEWAY_QUEUE_CAPACITY", 65_536)?,
            default_depth: var_or("GATEWAY_DEFAULT_DEPTH", 10)?,
        })
    }
}

/// Reads and parses an environment variable, falling back to `default` when unset.
pub fn var_or<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(raw) => raw.parse().with_context(|| format!("invalid value for {name}: {raw:?}")),
        Err(_) => Ok(default),
    }
}
