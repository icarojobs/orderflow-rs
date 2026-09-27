//! Kafka transport shared by the producer (gateway) and consumers (market-data).
//!
//! The event topic has a single partition on purpose: the gateway is a single
//! writer with a global sequence, and one partition keeps that order intact for
//! every consumer. Scaling out would mean one partition per symbol.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rskafka::BackoffConfig;
use rskafka::client::ClientBuilder;
use rskafka::client::error::{Error, ProtocolError};
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};

pub const PARTITION: i32 = 0;

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
}

impl KafkaConfig {
    /// Parses `KAFKA_BROKERS` (comma separated) and `KAFKA_TOPIC`. Returns `None`
    /// when no brokers are configured.
    pub fn from_env() -> Option<Self> {
        let brokers: Vec<String> = std::env::var("KAFKA_BROKERS")
            .unwrap_or_default()
            .split(',')
            .map(|b| b.trim().to_string())
            .filter(|b| !b.is_empty())
            .collect();
        if brokers.is_empty() {
            return None;
        }
        let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "orderflow.events".to_string());
        Some(Self { brokers, topic })
    }
}

/// Connects to the cluster, makes sure the topic exists and returns a client for its partition.
///
/// rskafka retries internally; the deadline bounds that so callers can apply
/// their own reconnect policy and report the failure.
pub async fn connect(config: &KafkaConfig, client_id: &str) -> Result<PartitionClient, Error> {
    let client = ClientBuilder::new(config.brokers.clone())
        .client_id(client_id.to_string())
        .backoff_config(BackoffConfig {
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            base: 2.0,
            deadline: Some(Duration::from_secs(5)),
        })
        .build()
        .await?;

    match client.controller_client()?.create_topic(&config.topic, 1, 1, 5_000).await {
        Ok(()) => tracing::info!(topic = %config.topic, "created topic"),
        Err(Error::ServerError { protocol_error: ProtocolError::TopicAlreadyExists, .. }) => {}
        Err(err) => return Err(err),
    }

    client.partition_client(config.topic.clone(), PARTITION, UnknownTopicHandling::Retry).await
}

/// Exponential backoff with jitter, used for reconnect loops.
#[derive(Debug)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self { initial, max, current: initial }
    }

    /// Returns the next delay (between 50% and 100% of the current step) and doubles the step.
    pub fn next_delay(&mut self) -> Duration {
        let step = self.current;
        self.current = (self.current * 2).min(self.max);
        let half = step / 2;
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.subsec_nanos() as u128);
        let jitter = nanos % (half.as_nanos().max(1));
        half + Duration::from_nanos(jitter as u64)
    }

    pub fn reset(&mut self) {
        self.current = self.initial;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_until_cap_and_resets() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_millis(400));
        let delays: Vec<_> = (0..5).map(|_| backoff.next_delay()).collect();
        for (delay, step) in delays.iter().zip([100, 200, 400, 400, 400]) {
            assert!(*delay >= Duration::from_millis(step / 2) && *delay <= Duration::from_millis(step));
        }
        backoff.reset();
        assert!(backoff.next_delay() <= Duration::from_millis(100));
    }
}
