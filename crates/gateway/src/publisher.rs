//! Ships matching events to Kafka off the hot path.
//!
//! The matcher hands events over a bounded channel. If the broker is slow or
//! down, the channel fills up and the matcher waits: we prefer backpressure over
//! silently dropping events. Batches are retried in order until they succeed, so
//! delivery is at-least-once and consumers dedupe by `sequence`.

use std::sync::Arc;
use std::time::Duration;

use events::MarketEvent;
use events::kafka::{self, Backoff, KafkaConfig};
use rskafka::chrono::DateTime;
use rskafka::client::partition::{Compression, PartitionClient};
use rskafka::record::Record;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub type EventSink = mpsc::Sender<Arc<MarketEvent>>;

pub fn spawn(
    config: KafkaConfig,
    capacity: usize,
    batch_size: usize,
    stop: CancellationToken,
) -> (EventSink, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(capacity);
    let task = tokio::spawn(run(config, rx, batch_size, stop));
    (tx, task)
}

async fn run(
    config: KafkaConfig,
    mut rx: mpsc::Receiver<Arc<MarketEvent>>,
    batch_size: usize,
    stop: CancellationToken,
) {
    let mut producer: Option<PartitionClient> = None;
    let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(10));
    let mut batch: Vec<Arc<MarketEvent>> = Vec::with_capacity(batch_size);

    loop {
        if batch.is_empty() && rx.recv_many(&mut batch, batch_size).await == 0 {
            break; // matcher is gone and the queue is drained
        }
        metrics::gauge!("orderflow_publisher_queue_depth").set(rx.len() as f64);

        let client = match &producer {
            Some(client) => client,
            None => match kafka::connect(&config, "orderflow-gateway").await {
                Ok(client) => {
                    tracing::info!(topic = %config.topic, "connected to Kafka");
                    backoff.reset();
                    producer.insert(client)
                }
                Err(err) => {
                    tracing::warn!(%err, "Kafka connect failed");
                    if !pause(&mut backoff, &stop).await {
                        break;
                    }
                    continue;
                }
            },
        };

        let records = batch.iter().map(|event| to_record(event)).collect();
        match client.produce(records, Compression::NoCompression).await {
            Ok(_) => {
                metrics::counter!("orderflow_events_published_total").increment(batch.len() as u64);
                batch.clear();
            }
            Err(err) => {
                tracing::warn!(%err, pending = batch.len(), "Kafka produce failed, reconnecting");
                metrics::counter!("orderflow_publish_errors_total").increment(1);
                producer = None;
                if !pause(&mut backoff, &stop).await {
                    break;
                }
            }
        }
    }

    let unsent = batch.len() + rx.len();
    if unsent > 0 {
        tracing::error!(unsent, "stopping with events that never reached Kafka");
    }
    tracing::info!("event publisher stopped");
}

/// Sleeps for the next backoff step. Returns false if we are stopping, so a dead
/// broker cannot hold the process hostage on exit.
async fn pause(backoff: &mut Backoff, stop: &CancellationToken) -> bool {
    if stop.is_cancelled() {
        return false;
    }
    tokio::select! {
        () = tokio::time::sleep(backoff.next_delay()) => true,
        () = stop.cancelled() => false,
    }
}

fn to_record(event: &MarketEvent) -> Record {
    Record {
        key: Some(event.symbol().as_bytes().to_vec()),
        value: Some(event.to_json()),
        headers: Default::default(),
        timestamp: DateTime::from_timestamp_nanos(event.timestamp_ns()),
    }
}
