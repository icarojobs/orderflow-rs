//! Kafka consumer that survives broker restarts.
//!
//! The consumer keeps its position in memory. When a fetch fails it drops the
//! connection, backs off and resumes from the same offset, so a broker restart
//! shows up as a pause, not as lost or repeated updates.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use events::MarketEvent;
use events::kafka::{self, Backoff, KafkaConfig};
use rskafka::client::error::{Error, ProtocolError};
use rskafka::client::partition::{OffsetAt, PartitionClient};
use tokio_util::sync::CancellationToken;

use crate::hub::{Hub, Update};

const MAX_FETCH_BYTES: i32 = 1024 * 1024;
const MAX_WAIT_MS: i32 = 500;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartFrom {
    Earliest,
    Latest,
}

pub struct Consumer {
    pub config: KafkaConfig,
    pub start: StartFrom,
    pub hub: Hub,
    /// Readiness flag: true while we hold a working partition client.
    pub connected: Arc<AtomicBool>,
}

impl Consumer {
    pub async fn run(self, stop: CancellationToken) {
        let mut backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(10));
        let mut offset: Option<i64> = None;
        let mut sequence = SequenceTracker::default();

        while !stop.is_cancelled() {
            let client = tokio::select! {
                result = kafka::connect(&self.config, "orderflow-market-data") => result,
                () = stop.cancelled() => break,
            };
            let client = match client {
                Ok(client) => client,
                Err(err) => {
                    tracing::warn!(%err, "Kafka connect failed");
                    metrics::counter!("md_kafka_reconnects_total").increment(1);
                    sleep_or_stop(backoff.next_delay(), &stop).await;
                    continue;
                }
            };

            let position = match offset {
                Some(position) => position,
                None => match self.initial_offset(&client).await {
                    Ok(position) => position,
                    Err(err) => {
                        tracing::warn!(%err, "could not resolve start offset");
                        sleep_or_stop(backoff.next_delay(), &stop).await;
                        continue;
                    }
                },
            };
            tracing::info!(topic = %self.config.topic, offset = position, "consuming");
            self.connected.store(true, Ordering::Relaxed);
            backoff.reset();

            let result = self.consume(&client, position, &mut sequence, &stop).await;
            self.connected.store(false, Ordering::Relaxed);
            match result {
                Ok(next) => offset = Some(next), // stopped cleanly
                Err((next, err)) => {
                    offset = Some(next);
                    tracing::warn!(%err, offset = next, "fetch failed, reconnecting");
                    metrics::counter!("md_kafka_reconnects_total").increment(1);
                    sleep_or_stop(backoff.next_delay(), &stop).await;
                }
            }
        }
        tracing::info!("consumer stopped");
    }

    async fn initial_offset(&self, client: &PartitionClient) -> Result<i64, Error> {
        let at = match self.start {
            StartFrom::Earliest => OffsetAt::Earliest,
            StartFrom::Latest => OffsetAt::Latest,
        };
        client.get_offset(at).await
    }

    /// Fetch loop. Returns the next offset to read, alongside the error if it failed.
    async fn consume(
        &self,
        client: &PartitionClient,
        mut offset: i64,
        sequence: &mut SequenceTracker,
        stop: &CancellationToken,
    ) -> Result<i64, (i64, Error)> {
        loop {
            let fetched = tokio::select! {
                result = client.fetch_records(offset, 1..MAX_FETCH_BYTES, MAX_WAIT_MS) => result,
                () = stop.cancelled() => return Ok(offset),
            };
            let (records, high_watermark) = match fetched {
                Ok(fetched) => fetched,
                Err(Error::ServerError { protocol_error: ProtocolError::OffsetOutOfRange, .. }) => {
                    // Retention removed what we wanted; jump to the oldest record still there.
                    let earliest = client.get_offset(OffsetAt::Earliest).await.map_err(|e| (offset, e))?;
                    tracing::warn!(from = offset, to = earliest, "offset out of range, skipping ahead");
                    offset = earliest;
                    continue;
                }
                Err(err) => return Err((offset, err)),
            };

            for record in records {
                offset = record.offset + 1;
                let Some(value) = record.record.value else { continue };
                let event = match MarketEvent::from_json(&value) {
                    Ok(event) => event,
                    Err(err) => {
                        tracing::warn!(%err, offset = record.offset, "skipping malformed event");
                        continue;
                    }
                };
                if !sequence.accept(event.sequence()) {
                    metrics::counter!("md_duplicates_total").increment(1);
                    continue;
                }
                metrics::counter!("md_events_consumed_total").increment(1);
                let json = String::from_utf8(value).unwrap_or_default();
                self.hub.publish(Update { symbol: event.symbol().into(), json: json.into() });
            }
            metrics::gauge!("md_consumer_lag").set((high_watermark - offset).max(0) as f64);
        }
    }
}

async fn sleep_or_stop(delay: Duration, stop: &CancellationToken) {
    tokio::select! {
        () = tokio::time::sleep(delay) => {},
        () = stop.cancelled() => {},
    }
}

/// Drops redelivered events (the publisher is at-least-once) and reports gaps.
/// A sequence of 1 means the gateway restarted, so tracking starts over.
#[derive(Debug, Default)]
pub struct SequenceTracker {
    last: u64,
}

impl SequenceTracker {
    pub fn accept(&mut self, sequence: u64) -> bool {
        if sequence == 1 || self.last == 0 {
            self.last = sequence;
            return true;
        }
        if sequence <= self.last {
            return false;
        }
        if sequence > self.last + 1 {
            let missing = sequence - self.last - 1;
            metrics::counter!("md_sequence_gaps_total").increment(1);
            tracing::warn!(last = self.last, next = sequence, missing, "sequence gap");
        }
        self.last = sequence;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::SequenceTracker;

    #[test]
    fn drops_duplicates_and_accepts_gaps() {
        let mut t = SequenceTracker::default();
        assert!(t.accept(5));
        assert!(t.accept(6));
        assert!(!t.accept(6));
        assert!(!t.accept(4));
        assert!(t.accept(9));
        assert!(t.accept(1), "producer restart resets tracking");
        assert!(t.accept(2));
    }
}
