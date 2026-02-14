use crate::broker::BrokerRef;
use crate::group::TopicPartition;
use crate::record::Record;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OffsetReset {
    Earliest,
    Latest,
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub group_id: String,
    pub auto_commit: bool,
    pub offset_reset: OffsetReset,
}

/// Kafka-compatible consumer. Owns a reference to the broker.
///
/// # Examples
///
/// ```
/// use merkql::broker::{Broker, BrokerConfig};
/// use merkql::consumer::{ConsumerConfig, OffsetReset};
/// use merkql::record::ProducerRecord;
/// use std::time::Duration;
///
/// let dir = tempfile::tempdir().unwrap();
/// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
///
/// // Produce some records first
/// let producer = Broker::producer(&broker);
/// producer.send(&ProducerRecord::new("events", None, "hello")).unwrap();
///
/// // Subscribe, poll, commit, close
/// let mut consumer = Broker::consumer(&broker, ConsumerConfig {
///     group_id: "example".into(),
///     auto_commit: false,
///     offset_reset: OffsetReset::Earliest,
/// });
/// consumer.subscribe(&["events"]).unwrap();
/// let records = consumer.poll(Duration::from_millis(100)).unwrap();
/// assert_eq!(records.len(), 1);
/// consumer.commit_sync().unwrap();
/// consumer.close().unwrap();
/// ```
pub struct Consumer {
    broker: BrokerRef,
    config: ConsumerConfig,
    subscribed_topics: Vec<String>,
    /// Current read positions: where the next poll will read from.
    positions: HashMap<TopicPartition, u64>,
    closed: bool,
}

impl Consumer {
    pub(crate) fn new(broker: BrokerRef, config: ConsumerConfig) -> Self {
        Consumer {
            broker,
            config,
            subscribed_topics: Vec::new(),
            positions: HashMap::new(),
            closed: false,
        }
    }

    /// Subscribe to topics. Resets positions based on committed offsets + reset policy.
    pub fn subscribe(&mut self, topics: &[&str]) -> Result<()> {
        if self.closed {
            bail!("consumer is closed");
        }

        self.subscribed_topics = topics.iter().map(|s| s.to_string()).collect();
        self.positions.clear();

        for topic_name in &self.subscribed_topics {
            if let Some(topic) = self.broker.topic(topic_name) {
                for part_id in topic.partition_ids() {
                    let tp = TopicPartition {
                        topic: topic_name.clone(),
                        partition: part_id,
                    };

                    // Check committed offset
                    let committed = self.broker.group(&self.config.group_id).and_then(|g| {
                        let guard = g.lock().unwrap();
                        guard.committed_offset(&tp)
                    });

                    let position = match committed {
                        Some(off) => off, // Resume from committed offset
                        None => match self.config.offset_reset {
                            OffsetReset::Earliest => 0,
                            OffsetReset::Latest => {
                                let part_arc = topic.partition(part_id).unwrap();
                                let part = part_arc.read().unwrap();
                                part.next_offset()
                            }
                        },
                    };

                    self.positions.insert(tp, position);
                }
            }
        }

        Ok(())
    }

    /// Poll for records. Returns records available from the current position
    /// up to the current tail of each partition. Takes read locks on partitions.
    pub fn poll(&mut self, _timeout: Duration) -> Result<Vec<Record>> {
        if self.closed {
            bail!("consumer is closed");
        }

        let mut records = Vec::new();

        for (tp, position) in &mut self.positions {
            if let Some(topic) = self.broker.topic(&tp.topic)
                && let Some(part_arc) = topic.partition(tp.partition)
            {
                let part = part_arc
                    .read()
                    .map_err(|e| anyhow::anyhow!("partition read lock: {}", e))?;
                let tail = part.next_offset();
                if *position < tail {
                    let batch = part.read_range(*position, tail)?;
                    *position = tail;
                    records.extend(batch);
                }
            }
        }

        Ok(records)
    }

    /// Commit current positions synchronously.
    pub fn commit_sync(&mut self) -> Result<()> {
        if self.closed {
            bail!("consumer is closed");
        }

        self.broker
            .commit_offsets(&self.config.group_id, &self.positions)
    }

    /// Close the consumer. If auto_commit is enabled, commits first.
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        if self.config.auto_commit {
            self.commit_sync()?;
        }

        self.closed = true;
        Ok(())
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close();
        }
    }
}
