use crate::compression::Compression;
use crate::consumer::{Consumer, ConsumerConfig};
use crate::group::{ConsumerGroup, TopicPartition};
use crate::producer::Producer;
use crate::topic::{RetentionConfig, Topic};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

pub type BrokerRef = Arc<Broker>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub default_partitions: u32,
    pub auto_create_topics: bool,
    #[serde(skip, default)]
    pub compression: Compression,
    #[serde(skip, default)]
    pub default_retention: RetentionConfig,
}

impl BrokerConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        BrokerConfig {
            data_dir: data_dir.into(),
            default_partitions: 1,
            auto_create_topics: true,
            compression: Compression::None,
            default_retention: RetentionConfig::default(),
        }
    }
}

/// Top-level coordinator. Manages topics, consumer groups, and provides
/// factory methods for consumers and producers.
///
/// Uses internal fine-grained locking:
/// - `RwLock` on topics map (read-heavy, write-rare)
/// - `Mutex` on groups map (write-heavy, per-group locking)
pub struct Broker {
    config: BrokerConfig,
    topics: RwLock<HashMap<String, Arc<Topic>>>,
    groups: Mutex<HashMap<String, Arc<Mutex<ConsumerGroup>>>>,
}

impl Broker {
    /// Open or create a broker, scanning for existing topics.
    ///
    /// # Examples
    ///
    /// ```
    /// use merkql::broker::{Broker, BrokerConfig};
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    /// ```
    pub fn open(config: BrokerConfig) -> Result<BrokerRef> {
        let merkql_dir = config.data_dir.join(".merkql");
        fs::create_dir_all(&merkql_dir).context("creating .merkql dir")?;

        // Persist config (only serializable fields)
        let config_path = merkql_dir.join("config.bin");
        let config_bytes = bincode::serialize(&config).context("serializing config")?;
        fs::write(&config_path, &config_bytes).context("writing config")?;

        let mut topics = HashMap::new();
        let mut groups = HashMap::new();

        // Scan existing topics
        let topics_dir = merkql_dir.join("topics");
        if topics_dir.exists() {
            for entry in fs::read_dir(&topics_dir).context("reading topics dir")? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let topic_dir = entry.path();
                    if topic_dir.join("meta.bin").exists() {
                        let topic = Topic::reopen(&topic_dir, config.compression)?;
                        topics.insert(topic.name().to_string(), Arc::new(topic));
                    }
                }
            }
        }

        // Scan existing groups
        let groups_dir = merkql_dir.join("groups");
        if groups_dir.exists() {
            for entry in fs::read_dir(&groups_dir).context("reading groups dir")? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let group_dir = entry.path();
                    let group_id = entry.file_name().to_string_lossy().to_string();
                    let group = ConsumerGroup::open(&group_id, &group_dir)?;
                    groups.insert(group_id, Arc::new(Mutex::new(group)));
                }
            }
        }

        Ok(Arc::new(Broker {
            config,
            topics: RwLock::new(topics),
            groups: Mutex::new(groups),
        }))
    }

    /// Create a consumer for this broker.
    ///
    /// # Examples
    ///
    /// ```
    /// use merkql::broker::{Broker, BrokerConfig};
    /// use merkql::consumer::{ConsumerConfig, OffsetReset};
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    /// let mut consumer = Broker::consumer(&broker, ConsumerConfig {
    ///     group_id: "my-group".into(),
    ///     auto_commit: false,
    ///     offset_reset: OffsetReset::Earliest,
    /// });
    /// ```
    pub fn consumer(broker: &BrokerRef, config: ConsumerConfig) -> Consumer {
        Consumer::new(Arc::clone(broker), config)
    }

    /// Create a producer for this broker.
    ///
    /// # Examples
    ///
    /// ```
    /// use merkql::broker::{Broker, BrokerConfig};
    /// use merkql::record::ProducerRecord;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    /// let producer = Broker::producer(&broker);
    /// producer.send(&ProducerRecord::new("topic", None, "value")).unwrap();
    /// ```
    pub fn producer(broker: &BrokerRef) -> Producer {
        Producer::new(Arc::clone(broker))
    }

    /// Get a topic by name. Returns Arc<Topic> clone.
    pub fn topic(&self, name: &str) -> Option<Arc<Topic>> {
        let topics = self.topics.read().unwrap();
        topics.get(name).cloned()
    }

    /// Get a consumer group by ID. Returns Arc<Mutex<ConsumerGroup>> clone.
    pub fn group(&self, group_id: &str) -> Option<Arc<Mutex<ConsumerGroup>>> {
        let groups = self.groups.lock().unwrap();
        groups.get(group_id).cloned()
    }

    /// Ensure a topic exists, creating it if auto_create_topics is enabled.
    pub fn ensure_topic(&self, name: &str) -> Result<()> {
        // Fast path: read lock
        {
            let topics = self.topics.read().unwrap();
            if topics.contains_key(name) {
                return Ok(());
            }
        }

        if !self.config.auto_create_topics {
            anyhow::bail!(
                "topic '{}' does not exist and auto_create_topics is disabled",
                name
            );
        }

        // Slow path: write lock with double-check
        let mut topics = self.topics.write().unwrap();
        if topics.contains_key(name) {
            return Ok(());
        }

        let topics_dir = self.config.data_dir.join(".merkql").join("topics");
        let topic_dir = topics_dir.join(name);
        let topic = Topic::open(
            name,
            self.config.default_partitions,
            &topic_dir,
            self.config.compression,
            self.config.default_retention.clone(),
        )?;
        topics.insert(name.to_string(), Arc::new(topic));
        Ok(())
    }

    /// Create a topic explicitly with a given number of partitions.
    pub fn create_topic(&self, name: &str, num_partitions: u32) -> Result<()> {
        let mut topics = self.topics.write().unwrap();
        if topics.contains_key(name) {
            return Ok(());
        }
        let topics_dir = self.config.data_dir.join(".merkql").join("topics");
        let topic_dir = topics_dir.join(name);
        let topic = Topic::open(
            name,
            num_partitions,
            &topic_dir,
            self.config.compression,
            self.config.default_retention.clone(),
        )?;
        topics.insert(name.to_string(), Arc::new(topic));
        Ok(())
    }

    /// Commit offsets for a consumer group.
    pub fn commit_offsets(
        &self,
        group_id: &str,
        offsets: &HashMap<TopicPartition, u64>,
    ) -> Result<()> {
        let group_arc = {
            let mut groups = self.groups.lock().unwrap();
            if !groups.contains_key(group_id) {
                let groups_dir = self.config.data_dir.join(".merkql").join("groups");
                let group_dir = groups_dir.join(group_id);
                let group = ConsumerGroup::open(group_id, &group_dir)?;
                groups.insert(group_id.to_string(), Arc::new(Mutex::new(group)));
            }
            groups.get(group_id).unwrap().clone()
        };

        let mut group = group_arc.lock().unwrap();
        group.commit(offsets)
    }
}
