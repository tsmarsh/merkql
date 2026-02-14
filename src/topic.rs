use crate::compression::Compression;
use crate::hash::Hash;
use crate::partition::Partition;
use crate::record::Record;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

/// Retention policy configuration for a topic.
///
/// # Examples
///
/// ```
/// use merkql::broker::BrokerConfig;
/// use merkql::topic::RetentionConfig;
///
/// let config = BrokerConfig {
///     default_retention: RetentionConfig { max_records: Some(10_000) },
///     ..BrokerConfig::new("/tmp/merkql-retention-example")
/// };
/// ```
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    pub max_records: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub num_partitions: u32,
}

/// A named collection of partitions with key-hash or round-robin routing.
pub struct Topic {
    config: TopicConfig,
    partitions: Vec<Arc<RwLock<Partition>>>,
    round_robin_counter: AtomicU32,
    retention: RetentionConfig,
}

impl Topic {
    /// Open or create a topic at the given directory.
    pub fn open(
        name: &str,
        num_partitions: u32,
        dir: impl Into<PathBuf>,
        compression: Compression,
        retention: RetentionConfig,
    ) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).context("creating topic dir")?;

        let config = TopicConfig {
            name: name.to_string(),
            num_partitions,
        };

        // Persist metadata
        let meta_path = dir.join("meta.bin");
        let meta_bytes = bincode::serialize(&config).context("serializing topic config")?;
        fs::write(&meta_path, &meta_bytes).context("writing topic metadata")?;

        // Open partitions
        let parts_dir = dir.join("partitions");
        fs::create_dir_all(&parts_dir)?;

        let mut partitions = Vec::new();
        for i in 0..num_partitions {
            let part = Partition::open(i, parts_dir.join(i.to_string()), compression)?;
            partitions.push(Arc::new(RwLock::new(part)));
        }

        Ok(Topic {
            config,
            partitions,
            round_robin_counter: AtomicU32::new(0),
            retention,
        })
    }

    /// Reopen an existing topic from its metadata.
    pub fn reopen(dir: impl Into<PathBuf>, compression: Compression) -> Result<Self> {
        let dir = dir.into();
        let meta_path = dir.join("meta.bin");
        let meta_bytes = fs::read(&meta_path).context("reading topic metadata")?;
        let config: TopicConfig =
            bincode::deserialize(&meta_bytes).context("deserializing topic config")?;

        let parts_dir = dir.join("partitions");
        let mut partitions = Vec::new();
        for i in 0..config.num_partitions {
            let part = Partition::open(i, parts_dir.join(i.to_string()), compression)?;
            partitions.push(Arc::new(RwLock::new(part)));
        }

        Ok(Topic {
            config,
            partitions,
            round_robin_counter: AtomicU32::new(0),
            retention: RetentionConfig::default(),
        })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn num_partitions(&self) -> u32 {
        self.config.num_partitions
    }

    /// Append a record, routing by key hash or round-robin.
    /// Takes &self (not &mut self) — acquires write lock on target partition only.
    pub fn append(&self, record: &mut Record) -> Result<u64> {
        let partition_id = self.route(&record.key);
        let part_arc = &self.partitions[partition_id as usize];
        let mut part = part_arc
            .write()
            .map_err(|e| anyhow::anyhow!("partition write lock: {}", e))?;
        let offset = part.append(record)?;

        // Apply retention if configured
        if let Some(max_records) = self.retention.max_records {
            let total = part.next_offset() - part.min_valid_offset();
            if total > max_records {
                let new_min = part.next_offset() - max_records;
                part.advance_retention(new_min)?;
            }
        }

        Ok(offset)
    }

    /// Get a partition by ID. Returns Arc<RwLock<Partition>> clone.
    pub fn partition(&self, id: u32) -> Option<Arc<RwLock<Partition>>> {
        self.partitions.get(id as usize).cloned()
    }

    /// Get all partition IDs.
    pub fn partition_ids(&self) -> Vec<u32> {
        (0..self.config.num_partitions).collect()
    }

    fn route(&self, key: &Option<String>) -> u32 {
        match key {
            Some(k) => {
                let hash = Hash::digest(k.as_bytes());
                let val = u32::from_be_bytes([hash.0[0], hash.0[1], hash.0[2], hash.0[3]]);
                val % self.config.num_partitions
            }
            None => {
                self.round_robin_counter.fetch_add(1, Ordering::Relaxed)
                    % self.config.num_partitions
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_record(topic: &str, key: Option<&str>, value: &str) -> Record {
        Record {
            key: key.map(|k| k.to_string()),
            value: value.into(),
            topic: topic.into(),
            partition: 0,
            offset: 0,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn single_partition_routing() {
        let dir = tempfile::tempdir().unwrap();
        let topic = Topic::open(
            "test",
            1,
            dir.path().join("topic"),
            Compression::None,
            RetentionConfig::default(),
        )
        .unwrap();

        let mut r1 = make_record("test", Some("k1"), "v1");
        let mut r2 = make_record("test", None, "v2");
        topic.append(&mut r1).unwrap();
        topic.append(&mut r2).unwrap();

        // Both should end up in partition 0
        assert_eq!(r1.partition, 0);
        assert_eq!(r2.partition, 0);
    }

    #[test]
    fn key_hash_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let topic = Topic::open(
            "test",
            4,
            dir.path().join("topic"),
            Compression::None,
            RetentionConfig::default(),
        )
        .unwrap();

        // Same key should always route to the same partition
        let mut r1 = make_record("test", Some("user-42"), "v1");
        let mut r2 = make_record("test", Some("user-42"), "v2");
        topic.append(&mut r1).unwrap();
        topic.append(&mut r2).unwrap();
        assert_eq!(r1.partition, r2.partition);
    }

    #[test]
    fn round_robin_distribution() {
        let dir = tempfile::tempdir().unwrap();
        let topic = Topic::open(
            "test",
            3,
            dir.path().join("topic"),
            Compression::None,
            RetentionConfig::default(),
        )
        .unwrap();

        let mut partitions_used = vec![0u32; 3];
        for i in 0..9 {
            let mut rec = make_record("test", None, &format!("v{}", i));
            topic.append(&mut rec).unwrap();
            partitions_used[rec.partition as usize] += 1;
        }

        // Each partition should get exactly 3 records
        assert_eq!(partitions_used, vec![3, 3, 3]);
    }

    #[test]
    fn metadata_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let topic_dir = dir.path().join("topic");

        {
            let topic = Topic::open(
                "my-topic",
                2,
                &topic_dir,
                Compression::None,
                RetentionConfig::default(),
            )
            .unwrap();
            let mut rec = make_record("my-topic", Some("k"), "v");
            topic.append(&mut rec).unwrap();
        }

        // Reopen
        let topic = Topic::reopen(&topic_dir, Compression::None).unwrap();
        assert_eq!(topic.name(), "my-topic");
        assert_eq!(topic.num_partitions(), 2);
    }
}
