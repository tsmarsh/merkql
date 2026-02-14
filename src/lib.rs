//! # merkql
//!
//! Embedded event log with Kafka-compatible semantics and merkle tree
//! integrity verification.
//!
//! ```
//! use merkql::broker::{Broker, BrokerConfig};
//! use merkql::consumer::{ConsumerConfig, OffsetReset};
//! use merkql::record::ProducerRecord;
//! use std::time::Duration;
//!
//! let dir = tempfile::tempdir().unwrap();
//! let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
//!
//! // Produce
//! let producer = Broker::producer(&broker);
//! producer.send(&ProducerRecord::new("events", None, "hello")).unwrap();
//!
//! // Consume
//! let mut consumer = Broker::consumer(&broker, ConsumerConfig {
//!     group_id: "example".into(),
//!     auto_commit: false,
//!     offset_reset: OffsetReset::Earliest,
//! });
//! consumer.subscribe(&["events"]).unwrap();
//! let records = consumer.poll(Duration::from_millis(100)).unwrap();
//! assert_eq!(records.len(), 1);
//! assert_eq!(records[0].value, "hello");
//! ```

pub mod broker;
pub mod compression;
pub mod consumer;
pub mod group;
pub mod hash;
pub mod node;
pub mod partition;
pub mod producer;
pub mod record;
pub mod store;
pub mod topic;
pub mod tree;
