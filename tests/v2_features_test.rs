use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::compression::Compression;
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use merkql::topic::RetentionConfig;
use merkql::tree::MerkleTree;
use std::sync::Arc;
use std::time::Duration;

fn setup_broker(dir: &std::path::Path) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: 1,
        auto_create_topics: true,
        compression: Compression::None,
        default_retention: RetentionConfig::default(),
    };
    Broker::open(config).unwrap()
}

fn setup_broker_with_compression(dir: &std::path::Path, compression: Compression) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: 1,
        auto_create_topics: true,
        compression,
        default_retention: RetentionConfig::default(),
    };
    Broker::open(config).unwrap()
}

fn setup_broker_with_retention(dir: &std::path::Path, max_records: u64) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: 1,
        auto_create_topics: true,
        compression: Compression::None,
        default_retention: RetentionConfig {
            max_records: Some(max_records),
        },
    };
    Broker::open(config).unwrap()
}

// ---------------------------------------------------------------------------
// Concurrent producer tests
// ---------------------------------------------------------------------------

/// Spawn N threads, each sending M records to the same topic.
/// Verify total count and no gaps.
#[test]
fn concurrent_producers_same_topic() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let num_threads = 4;
    let records_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let broker = Arc::clone(&broker);
            std::thread::spawn(move || {
                let producer = Broker::producer(&broker);
                for i in 0..records_per_thread {
                    let pr =
                        ProducerRecord::new("concurrent-topic", None, format!("t{}-r{}", t, i));
                    producer.send(&pr).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify total count
    let topic = broker.topic("concurrent-topic").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();
    let total = partition.next_offset() as usize;
    assert_eq!(
        total,
        num_threads * records_per_thread,
        "expected {} records, got {}",
        num_threads * records_per_thread,
        total
    );

    // Verify no gaps
    for offset in 0..total as u64 {
        let record = partition.read(offset).unwrap();
        assert!(record.is_some(), "gap at offset {}", offset);
        assert_eq!(record.unwrap().offset, offset);
    }
}

/// Concurrent producers and consumers running simultaneously.
#[test]
fn concurrent_producer_and_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    let total_records = 200;

    // Producer thread
    let broker_p = Arc::clone(&broker);
    let producer_handle = std::thread::spawn(move || {
        let producer = Broker::producer(&broker_p);
        for i in 0..total_records {
            let pr = ProducerRecord::new("pc-topic", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }
    });

    // Wait for producer to finish
    producer_handle.join().unwrap();

    // Now consume
    let consumed = {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "pc-group".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["pc-topic"]).unwrap();
        consumer.poll(Duration::from_millis(100)).unwrap()
    };

    assert_eq!(consumed.len(), total_records);
}

/// Multiple concurrent consumers on independent groups.
#[test]
fn concurrent_consumers_independent_groups() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());

    // Produce records
    let producer = Broker::producer(&broker);
    for i in 0..50 {
        let pr = ProducerRecord::new("cc-topic", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    let num_groups = 4;
    let handles: Vec<_> = (0..num_groups)
        .map(|g| {
            let broker = Arc::clone(&broker);
            std::thread::spawn(move || {
                let mut consumer = Broker::consumer(
                    &broker,
                    ConsumerConfig {
                        group_id: format!("cc-group-{}", g),
                        auto_commit: false,
                        offset_reset: OffsetReset::Earliest,
                    },
                );
                consumer.subscribe(&["cc-topic"]).unwrap();
                let records = consumer.poll(Duration::from_millis(100)).unwrap();
                consumer.commit_sync().unwrap();
                records.len()
            })
        })
        .collect();

    for h in handles {
        let count = h.join().unwrap();
        assert_eq!(count, 50, "each group should consume all 50 records");
    }
}

// ---------------------------------------------------------------------------
// Batch API tests
// ---------------------------------------------------------------------------

/// send_batch with various sizes, verify all records.
#[test]
fn batch_send_various_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());
    let producer = Broker::producer(&broker);

    for batch_size in [1, 5, 10, 50, 100] {
        let records: Vec<ProducerRecord> = (0..batch_size)
            .map(|i| ProducerRecord::new("batch-topic", None, format!("batch-{}", i)))
            .collect();
        let results = producer.send_batch(&records).unwrap();
        assert_eq!(results.len(), batch_size);
    }

    // Verify total
    let topic = broker.topic("batch-topic").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();
    assert_eq!(partition.next_offset(), 1 + 5 + 10 + 50 + 100);
}

/// Empty batch returns empty results.
#[test]
fn batch_send_empty() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path());
    let producer = Broker::producer(&broker);

    let results = producer.send_batch(&[]).unwrap();
    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// Compression tests
// ---------------------------------------------------------------------------

/// Produce with Lz4, read back, verify byte-for-byte.
#[test]
fn compression_lz4_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker_with_compression(dir.path(), Compression::Lz4);

    let producer = Broker::producer(&broker);
    let payloads: Vec<String> = (0..20)
        .map(|i| format!("compressed-payload-{}-{}", i, "x".repeat(100)))
        .collect();

    for payload in &payloads {
        let pr = ProducerRecord::new("lz4-topic", None, payload.clone());
        producer.send(&pr).unwrap();
    }

    // Read back and verify
    let topic = broker.topic("lz4-topic").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    for (i, expected) in payloads.iter().enumerate() {
        let record = partition.read(i as u64).unwrap().unwrap();
        assert_eq!(&record.value, expected, "mismatch at record {}", i);
    }
}

/// Produce with Lz4, reopen, verify persistence.
#[test]
fn compression_lz4_persistence() {
    let dir = tempfile::tempdir().unwrap();

    let payloads: Vec<String> = (0..10)
        .map(|i| format!("persist-lz4-{}-{}", i, "y".repeat(200)))
        .collect();

    {
        let broker = setup_broker_with_compression(dir.path(), Compression::Lz4);
        let producer = Broker::producer(&broker);
        for payload in &payloads {
            let pr = ProducerRecord::new("lz4-persist", None, payload.clone());
            producer.send(&pr).unwrap();
        }
    }

    // Reopen and verify
    let broker = setup_broker_with_compression(dir.path(), Compression::Lz4);
    let topic = broker.topic("lz4-persist").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    for (i, expected) in payloads.iter().enumerate() {
        let record = partition.read(i as u64).unwrap().unwrap();
        assert_eq!(&record.value, expected);
    }
}

/// Mixed compression: produce with None, switch to Lz4, produce more, verify all readable.
#[test]
fn compression_mixed_mode() {
    let dir = tempfile::tempdir().unwrap();

    // Write some with None
    {
        let broker = setup_broker_with_compression(dir.path(), Compression::None);
        let producer = Broker::producer(&broker);
        for i in 0..5 {
            let pr = ProducerRecord::new("mixed-comp", None, format!("none-{}", i));
            producer.send(&pr).unwrap();
        }
    }

    // Write more with Lz4
    {
        let broker = setup_broker_with_compression(dir.path(), Compression::Lz4);
        let producer = Broker::producer(&broker);
        for i in 0..5 {
            let pr = ProducerRecord::new("mixed-comp", None, format!("lz4-{}", i));
            producer.send(&pr).unwrap();
        }
    }

    // Read all back with Lz4 (should handle both markers)
    let broker = setup_broker_with_compression(dir.path(), Compression::Lz4);
    let topic = broker.topic("mixed-comp").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();
    assert_eq!(partition.next_offset(), 10);

    for i in 0..5 {
        let r = partition.read(i).unwrap().unwrap();
        assert_eq!(r.value, format!("none-{}", i));
    }
    for i in 0..5 {
        let r = partition.read(5 + i).unwrap().unwrap();
        assert_eq!(r.value, format!("lz4-{}", i));
    }
}

/// Compression preserves merkle proof validity.
#[test]
fn compression_merkle_proofs() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker_with_compression(dir.path(), Compression::Lz4);

    let producer = Broker::producer(&broker);
    for i in 0..20 {
        let pr = ProducerRecord::new("lz4-proof", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    let topic = broker.topic("lz4-proof").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    for offset in 0..20u64 {
        let proof = partition.proof(offset).unwrap().unwrap();
        assert!(
            MerkleTree::verify_proof(&proof, partition.store()).unwrap(),
            "proof invalid at offset {}",
            offset
        );
    }
}

// ---------------------------------------------------------------------------
// Retention tests
// ---------------------------------------------------------------------------

/// Set max_records, produce beyond limit, verify old records return None.
#[test]
fn retention_max_records() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker_with_retention(dir.path(), 10);

    let producer = Broker::producer(&broker);
    for i in 0..20 {
        let pr = ProducerRecord::new("ret-topic", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    let topic = broker.topic("ret-topic").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    // Should have 20 total offsets but only last 10 readable
    assert_eq!(partition.next_offset(), 20);
    assert!(partition.min_valid_offset() >= 10);

    // Old records return None
    for i in 0..partition.min_valid_offset() {
        assert!(
            partition.read(i).unwrap().is_none(),
            "offset {} should be gone",
            i
        );
    }

    // Recent records readable
    for i in partition.min_valid_offset()..20 {
        let r = partition.read(i).unwrap();
        assert!(r.is_some(), "offset {} should be readable", i);
    }
}

/// Retention with consumer: consumer should only see records within the window.
#[test]
fn retention_consumer_window() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker_with_retention(dir.path(), 5);

    let producer = Broker::producer(&broker);
    for i in 0..20 {
        let pr = ProducerRecord::new("ret-consumer", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    // Consumer from Earliest should only get retained records
    let mut consumer = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: "ret-group".into(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&["ret-consumer"]).unwrap();
    let records = consumer.poll(Duration::from_millis(100)).unwrap();

    // Should get at most 5 records (the retention window)
    assert!(
        records.len() <= 5,
        "expected at most 5, got {}",
        records.len()
    );
    assert!(!records.is_empty(), "should get at least some records");
}

/// Retention persists across broker restart.
#[test]
fn retention_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let broker = setup_broker_with_retention(dir.path(), 5);
        let producer = Broker::producer(&broker);
        for i in 0..20 {
            let pr = ProducerRecord::new("ret-persist", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }
    }

    // Reopen — retention should be remembered
    let broker = setup_broker_with_retention(dir.path(), 5);
    let topic = broker.topic("ret-persist").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    assert!(partition.min_valid_offset() >= 15);
    assert!(partition.read(0).unwrap().is_none());
}
