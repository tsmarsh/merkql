use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::compression::Compression;
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use merkql::topic::RetentionConfig;
use merkql::tree::MerkleTree;
use std::collections::HashSet;
use std::time::Duration;

/// 100K records: exercises the same properties as 1M but completes in minutes
/// rather than hours, and stays under ~500MB disk.
const SCALE: u64 = 100_000;

fn setup_broker(dir: &std::path::Path, partitions: u32) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: partitions,
        auto_create_topics: true,
        compression: Compression::None,
        default_retention: RetentionConfig::default(),
    };
    Broker::open(config).unwrap()
}

fn setup_broker_with_retention(
    dir: &std::path::Path,
    partitions: u32,
    max_records: u64,
) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: partitions,
        auto_create_topics: true,
        compression: Compression::None,
        default_retention: RetentionConfig {
            max_records: Some(max_records),
        },
    };
    Broker::open(config).unwrap()
}

/// Produce 100K records to a topic with 4 partitions. Verify every partition has
/// monotonically increasing, gap-free offsets. Verify total count = 100K.
#[test]
#[ignore]
fn scale_100k_total_order() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 4);
    let producer = Broker::producer(&broker);

    for i in 0..SCALE {
        let pr = ProducerRecord::new("scale-order", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    let topic = broker.topic("scale-order").unwrap();
    let mut total = 0u64;

    for part_id in topic.partition_ids() {
        let part_arc = topic.partition(part_id).unwrap();
        let partition = part_arc.read().unwrap();
        let count = partition.next_offset();
        total += count;

        // Verify monotonically increasing, gap-free offsets
        for offset in 0..count {
            let record = partition.read(offset).unwrap();
            assert!(
                record.is_some(),
                "gap at partition {} offset {}",
                part_id,
                offset
            );
        }
    }

    assert_eq!(total, SCALE);
}

/// Produce 100K records to a single partition. Verify proof generation succeeds at
/// offset 0, 50K, 99999. Verify all 3 proofs are valid. Check proof path length
/// is approximately log2(100K) ~ 17.
#[test]
#[ignore]
fn scale_100k_proof_depth() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);
    let producer = Broker::producer(&broker);

    for i in 0..SCALE {
        let pr = ProducerRecord::new("scale-proof", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    let topic = broker.topic("scale-proof").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    for &offset in &[0, SCALE / 2, SCALE - 1] {
        let proof = partition.proof(offset).unwrap().unwrap();
        assert!(
            MerkleTree::verify_proof(&proof, partition.store()).unwrap(),
            "proof invalid at offset {}",
            offset
        );
        // log2(100K) ≈ 17, allow margin
        assert!(
            proof.siblings.len() <= 25,
            "proof depth {} too large at offset {}",
            proof.siblings.len(),
            offset
        );
    }
}

/// Set max_records=1K. Produce 100K records. Verify only last 1K are readable.
/// Verify min_valid_offset ≈ 99K. Verify proof works for a recent record.
#[test]
#[ignore]
fn scale_100k_retention_sliding() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker_with_retention(dir.path(), 1, 1_000);
    let producer = Broker::producer(&broker);

    for i in 0..SCALE {
        let pr = ProducerRecord::new("scale-retention", None, format!("v{}", i));
        producer.send(&pr).unwrap();
    }

    let topic = broker.topic("scale-retention").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    assert_eq!(partition.next_offset(), SCALE);
    assert!(
        partition.min_valid_offset() >= SCALE - 1_000,
        "min_valid_offset {} should be >= {}",
        partition.min_valid_offset(),
        SCALE - 1_000
    );

    // Old records should be None
    assert!(partition.read(0).unwrap().is_none());

    // Recent records should be readable
    let recent_offset = SCALE - 1;
    let record = partition.read(recent_offset).unwrap();
    assert!(
        record.is_some(),
        "recent record at {} should be readable",
        recent_offset
    );

    // Proof should work for a recent record
    let proof = partition.proof(recent_offset).unwrap().unwrap();
    assert!(MerkleTree::verify_proof(&proof, partition.store()).unwrap());
}

/// Produce 100K records. Drop broker. Reopen. Verify next_offset = 100K.
/// Read 10 random records by offset, verify they exist.
#[test]
#[ignore]
fn scale_100k_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let broker = setup_broker(dir.path(), 1);
        let producer = Broker::producer(&broker);
        for i in 0..SCALE {
            let pr = ProducerRecord::new("scale-reopen", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }
    }

    // Reopen
    let broker = setup_broker(dir.path(), 1);
    let topic = broker.topic("scale-reopen").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    assert_eq!(partition.next_offset(), SCALE);

    // Read 10 records at various offsets using prime stride
    let mut offset = 0u64;
    for _ in 0..10 {
        let record = partition.read(offset).unwrap();
        assert!(
            record.is_some(),
            "record at offset {} missing after reopen",
            offset
        );
        offset = (offset + 9991) % SCALE; // prime stride
    }
}

/// Produce 100K records in 10 phases (10K each). Consumer group commits after
/// each phase. Verify no gaps, no duplicates across all phases.
#[test]
#[ignore]
fn scale_100k_consumer_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);
    let producer = Broker::producer(&broker);

    let mut all_values = HashSet::new();
    let phase_size = SCALE / 10;

    for phase in 0..10 {
        let start = phase * phase_size;
        for i in start..(start + phase_size) {
            let pr = ProducerRecord::new("scale-eo", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }

        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "scale-group".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["scale-eo"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();

        for record in &records {
            let is_new = all_values.insert(record.value.clone());
            assert!(is_new, "duplicate record: {}", record.value);
        }

        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }

    assert_eq!(
        all_values.len(),
        SCALE as usize,
        "expected {} unique records, got {}",
        SCALE,
        all_values.len()
    );
}

/// Produce 100K records via send_batch() in batches of 1000. Verify total count
/// and no gaps.
#[test]
#[ignore]
fn scale_100k_batch_throughput() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);
    let producer = Broker::producer(&broker);

    let batch_size: u64 = 1000;
    let num_batches = SCALE / batch_size;

    for batch_idx in 0..num_batches {
        let start = batch_idx * batch_size;
        let batch: Vec<ProducerRecord> = (start..(start + batch_size))
            .map(|i| ProducerRecord::new("scale-batch", None, format!("v{}", i)))
            .collect();
        producer.send_batch(&batch).unwrap();
    }

    let topic = broker.topic("scale-batch").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    assert_eq!(partition.next_offset(), SCALE);

    // Verify no gaps at boundaries
    for boundary in [0, batch_size - 1, batch_size, SCALE / 2, SCALE - 1] {
        let record = partition.read(boundary).unwrap();
        assert!(record.is_some(), "gap at offset {}", boundary);
    }
}
