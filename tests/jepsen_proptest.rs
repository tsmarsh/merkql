mod common;

use common::*;
use merkql::broker::{Broker, BrokerConfig};
use merkql::compression::Compression;
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use merkql::topic::RetentionConfig;
use merkql::tree::MerkleTree;
use proptest::prelude::*;
use std::collections::HashSet;
use std::time::Duration;

fn pt_broker_config(dir: &std::path::Path, partitions: u32) -> BrokerConfig {
    BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: partitions,
        auto_create_topics: true,
        compression: Compression::None,
        default_retention: RetentionConfig::default(),
    }
}

// ---------------------------------------------------------------------------
// Operations for random sequence generation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    Append { value: String },
    Read { offset: u64 },
    Commit,
    CloseReopen,
}

fn op_strategy(max_offset: u64) -> impl Strategy<Value = Op> {
    prop_oneof![
        70 => "[a-zA-Z0-9]{1,200}".prop_map(|v| Op::Append { value: v }),
        15 => (0..=max_offset).prop_map(|o| Op::Read { offset: o }),
        10 => Just(Op::Commit),
        5 => Just(Op::CloseReopen),
    ]
}

// ---------------------------------------------------------------------------
// Property 1: Random operations preserve invariants
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn random_ops_preserve_invariants(
        ops in proptest::collection::vec(op_strategy(200), 10..200),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut broker = Broker::open(pt_broker_config(dir.path(), 1)).unwrap();
        let mut appended_values: Vec<String> = Vec::new();

        // Create 3 topics up front
        broker.create_topic("prop-t0", 1).unwrap();
        broker.create_topic("prop-t1", 1).unwrap();
        broker.create_topic("prop-t2", 1).unwrap();

        let topics = ["prop-t0", "prop-t1", "prop-t2"];
        let mut consumer_created = false;

        for op in &ops {
            match op {
                Op::Append { value } => {
                    let topic = topics[appended_values.len() % 3];
                    let producer = Broker::producer(&broker);
                    let pr = ProducerRecord::new(topic, None, value.clone());
                    let record = producer.send(&pr).unwrap();
                    // Verify offset is sequential within partition
                    let t = broker.topic(topic).unwrap();
                    let part_arc = t.partition(record.partition).unwrap();
                    let p = part_arc.read().unwrap();
                    prop_assert!(record.offset < p.next_offset());
                    appended_values.push(value.clone());
                }
                Op::Read { offset } => {
                    for topic in &topics {
                        if let Some(t) = broker.topic(topic) {
                            let part_arc = t.partition(0).unwrap();
                            let p = part_arc.read().unwrap();
                            if *offset < p.next_offset() {
                                let record = p.read(*offset).unwrap();
                                prop_assert!(record.is_some());
                            }
                        }
                    }
                }
                Op::Commit => {
                    // Create consumer if needed and commit
                    if !appended_values.is_empty() && !consumer_created {
                        let mut consumer = Broker::consumer(
                            &broker,
                            ConsumerConfig {
                                group_id: "prop-group".into(),
                                auto_commit: false,
                                offset_reset: OffsetReset::Earliest,
                            },
                        );
                        let topic_refs: Vec<&str> = topics.to_vec();
                        consumer.subscribe(&topic_refs).unwrap();
                        consumer.poll(Duration::from_millis(10)).unwrap();
                        consumer.commit_sync().unwrap();
                        consumer.close().unwrap();
                        consumer_created = true;
                    }
                }
                Op::CloseReopen => {
                    drop(broker);
                    broker = Broker::open(pt_broker_config(dir.path(), 1)).unwrap();
                    consumer_created = false;
                }
            }
        }

        // Final invariant checks:
        // 1. Total order per partition
        for topic in &topics {
            if let Some(t) = broker.topic(topic) {
                let part_arc = t.partition(0).unwrap();
                let p = part_arc.read().unwrap();
                for offset in 0..p.next_offset() {
                    let record = p.read(offset).unwrap();
                    prop_assert!(record.is_some(), "gap at offset {}", offset);
                    prop_assert_eq!(record.unwrap().offset, offset);
                }
            }
        }

        // 2. Proofs valid for all records
        for topic in &topics {
            if let Some(t) = broker.topic(topic) {
                let part_arc = t.partition(0).unwrap();
                let p = part_arc.read().unwrap();
                for offset in 0..p.next_offset() {
                    if let Some(proof) = p.proof(offset).unwrap() {
                        let valid = MerkleTree::verify_proof(&proof, p.store()).unwrap();
                        prop_assert!(valid, "invalid proof at offset {}", offset);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Property 2: Payload size fidelity (up to 1MB)
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 100,
        timeout: 60000,
        ..ProptestConfig::default()
    })]

    #[test]
    fn payload_size_fidelity(
        payload in "[\\x20-\\x7E]{1,1048576}",
    ) {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        let producer = Broker::producer(&broker);

        let pr = ProducerRecord::new("fidelity", None, payload.clone());
        let record = producer.send(&pr).unwrap();

        let topic = broker.topic("fidelity").unwrap();
        let part_arc = topic.partition(record.partition).unwrap();
        let partition = part_arc.read().unwrap();
        let read_back = partition.read(record.offset).unwrap().unwrap();

        prop_assert_eq!(&read_back.value, &payload, "payload not preserved");
    }
}

// ---------------------------------------------------------------------------
// Property 3: Multi-topic/partition invariants
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn multi_topic_partition_invariants(
        num_topics in 1usize..=5,
        num_partitions in 1u32..=8,
        num_records in 10usize..=200,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), num_partitions);

        // Create topics
        let topic_names: Vec<String> = (0..num_topics).map(|i| format!("mt-{}", i)).collect();
        for name in &topic_names {
            broker.create_topic(name, num_partitions).unwrap();
        }

        // Produce records across all topics
        let producer = Broker::producer(&broker);
        for i in 0..num_records {
            let topic = &topic_names[i % num_topics];
            let pr = ProducerRecord::new(topic.as_str(), None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }

        // Verify invariants
        for name in &topic_names {
            let topic = broker.topic(name).unwrap();
            for pid in topic.partition_ids() {
                let part_arc = topic.partition(pid).unwrap();
                let partition = part_arc.read().unwrap();

                // Total order
                for offset in 0..partition.next_offset() {
                    let record = partition.read(offset).unwrap();
                    prop_assert!(record.is_some());
                    let record = record.unwrap();
                    prop_assert_eq!(record.offset, offset);
                    prop_assert_eq!(record.partition, pid);
                }

                // Proof validity
                for offset in 0..partition.next_offset() {
                    if let Some(proof) = partition.proof(offset).unwrap() {
                        let valid = MerkleTree::verify_proof(&proof, partition.store()).unwrap();
                        prop_assert!(valid, "invalid proof for {}/p{}/o{}", name, pid, offset);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Property 4: Multi-phase exactly-once
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn multi_phase_exactly_once(
        num_records in 50usize..=500,
        num_phases in 2usize..=6,
        reopen_between in proptest::bool::ANY,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut broker = Broker::open(pt_broker_config(dir.path(), 1)).unwrap();

        // Produce all records
        {
            let producer = Broker::producer(&broker);
            for i in 0..num_records {
                let pr = ProducerRecord::new("eo-prop", None, format!("v{}", i));
                producer.send(&pr).unwrap();
            }
        }

        // Consume in phases
        let mut all_consumed: Vec<String> = Vec::new();

        for _phase in 0..num_phases {
            if reopen_between {
                drop(broker);
                broker = Broker::open(pt_broker_config(dir.path(), 1)).unwrap();
            }

            let mut consumer = Broker::consumer(
                &broker,
                ConsumerConfig {
                    group_id: "eo-prop-group".into(),
                    auto_commit: false,
                    offset_reset: OffsetReset::Earliest,
                },
            );
            consumer.subscribe(&["eo-prop"]).unwrap();
            let records = consumer.poll(Duration::from_millis(100)).unwrap();
            all_consumed.extend(records.iter().map(|r| r.value.clone()));
            consumer.commit_sync().unwrap();
            consumer.close().unwrap();
        }

        // Verify exactly-once
        let expected: Vec<String> = (0..num_records).map(|i| format!("v{}", i)).collect();
        let unique: HashSet<&String> = all_consumed.iter().collect();

        prop_assert_eq!(
            all_consumed.len(),
            num_records,
            "should consume exactly {} records, got {}",
            num_records,
            all_consumed.len()
        );
        prop_assert_eq!(
            unique.len(),
            num_records,
            "should have no duplicates"
        );
        prop_assert_eq!(all_consumed, expected, "records should be in order");
    }
}

// ---------------------------------------------------------------------------
// Property 5: Binary payload fidelity
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 100,
        timeout: 30000,
        ..ProptestConfig::default()
    })]

    #[test]
    fn binary_payload_fidelity(
        bytes in proptest::collection::vec(proptest::num::u8::ANY, 1..65536),
    ) {
        let payload = String::from_utf8_lossy(&bytes).into_owned();
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        let producer = Broker::producer(&broker);

        let pr = ProducerRecord::new("binary-fidelity", None, payload.clone());
        let record = producer.send(&pr).unwrap();

        let topic = broker.topic("binary-fidelity").unwrap();
        let part_arc = topic.partition(record.partition).unwrap();
        let partition = part_arc.read().unwrap();
        let read_back = partition.read(record.offset).unwrap().unwrap();

        prop_assert_eq!(
            &read_back.value, &payload,
            "binary payload not preserved (len {})", payload.len()
        );
    }
}
