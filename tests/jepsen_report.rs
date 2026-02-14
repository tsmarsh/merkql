mod common;

use common::*;
use merkql::broker::Broker;
use merkql::record::ProducerRecord;
use std::fs;

/// Generates a comprehensive Jepsen-style verification report.
/// Runs all 6 checker functions, 5 nemesis checkers, and 6 benchmark measurements.
/// Outputs JSON to stdout and target/jepsen-report.json.
#[test]
fn generate_jepsen_report() {
    let mut properties = Vec::new();
    let mut benchmarks = Vec::new();
    let mut nemesis = Vec::new();

    // --- Property checks ---

    eprintln!("Running checker: Total Order...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_total_order(dir.path(), 10_000, 4));
    }

    eprintln!("Running checker: Durability...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_durability(dir.path(), 5000, 3));
    }

    eprintln!("Running checker: Exactly-Once...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_exactly_once(dir.path(), 1000));
    }

    eprintln!("Running checker: Merkle Integrity...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_merkle_integrity(dir.path(), 10_000, 4));
    }

    eprintln!("Running checker: No Data Loss...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_no_data_loss(dir.path(), 5000));
    }

    eprintln!("Running checker: Byte Fidelity...");
    {
        let dir = tempfile::tempdir().unwrap();
        properties.push(check_byte_fidelity(dir.path()));
    }

    // --- Nemesis checks ---

    eprintln!("Running nemesis: Crash Recovery...");
    {
        let dir = tempfile::tempdir().unwrap();
        nemesis.push(check_crash_recovery(dir.path(), 100, 10));
    }

    eprintln!("Running nemesis: Truncated Snapshot...");
    {
        let dir = tempfile::tempdir().unwrap();
        nemesis.push(check_truncated_snapshot(dir.path(), 100));
    }

    eprintln!("Running nemesis: Truncated Index...");
    {
        let dir = tempfile::tempdir().unwrap();
        nemesis.push(check_truncated_index(dir.path(), 100));
    }

    eprintln!("Running nemesis: Missing Snapshot...");
    {
        let dir = tempfile::tempdir().unwrap();
        nemesis.push(check_missing_snapshot(dir.path(), 100));
    }

    eprintln!("Running nemesis: Index Ahead of Snapshot...");
    {
        let dir = tempfile::tempdir().unwrap();
        nemesis.push(check_index_ahead_of_snapshot(dir.path(), 100));
    }

    // --- Benchmark measurements ---

    eprintln!("Running benchmark: Append Latency (256B)...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        let producer = Broker::producer(&broker);
        let payload = generate_payload(256);

        benchmarks.push(measure_latency(
            "Append Latency (256B payload)",
            1000,
            |_| {
                let pr = ProducerRecord::new("bench-append", None, payload.clone());
                producer.send(&pr).unwrap();
            },
        ));
    }

    eprintln!("Running benchmark: Append Latency by Payload Size...");
    for &size in &[64, 256, 1024, 4096, 16384, 65536] {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        let producer = Broker::producer(&broker);
        let payload = generate_payload(size);

        benchmarks.push(measure_latency(
            &format!("Append Latency ({}B payload)", size),
            500,
            |_| {
                let pr = ProducerRecord::new("bench-size", None, payload.clone());
                producer.send(&pr).unwrap();
            },
        ));
    }

    eprintln!("Running benchmark: Read Latency (sequential)...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        produce_n(
            &broker,
            "bench-read",
            10_000,
            |i| format!("v{}", i),
            |_| None,
        );

        benchmarks.push(measure_latency("Read Latency (sequential)", 10_000, |i| {
            let topic = broker.topic("bench-read").unwrap();
            let part_arc = topic.partition(0).unwrap();
            let partition = part_arc.read().unwrap();
            let _ = partition.read(i as u64).unwrap();
        }));
    }

    eprintln!("Running benchmark: Read Throughput (10K sequential scan)...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        produce_n(
            &broker,
            "bench-read-tp",
            10_000,
            |i| format!("v{}", i),
            |_| None,
        );

        benchmarks.push(measure_latency(
            "Read Throughput (10K sequential scan)",
            10_000,
            |i| {
                let topic = broker.topic("bench-read-tp").unwrap();
                let part_arc = topic.partition(0).unwrap();
                let partition = part_arc.read().unwrap();
                let _ = partition.read(i as u64).unwrap();
            },
        ));
    }

    eprintln!("Running benchmark: Proof Generation...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        produce_n(
            &broker,
            "bench-proof",
            10_000,
            |i| format!("v{}", i),
            |_| None,
        );

        benchmarks.push(measure_latency(
            "Proof Generate + Verify (10K log)",
            1000,
            |i| {
                let topic = broker.topic("bench-proof").unwrap();
                let part_arc = topic.partition(0).unwrap();
                let partition = part_arc.read().unwrap();
                let offset = (i * 7) as u64 % partition.next_offset(); // prime stride
                let proof = partition.proof(offset).unwrap().unwrap();
                let _ = merkql::tree::MerkleTree::verify_proof(&proof, partition.store()).unwrap();
            },
        ));
    }

    eprintln!("Running benchmark: Broker Reopen...");
    {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);
        produce_n(
            &broker,
            "bench-reopen",
            10_000,
            |i| format!("v{}", i),
            |_| None,
        );
        drop(broker);

        benchmarks.push(measure_latency("Broker Reopen (10K records)", 20, |_| {
            let _ = setup_broker(dir.path(), 1);
        }));
    }

    // --- Assemble and output report ---

    let report = JepsenReport {
        title: "merkql Jepsen-Style Verification Report".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        properties,
        nemesis,
        benchmarks,
    };

    let json = serde_json::to_string_pretty(&report).unwrap();
    println!("{}", json);

    // Write to target/
    let target_dir = std::path::Path::new("target");
    fs::create_dir_all(target_dir).unwrap();
    fs::write(target_dir.join("jepsen-report.json"), &json).unwrap();
    eprintln!("Report written to target/jepsen-report.json");

    // Assert all properties passed
    for prop in &report.properties {
        assert!(
            prop.passed,
            "Property '{}' failed: {}",
            prop.name, prop.details
        );
    }

    // Assert all nemesis tests passed
    for nem in &report.nemesis {
        assert!(nem.passed, "Nemesis '{}' failed: {}", nem.name, nem.details);
    }
}
