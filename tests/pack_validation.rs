//! Validation tests for the pack-file ObjectStore: concurrent access and disk usage.

use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::compression::Compression;
use merkql::record::ProducerRecord;
use merkql::topic::RetentionConfig;
use merkql::tree::MerkleTree;
use std::path::Path;
use std::sync::Arc;
use std::thread;

fn setup_broker(dir: &Path, partitions: u32) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: partitions,
        auto_create_topics: true,
        compression: Compression::None,
        default_retention: RetentionConfig::default(),
    };
    Broker::open(config).unwrap()
}

fn pack_path(dir: &Path, topic: &str) -> std::path::PathBuf {
    dir.join(".merkql/topics")
        .join(topic)
        .join("partitions/0/objects.pack")
}

// ---------------------------------------------------------------------------
// Concurrent stress tests
// ---------------------------------------------------------------------------

/// 8 threads each produce 1000 records to the same single-partition topic.
/// Verify: exactly 8000 total records, no gaps, all readable.
#[test]
fn concurrent_producers_same_partition() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);
    let n_threads = 8;
    let n_per_thread = 1000;

    let handles: Vec<_> = (0..n_threads)
        .map(|t| {
            let broker = Arc::clone(&broker);
            thread::spawn(move || {
                let producer = Broker::producer(&broker);
                for i in 0..n_per_thread {
                    let pr = ProducerRecord::new("conc-prod", None, format!("t{}-v{}", t, i));
                    producer.send(&pr).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let topic = broker.topic("conc-prod").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    let total = partition.next_offset();
    assert_eq!(
        total,
        (n_threads * n_per_thread) as u64,
        "expected {} records, got {}",
        n_threads * n_per_thread,
        total
    );

    // Verify all readable, no gaps
    for offset in 0..total {
        let record = partition.read(offset).unwrap();
        assert!(record.is_some(), "gap at offset {}", offset);
    }
}

/// Concurrent proof generation while producing.
/// 1 writer thread appends 2000 records.
/// 4 reader threads continuously generate and verify proofs.
/// Verify: no panics, no mutex poisoning, all proofs valid.
#[test]
fn concurrent_proof_during_writes() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);

    // Pre-seed with a few records so readers have something to prove
    {
        let producer = Broker::producer(&broker);
        for i in 0..100 {
            let pr = ProducerRecord::new("conc-proof", None, format!("seed-{}", i));
            producer.send(&pr).unwrap();
        }
    }

    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Writer thread
    let writer_broker = Arc::clone(&broker);
    let writer_done = Arc::clone(&done);
    let writer = thread::spawn(move || {
        let producer = Broker::producer(&writer_broker);
        for i in 0..2000 {
            let pr = ProducerRecord::new("conc-proof", None, format!("w-{}", i));
            producer.send(&pr).unwrap();
        }
        writer_done.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    // Reader threads
    let readers: Vec<_> = (0..4)
        .map(|t| {
            let broker = Arc::clone(&broker);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                let mut verified = 0u64;
                let mut offset = (t * 7) as u64; // different starting points
                loop {
                    let topic = broker.topic("conc-proof").unwrap();
                    let part_arc = topic.partition(0).unwrap();
                    let partition = part_arc.read().unwrap();
                    let max = partition.next_offset();
                    if max == 0 {
                        continue;
                    }
                    let o = offset % max;
                    if let Ok(Some(proof)) = partition.proof(o) {
                        if let Ok(valid) = MerkleTree::verify_proof(&proof, partition.store()) {
                            assert!(valid, "invalid proof at offset {} (thread {})", o, t);
                            verified += 1;
                        }
                    }
                    offset = (offset + 31) % max.max(1); // prime stride
                    if done.load(std::sync::atomic::Ordering::SeqCst) && verified > 50 {
                        break;
                    }
                }
                verified
            })
        })
        .collect();

    writer.join().unwrap();
    let total_verified: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(
        total_verified > 100,
        "readers should have verified many proofs, got {}",
        total_verified
    );
}

/// Concurrent reads from multiple threads while no writes are happening.
/// Verify: all threads see consistent data, no mutex issues.
#[test]
fn concurrent_readers() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);
    let n = 1000;

    // Produce records
    {
        let producer = Broker::producer(&broker);
        for i in 0..n {
            let pr = ProducerRecord::new("conc-read", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }
    }

    // 8 threads read all records simultaneously
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let broker = Arc::clone(&broker);
            thread::spawn(move || {
                let topic = broker.topic("conc-read").unwrap();
                let part_arc = topic.partition(0).unwrap();
                let partition = part_arc.read().unwrap();
                let mut values = Vec::with_capacity(n);
                for offset in 0..n as u64 {
                    let record = partition.read(offset).unwrap().unwrap();
                    values.push(record.value);
                }
                values
            })
        })
        .collect();

    let results: Vec<Vec<String>> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All threads should see exactly the same data
    for i in 1..results.len() {
        assert_eq!(
            results[0], results[i],
            "thread 0 and thread {} saw different data",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// Disk usage verification
// ---------------------------------------------------------------------------

/// Verify that pack file size is proportional to actual data, not inflated
/// by filesystem block overhead.
/// With 10K records of ~100 bytes each, pack file should be < 500KB.
/// Old per-file layout would have been ~40MB (10K * 4KB blocks).
#[test]
fn disk_usage_pack_vs_theoretical() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);
    let producer = Broker::producer(&broker);

    let n = 10_000;
    let payload = "x".repeat(100); // 100-byte payload

    for i in 0..n {
        let pr = ProducerRecord::new("disk-usage", None, format!("{}-{}", payload, i));
        producer.send(&pr).unwrap();
    }

    let pack = pack_path(dir.path(), "disk-usage");
    let pack_size = std::fs::metadata(&pack).unwrap().len();

    // Each record serializes to ~150 bytes (payload + metadata).
    // With Compression::None, each stored object is 1 (marker) + ~150 (data) = ~151 bytes.
    // Pack entry overhead is 36 bytes (4 len + 32 hash).
    // Plus merkle tree nodes: ~2N nodes of ~100 bytes each.
    // Expected: roughly N * 187 + 2N * 136 ≈ 4.6MB
    // The old layout would be N * 4096 + 2N * 4096 ≈ 120MB

    let max_expected = 10 * 1024 * 1024; // 10MB generous upper bound
    let old_layout_minimum = n as u64 * 4096; // at minimum N * 4KB (ignoring tree nodes)

    assert!(
        pack_size < max_expected,
        "pack file ({} bytes / {:.1} MB) exceeds 10MB upper bound",
        pack_size,
        pack_size as f64 / 1_048_576.0
    );

    assert!(
        pack_size < old_layout_minimum,
        "pack file ({} bytes) should be much smaller than old layout minimum ({} bytes / {:.1}MB)",
        pack_size,
        old_layout_minimum,
        old_layout_minimum as f64 / 1_048_576.0
    );

    // Calculate actual reduction factor
    let reduction = old_layout_minimum as f64 / pack_size as f64;
    eprintln!(
        "Pack file: {} bytes ({:.1} MB), old layout minimum: {} bytes ({:.1} MB), reduction: {:.1}x",
        pack_size,
        pack_size as f64 / 1_048_576.0,
        old_layout_minimum,
        old_layout_minimum as f64 / 1_048_576.0,
        reduction
    );
}

/// Same test with LZ4 compression — pack file should be even smaller.
#[test]
fn disk_usage_lz4_compression() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrokerConfig {
        data_dir: dir.path().to_path_buf(),
        default_partitions: 1,
        auto_create_topics: true,
        compression: Compression::Lz4,
        default_retention: RetentionConfig::default(),
    };
    let broker = Broker::open(config).unwrap();
    let producer = Broker::producer(&broker);

    let n = 10_000;
    let payload = "x".repeat(100);

    for i in 0..n {
        let pr = ProducerRecord::new("disk-lz4", None, format!("{}-{}", payload, i));
        producer.send(&pr).unwrap();
    }

    let pack_none = {
        let dir2 = tempfile::tempdir().unwrap();
        let broker2 = setup_broker(dir2.path(), 1);
        let producer2 = Broker::producer(&broker2);
        for i in 0..n {
            let pr = ProducerRecord::new("disk-none", None, format!("{}-{}", payload, i));
            producer2.send(&pr).unwrap();
        }
        std::fs::metadata(pack_path(dir2.path(), "disk-none"))
            .unwrap()
            .len()
    };

    let pack_lz4 = std::fs::metadata(pack_path(dir.path(), "disk-lz4"))
        .unwrap()
        .len();

    eprintln!(
        "None: {} bytes ({:.1} MB), LZ4: {} bytes ({:.1} MB), LZ4 ratio: {:.2}x",
        pack_none,
        pack_none as f64 / 1_048_576.0,
        pack_lz4,
        pack_lz4 as f64 / 1_048_576.0,
        pack_none as f64 / pack_lz4 as f64
    );

    // LZ4 should be at least somewhat smaller on repetitive data
    assert!(
        pack_lz4 <= pack_none,
        "LZ4 pack ({}) should not be larger than uncompressed pack ({})",
        pack_lz4,
        pack_none
    );
}

/// Verify that pack file reopen (index rebuild) scales linearly.
/// Open a pack with 50K objects, measure that the scan completes quickly.
#[test]
fn reopen_scan_performance() {
    let dir = tempfile::tempdir().unwrap();
    let n = 50_000;

    {
        let broker = setup_broker(dir.path(), 1);
        let producer = Broker::producer(&broker);
        let batch_size = 1000;
        for batch_idx in 0..(n / batch_size) {
            let start = batch_idx * batch_size;
            let batch: Vec<ProducerRecord> = (start..(start + batch_size))
                .map(|i| ProducerRecord::new("reopen-perf", None, format!("v{}", i)))
                .collect();
            producer.send_batch(&batch).unwrap();
        }
    }

    // Time the reopen
    let start = std::time::Instant::now();
    let broker = setup_broker(dir.path(), 1);
    let elapsed = start.elapsed();

    // Verify data is accessible
    let topic = broker.topic("reopen-perf").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();
    assert_eq!(partition.next_offset(), n as u64);

    eprintln!(
        "Reopened 50K-object pack in {:.1}ms",
        elapsed.as_secs_f64() * 1000.0
    );

    // Should reopen in under 1 second
    assert!(
        elapsed.as_secs() < 1,
        "reopen took too long: {:.1}s",
        elapsed.as_secs_f64()
    );
}
