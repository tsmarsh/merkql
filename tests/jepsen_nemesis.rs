mod common;

use common::*;
use merkql::broker::Broker;
use merkql::consumer::OffsetReset;
use merkql::record::ProducerRecord;

/// Nemesis 1: Crash simulation — drop broker without close across 10 cycles.
/// Asserts: all records survive ungraceful drops.
#[test]
fn crash_drop_without_close() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_crash_recovery(dir.path(), 100, 10);
    assert!(result.passed, "Crash recovery failed: {}", result.details);
    assert_eq!(result.records_after, result.records_before);
}

/// Nemesis 2: Truncated tree.snapshot — halve the snapshot file and reopen.
/// Asserts: broker either refuses to reopen (safe failure) or reopens gracefully.
#[test]
fn truncated_tree_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_truncated_snapshot(dir.path(), 100);
    assert!(result.passed, "Truncated snapshot: {}", result.details);
    // Must be either safe_failure or recovered — never silent corruption
    assert!(
        result.outcome == "safe_failure" || result.outcome == "recovered",
        "Unexpected outcome: {}",
        result.outcome
    );
}

/// Nemesis 3: Truncated offsets.idx — remove 16 bytes (half an entry) from the index.
/// Asserts: broker reopens with N-1 complete entries, all readable, zero errors.
#[test]
fn truncated_offsets_index() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_truncated_index(dir.path(), 100);
    assert!(result.passed, "Truncated index: {}", result.details);
    // Should have lost exactly one record (the partial entry)
    assert!(
        result.records_after == 99,
        "Expected 99 readable records after truncation, got {}",
        result.records_after
    );
}

/// Nemesis 4: Missing tree.snapshot — delete snapshot file and reopen.
/// Asserts: broker reopens, all records readable, new appends succeed.
#[test]
fn missing_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_missing_snapshot(dir.path(), 100);
    assert!(result.passed, "Missing snapshot: {}", result.details);
    assert_eq!(
        result.records_after, 100,
        "All records should be readable without snapshot"
    );
}

/// Nemesis 5: Index ahead of snapshot — simulates crash between index write and snapshot write.
/// Asserts: all records readable, at least N valid proofs out of N+1.
#[test]
fn index_ahead_of_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let result = check_index_ahead_of_snapshot(dir.path(), 100);
    assert!(result.passed, "Index ahead of snapshot: {}", result.details);
    assert_eq!(
        result.records_after, 101,
        "Expected 101 readable records, got {}",
        result.records_after
    );
}

// ---------------------------------------------------------------------------
// Pack file nemesis tests
// ---------------------------------------------------------------------------

fn pack_path(dir: &std::path::Path, topic: &str) -> std::path::PathBuf {
    dir.join(".merkql/topics")
        .join(topic)
        .join("partitions/0/objects.pack")
}

/// Nemesis 6: Truncated pack file — cut mid-header (2 bytes into a 4-byte length field).
/// Asserts: broker reopens, all complete records readable, partial entry discarded.
#[test]
fn truncated_pack_mid_header() {
    let dir = tempfile::tempdir().unwrap();
    let n = 100;

    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(
            &broker,
            "trunc-pack-hdr",
            n,
            |i| format!("v{}", i),
            |_| None,
        );
    }

    let pack = pack_path(dir.path(), "trunc-pack-hdr");
    let original_len = std::fs::metadata(&pack).unwrap().len();

    // Append 2 garbage bytes — simulates crash mid-header write of a new entry
    let mut data = std::fs::read(&pack).unwrap();
    data.extend_from_slice(&[0xDE, 0xAD]);
    std::fs::write(&pack, &data).unwrap();

    let broker = Broker::open(broker_config(dir.path())).unwrap();
    let consumed = consume_all(
        &broker,
        "trunc-pack-hdr-verify",
        &["trunc-pack-hdr"],
        OffsetReset::Earliest,
    );

    assert_eq!(
        consumed.len(),
        n,
        "all {} records should survive; partial header should be truncated",
        n
    );

    // Pack file should be truncated back to original size
    let new_len = std::fs::metadata(&pack).unwrap().len();
    assert_eq!(
        new_len, original_len,
        "pack should be truncated to last good entry"
    );
}

/// Nemesis 7: Truncated pack file — cut mid-hash (20 bytes into a 32-byte hash field).
/// Asserts: broker reopens, all prior records readable, partial entry discarded.
#[test]
fn truncated_pack_mid_hash() {
    let dir = tempfile::tempdir().unwrap();
    let n = 100;

    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(
            &broker,
            "trunc-pack-hash",
            n,
            |i| format!("v{}", i),
            |_| None,
        );
    }

    let pack = pack_path(dir.path(), "trunc-pack-hash");
    let original_len = std::fs::metadata(&pack).unwrap().len();

    // Append a valid length field (4 bytes) + 20 bytes of hash (incomplete)
    let mut data = std::fs::read(&pack).unwrap();
    data.extend_from_slice(&50u32.to_le_bytes()); // data_length = 50
    data.extend_from_slice(&[0xAB; 20]); // partial hash (need 32)
    std::fs::write(&pack, &data).unwrap();

    let broker = Broker::open(broker_config(dir.path())).unwrap();
    let consumed = consume_all(
        &broker,
        "trunc-pack-hash-verify",
        &["trunc-pack-hash"],
        OffsetReset::Earliest,
    );

    assert_eq!(consumed.len(), n);

    let new_len = std::fs::metadata(&pack).unwrap().len();
    assert_eq!(new_len, original_len);
}

/// Nemesis 8: Truncated pack file — cut mid-data (valid header + hash, but data truncated).
/// Asserts: broker reopens, all prior records readable, partial entry discarded.
#[test]
fn truncated_pack_mid_data() {
    let dir = tempfile::tempdir().unwrap();
    let n = 100;

    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(
            &broker,
            "trunc-pack-data",
            n,
            |i| format!("v{}", i),
            |_| None,
        );
    }

    let pack = pack_path(dir.path(), "trunc-pack-data");
    let original_len = std::fs::metadata(&pack).unwrap().len();

    // Append a complete header (4 + 32 bytes) but only 5 of 100 claimed data bytes
    let mut data = std::fs::read(&pack).unwrap();
    data.extend_from_slice(&100u32.to_le_bytes()); // claims 100 bytes of data
    data.extend_from_slice(&[0xCC; 32]); // full hash
    data.extend_from_slice(&[0xDD; 5]); // only 5 of 100 data bytes
    std::fs::write(&pack, &data).unwrap();

    let broker = Broker::open(broker_config(dir.path())).unwrap();
    let consumed = consume_all(
        &broker,
        "trunc-pack-data-verify",
        &["trunc-pack-data"],
        OffsetReset::Earliest,
    );

    assert_eq!(consumed.len(), n);

    let new_len = std::fs::metadata(&pack).unwrap().len();
    assert_eq!(new_len, original_len);
}

/// Nemesis 9: Missing pack file — delete objects.pack and reopen.
/// Asserts: broker reopens (index drives offset count), reads fail gracefully
/// for records whose objects are gone.
#[test]
fn missing_pack_file() {
    let dir = tempfile::tempdir().unwrap();
    let n = 50;

    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(&broker, "no-pack", n, |i| format!("v{}", i), |_| None);
    }

    let pack = pack_path(dir.path(), "no-pack");
    assert!(pack.exists(), "pack file should exist after producing");
    std::fs::remove_file(&pack).unwrap();

    // Broker should reopen — the index determines next_offset
    let broker = Broker::open(broker_config(dir.path())).unwrap();
    let topic = broker.topic("no-pack").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    // The partition knows about N offsets (from the index), but reads will fail
    // because the pack file is now empty
    assert_eq!(partition.next_offset(), n as u64);

    // Reading should error (objects are gone) — this is expected
    let read_result = partition.read(0);
    assert!(
        read_result.is_err(),
        "reading from empty pack should fail, not silently succeed"
    );

    // But we should be able to append new records
    drop(partition);
    drop(part_arc);
    let producer = Broker::producer(&broker);
    let pr = ProducerRecord::new("no-pack", None, "after-pack-delete");
    assert!(
        producer.send(&pr).is_ok(),
        "should be able to append after pack file loss"
    );
}

/// Nemesis 10: Corrupted byte in the middle of pack file — flip a byte in a
/// record's compressed data. The entry is structurally valid so it passes the
/// scan, but decompression should fail at read time.
#[test]
fn corrupted_pack_mid_file() {
    let dir = tempfile::tempdir().unwrap();
    let n = 100;

    {
        let broker = Broker::open(broker_config(dir.path())).unwrap();
        produce_n(&broker, "corrupt-mid", n, |i| format!("v{}", i), |_| None);
    }

    let pack = pack_path(dir.path(), "corrupt-mid");
    let mut data = std::fs::read(&pack).unwrap();

    // Corrupt a byte roughly in the middle of the file (inside some entry's data)
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&pack, &data).unwrap();

    // Broker should reopen (scan doesn't decompress)
    let broker = Broker::open(broker_config(dir.path())).unwrap();
    let topic = broker.topic("corrupt-mid").unwrap();
    let part_arc = topic.partition(0).unwrap();
    let partition = part_arc.read().unwrap();

    // Most records should still be readable; the corrupted one should error
    let mut readable = 0;
    let mut errors = 0;
    for offset in 0..partition.next_offset() {
        match partition.read(offset) {
            Ok(Some(_)) => readable += 1,
            Ok(None) => {}
            Err(_) => errors += 1,
        }
    }

    // At least some records should be readable (corruption is localized)
    assert!(
        readable > 0,
        "should have some readable records despite mid-file corruption"
    );
    // The total should still account for all offsets
    assert_eq!(
        readable + errors,
        n,
        "readable + errors should equal total records"
    );
}
