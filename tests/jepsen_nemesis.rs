mod common;

use common::*;

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
