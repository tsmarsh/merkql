# Failure Modes

This document catalogs every known failure mode of merkql, what happens, whether data is lost, and whether recovery is automatic.

## 1. Disk Full

| Component | Behavior | Data Loss | Recovery |
|---|---|---|---|
| **Atomic write** (snapshot, retention, offsets) | `fs::File::create` or `write_all` fails before rename. Original file untouched. | None | Automatic — retry after freeing space |
| **Object store put** | Temp file created, write fails, rename never happens. Original objects untouched. | None | Automatic |
| **Index append** | `BufWriter::write_all` or `flush` may fail. No partial entries written to disk (fixed-width, all-or-nothing at OS level). | Current record lost | Re-send the record |
| **Orphaned .tmp files** | If crash occurs between temp file creation and rename, `.tmp` files accumulate. | None | Manual cleanup (harmless but wastes space) |

## 2. Permission Errors

All I/O operations use `.context()` for error propagation. Permission errors surface as `anyhow::Error` with a descriptive message. No silent corruption — the operation fails cleanly and the caller sees the error.

## 3. Corrupt Metadata Files

All metadata files are protected by a 4-byte CRC32 checksum prepended to the data. On CRC mismatch, each file recovers to a safe default rather than failing to open.

| File | On CRC Mismatch | Recovery | Data Loss | Tested |
|---|---|---|---|---|
| **tree.snapshot** | Partition opens with empty tree. Records still readable via index. Merkle proofs for old records unavailable until tree is rebuilt by new appends. | Automatic | Proof history | Yes (unit + nemesis) |
| **offsets.idx** (truncated) | Next offset recalculated from `file_size / 32`. Partial entries (< 32 bytes) at the end are silently ignored. Records up to the last complete entry are readable. | Automatic | Last partial record | Yes (nemesis) |
| **retention.bin** | Defaults to `min_valid_offset = 0`. All records become readable (safe — serves older records rather than hiding valid ones). | Automatic | None | Yes (unit) |
| **meta.bin** (corrupt) | `Topic::reopen` fails. Broker initialization fails for this topic. | Manual | None (objects intact) | No |
| **offsets.bin** (consumer group) | Defaults to empty offsets. Consumer re-consumes from beginning (safe for at-least-once semantics). | Automatic | Committed offsets | Yes (unit) |
| **objects.pack.idx** | Falls back to `scan_pack` — full sequential scan of the pack file to rebuild the in-memory index. | Automatic | None | Yes (unit) |
| **config.bin** | Written on every `Broker::open`, never read back on reopen (config is passed in). | N/A | None | N/A |

## 4. Corrupt Object Store Files

Objects are content-addressed (SHA-256). If a stored file is corrupted:

1. **Corrupt compression marker**: `Compression::decompress` returns `Err("unknown compression marker")`.
2. **Valid marker, corrupt LZ4 payload**: `lz4_flex::decompress_size_prepended` returns an error.
3. **Bit-flip in decompressed data**: The decompressed bytes won't match the expected SHA-256 hash. Merkle proof verification will fail for any record referencing this object.

Objects are never modified after creation (content-addressed, immutable). Corruption can only come from disk errors or external interference.

## 5. Process Crash (SIGKILL)

| Component | State After Crash | Recovery |
|---|---|---|
| **Object store** | Complete — atomic write ensures objects are either fully written or absent. | Automatic |
| **tree.snapshot** | May be stale (behind index). Tree has fewer entries than index. | Tested (nemesis: "index ahead of snapshot"). Records readable, proofs valid for entries in snapshot. |
| **offsets.idx** | Structurally sound. BufWriter may have unflushed entries — those records' index entries are lost but the file is not corrupted (fixed-width entries, no framing). | Automatic — next_offset recalculated from file size |
| **retention.bin** | Atomic write — either old or new value, never corrupt. | Automatic |
| **offsets.bin** (consumer group) | Atomic write — either old or new committed offsets. | Automatic — consumer resumes from last committed position |

**Tested**: 1,000 records recovered after 10 ungraceful drop cycles (nemesis suite).

## 6. Lock Poisoning

If a thread panics while holding a lock:

- **Partition `RwLock`** (in `topic.rs`): Subsequent `read()`/`write()` calls will panic via `.unwrap()` on the poisoned lock.
- **Topic map `RwLock`** (in `broker.rs`): `ensure_topic`, `topic`, `create_topic` will panic.
- **Groups `Mutex`** (in `broker.rs`): `commit_offsets`, `group` will panic.
- **ConsumerGroup `Mutex`** (in `broker.rs`): Per-group operations will panic.

**Mitigation**: All I/O errors propagate via `Result` rather than panicking, so lock poisoning should only occur from logic bugs, not I/O failures. There are no `panic!()` calls in critical sections.

**Status**: Not tested. Risk is low given the error propagation design.

## 7. NFS / Network Filesystems

merkql is NFS-safe (NFS v4.1 / EFS compatible) with the following hardening:

- **CRC32 checksums**: All metadata files (tree.snapshot, retention.bin, offsets.bin, objects.pack.idx) are wrapped with a 4-byte CRC32 checksum. On mismatch, each file recovers to a safe default (see section 3).
- **fsync parent directory**: After every `rename` in atomic writes, the parent directory is fsynced to ensure directory entries are durable on NFS.
- **File-handle-based lengths**: File sizes are determined by seeking to the end of an open file handle, not by `fs::metadata()` on the path. This avoids NFS attribute caching (stale `st_size`).

**Limitations**: `rename()` is not strictly atomic on NFS. The CRC32 checksums protect against partial/corrupt renames — if the data doesn't pass CRC validation, the safe default is used.

## 8. Concurrent Access from Multiple Processes

Supported via `flock`-based writer exclusion:

- **Partition writes**: `Partition::append` and `Partition::append_batch` acquire an exclusive `flock` on `partition.lock` in the partition directory before writing. The lock is released when the write completes.
- **Consumer group commits**: `ConsumerGroup::persist` acquires an exclusive `flock` on `group.lock` before writing offsets.
- **Readers**: Do NOT acquire locks. Append-only data is safe for concurrent reads. The index file (`offsets.idx`) is only appended to, so readers see a consistent prefix.

**Model**: Single writer (serialized by flock), concurrent readers. A single writer can saturate EFS throughput (~200-1000 writes/sec at 1-5ms per fsync). The flock provides correctness, not performance — multi-writer adds complexity for zero throughput gain.

**Note**: In the typical deployment (single Lambda / single process), the flock is a safety net rather than a contention point. The `RwLock` on partitions in `Topic` already serializes in-process writes.

## 9. Orphaned .tmp Files

Created during atomic writes if the process dies between file creation and rename. These files are:

- Never read by merkql (only renamed-to paths are read)
- Harmless to operation
- Not cleaned up automatically
- Could accumulate over many crashes

To clean up manually: `find .merkql -name '*.tmp' -delete`

## 10. Panic Catalog

Every `.unwrap()` and `.expect()` in non-test code:

### Safe (structurally guaranteed)

| Location | Call | Why Safe |
|---|---|---|
| `store.rs:42` | `path.parent().unwrap()` | Object paths always have a parent (prefix/suffix structure) |
| `partition.rs:16` | `path.with_extension("tmp")` in `atomic_write` | Paths always have a parent |
| `group.rs:17` | `path.with_extension("tmp")` in `atomic_write` | Paths always have a parent |
| `node.rs:21,26` | `bincode::serialize().expect()` | Serializing in-memory structs with known types |
| `record.rs:18,23` | `bincode::serialize().expect()` | Serializing in-memory structs with known types |

### Lock unwraps (panic on poisoning)

| Location | Lock Type | Impact |
|---|---|---|
| `broker.rs:112` | `topics.read().unwrap()` | Topic lookup fails |
| `broker.rs:118` | `groups.lock().unwrap()` | Group lookup fails |
| `broker.rs:126` | `topics.read().unwrap()` | ensure_topic fast path fails |
| `broker.rs:140` | `topics.write().unwrap()` | Topic creation fails |
| `broker.rs:160` | `topics.write().unwrap()` | create_topic fails |
| `broker.rs:184` | `groups.lock().unwrap()` | commit_offsets fails |
| `broker.rs:191` | `groups.get().unwrap().clone()` | Safe — just checked `contains_key` |
| `broker.rs:194` | `group_arc.lock().unwrap()` | Per-group commit fails |
| `consumer.rs:65` | `g.lock().unwrap()` | Consumer subscribe fails |
| `consumer.rs:75` | `part_arc.read().unwrap()` | Latest offset lookup fails |

All lock-poisoning panics are cascading failures that indicate a prior logic bug. They do not cause data corruption (the data on disk remains consistent).

## 11. Storage Overhead

Objects are stored in a single append-only pack file (`objects.pack`) with a persisted index (`objects.pack.idx`). This eliminates the small-file problem — all objects share a single file, with no per-object filesystem overhead.

**Per-partition files**: `offsets.idx`, `objects.pack`, `objects.pack.idx`, `tree.snapshot`, `retention.bin`, `partition.lock` — 6 files regardless of record count.
