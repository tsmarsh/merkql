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

| File | On Corruption | Data Loss | Tested |
|---|---|---|---|
| **tree.snapshot** (truncated/invalid bincode) | `Partition::open` fails with deserialize error. Broker refuses to start. | None (data is in objects + index) | Yes (nemesis) |
| **tree.snapshot** (missing) | Partition reconstructs empty tree. Existing records readable via index. New appends succeed. Merkle proofs for old records unavailable until tree is rebuilt. | Proof history | Yes (nemesis) |
| **offsets.idx** (truncated) | Next offset recalculated from `file_size / 32`. Partial entries (< 32 bytes) at the end are silently ignored. Records up to the last complete entry are readable. | Last partial record | Yes (nemesis) |
| **retention.bin** (corrupt) | `Partition::open` fails with deserialize error. | None | No |
| **meta.bin** (corrupt) | `Topic::reopen` fails. Broker initialization fails for this topic. | None (objects intact) | No |
| **offsets.bin** (corrupt) | `ConsumerGroup::open` fails with deserialize error. | Committed offsets | No |
| **config.bin** | Written on every `Broker::open`, never read back on reopen (config is passed in). | None | N/A |

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

The atomic write pattern (`temp file → fsync → rename`) assumes `rename` is atomic. **On NFS, rename is NOT atomic.** Additionally, NFS may cache file metadata, leading to stale reads of `offsets.idx` size.

**merkql should NOT be used on NFS or other network filesystems.**

## 8. Concurrent Access from Multiple Processes

**NOT SUPPORTED.** merkql has no file-level locking. Two processes writing to the same `.merkql` directory will corrupt data:

- Duplicate offsets in the index
- Inconsistent tree snapshots
- Lost consumer group commits

Use a single process. If you need multi-process access, put an API server in front of merkql.

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

## 11. Storage Overhead (Small-File Problem)

The content-addressed object store creates one file per object: one for the serialized record, one for the merkle leaf node, and approximately one for each branch node. At scale this means ~3 files per record appended.

Each file is typically 50-200 bytes of actual data, but filesystem block allocation rounds up to 4KB per file. This creates a **~13x space amplification**: 100K records produce ~3.4MB of data but consume ~44MB on disk.

| Records | Actual Data | Allocated (ext4, 4KB blocks) | Files |
|---|---|---|---|
| 10K | ~340 KB | ~4.4 MB | ~30K |
| 100K | ~3.4 MB | ~44 MB | ~300K |
| 1M | ~34 MB | ~440 MB | ~3M |

**Implications**:
- **tmpfs**: Especially problematic since every inode consumes real RAM. 1M records can exhaust a 20GB tmpfs.
- **Inode exhaustion**: Some filesystems have fixed inode limits. 3M files for 1M records can hit these.
- **Directory performance**: The 256-way hash prefix directories mitigate this, but very large partitions (>1M records) may see slower `readdir` and filesystem metadata operations.

**Future mitigation**: Pack objects into segment files (like git packfiles) to amortize filesystem overhead. This would reduce the file count by 1000x+ while preserving content-addressing.
