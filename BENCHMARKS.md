# Benchmarks

All benchmarks run with [Criterion.rs](https://github.com/bheisler/criterion.rs) on a single machine. Numbers will vary on different hardware.

**Hardware**: AMD Ryzen / Linux 6.18, NVMe SSD, ext4, Rust 2024 edition, `--release` profile.

**Methodology**: Criterion default settings unless noted. Warm-up 1-3s, measurement 3-5s, 50-200 samples per benchmark. All operations are single-threaded, single-partition, uncompressed unless stated otherwise.

## Append Throughput

Single-record `producer.send()` — includes serialization, SHA-256 hashing, object store write, merkle tree update, index append, fsync, and snapshot write.

| Payload | Latency (median) | Throughput |
|---|---|---|
| 64 B | 45 us | 1.4 MB/s |
| 256 B | 31 us | 7.9 MB/s |
| 1 KB | 57 us | 17 MB/s |
| 4 KB | 40 us | 96 MB/s |
| 16 KB | 81 us | 192 MB/s |
| 64 KB | 103 us | 608 MB/s |
| 256 KB | 253 us | 987 MB/s |

Throughput scales with payload size because the fixed overhead (fsync, hashing, tree update) is amortized over larger payloads. Batch writes (`send_batch`) amortize fsync further — 1 fsync per batch instead of 1 per record.

## Append Latency (256B payload)

| Percentile | Latency |
|---|---|
| Median | 46 us |

Measured with 200 samples. The dominant cost is `fsync` on every write. Batch writes eliminate per-record fsync.

## Read Throughput

Sequential scan of 10,000 pre-populated records via `partition.read(offset)`.

| Operation | Throughput |
|---|---|
| Sequential 10K scan | ~179K records/sec |

Each read is an index seek (O(1) file seek) + object store lookup + decompression.

## Read Latency

Random-access reads using a prime stride pattern across 10K records.

| Operation | Latency |
|---|---|
| Random-access read | 4.8 us |

Sub-5-microsecond reads. The index is a flat file with 32-byte fixed-width entries — offset lookup is a single seek.

## Proof Operations

Generate + verify a merkle inclusion proof at a random offset.

| Log Size | Latency |
|---|---|
| 100 records | 76 us |
| 1,000 records | 180 us |
| 10,000 records | 150 us |

Proof generation walks the tree from root to leaf (O(log n) node reads). Verification reconstructs branch hashes along the path. The 10K result being faster than 1K is due to OS page cache effects — the 10K dataset's tree nodes are still warm from population.

## Broker Reopen

Time to reopen a broker with existing data (reads metadata, opens partitions, restores tree snapshots).

| Data Size | Reopen Time |
|---|---|
| 100 records | 22 us |
| 1,000 records | 29 us |
| 10,000 records | 14 us |
| 50,000 records | 14 us |

Reopen is fast and nearly constant-time because merkql persists an incremental tree snapshot — it doesn't replay the log. The snapshot is a compact bincode blob that restores the tree's pending-roots stack in a single read.

## Key Takeaways

1. **Append is fsync-dominated**: The ~30-50us floor for small payloads is fsync cost. Use `send_batch()` to amortize this across many records.
2. **Reads are cheap**: Sub-5us random access via the fixed-width index.
3. **Proofs are fast**: <200us even at 10K records. O(log n) tree traversal.
4. **Reopen is instant**: <30us regardless of data size, thanks to incremental snapshots.
5. **Throughput scales with payload**: Large payloads amortize fixed overhead, reaching ~1 GB/s at 256KB.
