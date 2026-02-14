# merkql

**Kafka semantics. Merkle integrity. Zero infrastructure.**

merkql is an embedded event log for Rust. It gives you topics, partitions, consumer groups, and offset management — the parts of Kafka you actually use — backed by a content-addressed merkle tree that makes every record cryptographically verifiable.

No JVM. No ZooKeeper. No network. Just a directory on disk.

```rust
let broker = Broker::open(BrokerConfig::new("/tmp/events")).unwrap();

let producer = Broker::producer(&broker);
producer.send(&ProducerRecord::new("orders", Some("order-42".into()),
    r#"{"item":"widget","qty":5}"#)).unwrap();

let mut consumer = Broker::consumer(&broker, ConsumerConfig {
    group_id: "billing".into(),
    auto_commit: false,
    offset_reset: OffsetReset::Earliest,
});
consumer.subscribe(&["orders"]).unwrap();
let records = consumer.poll(Duration::from_millis(100)).unwrap();
consumer.commit_sync().unwrap();
```

## Why merkql

**You need event streaming but not a cluster.** Kafka is the right abstraction — append-only logs, consumer groups, offset tracking — but running a Kafka cluster for a single-node service, an embedded device, or your integration tests is a tax on your time and infrastructure.

**You need to prove your data hasn't been tampered with.** Every partition is a merkle tree. Every record gets a cryptographic inclusion proof. You can verify that a specific record existed at a specific offset without trusting anything except the math. This matters for audit logs, compliance records, financial transactions, and any system where someone might later ask "how do we know this data is authentic?"

**You need it to not lose data.** Every metadata write uses atomic temp+fsync+rename. The index is fsynced on every write. Jepsen-style fault injection tests verify recovery from crashes, truncated files, and missing snapshots. The test suite doesn't just observe behavior — it asserts correctness.

## Use Cases

### Tamper-evident audit logs

Regulatory environments (SOX, HIPAA, PCI-DSS, GDPR) require demonstrating that audit records haven't been modified after the fact. merkql gives you a cryptographic proof for every record in the log — you can hand an auditor an inclusion proof and a root hash and they can independently verify the record's authenticity without access to your systems.

```rust
// Write an audit event
producer.send(&ProducerRecord::new("audit",
    Some("user-1001".into()),
    r#"{"action":"access_patient_record","record_id":"R-2847"}"#
)).unwrap();

// Later: generate a proof for any record
let topic = broker.topic("audit").unwrap();
let partition = topic.partition(0).unwrap().read().unwrap();
let proof = partition.proof(offset).unwrap().unwrap();

// Anyone can verify this proof independently
assert!(MerkleTree::verify_proof(&proof, partition.store()).unwrap());
```

### Event sourcing without infrastructure

Event-sourced applications need an append-only log with consumer groups and offset tracking. merkql provides this as a library — no Docker containers, no cluster management, no network configuration. Your event store starts when your process starts and stops when it stops.

```rust
let config = BrokerConfig {
    default_retention: RetentionConfig { max_records: Some(100_000) },
    compression: Compression::Lz4,
    ..BrokerConfig::new("./data/events")
};
let broker = Broker::open(config).unwrap();

// Domain events go to topics
producer.send(&ProducerRecord::new("orders.created", Some(order_id.into()), payload)).unwrap();
producer.send(&ProducerRecord::new("orders.shipped", Some(order_id.into()), payload)).unwrap();

// Each projection gets its own consumer group with independent progress
let mut projections = Broker::consumer(&broker, ConsumerConfig {
    group_id: "order-summary-projection".into(),
    auto_commit: false,
    offset_reset: OffsetReset::Earliest,
});
```

### Integration testing

Replace a real Kafka cluster in your test suite. merkql uses the same produce/subscribe/poll/commit lifecycle, so your test code exercises the same consumer group logic as production — without Docker, without port conflicts, without test isolation problems.

```rust
#[test]
fn order_processing_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();

    // Simulate upstream events
    let producer = Broker::producer(&broker);
    for order in test_orders() {
        producer.send(&ProducerRecord::new("incoming-orders", None, order)).unwrap();
    }

    // Run your real consumer logic against it
    let mut consumer = Broker::consumer(&broker, ConsumerConfig {
        group_id: "order-processor".into(),
        auto_commit: false,
        offset_reset: OffsetReset::Earliest,
    });
    consumer.subscribe(&["incoming-orders"]).unwrap();
    let records = consumer.poll(Duration::from_millis(100)).unwrap();

    assert_eq!(records.len(), test_orders().len());
}
```

### Edge and embedded systems

Devices that need local event buffering with integrity guarantees — IoT gateways, POS terminals, medical devices, vehicle telemetry. merkql runs in-process with no network dependencies. LZ4 compression keeps storage manageable. Retention policies prevent unbounded growth. When connectivity is restored, consumers can drain the log and forward events upstream.

### Local development

Run your Kafka-based microservices locally without Docker Compose. Point your service at a merkql broker during development and get the same topic/partition/consumer-group semantics with zero startup time.

## Features

| | |
|---|---|
| **Kafka-compatible API** | Topics, partitions, consumer groups, offset management, subscribe/poll/commit/close |
| **Merkle tree integrity** | SHA-256 content-addressed storage, inclusion proofs, tamper detection |
| **Crash-safe** | Atomic writes (temp+fsync+rename) for all metadata, index fsync on every write |
| **Concurrent** | `RwLock` per partition, `RwLock` on topic map, `Mutex` per consumer group — readers never block readers |
| **LZ4 compression** | Transparent, per-broker. Objects hashed before compression so proofs work regardless of mode. Mixed-mode reads supported |
| **Retention** | Configurable `max_records` per topic. Old records become unreachable |
| **Batch API** | `send_batch()` amortizes fsync — 1 per batch instead of 1 per record |
| **Persistent** | All state survives process restarts. Topics, partitions, offsets, and tree snapshots are durable |
| **Zero runtime dependencies** | No external services, no JVM, no network, no ZooKeeper |

## Performance

| Operation | Result |
|---|---|
| **Append (256B)** | 31 us / 7.9 MB/s |
| **Append (64KB)** | 103 us / 608 MB/s |
| **Sequential read (10K)** | 179K records/sec |
| **Random-access read** | 4.8 us |
| **Proof generate+verify (10K)** | 150 us |
| **Broker reopen (50K records)** | 14 us |

Batch writes amortize fsync — `send_batch()` uses 1 fsync per batch instead of 1 per record. See [BENCHMARKS.md](BENCHMARKS.md) for full results.

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
merkql = "0.1"
```

### Produce and consume

```rust
use merkql::broker::{Broker, BrokerConfig};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use std::time::Duration;

let broker = Broker::open(BrokerConfig::new("/tmp/my-log")).unwrap();

// Produce
let producer = Broker::producer(&broker);
producer.send(&ProducerRecord::new("events", Some("user-1".into()),
    r#"{"action":"login"}"#)).unwrap();

// Consume
let mut consumer = Broker::consumer(&broker, ConsumerConfig {
    group_id: "my-service".into(),
    auto_commit: false,
    offset_reset: OffsetReset::Earliest,
});
consumer.subscribe(&["events"]).unwrap();
let records = consumer.poll(Duration::from_millis(100)).unwrap();

for record in &records {
    println!("{}: {}", record.topic, record.value);
}

consumer.commit_sync().unwrap();
consumer.close().unwrap();
```

### Verify integrity

```rust
let topic = broker.topic("events").unwrap();
let partition = topic.partition(0).unwrap().read().unwrap();

// Generate and verify a proof for any offset
let proof = partition.proof(0).unwrap().unwrap();

use merkql::tree::MerkleTree;
assert!(MerkleTree::verify_proof(&proof, partition.store()).unwrap());
```

### Enable compression

```rust
use merkql::compression::Compression;

let config = BrokerConfig {
    compression: Compression::Lz4,
    ..BrokerConfig::new("/tmp/my-log")
};
let broker = Broker::open(config).unwrap();
// Reads auto-detect compression via per-object markers — mixed modes just work.
```

### Configure retention

```rust
use merkql::topic::RetentionConfig;

let config = BrokerConfig {
    default_retention: RetentionConfig { max_records: Some(10_000) },
    ..BrokerConfig::new("/tmp/my-log")
};
let broker = Broker::open(config).unwrap();
// Each partition auto-trims to the most recent 10,000 records.
```

### Batch writes

```rust
let records: Vec<ProducerRecord> = events.iter()
    .map(|e| ProducerRecord::new("events", None, e.to_string()))
    .collect();
let results = producer.send_batch(&records).unwrap();
// 1 fsync for the entire batch instead of 1 per record.
```

## Kafka API Mapping

If you know Kafka, you already know merkql:

| merkql | Kafka |
|---|---|
| `Broker::open(config)` | `bootstrap.servers` |
| `Broker::producer(broker)` | `new KafkaProducer<>(props)` |
| `producer.send(record)` | `producer.send(record)` |
| `producer.send_batch(records)` | *(no equivalent — merkql-specific)* |
| `Broker::consumer(broker, config)` | `new KafkaConsumer<>(props)` |
| `consumer.subscribe(&["topic"])` | `consumer.subscribe(List.of(...))` |
| `consumer.poll(timeout)` | `consumer.poll(Duration)` |
| `consumer.commit_sync()` | `consumer.commitSync()` |
| `consumer.close()` | `consumer.close()` |

## Architecture

```
.merkql/
  topics/
    {topic-name}/
      meta.bin                    # Topic config (partition count)
      partitions/
        {id}/
          objects/ab/cdef...      # Content-addressed merkle nodes + records
          offsets.idx             # Fixed-width index: offset -> record hash
          tree.snapshot           # Incremental tree state (atomic write)
          retention.bin           # Retention marker (atomic write)
  groups/
    {group-id}/
      offsets.bin                 # Committed offsets per topic-partition (atomic write)
  config.bin                      # Broker config
```

Every object is stored in a git-style content-addressed store keyed by its SHA-256 hash. Identical data is never stored twice. The merkle tree is an incremental binary carry chain — appends are O(log n) and the tree state is captured in a compact snapshot that survives restarts.

## Correctness

merkql ships with a Jepsen-style test suite: data-backed claims about correctness at scale, fault injection with real assertions, and property-based testing with random operation sequences. See [FAILURE-MODES.md](FAILURE-MODES.md) for a complete catalog of failure scenarios, recovery behavior, and what is and isn't tested.

### Properties verified

| Property | Claim | Evidence |
|---|---|---|
| **Total order** | Partition offsets are monotonically increasing and gap-free | 10,000 records across 4 partitions |
| **Durability** | All records survive broker close/reopen cycles | 5,000 records across 3 reopen cycles |
| **Exactly-once** | Consumer groups deliver every record exactly once across commit/restart | 1,000 records across 4 phases |
| **Merkle integrity** | 100% of records have valid inclusion proofs | 10,000 proofs across 4 partitions |
| **No data loss** | Every confirmed append is immediately readable | 5,000 records verified immediately after write |
| **Byte fidelity** | Values preserved exactly for edge-case payloads | 500 payloads (unicode, CJK, RTL, combining chars, control chars, random ASCII up to 64KB) |

### Fault injection

Every fault test asserts correctness — not just "it didn't crash":

| Fault | Assertion |
|---|---|
| **Crash (drop without close)** | All 1,000 records recovered after 10 ungraceful drop cycles |
| **Truncated tree.snapshot** | Broker refuses to reopen (safe failure) or reopens gracefully |
| **Truncated offsets.idx** | Broker reopens with 99 records (loses partial entry), zero read errors |
| **Missing tree.snapshot** | Broker reopens, all 100 records readable, new appends succeed |
| **Index ahead of snapshot** | 101 records readable, at least 100 valid proofs |

### Concurrency and feature tests

| Feature | Verified |
|---|---|
| **Concurrent producers** | N threads writing to same topic — no gaps, no duplicates |
| **Concurrent consumers** | Independent groups consuming simultaneously |
| **Batch API** | send_batch with sizes from 1 to 100, empty batch |
| **LZ4 compression** | Round-trip fidelity, persistence across restart, mixed-mode reads, merkle proof validity |
| **Retention** | max_records window enforcement, consumer view, persistence across restart |

### Property-based testing

50-100 proptest cases per family:

- **Random operation sequences** — Append/Read/Commit/CloseReopen across 3 topics
- **Payload fidelity** — Random printable ASCII 1B-1MB
- **Binary payloads** — Arbitrary byte sequences 1B-64KB, full byte spectrum
- **Multi-topic/partition** — 1-5 topics x 1-8 partitions x 10-200 records
- **Multi-phase exactly-once** — 50-500 records, 2-6 phases, optional broker reopen

### Running the tests

```bash
cargo test                                     # All 91 unit/integration tests
cargo test --test jepsen_checkers              # Correctness checkers
cargo test --test jepsen_nemesis               # Fault injection
cargo test --test jepsen_proptest              # Property-based tests
cargo test --test v2_features_test             # Concurrency, compression, retention
cargo test --test jepsen_report -- --nocapture # JSON report
cargo test --doc                               # Doc-tests (10 examples)
cargo test --release --test scale_test -- --ignored  # 100K-record scale tests
cargo bench                                    # Criterion benchmarks
scripts/run-fuzz.sh                            # Fuzz testing (cargo-fuzz)
scripts/run-miri.sh                            # Miri (pure-computation tests)
scripts/run-tsan.sh                            # Thread sanitizer (concurrent tests)
```

## Building

```bash
cargo build
cargo test
```

## License

MIT
