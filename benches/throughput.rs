use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::compression::Compression;
use merkql::record::ProducerRecord;
use merkql::topic::RetentionConfig;
use merkql::tree::MerkleTree;
use std::path::Path;

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

fn generate_payload(size: usize) -> String {
    "x".repeat(size)
}

// ---------------------------------------------------------------------------
// Group 1: Append throughput by payload size
// ---------------------------------------------------------------------------

fn append_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_throughput");
    group.sample_size(50);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(3));

    for size in [64, 256, 1024, 4096, 16384, 65536, 262144] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let dir = tempfile::tempdir().unwrap();
            let broker = setup_broker(dir.path(), 1);
            let producer = Broker::producer(&broker);
            let payload = generate_payload(size);

            b.iter(|| {
                let pr = ProducerRecord::new("bench", None, payload.clone());
                producer.send(&pr).unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 2: Append latency (256B, many samples)
// ---------------------------------------------------------------------------

fn append_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_latency");
    group.sample_size(200);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(3));

    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);
    let producer = Broker::producer(&broker);
    let payload = generate_payload(256);

    group.bench_function("256B", |b| {
        b.iter(|| {
            let pr = ProducerRecord::new("latency", None, payload.clone());
            producer.send(&pr).unwrap();
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 3: Read throughput (sequential scan)
// ---------------------------------------------------------------------------

fn read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_throughput");

    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);

    // Pre-populate 10K records
    let producer = Broker::producer(&broker);
    for i in 0..10_000 {
        let pr = ProducerRecord::new("read-bench", None, format!("value-{}", i));
        producer.send(&pr).unwrap();
    }

    group.throughput(Throughput::Elements(10_000));
    group.bench_function("10K_sequential", |b| {
        b.iter(|| {
            let topic = broker.topic("read-bench").unwrap();
            let part_arc = topic.partition(0).unwrap();
            let partition = part_arc.read().unwrap();
            for offset in 0..10_000u64 {
                let _ = partition.read(offset).unwrap();
            }
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 4: Read latency (random-access with prime stride)
// ---------------------------------------------------------------------------

fn read_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_latency");
    group.sample_size(1000);

    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(dir.path(), 1);

    // Pre-populate 10K records
    let producer = Broker::producer(&broker);
    for i in 0..10_000 {
        let pr = ProducerRecord::new("read-lat", None, format!("value-{}", i));
        producer.send(&pr).unwrap();
    }

    let mut offset = 0u64;
    group.bench_function("prime_stride_10K", |b| {
        b.iter(|| {
            let topic = broker.topic("read-lat").unwrap();
            let part_arc = topic.partition(0).unwrap();
            let partition = part_arc.read().unwrap();
            let _ = partition.read(offset % 10_000).unwrap();
            offset = (offset + 7919) % 10_000; // prime stride
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 5: Proof operations
// ---------------------------------------------------------------------------

fn proof_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("proof_operations");

    for log_size in [100, 1_000, 10_000] {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);

        let producer = Broker::producer(&broker);
        for i in 0..log_size {
            let pr = ProducerRecord::new("proof-bench", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }

        let mut offset = 0u64;
        group.bench_with_input(
            BenchmarkId::new("generate_verify", log_size),
            &log_size,
            |b, _| {
                b.iter(|| {
                    let topic = broker.topic("proof-bench").unwrap();
                    let part_arc = topic.partition(0).unwrap();
                    let partition = part_arc.read().unwrap();
                    let o = offset % partition.next_offset();
                    let proof = partition.proof(o).unwrap().unwrap();
                    let valid = MerkleTree::verify_proof(&proof, partition.store()).unwrap();
                    assert!(valid);
                    offset = (offset + 7) % partition.next_offset();
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Group 6: Broker reopen time vs data size
// ---------------------------------------------------------------------------

fn broker_reopen(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_reopen");

    for data_size in [100, 1_000, 10_000, 50_000] {
        let dir = tempfile::tempdir().unwrap();
        let broker = setup_broker(dir.path(), 1);

        let producer = Broker::producer(&broker);
        for i in 0..data_size {
            let pr = ProducerRecord::new("reopen-bench", None, format!("v{}", i));
            producer.send(&pr).unwrap();
        }
        drop(broker);

        group.bench_with_input(
            BenchmarkId::from_parameter(data_size),
            &data_size,
            |b, _| {
                b.iter(|| {
                    let _ = setup_broker(dir.path(), 1);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    append_throughput,
    append_latency,
    read_throughput,
    read_latency,
    proof_operations,
    broker_reopen
);
criterion_main!(benches);
