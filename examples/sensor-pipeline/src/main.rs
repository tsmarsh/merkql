use merkql::broker::{Broker, BrokerConfig};
use merkql::compression::Compression;
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use merkql::topic::RetentionConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct SensorReading {
    temp: f64,
    ts: u64,
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().to_path_buf();

    println!("=== MerkQL Sensor Pipeline Example ===\n");
    println!("Data directory: {}\n", data_path.display());

    // Step 1: Open broker with LZ4 compression + retention of 500 records
    println!("--- Step 1: Open broker with LZ4 compression, retention=500 records ---");
    let config = BrokerConfig {
        compression: Compression::Lz4,
        default_retention: RetentionConfig {
            max_records: Some(500),
        },
        ..BrokerConfig::new(&data_path)
    };
    let broker = Broker::open(config).unwrap();
    println!("Broker opened.\n");

    // Step 2: Produce 200 sensor readings one-at-a-time
    println!("--- Step 2: Produce 200 readings one-at-a-time ---");
    let producer = Broker::producer(&broker);
    let sensors = ["sensor-a", "sensor-b", "sensor-c", "sensor-d"];

    for i in 0..200 {
        let sensor_id = sensors[i % sensors.len()];
        let reading = SensorReading {
            temp: 20.0 + (i as f64 * 0.1) % 20.0,
            ts: 1700000000 + i as u64,
        };
        let value = serde_json::to_string(&reading).unwrap();
        producer
            .send(&ProducerRecord::new(
                "sensors",
                Some(sensor_id.to_string()),
                value,
            ))
            .unwrap();
    }
    println!("Produced 200 individual readings.\n");

    // Step 3: Produce 800 readings via send_batch() in batches of 100
    println!("--- Step 3: Produce 800 readings via send_batch (8 batches of 100) ---");
    for batch_num in 0..8 {
        let batch: Vec<ProducerRecord> = (0..100)
            .map(|j| {
                let idx = 200 + batch_num * 100 + j;
                let sensor_id = sensors[idx % sensors.len()];
                let reading = SensorReading {
                    temp: 20.0 + (idx as f64 * 0.1) % 20.0,
                    ts: 1700000000 + idx as u64,
                };
                ProducerRecord::new(
                    "sensors",
                    Some(sensor_id.to_string()),
                    serde_json::to_string(&reading).unwrap(),
                )
            })
            .collect();
        producer.send_batch(&batch).unwrap();
    }
    println!("Produced 800 readings in 8 batches.\n");

    // Step 4: Consumer group "analytics" polls all 1000, prints summary stats
    println!("--- Step 4: Consumer group 'analytics' — summary stats ---");
    {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "analytics".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["sensors"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        println!("  Polled {} records.", records.len());

        let mut stats: HashMap<String, (f64, f64, f64, usize)> = HashMap::new();
        for record in &records {
            let key = record.key.clone().unwrap_or("unknown".into());
            let reading: SensorReading = serde_json::from_str(&record.value).unwrap();
            let entry = stats.entry(key).or_insert((f64::MAX, f64::MIN, 0.0, 0));
            entry.0 = entry.0.min(reading.temp);
            entry.1 = entry.1.max(reading.temp);
            entry.2 += reading.temp;
            entry.3 += 1;
        }

        let mut sensor_names: Vec<_> = stats.keys().cloned().collect();
        sensor_names.sort();
        for sensor in &sensor_names {
            let (min, max, sum, count) = stats[sensor];
            println!(
                "  {}: min={:.1}, max={:.1}, avg={:.1} ({} readings)",
                sensor,
                min,
                max,
                sum / count as f64,
                count
            );
        }

        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
        println!();
    }

    // Step 5: Consumer group "alerts" filters readings > 35.0
    println!("--- Step 5: Consumer group 'alerts' — high-temp alerts ---");
    {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "alerts".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["sensors"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();

        let mut alert_count = 0;
        for record in &records {
            let reading: SensorReading = serde_json::from_str(&record.value).unwrap();
            if reading.temp > 35.0 {
                alert_count += 1;
                if alert_count <= 5 {
                    println!(
                        "  ALERT: {} temp={:.1} at ts={}",
                        record.key.as_deref().unwrap_or("unknown"),
                        reading.temp,
                        reading.ts
                    );
                }
            }
        }
        if alert_count > 5 {
            println!("  ... and {} more alerts", alert_count - 5);
        }
        println!("  Total alerts: {} out of {} readings\n", alert_count, records.len());

        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }

    // Step 6: Drop broker, reopen from same directory
    println!("--- Step 6: Reopen broker from disk ---");
    drop(broker);

    let config = BrokerConfig {
        compression: Compression::Lz4,
        default_retention: RetentionConfig {
            max_records: Some(500),
        },
        ..BrokerConfig::new(&data_path)
    };
    let broker = Broker::open(config).unwrap();
    println!("Broker reopened.\n");

    // Step 7: Consumer group "analytics" resumes — should get 0 new records
    println!("--- Step 7: 'analytics' resumes — should get 0 new records ---");
    {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "analytics".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["sensors"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        println!("  Polled {} records (expected 0).\n", records.len());
        consumer.close().unwrap();
    }

    // Step 8: Produce 100 more, "analytics" polls — gets exactly 100
    println!("--- Step 8: Produce 100 more, 'analytics' gets exactly 100 ---");
    let producer = Broker::producer(&broker);
    for i in 0..100 {
        let sensor_id = sensors[i % sensors.len()];
        let reading = SensorReading {
            temp: 25.0 + (i as f64 * 0.2) % 10.0,
            ts: 1700001100 + i as u64,
        };
        producer
            .send(&ProducerRecord::new(
                "sensors",
                Some(sensor_id.to_string()),
                serde_json::to_string(&reading).unwrap(),
            ))
            .unwrap();
    }
    println!("  Produced 100 more readings.");

    {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "analytics".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["sensors"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        println!("  Polled {} records (expected 100).", records.len());
        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }

    // Step 9: Print disk usage
    println!("\n--- Step 9: Disk usage ---");
    let size = dir_size(&data_path);
    println!("  Data directory: {:.1} KB", size as f64 / 1024.0);
    println!("\n=== Sensor Pipeline Example Complete ===");
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(meta) = p.metadata() {
                total += meta.len();
            }
        }
    }
    total
}
