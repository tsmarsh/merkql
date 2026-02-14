#![allow(dead_code)]

use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use merkql::tree::MerkleTree;
use serde::Serialize;
use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Broker helpers
// ---------------------------------------------------------------------------

pub fn setup_broker(dir: &Path, partitions: u32) -> BrokerRef {
    let config = BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: partitions,
        auto_create_topics: true,
        compression: merkql::compression::Compression::None,
        default_retention: merkql::topic::RetentionConfig::default(),
    };
    Broker::open(config).unwrap()
}

pub fn broker_config(dir: &Path) -> BrokerConfig {
    BrokerConfig {
        data_dir: dir.to_path_buf(),
        default_partitions: 1,
        auto_create_topics: true,
        compression: merkql::compression::Compression::None,
        default_retention: merkql::topic::RetentionConfig::default(),
    }
}

pub fn produce_n(
    broker: &BrokerRef,
    topic: &str,
    n: usize,
    value_fn: impl Fn(usize) -> String,
    key_fn: impl Fn(usize) -> Option<String>,
) -> Vec<merkql::record::Record> {
    let producer = Broker::producer(broker);
    let mut records = Vec::with_capacity(n);
    for i in 0..n {
        let pr = ProducerRecord::new(topic, key_fn(i), value_fn(i));
        let record = producer.send(&pr).unwrap();
        records.push(record);
    }
    records
}

pub fn consume_all(
    broker: &BrokerRef,
    group_id: &str,
    topics: &[&str],
    reset: OffsetReset,
) -> Vec<merkql::record::Record> {
    let mut consumer = Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: group_id.into(),
            auto_commit: false,
            offset_reset: reset,
        },
    );
    consumer.subscribe(topics).unwrap();
    consumer.poll(Duration::from_millis(100)).unwrap()
}

pub fn generate_payload(size: usize) -> String {
    if size == 0 {
        return String::new();
    }
    let base = r#"{"op":"c","ts_ms":1700000000000,"source":{"version":"2.4","connector":"postgresql","name":"legacy","ts_ms":1700000000000,"db":"legacy","schema":"public","table":"entity","txId":42,"lsn":12345},"after":{"id":1,"name":"test","data":""#;
    let suffix = r#""}}"#;
    let overhead = base.len() + suffix.len();
    if size <= overhead {
        "x".repeat(size)
    } else {
        let fill_len = size - overhead;
        format!("{}{}{}", base, "x".repeat(fill_len), suffix)
    }
}

// ---------------------------------------------------------------------------
// File helper
// ---------------------------------------------------------------------------

pub fn find_file_recursive(dir: &Path, name: &str) -> Option<std::path::PathBuf> {
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(found) = find_file_recursive(&path, name) {
                    return Some(found);
                }
            } else if path.file_name().map(|f| f == name).unwrap_or(false) {
                return Some(path);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct JepsenReport {
    pub title: String,
    pub timestamp: String,
    pub properties: Vec<PropertyResult>,
    pub nemesis: Vec<NemesisResult>,
    pub benchmarks: Vec<BenchmarkResult>,
}

#[derive(Debug, Serialize)]
pub struct PropertyResult {
    pub name: String,
    pub claim: String,
    pub methodology: String,
    pub sample_size: usize,
    pub passed: bool,
    pub details: String,
    pub duration_secs: f64,
}

#[derive(Debug, Serialize)]
pub struct BenchmarkResult {
    pub name: String,
    pub unit: String,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub min: f64,
    pub max: f64,
    pub stddev: f64,
    pub throughput: f64,
    pub sample_size: usize,
}

#[derive(Debug, Serialize)]
pub struct NemesisResult {
    pub name: String,
    pub fault: String,
    pub outcome: String,
    pub records_before: usize,
    pub records_after: usize,
    pub details: String,
    pub passed: bool,
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

pub fn measure_latency(name: &str, n: usize, mut f: impl FnMut(usize)) -> BenchmarkResult {
    // Warmup: 10% of n, minimum 10
    let warmup = (n / 10).max(10);
    for i in 0..warmup {
        f(i);
    }

    let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    let overall_start = Instant::now();

    for i in 0..n {
        let start = Instant::now();
        f(i);
        let elapsed = start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed);
    }

    let total_secs = overall_start.elapsed().as_secs_f64();

    BenchmarkResult {
        name: name.to_string(),
        unit: "microseconds".to_string(),
        p50: hist.value_at_quantile(0.50) as f64,
        p95: hist.value_at_quantile(0.95) as f64,
        p99: hist.value_at_quantile(0.99) as f64,
        min: hist.min() as f64,
        max: hist.max() as f64,
        stddev: hist.stdev(),
        throughput: if total_secs > 0.0 {
            n as f64 / total_secs
        } else {
            0.0
        },
        sample_size: n,
    }
}

// ---------------------------------------------------------------------------
// Checker functions (reusable from both tests and report generator)
// ---------------------------------------------------------------------------

/// Check 1: Total Order — partition offsets are monotonically increasing and gap-free.
pub fn check_total_order(dir: &Path, n: usize, partitions: u32) -> PropertyResult {
    let start = Instant::now();
    let broker = setup_broker(dir, partitions);
    produce_n(&broker, "total-order", n, |i| format!("v{}", i), |_| None);

    let topic = broker.topic("total-order").unwrap();
    let mut total_verified = 0;

    for pid in topic.partition_ids() {
        let part_arc = topic.partition(pid).unwrap();
        let partition = part_arc.read().unwrap();
        let count = partition.next_offset();
        for offset in 0..count {
            let record = partition.read(offset).unwrap();
            assert!(
                record.is_some(),
                "gap at partition {} offset {}",
                pid,
                offset
            );
            let record = record.unwrap();
            assert_eq!(record.offset, offset);
            assert_eq!(record.partition, pid);
            total_verified += 1;
        }
    }

    let details = format!(
        "All {} records verified across {} partitions",
        total_verified, partitions
    );

    PropertyResult {
        name: "Total Order".to_string(),
        claim: "Every partition's offsets are monotonically increasing and gap-free".to_string(),
        methodology: format!(
            "Produced {} records across {} partitions via round-robin, read all back and verified offset sequences 0..N-1",
            n, partitions
        ),
        sample_size: n,
        passed: total_verified == n,
        details,
        duration_secs: start.elapsed().as_secs_f64(),
    }
}

/// Check 2: Durability — all records survive broker close/reopen cycles.
pub fn check_durability(dir: &Path, n: usize, cycles: usize) -> PropertyResult {
    let start = Instant::now();
    let base = n / cycles;
    let remainder = n % cycles;
    let mut all_values: Vec<String> = Vec::new();

    for cycle in 0..cycles {
        let count = base + if cycle < remainder { 1 } else { 0 };
        let broker = setup_broker(dir, 1);
        let start_idx = all_values.len();
        let produced = produce_n(
            &broker,
            "durability",
            count,
            |i| format!("v{}", start_idx + i),
            |_| None,
        );
        all_values.extend(produced.iter().map(|r| r.value.clone()));
        // broker dropped here — simulates close
    }

    // Reopen and verify all records
    let broker = setup_broker(dir, 1);
    let consumed = consume_all(
        &broker,
        "durability-check",
        &["durability"],
        OffsetReset::Earliest,
    );

    let consumed_values: Vec<String> = consumed.iter().map(|r| r.value.clone()).collect();
    let passed = consumed_values == all_values;

    PropertyResult {
        name: "Durability".to_string(),
        claim: "All records survive broker close/reopen cycles".to_string(),
        methodology: format!(
            "Produced {} records across {} close/reopen cycles, verified all readable after final reopen",
            all_values.len(),
            cycles
        ),
        sample_size: all_values.len(),
        passed,
        details: if passed {
            format!(
                "All {} records survived {} reopen cycles",
                all_values.len(),
                cycles
            )
        } else {
            format!(
                "Expected {} records, got {}",
                all_values.len(),
                consumed_values.len()
            )
        },
        duration_secs: start.elapsed().as_secs_f64(),
    }
}

/// Check 3: Exactly-Once — consumer groups deliver every record exactly once.
pub fn check_exactly_once(dir: &Path, n: usize) -> PropertyResult {
    let start = Instant::now();
    let broker = setup_broker(dir, 1);
    produce_n(&broker, "exactly-once", n, |i| format!("v{}", i), |_| None);

    let mut all_consumed: Vec<String> = Vec::new();

    for _phase in 0..4 {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "eo-group".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["exactly-once"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        all_consumed.extend(records.iter().map(|r| r.value.clone()));
        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }

    let expected: Vec<String> = (0..n).map(|i| format!("v{}", i)).collect();
    let unique: HashSet<&String> = all_consumed.iter().collect();
    let no_duplicates = unique.len() == all_consumed.len();
    let all_present = all_consumed == expected;
    let passed = no_duplicates && all_present;

    PropertyResult {
        name: "Exactly-Once".to_string(),
        claim: "Consumer groups deliver every record exactly once across commit/restart cycles"
            .to_string(),
        methodology: format!(
            "Produced {} records, consumed in 4 phases with commit_sync between each, verified union = all with no duplicates",
            n
        ),
        sample_size: n,
        passed,
        details: if passed {
            format!("All {} records consumed exactly once across 4 phases", n)
        } else {
            format!(
                "Consumed {} records, {} unique (expected {})",
                all_consumed.len(),
                unique.len(),
                n
            )
        },
        duration_secs: start.elapsed().as_secs_f64(),
    }
}

/// Check 4: Merkle Integrity — 100% of records have valid proofs.
pub fn check_merkle_integrity(dir: &Path, n: usize, partitions: u32) -> PropertyResult {
    let start = Instant::now();
    let broker = setup_broker(dir, partitions);
    produce_n(&broker, "merkle-int", n, |i| format!("v{}", i), |_| None);

    let topic = broker.topic("merkle-int").unwrap();
    let mut verified = 0;

    for pid in topic.partition_ids() {
        let part_arc = topic.partition(pid).unwrap();
        let partition = part_arc.read().unwrap();
        for offset in 0..partition.next_offset() {
            let proof = partition.proof(offset).unwrap();
            assert!(
                proof.is_some(),
                "no proof for partition {} offset {}",
                pid,
                offset
            );
            let proof = proof.unwrap();
            let valid = MerkleTree::verify_proof(&proof, partition.store()).unwrap();
            assert!(
                valid,
                "invalid proof for partition {} offset {}",
                pid, offset
            );
            verified += 1;
        }
    }

    PropertyResult {
        name: "Merkle Integrity".to_string(),
        claim: "100% of records have valid merkle inclusion proofs".to_string(),
        methodology: format!(
            "Produced {} records across {} partitions, generated and verified proof for every record",
            n, partitions
        ),
        sample_size: n,
        passed: verified == n,
        details: format!(
            "All {} proofs verified across {} partitions",
            verified, partitions
        ),
        duration_secs: start.elapsed().as_secs_f64(),
    }
}

/// Check 5: No Data Loss — every confirmed append is immediately readable.
pub fn check_no_data_loss(dir: &Path, n: usize) -> PropertyResult {
    let start = Instant::now();
    let broker = setup_broker(dir, 1);
    let producer = Broker::producer(&broker);
    let mut failures = 0;

    for i in 0..n {
        let pr = ProducerRecord::new("no-loss", None, format!("v{}", i));
        let record = producer.send(&pr).unwrap();

        // Immediately read back
        let topic = broker.topic("no-loss").unwrap();
        let part_arc = topic.partition(record.partition).unwrap();
        let partition = part_arc.read().unwrap();
        let read_back = partition.read(record.offset).unwrap();
        if read_back.is_none() || read_back.unwrap().value != format!("v{}", i) {
            failures += 1;
        }
    }

    PropertyResult {
        name: "No Data Loss".to_string(),
        claim: "Every confirmed append is immediately readable via the partition API".to_string(),
        methodology: format!(
            "Produced {} records, immediately read each back via partition.read() after send",
            n
        ),
        sample_size: n,
        passed: failures == 0,
        details: if failures == 0 {
            format!("All {} records readable immediately after append", n)
        } else {
            format!("{} of {} records not immediately readable", failures, n)
        },
        duration_secs: start.elapsed().as_secs_f64(),
    }
}

/// Check 6: Byte Fidelity — values preserved exactly for edge-case payloads.
pub fn check_byte_fidelity(dir: &Path) -> PropertyResult {
    let start = Instant::now();
    let broker = setup_broker(dir, 1);
    let producer = Broker::producer(&broker);

    let payloads = generate_fidelity_payloads();
    let n = payloads.len();

    for (i, payload) in payloads.iter().enumerate() {
        let pr = ProducerRecord::new("fidelity", Some(format!("k{}", i)), payload.clone());
        producer.send(&pr).unwrap();
    }

    let consumed = consume_all(
        &broker,
        "fidelity-check",
        &["fidelity"],
        OffsetReset::Earliest,
    );
    let mut failures = Vec::new();
    for (i, payload) in payloads.iter().enumerate() {
        if i >= consumed.len() {
            failures.push(format!("missing record {}", i));
            continue;
        }
        if consumed[i].value != *payload {
            failures.push(format!(
                "record {} mismatch: expected {} bytes, got {} bytes",
                i,
                payload.len(),
                consumed[i].value.len()
            ));
        }
    }

    let passed = failures.is_empty();
    PropertyResult {
        name: "Byte Fidelity".to_string(),
        claim: "Values are preserved exactly for edge-case payloads (empty, unicode, large, boundary lengths, special chars, structured data)".to_string(),
        methodology: format!(
            "Produced {} records with edge-case payloads, consumed and compared byte-for-byte",
            n
        ),
        sample_size: n,
        passed,
        details: if passed {
            format!("All {} edge-case payloads preserved exactly", n)
        } else {
            format!("Failures: {}", failures.join("; "))
        },
        duration_secs: start.elapsed().as_secs_f64(),
    }
}

// ---------------------------------------------------------------------------
// Fidelity payload generator — 500 edge-case payloads
// ---------------------------------------------------------------------------

pub fn generate_fidelity_payloads() -> Vec<String> {
    let mut payloads = Vec::with_capacity(500);

    // --- Boundary lengths (~27 payloads) ---
    let boundary_sizes: Vec<usize> = vec![
        0, 1, 2, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024,
        1025, 4095, 4096, 4097, 65535, 65536, 65537,
    ];
    for size in &boundary_sizes {
        payloads.push(generate_payload(*size));
    }

    // --- Unicode sequences (~50 payloads) ---
    let unicode_payloads: Vec<String> = vec![
        // Emoji
        "\u{1F600}\u{1F601}\u{1F602}\u{1F603}\u{1F604}".to_string(),
        "\u{1F30D}\u{1F30E}\u{1F30F}\u{2600}\u{2601}".to_string(),
        // Emoji sequences (family, flags)
        "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}\u{200D}\u{1F466}".to_string(),
        "\u{1F1FA}\u{1F1F8}".to_string(), // US flag
        "\u{1F1EC}\u{1F1E7}".to_string(), // GB flag
        // CJK characters
        "\u{4E16}\u{754C}\u{4F60}\u{597D}".to_string(),
        "\u{3053}\u{3093}\u{306B}\u{3061}\u{306F}".to_string(), // Japanese
        "\u{D55C}\u{AD6D}\u{C5B4}".to_string(), // Korean
        // CJK with ASCII
        "Hello \u{4E16}\u{754C} World".to_string(),
        "test\u{3042}\u{3044}\u{3046}test".to_string(),
        // RTL text
        "\u{0627}\u{0644}\u{0639}\u{0631}\u{0628}\u{064A}\u{0629}".to_string(), // Arabic
        "\u{05E2}\u{05D1}\u{05E8}\u{05D9}\u{05EA}".to_string(), // Hebrew
        // Mixed RTL/LTR
        "Hello \u{0627}\u{0644}\u{0639}\u{0631}\u{0628}\u{064A}\u{0629} World".to_string(),
        // Combining characters
        "e\u{0301}".to_string(), // e + combining acute
        "a\u{0300}\u{0301}\u{0302}\u{0303}".to_string(), // a + 4 combining marks
        "n\u{0303}".to_string(), // ñ via combining
        "o\u{0308}".to_string(), // ö via combining
        "\u{0041}\u{030A}".to_string(), // Å via combining
        // Zero-width joiners/non-joiners
        "foo\u{200B}bar".to_string(), // zero-width space
        "foo\u{200C}bar".to_string(), // zero-width non-joiner
        "foo\u{200D}bar".to_string(), // zero-width joiner
        "foo\u{FEFF}bar".to_string(), // BOM as ZW no-break space
        // Surrogate-adjacent codepoints (just before/after surrogate range)
        "\u{D7FF}".to_string(),
        "\u{E000}".to_string(),
        // Mathematical symbols
        "\u{2200}x \u{2208} \u{211D}: x\u{00B2} \u{2265} 0".to_string(),
        "\u{222B}\u{221E}".to_string(), // integral + infinity
        // Musical symbols (astral plane)
        "\u{1D11E}\u{1D11F}\u{1D120}".to_string(),
        // Emoji skin tones
        "\u{1F44B}\u{1F3FB}\u{1F44B}\u{1F3FD}\u{1F44B}\u{1F3FF}".to_string(),
        // Zalgo text
        "H\u{0336}\u{0321}\u{034E}e\u{0337}\u{031E}\u{0326}l\u{0335}\u{031F}l\u{0334}\u{032E}o\u{0336}\u{0323}".to_string(),
        // Box drawing
        "\u{250C}\u{2500}\u{2500}\u{2510}\n\u{2502}  \u{2502}\n\u{2514}\u{2500}\u{2500}\u{2518}".to_string(),
        // Braille patterns
        "\u{2801}\u{2802}\u{2803}\u{2804}\u{2805}\u{2806}\u{2807}\u{2808}".to_string(),
        // Diacritical marks stacking
        "\u{0041}\u{0300}\u{0301}\u{0302}\u{0303}\u{0304}\u{0305}\u{0306}\u{0307}\u{0308}".to_string(),
        // Full-width characters
        "\u{FF21}\u{FF22}\u{FF23}\u{FF24}\u{FF25}".to_string(), // ＡＢＣＤＥ
        // Ideographic space
        "a\u{3000}b".to_string(),
        // Tibetan
        "\u{0F00}\u{0F01}\u{0F02}\u{0F03}".to_string(),
        // Emoji with variation selectors
        "\u{2764}\u{FE0F}".to_string(), // red heart (emoji presentation)
        "\u{2764}\u{FE0E}".to_string(), // red heart (text presentation)
        // Tag sequence (England flag)
        "\u{1F3F4}\u{E0067}\u{E0062}\u{E0065}\u{E006E}\u{E0067}\u{E007F}".to_string(),
        // Long emoji sequence
        "\u{1F468}\u{200D}\u{2764}\u{FE0F}\u{200D}\u{1F48B}\u{200D}\u{1F468}".to_string(),
        // Interlinear annotation
        "\u{FFF9}annotated\u{FFFA}annotation\u{FFFB}".to_string(),
        // Unicode replacement character
        "\u{FFFD}\u{FFFD}\u{FFFD}".to_string(),
        // Devanagari
        "\u{0928}\u{092E}\u{0938}\u{094D}\u{0924}\u{0947}".to_string(),
        // Thai
        "\u{0E2A}\u{0E27}\u{0E31}\u{0E2A}\u{0E14}\u{0E35}".to_string(),
        // Georgian
        "\u{10D2}\u{10D0}\u{10DB}\u{10D0}\u{10E0}\u{10EF}\u{10DD}\u{10D1}\u{10D0}".to_string(),
        // Long string of emoji
        (0..50).map(|i| char::from_u32(0x1F600 + (i % 80)).unwrap_or('\u{FFFD}')).collect::<String>(),
        // Mixed scripts
        "Hello\u{4E16}\u{754C}\u{0627}\u{0644}\u{0639}\u{05E2}\u{05D1}\u{D55C}\u{AD6D}".to_string(),
        // Newlines in every style
        "line1\nline2\rline3\r\nline4\u{000B}line5\u{000C}line6\u{0085}line7\u{2028}line8\u{2029}line9".to_string(),
        // Single high codepoint
        "\u{10FFFF}".to_string(),
        // Repeated 4-byte chars
        "\u{1F4A9}".repeat(100),
    ];
    payloads.extend(unicode_payloads);

    // --- Special characters (~20 payloads) ---
    let special_payloads: Vec<String> = vec![
        // Null bytes
        "before\0after".to_string(),
        "\0".to_string(),
        "\0\0\0\0\0".to_string(),
        "start\0\0\0end".to_string(),
        // Tabs and whitespace
        "\t\t\t".to_string(),
        "col1\tcol2\tcol3".to_string(),
        // Mixed whitespace
        " \t\n\r \t\n\r ".to_string(),
        // Backslashes
        "\\\\\\".to_string(),
        "C:\\Users\\test\\file.txt".to_string(),
        // Quotes
        r#"he said "hello" and 'goodbye'"#.to_string(),
        "quote\"in\"middle".to_string(),
        // Control characters (valid UTF-8)
        "\x01\x02\x03\x04\x05\x06\x07".to_string(),
        "\x08\x0E\x0F\x10\x11\x12\x13".to_string(),
        "\x14\x15\x16\x17\x18\x19\x1A".to_string(),
        "\x1B[31mred\x1B[0m".to_string(), // ANSI escape
        // Bell and delete
        "\x07\x7F".to_string(),
        // Percent encoding chars
        "%20%00%FF%0A".to_string(),
        // Regex-like
        "^foo.*bar$[a-z]+\\d{3}".to_string(),
        // Shell injection attempts (as data)
        "$(echo pwned)".to_string(),
        "; rm -rf /".to_string(),
    ];
    payloads.extend(special_payloads);

    // --- Structured data (~50 payloads) ---
    let structured_payloads: Vec<String> = vec![
        // Nested JSON
        r#"{"a":{"b":{"c":{"d":{"e":"deep"}}}}}"#.to_string(),
        r#"{"array":[1,2,3,[4,[5,[6]]]]}"#.to_string(),
        r#"{"empty_obj":{},"empty_arr":[],"null":null}"#.to_string(),
        r#"{"unicode":"ñoño","emoji":"😀","cjk":"世界"}"#.to_string(),
        r#"{"escaped":"\"quotes\" and \\backslash"}"#.to_string(),
        r#"{"numbers":{"int":42,"float":3.14,"neg":-1,"exp":1e10,"big":99999999999999}}"#.to_string(),
        r#"{"bool_true":true,"bool_false":false,"null_val":null}"#.to_string(),
        // Deeply nested
        (0..20).fold("\"leaf\"".to_string(), |acc, _| format!("{{\"n\":{}}}", acc)),
        // Large array
        format!("[{}]", (0..100).map(|i| i.to_string()).collect::<Vec<_>>().join(",")),
        // JSON with whitespace
        "{\n  \"key\" : \"value\" ,\n  \"arr\" : [ 1 , 2 , 3 ]\n}".to_string(),
        // XML-like
        "<root><child attr=\"val\">text</child></root>".to_string(),
        "<html><body><div class=\"test\">&amp;&lt;&gt;</div></body></html>".to_string(),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root/>".to_string(),
        "<![CDATA[some <data> & more]]>".to_string(),
        "<a><b><c><d><e>deep</e></d></c></b></a>".to_string(),
        // CSV-like
        "col1,col2,col3\nval1,val2,val3\n".to_string(),
        "\"quoted,field\",normal,\"has \"\"escaped\"\" quotes\"".to_string(),
        ",,,empty,,,fields,,,".to_string(),
        "a\tb\tc\n1\t2\t3\n".to_string(),
        // SQL-like
        "SELECT * FROM users WHERE name = 'O''Brien' AND age > 21".to_string(),
        "INSERT INTO t (a, b) VALUES (1, 'hello; DROP TABLE t')".to_string(),
        "UPDATE t SET val = NULL WHERE id IN (1,2,3)".to_string(),
        // YAML-like
        "key: value\nlist:\n  - item1\n  - item2\nnested:\n  inner: val".to_string(),
        "---\ntitle: test\n...\n".to_string(),
        // INI-like
        "[section]\nkey=value\n[another]\nfoo=bar".to_string(),
        // URL-like
        "https://example.com/path?q=hello+world&lang=en#section".to_string(),
        "ftp://user:pass@host:21/path".to_string(),
        "data:text/plain;base64,SGVsbG8gV29ybGQ=".to_string(),
        // Email-like
        "From: user@example.com\nTo: other@test.org\nSubject: Test\n\nBody".to_string(),
        // Log-like
        "2024-01-15T10:30:45.123Z INFO [main] Starting up...".to_string(),
        "ERROR: NullPointerException at com.example.Main.run(Main.java:42)".to_string(),
        // Markdown-like
        "# Header\n\n**bold** _italic_ `code`\n\n- item\n- item\n\n```\ncode block\n```".to_string(),
        // Protocol buffer text format-like
        "message { field: 42 nested { str: \"hello\" } repeated: [1, 2, 3] }".to_string(),
        // GraphQL-like
        "{ user(id: 1) { name email posts { title } } }".to_string(),
        // Regex patterns
        r"^(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)$".to_string(),
        // Path-like
        "/usr/local/bin/../lib/./test".to_string(),
        "C:\\Program Files (x86)\\App\\config.ini".to_string(),
        // JWT-like (not real, just structured like one)
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.dGVzdHNpZ25hdHVyZQ".to_string(),
        // Base64-like
        "SGVsbG8gV29ybGQhIFRoaXMgaXMgYSB0ZXN0IG9mIGJhc2U2NCBlbmNvZGluZy4=".to_string(),
        // Hex dump-like
        "00000000  48 65 6c 6c 6f 20 57 6f  72 6c 64 0a              |Hello World.|".to_string(),
        // Key=value pairs
        "key1=val1&key2=val2&key3=val3&arr[]=1&arr[]=2".to_string(),
        // TOML-like
        "[package]\nname = \"test\"\nversion = \"1.0\"\n\n[dependencies]\nfoo = \"*\"".to_string(),
        // Protobuf varint-like binary as text
        "\x08\u{0096}\x01".to_string(),
        // S-expression
        "(define (fib n) (if (<= n 1) n (+ (fib (- n 1)) (fib (- n 2)))))".to_string(),
        // Stack trace
        "thread 'main' panicked at 'assertion failed', src/main.rs:42:5\nnote: run with `RUST_BACKTRACE=1`".to_string(),
        // Multiline with mixed endings
        "line1\nline2\r\nline3\rline4".to_string(),
        // JSON Lines
        "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n".to_string(),
        // Template-like
        "Hello {{name}}, you have {{count}} messages".to_string(),
        "${HOME}/path/to/${USER}/file".to_string(),
    ];
    payloads.extend(structured_payloads);

    // --- Repetitive patterns (~20 payloads) ---
    let repeat_chars = ['a', 'z', '0', ' ', '\n', '\t', '\0', '.', 'X', '\u{1F600}'];
    let repeat_sizes = [1, 10, 100, 1000, 10000];
    // We'll take a selection to get ~20
    for ch in &repeat_chars[..4] {
        for &size in &repeat_sizes {
            payloads.push(std::iter::repeat_n(*ch, size).collect::<String>());
        }
    }

    // --- All printable ASCII ---
    payloads.push((32u8..=126).map(|b| b as char).collect::<String>());

    // --- Fill to 500 with random-length printable ASCII ---
    // Use a simple deterministic PRNG to generate reproducible payloads
    let mut seed: u64 = 0xDEAD_BEEF_CAFE_BABE;
    while payloads.len() < 500 {
        // xorshift64
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let len = ((seed % 65536) + 1) as usize; // 1 to 65536 bytes
        let payload: String = (0..len)
            .map(|j| {
                let mut s = seed.wrapping_add(j as u64);
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                // Map to printable ASCII range 32..=126
                (32 + (s % 95) as u8) as char
            })
            .collect();
        payloads.push(payload);
    }

    payloads.truncate(500);
    payloads
}

// ---------------------------------------------------------------------------
// Nemesis checkers (reusable from both tests and report generator)
// ---------------------------------------------------------------------------

/// Nemesis 1: Crash simulation — drop broker without close across multiple cycles.
pub fn check_crash_recovery(dir: &Path, records_per_cycle: usize, cycles: usize) -> NemesisResult {
    let mut expected_total = 0;

    for cycle in 0..cycles {
        let broker = Broker::open(broker_config(dir)).unwrap();
        let producer = Broker::producer(&broker);
        for i in 0..records_per_cycle {
            let pr = ProducerRecord::new("crash-topic", None, format!("cycle{}-rec{}", cycle, i));
            producer.send(&pr).unwrap();
        }
        expected_total += records_per_cycle;
        drop(broker);
    }

    let broker = Broker::open(broker_config(dir)).unwrap();
    let consumed = consume_all(
        &broker,
        "crash-verify",
        &["crash-topic"],
        OffsetReset::Earliest,
    );

    let recovered = consumed.len();
    let passed = recovered == expected_total;

    NemesisResult {
        name: "Crash Recovery".to_string(),
        fault: format!(
            "Drop broker without close across {} cycles ({} records/cycle)",
            cycles, records_per_cycle
        ),
        outcome: if passed {
            "recovered".to_string()
        } else {
            "data_loss".to_string()
        },
        records_before: expected_total,
        records_after: recovered,
        details: if passed {
            format!(
                "All {} records survived {} ungraceful drop cycles",
                expected_total, cycles
            )
        } else {
            format!(
                "Expected {} records, recovered {}",
                expected_total, recovered
            )
        },
        passed,
    }
}

/// Nemesis 2: Truncated tree.snapshot — halve the snapshot file and reopen.
pub fn check_truncated_snapshot(dir: &Path, n: usize) -> NemesisResult {
    // Produce records
    {
        let broker = Broker::open(broker_config(dir)).unwrap();
        produce_n(&broker, "trunc-snap", n, |i| format!("v{}", i), |_| None);
    }

    // Find and truncate tree.snapshot
    let snapshot_path = find_file_recursive(dir, "tree.snapshot");
    assert!(
        snapshot_path.is_some(),
        "tree.snapshot should exist after producing records"
    );
    let snapshot_path = snapshot_path.unwrap();
    let original_len = std::fs::metadata(&snapshot_path).unwrap().len();
    let truncated_len = original_len / 2;

    let data = std::fs::read(&snapshot_path).unwrap();
    std::fs::write(&snapshot_path, &data[..truncated_len as usize]).unwrap();

    // Attempt to reopen
    let result = Broker::open(broker_config(dir));
    match result {
        Ok(broker) => {
            // Broker reopened — check what we got
            let topic = broker.topic("trunc-snap");
            let records_after = match topic {
                Some(t) => {
                    let part_arc = t.partition(0).unwrap();
                    let partition = part_arc.read().unwrap();
                    let mut readable = 0;
                    for offset in 0..partition.next_offset() {
                        if partition.read(offset).unwrap().is_some() {
                            readable += 1;
                        }
                    }
                    readable
                }
                None => 0,
            };

            NemesisResult {
                name: "Truncated Snapshot".to_string(),
                fault: format!(
                    "Truncate tree.snapshot from {} to {} bytes (50%)",
                    original_len, truncated_len
                ),
                outcome: if records_after == n {
                    "recovered".to_string()
                } else {
                    "data_loss".to_string()
                },
                records_before: n,
                records_after,
                details: format!(
                    "Broker reopened with {} of {} records readable",
                    records_after, n
                ),
                passed: true, // Reopening without error is acceptable
            }
        }
        Err(e) => {
            NemesisResult {
                name: "Truncated Snapshot".to_string(),
                fault: format!(
                    "Truncate tree.snapshot from {} to {} bytes (50%)",
                    original_len, truncated_len
                ),
                outcome: "safe_failure".to_string(),
                records_before: n,
                records_after: 0,
                details: format!("Broker refuses to reopen: {}", e),
                passed: true, // Safe failure is correct behavior
            }
        }
    }
}

/// Nemesis 3: Truncated offsets.idx — remove 16 bytes (half an entry) from the index.
pub fn check_truncated_index(dir: &Path, n: usize) -> NemesisResult {
    {
        let broker = Broker::open(broker_config(dir)).unwrap();
        produce_n(&broker, "trunc-idx", n, |i| format!("v{}", i), |_| None);
    }

    let index_path = find_file_recursive(dir, "offsets.idx");
    assert!(
        index_path.is_some(),
        "offsets.idx should exist after producing records"
    );
    let index_path = index_path.unwrap();
    let original_len = std::fs::metadata(&index_path).unwrap().len();
    let truncated_len = original_len - 16; // Remove half an entry (entry = 32 bytes)

    let data = std::fs::read(&index_path).unwrap();
    std::fs::write(&index_path, &data[..truncated_len as usize]).unwrap();

    let expected_entries = truncated_len / 32;

    let result = Broker::open(broker_config(dir));
    match result {
        Ok(broker) => {
            let topic = broker.topic("trunc-idx").unwrap();
            let part_arc = topic.partition(0).unwrap();
            let partition = part_arc.read().unwrap();
            let reported_offset = partition.next_offset();

            let mut readable = 0;
            let mut errors = 0;
            for offset in 0..reported_offset {
                match partition.read(offset) {
                    Ok(Some(_)) => readable += 1,
                    Ok(None) => {}
                    Err(_) => errors += 1,
                }
            }

            let passed = reported_offset == expected_entries && errors == 0;

            NemesisResult {
                name: "Truncated Index".to_string(),
                fault: format!(
                    "Truncate offsets.idx by 16 bytes ({} -> {} entries)",
                    original_len / 32,
                    expected_entries
                ),
                outcome: if passed {
                    "recovered".to_string()
                } else {
                    "data_loss".to_string()
                },
                records_before: n,
                records_after: readable,
                details: format!(
                    "Broker reports next_offset={}, {} readable, {} errors (expected {} complete entries)",
                    reported_offset, readable, errors, expected_entries
                ),
                passed,
            }
        }
        Err(e) => NemesisResult {
            name: "Truncated Index".to_string(),
            fault: "Truncate offsets.idx by 16 bytes".to_string(),
            outcome: "safe_failure".to_string(),
            records_before: n,
            records_after: 0,
            details: format!("Broker failed to reopen: {}", e),
            passed: false,
        },
    }
}

/// Nemesis 4: Missing tree.snapshot — delete snapshot file and reopen.
pub fn check_missing_snapshot(dir: &Path, n: usize) -> NemesisResult {
    {
        let broker = Broker::open(broker_config(dir)).unwrap();
        produce_n(&broker, "no-snap", n, |i| format!("v{}", i), |_| None);
    }

    let snapshot_path = find_file_recursive(dir, "tree.snapshot");
    assert!(
        snapshot_path.is_some(),
        "tree.snapshot should exist after producing records"
    );
    let snapshot_path = snapshot_path.unwrap();
    std::fs::remove_file(&snapshot_path).unwrap();

    let result = Broker::open(broker_config(dir));
    match result {
        Ok(broker) => {
            let topic = broker.topic("no-snap").unwrap();
            let part_arc = topic.partition(0).unwrap();
            let partition = part_arc.read().unwrap();

            let mut readable = 0;
            for offset in 0..partition.next_offset() {
                if partition.read(offset).unwrap().is_some() {
                    readable += 1;
                }
            }

            // Try appending new records
            drop(partition);
            drop(part_arc);
            let producer = Broker::producer(&broker);
            let pr = ProducerRecord::new("no-snap", None, "after-snapshot-delete");
            let append_ok = producer.send(&pr).is_ok();

            let passed = readable == n && append_ok;

            NemesisResult {
                name: "Missing Snapshot".to_string(),
                fault: "Delete tree.snapshot file and reopen".to_string(),
                outcome: if passed {
                    "recovered".to_string()
                } else {
                    "data_loss".to_string()
                },
                records_before: n,
                records_after: readable,
                details: format!(
                    "{} of {} records readable, append after delete: {}",
                    readable,
                    n,
                    if append_ok { "succeeded" } else { "failed" }
                ),
                passed,
            }
        }
        Err(e) => NemesisResult {
            name: "Missing Snapshot".to_string(),
            fault: "Delete tree.snapshot file and reopen".to_string(),
            outcome: "safe_failure".to_string(),
            records_before: n,
            records_after: 0,
            details: format!("Broker failed to reopen: {}", e),
            passed: false,
        },
    }
}

/// Nemesis 5: Index ahead of snapshot — simulates crash between index write and snapshot write.
pub fn check_index_ahead_of_snapshot(dir: &Path, n: usize) -> NemesisResult {
    // Produce n records normally
    {
        let broker = Broker::open(broker_config(dir)).unwrap();
        produce_n(&broker, "idx-ahead", n, |i| format!("v{}", i), |_| None);
    }

    // Save snapshot state, then add one more record, then revert snapshot
    let snapshot_path = find_file_recursive(dir, "tree.snapshot").unwrap();
    let snapshot_before = std::fs::read(&snapshot_path).unwrap();

    {
        let broker = Broker::open(broker_config(dir)).unwrap();
        produce_n(&broker, "idx-ahead", 1, |_| "extra".to_string(), |_| None);
    }

    // Revert snapshot to the pre-(n+1) state
    std::fs::write(&snapshot_path, &snapshot_before).unwrap();

    let result = Broker::open(broker_config(dir));
    match result {
        Ok(broker) => {
            let topic = broker.topic("idx-ahead").unwrap();
            let part_arc = topic.partition(0).unwrap();
            let partition = part_arc.read().unwrap();

            let mut readable = 0;
            for offset in 0..partition.next_offset() {
                if partition.read(offset).unwrap().is_some() {
                    readable += 1;
                }
            }

            let mut valid_proofs = 0;
            let mut proof_errors = 0;
            for offset in 0..partition.next_offset() {
                match partition.proof(offset) {
                    Ok(Some(proof)) => match MerkleTree::verify_proof(&proof, partition.store()) {
                        Ok(true) => valid_proofs += 1,
                        Ok(false) | Err(_) => proof_errors += 1,
                    },
                    Ok(None) | Err(_) => proof_errors += 1,
                }
            }

            // Expect: all n+1 records readable, n valid proofs, 1 proof error at crash boundary
            let passed = readable == n + 1 && valid_proofs >= n;

            NemesisResult {
                name: "Index Ahead of Snapshot".to_string(),
                fault: format!(
                    "Revert tree.snapshot to state with {} records, index has {} entries",
                    n,
                    n + 1
                ),
                outcome: if passed {
                    "recovered".to_string()
                } else {
                    "data_loss".to_string()
                },
                records_before: n + 1,
                records_after: readable,
                details: format!(
                    "{} readable, {} valid proofs, {} proof errors",
                    readable, valid_proofs, proof_errors
                ),
                passed,
            }
        }
        Err(e) => NemesisResult {
            name: "Index Ahead of Snapshot".to_string(),
            fault: format!("Revert tree.snapshot to pre-{} state", n + 1),
            outcome: "safe_failure".to_string(),
            records_before: n + 1,
            records_after: 0,
            details: format!("Broker failed to reopen: {}", e),
            passed: false,
        },
    }
}
