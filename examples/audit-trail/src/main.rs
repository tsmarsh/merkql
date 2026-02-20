use merkql::broker::{Broker, BrokerConfig};
use merkql::hash::Hash;
use merkql::record::ProducerRecord;
use merkql::tree::MerkleTree;
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Debug, Serialize, Deserialize)]
struct AuditEvent {
    user: String,
    action: String,
    resource: String,
    ts: u64,
}

fn main() {
    let dir = tempfile::tempdir().unwrap();

    println!("=== MerkQL Audit Trail Example ===\n");

    // Step 1: Open broker (no compression, no retention — full history)
    println!("--- Step 1: Open broker (no compression, full history) ---");
    let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    println!("Broker opened.\n");

    // Step 2: Produce 50 audit events
    println!("--- Step 2: Produce 50 audit events ---");
    let producer = Broker::producer(&broker);
    let users = ["alice", "bob", "charlie", "diana", "eve"];
    let actions = ["CREATE", "READ", "UPDATE", "DELETE", "LOGIN"];
    let resources = ["document", "user", "config", "report", "dashboard"];

    for i in 0..50 {
        let event = AuditEvent {
            user: users[i % users.len()].to_string(),
            action: actions[i % actions.len()].to_string(),
            resource: resources[(i * 3) % resources.len()].to_string(),
            ts: 1700000000 + i as u64,
        };
        producer
            .send(&ProducerRecord::new(
                "audit",
                Some(event.user.clone()),
                serde_json::to_string(&event).unwrap(),
            ))
            .unwrap();
    }
    println!("Produced 50 audit events.\n");

    // Step 3: Generate and verify proofs for 5 sampled offsets
    println!("--- Step 3: Generate and verify merkle proofs ---");
    let sample_offsets: Vec<u64> = vec![0, 10, 25, 37, 49];
    let topic = broker.topic("audit").unwrap();
    let part_arc = topic.partition(0).unwrap();

    let mut proofs = Vec::new();
    {
        let partition = part_arc.read().unwrap();
        for &offset in &sample_offsets {
            let proof = partition.proof(offset).unwrap().unwrap();
            let valid = MerkleTree::verify_proof(&proof, partition.store()).unwrap();
            println!(
                "  Offset {:>2}: leaf={:.16}...  depth={}  valid={}",
                offset,
                proof.leaf_hash.to_hex(),
                proof.siblings.len(),
                valid
            );
            assert!(valid, "Proof should be valid");
            proofs.push(proof);
        }
    }
    println!();

    // Step 4: Capture the merkle root hash
    println!("--- Step 4: Capture merkle root ---");
    let root_before = {
        let partition = part_arc.read().unwrap();
        partition.merkle_root().unwrap().unwrap()
    };
    println!("  Root after 50 events: {:.32}...\n", root_before.to_hex());

    // Step 5: Produce 10 more events
    println!("--- Step 5: Produce 10 more events ---");
    for i in 50..60 {
        let event = AuditEvent {
            user: users[i % users.len()].to_string(),
            action: actions[i % actions.len()].to_string(),
            resource: resources[(i * 3) % resources.len()].to_string(),
            ts: 1700000000 + i as u64,
        };
        producer
            .send(&ProducerRecord::new(
                "audit",
                Some(event.user.clone()),
                serde_json::to_string(&event).unwrap(),
            ))
            .unwrap();
    }
    println!("Produced 10 more events.\n");

    // Step 6: New root — should have changed
    println!("--- Step 6: Root progression ---");
    let root_after = {
        let partition = part_arc.read().unwrap();
        partition.merkle_root().unwrap().unwrap()
    };
    println!("  Root before: {:.32}...", root_before.to_hex());
    println!("  Root after:  {:.32}...", root_after.to_hex());
    assert_ne!(
        root_before, root_after,
        "Root should change after appending"
    );
    println!("  Root changed as expected.\n");

    // Step 7: Re-verify the 5 earlier proofs (appending doesn't invalidate them)
    println!("--- Step 7: Re-verify earlier proofs after appending ---");
    {
        let partition = part_arc.read().unwrap();
        for (i, proof) in proofs.iter().enumerate() {
            let valid = MerkleTree::verify_proof(proof, partition.store()).unwrap();
            println!(
                "  Proof for offset {}: still valid = {}",
                sample_offsets[i], valid
            );
            assert!(valid, "Earlier proof should remain valid after appending");
        }
    }
    println!();

    // Step 8: Tamper detection demo
    println!("--- Step 8: Tamper detection ---");
    let tamper_offset: u64 = 10;
    {
        let partition = part_arc.read().unwrap();

        // Read the record and get its expected hash
        let record = partition.read(tamper_offset).unwrap().unwrap();
        let serialized = record.serialize();
        let expected_hash = Hash::digest(&serialized);
        println!(
            "  Record at offset {}: user={}, action={}",
            tamper_offset,
            serde_json::from_str::<AuditEvent>(&record.value)
                .unwrap()
                .user,
            serde_json::from_str::<AuditEvent>(&record.value)
                .unwrap()
                .action
        );
        println!("  Expected hash: {:.32}...", expected_hash.to_hex());

        // Corrupt the pack file: find the data and flip some bytes
        let pack_path = dir
            .path()
            .join(".merkql/topics/audit/partitions/0/objects.pack");
        tamper_pack_file(&pack_path, &expected_hash);
        println!(
            "  Tampered with pack file data for offset {}.",
            tamper_offset
        );

        // Now try to read the record — the data will be corrupted
        let corrupted_data = partition.store().get(&expected_hash).unwrap();
        let corrupted_hash = Hash::digest(&corrupted_data);
        let hashes_match = expected_hash == corrupted_hash;
        println!("  Re-read hash:  {:.32}...", corrupted_hash.to_hex());
        println!(
            "  Integrity check: hashes match = {} (tamper {})",
            hashes_match,
            if hashes_match {
                "NOT detected"
            } else {
                "DETECTED"
            }
        );
        assert!(
            !hashes_match,
            "Corrupted data should produce a different hash"
        );
    }
    println!();

    // Summary
    println!("--- Summary ---");
    println!(
        "  All {} proofs verified successfully.",
        sample_offsets.len()
    );
    println!("  Root progression confirmed after appending new events.");
    println!("  Earlier proofs remain valid after appending (append-only property).");
    println!("  Tamper detected at offset {}.", tamper_offset);
    println!("\n=== Audit Trail Example Complete ===");
}

/// Corrupt the pack file by finding an entry matching the given hash and flipping bytes in its data.
fn tamper_pack_file(pack_path: &std::path::Path, target_hash: &Hash) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(pack_path)
        .unwrap();

    let file_len = file.metadata().unwrap().len();
    let mut pos: u64 = 0;

    while pos < file_len {
        // Read entry header: 4 bytes length + 32 bytes hash
        file.seek(SeekFrom::Start(pos)).unwrap();
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf).unwrap();
        let data_len = u32::from_le_bytes(len_buf);

        let mut hash_buf = [0u8; 32];
        file.read_exact(&mut hash_buf).unwrap();

        let data_offset = pos + 36; // 4 + 32
        let entry_end = data_offset + data_len as u64;

        if Hash(hash_buf) == *target_hash && data_len > 0 {
            // Flip bytes in the middle of the data
            let flip_offset = data_offset + (data_len as u64 / 2);
            file.seek(SeekFrom::Start(flip_offset)).unwrap();
            let mut byte = [0u8; 1];
            file.read_exact(&mut byte).unwrap();
            byte[0] ^= 0xFF;
            file.seek(SeekFrom::Start(flip_offset)).unwrap();
            file.write_all(&byte).unwrap();
            file.sync_all().unwrap();
            return;
        }

        pos = entry_end;
    }

    panic!("Target hash not found in pack file");
}
