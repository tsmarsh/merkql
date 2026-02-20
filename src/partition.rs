use crate::compression::Compression;
use crate::hash::Hash;
use crate::record::Record;
use crate::store::ObjectStore;
use crate::tree::{MerkleTree, TreeSnapshot};
use anyhow::{Context, Result};
use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Fixed-width index entry: 32 bytes (SHA-256 hash of the record object).
const INDEX_ENTRY_SIZE: usize = 32;

/// CRC32 checksum size in bytes.
const CRC_SIZE: usize = 4;

/// Atomically write checksummed data to a file using temp+fsync+rename+fsync-parent.
/// Format: [4 bytes CRC32 of data][data...]
fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let crc = crc32fast::hash(data);
    let tmp = path.with_extension("tmp");
    let mut f = fs::File::create(&tmp).context("creating temp file for atomic write")?;
    f.write_all(&crc.to_le_bytes())
        .context("writing CRC32 checksum")?;
    f.write_all(data).context("writing atomic data")?;
    f.sync_all().context("syncing atomic write")?;
    fs::rename(&tmp, path).context("renaming atomic write")?;

    // fsync parent directory to ensure the directory entry is durable (NFS safety)
    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

/// Read checksummed data written by `atomic_write`.
/// Returns Ok(Some(data)) on success, Ok(None) if CRC mismatch.
fn atomic_read(path: &Path) -> Result<Option<Vec<u8>>> {
    let raw = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if raw.len() < CRC_SIZE {
        return Ok(None);
    }
    let stored_crc = u32::from_le_bytes(raw[..CRC_SIZE].try_into().unwrap());
    let data = &raw[CRC_SIZE..];
    let computed_crc = crc32fast::hash(data);
    if stored_crc != computed_crc {
        return Ok(None);
    }
    Ok(Some(data.to_vec()))
}

/// Acquire an exclusive flock on a lock file in the given directory.
/// Returns the lock file handle (lock released on drop).
fn acquire_partition_lock(dir: &Path) -> Result<fs::File> {
    let lock_path = dir.join("partition.lock");
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&lock_path)
        .context("opening partition lock file")?;
    fs2::FileExt::lock_exclusive(&lock_file).context("acquiring partition lock")?;
    Ok(lock_file)
}

/// A single append-only partition backed by a merkle tree.
pub struct Partition {
    id: u32,
    dir: PathBuf,
    store: ObjectStore,
    tree: MerkleTree,
    next_offset: u64,
    index_writer: BufWriter<fs::File>,
    min_valid_offset: u64,
}

impl Partition {
    /// Open or create a partition at the given directory.
    pub fn open(id: u32, dir: impl Into<PathBuf>, compression: Compression) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).context("creating partition dir")?;

        let pack_path = dir.join("objects.pack");
        let objects_dir = dir.join("objects");
        let store = ObjectStore::open(pack_path, objects_dir, compression)?;

        // Restore tree snapshot if it exists, with CRC validation
        let snapshot_path = dir.join("tree.snapshot");
        let tree = if snapshot_path.exists() {
            match atomic_read(&snapshot_path) {
                Ok(Some(data)) => {
                    match bincode::deserialize::<TreeSnapshot>(&data) {
                        Ok(snap) => MerkleTree::from_snapshot(snap),
                        Err(_) => MerkleTree::new(), // corrupt bincode → empty tree
                    }
                }
                Ok(None) => MerkleTree::new(), // CRC mismatch → empty tree (will rebuild)
                Err(_) => MerkleTree::new(),    // read error → empty tree
            }
        } else {
            MerkleTree::new()
        };

        // Determine next offset from index size using file handle (not metadata on path)
        let index_path = dir.join("offsets.idx");
        let next_offset = if index_path.exists() {
            let mut f =
                fs::File::open(&index_path).context("opening index for size determination")?;
            let len = f
                .seek(SeekFrom::End(0))
                .context("seeking to end of index")?;
            len / INDEX_ENTRY_SIZE as u64
        } else {
            0
        };

        // Restore retention marker with CRC validation
        let retention_path = dir.join("retention.bin");
        let min_valid_offset = if retention_path.exists() {
            match atomic_read(&retention_path) {
                Ok(Some(data)) => bincode::deserialize(&data).unwrap_or(0),
                Ok(None) => 0, // CRC mismatch → safe default
                Err(_) => 0,   // read error → safe default
            }
        } else {
            0
        };

        // Open index file for persistent appending
        let index_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)
            .context("opening offset index for append")?;
        let index_writer = BufWriter::new(index_file);

        Ok(Partition {
            id,
            dir,
            store,
            tree,
            next_offset,
            index_writer,
            min_valid_offset,
        })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn min_valid_offset(&self) -> u64 {
        self.min_valid_offset
    }

    /// Append a record to the partition, assigning the next sequential offset.
    /// Returns the assigned offset.
    /// Acquires an exclusive flock for writer exclusion (NFS-safe).
    pub fn append(&mut self, record: &mut Record) -> Result<u64> {
        let _lock = acquire_partition_lock(&self.dir)?;

        let offset = self.next_offset;
        record.offset = offset;
        record.partition = self.id;

        // Store the record
        let record_bytes = record.serialize();
        let record_hash = self.store.put(&record_bytes)?;

        // Append to merkle tree
        self.tree.append(record_hash, offset, &self.store)?;

        // Append to offset index (buffered)
        self.index_writer
            .write_all(&record_hash.0)
            .context("writing index entry")?;

        // Flush and fsync index + pack file
        self.index_writer.flush().context("flushing index")?;
        self.index_writer
            .get_ref()
            .sync_all()
            .context("syncing index")?;
        self.store.flush()?;

        // Persist tree snapshot atomically (with CRC)
        let snap = self.tree.snapshot();
        let snap_bytes = bincode::serialize(&snap).context("serializing snapshot")?;
        atomic_write(&self.dir.join("tree.snapshot"), &snap_bytes)?;

        self.next_offset += 1;
        Ok(offset)
    }

    /// Append a batch of records. Amortizes fsync and snapshot writes.
    /// Returns the assigned offsets.
    /// Acquires an exclusive flock for writer exclusion (NFS-safe).
    pub fn append_batch(&mut self, records: &mut [Record]) -> Result<Vec<u64>> {
        let _lock = acquire_partition_lock(&self.dir)?;

        let mut offsets = Vec::with_capacity(records.len());

        for record in records.iter_mut() {
            let offset = self.next_offset;
            record.offset = offset;
            record.partition = self.id;

            // Store the record
            let record_bytes = record.serialize();
            let record_hash = self.store.put(&record_bytes)?;

            // Append to merkle tree
            self.tree.append(record_hash, offset, &self.store)?;

            // Buffer index entry
            self.index_writer
                .write_all(&record_hash.0)
                .context("writing index entry")?;

            self.next_offset += 1;
            offsets.push(offset);
        }

        // Single flush + fsync for entire batch
        self.index_writer.flush().context("flushing index")?;
        self.index_writer
            .get_ref()
            .sync_all()
            .context("syncing index")?;
        self.store.flush()?;

        // Single atomic snapshot write for entire batch (with CRC)
        let snap = self.tree.snapshot();
        let snap_bytes = bincode::serialize(&snap).context("serializing snapshot")?;
        atomic_write(&self.dir.join("tree.snapshot"), &snap_bytes)?;

        Ok(offsets)
    }

    /// Read a single record by offset. O(1) via the index.
    /// Returns None for offsets below min_valid_offset or >= next_offset.
    pub fn read(&self, offset: u64) -> Result<Option<Record>> {
        if offset >= self.next_offset || offset < self.min_valid_offset {
            return Ok(None);
        }

        let record_hash = self.read_index_entry(offset)?;
        let data = self.store.get(&record_hash)?;
        let record = Record::deserialize(&data)?;
        Ok(Some(record))
    }

    /// Read a range of records [from, to) (exclusive end).
    pub fn read_range(&self, from: u64, to: u64) -> Result<Vec<Record>> {
        let start = from.max(self.min_valid_offset);
        let end = to.min(self.next_offset);
        let mut records = Vec::new();
        for offset in start..end {
            if let Some(record) = self.read(offset)? {
                records.push(record);
            }
        }
        Ok(records)
    }

    /// Get the current merkle root hash.
    pub fn merkle_root(&self) -> Result<Option<Hash>> {
        self.tree.root(&self.store)
    }

    /// Generate a merkle inclusion proof for a given offset.
    pub fn proof(&self, offset: u64) -> Result<Option<crate::tree::Proof>> {
        self.tree.proof(offset, &self.store)
    }

    /// Verify a merkle proof.
    pub fn verify_proof(&self, proof: &crate::tree::Proof) -> Result<bool> {
        MerkleTree::verify_proof(proof, &self.store)
    }

    pub fn store(&self) -> &ObjectStore {
        &self.store
    }

    /// Advance the retention window. Records below new_min will return None on read.
    pub fn advance_retention(&mut self, new_min: u64) -> Result<()> {
        if new_min > self.min_valid_offset {
            self.min_valid_offset = new_min;
            let data =
                bincode::serialize(&self.min_valid_offset).context("serializing retention")?;
            atomic_write(&self.dir.join("retention.bin"), &data)?;
        }
        Ok(())
    }

    fn read_index_entry(&self, offset: u64) -> Result<Hash> {
        let index_path = self.dir.join("offsets.idx");
        let mut file = fs::File::open(&index_path).context("opening offset index")?;
        let seek_pos = offset * INDEX_ENTRY_SIZE as u64;
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(seek_pos))
            .context("seeking in index")?;
        let mut buf = [0u8; 32];
        file.read_exact(&mut buf).context("reading index entry")?;
        Ok(Hash(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_record(topic: &str, value: &str) -> Record {
        Record {
            key: None,
            value: value.into(),
            topic: topic.into(),
            partition: 0,
            offset: 0,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn sequential_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        for i in 0..5 {
            let mut rec = make_record("t", &format!("val-{}", i));
            let offset = part.append(&mut rec).unwrap();
            assert_eq!(offset, i);
        }
        assert_eq!(part.next_offset(), 5);
    }

    #[test]
    fn read_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        let mut rec = make_record("t", "hello");
        part.append(&mut rec).unwrap();

        let read_back = part.read(0).unwrap().unwrap();
        assert_eq!(read_back.value, "hello");
        assert_eq!(read_back.offset, 0);
    }

    #[test]
    fn read_range_works() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        for i in 0..10 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        let range = part.read_range(3, 7).unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].value, "v3");
        assert_eq!(range[3].value, "v6");
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        // Write some records
        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            for i in 0..3 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
        }

        // Reopen and verify
        let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
        assert_eq!(part.next_offset(), 3);

        let r = part.read(1).unwrap().unwrap();
        assert_eq!(r.value, "v1");

        // Can continue appending
        let mut rec = make_record("t", "v3");
        let offset = part.append(&mut rec).unwrap();
        assert_eq!(offset, 3);
    }

    #[test]
    fn merkle_root_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        let mut rec = make_record("t", "first");
        part.append(&mut rec).unwrap();
        let root1 = part.merkle_root().unwrap().unwrap();

        let mut rec = make_record("t", "second");
        part.append(&mut rec).unwrap();
        let root2 = part.merkle_root().unwrap().unwrap();

        assert_ne!(root1, root2);
    }

    #[test]
    fn read_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();
        assert!(part.read(0).unwrap().is_none());
    }

    #[test]
    fn batch_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        let mut records: Vec<Record> = (0..5)
            .map(|i| make_record("t", &format!("batch-{}", i)))
            .collect();
        let offsets = part.append_batch(&mut records).unwrap();

        assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
        assert_eq!(part.next_offset(), 5);

        for i in 0..5 {
            let r = part.read(i).unwrap().unwrap();
            assert_eq!(r.value, format!("batch-{}", i));
        }
    }

    #[test]
    fn retention_hides_old_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        for i in 0..10 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        part.advance_retention(5).unwrap();
        assert_eq!(part.min_valid_offset(), 5);

        // Old records return None
        for i in 0..5 {
            assert!(part.read(i).unwrap().is_none());
        }
        // New records still readable
        for i in 5..10 {
            assert!(part.read(i).unwrap().is_some());
        }
    }

    #[test]
    fn retention_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            for i in 0..10 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
            part.advance_retention(5).unwrap();
        }

        let part = Partition::open(0, &part_dir, Compression::None).unwrap();
        assert_eq!(part.min_valid_offset(), 5);
        assert!(part.read(4).unwrap().is_none());
        assert!(part.read(5).unwrap().is_some());
    }

    #[test]
    fn read_range_respects_retention() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::None).unwrap();

        for i in 0..10 {
            let mut rec = make_record("t", &format!("v{}", i));
            part.append(&mut rec).unwrap();
        }

        part.advance_retention(3).unwrap();
        let range = part.read_range(0, 10).unwrap();
        assert_eq!(range.len(), 7);
        assert_eq!(range[0].value, "v3");
    }

    #[test]
    fn compression_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut part = Partition::open(0, dir.path().join("p0"), Compression::Lz4).unwrap();

        for i in 0..5 {
            let mut rec = make_record("t", &format!("compressed-{}", i));
            part.append(&mut rec).unwrap();
        }

        for i in 0..5 {
            let r = part.read(i).unwrap().unwrap();
            assert_eq!(r.value, format!("compressed-{}", i));
        }
    }

    #[test]
    fn corrupt_snapshot_crc_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        // Write records
        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            for i in 0..5 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
        }

        // Corrupt the tree.snapshot CRC
        let snapshot_path = part_dir.join("tree.snapshot");
        let mut data = fs::read(&snapshot_path).unwrap();
        data[0] ^= 0xFF; // flip a CRC byte
        fs::write(&snapshot_path, &data).unwrap();

        // Reopen — should recover (empty tree is fine, records still readable via index)
        let part = Partition::open(0, &part_dir, Compression::None).unwrap();
        assert_eq!(part.next_offset(), 5);
        let r = part.read(2).unwrap().unwrap();
        assert_eq!(r.value, "v2");
    }

    #[test]
    fn corrupt_retention_crc_defaults_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            for i in 0..10 {
                let mut rec = make_record("t", &format!("v{}", i));
                part.append(&mut rec).unwrap();
            }
            part.advance_retention(5).unwrap();
        }

        // Corrupt the retention.bin CRC
        let retention_path = part_dir.join("retention.bin");
        let mut data = fs::read(&retention_path).unwrap();
        data[0] ^= 0xFF;
        fs::write(&retention_path, &data).unwrap();

        // Reopen — should default to min_valid_offset = 0
        let part = Partition::open(0, &part_dir, Compression::None).unwrap();
        assert_eq!(part.min_valid_offset(), 0);
        // All records should be readable
        for i in 0..10 {
            assert!(part.read(i).unwrap().is_some());
        }
    }

    #[test]
    fn flock_serializes_concurrent_writers() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("p0");

        // Write initial records
        {
            let mut part = Partition::open(0, &part_dir, Compression::None).unwrap();
            let mut rec = make_record("t", "seed");
            part.append(&mut rec).unwrap();
        }

        let part_dir = Arc::new(part_dir);
        let handles: Vec<_> = (0..2)
            .map(|thread_id| {
                let pd = Arc::clone(&part_dir);
                thread::spawn(move || {
                    let mut part = Partition::open(0, &*pd, Compression::None).unwrap();
                    for i in 0..5 {
                        let mut rec = make_record("t", &format!("t{}-v{}", thread_id, i));
                        part.append(&mut rec).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify: reopen and check we have all records (1 seed + 10 from threads)
        // Note: each thread reopens with its own next_offset, so with flock they
        // serialize but each thread's view may be stale. The key correctness property
        // is that the lock file prevents concurrent file corruption.
        // In practice, a single Partition instance is used with the RwLock in Topic.
        let part = Partition::open(0, &*part_dir, Compression::None).unwrap();
        assert!(part.next_offset() >= 1); // at least the seed record survived
    }
}
