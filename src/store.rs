use crate::compression::Compression;
use crate::hash::Hash;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Header size per pack entry: 4 bytes (data_length) + 32 bytes (hash).
const ENTRY_HEADER_SIZE: u64 = 36;

/// Size of one index entry: 32 (hash) + 8 (data_offset) + 4 (data_length).
const IDX_ENTRY_SIZE: usize = 44;

/// Size of the index file header: 8 bytes (pack_size as u64 LE).
const IDX_HEADER_SIZE: usize = 8;

struct PackInner {
    index: HashMap<Hash, (u64, u32)>, // hash → (data_offset, data_length)
    file: File,
    write_pos: u64,
}

/// Content-addressed pack-file store using SHA-256 hashes.
///
/// All objects are stored in a single append-only file (`objects.pack`) with format:
/// `[4 bytes: data_length as u32 LE][32 bytes: SHA-256 hash][data_length bytes: compressed data]`
///
/// Objects are hashed BEFORE compression so merkle proofs remain valid
/// regardless of compression setting.
pub struct ObjectStore {
    inner: Mutex<PackInner>,
    compression: Compression,
    idx_path: PathBuf,
}

impl ObjectStore {
    /// Open or create a pack-file object store.
    ///
    /// If `pack_path` exists, its entries are scanned to rebuild the in-memory index.
    /// If `legacy_dir` exists (old per-file layout), objects are migrated into the
    /// pack file and the directory is removed.
    pub fn open(
        pack_path: impl Into<PathBuf>,
        legacy_dir: impl Into<PathBuf>,
        compression: Compression,
    ) -> Result<Self> {
        let pack_path = pack_path.into();
        let legacy_dir = legacy_dir.into();
        let idx_path = pack_path.with_extension("idx");

        let pack_exists = pack_path.exists();
        let legacy_exists = legacy_dir.is_dir();

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&pack_path)
            .context("opening pack file")?;

        let mut index = HashMap::new();
        let mut write_pos: u64 = 0;

        if pack_exists {
            let pack_len = file.metadata().context("reading pack metadata")?.len();
            // Try loading persisted index first
            if let Some(loaded) = Self::load_index(&idx_path, pack_len)? {
                index = loaded.0;
                write_pos = loaded.1;
            } else {
                // Index missing or stale — fall back to full scan
                write_pos = Self::scan_pack(&mut file, &mut index)?;
            }
        }

        if legacy_exists && !pack_exists {
            // Migrate from old per-file layout
            write_pos =
                Self::migrate_legacy(&legacy_dir, &mut file, &mut index, write_pos)?;
            file.sync_all().context("syncing after migration")?;
            fs::remove_dir_all(&legacy_dir).context("removing legacy objects dir")?;
        }

        Ok(ObjectStore {
            inner: Mutex::new(PackInner {
                index,
                file,
                write_pos,
            }),
            compression,
            idx_path,
        })
    }

    /// Store bytes, returning their content hash. Idempotent — if the object
    /// already exists, this is a no-op.
    /// Hash is computed on uncompressed data; storage uses compressed form.
    pub fn put(&self, data: &[u8]) -> Result<Hash> {
        let hash = Hash::digest(data);
        let compressed = self.compression.compress(data);

        let mut inner = self.inner.lock().unwrap();
        if inner.index.contains_key(&hash) {
            return Ok(hash);
        }

        let data_len = compressed.len() as u32;
        let pos = inner.write_pos;

        inner
            .file
            .seek(SeekFrom::Start(pos))
            .context("seeking to write position")?;
        inner
            .file
            .write_all(&data_len.to_le_bytes())
            .context("writing data length")?;
        inner
            .file
            .write_all(&hash.0)
            .context("writing hash")?;
        inner
            .file
            .write_all(&compressed)
            .context("writing compressed data")?;

        let data_offset = pos + ENTRY_HEADER_SIZE;
        inner.index.insert(hash, (data_offset, data_len));
        inner.write_pos = data_offset + data_len as u64;

        Ok(hash)
    }

    /// Retrieve bytes by hash. Returns decompressed data.
    pub fn get(&self, hash: &Hash) -> Result<Vec<u8>> {
        let (data_offset, data_len) = {
            let inner = self.inner.lock().unwrap();
            *inner
                .index
                .get(hash)
                .with_context(|| format!("object not found: {}", hash))?
        };

        let mut buf = vec![0u8; data_len as usize];
        {
            let mut inner = self.inner.lock().unwrap();
            inner
                .file
                .seek(SeekFrom::Start(data_offset))
                .with_context(|| format!("seeking to object {}", hash))?;
            inner
                .file
                .read_exact(&mut buf)
                .with_context(|| format!("reading object {}", hash))?;
        }

        Compression::decompress(&buf).with_context(|| format!("decompressing object {}", hash))
    }

    /// Check if an object exists.
    pub fn exists(&self, hash: &Hash) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.index.contains_key(hash)
    }

    /// Flush the pack file and persist the index to disk.
    pub fn flush(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        inner.file.sync_all().context("syncing pack file")?;
        Self::save_index(&self.idx_path, &inner.index, inner.write_pos)?;
        Ok(())
    }

    /// Scan a pack file sequentially, rebuilding the in-memory index.
    /// Returns the write position (end of last valid entry).
    /// Truncates the file at the last valid entry if a partial write is detected.
    fn scan_pack(file: &mut File, index: &mut HashMap<Hash, (u64, u32)>) -> Result<u64> {
        let file_len = file.metadata().context("reading pack metadata")?.len();
        let mut pos: u64 = 0;
        let mut last_good_pos: u64 = 0;

        file.seek(SeekFrom::Start(0))
            .context("seeking to start of pack")?;

        loop {
            if pos >= file_len {
                break;
            }

            // Need at least 4 bytes for data_length
            if pos + 4 > file_len {
                break;
            }

            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let data_len = u32::from_le_bytes(len_buf);

            // Need 32 bytes for hash
            if pos + 4 + 32 > file_len {
                break;
            }

            let mut hash_buf = [0u8; 32];
            if file.read_exact(&mut hash_buf).is_err() {
                break;
            }

            // Need data_len bytes for data
            let entry_end = pos + ENTRY_HEADER_SIZE + data_len as u64;
            if entry_end > file_len {
                break;
            }

            // Skip over the data
            if file.seek(SeekFrom::Start(entry_end)).is_err() {
                break;
            }

            let hash = Hash(hash_buf);
            let data_offset = pos + ENTRY_HEADER_SIZE;
            index.insert(hash, (data_offset, data_len));

            last_good_pos = entry_end;
            pos = entry_end;
        }

        // Truncate any partial entry at the end
        if last_good_pos < file_len {
            file.set_len(last_good_pos)
                .context("truncating partial entry")?;
        }

        Ok(last_good_pos)
    }

    /// Load the persisted index file. Returns None if the file doesn't exist,
    /// is too small, or if pack_size doesn't match the actual pack file length
    /// (indicating a crash between pack write and index write).
    fn load_index(
        idx_path: &Path,
        pack_len: u64,
    ) -> Result<Option<(HashMap<Hash, (u64, u32)>, u64)>> {
        let data = match fs::read(idx_path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).context("reading index file"),
        };

        if data.len() < IDX_HEADER_SIZE {
            return Ok(None);
        }

        let stored_pack_size = u64::from_le_bytes(data[..8].try_into().unwrap());
        if stored_pack_size != pack_len {
            // Pack file changed since index was written — stale
            return Ok(None);
        }

        let entry_bytes = &data[IDX_HEADER_SIZE..];
        if entry_bytes.len() % IDX_ENTRY_SIZE != 0 {
            return Ok(None);
        }

        let n = entry_bytes.len() / IDX_ENTRY_SIZE;
        let mut index = HashMap::with_capacity(n);

        for i in 0..n {
            let base = i * IDX_ENTRY_SIZE;
            let mut hash_buf = [0u8; 32];
            hash_buf.copy_from_slice(&entry_bytes[base..base + 32]);
            let data_offset =
                u64::from_le_bytes(entry_bytes[base + 32..base + 40].try_into().unwrap());
            let data_len =
                u32::from_le_bytes(entry_bytes[base + 40..base + 44].try_into().unwrap());
            index.insert(Hash(hash_buf), (data_offset, data_len));
        }

        Ok(Some((index, stored_pack_size)))
    }

    /// Atomically write the index file (temp + fsync + rename).
    fn save_index(
        idx_path: &Path,
        index: &HashMap<Hash, (u64, u32)>,
        pack_size: u64,
    ) -> Result<()> {
        let tmp = idx_path.with_extension("idx.tmp");
        let mut buf = Vec::with_capacity(IDX_HEADER_SIZE + index.len() * IDX_ENTRY_SIZE);

        buf.extend_from_slice(&pack_size.to_le_bytes());
        for (hash, &(data_offset, data_len)) in index {
            buf.extend_from_slice(&hash.0);
            buf.extend_from_slice(&data_offset.to_le_bytes());
            buf.extend_from_slice(&data_len.to_le_bytes());
        }

        let mut f = File::create(&tmp).context("creating temp index file")?;
        f.write_all(&buf).context("writing index")?;
        f.sync_all().context("syncing index file")?;
        fs::rename(&tmp, idx_path).context("renaming index file")?;
        Ok(())
    }

    /// Migrate objects from the legacy per-file layout into the pack file.
    fn migrate_legacy(
        legacy_dir: &Path,
        file: &mut File,
        index: &mut HashMap<Hash, (u64, u32)>,
        mut write_pos: u64,
    ) -> Result<u64> {
        // Walk objects/{prefix}/{suffix}
        let entries = fs::read_dir(legacy_dir).context("reading legacy objects dir")?;
        for prefix_entry in entries {
            let prefix_entry = prefix_entry.context("reading prefix dir entry")?;
            let prefix_path = prefix_entry.path();
            if !prefix_path.is_dir() {
                continue;
            }
            let prefix_name = match prefix_path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            let suffix_entries =
                fs::read_dir(&prefix_path).context("reading suffix dir entries")?;
            for suffix_entry in suffix_entries {
                let suffix_entry = suffix_entry.context("reading suffix entry")?;
                let suffix_path = suffix_entry.path();
                if !suffix_path.is_file() {
                    continue;
                }
                let suffix_name = match suffix_path.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };

                // Skip temp files
                if suffix_name.starts_with(".tmp-") {
                    continue;
                }

                let hex_str = format!("{}{}", prefix_name, suffix_name);
                let hash = match Hash::from_hex(&hex_str) {
                    Ok(h) => h,
                    Err(_) => continue,
                };

                if index.contains_key(&hash) {
                    continue;
                }

                let compressed = fs::read(&suffix_path)
                    .with_context(|| format!("reading legacy object {}", hex_str))?;
                let data_len = compressed.len() as u32;

                file.seek(SeekFrom::Start(write_pos))
                    .context("seeking for migration write")?;
                file.write_all(&data_len.to_le_bytes())
                    .context("writing migrated data length")?;
                file.write_all(&hash.0)
                    .context("writing migrated hash")?;
                file.write_all(&compressed)
                    .context("writing migrated data")?;

                let data_offset = write_pos + ENTRY_HEADER_SIZE;
                index.insert(hash, (data_offset, data_len));
                write_pos = data_offset + data_len as u64;
            }
        }

        Ok(write_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_store(dir: &Path, compression: Compression) -> ObjectStore {
        let pack_path = dir.join("objects.pack");
        let legacy_dir = dir.join("objects");
        ObjectStore::open(pack_path, legacy_dir, compression).unwrap()
    }

    #[test]
    fn put_get_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let data = b"hello merkql";
        let hash = store.put(data).unwrap();
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn put_get_round_trip_lz4() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::Lz4);

        let data = b"hello merkql compressed";
        let hash = store.put(data).unwrap();
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn idempotent_put() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let data = b"same content";
        let h1 = store.put(data).unwrap();
        let h2 = store.put(data).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn get_nonexistent_fails() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let hash = Hash::digest(b"not stored");
        assert!(store.get(&hash).is_err());
    }

    #[test]
    fn exists_check() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path(), Compression::None);

        let hash = Hash::digest(b"check me");
        assert!(!store.exists(&hash));

        store.put(b"check me").unwrap();
        assert!(store.exists(&hash));
    }

    #[test]
    fn hash_same_regardless_of_compression() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("none")).unwrap();
        fs::create_dir_all(dir.path().join("lz4")).unwrap();
        let store_none = open_store(&dir.path().join("none"), Compression::None);
        let store_lz4 = open_store(&dir.path().join("lz4"), Compression::Lz4);

        let data = b"content to hash";
        let h1 = store_none.put(data).unwrap();
        let h2 = store_lz4.put(data).unwrap();
        assert_eq!(h1, h2, "hash must be computed on uncompressed data");
    }

    #[test]
    fn mixed_compression_read() {
        let dir = tempfile::tempdir().unwrap();

        // Write with None
        {
            let store = open_store(dir.path(), Compression::None);
            store.put(b"written uncompressed").unwrap();
        }

        // Read with Lz4 store — should still work because decompress reads the marker
        let store = open_store(dir.path(), Compression::Lz4);
        let hash = Hash::digest(b"written uncompressed");
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, b"written uncompressed");
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let hash;

        {
            let store = open_store(dir.path(), Compression::None);
            hash = store.put(b"persistent data").unwrap();
        }

        // Reopen and verify
        let store = open_store(dir.path(), Compression::None);
        assert!(store.exists(&hash));
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, b"persistent data");
    }

    #[test]
    fn crash_recovery_truncates_partial() {
        let dir = tempfile::tempdir().unwrap();
        let pack_path = dir.path().join("objects.pack");
        let hash;

        {
            let store = open_store(dir.path(), Compression::None);
            hash = store.put(b"good data").unwrap();
            store.flush().unwrap();
        }

        // Append garbage (simulating a partial write / crash)
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(&pack_path)
                .unwrap();
            f.write_all(&[0xFF; 10]).unwrap(); // partial entry
        }

        // Reopen — should recover the good data and truncate the garbage
        let store = open_store(dir.path(), Compression::None);
        assert!(store.exists(&hash));
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, b"good data");
    }

    #[test]
    fn legacy_migration() {
        let dir = tempfile::tempdir().unwrap();
        let objects_dir = dir.path().join("objects");

        // Create legacy layout: objects/{prefix}/{suffix}
        let data = b"legacy object data";
        let compressed = Compression::None.compress(data);
        let hash = Hash::digest(data);

        let prefix_dir = objects_dir.join(hash.prefix());
        fs::create_dir_all(&prefix_dir).unwrap();
        fs::write(prefix_dir.join(hash.suffix()), &compressed).unwrap();

        // Open should migrate
        let store = open_store(dir.path(), Compression::None);

        // Legacy dir should be removed
        assert!(!objects_dir.exists());

        // Data should be accessible
        assert!(store.exists(&hash));
        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }
}
