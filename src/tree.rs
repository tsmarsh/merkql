use crate::hash::Hash;
use crate::node::Node;
use crate::store::ObjectStore;
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Merkle inclusion proof: a path from a leaf to the root.
#[derive(Debug, Clone)]
pub struct Proof {
    pub leaf_hash: Hash,
    pub siblings: Vec<(Hash, Side)>,
    pub root: Hash,
}

#[derive(Debug, Clone, Copy)]
pub enum Side {
    Left,
    Right,
}

/// Incremental merkle tree using a pending-roots stack (binary carry chain).
///
/// Each entry in `pending` is `(height, hash)`. When two entries have the same
/// height they merge into a branch one level up — exactly like binary addition.
pub struct MerkleTree {
    pending: Vec<(u32, Hash, u64, u64)>, // (height, hash, min_offset, max_offset)
    count: u64,
}

/// Snapshot for persistence/restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeSnapshot {
    pub pending: Vec<(u32, Hash, u64, u64)>,
    pub count: u64,
}

impl Default for MerkleTree {
    fn default() -> Self {
        Self::new()
    }
}

impl MerkleTree {
    pub fn new() -> Self {
        MerkleTree {
            pending: Vec::new(),
            count: 0,
        }
    }

    pub fn from_snapshot(snapshot: TreeSnapshot) -> Self {
        MerkleTree {
            pending: snapshot.pending,
            count: snapshot.count,
        }
    }

    pub fn snapshot(&self) -> TreeSnapshot {
        TreeSnapshot {
            pending: self.pending.clone(),
            count: self.count,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    /// Append a leaf node to the tree.
    /// The leaf node must already be stored in the object store.
    pub fn append(&mut self, record_hash: Hash, offset: u64, store: &ObjectStore) -> Result<Hash> {
        let leaf = Node::Leaf {
            record_hash,
            offset,
        };
        let leaf_bytes = leaf.serialize();
        let leaf_hash = store.put(&leaf_bytes)?;

        let mut current_hash = leaf_hash;
        let mut current_height: u32 = 0;
        let mut min_offset = offset;
        let max_offset = offset;

        // Merge with pending entries at the same height (binary carry)
        while let Some(&(h, _, _, _)) = self.pending.last() {
            if h != current_height {
                break;
            }
            let (_, left_hash, left_min, _) = self.pending.pop().unwrap();
            min_offset = left_min;

            let branch = Node::Branch {
                left: left_hash,
                right: current_hash,
                offset_range: (min_offset, max_offset),
            };
            let branch_bytes = branch.serialize();
            current_hash = store.put(&branch_bytes)?;
            current_height += 1;
        }

        self.pending
            .push((current_height, current_hash, min_offset, max_offset));
        self.count += 1;
        Ok(current_hash)
    }

    /// Get the current merkle root. If there are multiple pending roots,
    /// conceptually merges them right-to-left (rightmost is the newest).
    pub fn root(&self, store: &ObjectStore) -> Result<Option<Hash>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        if self.pending.len() == 1 {
            return Ok(Some(self.pending[0].1));
        }

        // Merge all pending roots right-to-left
        let mut iter = self.pending.iter().rev();
        let &(_, mut current, _, max_off) = iter.next().unwrap();
        for &(_, left_hash, min_off, _) in iter {
            let branch = Node::Branch {
                left: left_hash,
                right: current,
                offset_range: (min_off, max_off),
            };
            let bytes = branch.serialize();
            current = store.put(&bytes)?;
        }
        Ok(Some(current))
    }

    /// Generate an inclusion proof for a given offset.
    /// Walks the tree from root to the leaf at `offset`.
    pub fn proof(&self, offset: u64, store: &ObjectStore) -> Result<Option<Proof>> {
        let root = match self.root(store)? {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut siblings = Vec::new();
        let leaf_hash = collect_proof(root, offset, &mut siblings, store)?;

        match leaf_hash {
            Some(lh) => Ok(Some(Proof {
                leaf_hash: lh,
                siblings,
                root,
            })),
            None => Ok(None),
        }
    }

    /// Verify a proof against a claimed root. Pure computation — no I/O needed
    /// beyond reading the sibling nodes to reconstruct branch hashes.
    ///
    /// # Examples
    ///
    /// ```
    /// use merkql::broker::{Broker, BrokerConfig};
    /// use merkql::record::ProducerRecord;
    /// use merkql::tree::MerkleTree;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    /// let producer = Broker::producer(&broker);
    /// producer.send(&ProducerRecord::new("t", None, "data")).unwrap();
    ///
    /// let topic = broker.topic("t").unwrap();
    /// let part_arc = topic.partition(0).unwrap();
    /// let partition = part_arc.read().unwrap();
    /// let proof = partition.proof(0).unwrap().unwrap();
    /// assert!(MerkleTree::verify_proof(&proof, partition.store()).unwrap());
    /// ```
    pub fn verify_proof(proof: &Proof, store: &ObjectStore) -> Result<bool> {
        let mut current = proof.leaf_hash;

        for (sibling_hash, side) in &proof.siblings {
            // We need to reconstruct the branch node to get a deterministic hash.
            // But we need the offset_range. We can read the branch from the store
            // to get it, or we can just hash the way we built it.
            // Since nodes are in the store, read the parent to verify.
            let (left, right) = match side {
                Side::Right => (current, *sibling_hash),
                Side::Left => (*sibling_hash, current),
            };

            // Reconstruct the branch by reading the expected parent from store
            // and verifying it matches
            let left_data = store.get(&left)?;
            let left_node = Node::deserialize(&left_data)?;
            let right_data = store.get(&right)?;
            let right_node = Node::deserialize(&right_data)?;

            let min_off = match &left_node {
                Node::Leaf { offset, .. } => *offset,
                Node::Branch { offset_range, .. } => offset_range.0,
            };
            let max_off = match &right_node {
                Node::Leaf { offset, .. } => *offset,
                Node::Branch { offset_range, .. } => offset_range.1,
            };

            let branch = Node::Branch {
                left,
                right,
                offset_range: (min_off, max_off),
            };
            current = Hash::digest(&branch.serialize());
        }

        Ok(current == proof.root)
    }
}

fn collect_proof(
    node_hash: Hash,
    target_offset: u64,
    siblings: &mut Vec<(Hash, Side)>,
    store: &ObjectStore,
) -> Result<Option<Hash>> {
    let data = store.get(&node_hash)?;
    let node = Node::deserialize(&data)?;

    match node {
        Node::Leaf { offset, .. } => {
            if offset == target_offset {
                Ok(Some(node_hash))
            } else {
                Ok(None)
            }
        }
        Node::Branch {
            left,
            right,
            offset_range,
        } => {
            if target_offset < offset_range.0 || target_offset > offset_range.1 {
                return Ok(None);
            }

            if let Some(lh) = collect_proof(left, target_offset, siblings, store)? {
                siblings.push((right, Side::Right));
                return Ok(Some(lh));
            }
            if let Some(lh) = collect_proof(right, target_offset, siblings, store)? {
                siblings.push((left, Side::Left));
                return Ok(Some(lh));
            }
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> (tempfile::TempDir, ObjectStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::open(
            dir.path().join("objects.pack"),
            dir.path().join("objects"),
            crate::compression::Compression::None,
        )
        .unwrap();
        (dir, store)
    }

    #[test]
    fn single_leaf() {
        let (_dir, store) = test_store();
        let mut tree = MerkleTree::new();
        let rh = Hash::digest(b"record-0");
        tree.append(rh, 0, &store).unwrap();
        assert_eq!(tree.count(), 1);
        let root = tree.root(&store).unwrap();
        assert!(root.is_some());
    }

    #[test]
    fn two_leaves_merge() {
        let (_dir, store) = test_store();
        let mut tree = MerkleTree::new();
        tree.append(Hash::digest(b"r0"), 0, &store).unwrap();
        tree.append(Hash::digest(b"r1"), 1, &store).unwrap();
        assert_eq!(tree.count(), 2);
        // Two leaves should merge into a single branch at height 1
        assert_eq!(tree.pending.len(), 1);
        assert_eq!(tree.pending[0].0, 1); // height
    }

    #[test]
    fn three_leaves_pending() {
        let (_dir, store) = test_store();
        let mut tree = MerkleTree::new();
        tree.append(Hash::digest(b"r0"), 0, &store).unwrap();
        tree.append(Hash::digest(b"r1"), 1, &store).unwrap();
        tree.append(Hash::digest(b"r2"), 2, &store).unwrap();
        assert_eq!(tree.count(), 3);
        // Binary carry: 3 = 11 in binary → two pending roots
        assert_eq!(tree.pending.len(), 2);
    }

    #[test]
    fn four_leaves_single_root() {
        let (_dir, store) = test_store();
        let mut tree = MerkleTree::new();
        for i in 0..4 {
            tree.append(Hash::digest(format!("r{}", i).as_bytes()), i, &store)
                .unwrap();
        }
        assert_eq!(tree.count(), 4);
        // 4 = 100 in binary → single pending root at height 2
        assert_eq!(tree.pending.len(), 1);
        assert_eq!(tree.pending[0].0, 2);
    }

    #[test]
    fn proof_validity() {
        let (_dir, store) = test_store();
        let mut tree = MerkleTree::new();
        for i in 0..8 {
            tree.append(Hash::digest(format!("r{}", i).as_bytes()), i, &store)
                .unwrap();
        }

        for offset in 0..8 {
            let proof = tree.proof(offset, &store).unwrap().unwrap();
            assert!(MerkleTree::verify_proof(&proof, &store).unwrap());
        }
    }

    #[test]
    fn proof_nonexistent_offset() {
        let (_dir, store) = test_store();
        let mut tree = MerkleTree::new();
        tree.append(Hash::digest(b"r0"), 0, &store).unwrap();
        let proof = tree.proof(99, &store).unwrap();
        assert!(proof.is_none());
    }

    #[test]
    fn snapshot_round_trip() {
        let (_dir, store) = test_store();
        let mut tree = MerkleTree::new();
        for i in 0..5 {
            tree.append(Hash::digest(format!("r{}", i).as_bytes()), i, &store)
                .unwrap();
        }

        let snap = tree.snapshot();
        let bytes = bincode::serialize(&snap).unwrap();
        let restored_snap: TreeSnapshot = bincode::deserialize(&bytes).unwrap();
        let restored = MerkleTree::from_snapshot(restored_snap);

        assert_eq!(tree.count(), restored.count());
        assert_eq!(tree.root(&store).unwrap(), restored.root(&store).unwrap());
    }

    #[test]
    fn root_changes_with_each_append() {
        let (_dir, store) = test_store();
        let mut tree = MerkleTree::new();
        let mut roots = Vec::new();

        for i in 0..5 {
            tree.append(Hash::digest(format!("r{}", i).as_bytes()), i, &store)
                .unwrap();
            roots.push(tree.root(&store).unwrap().unwrap());
        }

        // All roots should be distinct
        for i in 0..roots.len() {
            for j in (i + 1)..roots.len() {
                assert_ne!(roots[i], roots[j]);
            }
        }
    }
}
