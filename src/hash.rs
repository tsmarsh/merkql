use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    pub fn digest(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        Hash(hasher.finalize().into())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn from_hex(s: &str) -> anyhow::Result<Self> {
        let bytes = hex::decode(s)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid hash length"))?;
        Ok(Hash(arr))
    }

    /// First 2 hex chars — used as directory prefix for object storage.
    pub fn prefix(&self) -> String {
        format!("{:02x}", self.0[0])
    }

    /// Remaining 62 hex chars — used as filename within prefix dir.
    pub fn suffix(&self) -> String {
        hex::encode(&self.0[1..])
    }

}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", &self.to_hex()[..12])
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, ::anyhow::Error> {
        if !s.len().is_multiple_of(2) {
            ::anyhow::bail!("odd-length hex string");
        }
        (0..s.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&s[i..i + 2], 16)
                    .map_err(|e| ::anyhow::anyhow!("invalid hex: {}", e))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_determinism() {
        let h1 = Hash::digest(b"hello world");
        let h2 = Hash::digest(b"hello world");
        assert_eq!(h1, h2);

        let h3 = Hash::digest(b"hello world!");
        assert_ne!(h1, h3);
    }

    #[test]
    fn hex_round_trip() {
        let h = Hash::digest(b"test data");
        let hex_str = h.to_hex();
        assert_eq!(hex_str.len(), 64);
        let h2 = Hash::from_hex(&hex_str).unwrap();
        assert_eq!(h, h2);
    }

    #[test]
    fn prefix_suffix_splitting() {
        let h = Hash::digest(b"test");
        let prefix = h.prefix();
        let suffix = h.suffix();
        assert_eq!(prefix.len(), 2);
        assert_eq!(suffix.len(), 62);

        // Reconstruct full hex from prefix + suffix
        let reconstructed = Hash::from_hex(&format!("{}{}", prefix, suffix)).unwrap();
        assert_eq!(h, reconstructed);
    }

    #[test]
    fn from_hex_invalid() {
        assert!(Hash::from_hex("not-hex").is_err());
        assert!(Hash::from_hex("abcd").is_err()); // too short
    }
}
