use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// Compression mode for stored objects.
/// A 1-byte marker at the start of every stored object identifies the compression:
/// 0x00 = None, 0x01 = LZ4.
/// This allows mixed-mode reads — a broker can switch compression without corrupting existing data.
///
/// # Examples
///
/// ```
/// use merkql::compression::Compression;
/// use merkql::broker::BrokerConfig;
///
/// let config = BrokerConfig {
///     compression: Compression::Lz4,
///     ..BrokerConfig::new("/tmp/merkql-compression-example")
/// };
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compression {
    #[default]
    None,
    Lz4,
}

const MARKER_NONE: u8 = 0x00;
const MARKER_LZ4: u8 = 0x01;

impl Compression {
    /// Compress data, prepending a 1-byte compression marker.
    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            Compression::None => {
                let mut out = Vec::with_capacity(1 + data.len());
                out.push(MARKER_NONE);
                out.extend_from_slice(data);
                out
            }
            Compression::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(data);
                let mut out = Vec::with_capacity(1 + compressed.len());
                out.push(MARKER_LZ4);
                out.extend_from_slice(&compressed);
                out
            }
        }
    }

    /// Decompress data by reading the 1-byte marker and decompressing accordingly.
    /// This is independent of the `self` compression mode — it reads the marker from the data.
    pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            bail!("empty compressed data");
        }
        match data[0] {
            MARKER_NONE => Ok(data[1..].to_vec()),
            MARKER_LZ4 => {
                lz4_flex::decompress_size_prepended(&data[1..]).context("lz4 decompression failed")
            }
            marker => bail!("unknown compression marker: 0x{:02x}", marker),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_round_trip() {
        let data = b"hello merkql";
        let compressed = Compression::None.compress(data);
        assert_eq!(compressed[0], MARKER_NONE);
        let decompressed = Compression::decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn lz4_round_trip() {
        let data = b"hello merkql, this is some data to compress";
        let compressed = Compression::Lz4.compress(data);
        assert_eq!(compressed[0], MARKER_LZ4);
        let decompressed = Compression::decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn mixed_mode_reads() {
        let data = b"same data, different compression";
        let c_none = Compression::None.compress(data);
        let c_lz4 = Compression::Lz4.compress(data);

        // Both should decompress correctly regardless of which Compression instance calls decompress
        assert_eq!(Compression::decompress(&c_none).unwrap(), data);
        assert_eq!(Compression::decompress(&c_lz4).unwrap(), data);
    }

    #[test]
    fn empty_data() {
        let data = b"";
        let c = Compression::Lz4.compress(data);
        let d = Compression::decompress(&c).unwrap();
        assert_eq!(d, data);
    }

    #[test]
    fn large_data() {
        let data = vec![42u8; 1_000_000];
        let compressed = Compression::Lz4.compress(&data);
        // LZ4 should compress repetitive data well
        assert!(compressed.len() < data.len());
        let decompressed = Compression::decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn invalid_marker() {
        let data = vec![0xFF, 1, 2, 3];
        assert!(Compression::decompress(&data).is_err());
    }

    #[test]
    fn empty_input_errors() {
        assert!(Compression::decompress(&[]).is_err());
    }
}
