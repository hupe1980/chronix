//! Segment file header and footer structures.
//!
//! Every `.csx` segment file begins with a [`SegmentHeader`] and ends with a
//! [`SegmentFooter`]. The footer contains the `CRC32c` checksum of the entire
//! file (minus the footer itself) for integrity verification.
//!
//! ## Format Versioning
//!
//! The header contains a `version: u16` field (bytes 4–5, after the 4-byte
//! magic `CXSG`). The current format version is [`VERSION`].
//!
//! On read, [`SegmentHeader::from_bytes`] requires `version == VERSION`
//! exactly — a file from a newer writer *or* an older one is refused with
//! [`SegmentError::UnsupportedVersion`].
//!
//! Accepting an older version would be silent corruption rather than
//! compatibility: v2 named its time column `timestamp` and v3 names it
//! `_time`, so a v2 file opened by a v3 reader yields a batch with a column
//! nothing looks for — no error, no rows. Chronix is pre-release and there is
//! no data in the wild, so the reader refuses instead of translating.

use crate::segment::error::{Result, SegmentError};
use crate::segment::to_array;

/// Segment file magic bytes: `CXSG`.
pub const MAGIC: [u8; 4] = *b"CXSG";

/// Current segment format version.
///
/// **v1 is the first format that exists.** The layout changed several times
/// before release; those generations were superseded before anyone could
/// hold one, and their readers were deleted with them, so the count starts
/// at the version that ships rather than at the number of edits it took to
/// get there.
///
/// The version is checked for **equality**, not `<=`. Two layouts can differ
/// in a way no tolerant reader bridges — one that named the time column
/// `timestamp` against one that names it `_time` yields a batch whose time
/// column nothing looks for, which is zero rows and no error — so a file
/// this reader did not write is refused rather than guessed at.
///
/// [`ColumnBlockMeta::validity_length`]: crate::segment::metadata::ColumnBlockMeta::validity_length
pub const VERSION: u16 = 1;

/// Size of the serialized header in bytes.
pub const HEADER_SIZE: usize = 4 + 2 + 2 + 8 + 8 + 8 + 8 + 2 + 4 + 1 + 1;
// magic(4) + version(2) + flags(2) + created_at(8) + min_ts(8) + max_ts(8)
// + row_count(8) + column_count(2) + series_count(4) + compression(1)
// + sort_order(1)

/// Size of the serialized footer in bytes.
pub const FOOTER_SIZE: usize = 8 + 4 + 4 + 4 + 4;
// metadata_offset(8) + row_group_count(4) + checksum(4) + magic(4)

/// Segment file header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Format version.
    pub version: u16,
    /// Flags (reserved for future use).
    pub flags: u16,
    /// Creation timestamp (nanoseconds since epoch).
    pub created_at: i64,
    /// Minimum timestamp across all rows in the segment.
    pub min_timestamp: i64,
    /// Maximum timestamp across all rows in the segment.
    pub max_timestamp: i64,
    /// Total number of rows in the segment.
    pub row_count: u64,
    /// Number of columns (including timestamp).
    pub column_count: u16,
    /// Number of unique series in the segment.
    pub series_count: u32,
    /// Compression codec: 0 = LZ4, 1 = Zstd, 2 = None.
    pub compression: u8,
    /// Sort order: 0 = unsorted, 1 = by series+timestamp.
    pub sort_order: u8,
}

impl SegmentHeader {
    /// Serialize the header to bytes (always [`HEADER_SIZE`] bytes).
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE);
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());
        buf.extend_from_slice(&self.min_timestamp.to_le_bytes());
        buf.extend_from_slice(&self.max_timestamp.to_le_bytes());
        buf.extend_from_slice(&self.row_count.to_le_bytes());
        buf.extend_from_slice(&self.column_count.to_le_bytes());
        buf.extend_from_slice(&self.series_count.to_le_bytes());
        buf.push(self.compression);
        buf.push(self.sort_order);
        buf
    }

    /// Deserialize a header from bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentError::InvalidMagic`] if magic bytes don't match,
    /// or [`SegmentError::UnsupportedVersion`] if version is not supported.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "header too short: expected {HEADER_SIZE}, got {}",
                    data.len()
                ),
            });
        }

        if data[..4] != MAGIC {
            return Err(SegmentError::InvalidMagic);
        }

        let version = u16::from_le_bytes([data[4], data[5]]);
        // Equality, not `<=`: an older segment is a *different* format, and
        // reading one would produce a batch whose time column has the name
        // this version stopped using.
        if version != VERSION {
            return Err(SegmentError::UnsupportedVersion { version });
        }

        let flags = u16::from_le_bytes([data[6], data[7]]);
        let created_at = i64::from_le_bytes(to_array!(data[8..16], "header created_at")?);
        let min_timestamp = i64::from_le_bytes(to_array!(data[16..24], "header min_timestamp")?);
        let max_timestamp = i64::from_le_bytes(to_array!(data[24..32], "header max_timestamp")?);
        let row_count = u64::from_le_bytes(to_array!(data[32..40], "header row_count")?);
        let column_count = u16::from_le_bytes([data[40], data[41]]);
        let series_count = u32::from_le_bytes(to_array!(data[42..46], "header series_count")?);
        let compression = data[46];
        let sort_order = data[47];

        Ok(Self {
            version,
            flags,
            created_at,
            min_timestamp,
            max_timestamp,
            row_count,
            column_count,
            series_count,
            compression,
            sort_order,
        })
    }
}

/// Segment file footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFooter {
    /// Byte offset where column metadata section begins.
    pub metadata_offset: u64,
    /// Number of row groups in the segment.
    pub row_group_count: u32,
    /// `CRC32c` checksum of the entire file excluding this footer.
    pub checksum: u32,
    /// Independent `CRC32c` checksum of the metadata section.
    /// Allows metadata validation without re-checksumming the entire file.
    pub metadata_checksum: u32,
}

impl SegmentFooter {
    /// Serialize the footer to bytes (always [`FOOTER_SIZE`] bytes).
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);
        buf.extend_from_slice(&self.metadata_offset.to_le_bytes());
        buf.extend_from_slice(&self.row_group_count.to_le_bytes());
        buf.extend_from_slice(&self.checksum.to_le_bytes());
        buf.extend_from_slice(&self.metadata_checksum.to_le_bytes());
        buf.extend_from_slice(&MAGIC);
        buf
    }

    /// Deserialize a footer from bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentError::InvalidMagic`] if trailing magic bytes don't
    /// match, or [`SegmentError::CorruptFile`] if the data is too short.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < FOOTER_SIZE {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "footer too short: expected {FOOTER_SIZE}, got {}",
                    data.len()
                ),
            });
        }

        let metadata_offset = u64::from_le_bytes(to_array!(data[..8], "footer metadata_offset")?);
        let row_group_count = u32::from_le_bytes(to_array!(data[8..12], "footer row_group_count")?);
        let checksum = u32::from_le_bytes(to_array!(data[12..16], "footer checksum")?);
        let metadata_checksum =
            u32::from_le_bytes(to_array!(data[16..20], "footer metadata_checksum")?);

        if data[20..24] != MAGIC {
            return Err(SegmentError::InvalidMagic);
        }

        Ok(Self {
            metadata_offset,
            row_group_count,
            checksum,
            metadata_checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let header = SegmentHeader {
            version: VERSION,
            flags: 0,
            created_at: 1_700_000_000_000_000_000,
            min_timestamp: 1_000_000,
            max_timestamp: 2_000_000,
            row_count: 10_000,
            column_count: 5,
            series_count: 42,
            compression: 0,
            sort_order: 1,
        };
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), HEADER_SIZE);
        let recovered = SegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(header, recovered);
    }

    #[test]
    fn header_invalid_magic() {
        let mut bytes = SegmentHeader {
            version: VERSION,
            flags: 0,
            created_at: 0,
            min_timestamp: 0,
            max_timestamp: 0,
            row_count: 0,
            column_count: 0,
            series_count: 0,
            compression: 0,
            sort_order: 0,
        }
        .to_bytes();
        bytes[0] = b'X';
        assert!(matches!(
            SegmentHeader::from_bytes(&bytes),
            Err(SegmentError::InvalidMagic)
        ));
    }

    #[test]
    fn footer_roundtrip() {
        let footer = SegmentFooter {
            metadata_offset: 1024,
            row_group_count: 3,
            checksum: 0xDEAD_BEEF,
            metadata_checksum: 0xCAFE_BABE,
        };
        let bytes = footer.to_bytes();
        assert_eq!(bytes.len(), FOOTER_SIZE);
        let recovered = SegmentFooter::from_bytes(&bytes).unwrap();
        assert_eq!(footer, recovered);
    }

    #[test]
    fn footer_invalid_magic() {
        let mut bytes = SegmentFooter {
            metadata_offset: 0,
            row_group_count: 0,
            checksum: 0,
            metadata_checksum: 0,
        }
        .to_bytes();
        let len = bytes.len();
        bytes[len - 1] = b'X';
        assert!(matches!(
            SegmentFooter::from_bytes(&bytes),
            Err(SegmentError::InvalidMagic)
        ));
    }

    #[test]
    fn header_too_short() {
        assert!(SegmentHeader::from_bytes(&[0; 10]).is_err());
    }

    #[test]
    fn footer_too_short() {
        assert!(SegmentFooter::from_bytes(&[0; 5]).is_err());
    }

    /// The version check is equality, so it refuses in **both** directions.
    ///
    /// Two layouts can differ in a way no tolerant reader bridges — one that
    /// names the time column `timestamp` against one that names it `_time`
    /// returns a batch whose time column nothing looks for, which is no rows
    /// and no error. Below `VERSION` that is 0, which is also what a zeroed
    /// or truncated header reads as; far above it stands for a file from a
    /// future Chronix.
    ///
    /// This replaced four tests that asked the same question with four
    /// different wrong values.
    #[test]
    fn header_version_is_refused_in_both_directions() {
        let header = SegmentHeader {
            version: VERSION,
            flags: 0,
            created_at: 0,
            min_timestamp: 0,
            max_timestamp: 0,
            row_count: 0,
            column_count: 0,
            series_count: 0,
            compression: 0,
            sort_order: 0,
        };
        for wrong in [VERSION - 1, VERSION + 1, 999] {
            let mut bytes = header.to_bytes();
            bytes[4..6].copy_from_slice(&wrong.to_le_bytes());
            assert!(
                matches!(
                    SegmentHeader::from_bytes(&bytes),
                    Err(SegmentError::UnsupportedVersion { .. })
                ),
                "version {wrong} must be refused, not read as {VERSION}"
            );
        }
    }
}
