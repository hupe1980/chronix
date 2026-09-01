//! Per-column metadata within a segment file.
//!
//! Column metadata enables selective reading: the reader can determine which
//! columns exist, their data types, and where their data is stored without
//! reading any row group data.

use chronix_encoding::EncodingType;
use serde::{Deserialize, Serialize};

use crate::segment::error::{Result, SegmentError};
use crate::segment::stats::{ColumnStats, COLUMN_STATS_SIZE};
use crate::segment::to_array;

/// Metadata for a single column within a segment.
///
/// # Segment-level statistics
///
/// The [`stats`](Self::stats) field stores aggregate statistics across all
/// row groups: `min_value`, `max_value`, `null_count`, `value_count`,
/// `sum`, and `distinct_count`.  These are computed by the segment writer
/// during encoding and merged via `merge_stats()`.  Query-time code can
/// use these to prune entire segments without reading row-group data.
///
/// Metadata here is kept proportional to the data. Columns previously also
/// carried a fixed-size HyperLogLog sketch and an equi-depth histogram; both
/// were removed because nothing read them — the query planner works
/// from the exact `distinct_count` above — while they cost ~49 KiB per
/// segment regardless of how little data the segment held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnMeta {
    /// Column name.
    pub name: String,
    /// Column data type tag (maps to `ColumnType`).
    pub data_type: u8,
    /// Column role tag: 0 = timestamp, 1 = tag, 2 = field.
    pub role: u8,
    /// The encoding the writer *intended* for this column, chosen from the
    /// column's type before any data was seen.
    ///
    /// This is a hint only. The adaptive selector may fall back to a different
    /// codec per block (e.g. Chimp → Gorilla → plain), so the authoritative
    /// encoding for reading is always
    /// [`ColumnBlockMeta::encoding`](crate::segment::metadata::ColumnBlockMeta::encoding).
    /// The field was previously named `encoding`, which read as a statement of
    /// fact; nothing decodes from it, but the name invited that mistake.
    pub default_encoding: u8,
    /// Global statistics across all row groups (min, max, null_count,
    /// value_count, sum, distinct_count).  Enables segment-level pruning
    /// at query time.
    pub stats: ColumnStats,
    /// Bloom filter for tag columns.
    ///
    /// Contains the serialized bloom filter bit array for tag columns,
    /// enabling segment-level predicate pushdown: if a queried tag value
    /// is definitely not in the bloom filter, the segment can be skipped.
    /// `None` for non-tag columns, and for segments written before per-tag
    /// row-group blooms existed.
    #[serde(default)]
    pub bloom_filter: Option<Vec<u8>>,
    /// Whether this column is encrypted at rest (field-level encryption).
    ///
    /// When `true`, all column blocks for this column are AES-256-GCM
    /// encrypted and `key_id` identifies the encryption key.
    /// Statistics (min/max/sum/distinct) are zeroed to prevent leakage.
    #[serde(default)]
    pub encrypted: bool,
    /// Encryption key ID for this column. Used by the reader to look up
    /// the decryption key from a `FieldKeyProvider`.
    #[serde(default)]
    pub key_id: Option<String>,
    /// Per-row-group bloom filters for string columns .
    ///
    /// `row_group_blooms[rg_idx]` is a serialized bloom filter containing
    /// all distinct string values for this column in that row group.
    /// Enables row-group-level predicate pushdown: if a queried value is
    /// definitely not in the bloom filter, the row group can be skipped.
    /// `None` for non-string columns, encrypted columns, or segments
    /// written before .
    #[serde(default)]
    pub row_group_blooms: Option<Vec<Vec<u8>>>,
}

/// Column data type tags.
pub mod data_types {
    /// Timestamp column (i64 nanoseconds).
    pub const TIMESTAMP: u8 = 0;
    /// String column (tags or string fields).
    pub const STRING: u8 = 1;
    /// 64-bit float column.
    pub const F64: u8 = 2;
    /// Signed 64-bit integer column.
    pub const I64: u8 = 3;
    /// Unsigned 64-bit integer column.
    pub const U64: u8 = 4;
    /// Boolean column.
    pub const BOOL: u8 = 5;
}

/// Column role tags.
pub mod roles {
    /// Timestamp column.
    pub const TIMESTAMP: u8 = 0;
    /// Tag column (string, indexed, low cardinality).
    pub const TAG: u8 = 1;
    /// Field column (any data type).
    pub const FIELD: u8 = 2;
}

/// Per-row-group column block location and metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnBlockMeta {
    /// Index of the column in the column metadata table.
    pub column_index: u16,
    /// Encoding type for this specific block.
    pub encoding: EncodingType,
    /// Whether the block is LZ4-compressed.
    pub compressed: bool,
    /// Byte offset within the segment file.
    pub offset: u64,
    /// Length of the (possibly compressed) block in bytes.
    pub length: u32,
    /// Number of values in this block.
    pub value_count: u32,
    /// CRC32c checksum of the block data for integrity verification.
    /// A value of 0 indicates no checksum (backward compatibility with old segments).
    pub block_crc: u32,
    /// Whether this block is encrypted (AES-256-GCM).
    pub encrypted: bool,
    /// Byte offset of this block's validity bitmap within the segment file.
    ///
    /// See [`validity_length`](Self::validity_length). Meaningless when
    /// `validity_length == 0`.
    pub validity_offset: u64,
    /// Length in bytes of this block's validity bitmap, or `0` when the block
    /// contains no nulls (`.csx` v2, D-NULL).
    ///
    /// The bitmap is an Arrow-compatible LSB-first packed bitmap of
    /// `value_count` bits: bit *i* set means row *i* holds a real value, bit
    /// clear means SQL `NULL`. It is stored uncompressed and unencrypted
    /// immediately after the block's value bytes.
    ///
    /// Before v2, absent values were encoded as type-specific sentinels
    /// (`0`/`""`/`false`) and only an aggregate `null_count` survived, so a
    /// stored zero and an absent field were indistinguishable at read time.
    pub validity_length: u32,
    /// Block-level statistics.
    pub stats: ColumnStats,
}

impl ColumnBlockMeta {
    /// Serialized size of a column block metadata entry.
    pub const SIZE: usize = 2 + 1 + 1 + 8 + 4 + 4 + 4 + 1 + 8 + 4 + COLUMN_STATS_SIZE;
    // column_index(2) + encoding(1) + compressed(1) + offset(8) + length(4)
    // + value_count(4) + block_crc(4) + encrypted(1) + validity_offset(8)
    // + validity_length(4) + stats(COLUMN_STATS_SIZE)

    /// Serialize to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::SIZE);
        buf.extend_from_slice(&self.column_index.to_le_bytes());
        buf.push(self.encoding.tag());
        buf.push(u8::from(self.compressed));
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());
        buf.extend_from_slice(&self.value_count.to_le_bytes());
        buf.extend_from_slice(&self.block_crc.to_le_bytes());
        buf.push(u8::from(self.encrypted));
        buf.extend_from_slice(&self.validity_offset.to_le_bytes());
        buf.extend_from_slice(&self.validity_length.to_le_bytes());
        buf.extend_from_slice(&self.stats.to_bytes());
        buf
    }

    /// Deserialize from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the data is too short or contains invalid values.
    ///
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "column block meta too short: expected {}, got {}",
                    Self::SIZE,
                    data.len()
                ),
            });
        }

        let column_index = u16::from_le_bytes([data[0], data[1]]);
        let encoding = EncodingType::from_tag(data[2]).map_err(|e| SegmentError::CorruptFile {
            detail: format!("invalid encoding tag: {e}"),
        })?;
        let compressed = data[3] != 0;
        let offset = u64::from_le_bytes(to_array!(data[4..12], "block meta offset")?);
        let length = u32::from_le_bytes(to_array!(data[12..16], "block meta length")?);
        let value_count = u32::from_le_bytes(to_array!(data[16..20], "block meta value_count")?);
        let block_crc = u32::from_le_bytes(to_array!(data[20..24], "block meta block_crc")?);
        let encrypted = data[24] != 0;
        let validity_offset =
            u64::from_le_bytes(to_array!(data[25..33], "block meta validity_offset")?);
        let validity_length =
            u32::from_le_bytes(to_array!(data[33..37], "block meta validity_length")?);
        let stats = ColumnStats::from_bytes(&data[37..])?;

        Ok(Self {
            column_index,
            encoding,
            compressed,
            offset,
            length,
            value_count,
            block_crc,
            encrypted,
            validity_offset,
            validity_length,
            stats,
        })
    }
}

/// Metadata section stored after all row groups, before the footer.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentMetadata {
    /// Column metadata table.
    pub columns: Vec<ColumnMeta>,
    /// Per-row-group, per-column block metadata.
    pub row_group_blocks: Vec<Vec<ColumnBlockMeta>>,
    /// Optional Zstd compression dictionary trained from this segment's
    /// data. When present, blocks tagged with `CODEC_TAG_ZSTD_DICT`
    /// must be decompressed using this dictionary.
    pub zstd_dictionary: Option<Vec<u8>>,
}

impl SegmentMetadata {
    /// Serialize the metadata section to bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if column metadata serialization fails.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let encoded =
            postcard::to_stdvec(&self.columns).map_err(|e| SegmentError::CorruptFile {
                detail: format!("column metadata serialization failed: {e}"),
            })?;

        // Format: [column_meta_len: u32][column_meta_postcard]
        //         [rg_count: u32]
        //         for each rg: [block_count: u16][block_meta...][block_meta...]
        //         [zstd_dict_len: u32][dict_bytes]  (0 = no dict)
        let mut buf = Vec::new();

        // Column metadata (postcard)
        buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        buf.extend_from_slice(&encoded);

        // Row group block metadata
        buf.extend_from_slice(&(self.row_group_blocks.len() as u32).to_le_bytes());
        for rg_blocks in &self.row_group_blocks {
            buf.extend_from_slice(&(rg_blocks.len() as u16).to_le_bytes());
            for block in rg_blocks {
                buf.extend_from_slice(&block.to_bytes());
            }
        }

        // Zstd dictionary (trailing, backward-compatible)
        if let Some(dict) = &self.zstd_dictionary {
            buf.extend_from_slice(&(dict.len() as u32).to_le_bytes());
            buf.extend_from_slice(dict);
        } else {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }

        Ok(buf)
    }

    /// Deserialize the metadata section from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the data is malformed.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(SegmentError::CorruptFile {
                detail: "metadata section too short".to_string(),
            });
        }

        let json_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut offset = 4;

        if data.len() < offset + json_len {
            return Err(SegmentError::CorruptFile {
                detail: "metadata column section truncated".to_string(),
            });
        }

        let columns: Vec<ColumnMeta> = postcard::from_bytes(&data[offset..offset + json_len])
            .map_err(|e| SegmentError::CorruptFile {
                detail: format!("invalid column metadata: {e}"),
            })?;
        offset += json_len;

        if data.len() < offset + 4 {
            return Err(SegmentError::CorruptFile {
                detail: "metadata missing row group count".to_string(),
            });
        }

        let rg_count = u32::from_le_bytes(to_array!(
            data[offset..offset + 4],
            "metadata row group count"
        )?) as usize;
        offset += 4;

        // Validate that the claimed rg_count is plausible given remaining data.
        // Each row group needs at least 2 bytes (block_count), so the total
        // remaining data must be at least rg_count * 2.
        let remaining = data.len().saturating_sub(offset);
        if rg_count > remaining / 2 {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "row group count ({rg_count}) exceeds available metadata ({remaining} bytes)"
                ),
            });
        }

        let mut row_group_blocks = Vec::with_capacity(rg_count);
        for _ in 0..rg_count {
            if data.len() < offset + 2 {
                return Err(SegmentError::CorruptFile {
                    detail: "metadata missing block count".to_string(),
                });
            }
            let block_count = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            let mut blocks = Vec::with_capacity(block_count);
            for _ in 0..block_count {
                let block = ColumnBlockMeta::from_bytes(&data[offset..])?;
                offset += ColumnBlockMeta::SIZE;
                blocks.push(block);
            }
            row_group_blocks.push(blocks);
        }

        // Zstd dictionary (trailing, optional for backward compat).
        let zstd_dictionary = if data.len() >= offset + 4 {
            let dict_len = u32::from_le_bytes(to_array!(
                data[offset..offset + 4],
                "metadata zstd_dict_len"
            )?) as usize;
            offset += 4;
            if dict_len > 0 {
                if data.len() < offset + dict_len {
                    return Err(SegmentError::CorruptFile {
                        detail: "metadata zstd dictionary truncated".to_string(),
                    });
                }
                let dict = data[offset..offset + dict_len].to_vec();
                Some(dict)
            } else {
                None
            }
        } else {
            // Old segment without dictionary section — backward compatible.
            None
        };

        Ok(Self {
            columns,
            row_group_blocks,
            zstd_dictionary,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_block_meta_roundtrip() {
        let meta = ColumnBlockMeta {
            column_index: 3,
            encoding: EncodingType::Chimp,
            compressed: true,
            offset: 1024,
            length: 512,
            value_count: 1000,
            block_crc: 0xDEAD_BEEF,
            encrypted: false,
            validity_offset: 0,
            validity_length: 0,
            stats: ColumnStats::empty(),
        };
        let bytes = meta.to_bytes();
        assert_eq!(bytes.len(), ColumnBlockMeta::SIZE);
        let recovered = ColumnBlockMeta::from_bytes(&bytes).unwrap();
        assert_eq!(meta, recovered);
    }

    #[test]
    fn segment_metadata_roundtrip() {
        let meta = SegmentMetadata {
            columns: vec![
                ColumnMeta {
                    name: "timestamp".to_string(),
                    data_type: data_types::TIMESTAMP,
                    role: roles::TIMESTAMP,
                    default_encoding: EncodingType::DeltaOfDelta.tag(),
                    stats: ColumnStats::empty(),
                    bloom_filter: None,
                    encrypted: false,
                    key_id: None,
                    row_group_blooms: None,
                },
                ColumnMeta {
                    name: "host".to_string(),
                    data_type: data_types::STRING,
                    role: roles::TAG,
                    default_encoding: EncodingType::Dictionary.tag(),
                    stats: ColumnStats::empty(),
                    bloom_filter: None,
                    encrypted: false,
                    key_id: None,
                    row_group_blooms: None,
                },
                ColumnMeta {
                    name: "cpu".to_string(),
                    data_type: data_types::F64,
                    role: roles::FIELD,
                    default_encoding: EncodingType::Chimp.tag(),
                    stats: ColumnStats::empty(),
                    bloom_filter: None,
                    encrypted: false,
                    key_id: None,
                    row_group_blooms: None,
                },
            ],
            row_group_blocks: vec![vec![
                ColumnBlockMeta {
                    column_index: 0,
                    encoding: EncodingType::DeltaOfDelta,
                    compressed: false,
                    offset: 48,
                    length: 200,
                    value_count: 1000,
                    block_crc: 0,
                    encrypted: false,
                    validity_offset: 0,
                    validity_length: 0,
                    stats: ColumnStats::empty(),
                },
                ColumnBlockMeta {
                    column_index: 1,
                    encoding: EncodingType::Dictionary,
                    compressed: true,
                    offset: 248,
                    length: 50,
                    value_count: 1000,
                    block_crc: 0,
                    encrypted: false,
                    validity_offset: 0,
                    validity_length: 0,
                    stats: ColumnStats::empty(),
                },
            ]],
            zstd_dictionary: None,
        };

        let bytes = meta.to_bytes().unwrap();
        let recovered = SegmentMetadata::from_bytes(&bytes).unwrap();
        assert_eq!(meta, recovered);
    }

    #[test]
    fn corrupt_metadata_detected() {
        assert!(SegmentMetadata::from_bytes(&[0; 2]).is_err());
    }
}
