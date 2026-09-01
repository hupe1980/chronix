//! # Write-Ahead Log
//!
//! Write-Ahead Log (WAL) for the Chronix time-series database.
//!
//! The WAL provides durable, crash-safe persistence of write operations. Every
//! write is first logged to the WAL before being applied to the memtable.
//! On crash recovery, the WAL is replayed to reconstruct in-memory state.
//!
//! ## Record Format
//!
//! Each WAL record is:
//! ```text
//! [crc32c: u32][length: u32][sequence_no: u64][record_type: u8][payload_version: u8][payload: [u8]]
//! ```
//!
//! The `record_type` discriminant enables forward-compatible payload
//! handling.
//!
//! ## File Format
//!
//! WAL files start with a header:
//! ```text
//! [magic: b"CXWL"][version: u16]
//! ```
//!
//! Files are named `wal_{sequence_start}.cxwl` and rotated when they exceed
//! `max_file_size`.

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod reader;
mod writer;

pub use reader::{replay_all, replay_range, WalReader, WalRecord};
pub use writer::WalWriter;

/// WAL file magic bytes.
pub const WAL_MAGIC: &[u8; 4] = b"CXWL";

/// Current WAL format version.
pub const WAL_VERSION: u16 = 2;

/// WAL file header size: 4 (magic) + 2 (version) = 6 bytes.
pub const WAL_HEADER_SIZE: usize = 6;

/// WAL record header size:
/// 4 (crc) + 4 (length) + 8 (sequence) + 1 (record_type) + 1 (payload_version) = 18 bytes.
pub const WAL_RECORD_HEADER_SIZE: usize = 18;

/// WAL record types — discriminant for forward-compatible payload handling.
///
/// New record types can be added without breaking existing readers as long
/// as readers skip unknown types gracefully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    /// Single data write (point insert).
    Data = 0,
    /// Batch write (multiple points in one atomic record).
    Batch = 1,
    /// Schema change (measurement schema registration).
    Schema = 2,
    /// Tombstone (series deletion marker).
    Tombstone = 3,
}

impl WalRecordType {
    /// Convert from raw byte, returning `None` for unknown types.
    #[must_use]
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(Self::Data),
            1 => Some(Self::Batch),
            2 => Some(Self::Schema),
            3 => Some(Self::Tombstone),
            _ => None,
        }
    }
}

/// Current payload serialisation version.
///
/// Bumped when the encoding of `Point` / batch payloads changes in a
/// backwards-incompatible way.  Old readers that see a higher version
/// can reject the record with a clear error instead of silently
/// mis-parsing data.
pub const WAL_PAYLOAD_VERSION: u8 = 1;

/// Magic bytes for LZ4-compressed WAL payloads.
pub(crate) const WAL_COMPRESS_MAGIC: &[u8; 4] = b"CXLZ";

/// Compress a WAL payload with LZ4.
///
/// The compressed payload is prefixed with [`WAL_COMPRESS_MAGIC`] so the
/// reader can auto-detect compressed vs. uncompressed records.
pub(crate) fn compress_wal_payload(payload: &[u8]) -> Vec<u8> {
    let compressed = lz4_flex::compress_prepend_size(payload);
    let mut result = Vec::with_capacity(4 + compressed.len());
    result.extend_from_slice(WAL_COMPRESS_MAGIC);
    result.extend_from_slice(&compressed);
    result
}

/// Decompress a WAL payload if it was LZ4-compressed.
///
/// Returns the original payload unchanged for non-compressed records
/// (no `CXLZ` prefix).
pub(crate) fn decompress_wal_payload(payload: Vec<u8>) -> Result<Vec<u8>, String> {
    if payload.len() >= 4 && &payload[..4] == WAL_COMPRESS_MAGIC {
        lz4_flex::decompress_size_prepended(&payload[4..])
            .map_err(|e| format!("LZ4 decompression failed: {e}"))
    } else {
        Ok(payload)
    }
}

/// Magic header for batch-framed payloads inside a WAL record.
pub const BATCH_MAGIC: &[u8; 4] = b"CXBT";

/// Encode multiple payloads into a single batch-framed payload.
///
/// Layout:
/// ```text
/// [CXBT: 4 bytes]          — batch magic
/// [count: u32 le]          — number of sub-payloads
/// for each sub-payload:
///   [len: u32 le]          — sub-payload length
///   [data: len bytes]      — sub-payload data
/// ```
///
/// This format is written as a single WAL record so the CRC covers the
/// entire batch — either all sub-payloads survive or none (crash atomicity).
#[must_use]
pub fn encode_batch_payload(payloads: &[&[u8]]) -> Vec<u8> {
    let total: usize = 4 + 4 + payloads.iter().map(|p| 4 + p.len()).sum::<usize>();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(BATCH_MAGIC);
    buf.extend_from_slice(&(payloads.len() as u32).to_le_bytes());
    for p in payloads {
        buf.extend_from_slice(&(p.len() as u32).to_le_bytes());
        buf.extend_from_slice(p);
    }
    buf
}

/// Decode a batch-framed payload back into individual sub-payloads.
///
/// Returns `None` if the payload does not start with the [`BATCH_MAGIC`]
/// header (i.e. it is a plain single-record payload).
#[must_use]
pub fn decode_batch_payload(data: &[u8]) -> Option<Vec<Vec<u8>>> {
    if data.len() < 8 || &data[..4] != BATCH_MAGIC {
        return None;
    }
    let count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;

    // Sanity-check count against remaining data to prevent OOM
    // on crafted payloads. Each sub-payload needs at least a 4-byte length prefix.
    let remaining = data.len() - 8;
    if count > remaining / 4 {
        return None; // implausible count
    }

    let mut offset = 8;
    let mut payloads = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > data.len() {
            return None; // truncated
        }
        let len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;
        if offset + len > data.len() {
            return None; // truncated
        }
        payloads.push(data[offset..offset + len].to_vec());
        offset += len;
    }
    Some(payloads)
}
