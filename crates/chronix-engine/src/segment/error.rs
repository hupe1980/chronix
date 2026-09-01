//! Segment error types.

use thiserror::Error;

/// Errors that can occur during segment operations.
#[derive(Debug, Error)]
pub enum SegmentError {
    /// The segment file is corrupt or has an invalid format.
    #[error("corrupt segment file: {detail}")]
    CorruptFile {
        /// Human-readable description of the corruption.
        detail: String,
    },

    /// Invalid magic bytes in header or footer.
    #[error("invalid magic bytes: expected CXSG")]
    InvalidMagic,

    /// `CRC32c` checksum mismatch.
    #[error("checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        /// Expected checksum value.
        expected: u32,
        /// Actual computed checksum.
        actual: u32,
    },

    /// Unsupported segment version.
    #[error("unsupported segment version: {version}")]
    UnsupportedVersion {
        /// The version number found in the file.
        version: u16,
    },

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An encoding error occurred.
    #[error("encoding error: {0}")]
    Encoding(#[from] chronix_encoding::EncodingError),

    /// The segment writer has already been finalized.
    #[error("segment writer already finalized")]
    AlreadyFinalized,

    /// No data has been written to the segment.
    #[error("no data written to segment")]
    EmptySegment,

    /// Row group index is out of range.
    #[error("row group index {index} out of range (max: {max})")]
    RowGroupOutOfRange {
        /// Requested row group index.
        index: usize,
        /// Maximum valid row group index.
        max: usize,
    },

    /// Column not found.
    #[error("column not found: {name}")]
    ColumnNotFound {
        /// The requested column name.
        name: String,
    },
}

/// A specialized `Result` type for segment operations.
pub type Result<T> = std::result::Result<T, SegmentError>;
