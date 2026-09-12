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
    /// An encrypted column cannot be read because its key is not available.
    ///
    /// **Not** [`CorruptFile`](Self::CorruptFile), which is what this used to
    /// be: nothing is wrong with the bytes. The deployment is missing a key,
    /// and the two have completely different next steps — one is "restore
    /// from a backup", the other is "set the environment variable". The
    /// commonest way to arrive here is restoring an encrypted backup onto a
    /// machine the key was never given to.
    #[error(
        "column '{column}' is encrypted with key '{key_id}', which this database has no \
         key for — set it under [database.field_encryption] and restart"
    )]
    MissingEncryptionKey {
        /// The column that cannot be read.
        column: String,
        /// The key id its blocks were written under.
        key_id: String,
    },

    /// The writer was asked for something the format cannot honestly do.
    ///
    /// Distinct from [`CorruptFile`](Self::CorruptFile): nothing is wrong
    /// with the data, the *configuration* is wrong — declaring a tag column
    /// encrypted, for instance, which the format would accept and which
    /// would publish the value in the sidecar beside it.
    #[error("invalid segment writer configuration: {detail}")]
    InvalidConfiguration {
        /// What was asked for, and why it cannot be done.
        detail: String,
    },

    /// A requested column is not in the segment.
    #[error("column not found: {name}")]
    ColumnNotFound {
        /// The requested column name.
        name: String,
    },
}

/// A specialized `Result` type for segment operations.
pub type Result<T> = std::result::Result<T, SegmentError>;
