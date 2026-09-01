//! Compaction error types.

use thiserror::Error;

/// Errors that can occur during compaction.
#[derive(Debug, Error)]
pub enum CompactionError {
    /// No segments to compact.
    #[error("No segments eligible for compaction")]
    NoEligibleSegments,

    /// Segment I/O error.
    #[error("Segment error: {0}")]
    Segment(#[from] crate::segment::SegmentError),

    /// Encoding error during re-encoding.
    #[error("Encoding error: {0}")]
    Encoding(#[from] chronix_encoding::EncodingError),

    /// Index error.
    #[error("Index error: {0}")]
    Index(#[from] crate::index::IndexError),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal error.
    #[error("Internal compaction error: {0}")]
    Internal(String),

    /// Compaction task exceeded the configured timeout.
    #[error("Compaction timeout: task exceeded {0}ms limit")]
    TaskTimeout(u64),

    /// Input segments exceed the configured memory budget.
    #[error("Compaction input too large: {loaded_bytes} bytes exceeds {limit_bytes} byte limit")]
    InputTooLarge {
        /// Bytes already loaded when the limit was hit.
        loaded_bytes: u64,
        /// Configured maximum.
        limit_bytes: u64,
    },
}

/// A specialized `Result` type for compaction operations.
pub type Result<T> = std::result::Result<T, CompactionError>;
