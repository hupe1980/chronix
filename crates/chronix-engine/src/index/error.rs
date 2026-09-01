//! Index error types.

use thiserror::Error;

/// Indexing errors.
#[derive(Debug, Error)]
pub enum IndexError {
    /// Underlying I/O error.
    #[error("Index I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Segment not found in the catalog.
    #[error("Segment not found in catalog: {0}")]
    SegmentNotFound(u64),

    /// Corrupt index data.
    #[error("Corrupt index: {detail}")]
    Corrupt {
        /// Description of the corruption.
        detail: String,
    },

    /// Binary (postcard) serialization error.
    #[error("Binary serialization error: {0}")]
    BinarySerialization(String),

    /// Manifest replay error.
    #[error("Manifest error: {detail}")]
    Manifest {
        /// Description of the manifest error.
        detail: String,
    },
}

/// A specialized `Result` type for index operations.
pub type Result<T> = std::result::Result<T, IndexError>;
