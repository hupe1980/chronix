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

    /// The on-disk data belongs to a different format generation.
    ///
    /// Deliberately distinct from [`Corrupt`](Self::Corrupt): the bytes are
    /// intact and the reader is from another generation, so the remedy is to
    /// match the versions up rather than to restore from a backup. The two
    /// used to be one variant, which sent an operator who had upgraded
    /// looking for a failing disk.
    #[error("{detail}")]
    UnsupportedVersion {
        /// What was read, what this build expects, and what to do.
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
