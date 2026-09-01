//! Storage backend error types.

use std::path::PathBuf;

use thiserror::Error;

/// Storage backend errors.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Underlying I/O error.
    #[error("Storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Segment not found at the expected path.
    #[error("Segment not found: {path}")]
    NotFound {
        /// Filesystem path that was not found.
        path: PathBuf,
    },

    /// Invalid segment path construction.
    #[error("Invalid segment path: {detail}")]
    InvalidPath {
        /// Description of the path error.
        detail: String,
    },
}

/// A specialized `Result` type for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;
