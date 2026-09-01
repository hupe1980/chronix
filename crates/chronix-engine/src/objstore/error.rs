//! Object storage error types.

use thiserror::Error;

/// Object storage errors.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ObjStoreError {
    /// Underlying object store error.
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),

    /// An uploaded archive object is missing or the wrong size.
    ///
    /// Reported as an error rather than a warning because the caller's next
    /// step after a successful archive is to delete the original.
    #[error("archive {object}: expected {expected} bytes, found {}", .found.map_or_else(|| "no object".to_string(), |n| n.to_string()))]
    IntegrityCheckFailed {
        /// Object-store path that failed verification.
        object: String,
        /// Size the local segment had.
        expected: usize,
        /// Size found remotely, or `None` if the object is absent.
        found: Option<usize>,
    },

    /// Local cache I/O error.
    #[error("cache I/O error: {0}")]
    CacheIo(#[source] std::io::Error),

    /// Object not found at the given path.
    #[error("object not found: {path}")]
    NotFound {
        /// The path that was not found.
        path: String,
    },

    /// Invalid configuration.
    #[error("invalid configuration: {detail}")]
    InvalidConfig {
        /// Description of the configuration error.
        detail: String,
    },

    /// URL parsing error.
    #[error("invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// Storage backend compatibility error.
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),

    /// Segment read error while re-encoding for the cold tier.
    #[error("segment error: {0}")]
    Segment(#[source] crate::segment::SegmentError),

    /// Parquet encode/decode error in the cold tier.
    #[error("parquet error: {0}")]
    Parquet(String),

    /// Local file I/O error.
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),
}

/// A specialised `Result` type for object storage operations.
pub type Result<T> = std::result::Result<T, ObjStoreError>;

impl From<ObjStoreError> for crate::storage::StorageError {
    fn from(e: ObjStoreError) -> Self {
        match e {
            ObjStoreError::NotFound { path } => crate::storage::StorageError::NotFound {
                path: std::path::PathBuf::from(path),
            },
            other => crate::storage::StorageError::InvalidPath {
                detail: other.to_string(),
            },
        }
    }
}
