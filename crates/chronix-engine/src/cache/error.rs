//! Cache error types.

use thiserror::Error;

/// Errors that can occur in the caching layer.
#[derive(Debug, Error)]
pub enum CacheError {
    /// Segment I/O error.
    #[error("Segment error: {0}")]
    Segment(#[from] crate::segment::SegmentError),

    /// Internal error.
    #[error("Cache error: {0}")]
    Internal(String),
}

/// A specialized `Result` type for cache operations.
pub type Result<T> = std::result::Result<T, CacheError>;
