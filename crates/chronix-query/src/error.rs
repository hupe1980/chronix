//! Query engine error types.

use thiserror::Error;

/// Query engine errors.
#[derive(Debug, Error)]
pub enum QueryError {
    /// Measurement not found in the schema registry.
    #[error("Measurement not found: '{0}'")]
    MeasurementNotFound(String),

    /// Field not found in the measurement schema.
    #[error("Field '{field}' not found in measurement '{measurement}'")]
    FieldNotFound {
        /// Measurement name.
        measurement: String,
        /// Field name that was not found.
        field: String,
    },

    /// Invalid time range (start > end).
    #[error("Invalid time range: start ({start}) > end ({end})")]
    InvalidTimeRange {
        /// Start timestamp.
        start: i64,
        /// End timestamp.
        end: i64,
    },

    /// Query validation error.
    #[error("Query validation error: {0}")]
    Validation(String),

    /// Segment read error.
    #[error("Segment error: {0}")]
    Segment(#[from] chronix_engine::segment::SegmentError),

    /// Arrow computation error.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Index error.
    #[error("Index error: {0}")]
    Index(#[from] chronix_engine::index::IndexError),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Per-query memory budget exceeded.
    ///
    /// Returned when a query operator attempts to allocate more memory
    /// than the configured per-query budget allows. This prevents
    /// individual queries from causing OOM on the server.
    #[error(
        "Query memory budget exceeded: requested {requested} bytes, \
         already allocated {allocated} of {budget} bytes"
    )]
    QueryMemoryExceeded {
        /// Bytes requested in this allocation.
        requested: usize,
        /// Bytes already allocated by this query.
        allocated: usize,
        /// Total budget for this query.
        budget: usize,
    },
}

/// A specialized `Result` type for query operations.
pub type Result<T> = std::result::Result<T, QueryError>;
