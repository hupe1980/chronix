//! Unified error type for the Chronix embedded database.
//!
//! [`DbError`] composes all sub-crate errors into a single, ergonomic
//! error hierarchy for the public API.

use thiserror::Error;

/// Top-level error type for the Chronix database.
///
/// Every operation on the [`Chronix`](crate::Chronix) handle returns
/// `Result<T, DbError>`. Convert freely from any sub-crate error via
/// `#[from]` blanket impls.
#[derive(Debug, Error)]
pub enum DbError {
    /// Core / configuration errors.
    #[error(transparent)]
    Core(#[from] chronix_core::ChronixError),

    /// Schema validation errors.
    #[error("Schema error: {0}")]
    Schema(#[from] chronix_core::SchemaError),

    /// Configuration errors.
    #[error("Config error: {0}")]
    Config(#[from] chronix_core::ConfigError),

    /// Write-Ahead Log errors.
    #[error("WAL error: {0}")]
    Wal(#[from] chronix_core::WalError),

    /// Encoding errors.
    #[error("Encoding error: {0}")]
    Encoding(#[from] chronix_encoding::EncodingError),

    /// Segment I/O errors.
    #[error("Segment error: {0}")]
    Segment(#[from] chronix_engine::segment::SegmentError),

    /// Memtable errors.
    #[error("Memtable error: {0}")]
    Memtable(#[from] chronix_engine::memtable::MemtableError),

    /// Storage backend errors.
    #[error("Storage error: {0}")]
    Storage(#[from] chronix_engine::storage::StorageError),

    /// Index errors.
    #[error("Index error: {0}")]
    Index(#[from] chronix_engine::index::IndexError),

    /// Query engine errors.
    #[error("Query error: {0}")]
    Query(#[from] chronix_query::QueryError),

    /// I/O errors.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// SQL planning or execution error.
    #[cfg(feature = "sql")]
    #[error("SQL error: {0}")]
    Sql(#[from] datafusion::error::DataFusionError),

    /// PromQL parse or evaluation error.
    #[error("PromQL error: {0}")]
    PromQl(String),

    /// Database is closed.
    #[error("Database is closed")]
    Closed,

    /// File lock acquisition failed.
    #[error("Failed to acquire database lock on {path}: another process holds the lock")]
    LockFailed {
        /// Path to the lock file.
        path: String,
    },

    /// Internal invariant violation.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Series cardinality limit exceeded.
    #[error(
        "Series cardinality limit exceeded: {current} unique series \
         (limit: {limit})"
    )]
    CardinalityExceeded {
        /// Number of unique series already tracked.
        current: usize,
        /// Configured maximum.
        limit: usize,
    },

    /// Query execution exceeded the configured timeout.
    #[error("Query timeout: execution exceeded {0:?}")]
    QueryTimeout(std::time::Duration),

    /// A point's timestamp is further ahead of the wall clock than
    /// `future_write_tolerance` allows.
    ///
    /// Rejected before admission so it cannot anchor the out-of-order
    /// window in the future and lock every real write out.
    #[error("timestamp {timestamp} is beyond the future-write limit {limit}")]
    FutureTimestamp {
        /// The rejected timestamp, nanoseconds since the epoch.
        timestamp: i64,
        /// The newest timestamp accepted at the time of the write.
        limit: i64,
    },

    /// Write admission denied — transient overload.
    ///
    /// A recoverable resource limit was hit (e.g. memtable memory at
    /// capacity). A background flush is already in progress; the client
    /// should back off briefly and retry.
    #[error("Transient overload: {reason} (retry after backoff)")]
    TransientOverload {
        /// Human-readable reason for the rejection.
        reason: String,
    },

    /// Write admission denied — persistent overload.
    ///
    /// A structural backlog has formed (e.g. WAL file count exceeds the
    /// safe threshold). The condition may not resolve without operator
    /// intervention or extended recovery time. Callers should avoid
    /// immediate retries and escalate via alerting.
    #[error("Persistent overload: {reason} (operator attention required)")]
    PersistentOverload {
        /// Human-readable reason for the rejection.
        reason: String,
    },
}

// `is_transient()` used to live here: a public predicate documented as
// "callers can use this to decide whether to retry", with **no callers**
// anywhere in the tree — including `chronixd`, the caller that most needed
// one, which flattened every variant into `500 DATABASE_ERROR: an internal
// error occurred` instead. A boolean could not have carried the answer
// anyway: "retry in a second" (a memtable being flushed), "retry in a
// minute, and page somebody" (a poisoned WAL) and "never retry, fix the
// query" (a cardinality limit) are three different instructions, and it
// lumped `Io` — which is where `ENOSPC` arrives — in with the first.
// Classification now lives in one exhaustive match in `chronixd::error`,
// where adding a variant here is a compile error rather than a silent 500.

/// Outcome of a batch insert.
///
/// Admission is decided per point **before** anything is made durable, so
/// a batch splits cleanly into the points that were accepted — all of them
/// in one WAL record, all of them in the memtable — and the points that
/// were rejected, each with its reason. The only per-point rejection is a
/// timestamp outside the out-of-order window.
///
/// Everything that fails the batch as a whole — a closed database,
/// overload, the cardinality budget, a schema type conflict, a WAL error —
/// returns `Err`, and then nothing from the batch is durable.
///
/// Discarding this value silently discards the rejections; `#[must_use]`
/// makes that a compiler warning rather than a lost write.
#[must_use = "an insert can be partial — check `is_complete()` or `rejected`"]
#[derive(Debug, Default)]
pub struct InsertResult {
    /// Number of points accepted: durable in the WAL and visible to reads.
    pub accepted: usize,
    /// Rejected points, as `(index into the batch, reason)`.
    pub rejected: Vec<(usize, DbError)>,
}

impl InsertResult {
    /// Returns `true` when every point was accepted.
    pub fn is_complete(&self) -> bool {
        self.rejected.is_empty()
    }

    /// Returns `true` when at least one point was rejected.
    pub fn is_partial(&self) -> bool {
        !self.rejected.is_empty()
    }

    /// Convert a partial insert into an error.
    ///
    /// For callers that want "all or nothing" semantics rather than
    /// inspecting per-point rejections. Returns the number of points
    /// accepted.
    ///
    /// # Errors
    ///
    /// Returns the first rejection's error if any point was rejected. The
    /// accepted points stay written — admission is per point, and the
    /// caller asked for the strict *report*, not a rollback.
    pub fn into_complete(self) -> Result<usize> {
        match self.rejected.into_iter().next() {
            None => Ok(self.accepted),
            Some((_, err)) => Err(err),
        }
    }
}

/// A specialized `Result` type for Chronix database operations.
pub type Result<T> = std::result::Result<T, DbError>;
