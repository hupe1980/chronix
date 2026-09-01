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

impl DbError {
    /// Returns `true` when the error represents a transient condition that
    /// is likely to resolve on its own (e.g. after a flush or timeout).
    ///
    /// Callers can use this to decide whether to retry with back-off or
    /// fail immediately.
    pub fn is_transient(&self) -> bool {
        match self {
            // Explicit transient overload.
            Self::TransientOverload { .. } => true,
            // Query timeout — the same query may succeed if the system is
            // less loaded.
            Self::QueryTimeout(_) => true,
            // I/O errors are often transient (disk hiccup, fd limit).
            Self::Io(_) => true,
            // Everything else is considered permanent or unknown.
            _ => false,
        }
    }
}

/// Result of a batch insert where WAL commit succeeded but some
/// memtable insertions may have failed.
///
/// Pre-WAL failures (closed, backpressure, cardinality, WAL write) always
/// return `Err(DbError)` — the entire batch failed and nothing is durable.
/// Once the WAL has committed, any subsequent memtable insertion failures are
/// captured in [`InsertResult::errors`] while the data remains durable in the
/// WAL and will be recovered on restart.
/// Outcome of a batch insert.
///
/// A batch insert returns `Ok` even when **some or all** points were rejected
/// by the memtable — for example a point more than ±2 shards out of order.
/// The per-point errors live in [`errors`](Self::errors), so discarding this
/// value silently discards those rejections; `#[must_use]` makes that a
/// compiler warning rather than a lost write.
#[must_use = "an insert can be partial — check `is_complete()` or `errors`"]
#[derive(Debug)]
pub struct InsertResult {
    /// Number of points durably committed to WAL.
    pub wal_committed: usize,
    /// Number of points successfully inserted into the memtable.
    pub memtable_inserted: usize,
    /// Per-point errors (index into original batch → error).
    /// Empty when all points succeeded.
    pub errors: Vec<(usize, DbError)>,
}

impl InsertResult {
    /// Returns `true` when all points were inserted into the memtable.
    pub fn is_complete(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns `true` when at least one memtable insertion failed.
    pub fn is_partial(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Convert a partial insert into an error.
    ///
    /// For callers that want "all or nothing" semantics rather than
    /// inspecting per-point errors. Returns the number of points inserted.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Internal`] naming the first rejection if any point
    /// was rejected.
    pub fn into_complete(self) -> Result<usize> {
        match self.errors.into_iter().next() {
            None => Ok(self.memtable_inserted),
            Some((idx, err)) => Err(DbError::Internal(format!(
                "insert was partial: {} of {} points rejected; first at index {idx}: {err}",
                self.memtable_inserted, self.wal_committed
            ))),
        }
    }
}

/// A specialized `Result` type for Chronix database operations.
pub type Result<T> = std::result::Result<T, DbError>;
