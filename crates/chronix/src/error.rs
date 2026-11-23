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
    ///
    /// `span` is the bytes of the query the error is about, and `hint` what
    /// the query probably meant; both are already in `message`.
    #[error("PromQL error: {message}")]
    PromQl {
        /// One sentence: what is wrong, and the suggestion when there is one.
        message: String,
        /// The bytes of the query the error is about.
        span: Option<crate::diagnostics::Span>,
        /// What the query probably meant: `did you mean "rate"?`.
        hint: Option<String>,
    },

    /// Database is closed.
    #[error("Database is closed")]
    Closed,

    /// The database was opened with
    /// [`read_only`](crate::prelude::ChronixConfig::read_only), and this would write.
    #[error(
        "this database is open read-only, so {operation} is refused — \
         open it without `read_only` to change it"
    )]
    ReadOnly {
        /// What was refused, e.g. `"insert"`.
        operation: &'static str,
    },

    /// A read-only open was asked for a directory a running process holds.
    #[error(
        "{path} is in use by process {pid} on {host}; query that process \
         (its server, or its `/mcp` endpoint) instead of opening its directory"
    )]
    DirectoryLive {
        /// The data directory.
        path: String,
        /// The holder's process id.
        pid: u32,
        /// The holder's host.
        host: String,
    },

    /// File lock acquisition failed.
    #[error("Failed to acquire database lock on {path}: another process holds the lock")]
    LockFailed {
        /// Path to the lock file.
        path: String,
    },

    /// Internal invariant violation.
    #[error("Internal error: {0}")]
    Internal(String),

    /// The caller asked for something that cannot be done, and retrying will
    /// not change that: a restore target that already exists, a backup
    /// directory with no manifest or with a segment missing, a checkpoint
    /// aimed at the database's own segment directory.
    ///
    /// Distinct from [`Internal`](Self::Internal) because the two reach a
    /// client differently and must: an internal error is a redacted `500`,
    /// and redacting *these* leaves an operator staring at
    /// `an internal error occurred` while trying to restore a backup. The
    /// message names the caller's own paths and nothing else.
    #[error("{0}")]
    InvalidRequest(String),

    /// A named object the caller asked for does not exist.
    ///
    /// Distinct from [`InvalidRequest`](Self::InvalidRequest) because the
    /// request was well formed and the answer is about the *database's*
    /// contents: `chronixd` renders it `404`, which is what tells a client
    /// that retrying the same call unchanged will keep failing while a
    /// different name might not.
    #[error("no {kind} named {name:?}")]
    NotFound {
        /// What kind of object — `"rollup"`, and whatever follows it.
        kind: &'static str,
        /// The name the caller gave.
        name: String,
    },

    /// A named object the caller asked to create already exists.
    ///
    /// The mirror of [`NotFound`](Self::NotFound), and `409` on the wire.
    /// Declaring the same rollup on every start is idempotent by nature —
    /// the registry is persisted, so every run after the first meets its own
    /// tier — and a caller has to be able to tell "already there" from
    /// "failed" without matching on a message.
    #[error("{kind} {name:?} already exists")]
    Conflict {
        /// What kind of object already exists.
        kind: &'static str,
        /// The name the caller gave.
        name: String,
    },

    /// An existing rollup differs from the definition passed to
    /// [`ensure_rollup`](crate::Chronix::ensure_rollup).
    #[error("rollup {name:?} has a different definition ({})", fields.join(", "))]
    RollupDefinitionConflict {
        /// The rollup whose definition was compared.
        name: String,
        /// All differing [`RollupConfig`](crate::RollupConfig) property names.
        fields: Vec<&'static str>,
    },

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

    /// A namespace is at one of its limits, and this write would cross it.
    ///
    /// Raised by the same admission step as
    /// [`CardinalityExceeded`](Self::CardinalityExceeded), against the same
    /// exact set of known series, and checked **before** it: a batch over both
    /// is refused with this one, because it names the limit the writer can
    /// act on. The batch is refused whole and registers nothing. `chronixd`
    /// renders it `400 NAMESPACE_QUOTA_EXCEEDED` — not `429`, because a
    /// retry of the same batch can only fail the same way.
    #[error(
        "namespace {namespace:?} is at its {} quota ({current} of {limit}); {}",
        resource.noun(),
        resource.remedy()
    )]
    NamespaceQuotaExceeded {
        /// The namespace whose limit refused the write.
        namespace: String,
        /// Which limit.
        resource: QuotaResource,
        /// The namespace's usage before this batch.
        current: u64,
        /// The configured limit.
        limit: u64,
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

// There is deliberately no `is_transient()`: "retry in a second", "retry
// later and page somebody" and "never retry, fix the request" are three
// different answers. Classification is one exhaustive match in
// `chronixd::error`, so a new variant here is a compile error there.

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

/// Which namespace limit refused a write — see
/// [`DbError::NamespaceQuotaExceeded`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QuotaResource {
    /// Distinct series in the namespace (`max_series_count`).
    SeriesCount,
    /// Measurements the namespace has written to (`max_measurements`).
    Measurements,
}

impl QuotaResource {
    /// The quota's name as it appears in configuration.
    #[must_use]
    pub const fn setting(self) -> &'static str {
        match self {
            Self::SeriesCount => "max_series_count",
            Self::Measurements => "max_measurements",
        }
    }

    const fn noun(self) -> &'static str {
        match self {
            Self::SeriesCount => "series",
            Self::Measurements => "measurement",
        }
    }

    const fn remedy(self) -> &'static str {
        match self {
            Self::SeriesCount => "raise max_series_count or reduce cardinality",
            Self::Measurements => "raise max_measurements or write to an existing measurement",
        }
    }
}

impl std::fmt::Display for QuotaResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.setting())
    }
}

impl DbError {
    /// A PromQL error with nothing to point at.
    #[must_use]
    pub fn promql(message: impl Into<String>) -> Self {
        Self::PromQl {
            message: message.into(),
            span: None,
            hint: None,
        }
    }

    /// What the error is about in `query`, and what was probably meant.
    ///
    /// For a PromQL error, its own span and hint. For a SQL error, what
    /// `DataFusion` knows — the span of the name it could not resolve — with
    /// the suggestion and the one-line message rewritten the same way for
    /// tables, columns and functions. Unknown *tables* get no suggestion here,
    /// because which tables a caller may be offered depends on its namespace:
    /// [`Chronix::diagnose`](crate::Chronix::diagnose) adds them.
    #[must_use]
    pub fn diagnosis(&self, query: &str) -> crate::diagnostics::Diagnosis {
        match self {
            Self::PromQl { span, hint, .. } => crate::diagnostics::Diagnosis {
                message: None,
                hint: hint.clone(),
                span: *span,
            },
            #[cfg(feature = "sql")]
            Self::Sql(e) => crate::sql::diagnose::sql_diagnosis(e, query, None),
            _ => {
                let _ = query;
                crate::diagnostics::Diagnosis::default()
            }
        }
    }
}

impl From<crate::promql::ParseError> for DbError {
    fn from(e: crate::promql::ParseError) -> Self {
        Self::PromQl {
            message: e.to_string(),
            span: e.span,
            hint: e.hint,
        }
    }
}
