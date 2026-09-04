//! Error types for the memtable crate.

use thiserror::Error;

/// Errors that can occur during memtable operations.
#[derive(Debug, Error)]
pub enum MemtableError {
    /// The memtable is frozen and cannot accept new writes.
    #[error("memtable is frozen and cannot accept writes")]
    Frozen,

    /// Memory capacity exceeded — the memtable has reached its size limit.
    #[error("memtable capacity exceeded: current {current} bytes, limit {limit} bytes")]
    CapacityExceeded {
        /// Current memory usage in bytes.
        current: usize,
        /// Configured memory limit in bytes.
        limit: usize,
    },

    /// A schema violation was detected during insert.
    #[error("schema error: {0}")]
    Schema(#[from] chronix_core::SchemaError),

    /// The target shard is outside the allowed out-of-order tolerance.
    #[error(
        "write to shard {target} rejected: outside tolerance window \
         [{min_allowed}..={max_allowed}]"
    )]
    ShardOutOfRange {
        /// The shard the write was targeting.
        target: i64,
        /// Minimum allowed shard ID.
        min_allowed: i64,
        /// Maximum allowed shard ID.
        max_allowed: i64,
    },

    /// An I/O error occurred during flush.
    #[error("flush I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A segment write error occurred during flush.
    #[error("segment write error: {0}")]
    Segment(#[from] crate::segment::SegmentError),

    /// Registering a flushed segment failed; the memtable stays frozen.
    #[error("segment registration failed: {0}")]
    Flush(String),

    /// No frozen memtable is available for flushing.
    #[error("no frozen memtable available for flush")]
    NoFrozenMemtable,

    /// A WAL error occurred during truncation.
    #[error("WAL error: {0}")]
    Wal(#[from] chronix_core::WalError),
}

/// Convenience type alias for memtable results.
pub type Result<T> = std::result::Result<T, MemtableError>;
