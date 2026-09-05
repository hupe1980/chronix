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

    /// The timestamp is outside the allowed out-of-order tolerance.
    ///
    /// The message names **timestamps**, because shard identifiers are an
    /// internal quotient nobody can act on: a Telegraf agent that buffered for
    /// an hour was told `write to shard 496833 rejected: outside tolerance
    /// window [496836..=496840]`, which says neither how late the point was
    /// nor what to do about it. It also names the way out, because there is
    /// one — importing history is `backfill`, and every network write path
    /// exposes it.
    #[error(
        "timestamp {timestamp} is outside the out-of-order window \
         [{earliest}, {latest}] (+/-{tolerance} shard(s) of {shard_secs}s \
         around the newest admitted write); import history with a backfill \
         write instead"
    )]
    ShardOutOfRange {
        /// The rejected timestamp, in epoch nanoseconds.
        timestamp: i64,
        /// Oldest timestamp the window admits, in epoch nanoseconds.
        earliest: i64,
        /// Newest timestamp the window admits, in epoch nanoseconds.
        latest: i64,
        /// Configured `ooo_shard_tolerance`.
        tolerance: i64,
        /// Configured `shard_duration`, in seconds.
        shard_secs: u64,
    },

    /// A flush was asked for a shard the router does not hold.
    ///
    /// Its own variant: this was reported as `ShardOutOfRange` with a target
    /// shard and a `[0..=0]` window, which is a different fact stated with
    /// made-up numbers.
    #[error("no such shard: {0}")]
    NoSuchShard(i64),

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
