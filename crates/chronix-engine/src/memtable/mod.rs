//! # chronix-memtable
//!
//! High-performance concurrent memtable for the Chronix time-series database.
//!
//! This crate provides the in-memory write buffer that sits between the WAL
//! and on-disk segment files. Points are inserted into a lock-free concurrent
//! skip list and can be scanned in sorted order. When the memtable reaches a
//! configurable size threshold, it is frozen and flushed to a columnar segment
//! file via [`crate::segment::SegmentWriter`].
//!
//! # Architecture
//!
//! - **[`Memtable`]** — Concurrent skip-list-backed write buffer
//! - **[`FlushController`]** — Freeze-and-swap lifecycle management
//! - **[`ShardRouter`]** — Time-shard routing for out-of-order writes

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod error;
pub mod flush;
pub mod interner;
pub mod key;
#[allow(clippy::module_inception)]
pub mod memtable;
pub mod shard;

pub use error::MemtableError;
pub use flush::{FlushConfig, FlushController, FlushResult};
pub use interner::StringInterner;
pub use key::{MemtableEntry, MemtableKey};
pub use memtable::Memtable;
pub use shard::{ShardRouter, ShardRouterConfig};
