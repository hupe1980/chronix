//! # Chronix Engine
//!
//! The storage engine of Chronix — everything between an accepted write and
//! a pruned, decoded read:
//!
//! - [`wal`] — write-ahead log: CRC32c-protected records, group commit,
//!   configurable fsync policies, rotation, truncation, crash recovery.
//! - [`memtable`] — lock-free skip-list write buffer with freeze-and-swap
//!   flush lifecycle and time-shard routing.
//! - [`segment`] — immutable columnar `.csx` segment files: row groups,
//!   per-column encoding and stats, LZ4/Zstd compression, mmap read path,
//!   optional AES-256-GCM field encryption (feature `field-encryption`).
//! - [`storage`] — pluggable storage backend trait with an atomic
//!   local-filesystem implementation and transparent encryption wrapper.
//! - [`index`] — pruning structures: series bloom filters,
//!   inverted tag index, skip index, zone maps, and the segment catalog with
//!   its manifest WAL + snapshots.
//! - [`compaction`] — hybrid TWCS + size-tiered compaction with streaming
//!   K-way merge, tombstone cleanup, and write-amplification budgeting.
//! - [`cache`] — last-value cache, TinyLFU segment cache, metadata cache.
//! - `objstore` *(feature `object-store`)* — S3/GCS/Azure object storage
//!   backend with local disk caching for cold-data tiering.

pub mod cache;
pub mod compaction;
pub mod index;
pub mod memtable;
#[cfg(feature = "object-store")]
pub mod objstore;
pub mod segment;
pub mod storage;
pub mod wal;
