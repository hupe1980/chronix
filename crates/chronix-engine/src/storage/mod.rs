//! # chronix-storage
//!
//! Pluggable storage backend for the Chronix time-series database.
//!
//! This module defines the [`StorageBackend`] trait that abstracts segment
//! file storage. It has **one** implementation,
//! [`ObjectStoreBackend`](crate::objstore::ObjectStoreBackend) — the cold
//! tier — because the hot path does not go through it: a flush hands an
//! absolute path to [`SegmentWriter`](crate::segment::SegmentWriter) and a
//! read `mmap`s the file.
//!
//! It used to have two more, and both were reachable from nowhere. A
//! `LocalFsBackend` reimplemented the hot path's file I/O behind the async
//! trait and nothing called it. An `EncryptingBackend` wrapped any backend in
//! AES-256-GCM and nothing called that either — and because the trait is only
//! implemented by the cold tier, the most it could ever have encrypted is the
//! objects a bucket already encrypts server-side. The documentation described
//! it as Chronix's encryption at rest, down to a configuration key
//! (`storage.encryption.enabled`) that has never existed.
//!
//! ## Architecture
//!
//! Object keys are namespace-scoped and shard-based, for defence-in-depth
//! tenant isolation:
//! ```text
//! ns_{namespace}/shard_{id}/segment_name.csx
//! ```
//!
//! The storage backend is `Send + Sync + 'static` for use across async tasks.

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod backend;
pub mod error;

pub use backend::{SegmentPath, StorageBackend};
pub use error::StorageError;
