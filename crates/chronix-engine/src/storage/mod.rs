//! The storage backend trait for the cold tier.
//!
//! This module defines the [`StorageBackend`] trait that abstracts segment
//! file storage. It has **one** implementation,
//! `objstore::ObjectStoreBackend` (feature `object-store`) — the cold
//! tier — because the hot path does not go through it: a flush hands an
//! absolute path to [`SegmentWriter`](crate::segment::SegmentWriter) and a
//! read `mmap`s the file.
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
