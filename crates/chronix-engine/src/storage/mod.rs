//! # chronix-storage
//!
//! Pluggable storage backend for the Chronix time-series database.
//!
//! This crate defines the [`StorageBackend`] trait that abstracts segment file
//! storage, and provides a [`LocalFsBackend`] implementation for local
//! filesystem storage using atomic writes and positioned reads.
//!
//! ## Architecture
//!
//! Segment files are stored in a namespace-scoped, shard-based directory
//! hierarchy for defense-in-depth tenant isolation:
//! ```text
//! {data_dir}/ns_{namespace}/shard_{id}/segment_name.csx
//! ```
//!
//! The storage backend is designed to be `Send + Sync + 'static` for use
//! across async tasks.

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod backend;
pub mod encrypting;
pub mod error;
pub mod local;

pub use backend::{SegmentPath, StorageBackend};
pub use encrypting::EncryptingBackend;
pub use error::StorageError;
pub use local::LocalFsBackend;
