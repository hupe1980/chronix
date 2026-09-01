//! # chronix-cache
//!
//! Caching layer for the Chronix time-series database.
//!
//! This crate provides:
//! - [`LastValueCache`] — O(1) lookup of the most recent point per series
//! - [`SegmentCache`] — LRU cache for decoded Arrow arrays from segments
//! - [`MetadataCache`] — in-memory cache for segment metadata

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod error;
pub mod lvc;
pub mod metadata;
pub mod segment;

pub use error::CacheError;
pub use lvc::LastValueCache;
pub use metadata::MetadataCache;
pub use segment::{CacheLoadError, SegmentCache};
