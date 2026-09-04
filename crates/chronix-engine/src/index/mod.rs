//! # chronix-index
//!
//! Indexing structures for the Chronix time-series database.
//!
//! This crate provides indexing structures for efficient segment pruning:
//!
//! - [`TimeIndex`] — Sorted time-range index for O(log N) segment lookup
//! - [`SeriesBloomFilter`] — Per-segment bloom filters for series key pruning
//! - [`series_index`] — The per-segment series list the blooms, the tag index
//!   and the cardinality budget are rebuilt from at open
//! - [`SegmentCatalog`] — Segment metadata catalog with manifest persistence
//! - [`TagInvertedIndex`] — Inverted tag index for tag-value → segment mapping
//! - [`ZoneMapPredicate`] — Predicate pushdown using per-row-group column stats
//!
//! Together, these indices enable multi-level pruning that eliminates segments
//! and row groups from query processing before any data is decoded.

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod bloom;
pub mod catalog;
pub mod error;
pub mod inverted;
pub mod series_index;
pub mod time_index;
pub mod zone_map;

pub use bloom::SeriesBloomFilter;
pub use catalog::{CatalogColumnStats, SegmentCatalog, SegmentCatalogEntry};
pub use error::IndexError;
pub use inverted::TagInvertedIndex;
pub use time_index::{TimeIndex, TimeIndexEntry};
pub use zone_map::{
    prune_row_groups, segment_overlaps_range, ZoneMapOp, ZoneMapPredicate, ZoneMapResult,
};
