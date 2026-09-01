//! # chronix-compaction
//!
//! Hybrid compaction engine for the Chronix time-series database.
//!
//! Combines **TWCS** (Time-Window Compaction Strategy) with **size-tiered
//! merging** and **write-amplification budgeting** for optimal background
//! I/O management.
//!
//! This crate provides:
//! - [`CompactionPicker`] — hybrid TWCS + size-tiered segment selection
//! - [`CompactionExecutor`] — merge-sort compaction with deduplication
//! - [`CompactionLevel`] — segment tiering (L0, L1, L2)
//! - [`CompactionTask`] — description of a pending compaction job
//! - [`CompactionPolicy`] — per-namespace compaction configuration
//! - [`CompactionStrategy`] — strategy selection (TWCS, SizeTiered, Hybrid)
//! - [`SizeTier`] — segment size classification (Tiny, Small, Medium, Large)
//! - [`WriteAmpTracker`] — write-amplification budget tracking
//! - [`SegmentState`] — lifecycle state machine for segment files

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod error;
pub mod executor;
pub mod picker;

pub use error::CompactionError;
pub use executor::CompactionExecutor;
pub use picker::{
    CompactionLevel, CompactionPicker, CompactionPolicy, CompactionStrategy, CompactionTask,
    SegmentState, SizeTier, WriteAmpTracker,
};
