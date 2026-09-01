//! # chronix-query
//!
//! Query engine for the Chronix time-series database.
//!
//! This crate implements the full query pipeline:
//!
//! 1. **Planning** — [`QueryPlan`] and [`QueryBuilder`] for fluent query construction
//! 2. **Pruning** — Multi-level segment elimination (time, bloom, stats)
//! 3. **Projection** — Read only the columns needed by the query
//! 4. **Filtering** — Vectorized predicate evaluation on Arrow arrays
//! 5. **Deduplication** — Sort-merge dedup for overlapping segments
//! 6. **Aggregation** — count, sum, min, max, avg, first, last
//! 7. **Downsampling** — Time-bucket aggregation
//! 8. **Window functions** — row_number, rank, lead, lag, delta, rate, irate,
//!    moving average, cumulative sum, increase
//!
//! All results are returned as Apache Arrow `RecordBatch` for zero-copy
//! interoperability with data processing ecosystems.

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

pub mod aggregate;
pub mod convert;
pub mod dedup;
pub mod downsample;
pub mod encoded_pushdown;
pub mod error;
pub mod filter;
pub mod memory;
pub mod plan;
pub mod pruning;
pub mod window;

pub use aggregate::AggFn;
pub use convert::points_to_record_batch;
pub use encoded_pushdown::{ColumnPredicate, EncodedDomainEvaluator, RowGroupVerdict};
pub use error::QueryError;
pub use memory::MemoryTracker;
pub use plan::{FieldPredicate, QueryBuilder, QueryPlan, ZoneMapOp};
pub use window::WindowFn;

/// Consolidated Arrow column → `f64` extraction.
///
/// Supports `Float64Array`, `Int64Array`, and `UInt64Array`. Returns
/// `None` for null values or unsupported types.
pub fn extract_f64(col: &dyn arrow::array::Array, index: usize) -> Option<f64> {
    use arrow::array::{Float64Array, Int64Array, UInt64Array};
    if col.is_null(index) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.value(index))
        .or_else(|| {
            col.as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(index) as f64)
        })
        .or_else(|| {
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .map(|a| a.value(index) as f64)
        })
}
