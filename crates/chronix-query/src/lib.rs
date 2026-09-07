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
pub use aggregate::{AggResult, DecimalTrack, Num};
pub use convert::points_to_record_batch;
pub use encoded_pushdown::{ColumnPredicate, EncodedDomainEvaluator, RowGroupVerdict};
pub use error::QueryError;
pub use memory::MemoryTracker;
pub use plan::{FieldPredicate, QueryBuilder, QueryPlan, ZoneMapOp};
pub use window::WindowFn;

/// The Arrow type a schema column decodes to — one answer, in one place.
///
/// It was four copies of this match (the scan schema, the SQL table
/// provider, the server's schema endpoint, the segment reader), each ending
/// in a catch-all that mapped anything it did not name to a string. That is
/// harmless while every unnamed type *is* a string, and turns a decimal
/// column into `Utf8` the moment one is not.
///
/// `Timestamp` maps to `Int64`, the storage representation. The SQL layer
/// presents the time column as `Timestamp(Nanosecond)` and converts on the
/// way out; see `chronix::sql`.
#[must_use]
pub fn column_type_to_arrow(ct: chronix_core::ColumnType) -> arrow::datatypes::DataType {
    use arrow::datatypes::DataType;
    use chronix_core::ColumnType;
    match ct {
        ColumnType::Timestamp | ColumnType::I64 => DataType::Int64,
        ColumnType::U64 => DataType::UInt64,
        ColumnType::F64 => DataType::Float64,
        ColumnType::Bool => DataType::Boolean,
        ColumnType::String => DataType::Utf8,
        ColumnType::Decimal { scale } => DataType::Decimal128(
            chronix_core::DECIMAL_PRECISION,
            i8::try_from(scale).unwrap_or(0),
        ),
    }
}

/// Consolidated Arrow column → `f64` extraction.
///
/// Supports `Float64Array`, `Int64Array`, `UInt64Array` and
/// `Decimal128Array`. Returns `None` for null values or unsupported types.
///
/// # The one place a decimal becomes approximate
///
/// Storing or aggregating a `Decimal128` column keeps it exact: the segment
/// holds the mantissa, `sum`/`min`/`max`/`first`/`last` fold in `i128`, and
/// SQL sees `Decimal128(38, s)`.
///
/// What comes through here is the other kind of question — a moving average,
/// a percentile, a forecast, an anomaly score, a chart's downsample, a
/// PromQL sample. Those are defined in floating point and their answers are
/// estimates whatever the input was, so converting is honest. Keeping it in
/// one named function leaves one boundary to audit rather than an `as f64`
/// in every module.
#[allow(clippy::cast_precision_loss)]
pub fn extract_f64(col: &dyn arrow::array::Array, index: usize) -> Option<f64> {
    use arrow::array::{Decimal128Array, Float64Array, Int64Array, UInt64Array};
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
        .or_else(|| {
            col.as_any().downcast_ref::<Decimal128Array>().map(|a| {
                let scale = u32::try_from(a.scale()).unwrap_or(0);
                a.value(index) as f64 / chronix_core::pow10(scale).unwrap_or(1) as f64
            })
        })
}
