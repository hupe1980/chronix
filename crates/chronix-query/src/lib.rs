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

/// The inverse of [`column_type_to_arrow`], for one cell.
///
/// `Ok(None)` means the cell is null. `Err` means the column's Arrow type is
/// not one the storage layer can hold — which is a *schema* mistake by the
/// caller, not a value, and must not be mistaken for "no data".
///
/// Callers *store* what this produces — a distributed read, and a Raft region
/// snapshot — so it is one total conversion rather than a `match` at each
/// site. A quiet arm there drops a field from a point with nothing to say so.
///
/// # Errors
///
/// Returns [`QueryError::Validation`] when the Arrow type has no
/// [`ColumnType`](chronix_core::ColumnType), or when a decimal carries a
/// negative scale — legal in Arrow, unrepresentable in storage.
pub fn arrow_cell_to_field_value(
    col: &dyn arrow::array::Array,
    index: usize,
) -> error::Result<Option<chronix_core::FieldValue>> {
    use arrow::array::{
        BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray, UInt64Array,
    };
    use arrow::datatypes::DataType;
    use chronix_core::FieldValue;

    if col.is_null(index) {
        return Ok(None);
    }

    let bad = |what: &str| {
        QueryError::Validation(format!(
            "column has {what}, which no Chronix column type can hold"
        ))
    };
    let mismatch = || bad("an array that disagrees with its own Arrow type");

    // The six storage types, and nothing else. `Timestamp` shares `Int64`
    // with `I64`; a timestamp *field* is an `I64` value here, which is what
    // the schema layer stores it as.
    let v = match col.data_type() {
        DataType::Float64 => FieldValue::F64(
            col.as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(mismatch)?
                .value(index),
        ),
        DataType::Int64 => FieldValue::I64(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(mismatch)?
                .value(index),
        ),
        DataType::UInt64 => FieldValue::U64(
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(mismatch)?
                .value(index),
        ),
        DataType::Boolean => FieldValue::Bool(
            col.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(mismatch)?
                .value(index),
        ),
        DataType::Utf8 => FieldValue::String(
            col.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mismatch)?
                .value(index)
                .to_owned(),
        ),
        DataType::Decimal128(_, scale) => {
            let arr = col
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(mismatch)?;
            let scale = u8::try_from(*scale).map_err(|_| bad("a decimal with a negative scale"))?;
            FieldValue::Decimal(
                chronix_core::Decimal::new(arr.value(index), scale)
                    .map_err(|e| QueryError::Validation(format!("decimal cell: {e}")))?,
            )
        }
        other => return Err(bad(&format!("Arrow type {other}"))),
    };
    Ok(Some(v))
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod cell_tests {
    use super::{arrow_cell_to_field_value, column_type_to_arrow};
    use arrow::array::{
        BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray, UInt64Array,
    };
    use chronix_core::{ColumnType, FieldValue};

    #[test]
    fn every_storage_type_survives_the_round_trip() {
        // The property that matters: whatever `column_type_to_arrow` can
        // produce, this can read back. A type missing here is a field that
        // disappears from a replicated region.
        let cases: Vec<(ColumnType, arrow::array::ArrayRef, FieldValue)> = vec![
            (
                ColumnType::F64,
                std::sync::Arc::new(Float64Array::from(vec![1.5])),
                FieldValue::F64(1.5),
            ),
            (
                ColumnType::I64,
                std::sync::Arc::new(Int64Array::from(vec![i64::MIN])),
                FieldValue::I64(i64::MIN),
            ),
            (
                ColumnType::U64,
                std::sync::Arc::new(UInt64Array::from(vec![u64::MAX])),
                FieldValue::U64(u64::MAX),
            ),
            (
                ColumnType::Bool,
                std::sync::Arc::new(BooleanArray::from(vec![true])),
                FieldValue::Bool(true),
            ),
            (
                ColumnType::String,
                std::sync::Arc::new(StringArray::from(vec!["s"])),
                FieldValue::String("s".to_string()),
            ),
            (
                ColumnType::Decimal { scale: 4 },
                std::sync::Arc::new(
                    Decimal128Array::from(vec![12_345_i128])
                        .with_precision_and_scale(chronix_core::DECIMAL_PRECISION, 4)
                        .unwrap(),
                ),
                FieldValue::Decimal(chronix_core::Decimal::new(12_345, 4).unwrap()),
            ),
        ];

        for (ct, arr, want) in cases {
            assert_eq!(
                arr.data_type(),
                &column_type_to_arrow(ct),
                "test array does not match {ct:?}"
            );
            let got = arrow_cell_to_field_value(arr.as_ref(), 0).unwrap();
            assert_eq!(got, Some(want), "round trip failed for {ct:?}");
        }
    }

    #[test]
    fn a_null_cell_is_absent_not_an_error() {
        let arr = Float64Array::from(vec![None::<f64>]);
        assert_eq!(arrow_cell_to_field_value(&arr, 0).unwrap(), None);
    }

    #[test]
    fn a_type_storage_cannot_hold_is_an_error_not_a_silent_drop() {
        // The defect this exists for: `_ => None` read as "no value here"
        // for a column that in fact held one.
        let arr = arrow::array::Int32Array::from(vec![1]);
        let err = arrow_cell_to_field_value(&arr, 0).unwrap_err();
        assert!(
            err.to_string().contains("Int32"),
            "the error should name the type: {err}"
        );
    }

    #[test]
    fn a_negative_scale_is_an_error_not_a_dropped_column() {
        // Legal in Arrow, unrepresentable in storage. It used to be read
        // into a `u8`, fail, and take the whole cell with it.
        let arr = Decimal128Array::from(vec![123_i128])
            .with_precision_and_scale(10, -2)
            .unwrap();
        let err = arrow_cell_to_field_value(&arr, 0).unwrap_err();
        assert!(
            err.to_string().contains("negative scale"),
            "the error should say why: {err}"
        );
    }
}
