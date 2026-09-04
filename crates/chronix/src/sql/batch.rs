//! Shaping a storage batch into the batch SQL and the archive both expect.
//!
//! The storage layer emits `timestamp: Int64` and its own canonical column
//! order; every consumer above it wants `_time: Timestamp(ns)` and the table
//! schema's order. That mapping lives here once.
//!
//! It lives here because it had **two** implementations, and they drifted: the
//! SQL scan converted the timestamp column and the cold archive did not, so
//! the same measurement was `_time` when queried hot and `timestamp` when
//! queried from the archive, and `time_bucket` worked on one and not the
//! other.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::DataFusionError;

/// Convert the "timestamp" / "time" Int64 column to `_time` Timestamp(Nanosecond).
pub(crate) fn convert_timestamp_column(batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
    if batch.num_rows() == 0 {
        // Return empty batch — avoid schema mismatches.
        return Ok(batch);
    }

    let schema = batch.schema();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        if (field.name() == "timestamp" || field.name() == "time")
            && *field.data_type() == DataType::Int64
        {
            let ts = compute::cast(col, &DataType::Timestamp(TimeUnit::Nanosecond, None))
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            new_fields.push(
                arrow::datatypes::Field::new(
                    "_time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                )
                .into(),
            );
            new_columns.push(ts);
        } else {
            new_fields.push(field.clone());
            new_columns.push(col.clone());
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Reorder/select `batch`'s columns so the result matches `target` exactly,
/// resolving columns **by name**.
///
/// Columns present in `target` but absent from `batch` become all-null arrays
/// of the target type — this is the correct reading for a measurement whose
/// schema has gained a field that this particular segment predates.
///
/// This replaces an index-based `RecordBatch::project`, which assumed
/// the storage batch and the DataFusion table schema agreed on column order.
/// They do not: storage emits `timestamp, tags…, fields…` each sorted by name,
/// while the table schema follows schema-registration order.
pub(crate) fn align_batch_to_schema(
    batch: &RecordBatch,
    target: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let src = batch.schema();

    // Fast path: already identical, no work to do.
    if src.fields().len() == target.fields().len()
        && src
            .fields()
            .iter()
            .zip(target.fields())
            .all(|(a, b)| a.name() == b.name())
    {
        return Ok(batch.clone());
    }

    let index: HashMap<&str, usize> = src
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();

    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        match index.get(field.name().as_str()) {
            Some(&i) => {
                let col = batch.column(i);
                // Storage may hand back a narrower/wider numeric type than the
                // table schema advertises; cast rather than fail the query.
                if col.data_type() == field.data_type() {
                    columns.push(Arc::clone(col));
                } else {
                    columns.push(
                        compute::cast(col, field.data_type())
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                    );
                }
            }
            None => columns.push(arrow::array::new_null_array(
                field.data_type(),
                batch.num_rows(),
            )),
        }
    }

    // The row count is carried explicitly rather than inferred from the
    // columns, because an aggregate that reads no column — `count(*)` — pushes
    // an **empty** projection down, and Arrow refuses a zero-column batch
    // unless it is told how many rows it stands for. Inferring it made the
    // most ordinary SQL query there is fail with "must either specify a row
    // count or at least one column".
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(target), columns, &options)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Shape a read-path batch into an archive object's schema.
///
/// The two steps the SQL scan also takes, in the same order: rename and retype
/// the timestamp column, then resolve columns by name against the target.
///
/// # Errors
///
/// Returns an error if a column cannot be cast to its target type.
#[cfg(feature = "object-store")]
pub(crate) fn to_archive_batch(
    batch: &RecordBatch,
    target: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let converted = convert_timestamp_column(batch.clone())?;
    align_batch_to_schema(&converted, target)
}

// The archive conversion only exists behind `object-store`, and so do its
// tests — an unconditional test module would import a function that is not
// compiled.
#[cfg(all(test, feature = "object-store"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    fn storage_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("watts", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap()
    }

    /// The archive must get `_time`, the target's column order, and nulls for
    /// a field this batch predates.
    #[test]
    fn archive_batch_matches_the_target_schema() {
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("amps", DataType::Float64, true),
            Field::new("watts", DataType::Float64, true),
        ]));

        let out = to_archive_batch(&storage_batch(), &target).unwrap();
        assert_eq!(out.schema(), target, "the archive schema is the contract");
        assert_eq!(out.num_rows(), 2);
        assert!(
            out.column(2).null_count() == 2,
            "a field the batch predates must be null, not absent"
        );
    }
}
