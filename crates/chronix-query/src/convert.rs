//! Conversion from [`Point`] collections to Arrow [`RecordBatch`].
//!
//! This module bridges the chronix-core `Point` type and Apache Arrow
//! columnar format, enabling zero-copy interop between the memtable
//! (row-oriented) and the query engine (columnar).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use chronix_core::types::{FieldValue, Point};

use crate::error::{QueryError, Result};

/// Column role in the output schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnRole {
    Timestamp,
    Tag,
    Field(FieldType),
}

/// Field value type for schema inference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldType {
    I64,
    U64,
    F64,
    Bool,
    String,
}

/// A discovered column from point data.
#[derive(Debug)]
struct Column {
    name: String,
    role: ColumnRole,
}

/// Convert a slice of [`Point`]s to an Arrow [`RecordBatch`].
///
/// The resulting batch has the same columnar structure as what
/// `SegmentReader::read_all()` produces:
/// - `timestamp` column (Int64)
/// - One column per tag key (Utf8)
/// - One column per field name (typed: I64, U64, F64, Bool, or Utf8)
///
/// Points are assumed to all belong to the same measurement.
///
/// # Errors
///
/// Returns [`QueryError::Arrow`] if the `RecordBatch` cannot be constructed.
pub fn points_to_record_batch(points: &[Point]) -> Result<RecordBatch> {
    if points.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    // Discover schema by scanning all points
    let columns = discover_columns(points);

    // Build Arrow arrays
    let mut arrow_arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());

    for col in &columns {
        match col.role {
            ColumnRole::Timestamp => {
                let values: Vec<i64> = points.iter().map(Point::timestamp).collect();
                arrow_arrays.push(Arc::new(Int64Array::from(values)));
                fields.push(Field::new(&col.name, DataType::Int64, true));
            }
            ColumnRole::Tag => {
                let values: Vec<Option<&str>> = points
                    .iter()
                    .map(|p| p.series_key().tag(&col.name))
                    .collect();
                arrow_arrays.push(Arc::new(StringArray::from(values)));
                fields.push(Field::new(&col.name, DataType::Utf8, true));
            }
            ColumnRole::Field(ft) => match ft {
                FieldType::I64 => {
                    let values: Vec<Option<i64>> = points
                        .iter()
                        .map(|p| match p.field(&col.name) {
                            Some(FieldValue::I64(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();
                    arrow_arrays.push(Arc::new(Int64Array::from(values)));
                    fields.push(Field::new(&col.name, DataType::Int64, true));
                }
                FieldType::U64 => {
                    let values: Vec<Option<u64>> = points
                        .iter()
                        .map(|p| match p.field(&col.name) {
                            Some(FieldValue::U64(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();
                    arrow_arrays.push(Arc::new(UInt64Array::from(values)));
                    fields.push(Field::new(&col.name, DataType::UInt64, true));
                }
                FieldType::F64 => {
                    let values: Vec<Option<f64>> = points
                        .iter()
                        .map(|p| match p.field(&col.name) {
                            Some(FieldValue::F64(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();
                    arrow_arrays.push(Arc::new(Float64Array::from(values)));
                    fields.push(Field::new(&col.name, DataType::Float64, true));
                }
                FieldType::Bool => {
                    let values: Vec<Option<bool>> = points
                        .iter()
                        .map(|p| match p.field(&col.name) {
                            Some(FieldValue::Bool(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();
                    arrow_arrays.push(Arc::new(BooleanArray::from(values)));
                    fields.push(Field::new(&col.name, DataType::Boolean, true));
                }
                FieldType::String => {
                    let values: Vec<Option<String>> = points
                        .iter()
                        .map(|p| match p.field(&col.name) {
                            Some(FieldValue::String(v)) => Some(v.clone()),
                            _ => None,
                        })
                        .collect();
                    let refs: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
                    arrow_arrays.push(Arc::new(StringArray::from(refs)));
                    fields.push(Field::new(&col.name, DataType::Utf8, true));
                }
            },
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrow_arrays).map_err(QueryError::from)
}

/// Discover column definitions by scanning all points.
///
/// Returns columns in deterministic order: timestamp first, then tags
/// (sorted), then fields (sorted).
fn discover_columns(points: &[Point]) -> Vec<Column> {
    let mut tag_keys: BTreeSet<String> = BTreeSet::new();
    let mut field_defs: BTreeMap<String, FieldType> = BTreeMap::new();

    for p in points {
        for key in p.series_key().tag_keys() {
            tag_keys.insert(key.to_string());
        }
        for (name, value) in p.fields() {
            let ft = match value {
                FieldValue::I64(_) => FieldType::I64,
                FieldValue::U64(_) => FieldType::U64,
                FieldValue::F64(_) => FieldType::F64,
                FieldValue::Bool(_) => FieldType::Bool,
                FieldValue::String(_) => FieldType::String,
            };
            // Detect type conflicts across points and widen
            // numeric types automatically (I64/U64 → F64) rather than
            // silently using the first-seen type.
            field_defs
                .entry(name.to_string())
                .and_modify(|existing| {
                    if *existing != ft {
                        // Widen numeric types to F64 (the most general
                        // numeric representation). Non-numeric conflicts
                        // (e.g. Bool vs String) default to String.
                        *existing = match (*existing, ft) {
                            (FieldType::I64, FieldType::F64)
                            | (FieldType::F64, FieldType::I64)
                            | (FieldType::U64, FieldType::F64)
                            | (FieldType::F64, FieldType::U64)
                            | (FieldType::I64, FieldType::U64)
                            | (FieldType::U64, FieldType::I64) => FieldType::F64,
                            _ => FieldType::String,
                        };
                    }
                })
                .or_insert(ft);
        }
    }

    let mut columns = Vec::new();

    // Timestamp first
    columns.push(Column {
        name: "timestamp".to_string(),
        role: ColumnRole::Timestamp,
    });

    // Tags sorted
    for key in &tag_keys {
        columns.push(Column {
            name: key.clone(),
            role: ColumnRole::Tag,
        });
    }

    // Fields sorted
    for (name, ft) in &field_defs {
        columns.push(Column {
            name: name.clone(),
            role: ColumnRole::Field(*ft),
        });
    }

    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use chronix_core::types::SeriesKey;

    fn make_point(measurement: &str, host: &str, ts: i64, value: f64) -> Point {
        let tags: BTreeMap<String, String> = [("host".to_string(), host.to_string())]
            .into_iter()
            .collect();
        let series_key = SeriesKey::new(measurement.to_string(), tags).unwrap();
        let fields: BTreeMap<String, FieldValue> = [("value".to_string(), FieldValue::F64(value))]
            .into_iter()
            .collect();
        Point::new(series_key, fields, ts).unwrap()
    }

    #[test]
    fn empty_points_returns_empty_batch() {
        let batch = points_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn single_point_roundtrip() {
        let points = vec![make_point("cpu", "server-1", 1000, 42.5)];
        let batch = points_to_record_batch(&points).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3); // timestamp, host, value

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 1000);

        let host = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(host.value(0), "server-1");

        let value = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((value.value(0) - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn multiple_points_same_series() {
        let points: Vec<Point> = (0..10)
            .map(|i| make_point("cpu", "a", i * 100, i as f64))
            .collect();
        let batch = points_to_record_batch(&points).unwrap();

        assert_eq!(batch.num_rows(), 10);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn multiple_series_different_tags() {
        let points = vec![
            make_point("cpu", "a", 100, 1.0),
            make_point("cpu", "b", 200, 2.0),
        ];
        let batch = points_to_record_batch(&points).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let host = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(host.value(0), "a");
        assert_eq!(host.value(1), "b");
    }

    #[test]
    fn multiple_field_types() {
        let tags: BTreeMap<String, String> = BTreeMap::new();
        let key = SeriesKey::new("m".to_string(), tags).unwrap();
        let fields: BTreeMap<String, FieldValue> = [
            ("f_i64".to_string(), FieldValue::I64(42)),
            ("f_u64".to_string(), FieldValue::U64(99)),
            ("f_f64".to_string(), FieldValue::F64(3.125)),
            ("f_bool".to_string(), FieldValue::Bool(true)),
            ("f_str".to_string(), FieldValue::String("hello".into())),
        ]
        .into_iter()
        .collect();
        let point = Point::new(key, fields, 1000).unwrap();

        let batch = points_to_record_batch(&[point]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // timestamp + 5 fields = 6 columns (no tags)
        assert_eq!(batch.num_columns(), 6);

        // Verify schema field types
        let schema = batch.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64); // timestamp
        assert_eq!(schema.field(1).data_type(), &DataType::Boolean); // f_bool
        assert_eq!(schema.field(2).data_type(), &DataType::Float64); // f_f64
        assert_eq!(schema.field(3).data_type(), &DataType::Int64); // f_i64
        assert_eq!(schema.field(4).data_type(), &DataType::Utf8); // f_str
        assert_eq!(schema.field(5).data_type(), &DataType::UInt64); // f_u64
    }

    #[test]
    fn schema_order_is_deterministic() {
        let tags: BTreeMap<String, String> = [
            ("dc".to_string(), "us".to_string()),
            ("host".to_string(), "a".to_string()),
        ]
        .into_iter()
        .collect();
        let key = SeriesKey::new("cpu".to_string(), tags).unwrap();
        let fields: BTreeMap<String, FieldValue> = [
            ("z_field".to_string(), FieldValue::F64(1.0)),
            ("a_field".to_string(), FieldValue::F64(2.0)),
        ]
        .into_iter()
        .collect();
        let point = Point::new(key, fields, 1000).unwrap();

        let batch = points_to_record_batch(&[point]).unwrap();
        let schema = batch.schema();

        // Order: timestamp, dc, host (tags sorted), a_field, z_field (fields sorted)
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "dc");
        assert_eq!(schema.field(2).name(), "host");
        assert_eq!(schema.field(3).name(), "a_field");
        assert_eq!(schema.field(4).name(), "z_field");
    }
}
