//! Vectorized predicate filtering on Arrow arrays.
//!
//! Applies time-range and tag-equality filters to Arrow `RecordBatch`es,
//! producing filtered batches containing only matching rows.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::compute;
use arrow::compute::kernels::boolean as arrow_bool;
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;

use crate::error::{QueryError, Result};
use chronix_core::TombstoneSet;

/// Apply a time range filter: `timestamp >= start AND timestamp <= end`.
///
/// Returns a `BooleanArray` mask where `true` indicates a matching row.
///
/// # Errors
///
/// Returns an error if the column is not an `Int64Array`.
pub fn time_range_mask(timestamps: &Int64Array, start: i64, end: i64) -> Result<BooleanArray> {
    let gte = compute::kernels::cmp::gt_eq(timestamps, &Int64Array::new_scalar(start))?;
    let lte = compute::kernels::cmp::lt_eq(timestamps, &Int64Array::new_scalar(end))?;
    let combined = arrow_bool::and(&gte, &lte)?;
    Ok(combined)
}

/// Apply a tag equality filter: `column == value`.
///
/// Returns a `BooleanArray` mask where `true` indicates a matching row.
///
/// # Errors
///
/// Returns an error if the column is not a `StringArray`.
pub fn tag_equality_mask(column: &StringArray, value: &str) -> Result<BooleanArray> {
    let scalar = StringArray::new_scalar(value);
    let mask = compute::kernels::cmp::eq(column, &scalar)?;
    Ok(mask)
}

/// Dictionary-encoded equality filter using Arrow's vectorized `eq` kernel.
///
/// Arrow's comparison kernels handle `DictionaryArray` natively — they
/// resolve dictionary indices internally and use SIMD-friendly comparison
/// paths.  This replaces the previous manual row-by-row index matching
/// with a single kernel call.
///
/// Falls back to cast→StringArray if the dictionary value type is not Utf8 or
/// the key type is not Int32.
pub fn dict_equality_mask(col: &dyn Array, value: &str) -> Result<BooleanArray> {
    // Fast path for the common Dictionary<Int32, Utf8> case:
    // Arrow's eq kernel handles DictionaryArray directly.
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
    {
        let scalar = StringArray::new_scalar(value);
        let mask = compute::kernels::cmp::eq(dict, &scalar)?;
        return Ok(mask);
    }

    // Fallback: cast to Utf8 for other dictionary key/value type combos.
    let casted = compute::cast(col, &DataType::Utf8)?;
    let str_arr = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            QueryError::Validation("dictionary column could not be cast to Utf8".into())
        })?;
    tag_equality_mask(str_arr, value)
}

/// Combine multiple boolean masks with AND.
///
/// Returns the intersection of all masks.
///
/// # Errors
///
/// Returns an error if masks have different lengths.
pub fn combine_masks(masks: &[BooleanArray]) -> Result<BooleanArray> {
    if masks.is_empty() {
        return Err(QueryError::Validation("no masks to combine".into()));
    }

    let mut result = masks[0].clone();
    for mask in &masks[1..] {
        result = arrow_bool::and(&result, mask)?;
    }
    Ok(result)
}

/// Apply a boolean mask to a `RecordBatch`, returning only matching rows.
///
/// # Errors
///
/// Returns an error if the filter operation fails.
pub fn apply_filter(batch: &RecordBatch, mask: &BooleanArray) -> Result<RecordBatch> {
    let filtered_columns: std::result::Result<Vec<_>, _> = batch
        .columns()
        .iter()
        .map(|col| compute::filter(col.as_ref(), mask))
        .collect();
    let filtered_columns = filtered_columns?;

    let batch = RecordBatch::try_new(batch.schema(), filtered_columns)?;
    Ok(batch)
}

/// Apply time range and tag filters to a `RecordBatch`.
///
/// This is the main entry point for the filter pipeline. It:
/// 1. Applies the time range filter on the `"timestamp"` column
/// 2. Applies tag equality filters on the specified columns
/// 3. Combines all masks with AND
/// 4. Filters the batch
///
/// Returns the filtered `RecordBatch`.
///
/// # Errors
///
/// Returns an error if columns are not found or have wrong types.
pub fn filter_batch(
    batch: &RecordBatch,
    time_start: i64,
    time_end: i64,
    tag_filters: &[(&str, &str)],
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let mut masks: Vec<BooleanArray> = Vec::new();

    // Time range filter
    let time_col = batch
        .column_by_name("timestamp")
        .ok_or_else(|| QueryError::Validation("'timestamp' column not found".into()))?;
    let timestamps = time_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| QueryError::Validation("'timestamp' column is not Int64".into()))?;

    // Only apply time filter if the range isn't "everything"
    if time_start != i64::MIN || time_end != i64::MAX {
        masks.push(time_range_mask(timestamps, time_start, time_end)?);
    }

    // Tag equality filters
    for (key, value) in tag_filters {
        if let Some(col) = batch.column_by_name(key) {
            // Handle tag filtering on various string
            // representations. For Dictionary-encoded columns, operate on
            // encoded indices directly to avoid string materialization.
            let mask = match col.data_type() {
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|s| tag_equality_mask(s, value)),
                DataType::Dictionary(_, _) => Some(dict_equality_mask(col.as_ref(), value)),
                DataType::LargeUtf8 | DataType::Utf8View => {
                    compute::cast(col.as_ref(), &DataType::Utf8)
                        .ok()
                        .and_then(|arr| arr.as_any().downcast_ref::<StringArray>().cloned())
                        .map(|s| tag_equality_mask(&s, value))
                }
                _ => None,
            };
            match mask {
                Some(Ok(m)) => masks.push(m),
                Some(Err(e)) => return Err(e),
                None => {
                    // Column is not any string type — no row can match.
                    return apply_filter(batch, &BooleanArray::from(vec![false; batch.num_rows()]));
                }
            }
        } else {
            // Column does not exist in batch — no row can match this filter.
            // Return an empty batch immediately.
            return apply_filter(batch, &BooleanArray::from(vec![false; batch.num_rows()]));
        }
    }

    if masks.is_empty() {
        return Ok(batch.clone());
    }

    let combined = combine_masks(&masks)?;
    apply_filter(batch, &combined)
}

/// Build a `RecordBatch` with the specified column projection.
///
/// Selects only the named columns from the input batch. Always includes `"timestamp"`.
///
/// # Errors
///
/// Returns an error if the batch cannot be constructed.
pub fn project_batch(batch: &RecordBatch, projection: &[String]) -> Result<RecordBatch> {
    if projection.is_empty() {
        return Ok(batch.clone());
    }

    // Always include timestamp column
    let mut col_names: Vec<&str> = vec!["timestamp"];
    for name in projection {
        if name != "timestamp" {
            col_names.push(name);
        }
    }

    let mut fields = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    for name in &col_names {
        if let Ok(idx) = batch.schema().index_of(name) {
            fields.push(batch.schema().field(idx).clone());
            columns.push(Arc::clone(batch.column(idx)));
        }
    }

    if fields.is_empty() {
        return Ok(batch.clone());
    }

    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let projected = RecordBatch::try_new(schema, columns)?;
    Ok(projected)
}

/// Filter out rows belonging to tombstoned series.
///
/// For each row, the canonical series key is computed from `(measurement, tag columns)`
/// using the same format as [`chronix_core::SeriesKey::canonical_form()`]:
/// `measurement\0tag1=v1\0tag2=v2` (tags sorted by name).
/// Rows whose canonical key appears in `tombstones` are excluded.
///
/// When `tag_col_names` is provided, only those columns are treated as tags.
/// This is critical when string-typed field columns exist — without explicit
/// tag names, string fields would be misidentified as tags, producing
/// incorrect canonical keys that don't match [`chronix_core::SeriesKey::canonical_form()`].
///
/// If no tombstones exist, the batch is returned unchanged (fast path).
///
/// # Errors
///
/// Returns an error if the filter operation fails.
pub fn filter_tombstoned(
    batch: &RecordBatch,
    measurement: &str,
    tombstones: &TombstoneSet,
    tag_col_names: Option<&[&str]>,
) -> Result<RecordBatch> {
    if tombstones.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    // Resolve tag columns — prefer explicit names when available,
    // fall back to heuristic (all string-like columns except "timestamp").
    let schema = batch.schema();
    let tag_columns: Vec<(usize, &str)> = if let Some(names) = tag_col_names {
        names
            .iter()
            .filter_map(|&name| schema.index_of(name).ok().map(|idx| (idx, name)))
            .collect()
    } else {
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| is_string_like(f.data_type()) && f.name() != "timestamp")
            .map(|(i, f)| (i, f.name().as_str()))
            .collect()
    };

    // Build per-row boolean mask using canonical series key for
    // collision-proof tombstone matching.
    // Support ranged tombstones via timestamp lookup.
    let ts_col = batch
        .column_by_name("timestamp")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let keep: Vec<bool> = (0..batch.num_rows())
        .map(|row| {
            let canonical = compute_series_canonical(measurement, &tag_columns, batch, row);
            let ts = ts_col.map_or(0, |a| a.value(row));
            !tombstones.is_tombstoned(&canonical, ts)
        })
        .collect();

    // Fast path: if all rows survive, return unchanged
    if keep.iter().all(|&k| k) {
        return Ok(batch.clone());
    }

    let mask = BooleanArray::from(keep);
    apply_filter(batch, &mask)
}

/// Returns `true` if the data type is a string-like type (Utf8, LargeUtf8,
/// Utf8View, or Dictionary with a string value type).
fn is_string_like(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Dictionary(_, _)
    )
}

/// Extract the string value at `row` from a column that may be `Utf8` or
/// `Dictionary<Int32, Utf8>`.
///
/// Returns `None` if the value is null or the column type is unsupported.
fn extract_string_value(arr: &dyn Array, row: usize) -> Option<&str> {
    if arr.is_null(row) {
        return None;
    }
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(s.value(row));
    }
    if let Some(dict) = arr
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
    {
        let values = dict.values().as_any().downcast_ref::<StringArray>()?;
        let key = dict.keys().value(row);
        return Some(values.value(key as usize));
    }
    None
}

/// Compute the canonical series key string for a single row.
///
/// Returns `measurement\0tag1=v1\0tag2=v2` (tags sorted by name),
/// matching [`chronix_core::SeriesKey::canonical_form()`] exactly.
/// Used for collision-proof tombstone matching.
///
/// Handles `Utf8` and `Dictionary<Int32, Utf8>` column types.
fn compute_series_canonical(
    measurement: &str,
    tag_columns: &[(usize, &str)],
    batch: &RecordBatch,
    row: usize,
) -> String {
    let mut tag_pairs: Vec<(&str, &str)> = tag_columns
        .iter()
        .filter_map(|&(col_idx, col_name)| {
            let arr = batch.column(col_idx);
            let value = extract_string_value(arr.as_ref(), row)?;
            Some((col_name, value))
        })
        .collect();
    tag_pairs.sort_unstable_by_key(|&(name, _)| name);

    // The canonical format has exactly one definition — see
    // `chronix_core::push_canonical`. Inlining it here is what let tombstone
    // matching break silently when the separators changed.
    chronix_core::canonical_from_pairs(measurement, tag_pairs.iter().copied())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let timestamps = Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500]));
        let hosts = Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"]));
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));

        RecordBatch::try_new(schema, vec![timestamps, hosts, values]).unwrap()
    }

    #[test]
    fn time_range_filter() {
        let batch = test_batch();
        let filtered = filter_batch(&batch, 200, 400, &[]).unwrap();
        assert_eq!(filtered.num_rows(), 3);

        let times = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(times.value(0), 200);
        assert_eq!(times.value(1), 300);
        assert_eq!(times.value(2), 400);
    }

    #[test]
    fn tag_equality_filter() {
        let batch = test_batch();
        let filtered = filter_batch(&batch, i64::MIN, i64::MAX, &[("host", "a")]).unwrap();
        assert_eq!(filtered.num_rows(), 3);

        let hosts = filtered
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..3 {
            assert_eq!(hosts.value(i), "a");
        }
    }

    #[test]
    fn combined_time_and_tag_filter() {
        let batch = test_batch();
        let filtered = filter_batch(&batch, 200, 400, &[("host", "a")]).unwrap();
        assert_eq!(filtered.num_rows(), 1);

        let times = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(times.value(0), 300);
    }

    #[test]
    fn filter_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::new_empty(schema);
        let result = filter_batch(&batch, 0, 100, &[]).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn project_columns() {
        let batch = test_batch();
        let projected = project_batch(&batch, &["value".to_string()]).unwrap();
        assert_eq!(projected.num_columns(), 2); // timestamp + value
        assert_eq!(projected.schema().field(0).name(), "timestamp");
        assert_eq!(projected.schema().field(1).name(), "value");
    }

    #[test]
    fn project_empty_returns_all() {
        let batch = test_batch();
        let projected = project_batch(&batch, &[]).unwrap();
        assert_eq!(projected.num_columns(), batch.num_columns());
    }

    #[test]
    fn time_range_mask_correctness() {
        let ts = Int64Array::from(vec![10, 20, 30, 40, 50]);
        let mask = time_range_mask(&ts, 20, 40).unwrap();
        let expected = BooleanArray::from(vec![false, true, true, true, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn tag_equality_mask_correctness() {
        let col = StringArray::from(vec!["a", "b", "a", "c", "a"]);
        let mask = tag_equality_mask(&col, "a").unwrap();
        let expected = BooleanArray::from(vec![true, false, true, false, true]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn combine_two_masks() {
        let m1 = BooleanArray::from(vec![true, true, false, true]);
        let m2 = BooleanArray::from(vec![true, false, false, true]);
        let result = combine_masks(&[m1, m2]).unwrap();
        let expected = BooleanArray::from(vec![true, false, false, true]);
        assert_eq!(result, expected);
    }

    #[test]
    fn dict_equality_mask_matches() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        // Build a Dict<Int32, Utf8> array: ["a", "b", "a", "c", "a"]
        let dict: DictionaryArray<Int32Type> = vec!["a", "b", "a", "c", "a"].into_iter().collect();
        let mask = dict_equality_mask(&dict, "a").unwrap();
        let expected = BooleanArray::from(vec![true, false, true, false, true]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn dict_equality_mask_no_match() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        let dict: DictionaryArray<Int32Type> = vec!["a", "b", "c"].into_iter().collect();
        let mask = dict_equality_mask(&dict, "z").unwrap();
        let expected = BooleanArray::from(vec![false, false, false]);
        assert_eq!(mask, expected);
    }

    #[test]
    fn filter_batch_with_dict_column() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        let timestamps = Int64Array::from(vec![100, 200, 300, 400, 500]);
        let hosts: DictionaryArray<Int32Type> = vec!["a", "b", "a", "b", "a"].into_iter().collect();
        let values = arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", hosts.data_type().clone(), false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(timestamps), Arc::new(hosts), Arc::new(values)],
        )
        .unwrap();
        let filtered = filter_batch(&batch, i64::MIN, i64::MAX, &[("host", "a")]).unwrap();
        assert_eq!(filtered.num_rows(), 3);
    }

    #[test]
    fn tombstone_filter_with_dict_column() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let timestamps = Int64Array::from(vec![100, 200, 300]);
        let hosts: DictionaryArray<Int32Type> =
            vec!["host-a", "host-b", "host-a"].into_iter().collect();
        let values = Float64Array::from(vec![1.0, 2.0, 3.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", hosts.data_type().clone(), false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(timestamps), Arc::new(hosts), Arc::new(values)],
        )
        .unwrap();

        // Tombstone for the canonical form of cpu{host=host-a}
        let mut tombstones = TombstoneSet::new();
        tombstones.insert(chronix_core::Tombstone::all_time(
            chronix_core::canonical_from_pairs("cpu", [("host", "host-a")]),
        ));

        let result = filter_tombstoned(&batch, "cpu", &tombstones, Some(&["host"])).unwrap();
        // Only "host-b" rows should survive
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn tombstone_filter_heuristic_detects_dict_columns() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let timestamps = Int64Array::from(vec![100, 200]);
        let hosts: DictionaryArray<Int32Type> = vec!["host-x", "host-y"].into_iter().collect();
        let values = Float64Array::from(vec![1.0, 2.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", hosts.data_type().clone(), false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(timestamps), Arc::new(hosts), Arc::new(values)],
        )
        .unwrap();

        // Use heuristic (no explicit tag_col_names)
        let mut tombstones = TombstoneSet::new();
        tombstones.insert(chronix_core::Tombstone::all_time(
            chronix_core::canonical_from_pairs("cpu", [("host", "host-x")]),
        ));

        let result = filter_tombstoned(&batch, "cpu", &tombstones, None).unwrap();
        assert_eq!(result.num_rows(), 1);
    }
}
