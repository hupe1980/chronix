//! Window functions for time-series query processing.
//!
//! Window functions compute values over a sliding window or partition of
//! rows without collapsing the result set (unlike aggregation, which
//! produces fewer rows). Each input row yields exactly one output value.
//!
//! # Supported functions
//!
//! | Function | Description |
//! |----------|-------------|
//! | `RowNumber` | Sequential 1-based row number within partition |
//! | `Rank` | Rank with gaps (1224) for ties on the order column |
//! | `DenseRank` | Rank without gaps (1223) for ties on the order column |
//! | `Lead(offset, default)` | Value `offset` rows ahead (default if out of range) |
//! | `Lag(offset, default)` | Value `offset` rows behind (default if out of range) |
//! | `Delta` | Difference between current and previous value |
//! | `Rate` | Per-second rate: `delta(value) / delta(timestamp)` |
//! | `IRate` | Instantaneous per-second rate between last two samples |
//! | `MovingAverage(window)` | Simple sliding-window mean |
//! | `CumulativeSum` | Running total |
//! | `Increase` | Monotonic counter increase (resets to 0 on counter wrap) |

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{QueryError, Result};

/// Window function type.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFn {
    /// Sequential 1-based row number within partition.
    RowNumber,
    /// Rank with gaps for ties on the order column.
    ///
    /// When `order_column` is `Some`, rank is determined by that
    /// column (SQL standard `RANK() OVER (ORDER BY ...)`). When `None`,
    /// defaults to the value column for backward compatibility.
    Rank {
        /// Column to determine rank ordering. `None` uses the value column.
        order_column: Option<String>,
    },
    /// Rank without gaps for ties on the order column.
    ///
    /// Same `order_column` semantics as [`Rank`](Self::Rank).
    DenseRank {
        /// Column to determine rank ordering. `None` uses the value column.
        order_column: Option<String>,
    },
    /// Value `offset` rows ahead; `default` if out of range.
    Lead {
        /// Number of rows to look ahead.
        offset: usize,
        /// Default value when the look-ahead extends past the partition end.
        default: f64,
    },
    /// Value `offset` rows behind; `default` if out of range.
    Lag {
        /// Number of rows to look behind.
        offset: usize,
        /// Default value when the look-behind extends before the partition start.
        default: f64,
    },
    /// Difference: `value[i] - value[i-1]` (null for first row).
    Delta,
    /// Per-second rate: `(value[i] - value[i-1]) / (timestamp[i] - timestamp[i-1])`.
    ///
    /// # Timestamp unit assumption
    ///
    /// Timestamps are assumed to be **nanoseconds since the Unix epoch**
    /// (the standard unit throughout Chronix).  The delta is divided by
    /// `1_000_000_000.0` to convert to seconds before computing the rate.
    /// If timestamps use a different unit (e.g. microseconds or millis),
    /// the rate values will be incorrect by the corresponding scale factor.
    Rate,
    /// Instantaneous per-second rate between the last two samples in the
    /// partition. Every row in the partition gets the same value.
    ///
    /// Uses the same nanosecond timestamp assumption as [`Rate`](Self::Rate).
    IRate,
    /// Simple moving average over a sliding window of `window_size` rows.
    MovingAverage {
        /// Number of rows in the sliding window.
        window_size: usize,
    },
    /// Running cumulative sum.
    CumulativeSum,
    /// Monotonic counter increase (resets to 0 on counter wrap-around).
    Increase,
    /// RANGE-frame moving average over a timestamp window.
    ///
    /// For each row, computes the mean of all values whose timestamps fall
    /// within `[current_ts - range_ns, current_ts]`. Uses an efficient
    /// two-pointer O(n) scan (data is sorted by timestamp).
    RangeMovingAverage {
        /// Window size in nanoseconds.
        range_ns: i64,
    },
    /// RANGE-frame sum over a timestamp window.
    RangeSum {
        /// Window size in nanoseconds.
        range_ns: i64,
    },
    /// RANGE-frame count over a timestamp window.
    RangeCount {
        /// Window size in nanoseconds.
        range_ns: i64,
    },
}

/// Human-readable function name for column naming.
#[must_use]
pub fn window_fn_name(func: &WindowFn) -> String {
    match func {
        WindowFn::RowNumber => "row_number".to_string(),
        WindowFn::Rank { .. } => "rank".to_string(),
        WindowFn::DenseRank { .. } => "dense_rank".to_string(),
        WindowFn::Lead { offset, .. } => format!("lead_{offset}"),
        WindowFn::Lag { offset, .. } => format!("lag_{offset}"),
        WindowFn::Delta => "delta".to_string(),
        WindowFn::Rate => "rate".to_string(),
        WindowFn::IRate => "irate".to_string(),
        WindowFn::MovingAverage { window_size } => format!("moving_avg_{window_size}"),
        WindowFn::CumulativeSum => "cumulative_sum".to_string(),
        WindowFn::Increase => "increase".to_string(),
        WindowFn::RangeMovingAverage { range_ns } => format!("range_avg_{range_ns}ns"),
        WindowFn::RangeSum { range_ns } => format!("range_sum_{range_ns}ns"),
        WindowFn::RangeCount { range_ns } => format!("range_count_{range_ns}ns"),
    }
}

/// Extract a value at `index` from a numeric column as `f64`.
#[allow(clippy::cast_precision_loss)]
fn extract_f64(col: &dyn Array, index: usize) -> Option<f64> {
    crate::extract_f64(col, index)
}

/// Bitwise equality for `Option<f64>` so NaN == NaN.
fn f64_opt_eq(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => x.to_bits() == y.to_bits(),
        (None, None) => true,
        _ => false,
    }
}

/// Apply window functions to a `RecordBatch`.
///
/// The input batch is expected to be sorted by timestamp. Window functions
/// are computed over the specified `value_column` and new columns are
/// appended to the result. All original columns are preserved.
///
/// When `partition_by` is non-empty, the batch is logically partitioned by
/// those columns and each window function is computed independently within
/// each partition.
///
/// # Errors
///
/// Returns an error if required columns are missing or have unsupported types.
pub fn apply_window(
    batch: &RecordBatch,
    functions: &[WindowFn],
    partition_by: &[String],
    value_column: &str,
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<RecordBatch> {
    if functions.is_empty() {
        return Ok(batch.clone());
    }

    let schema = batch.schema();
    let n = batch.num_rows();

    // Pre-check memory budget for window function output.
    // Each window function adds one Float64 column (8 bytes/row).
    if let Some(tracker) = memory_tracker {
        let estimated_bytes = n * functions.len() * 8 + n * 16; // result cols + overhead
        tracker.try_allocate(estimated_bytes)?;
    }

    // Locate the value column
    let val_idx = schema
        .index_of(value_column)
        .map_err(|_| QueryError::FieldNotFound {
            measurement: String::new(),
            field: value_column.to_string(),
        })?;
    let val_col = batch.column(val_idx).as_ref();

    // Locate the timestamp column (needed for Rate / IRate)
    let ts_col = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned());

    // Validate that timestamps look like nanoseconds when a
    // rate function is requested. Modern nanosecond epoch timestamps
    // (post-2001) are >= ~9.78e17. Values in the seconds range
    // (< ~2e10 for modern dates) that also fall in the plausible
    // second-epoch range indicate a caller accidentally passed seconds.
    // We check that the *largest* timestamp is at least in the
    // nanosecond-plausible range; tiny timestamps from synthetic test
    // data are allowed since the rate arithmetic is unit-correct
    // regardless of the epoch offset.
    let needs_ts = functions.iter().any(|f| {
        matches!(
            f,
            WindowFn::Rate
                | WindowFn::IRate
                | WindowFn::RangeMovingAverage { .. }
                | WindowFn::RangeSum { .. }
                | WindowFn::RangeCount { .. }
        )
    });
    if needs_ts {
        if let Some(ref ts) = ts_col {
            if !ts.is_empty() {
                // Find the maximum timestamp to judge the unit
                let max_ts = (0..ts.len()).map(|i| ts.value(i)).max().unwrap_or(0);
                // Seconds epoch for year ~2001 → ~978_307_200;
                // Nanosecond epoch for same → ~978_307_200_000_000_000.
                // If max is in [1_000_000_000, 100_000_000_000) it looks
                // like seconds (1970-2001..~5138). Warn the caller.
                if (1_000_000_000..100_000_000_000).contains(&max_ts) {
                    tracing::warn!(
                        max_ts,
                        "timestamps may be in seconds instead of nanoseconds; \
                         Rate/IRate assume nanosecond timestamps"
                    );
                }
            }
        }
    }

    // Build partitions: Vec<Vec<usize>> — each inner Vec holds row indices
    let partitions = build_partitions(batch, partition_by)?;

    // Compute each window function
    let mut new_fields: Vec<Field> = Vec::with_capacity(functions.len());
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(functions.len());

    for func in functions {
        let col_name = format!("{value_column}_{}", window_fn_name(func));
        new_fields.push(Field::new(&col_name, DataType::Float64, true));

        let mut result = vec![None; n];

        for partition in &partitions {
            compute_window_fn(
                func,
                partition,
                val_col,
                ts_col.as_ref(),
                batch,
                &mut result,
            )?;
        }

        let arr: Float64Array = result.into_iter().collect();
        new_columns.push(Arc::new(arr));
    }

    // Build output: original columns + new window columns
    let mut out_fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    out_fields.extend(new_fields);

    let mut out_columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|i| batch.column(i).clone())
        .collect();
    out_columns.extend(new_columns);

    let out_schema = Arc::new(Schema::new(out_fields));
    let out_batch = RecordBatch::try_new(out_schema, out_columns)?;
    if let Some(tracker) = memory_tracker {
        tracker.try_allocate(out_batch.get_array_memory_size())?;
    }
    Ok(out_batch)
}

/// Build partitions from the batch based on `partition_by` columns.
///
/// If `partition_by` is empty, returns a single partition with all row indices.
fn build_partitions(batch: &RecordBatch, partition_by: &[String]) -> Result<Vec<Vec<usize>>> {
    let n = batch.num_rows();

    if partition_by.is_empty() {
        return Ok(vec![(0..n).collect()]);
    }

    use arrow::array::StringArray;
    use std::collections::HashMap;

    let part_cols: Vec<&StringArray> = partition_by
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    QueryError::Validation(format!(
                        "partition-by column '{name}' not found or not string"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    // Group rows by partition key (uses hashing like aggregate_grouped)
    let mut groups: Vec<(Vec<Option<&str>>, Vec<usize>)> = Vec::new();
    let mut index: HashMap<u64, Vec<usize>> = HashMap::new();

    for row in 0..n {
        let mut hasher = std::hash::DefaultHasher::new();
        for col in &part_cols {
            if col.is_null(row) {
                std::hash::Hash::hash(&0u8, &mut hasher);
            } else {
                std::hash::Hash::hash(&1u8, &mut hasher);
                std::hash::Hash::hash(col.value(row), &mut hasher);
            }
        }
        let hash = std::hash::Hasher::finish(&hasher);

        let key: Vec<Option<&str>> = part_cols
            .iter()
            .map(|col| {
                if col.is_null(row) {
                    None
                } else {
                    Some(col.value(row))
                }
            })
            .collect();

        let bucket = index.entry(hash).or_default();
        let mut found = false;
        for &gidx in bucket.iter() {
            if groups[gidx].0 == key {
                groups[gidx].1.push(row);
                found = true;
                break;
            }
        }
        if !found {
            let gidx = groups.len();
            groups.push((key, vec![row]));
            bucket.push(gidx);
        }
    }

    Ok(groups.into_iter().map(|(_, rows)| rows).collect())
}

/// Compute a single window function for a single partition.
///
/// Values are batch-extracted into typed slices once per
/// partition, eliminating per-row dynamic dispatch through `extract_f64`.
/// This provides ~5-10× improvement on large partitions by avoiding
/// repeated `as_any().downcast_ref()` chains per row.
///
/// Results are written directly into the `output` slice at the partition's
/// row indices.
#[allow(clippy::cast_precision_loss)]
fn compute_window_fn(
    func: &WindowFn,
    partition: &[usize],
    val_col: &dyn Array,
    ts_col: Option<&Int64Array>,
    batch: &RecordBatch,
    output: &mut [Option<f64>],
) -> std::result::Result<(), QueryError> {
    let len = partition.len();
    if len == 0 {
        return Ok(());
    }

    // Batch-extract all partition values up front to avoid
    // per-row dynamic dispatch. For functions that need the order column
    // (Rank/DenseRank), extraction is done separately below.
    let needs_values = !matches!(
        func,
        WindowFn::RowNumber | WindowFn::Rank { .. } | WindowFn::DenseRank { .. }
    );
    let values: Vec<Option<f64>> = if needs_values {
        partition.iter().map(|&r| extract_f64(val_col, r)).collect()
    } else {
        Vec::new()
    };

    // Batch-extract timestamps once for functions that need them.
    let timestamps: Vec<i64> = if matches!(
        func,
        WindowFn::Rate
            | WindowFn::IRate
            | WindowFn::RangeMovingAverage { .. }
            | WindowFn::RangeSum { .. }
            | WindowFn::RangeCount { .. }
    ) {
        if let Some(ts) = ts_col {
            partition.iter().map(|&r| ts.value(r)).collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    match func {
        WindowFn::RowNumber => {
            for (i, &row) in partition.iter().enumerate() {
                output[row] = Some((i + 1) as f64);
            }
        }

        WindowFn::Rank { order_column } => {
            // Rank by the specified order column, falling back to val_col.
            // Use bitwise comparison so NaN == NaN
            let order_col: &dyn Array = order_column
                .as_ref()
                .and_then(|name| batch.column_by_name(name))
                .map(std::convert::AsRef::as_ref)
                .unwrap_or(val_col);
            let order_values: Vec<Option<f64>> = partition
                .iter()
                .map(|&r| extract_f64(order_col, r))
                .collect();
            let mut rank = 1usize;
            output[partition[0]] = Some(1.0);
            for i in 1..len {
                if !f64_opt_eq(order_values[i], order_values[i - 1]) {
                    rank = i + 1;
                }
                output[partition[i]] = Some(rank as f64);
            }
        }

        WindowFn::DenseRank { order_column } => {
            // Rank by the specified order column, falling back to val_col.
            // Use bitwise comparison so NaN == NaN
            let order_col: &dyn Array = order_column
                .as_ref()
                .and_then(|name| batch.column_by_name(name))
                .map(std::convert::AsRef::as_ref)
                .unwrap_or(val_col);
            let order_values: Vec<Option<f64>> = partition
                .iter()
                .map(|&r| extract_f64(order_col, r))
                .collect();
            let mut rank = 1usize;
            output[partition[0]] = Some(1.0);
            for i in 1..len {
                if !f64_opt_eq(order_values[i], order_values[i - 1]) {
                    rank += 1;
                }
                output[partition[i]] = Some(rank as f64);
            }
        }

        WindowFn::Lead { offset, default } => {
            for (i, &row) in partition.iter().enumerate() {
                let target = i + offset;
                output[row] = if target < len {
                    values[target].or(Some(*default))
                } else {
                    Some(*default)
                };
            }
        }

        WindowFn::Lag { offset, default } => {
            for (i, &row) in partition.iter().enumerate() {
                output[row] = if i >= *offset {
                    values[i - offset].or(Some(*default))
                } else {
                    Some(*default)
                };
            }
        }

        WindowFn::Delta => {
            output[partition[0]] = None; // no previous value
            for i in 1..len {
                output[partition[i]] = match (values[i], values[i - 1]) {
                    (Some(c), Some(p)) => Some(c - p),
                    _ => None,
                };
            }
        }

        WindowFn::Rate => {
            // Per-second rate = delta(value) / delta(timestamp_seconds)
            // Timestamps are nanoseconds.
            output[partition[0]] = None;
            if !timestamps.is_empty() {
                for i in 1..len {
                    let dt = (timestamps[i] - timestamps[i - 1]) as f64 / 1_000_000_000.0;
                    output[partition[i]] = match (values[i], values[i - 1]) {
                        (Some(c), Some(p)) if dt > 0.0 => Some((c - p) / dt),
                        _ => None,
                    };
                }
            }
        }

        WindowFn::IRate => {
            // Instantaneous rate between the last two samples, applied to all rows.
            let irate = if len >= 2 && !timestamps.is_empty() {
                let dt = (timestamps[len - 1] - timestamps[len - 2]) as f64 / 1_000_000_000.0;
                match (values[len - 1], values[len - 2]) {
                    (Some(vl), Some(vp)) if dt > 0.0 => Some((vl - vp) / dt),
                    _ => None,
                }
            } else {
                None
            };
            for &row in partition {
                output[row] = irate;
            }
        }

        WindowFn::MovingAverage { window_size } => {
            let ws = *window_size;
            // O(n) sliding window instead of O(n×w) recomputation.
            let mut sum = 0.0;
            let mut count = 0usize;
            for i in 0..len {
                // Add the entering element
                if let Some(val) = values[i] {
                    sum += val;
                    count += 1;
                }
                // Remove the leaving element (the one that slides out)
                if i >= ws {
                    if let Some(val) = values[i - ws] {
                        sum -= val;
                        count -= 1;
                    }
                }
                if i + 1 < ws {
                    // Not enough data for a full window yet
                    output[partition[i]] = None;
                } else {
                    output[partition[i]] = if count > 0 {
                        Some(sum / count as f64)
                    } else {
                        None
                    };
                }
            }
        }

        WindowFn::CumulativeSum => {
            let mut running = 0.0;
            for (i, &row) in partition.iter().enumerate() {
                if let Some(v) = values[i] {
                    running += v;
                    output[row] = Some(running);
                } else {
                    output[row] = None;
                }
            }
        }

        WindowFn::Increase => {
            // Monotonic counter increase: like CumulativeSum of positive deltas.
            // On counter reset (curr < prev), delta is treated as 0.
            let mut total = 0.0;
            output[partition[0]] = Some(0.0);
            for i in 1..len {
                match (values[i], values[i - 1]) {
                    (Some(c), Some(p)) => {
                        if c >= p {
                            total += c - p;
                        }
                        // else counter reset — delta is 0
                        output[partition[i]] = Some(total);
                    }
                    _ => {
                        output[partition[i]] = None;
                    }
                }
            }
        }

        // ── RANGE frame window functions ─────────────────────
        //
        // These operate on timestamp ranges instead of fixed row counts.
        // Uses an efficient two-pointer O(n) approach: a trailing pointer
        // marks the start of the current window and only advances forward,
        // giving amortized O(1) per row.
        WindowFn::RangeMovingAverage { range_ns } => {
            if timestamps.is_empty() {
                return Err(QueryError::Validation(
                    "timestamp column required for RangeMovingAverage".into(),
                ));
            }
            let range = *range_ns;

            let mut start = 0usize;
            let mut sum = 0.0;
            let mut count = 0usize;

            for i in 0..len {
                if let Some(v) = values[i] {
                    sum += v;
                    count += 1;
                }
                while start < i && timestamps[i] - timestamps[start] > range {
                    if let Some(v) = values[start] {
                        sum -= v;
                        count -= 1;
                    }
                    start += 1;
                }
                output[partition[i]] = if count > 0 {
                    Some(sum / count as f64)
                } else {
                    None
                };
            }
        }

        WindowFn::RangeSum { range_ns } => {
            if timestamps.is_empty() {
                return Err(QueryError::Validation(
                    "timestamp column required for RangeSum".into(),
                ));
            }
            let range = *range_ns;

            let mut start = 0usize;
            let mut sum = 0.0;
            let mut count = 0usize;

            for i in 0..len {
                if let Some(v) = values[i] {
                    sum += v;
                    count += 1;
                }
                while start < i && timestamps[i] - timestamps[start] > range {
                    if let Some(v) = values[start] {
                        sum -= v;
                        count -= 1;
                    }
                    start += 1;
                }
                output[partition[i]] = if count > 0 { Some(sum) } else { None };
            }
        }

        WindowFn::RangeCount { range_ns } => {
            if timestamps.is_empty() {
                return Err(QueryError::Validation(
                    "timestamp column required for RangeCount".into(),
                ));
            }
            let range = *range_ns;

            let mut start = 0usize;
            let mut count = 0usize;

            for i in 0..len {
                if values[i].is_some() {
                    count += 1;
                }
                while start < i && timestamps[i] - timestamps[start] > range {
                    if values[start].is_some() {
                        count -= 1;
                    }
                    start += 1;
                }
                output[partition[i]] = if count > 0 { Some(count as f64) } else { None };
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::array::UInt64Array;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let timestamps = Arc::new(Int64Array::from(vec![
            1_000_000_000,
            2_000_000_000,
            3_000_000_000,
            4_000_000_000,
            5_000_000_000,
        ])); // 1s, 2s, 3s, 4s, 5s in nanoseconds
        let hosts = Arc::new(StringArray::from(vec!["a", "a", "a", "a", "a"]));
        let values = Arc::new(Float64Array::from(vec![10.0, 20.0, 15.0, 25.0, 30.0]));

        RecordBatch::try_new(schema, vec![timestamps, hosts, values]).unwrap()
    }

    fn partitioned_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let timestamps = Arc::new(Int64Array::from(vec![
            1_000_000_000,
            1_000_000_000,
            2_000_000_000,
            2_000_000_000,
            3_000_000_000,
            3_000_000_000,
        ]));
        let hosts = Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a", "b"]));
        let values = Arc::new(Float64Array::from(vec![
            10.0, 100.0, 20.0, 200.0, 30.0, 300.0,
        ]));

        RecordBatch::try_new(schema, vec![timestamps, hosts, values]).unwrap()
    }

    #[test]
    fn row_number() {
        let batch = test_batch();
        let result = apply_window(&batch, &[WindowFn::RowNumber], &[], "value", None).unwrap();

        // Original columns preserved
        assert_eq!(result.num_columns(), 4); // 3 original + 1 window
        assert_eq!(result.num_rows(), 5);

        let rn = result
            .column_by_name("value_row_number")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..5 {
            assert!((rn.value(i) - (i + 1) as f64).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn rank_and_dense_rank() {
        // Values: 10, 20, 20, 25, 30 → rank: 1, 2, 2, 4, 5; dense_rank: 1, 2, 2, 3, 4
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let ts = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let vals = Arc::new(Float64Array::from(vec![10.0, 20.0, 20.0, 25.0, 30.0]));
        let batch = RecordBatch::try_new(schema, vec![ts, vals]).unwrap();

        let result = apply_window(
            &batch,
            &[
                WindowFn::Rank { order_column: None },
                WindowFn::DenseRank { order_column: None },
            ],
            &[],
            "value",
            None,
        )
        .unwrap();

        let rank = result
            .column_by_name("value_rank")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(rank.value(0), 1.0);
        assert_eq!(rank.value(1), 2.0);
        assert_eq!(rank.value(2), 2.0);
        assert_eq!(rank.value(3), 4.0);
        assert_eq!(rank.value(4), 5.0);

        let drank = result
            .column_by_name("value_dense_rank")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(drank.value(0), 1.0);
        assert_eq!(drank.value(1), 2.0);
        assert_eq!(drank.value(2), 2.0);
        assert_eq!(drank.value(3), 3.0);
        assert_eq!(drank.value(4), 4.0);
    }

    #[test]
    fn lead_and_lag() {
        let batch = test_batch(); // values: 10, 20, 15, 25, 30
        let result = apply_window(
            &batch,
            &[
                WindowFn::Lead {
                    offset: 1,
                    default: -1.0,
                },
                WindowFn::Lag {
                    offset: 1,
                    default: -1.0,
                },
            ],
            &[],
            "value",
            None,
        )
        .unwrap();

        let lead = result
            .column_by_name("value_lead_1")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 20.0);
        assert_eq!(lead.value(3), 30.0);
        assert_eq!(lead.value(4), -1.0); // default

        let lag = result
            .column_by_name("value_lag_1")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lag.value(0), -1.0); // default
        assert_eq!(lag.value(1), 10.0);
        assert_eq!(lag.value(4), 25.0);
    }

    #[test]
    fn lead_offset_2() {
        let batch = test_batch(); // values: 10, 20, 15, 25, 30
        let result = apply_window(
            &batch,
            &[WindowFn::Lead {
                offset: 2,
                default: 0.0,
            }],
            &[],
            "value",
            None,
        )
        .unwrap();

        let lead = result
            .column_by_name("value_lead_2")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lead.value(0), 15.0);
        assert_eq!(lead.value(2), 30.0);
        assert_eq!(lead.value(3), 0.0); // default
        assert_eq!(lead.value(4), 0.0); // default
    }

    #[test]
    fn delta() {
        let batch = test_batch(); // values: 10, 20, 15, 25, 30
        let result = apply_window(&batch, &[WindowFn::Delta], &[], "value", None).unwrap();

        let delta = result
            .column_by_name("value_delta")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(delta.is_null(0)); // first row is null
        assert_eq!(delta.value(1), 10.0); // 20-10
        assert_eq!(delta.value(2), -5.0); // 15-20
        assert_eq!(delta.value(3), 10.0); // 25-15
        assert_eq!(delta.value(4), 5.0); // 30-25
    }

    #[test]
    fn rate() {
        let batch = test_batch(); // values: 10, 20, 15, 25, 30; timestamps 1s apart (in ns)
        let result = apply_window(&batch, &[WindowFn::Rate], &[], "value", None).unwrap();

        let rate = result
            .column_by_name("value_rate")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(rate.is_null(0));
        // delta_v = 10, delta_t = 1s → rate = 10/s
        assert!((rate.value(1) - 10.0).abs() < f64::EPSILON);
        // delta_v = -5, delta_t = 1s → rate = -5/s
        assert!((rate.value(2) - (-5.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn irate() {
        let batch = test_batch(); // values: 10, 20, 15, 25, 30; last two: 25, 30 at 4s, 5s
        let result = apply_window(&batch, &[WindowFn::IRate], &[], "value", None).unwrap();

        let irate = result
            .column_by_name("value_irate")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // (30 - 25) / (5s - 4s) = 5.0 per second
        for i in 0..5 {
            assert!((irate.value(i) - 5.0).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn moving_average() {
        let batch = test_batch(); // values: 10, 20, 15, 25, 30
        let result = apply_window(
            &batch,
            &[WindowFn::MovingAverage { window_size: 3 }],
            &[],
            "value",
            None,
        )
        .unwrap();

        let ma = result
            .column_by_name("value_moving_avg_3")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(ma.is_null(0)); // not enough data
        assert!(ma.is_null(1));
        // avg(10, 20, 15) = 15
        assert!((ma.value(2) - 15.0).abs() < f64::EPSILON);
        // avg(20, 15, 25) = 20
        assert!((ma.value(3) - 20.0).abs() < f64::EPSILON);
        // avg(15, 25, 30) ≈ 23.333
        assert!((ma.value(4) - 70.0 / 3.0).abs() < 1e-10);
    }

    #[test]
    fn cumulative_sum() {
        let batch = test_batch(); // values: 10, 20, 15, 25, 30
        let result = apply_window(&batch, &[WindowFn::CumulativeSum], &[], "value", None).unwrap();

        let cs = result
            .column_by_name("value_cumulative_sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(cs.value(0), 10.0);
        assert_eq!(cs.value(1), 30.0);
        assert_eq!(cs.value(2), 45.0);
        assert_eq!(cs.value(3), 70.0);
        assert_eq!(cs.value(4), 100.0);
    }

    #[test]
    fn increase() {
        // Counter: 10, 20, 15 (reset!), 25, 30
        let batch = test_batch();
        let result = apply_window(&batch, &[WindowFn::Increase], &[], "value", None).unwrap();

        let inc = result
            .column_by_name("value_increase")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(inc.value(0), 0.0); // start
        assert_eq!(inc.value(1), 10.0); // 20-10
        assert_eq!(inc.value(2), 10.0); // reset (15<20) → no increase
        assert_eq!(inc.value(3), 20.0); // 25-15 = +10
        assert_eq!(inc.value(4), 25.0); // 30-25 = +5
    }

    #[test]
    fn partitioned_row_number() {
        let batch = partitioned_batch();
        let result = apply_window(
            &batch,
            &[WindowFn::RowNumber],
            &["host".to_string()],
            "value",
            None,
        )
        .unwrap();

        let rn = result
            .column_by_name("value_row_number")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // host "a": rows 0, 2, 4 → row_number 1, 2, 3
        assert_eq!(rn.value(0), 1.0);
        assert_eq!(rn.value(2), 2.0);
        assert_eq!(rn.value(4), 3.0);
        // host "b": rows 1, 3, 5 → row_number 1, 2, 3
        assert_eq!(rn.value(1), 1.0);
        assert_eq!(rn.value(3), 2.0);
        assert_eq!(rn.value(5), 3.0);
    }

    #[test]
    fn partitioned_delta() {
        let batch = partitioned_batch();
        let result = apply_window(
            &batch,
            &[WindowFn::Delta],
            &["host".to_string()],
            "value",
            None,
        )
        .unwrap();

        let delta = result
            .column_by_name("value_delta")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // host "a": 10, 20, 30 → null, 10, 10
        assert!(delta.is_null(0));
        assert_eq!(delta.value(2), 10.0);
        assert_eq!(delta.value(4), 10.0);
        // host "b": 100, 200, 300 → null, 100, 100
        assert!(delta.is_null(1));
        assert_eq!(delta.value(3), 100.0);
        assert_eq!(delta.value(5), 100.0);
    }

    #[test]
    fn empty_functions_returns_clone() {
        let batch = test_batch();
        let result = apply_window(&batch, &[], &[], "value", None).unwrap();
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 5);
    }

    #[test]
    fn multiple_window_functions() {
        let batch = test_batch();
        let result = apply_window(
            &batch,
            &[
                WindowFn::RowNumber,
                WindowFn::Delta,
                WindowFn::CumulativeSum,
            ],
            &[],
            "value",
            None,
        )
        .unwrap();

        assert_eq!(result.num_columns(), 6); // 3 original + 3 window
        assert!(result.column_by_name("value_row_number").is_some());
        assert!(result.column_by_name("value_delta").is_some());
        assert!(result.column_by_name("value_cumulative_sum").is_some());
    }

    #[test]
    fn missing_value_column_errors() {
        let batch = test_batch();
        let err = apply_window(&batch, &[WindowFn::Delta], &[], "nonexistent", None).unwrap_err();
        assert!(matches!(err, QueryError::FieldNotFound { .. }));
    }

    #[test]
    fn i64_and_u64_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("icount", DataType::Int64, false),
            Field::new("ucount", DataType::UInt64, false),
        ]));
        let ts = Arc::new(Int64Array::from(vec![
            1_000_000_000,
            2_000_000_000,
            3_000_000_000,
        ]));
        let ival = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let uval = Arc::new(UInt64Array::from(vec![100u64, 200, 300]));
        let batch = RecordBatch::try_new(schema, vec![ts, ival, uval]).unwrap();

        let result = apply_window(&batch, &[WindowFn::CumulativeSum], &[], "icount", None).unwrap();
        let cs = result
            .column_by_name("icount_cumulative_sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(cs.value(0), 10.0);
        assert_eq!(cs.value(1), 30.0);
        assert_eq!(cs.value(2), 60.0);

        let result2 = apply_window(&batch, &[WindowFn::Delta], &[], "ucount", None).unwrap();
        let delta = result2
            .column_by_name("ucount_delta")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(delta.is_null(0));
        assert_eq!(delta.value(1), 100.0);
        assert_eq!(delta.value(2), 100.0);
    }

    #[test]
    fn window_respects_memory_tracker() {
        let batch = test_batch();
        let tracker = crate::memory::MemoryTracker::new(1_000_000);
        let result = apply_window(
            &batch,
            &[WindowFn::CumulativeSum],
            &[],
            "value",
            Some(&tracker),
        )
        .unwrap();
        assert!(tracker.allocated() > 0);
        assert_eq!(result.num_rows(), 5);
    }

    #[test]
    fn window_exceeds_memory_budget() {
        let batch = test_batch();
        let tracker = crate::memory::MemoryTracker::new(1);
        let err = apply_window(
            &batch,
            &[WindowFn::CumulativeSum],
            &[],
            "value",
            Some(&tracker),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            crate::error::QueryError::QueryMemoryExceeded { .. }
        ));
    }

    // ── RANGE frame window function tests ──────────────────

    #[test]
    fn range_moving_average() {
        let batch = test_batch();
        // Timestamps: 1e9, 2e9, 3e9, 4e9, 5e9
        // Values:     10,  20,  15,  25,  30
        // Range = 2e9 ns (2 seconds) → window includes rows within 2s
        let result = apply_window(
            &batch,
            &[WindowFn::RangeMovingAverage {
                range_ns: 2_000_000_000,
            }],
            &[],
            "value",
            None,
        )
        .unwrap();

        let avg = result
            .column_by_name("value_range_avg_2000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // row 0: ts=1, window=[10], avg=10
        assert!((avg.value(0) - 10.0).abs() < f64::EPSILON);
        // row 1: ts=2, window=[10,20], avg=15
        assert!((avg.value(1) - 15.0).abs() < f64::EPSILON);
        // row 2: ts=3, window=[10,20,15] (3-1=2 ≤ 2), avg=15
        assert!((avg.value(2) - 15.0).abs() < f64::EPSILON);
        // row 3: ts=4, window=[20,15,25] (4-1=3>2, evict row 0), avg=20
        assert!((avg.value(3) - 20.0).abs() < f64::EPSILON);
        // row 4: ts=5, window=[15,25,30] (5-2=3>2, evict row 1), avg≈23.33
        assert!((avg.value(4) - 70.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn range_sum() {
        let batch = test_batch();
        // Values: 10, 20, 15, 25, 30
        let result = apply_window(
            &batch,
            &[WindowFn::RangeSum {
                range_ns: 1_000_000_000,
            }],
            &[],
            "value",
            None,
        )
        .unwrap();

        let sum = result
            .column_by_name("value_range_sum_1000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Range = 1s
        // row 0: ts=1, window=[10], sum=10
        assert!((sum.value(0) - 10.0).abs() < f64::EPSILON);
        // row 1: ts=2, window=[10,20] (2-1=1 ≤ 1), sum=30
        assert!((sum.value(1) - 30.0).abs() < f64::EPSILON);
        // row 2: ts=3, window=[20,15] (3-1=2>1, evict row 0), sum=35
        assert!((sum.value(2) - 35.0).abs() < f64::EPSILON);
        // row 3: ts=4, window=[15,25] (4-2=2>1, evict row 1), sum=40
        assert!((sum.value(3) - 40.0).abs() < f64::EPSILON);
        // row 4: ts=5, window=[25,30] (5-3=2>1, evict row 2), sum=55
        assert!((sum.value(4) - 55.0).abs() < f64::EPSILON);
    }

    #[test]
    fn range_count() {
        let batch = test_batch();
        let result = apply_window(
            &batch,
            &[WindowFn::RangeCount {
                range_ns: 1_500_000_000,
            }],
            &[],
            "value",
            None,
        )
        .unwrap();

        let cnt = result
            .column_by_name("value_range_count_1500000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Range = 1.5s
        // row 0: ts=1, window=[1], count=1
        assert!((cnt.value(0) - 1.0).abs() < f64::EPSILON);
        // row 1: ts=2, window=[1,2], count=2
        assert!((cnt.value(1) - 2.0).abs() < f64::EPSILON);
        // row 2: ts=3, window=[2,3] (3-1=2>1.5, evict row 0), count=2
        assert!((cnt.value(2) - 2.0).abs() < f64::EPSILON);
        // row 3: ts=4, window=[3,4] (4-2=2>1.5, evict row 1), count=2
        assert!((cnt.value(3) - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn range_moving_average_partitioned() {
        let batch = test_batch();
        // All hosts are "a", so one partition with all 5 rows
        // Values: 10, 20, 15, 25, 30  → total = 100
        let result = apply_window(
            &batch,
            &[WindowFn::RangeMovingAverage {
                range_ns: 5_000_000_000,
            }],
            &["host".to_string()],
            "value",
            None,
        )
        .unwrap();

        let avg = result
            .column_by_name("value_range_avg_5000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // 5s range covers all rows (5-1=4 ≤ 5)
        // Row 4: avg = (10+20+15+25+30)/5 = 20
        assert!((avg.value(4) - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn range_zero_range_is_self_only() {
        // range_ns = 0 means each window includes only the current row
        let batch = test_batch();
        let result = apply_window(
            &batch,
            &[WindowFn::RangeMovingAverage { range_ns: 0 }],
            &[],
            "value",
            None,
        )
        .unwrap();

        let avg = result
            .column_by_name("value_range_avg_0ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Each row's window contains only itself
        assert!((avg.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((avg.value(1) - 20.0).abs() < f64::EPSILON);
        assert!((avg.value(2) - 15.0).abs() < f64::EPSILON);
        assert!((avg.value(3) - 25.0).abs() < f64::EPSILON);
        assert!((avg.value(4) - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn range_with_null_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let ts = Arc::new(Int64Array::from(vec![
            1_000_000_000,
            2_000_000_000,
            3_000_000_000,
            4_000_000_000,
        ]));
        // Second and third values are null
        let values = Arc::new(Float64Array::from(vec![Some(10.0), None, None, Some(40.0)]));
        let batch = RecordBatch::try_new(schema, vec![ts, values]).unwrap();

        let result = apply_window(
            &batch,
            &[
                WindowFn::RangeMovingAverage {
                    range_ns: 2_000_000_000,
                },
                WindowFn::RangeSum {
                    range_ns: 2_000_000_000,
                },
                WindowFn::RangeCount {
                    range_ns: 2_000_000_000,
                },
            ],
            &[],
            "value",
            None,
        )
        .unwrap();

        let avg = result
            .column_by_name("value_range_avg_2000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sum = result
            .column_by_name("value_range_sum_2000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let cnt = result
            .column_by_name("value_range_count_2000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // row 0: window=[10], avg=10, sum=10, count=1
        assert!((avg.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((sum.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((cnt.value(0) - 1.0).abs() < f64::EPSILON);

        // row 1: ts=2, window=[10, null], only 10 is non-null, avg=10, sum=10, count=1
        assert!((avg.value(1) - 10.0).abs() < f64::EPSILON);
        assert!((sum.value(1) - 10.0).abs() < f64::EPSILON);
        assert!((cnt.value(1) - 1.0).abs() < f64::EPSILON);

        // row 2: ts=3, window=[null, null] (10 evicted: 3-1=2 not > 2, so [10,null,null])
        //   Actually: 3-1=2 which is NOT > 2, so row 0 stays. window=[10,null,null]
        assert!((avg.value(2) - 10.0).abs() < f64::EPSILON);

        // row 3: ts=4, window=[null, null, 40] (4-1=3 > 2, evict row 0)
        assert!((avg.value(3) - 40.0).abs() < f64::EPSILON);
        assert!((sum.value(3) - 40.0).abs() < f64::EPSILON);
        assert!((cnt.value(3) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn range_all_null_returns_none() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let ts = Arc::new(Int64Array::from(vec![1_000_000_000, 2_000_000_000]));
        let values = Arc::new(Float64Array::from(vec![None, None]));
        let batch = RecordBatch::try_new(schema, vec![ts, values]).unwrap();

        let result = apply_window(
            &batch,
            &[
                WindowFn::RangeMovingAverage {
                    range_ns: 5_000_000_000,
                },
                WindowFn::RangeSum {
                    range_ns: 5_000_000_000,
                },
                WindowFn::RangeCount {
                    range_ns: 5_000_000_000,
                },
            ],
            &[],
            "value",
            None,
        )
        .unwrap();

        let avg = result
            .column_by_name("value_range_avg_5000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sum = result
            .column_by_name("value_range_sum_5000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let cnt = result
            .column_by_name("value_range_count_5000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // All-null → all functions should return None
        assert!(avg.is_null(0));
        assert!(avg.is_null(1));
        assert!(sum.is_null(0));
        assert!(sum.is_null(1));
        assert!(cnt.is_null(0));
        assert!(cnt.is_null(1));
    }

    #[test]
    fn range_missing_timestamp_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let batch = RecordBatch::try_new(schema, vec![values]).unwrap();

        let err = apply_window(
            &batch,
            &[WindowFn::RangeMovingAverage { range_ns: 1000 }],
            &[],
            "value",
            None,
        )
        .unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));

        let err = apply_window(
            &batch,
            &[WindowFn::RangeSum { range_ns: 1000 }],
            &[],
            "value",
            None,
        )
        .unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));

        let err = apply_window(
            &batch,
            &[WindowFn::RangeCount { range_ns: 1000 }],
            &[],
            "value",
            None,
        )
        .unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn range_duplicate_timestamps() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        // Two rows at the same timestamp
        let ts = Arc::new(Int64Array::from(vec![
            1_000_000_000,
            1_000_000_000,
            2_000_000_000,
        ]));
        let values = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0]));
        let batch = RecordBatch::try_new(schema, vec![ts, values]).unwrap();

        let result = apply_window(
            &batch,
            &[WindowFn::RangeMovingAverage {
                range_ns: 500_000_000,
            }],
            &[],
            "value",
            None,
        )
        .unwrap();

        let avg = result
            .column_by_name("value_range_avg_500000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // row 0: ts=1, window=[10], avg=10
        assert!((avg.value(0) - 10.0).abs() < f64::EPSILON);
        // row 1: ts=1, same as row 0 (diff=0 ≤ 0.5s), window=[10,20], avg=15
        assert!((avg.value(1) - 15.0).abs() < f64::EPSILON);
        // row 2: ts=2, diff=1 > 0.5s, both ts=1 rows evicted, window=[30], avg=30
        assert!((avg.value(2) - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn range_single_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let ts = Arc::new(Int64Array::from(vec![1_000_000_000]));
        let values = Arc::new(Float64Array::from(vec![42.0]));
        let batch = RecordBatch::try_new(schema, vec![ts, values]).unwrap();

        let result = apply_window(
            &batch,
            &[
                WindowFn::RangeMovingAverage {
                    range_ns: 1_000_000_000,
                },
                WindowFn::RangeSum {
                    range_ns: 1_000_000_000,
                },
                WindowFn::RangeCount {
                    range_ns: 1_000_000_000,
                },
            ],
            &[],
            "value",
            None,
        )
        .unwrap();

        let avg = result
            .column_by_name("value_range_avg_1000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sum = result
            .column_by_name("value_range_sum_1000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let cnt = result
            .column_by_name("value_range_count_1000000000ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!((avg.value(0) - 42.0).abs() < f64::EPSILON);
        assert!((sum.value(0) - 42.0).abs() < f64::EPSILON);
        assert!((cnt.value(0) - 1.0).abs() < f64::EPSILON);
    }
}
