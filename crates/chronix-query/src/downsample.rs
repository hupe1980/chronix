//! Time-bucket downsampling engine.
//!
//! Groups data points into fixed-width time buckets and applies an
//! aggregation function per bucket. This enables efficient data reduction
//! for dashboards, alerting queries, and long-term storage.
//!
//! # Two entry points
//!
//! [`downsample`] takes a whole `RecordBatch` and returns every bucket in it.
//! It is correct only when the batch contains *all* the rows for the buckets
//! it covers.
//!
//! [`StreamingDownsampler`] is the one to use over a chunked scan. Bucket
//! boundaries do not line up with batch boundaries, so downsampling each
//! batch independently emits the straddling interval twice — once per side —
//! which a caller then sums, averages or plots as two points. The
//! streaming form keeps one open bucket across batches and closes it only
//! when a row from a later bucket arrives, so a bucket is emitted exactly
//! once no matter how the input was chunked. Peak memory is one bucket's
//! accumulator, not one batch's worth of rows.

use std::sync::Arc;

use fnv::FnvHashMap;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::aggregate::AggFn;
use crate::error::{QueryError, Result};

/// Extract the value at `index` from a numeric column as `f64`.
///
/// Returns `None` if the value is null.
#[allow(clippy::cast_precision_loss)]
fn extract_f64(col: &dyn Array, index: usize) -> Option<f64> {
    crate::extract_f64(col, index)
}

/// Downsample a `RecordBatch` into fixed-width time buckets.
///
/// For each bucket `[bucket_start, bucket_start + interval)`, the
/// aggregation function is applied to the values in the `value_column`.
///
/// Returns a new `RecordBatch` with two columns: `_time` (bucket start)
/// and the aggregated value column.
///
/// # Arguments
///
/// * `batch` – input data (must have a `_time` Int64 column)
/// * `value_column` – name of the column to aggregate
/// * `interval` – bucket width in the same unit as timestamps (e.g. nanos)
/// * `function` – aggregation function to apply per bucket
///
/// # Errors
///
/// Returns an error if columns are missing or have unexpected types.
#[allow(clippy::cast_precision_loss)]
pub fn downsample(
    batch: &RecordBatch,
    value_column: &str,
    interval: i64,
    function: &AggFn,
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<RecordBatch> {
    if interval <= 0 {
        return Err(QueryError::Validation(
            "downsample interval must be positive".into(),
        ));
    }

    let schema = batch.schema();

    let time_idx = schema
        .index_of(chronix_core::TIME_COLUMN)
        .map_err(|_| QueryError::Validation("'timestamp' column not found".into()))?;
    let val_idx = schema
        .index_of(value_column)
        .map_err(|_| QueryError::FieldNotFound {
            measurement: String::new(),
            field: value_column.to_string(),
        })?;

    let timestamps = batch
        .column(time_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| QueryError::Validation("'timestamp' column is not Int64".into()))?;

    let value_col = batch.column(val_idx).as_ref();

    // Validate the value column is a supported numeric type
    match value_col.data_type() {
        DataType::Float64 | DataType::Int64 | DataType::UInt64 | DataType::Decimal128(_, _) => {}
        dt => {
            return Err(QueryError::Validation(format!(
                "'{value_column}' column has unsupported type {dt} \
                 (expected Float64, Int64, UInt64, or Decimal128)"
            )));
        }
    }

    // Group indices by bucket – use a hash map for O(1) inserts,
    // then sort the keys once at the end to restore time order.
    let mut buckets: FnvHashMap<i64, Vec<usize>> = FnvHashMap::default();
    for i in 0..timestamps.len() {
        if timestamps.is_null(i) {
            continue;
        }
        let ts = timestamps.value(i);
        // Overflow-safe bucket alignment: subtracting the remainder avoids
        // the `div_euclid * interval` multiplication that can overflow i64
        // for extreme negative timestamps.
        let bucket_start = ts - ts.rem_euclid(interval);
        buckets.entry(bucket_start).or_default().push(i);
    }

    // Sort bucket keys so the output is ordered by time.
    let mut sorted_keys: Vec<i64> = buckets.keys().copied().collect();
    sorted_keys.sort_unstable();

    // Compute aggregate per bucket
    let mut out_times = Vec::with_capacity(buckets.len());
    let mut out_values = Vec::with_capacity(buckets.len());

    for bucket_start in &sorted_keys {
        let indices = &buckets[bucket_start];
        // Collect (timestamp, value) pairs for this bucket.
        // Timestamps are needed for First/Last semantics.
        let raw_pairs: Vec<(i64, f64)> = indices
            .iter()
            .filter_map(|&i| {
                let val = extract_f64(value_col, i)?;
                Some((timestamps.value(i), val))
            })
            .collect();

        if raw_pairs.is_empty() {
            continue;
        }

        let agg_value = match function {
            // Count counts all non-null, non-NaN rows for consistency
            // with the other aggregation functions.
            AggFn::Count => raw_pairs.iter().filter(|(_, v)| !v.is_nan()).count() as f64,
            _ => {
                // For numeric aggregations, filter out NaN
                let clean: Vec<(i64, f64)> =
                    raw_pairs.into_iter().filter(|(_, v)| !v.is_nan()).collect();
                if clean.is_empty() {
                    continue;
                }
                match function {
                    AggFn::Sum => clean.iter().map(|(_, v)| v).sum(),
                    AggFn::Min => clean.iter().map(|(_, v)| *v).fold(f64::INFINITY, f64::min),
                    AggFn::Max => clean
                        .iter()
                        .map(|(_, v)| *v)
                        .fold(f64::NEG_INFINITY, f64::max),
                    AggFn::Avg => {
                        let sum: f64 = clean.iter().map(|(_, v)| v).sum();
                        sum / clean.len() as f64
                    }
                    // First: value with the smallest timestamp in the bucket
                    AggFn::First => clean
                        .iter()
                        .min_by_key(|(ts, _)| *ts)
                        .map(|(_, v)| *v)
                        .expect("clean is non-empty"),
                    // Last: value with the largest timestamp in the bucket
                    AggFn::Last => clean
                        .iter()
                        .max_by_key(|(ts, _)| *ts)
                        .map(|(_, v)| *v)
                        .expect("clean is non-empty"),
                    AggFn::Count => {
                        // Count is handled in the outer branch; defensive error.
                        return Err(QueryError::Validation(
                            "Count aggregation reached inner branch unexpectedly".into(),
                        ));
                    }
                }
            }
        };

        out_times.push(*bucket_start);
        out_values.push(agg_value);
    }

    let out_schema = Arc::new(Schema::new(vec![
        Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
        Field::new(value_column, DataType::Float64, false),
    ]));

    let result = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(Int64Array::from(out_times)) as ArrayRef,
            Arc::new(Float64Array::from(out_values)) as ArrayRef,
        ],
    )?;

    if let Some(tracker) = memory_tracker {
        tracker.try_allocate(result.get_array_memory_size())?;
    }

    Ok(result)
}

/// Boundary-safe downsampling over a sequence of time-ordered batches.
///
/// Rows must arrive ascending by timestamp — which is what the query engine's
/// merge guarantees — so a bucket is complete as soon as a row belonging to a
/// later bucket is seen. Buckets are emitted in order and exactly once.
///
/// # Example
///
/// ```no_run
/// # use chronix_query::downsample::StreamingDownsampler;
/// # use chronix_query::aggregate::AggFn;
/// # use arrow::record_batch::RecordBatch;
/// # fn batches() -> Vec<RecordBatch> { vec![] }
/// let mut ds = StreamingDownsampler::new("value", 60_000_000_000, AggFn::Avg)?;
/// let mut out = Vec::new();
/// for batch in batches() {
///     out.extend(ds.push(&batch)?);
/// }
/// out.extend(ds.finish()?);
/// # Ok::<(), chronix_query::QueryError>(())
/// ```
pub struct StreamingDownsampler {
    value_column: String,
    interval: i64,
    function: AggFn,
    /// Start timestamp of the bucket currently being accumulated.
    open_bucket: Option<i64>,
    /// Accumulated state for the open bucket.
    acc: BucketAcc,
    /// Completed buckets not yet drained into an output batch.
    pending: Vec<(i64, f64)>,
    /// Rows per emitted batch.
    chunk_rows: usize,
}

/// Running state for one time bucket.
#[derive(Default)]
struct BucketAcc {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    first_ts: i64,
    first_val: f64,
    last_ts: i64,
    last_val: f64,
}

impl BucketAcc {
    fn reset(&mut self) {
        self.count = 0;
        self.sum = 0.0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        self.first_ts = i64::MAX;
        self.first_val = 0.0;
        self.last_ts = i64::MIN;
        self.last_val = 0.0;
    }

    fn update(&mut self, ts: i64, v: f64) {
        // NaN is excluded, matching `downsample`'s per-bucket filtering.
        if v.is_nan() {
            return;
        }
        self.count += 1;
        self.sum += v;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        if ts < self.first_ts {
            self.first_ts = ts;
            self.first_val = v;
        }
        // `>=` so the last-encountered value wins on ties.
        if ts >= self.last_ts {
            self.last_ts = ts;
            self.last_val = v;
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn finalize(&self, function: AggFn) -> Option<f64> {
        if self.count == 0 {
            // `Count` still reports a bucket that had only NaN rows as absent,
            // matching `downsample`, which skips such buckets entirely.
            return None;
        }
        Some(match function {
            AggFn::Count => self.count as f64,
            AggFn::Sum => self.sum,
            AggFn::Min => self.min,
            AggFn::Max => self.max,
            AggFn::Avg => self.sum / self.count as f64,
            AggFn::First => self.first_val,
            AggFn::Last => self.last_val,
        })
    }
}

impl StreamingDownsampler {
    /// Rows per emitted batch.
    const DEFAULT_CHUNK_ROWS: usize = 65_536;

    /// Create a downsampler for `value_column` over `interval`-wide buckets.
    ///
    /// # Errors
    ///
    /// Returns an error if `interval` is not positive.
    pub fn new(value_column: &str, interval: i64, function: AggFn) -> Result<Self> {
        if interval <= 0 {
            return Err(QueryError::Validation(
                "downsample interval must be positive".into(),
            ));
        }
        let mut acc = BucketAcc::default();
        acc.reset();
        Ok(Self {
            value_column: value_column.to_string(),
            interval,
            function,
            open_bucket: None,
            acc,
            pending: Vec::new(),
            chunk_rows: Self::DEFAULT_CHUNK_ROWS,
        })
    }

    /// Fold one batch in, returning any batches of completed buckets.
    ///
    /// # Errors
    ///
    /// Returns an error if the timestamp or value column is missing or has an
    /// unsupported type, or if a row arrives out of timestamp order.
    pub fn push(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let schema = batch.schema();
        let time_idx = schema
            .index_of(chronix_core::TIME_COLUMN)
            .map_err(|_| QueryError::Validation("'timestamp' column not found".into()))?;
        let val_idx =
            schema
                .index_of(&self.value_column)
                .map_err(|_| QueryError::FieldNotFound {
                    measurement: String::new(),
                    field: self.value_column.clone(),
                })?;

        let timestamps = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| QueryError::Validation("'timestamp' column is not Int64".into()))?;
        let value_col = batch.column(val_idx).as_ref();
        match value_col.data_type() {
            DataType::Float64 | DataType::Int64 | DataType::UInt64 | DataType::Decimal128(_, _) => {
            }
            dt => {
                return Err(QueryError::Validation(format!(
                    "'{}' column has unsupported type {dt} \
                     (expected Float64, Int64, UInt64, or Decimal128)",
                    self.value_column
                )));
            }
        }

        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                continue;
            }
            let ts = timestamps.value(i);
            // Overflow-safe bucket alignment: subtracting the remainder avoids
            // the `div_euclid * interval` multiplication that can overflow i64
            // for extreme negative timestamps.
            let bucket = ts - ts.rem_euclid(self.interval);

            match self.open_bucket {
                Some(open) if bucket == open => {}
                Some(open) if bucket < open => {
                    // Out-of-order input would silently corrupt a closed
                    // bucket, so say so instead of producing a wrong number.
                    return Err(QueryError::Validation(format!(
                        "downsample input is not ascending by timestamp: bucket {bucket} \
                         arrived after {open} was closed"
                    )));
                }
                Some(_) => self.close_open_bucket(),
                None => {}
            }
            self.open_bucket = Some(bucket);

            if let Some(v) = extract_f64(value_col, i) {
                self.acc.update(ts, v);
            }
        }

        self.drain(false)
    }

    /// Close the final bucket and return any remaining output.
    ///
    /// # Errors
    ///
    /// Returns an error if the output batch cannot be built.
    pub fn finish(mut self) -> Result<Vec<RecordBatch>> {
        if self.open_bucket.is_some() {
            self.close_open_bucket();
        }
        self.drain(true)
    }

    fn close_open_bucket(&mut self) {
        if let (Some(start), Some(v)) = (self.open_bucket, self.acc.finalize(self.function)) {
            self.pending.push((start, v));
        }
        self.acc.reset();
        self.open_bucket = None;
    }

    /// Turn completed buckets into output batches. Unless `flush`, a partial
    /// chunk is kept back so emitted batches stay a uniform size.
    fn drain(&mut self, flush: bool) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        while self.pending.len() >= self.chunk_rows || (flush && !self.pending.is_empty()) {
            let take = self.chunk_rows.min(self.pending.len());
            let rows: Vec<(i64, f64)> = self.pending.drain(..take).collect();
            // Propagate rather than skip: dropping a failed batch here would
            // silently lose completed buckets.
            out.push(build_batch(&self.value_column, &rows)?);
        }
        Ok(out)
    }
}

/// Build the two-column output batch for a run of completed buckets.
fn build_batch(value_column: &str, rows: &[(i64, f64)]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
        Field::new(value_column, DataType::Float64, false),
    ]));
    let times: Vec<i64> = rows.iter().map(|(t, _)| *t).collect();
    let values: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(times)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;

    fn make_batch(times: Vec<i64>, values: Vec<f64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(times)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn downsample_avg_10s_buckets() {
        let batch = make_batch(
            vec![0, 5, 10, 15, 20, 25],
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        );

        let result = downsample(&batch, "value", 10, &AggFn::Avg, None).unwrap();
        assert_eq!(result.num_rows(), 3);

        let times = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Bucket [0, 10): ts=0, 5 → avg 1.5
        assert_eq!(times.value(0), 0);
        assert!((values.value(0) - 1.5).abs() < f64::EPSILON);

        // Bucket [10, 20): ts=10, 15 → avg 3.5
        assert_eq!(times.value(1), 10);
        assert!((values.value(1) - 3.5).abs() < f64::EPSILON);

        // Bucket [20, 30): ts=20, 25 → avg 5.5
        assert_eq!(times.value(2), 20);
        assert!((values.value(2) - 5.5).abs() < f64::EPSILON);
    }

    #[test]
    fn downsample_sum() {
        let batch = make_batch(vec![0, 5, 10, 15], vec![10.0, 20.0, 30.0, 40.0]);

        let result = downsample(&batch, "value", 10, &AggFn::Sum, None).unwrap();
        assert_eq!(result.num_rows(), 2);

        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!((values.value(0) - 30.0).abs() < f64::EPSILON);
        assert!((values.value(1) - 70.0).abs() < f64::EPSILON);
    }

    #[test]
    fn downsample_count() {
        let batch = make_batch(vec![0, 1, 2, 10, 20, 21, 22, 23], vec![1.0; 8]);

        let result = downsample(&batch, "value", 10, &AggFn::Count, None).unwrap();
        assert_eq!(result.num_rows(), 3);

        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!((values.value(0) - 3.0).abs() < f64::EPSILON); // [0,10): 3 points
        assert!((values.value(1) - 1.0).abs() < f64::EPSILON); // [10,20): 1 point
        assert!((values.value(2) - 4.0).abs() < f64::EPSILON); // [20,30): 4 points
    }

    #[test]
    fn downsample_min_max() {
        let batch = make_batch(vec![0, 5, 10, 15], vec![3.0, 1.0, 4.0, 2.0]);

        let min_result = downsample(&batch, "value", 10, &AggFn::Min, None).unwrap();
        let max_result = downsample(&batch, "value", 10, &AggFn::Max, None).unwrap();

        let min_vals = min_result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max_vals = max_result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!((min_vals.value(0) - 1.0).abs() < f64::EPSILON); // min(3, 1)
        assert!((min_vals.value(1) - 2.0).abs() < f64::EPSILON); // min(4, 2)
        assert!((max_vals.value(0) - 3.0).abs() < f64::EPSILON); // max(3, 1)
        assert!((max_vals.value(1) - 4.0).abs() < f64::EPSILON); // max(4, 2)
    }

    #[test]
    fn downsample_first_last() {
        let batch = make_batch(vec![0, 5, 10, 15], vec![10.0, 20.0, 30.0, 40.0]);

        let first = downsample(&batch, "value", 10, &AggFn::First, None).unwrap();
        let last = downsample(&batch, "value", 10, &AggFn::Last, None).unwrap();

        let first_vals = first
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let last_vals = last
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!((first_vals.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((first_vals.value(1) - 30.0).abs() < f64::EPSILON);
        assert!((last_vals.value(0) - 20.0).abs() < f64::EPSILON);
        assert!((last_vals.value(1) - 40.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zero_interval_fails() {
        let batch = make_batch(vec![0], vec![1.0]);
        assert!(downsample(&batch, "value", 0, &AggFn::Avg, None).is_err());
    }

    #[test]
    fn empty_batch() {
        let batch = make_batch(vec![], vec![]);
        let result = downsample(&batch, "value", 10, &AggFn::Avg, None).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn downsample_i64_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 5, 10, 15])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = downsample(&batch, "value", 10, &AggFn::Sum, None).unwrap();
        assert_eq!(result.num_rows(), 2);

        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((values.value(0) - 30.0).abs() < f64::EPSILON);
        assert!((values.value(1) - 70.0).abs() < f64::EPSILON);
    }

    #[test]
    fn downsample_u64_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 5, 10, 15])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![100u64, 200, 300, 400])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = downsample(&batch, "value", 10, &AggFn::Avg, None).unwrap();
        assert_eq!(result.num_rows(), 2);

        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((values.value(0) - 150.0).abs() < f64::EPSILON);
        assert!((values.value(1) - 350.0).abs() < f64::EPSILON);
    }

    #[test]
    fn downsample_unsupported_type_fails() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0i64])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![true])) as ArrayRef,
            ],
        )
        .unwrap();

        let err = downsample(&batch, "value", 10, &AggFn::Avg, None).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unsupported"),
            "expected unsupported type error, got: {msg}"
        );
    }

    #[test]
    fn downsample_respects_memory_tracker() {
        let batch = make_batch(vec![0, 5, 10, 15], vec![1.0, 2.0, 3.0, 4.0]);
        let tracker = crate::memory::MemoryTracker::new(1_000_000);
        let result = downsample(&batch, "value", 10, &AggFn::Avg, Some(&tracker)).unwrap();
        assert!(tracker.allocated() > 0);
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn downsample_exceeds_memory_budget() {
        let batch = make_batch(vec![0, 5, 10, 15], vec![1.0, 2.0, 3.0, 4.0]);
        let tracker = crate::memory::MemoryTracker::new(1);
        let err = downsample(&batch, "value", 10, &AggFn::Avg, Some(&tracker)).unwrap_err();
        assert!(matches!(
            err,
            crate::error::QueryError::QueryMemoryExceeded { .. }
        ));
    }
}
