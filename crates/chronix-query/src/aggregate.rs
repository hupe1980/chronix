//! Aggregation functions for query results.
//!
//! Implements core aggregation operations on Arrow arrays:
//! `count`, `sum`, `min`, `max`, `avg`, `first`, `last`.
//!
//! Supports group-by aggregation partitioned by tag columns.
//!
//! # Memory model
//!
//! [`StreamingAggregator`] is the form to reach for over a lazy scan: batches
//! are pushed in and dropped, and the state held is one accumulator set per
//! group. Peak memory is therefore `O(groups × fields)` and independent of
//! how many rows were aggregated — a `SELECT avg(x)` over a week of 1 s data
//! produces its single row without ever holding the week.
//!
//! The whole-slice entry points ([`aggregate_batch`], [`aggregate_grouped`],
//! [`aggregate_grouped_streaming`]) are conveniences for callers that already
//! hold their input.
//!
//! The *output* is still a single `RecordBatch`, so a group-by over a very
//! high-cardinality column is bounded by the group count rather than the row
//! count. Pass a [`MemoryTracker`](crate::memory::MemoryTracker) to fail early
//! rather than OOM in that case.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::compute::kernels::aggregate as arrow_agg;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{QueryError, Result};

/// Aggregation function type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    /// Count of non-null values.
    Count,
    /// Sum of values.
    Sum,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
    /// Average (mean) value.
    Avg,
    /// First value — the value at the minimum timestamp.
    ///
    /// When a timestamp column is available (the normal case), this scans
    /// for the row with the smallest timestamp. The data need not be sorted.
    /// Only falls back to array position if no timestamp column is present.
    ///
    /// # Edge cases
    ///
    /// - **Empty bucket / all nulls**: returns `None` (NULL in SQL).
    /// - **Ties** (multiple rows with the same minimum timestamp): the
    ///   first row encountered during the linear scan wins — i.e.
    ///   the result is deterministic for a given input order but
    ///   **not** stable across different physical orderings.
    /// - **Null timestamp**: the row is skipped; only non-null
    ///   timestamps participate in the min/max search.
    First,
    /// Last value — the value at the maximum timestamp.
    ///
    /// When a timestamp column is available (the normal case), this scans
    /// for the row with the largest timestamp. The data need not be sorted.
    /// Only falls back to array position if no timestamp column is present.
    ///
    /// # Edge cases
    ///
    /// - **Empty bucket / all nulls**: returns `None` (NULL in SQL).
    /// - **Ties** (multiple rows with the same maximum timestamp): the
    ///   last row encountered during the linear scan wins — i.e.
    ///   the result is deterministic for a given input order but
    ///   **not** stable across different physical orderings.
    /// - **Null timestamp**: the row is skipped; only non-null
    ///   timestamps participate in the min/max search.
    Last,
}

/// Generate a typed aggregation function that applies `AggFn` to an Arrow
/// primitive array and returns the result as `Option<f64>`.
///
/// For `Float64Array` values are used directly; for integer types they are
/// cast to `f64` via the `$conv` expression.
macro_rules! impl_aggregate {
    ($name:ident, $arr_ty:ty, $conv:expr) => {
        #[allow(clippy::cast_precision_loss)]
        fn $name(values: &$arr_ty, function: AggFn) -> Option<f64> {
            // COUNT on empty input returns 0 (SQL semantics), not NULL.
            if values.is_empty() {
                return if function == AggFn::Count {
                    Some(0.0)
                } else {
                    None
                };
            }
            match function {
                // O(1) non-null count via Arrow metadata instead
                // of iterating all values.
                AggFn::Count => Some((values.len() - values.null_count()) as f64),
                AggFn::Sum => arrow_agg::sum(values).map($conv),
                AggFn::Min => arrow_agg::min(values).map($conv),
                AggFn::Max => arrow_agg::max(values).map($conv),
                AggFn::Avg => {
                    let sum = arrow_agg::sum(values).map($conv)?;
                    let count = values.len() - values.null_count();
                    if count == 0 {
                        None
                    } else {
                        Some(sum / count as f64)
                    }
                }
                AggFn::First => values.iter().flatten().next().map($conv),
                AggFn::Last => values.iter().flatten().last().map($conv),
            }
        }
    };
}

impl_aggregate!(aggregate_f64, Float64Array, |v| v);
impl_aggregate!(aggregate_i64, Int64Array, |v| v as f64);
impl_aggregate!(aggregate_u64, UInt64Array, |v| v as f64);

/// Apply aggregation to a `RecordBatch` without grouping.
///
/// Returns a single-row `RecordBatch` with one column per field per function.
///
/// # Errors
///
/// Returns an error if the batch is empty or columns have unexpected types.
pub fn aggregate_batch(
    batch: &RecordBatch,
    functions: &[AggFn],
    field_columns: &[&str],
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<RecordBatch> {
    let mut result_fields = Vec::new();
    let mut result_columns: Vec<ArrayRef> = Vec::new();

    // Extract timestamp column for timestamp-aware First/Last.
    let ts_col = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

    for &field_name in field_columns {
        let col = batch
            .column_by_name(field_name)
            .ok_or_else(|| QueryError::Validation(format!("column '{field_name}' not found")))?;

        for func in functions {
            let func_name = format!("{field_name}_{}", agg_fn_name(*func));
            let value = aggregate_column_ts(col, *func, ts_col);

            result_fields.push(Field::new(&func_name, DataType::Float64, true));
            result_columns.push(Arc::new(Float64Array::from(vec![value])));
        }
    }

    if result_fields.is_empty() {
        return Err(QueryError::Validation("no columns to aggregate".into()));
    }

    let schema = Arc::new(Schema::new(result_fields));
    let batch = RecordBatch::try_new(schema, result_columns)?;
    if let Some(tracker) = memory_tracker {
        tracker.try_allocate(batch.get_array_memory_size())?;
    }
    Ok(batch)
}

/// Apply aggregation with group-by support.
///
/// Groups rows by the specified tag columns, then applies each aggregation
/// function to each field column within each group.
///
/// # Vectorized design
///
/// Uses a single-pass incremental accumulator approach instead of
/// per-group `compute::take()` + scalar aggregation. Each row is
/// processed exactly once with zero intermediate array allocations:
///
/// 1. Hash group keys (FNV-1a, zero-copy)
/// 2. Look up or create `IncrementalAccumulator` per (group, field)
/// 3. Update accumulator with the row's value and timestamp
/// 4. Finalize all accumulators into result arrays
///
/// This eliminates O(N_groups × N_fields) `compute::take()` allocations
/// and replaces them with O(1) accumulator updates per row. One
/// accumulator per (group, field) supports all `AggFn` variants
/// simultaneously.
///
/// # Errors
///
/// Returns an error if columns are not found.
pub fn aggregate_grouped(
    batch: &RecordBatch,
    functions: &[AggFn],
    field_columns: &[&str],
    group_by: &[&str],
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<RecordBatch> {
    if group_by.is_empty() {
        return aggregate_batch(batch, functions, field_columns, memory_tracker);
    }

    // Build group keys by combining group-by column values
    let group_cols: Vec<&StringArray> = group_by
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    QueryError::Validation(format!(
                        "group-by column '{name}' not found or not string"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    // Pre-resolve field columns and extract typed arrays ONCE.
    let field_cols: Vec<&dyn Array> = field_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .map(std::convert::AsRef::as_ref)
                .ok_or_else(|| QueryError::Validation(format!("column '{name}' not found")))
        })
        .collect::<Result<Vec<_>>>()?;

    // Pre-downcast to eliminate per-row dynamic dispatch.
    let typed_cols: Vec<TypedColumn<'_>> = field_cols
        .iter()
        .map(|c| TypedColumn::from_array(*c))
        .collect();

    let ts_col = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

    // One accumulator per (group, field_column) — a single
    // IncrementalAccumulator tracks count/sum/min/max/first/last
    // simultaneously, so we never need separate accumulators per function.
    let num_fields = field_columns.len();

    // Per-group state: key + one accumulator per field column.
    let mut groups: Vec<(Vec<Option<&str>>, Vec<IncrementalAccumulator>)> = Vec::new();
    let mut group_index: std::collections::HashMap<u64, Vec<usize>> =
        std::collections::HashMap::new();

    for row in 0..batch.num_rows() {
        let ts: Option<i64> = ts_col.and_then(|t| {
            if t.is_null(row) {
                None
            } else {
                Some(t.value(row))
            }
        });

        // Hash the group key without allocating Strings
        let mut hasher = fnv::FnvHasher::default();
        for col in &group_cols {
            if col.is_null(row) {
                std::hash::Hash::hash(&0u8, &mut hasher);
            } else {
                std::hash::Hash::hash(&1u8, &mut hasher);
                std::hash::Hash::hash(col.value(row), &mut hasher);
            }
        }
        let hash = std::hash::Hasher::finish(&hasher);

        // Build a zero-copy key for collision checking
        let key: Vec<Option<&str>> = group_cols
            .iter()
            .map(|col| {
                if col.is_null(row) {
                    None
                } else {
                    Some(col.value(row))
                }
            })
            .collect();

        // Find or create the group
        let bucket = group_index.entry(hash).or_default();
        // Cap chain length to prevent hash-collision DoS.
        if bucket.len() >= 1024 {
            return Err(QueryError::Validation(
                "group-by hash chain length exceeded limit (possible collision attack)".into(),
            ));
        }
        let gidx = {
            let mut found = None;
            for &idx in bucket.iter() {
                if groups[idx].0 == key {
                    found = Some(idx);
                    break;
                }
            }
            match found {
                Some(idx) => idx,
                None => {
                    // Check memory budget before adding a new group.
                    if let Some(tracker) = memory_tracker {
                        let estimated_group_bytes = 24 + group_by.len() * 16  // key Vec header + elements
                            + num_fields * std::mem::size_of::<IncrementalAccumulator>()
                            + 64; // HashMap entry overhead
                        tracker.try_allocate(estimated_group_bytes)?;
                    }
                    let idx = groups.len();
                    let accs = (0..num_fields)
                        .map(|_| IncrementalAccumulator::new())
                        .collect();
                    groups.push((key, accs));
                    bucket.push(idx);
                    idx
                }
            }
        };

        // Update one accumulator per field — single pass, no
        // intermediate array allocations. TypedColumn pre-downcast eliminates
        // per-row dynamic dispatch.
        for (fi, typed) in typed_cols.iter().enumerate() {
            if let Some(v) = typed.value_f64(row) {
                groups[gidx].1[fi].update(v, ts);
            }
        }
    }

    // Build result schema
    let mut result_fields: Vec<Field> = group_by
        .iter()
        .map(|name| Field::new(*name, DataType::Utf8, true))
        .collect();

    for &field_name in field_columns {
        for func in functions {
            let func_name = format!("{field_name}_{}", agg_fn_name(*func));
            result_fields.push(Field::new(&func_name, DataType::Float64, true));
        }
    }

    // Build result arrays
    let num_groups = groups.len();
    let mut result_columns: Vec<ArrayRef> = Vec::new();

    // Group-by value columns.
    for col_idx in 0..group_by.len() {
        let vals: Vec<Option<&str>> = groups.iter().map(|(key, _)| key[col_idx]).collect();
        let arr: StringArray = vals.into_iter().collect();
        result_columns.push(Arc::new(arr));
    }

    // Finalize accumulators — each IncrementalAccumulator
    // produces results for any requested AggFn in O(1).
    for fi in 0..num_fields {
        for func in functions {
            let vals: Vec<Option<f64>> = (0..num_groups)
                .map(|gi| groups[gi].1[fi].finalize(*func))
                .collect();
            result_columns.push(Arc::new(Float64Array::from(vals)));
        }
    }

    let schema = Arc::new(Schema::new(result_fields));
    let batch = RecordBatch::try_new(schema, result_columns)?;
    if let Some(tracker) = memory_tracker {
        tracker.try_allocate(batch.get_array_memory_size())?;
    }
    Ok(batch)
}

/// Aggregate a single column using the given function.
///
/// Timestamp-aware column aggregation.
#[allow(clippy::cast_precision_loss)]
fn aggregate_column_ts(
    col: &dyn Array,
    func: AggFn,
    timestamps: Option<&Int64Array>,
) -> Option<f64> {
    // For First/Last with a timestamp column, find the index of the
    // min/max timestamp and return the value at that index.
    if let Some(ts) = timestamps {
        if matches!(func, AggFn::First | AggFn::Last) {
            return first_last_by_ts(col, ts, func);
        }
    }

    if let Some(f64_col) = col.as_any().downcast_ref::<Float64Array>() {
        aggregate_f64(f64_col, func)
    } else if let Some(i64_col) = col.as_any().downcast_ref::<Int64Array>() {
        aggregate_i64(i64_col, func)
    } else if let Some(u64_col) = col.as_any().downcast_ref::<UInt64Array>() {
        aggregate_u64(u64_col, func)
    } else {
        match func {
            AggFn::Count => Some((col.len() - col.null_count()) as f64),
            _ => None,
        }
    }
}

/// Return the value at the row with the minimum (First) or maximum (Last)
/// timestamp.  Works for any numeric column type.
#[allow(clippy::cast_precision_loss)]
fn first_last_by_ts(col: &dyn Array, ts: &Int64Array, func: AggFn) -> Option<f64> {
    let find_idx = |cmp: fn(i64, i64) -> bool| -> Option<usize> {
        let mut best_idx: Option<usize> = None;
        let mut best_ts: i64 = 0;
        for i in 0..ts.len() {
            if ts.is_null(i) || col.is_null(i) {
                continue;
            }
            let t = ts.value(i);
            if best_idx.is_none() || cmp(t, best_ts) {
                best_idx = Some(i);
                best_ts = t;
            }
        }
        best_idx
    };

    let idx = match func {
        AggFn::First => find_idx(|t, best| t < best),
        // Use `>=` so that on ties, the last row encountered wins.
        // This makes Last deterministic: for a given input order, the
        // latest-inserted row at the max timestamp is always selected.
        AggFn::Last => find_idx(|t, best| t >= best),
        _ => return None,
    }?;

    // Extract the value at the selected index.
    if let Some(f64_col) = col.as_any().downcast_ref::<Float64Array>() {
        Some(f64_col.value(idx))
    } else if let Some(i64_col) = col.as_any().downcast_ref::<Int64Array>() {
        Some(i64_col.value(idx) as f64)
    } else {
        col.as_any()
            .downcast_ref::<UInt64Array>()
            .map(|u64_col| u64_col.value(idx) as f64)
    }
}

// take_indices() removed — grouped aggregation now uses
// IncrementalAccumulator per (group, field) in a single pass,
// eliminating per-group compute::take() allocations.

/// Human-readable function name for column naming.
#[must_use]
pub fn agg_fn_name(func: AggFn) -> &'static str {
    match func {
        AggFn::Count => "count",
        AggFn::Sum => "sum",
        AggFn::Min => "min",
        AggFn::Max => "max",
        AggFn::Avg => "avg",
        AggFn::First => "first",
        AggFn::Last => "last",
    }
}

// ── Streaming (incremental) aggregation ─────────────────────────

/// Pre-downcasted column reference — eliminates per-row `as_any().downcast_ref()`
/// calls in the inner loop. The downcast is performed once per column.
#[derive(Clone, Copy)]
enum TypedColumn<'a> {
    Float64(&'a Float64Array),
    Int64(&'a Int64Array),
    UInt64(&'a UInt64Array),
    Other,
}

impl<'a> TypedColumn<'a> {
    fn from_array(arr: &'a dyn Array) -> Self {
        if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
            Self::Float64(a)
        } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
            Self::Int64(a)
        } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
            Self::UInt64(a)
        } else {
            Self::Other
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn value_f64(&self, row: usize) -> Option<f64> {
        match self {
            Self::Float64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
            Self::Int64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row) as f64)
                }
            }
            Self::UInt64(a) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row) as f64)
                }
            }
            Self::Other => None,
        }
    }
}

/// Per-field incremental accumulator that supports all `AggFn` operations
/// without materializing the input.
#[derive(Debug, Clone)]
struct IncrementalAccumulator {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    /// Value at the minimum timestamp (First).
    first_ts: i64,
    first_val: f64,
    first_set: bool,
    /// Value at the maximum timestamp (Last).
    last_ts: i64,
    last_val: f64,
    last_set: bool,
}

impl IncrementalAccumulator {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first_ts: i64::MAX,
            first_val: 0.0,
            first_set: false,
            last_ts: i64::MIN,
            last_val: 0.0,
            last_set: false,
        }
    }

    fn update(&mut self, value: f64, timestamp: Option<i64>) {
        self.count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        // Skip First/Last tracking when timestamp is null (consistent
        // with the non-grouped `first_last_by_ts` path which also skips
        // null-timestamp rows).
        if let Some(ts) = timestamp {
            if ts < self.first_ts || !self.first_set {
                self.first_ts = ts;
                self.first_val = value;
                self.first_set = true;
            }
            // Use `>=` so that on ties, the last row encountered wins
            // (consistent with the non-grouped `first_last_by_ts` path).
            if ts >= self.last_ts || !self.last_set {
                self.last_ts = ts;
                self.last_val = value;
                self.last_set = true;
            }
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn finalize(&self, func: AggFn) -> Option<f64> {
        if self.count == 0 {
            return if func == AggFn::Count {
                Some(0.0)
            } else {
                None
            };
        }
        match func {
            AggFn::Count => Some(self.count as f64),
            AggFn::Sum => Some(self.sum),
            AggFn::Min => Some(self.min),
            AggFn::Max => Some(self.max),
            AggFn::Avg => Some(self.sum / self.count as f64),
            AggFn::First => Some(self.first_val),
            AggFn::Last => Some(self.last_val),
        }
    }
}

/// Aggregation strategy chosen based on estimated group-key cardinality.
///
/// When cardinality metadata is available from segment column
/// statistics, the engine selects the strategy that minimises overhead:
///
/// - **Hash**: O(N) single-pass with FNV-1a hash table. Best for high
///   cardinality (many distinct groups) where sort cost dominates.
/// - **Sort**: Sort on group-by columns via Arrow `sort_to_indices`, then
///   linear scan over contiguous runs. Best for low cardinality where the
///   sort is cheap and avoids hash-table memory overhead. Produces sorted
///   output — advantageous for downstream ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationStrategy {
    /// FNV-1a hash-table grouping (default).
    Hash,
    /// Sort group-by columns first, then linear-scan contiguous runs.
    Sort,
}

/// Cardinality threshold below which sort-based aggregation is preferred.
/// Empirically, sort wins when group count is ≤ ~1024 because the O(N log N)
/// sort cost is offset by better cache locality, no hash collisions, and
/// sorted output for free.
const SORT_CARDINALITY_THRESHOLD: usize = 1024;

/// Choose an aggregation strategy based on estimated cardinality.
///
/// - `None` → Hash (conservative default, universal).
/// - `Some(c)` where `c ≤ SORT_CARDINALITY_THRESHOLD` → Sort.
/// - `Some(c)` where `c > SORT_CARDINALITY_THRESHOLD` → Hash.
#[must_use]
pub fn choose_strategy(estimated_cardinality: Option<usize>) -> AggregationStrategy {
    match estimated_cardinality {
        Some(c) if c <= SORT_CARDINALITY_THRESHOLD => AggregationStrategy::Sort,
        _ => AggregationStrategy::Hash,
    }
}

/// Sort-based group-by aggregation.
///
/// Sorts the batch on group-by columns, then iterates over contiguous runs
/// of equal keys, accumulating into `IncrementalAccumulator` per field.
/// Produces output in sorted group-key order.
///
/// Preferred for low-cardinality group-by where sort cost is small and the
/// sorted output is often useful for downstream processing.
///
/// # Errors
///
/// Returns an error if required columns are missing.
pub fn aggregate_sorted(
    batch: &RecordBatch,
    functions: &[AggFn],
    field_columns: &[&str],
    group_by: &[&str],
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<RecordBatch> {
    use arrow::row::{RowConverter, SortField};

    if group_by.is_empty() {
        return aggregate_batch(batch, functions, field_columns, memory_tracker);
    }

    // Build Arrow SortFields for group-by columns.
    let group_arrays: Vec<ArrayRef> = group_by
        .iter()
        .map(|name| {
            batch.column_by_name(name).cloned().ok_or_else(|| {
                QueryError::Validation(format!("group-by column '{name}' not found"))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let sort_fields: Vec<SortField> = group_arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();

    let converter = RowConverter::new(sort_fields)?;
    let rows = converter.convert_columns(&group_arrays)?;

    // Build sort indices from row-encoded keys.
    let mut indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
    indices.sort_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));

    // Pre-resolve field columns.
    let field_cols: Vec<&dyn Array> = field_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .map(std::convert::AsRef::as_ref)
                .ok_or_else(|| QueryError::Validation(format!("column '{name}' not found")))
        })
        .collect::<Result<Vec<_>>>()?;
    let typed_cols: Vec<TypedColumn<'_>> = field_cols
        .iter()
        .map(|c| TypedColumn::from_array(*c))
        .collect();

    let ts_col = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

    let group_cols: Vec<&StringArray> = group_by
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    QueryError::Validation(format!(
                        "group-by column '{name}' not found or not string"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let num_fields = field_columns.len();
    let mut groups: Vec<(Vec<Option<String>>, Vec<IncrementalAccumulator>)> = Vec::new();

    // Linear scan over sorted indices — contiguous equal keys form groups.
    let mut run_start = 0usize;
    while run_start < indices.len() {
        let anchor_row = rows.row(indices[run_start] as usize);
        let mut run_end = run_start + 1;
        while run_end < indices.len() && rows.row(indices[run_end] as usize) == anchor_row {
            run_end += 1;
        }

        // Extract group key from the first row in the run.
        let first_idx = indices[run_start] as usize;
        let key: Vec<Option<String>> = group_cols
            .iter()
            .map(|col| {
                if col.is_null(first_idx) {
                    None
                } else {
                    Some(col.value(first_idx).to_owned())
                }
            })
            .collect();

        if let Some(tracker) = memory_tracker {
            let est = 24
                + group_by.len() * 48
                + num_fields * std::mem::size_of::<IncrementalAccumulator>()
                + 64;
            tracker.try_allocate(est)?;
        }

        let mut accs: Vec<IncrementalAccumulator> = (0..num_fields)
            .map(|_| IncrementalAccumulator::new())
            .collect();

        for &idx in &indices[run_start..run_end] {
            let row = idx as usize;
            let ts: Option<i64> = ts_col.and_then(|t| {
                if t.is_null(row) {
                    None
                } else {
                    Some(t.value(row))
                }
            });
            for (fi, typed) in typed_cols.iter().enumerate() {
                if let Some(v) = typed.value_f64(row) {
                    accs[fi].update(v, ts);
                }
            }
        }

        groups.push((key, accs));
        run_start = run_end;
    }

    // Build result schema and arrays.
    let mut result_fields: Vec<Field> = group_by
        .iter()
        .map(|name| Field::new(*name, DataType::Utf8, true))
        .collect();
    for &field_name in field_columns {
        for func in functions {
            let func_name = format!("{field_name}_{}", agg_fn_name(*func));
            result_fields.push(Field::new(&func_name, DataType::Float64, true));
        }
    }

    let num_groups = groups.len();
    let mut result_columns: Vec<ArrayRef> = Vec::new();

    for col_idx in 0..group_by.len() {
        let vals: Vec<Option<&str>> = groups
            .iter()
            .map(|(key, _)| key[col_idx].as_deref())
            .collect();
        let arr: StringArray = vals.into_iter().collect();
        result_columns.push(Arc::new(arr));
    }

    for fi in 0..num_fields {
        for func in functions {
            let vals: Vec<Option<f64>> = (0..num_groups)
                .map(|gi| groups[gi].1[fi].finalize(*func))
                .collect();
            result_columns.push(Arc::new(Float64Array::from(vals)));
        }
    }

    let schema = Arc::new(Schema::new(result_fields));
    let batch = RecordBatch::try_new(schema, result_columns)?;
    if let Some(tracker) = memory_tracker {
        tracker.try_allocate(batch.get_array_memory_size())?;
    }
    Ok(batch)
}

/// Streaming group-by aggregation over multiple `RecordBatch`es.
///
/// Convenience wrapper over [`StreamingAggregator`] for callers that already
/// hold every batch. Prefer pushing batches into the aggregator directly when
/// they arrive from a lazy source — that is what keeps peak memory
/// proportional to the number of *groups* rather than the number of rows.
///
/// # Errors
///
/// Returns an error if required columns are missing or `batches` is empty.
pub fn aggregate_grouped_streaming(
    batches: &[RecordBatch],
    functions: &[AggFn],
    field_columns: &[&str],
    group_by: &[&str],
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(QueryError::Validation("no batches to aggregate".into()));
    }
    let mut agg = StreamingAggregator::new(functions, field_columns, group_by);
    for batch in batches {
        agg.push(batch, memory_tracker)?;
    }
    agg.finish(memory_tracker)
}

/// Constant-memory aggregation state that batches are pushed into.
///
/// Aggregation does not need to see its input all at once: every supported
/// function has an incremental form, so the state is `O(groups × fields)` and
/// each batch can be dropped as soon as it has been folded in. Holding the
/// input instead would make a `SELECT avg(x) FROM m` over a week of 1 s data
/// cost gigabytes for a single output row.
///
/// Rows must carry the group-by and field columns by name; a batch missing a
/// column simply contributes nothing for it, which is how schema evolution
/// across segments is tolerated.
pub struct StreamingAggregator {
    functions: Vec<AggFn>,
    field_columns: Vec<String>,
    group_by: Vec<String>,
    state: AggState,
}

enum AggState {
    /// One accumulator set per distinct group key.
    Grouped {
        groups: Vec<(Vec<Option<String>>, Vec<IncrementalAccumulator>)>,
        /// Group-key hash → candidate indices into `groups` (collision chain).
        index: std::collections::HashMap<u64, Vec<usize>>,
    },
    /// Whole dataset is one group — folded with Arrow kernels per batch.
    Ungrouped(UngroupedState),
}

/// Per-field partial state for the ungrouped fold.
struct UngroupedState {
    counts: Vec<u64>,
    sums: Vec<f64>,
    mins: Vec<f64>,
    maxs: Vec<f64>,
    first_ts: Vec<i64>,
    first_val: Vec<f64>,
    first_set: Vec<bool>,
    last_ts: Vec<i64>,
    last_val: Vec<f64>,
    last_set: Vec<bool>,
    needs_first_last: bool,
}

impl StreamingAggregator {
    /// Create an aggregator for `functions` over `field_columns`, grouped by
    /// `group_by` (empty for a whole-dataset aggregate).
    #[must_use]
    pub fn new(functions: &[AggFn], field_columns: &[&str], group_by: &[&str]) -> Self {
        let num_fields = field_columns.len();
        let state = if group_by.is_empty() {
            AggState::Ungrouped(UngroupedState {
                counts: vec![0; num_fields],
                sums: vec![0.0; num_fields],
                mins: vec![f64::INFINITY; num_fields],
                maxs: vec![f64::NEG_INFINITY; num_fields],
                first_ts: vec![i64::MAX; num_fields],
                first_val: vec![0.0; num_fields],
                first_set: vec![false; num_fields],
                last_ts: vec![i64::MIN; num_fields],
                last_val: vec![0.0; num_fields],
                last_set: vec![false; num_fields],
                needs_first_last: functions
                    .iter()
                    .any(|f| matches!(f, AggFn::First | AggFn::Last)),
            })
        } else {
            AggState::Grouped {
                groups: Vec::new(),
                index: std::collections::HashMap::new(),
            }
        };
        Self {
            functions: functions.to_vec(),
            field_columns: field_columns.iter().map(|s| (*s).to_string()).collect(),
            group_by: group_by.iter().map(|s| (*s).to_string()).collect(),
            state,
        }
    }

    /// Fold one batch into the running state. The batch may be dropped after.
    ///
    /// # Errors
    ///
    /// Returns an error if the optional `memory_tracker` budget is exceeded by
    /// newly discovered groups.
    pub fn push(
        &mut self,
        batch: &RecordBatch,
        memory_tracker: Option<&crate::memory::MemoryTracker>,
    ) -> Result<()> {
        let field_columns: Vec<&str> = self.field_columns.iter().map(String::as_str).collect();
        match &mut self.state {
            AggState::Ungrouped(st) => {
                st.push(batch, &field_columns);
                Ok(())
            }
            AggState::Grouped { groups, index } => {
                let group_by: Vec<&str> = self.group_by.iter().map(String::as_str).collect();
                push_grouped(
                    batch,
                    &field_columns,
                    &group_by,
                    groups,
                    index,
                    memory_tracker,
                )
            }
        }
    }

    /// Finalise the accumulated state into a result batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the result batch cannot be built or the optional
    /// `memory_tracker` budget is exceeded.
    pub fn finish(
        self,
        memory_tracker: Option<&crate::memory::MemoryTracker>,
    ) -> Result<RecordBatch> {
        let field_columns: Vec<&str> = self.field_columns.iter().map(String::as_str).collect();
        let batch = match self.state {
            AggState::Ungrouped(st) => st.finish(&self.functions, &field_columns)?,
            AggState::Grouped { groups, .. } => {
                let group_by: Vec<&str> = self.group_by.iter().map(String::as_str).collect();
                finish_grouped(&groups, &self.functions, &field_columns, &group_by)?
            }
        };
        if let Some(tracker) = memory_tracker {
            tracker.try_allocate(batch.get_array_memory_size())?;
        }
        Ok(batch)
    }
}

/// Fold one batch into the grouped accumulator state.
fn push_grouped(
    batch: &RecordBatch,
    field_columns: &[&str],
    group_by: &[&str],
    groups: &mut Vec<(Vec<Option<String>>, Vec<IncrementalAccumulator>)>,
    group_index: &mut std::collections::HashMap<u64, Vec<usize>>,
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<()> {
    let num_fields = field_columns.len();

    let group_cols: Vec<Option<&StringArray>> = group_by
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        })
        .collect();

    let ts_col = batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

    // Resolve field columns for this batch — pre-downcast once.
    let typed_cols: Vec<Option<TypedColumn<'_>>> = field_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .map(|c| TypedColumn::from_array(c.as_ref()))
        })
        .collect();

    for row in 0..batch.num_rows() {
        let ts: Option<i64> = ts_col.and_then(|t| {
            if t.is_null(row) {
                None
            } else {
                Some(t.value(row))
            }
        });

        // Hash the group key.
        let mut hasher = fnv::FnvHasher::default();
        for col in &group_cols {
            match col {
                Some(c) if !c.is_null(row) => {
                    std::hash::Hash::hash(&1u8, &mut hasher);
                    std::hash::Hash::hash(c.value(row), &mut hasher);
                }
                _ => std::hash::Hash::hash(&0u8, &mut hasher),
            }
        }
        let hash = std::hash::Hasher::finish(&hasher);

        let key: Vec<Option<&str>> = group_cols
            .iter()
            .map(|col| match col {
                Some(c) if !c.is_null(row) => Some(c.value(row)),
                _ => None,
            })
            .collect();

        // Find or create the group. The hash is only a bucket selector — the
        // full key is compared, so a collision cannot merge two series.
        let bucket = group_index.entry(hash).or_default();
        let gidx = {
            let mut found = None;
            for &idx in bucket.iter() {
                let existing: Vec<Option<&str>> =
                    groups[idx].0.iter().map(|o| o.as_deref()).collect();
                if existing == key {
                    found = Some(idx);
                    break;
                }
            }
            match found {
                Some(idx) => idx,
                None => {
                    // Check memory budget before adding a new group.
                    if let Some(tracker) = memory_tracker {
                        let estimated_bytes = group_by.len() * 48  // owned Strings
                            + num_fields * std::mem::size_of::<IncrementalAccumulator>()
                            + 48; // HashMap entry + Vec overhead
                        tracker.try_allocate(estimated_bytes)?;
                    }
                    let idx = groups.len();
                    let owned_key: Vec<Option<String>> =
                        key.iter().map(|o| o.map(String::from)).collect();
                    let accs = (0..num_fields)
                        .map(|_| IncrementalAccumulator::new())
                        .collect();
                    groups.push((owned_key, accs));
                    bucket.push(idx);
                    idx
                }
            }
        };

        // Update one accumulator per field — single pass, no
        // redundant accumulator updates across function variants.
        for (fi, typed) in typed_cols.iter().enumerate() {
            if let Some(v) = typed.and_then(|tc| tc.value_f64(row)) {
                groups[gidx].1[fi].update(v, ts);
            }
        }
    }

    Ok(())
}

/// Build the result batch from grouped accumulator state.
fn finish_grouped(
    groups: &[(Vec<Option<String>>, Vec<IncrementalAccumulator>)],
    functions: &[AggFn],
    field_columns: &[&str],
    group_by: &[&str],
) -> Result<RecordBatch> {
    let num_groups = groups.len();
    let mut result_fields: Vec<Field> = group_by
        .iter()
        .map(|name| Field::new(*name, DataType::Utf8, true))
        .collect();
    for &field_name in field_columns {
        for func in functions {
            let name = format!("{field_name}_{}", agg_fn_name(*func));
            result_fields.push(Field::new(&name, DataType::Float64, true));
        }
    }

    let mut result_columns: Vec<ArrayRef> = Vec::new();

    // Group-by value columns.
    for col_idx in 0..group_by.len() {
        let vals: Vec<Option<&str>> = groups
            .iter()
            .map(|(key, _)| key[col_idx].as_deref())
            .collect();
        let arr: StringArray = vals.into_iter().collect();
        result_columns.push(Arc::new(arr));
    }

    // Aggregation result columns — one accumulator per field.
    for fi in 0..field_columns.len() {
        for func in functions {
            let vals: Vec<Option<f64>> = (0..num_groups)
                .map(|gi| groups[gi].1[fi].finalize(*func))
                .collect();
            result_columns.push(Arc::new(Float64Array::from(vals)));
        }
    }

    let schema = Arc::new(Schema::new(result_fields));
    RecordBatch::try_new(schema, result_columns).map_err(Into::into)
}

impl UngroupedState {
    /// Fold one batch, using Arrow SIMD kernels for count/sum/min/max.
    /// First/Last still require a linear scan to find the row at the
    /// min/max timestamp.
    #[allow(clippy::cast_precision_loss)]
    fn push(&mut self, batch: &RecordBatch, field_columns: &[&str]) {
        let ts_col = batch
            .column_by_name(chronix_core::TIME_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        for (fi, &field_name) in field_columns.iter().enumerate() {
            let Some(col) = batch.column_by_name(field_name) else {
                continue;
            };

            // Vectorized count via O(1) Arrow metadata.
            self.counts[fi] += (col.len() - col.null_count()) as u64;

            // Vectorized Sum/Min/Max using Arrow SIMD kernels.
            let typed = TypedColumn::from_array(col.as_ref());
            match typed {
                TypedColumn::Float64(a) => {
                    if let Some(s) = arrow_agg::sum(a) {
                        self.sums[fi] += s;
                    }
                    if let Some(m) = arrow_agg::min(a) {
                        if m < self.mins[fi] {
                            self.mins[fi] = m;
                        }
                    }
                    if let Some(m) = arrow_agg::max(a) {
                        if m > self.maxs[fi] {
                            self.maxs[fi] = m;
                        }
                    }
                }
                TypedColumn::Int64(a) => {
                    if let Some(s) = arrow_agg::sum(a) {
                        self.sums[fi] += s as f64;
                    }
                    if let Some(m) = arrow_agg::min(a) {
                        if (m as f64) < self.mins[fi] {
                            self.mins[fi] = m as f64;
                        }
                    }
                    if let Some(m) = arrow_agg::max(a) {
                        if (m as f64) > self.maxs[fi] {
                            self.maxs[fi] = m as f64;
                        }
                    }
                }
                TypedColumn::UInt64(a) => {
                    if let Some(s) = arrow_agg::sum(a) {
                        self.sums[fi] += s as f64;
                    }
                    if let Some(m) = arrow_agg::min(a) {
                        if (m as f64) < self.mins[fi] {
                            self.mins[fi] = m as f64;
                        }
                    }
                    if let Some(m) = arrow_agg::max(a) {
                        if (m as f64) > self.maxs[fi] {
                            self.maxs[fi] = m as f64;
                        }
                    }
                }
                TypedColumn::Other => {}
            }

            // First/Last: find value at the row with min/max timestamp.
            // This is inherently a scatter operation (argmin/argmax + value
            // lookup), so a single linear scan per batch is optimal.
            if self.needs_first_last {
                if let Some(ts) = ts_col {
                    let n = ts.len().min(col.len());
                    for i in 0..n {
                        if ts.is_null(i) || col.is_null(i) {
                            continue;
                        }
                        let t = ts.value(i);
                        if let Some(v) = typed.value_f64(i) {
                            if t < self.first_ts[fi] || !self.first_set[fi] {
                                self.first_ts[fi] = t;
                                self.first_val[fi] = v;
                                self.first_set[fi] = true;
                            }
                            // `>=` so last-encountered wins on ties.
                            if t >= self.last_ts[fi] || !self.last_set[fi] {
                                self.last_ts[fi] = t;
                                self.last_val[fi] = v;
                                self.last_set[fi] = true;
                            }
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn finish(&self, functions: &[AggFn], field_columns: &[&str]) -> Result<RecordBatch> {
        let mut result_fields = Vec::new();
        let mut result_columns: Vec<ArrayRef> = Vec::new();
        for (fi, &field_name) in field_columns.iter().enumerate() {
            for func in functions {
                let name = format!("{field_name}_{}", agg_fn_name(*func));
                result_fields.push(Field::new(&name, DataType::Float64, true));
                let value = if self.counts[fi] == 0 {
                    if *func == AggFn::Count {
                        Some(0.0)
                    } else {
                        None
                    }
                } else {
                    match func {
                        AggFn::Count => Some(self.counts[fi] as f64),
                        AggFn::Sum => Some(self.sums[fi]),
                        AggFn::Min => Some(self.mins[fi]),
                        AggFn::Max => Some(self.maxs[fi]),
                        AggFn::Avg => Some(self.sums[fi] / self.counts[fi] as f64),
                        AggFn::First => self.first_set[fi].then_some(self.first_val[fi]),
                        AggFn::Last => self.last_set[fi].then_some(self.last_val[fi]),
                    }
                };
                result_columns.push(Arc::new(Float64Array::from(vec![value])));
            }
        }

        let schema = Arc::new(Schema::new(result_fields));
        RecordBatch::try_new(schema, result_columns).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let timestamps = Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500]));
        let hosts = Arc::new(StringArray::from(vec!["a", "a", "b", "b", "a"]));
        let values = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]));

        RecordBatch::try_new(schema, vec![timestamps, hosts, values]).unwrap()
    }

    #[test]
    fn aggregate_count() {
        let batch = test_batch();
        let result = aggregate_batch(&batch, &[AggFn::Count], &["value"], None).unwrap();
        assert_eq!(result.num_rows(), 1);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_sum() {
        let batch = test_batch();
        let result = aggregate_batch(&batch, &[AggFn::Sum], &["value"], None).unwrap();
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_min_max() {
        let batch = test_batch();
        let result = aggregate_batch(&batch, &[AggFn::Min, AggFn::Max], &["value"], None).unwrap();
        let min_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((min_col.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((max_col.value(0) - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_avg() {
        let batch = test_batch();
        let result = aggregate_batch(&batch, &[AggFn::Avg], &["value"], None).unwrap();
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_first_last() {
        let batch = test_batch();
        let result =
            aggregate_batch(&batch, &[AggFn::First, AggFn::Last], &["value"], None).unwrap();
        let first = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let last = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((first.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((last.value(0) - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_grouped_by_host() {
        let batch = test_batch();
        let result = aggregate_grouped(
            &batch,
            &[AggFn::Sum, AggFn::Count],
            &["value"],
            &["host"],
            None,
        )
        .unwrap();

        assert_eq!(result.num_rows(), 2); // two groups: a, b

        let hosts = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let counts = result
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Groups maintain insertion order (first-seen order)
        assert_eq!(hosts.value(0), "a");
        assert!((sums.value(0) - 80.0).abs() < f64::EPSILON); // 10+20+50
        assert!((counts.value(0) - 3.0).abs() < f64::EPSILON);

        assert_eq!(hosts.value(1), "b");
        assert!((sums.value(1) - 70.0).abs() < f64::EPSILON); // 30+40
        assert!((counts.value(1) - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn all_functions_on_known_data() {
        let batch = test_batch();
        let result = aggregate_batch(
            &batch,
            &[
                AggFn::Count,
                AggFn::Sum,
                AggFn::Min,
                AggFn::Max,
                AggFn::Avg,
                AggFn::First,
                AggFn::Last,
            ],
            &["value"],
            None,
        )
        .unwrap();

        assert_eq!(result.num_columns(), 7);
        let cols: Vec<f64> = (0..7)
            .map(|i| {
                result
                    .column(i)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect();

        assert!((cols[0] - 5.0).abs() < f64::EPSILON); // count
        assert!((cols[1] - 150.0).abs() < f64::EPSILON); // sum
        assert!((cols[2] - 10.0).abs() < f64::EPSILON); // min
        assert!((cols[3] - 50.0).abs() < f64::EPSILON); // max
        assert!((cols[4] - 30.0).abs() < f64::EPSILON); // avg
        assert!((cols[5] - 10.0).abs() < f64::EPSILON); // first
        assert!((cols[6] - 50.0).abs() < f64::EPSILON); // last
    }

    // ── U64 aggregation tests ─────────────────────────────────────

    fn u64_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("count", DataType::UInt64, false),
        ]));
        let timestamps = Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500]));
        let counts = Arc::new(UInt64Array::from(vec![10u64, 20, 30, 40, 50]));
        RecordBatch::try_new(schema, vec![timestamps, counts]).unwrap()
    }

    #[test]
    fn aggregate_u64_sum() {
        let batch = u64_batch();
        let result = aggregate_batch(&batch, &[AggFn::Sum], &["count"], None).unwrap();
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_u64_min_max() {
        let batch = u64_batch();
        let result = aggregate_batch(&batch, &[AggFn::Min, AggFn::Max], &["count"], None).unwrap();
        let min_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((min_col.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((max_col.value(0) - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_u64_avg() {
        let batch = u64_batch();
        let result = aggregate_batch(&batch, &[AggFn::Avg], &["count"], None).unwrap();
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_u64_first_last() {
        let batch = u64_batch();
        let result =
            aggregate_batch(&batch, &[AggFn::First, AggFn::Last], &["count"], None).unwrap();
        let first = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let last = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((first.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((last.value(0) - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_u64_count() {
        let batch = u64_batch();
        let result = aggregate_batch(&batch, &[AggFn::Count], &["count"], None).unwrap();
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_respects_memory_tracker() {
        let batch = test_batch();
        let tracker = crate::memory::MemoryTracker::new(1_000_000);
        let result = aggregate_batch(&batch, &[AggFn::Sum], &["value"], Some(&tracker)).unwrap();
        assert!(tracker.allocated() > 0);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_exceeds_memory_budget() {
        let batch = test_batch();
        let tracker = crate::memory::MemoryTracker::new(1); // 1 byte budget
        let err = aggregate_batch(&batch, &[AggFn::Sum], &["value"], Some(&tracker)).unwrap_err();
        assert!(matches!(
            err,
            crate::error::QueryError::QueryMemoryExceeded { .. }
        ));
    }

    #[test]
    fn grouped_aggregate_respects_memory_tracker() {
        let batch = test_batch();
        let tracker = crate::memory::MemoryTracker::new(1_000_000);
        let result =
            aggregate_grouped(&batch, &[AggFn::Sum], &["value"], &["host"], Some(&tracker))
                .unwrap();
        assert!(tracker.allocated() > 0);
        assert_eq!(result.num_rows(), 2);
    }

    /// Out-of-order timestamps: First/Last should use timestamp ordering,
    /// not positional ordering.
    #[test]
    fn first_last_out_of_order_timestamps() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let ts = Int64Array::from(vec![300, 100, 500, 200, 400]);
        let host = StringArray::from(vec!["a", "a", "a", "a", "a"]);
        let value = Float64Array::from(vec![3.0, 1.0, 5.0, 2.0, 4.0]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(host), Arc::new(value)])
                .unwrap();

        let result =
            aggregate_batch(&batch, &[AggFn::First, AggFn::Last], &["value"], None).unwrap();
        let first = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let last = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // First = value at min timestamp (100) = 1.0
        assert!(
            (first.value(0) - 1.0).abs() < f64::EPSILON,
            "First should be 1.0, got {}",
            first.value(0)
        );
        // Last = value at max timestamp (500) = 5.0
        assert!(
            (last.value(0) - 5.0).abs() < f64::EPSILON,
            "Last should be 5.0, got {}",
            last.value(0)
        );
    }

    /// Timestamp ties: Last should keep the last-encountered row.
    #[test]
    fn first_last_timestamp_ties() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        // Two rows at ts=100 (first wins = 1.0), two rows at ts=200 (last-encountered wins = 4.0).
        let ts = Int64Array::from(vec![100, 200, 100, 200]);
        let host = StringArray::from(vec!["a", "a", "a", "a"]);
        let value = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(host), Arc::new(value)])
                .unwrap();

        let result =
            aggregate_batch(&batch, &[AggFn::First, AggFn::Last], &["value"], None).unwrap();
        let first = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let last = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // First: first-encountered at min-ts (100) = 1.0
        assert!((first.value(0) - 1.0).abs() < f64::EPSILON);
        // Last: last-encountered at max-ts (200) = 4.0
        assert!((last.value(0) - 4.0).abs() < f64::EPSILON);
    }

    /// Grouped First/Last should also use timestamps correctly.
    #[test]
    fn grouped_first_last_by_timestamp() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        // host=a: ts=[200,100] vals=[20,10] → first=10, last=20
        // host=b: ts=[300,100,200] vals=[30,10,20] → first=10, last=30
        let ts = Int64Array::from(vec![200, 100, 300, 100, 200]);
        let host = StringArray::from(vec!["a", "a", "b", "b", "b"]);
        let value = Float64Array::from(vec![20.0, 10.0, 30.0, 10.0, 20.0]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(host), Arc::new(value)])
                .unwrap();

        let result = aggregate_grouped(
            &batch,
            &[AggFn::First, AggFn::Last],
            &["value"],
            &["host"],
            None,
        )
        .unwrap();

        // Find host=a and host=b rows in result
        let host_col = result.column_by_name("host").unwrap();
        let host_arr = host_col.as_any().downcast_ref::<StringArray>().unwrap();
        let first_col = result.column_by_name("value_first").unwrap();
        let first_arr = first_col.as_any().downcast_ref::<Float64Array>().unwrap();
        let last_col = result.column_by_name("value_last").unwrap();
        let last_arr = last_col.as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..result.num_rows() {
            match host_arr.value(i) {
                "a" => {
                    assert!(
                        (first_arr.value(i) - 10.0).abs() < f64::EPSILON,
                        "host=a first"
                    );
                    assert!(
                        (last_arr.value(i) - 20.0).abs() < f64::EPSILON,
                        "host=a last"
                    );
                }
                "b" => {
                    assert!(
                        (first_arr.value(i) - 10.0).abs() < f64::EPSILON,
                        "host=b first"
                    );
                    assert!(
                        (last_arr.value(i) - 30.0).abs() < f64::EPSILON,
                        "host=b last"
                    );
                }
                other => panic!("unexpected host: {other}"),
            }
        }
    }

    /// Grouped Last with timestamp ties should use last-encountered.
    #[test]
    fn grouped_last_timestamp_tie_breaking() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        // host=a: two rows at ts=100, values [1.0, 2.0] → last-encountered wins = 2.0
        let ts = Int64Array::from(vec![100, 100]);
        let host = StringArray::from(vec!["a", "a"]);
        let value = Float64Array::from(vec![1.0, 2.0]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(host), Arc::new(value)])
                .unwrap();

        let result =
            aggregate_grouped(&batch, &[AggFn::Last], &["value"], &["host"], None).unwrap();

        let last_col = result.column_by_name("value_last").unwrap();
        let last_arr = last_col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(
            (last_arr.value(0) - 2.0).abs() < f64::EPSILON,
            "Last with tie should keep last-encountered (2.0), got {}",
            last_arr.value(0)
        );
    }

    /// Single-row batch: First and Last should return the same value.
    #[test]
    fn first_last_single_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let ts = Int64Array::from(vec![100]);
        let host = StringArray::from(vec!["a"]);
        let value = Float64Array::from(vec![42.0]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(host), Arc::new(value)])
                .unwrap();

        let result =
            aggregate_batch(&batch, &[AggFn::First, AggFn::Last], &["value"], None).unwrap();
        let first = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let last = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((first.value(0) - 42.0).abs() < f64::EPSILON);
        assert!((last.value(0) - 42.0).abs() < f64::EPSILON);
    }

    // ── Sort-based aggregation tests ─────────────────

    #[test]
    fn sort_aggregate_matches_hash() {
        let batch = test_batch();
        let hash_result = aggregate_grouped(
            &batch,
            &[AggFn::Sum, AggFn::Count, AggFn::Min, AggFn::Max, AggFn::Avg],
            &["value"],
            &["host"],
            None,
        )
        .unwrap();
        let sort_result = aggregate_sorted(
            &batch,
            &[AggFn::Sum, AggFn::Count, AggFn::Min, AggFn::Max, AggFn::Avg],
            &["value"],
            &["host"],
            None,
        )
        .unwrap();

        // Same number of groups.
        assert_eq!(hash_result.num_rows(), sort_result.num_rows());

        // Collect results by host for order-independent comparison.
        let hash_hosts = hash_result
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sort_hosts = sort_result
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for host in &["a", "b"] {
            let hi = (0..hash_result.num_rows())
                .find(|&i| hash_hosts.value(i) == *host)
                .unwrap();
            let si = (0..sort_result.num_rows())
                .find(|&i| sort_hosts.value(i) == *host)
                .unwrap();

            for col_name in &[
                "value_sum",
                "value_count",
                "value_min",
                "value_max",
                "value_avg",
            ] {
                let hv = hash_result
                    .column_by_name(col_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(hi);
                let sv = sort_result
                    .column_by_name(col_name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(si);
                assert!(
                    (hv - sv).abs() < f64::EPSILON,
                    "mismatch for host={host} col={col_name}: hash={hv}, sort={sv}"
                );
            }
        }
    }

    #[test]
    fn sort_aggregate_first_last() {
        let batch = test_batch();
        let result = aggregate_sorted(
            &batch,
            &[AggFn::First, AggFn::Last],
            &["value"],
            &["host"],
            None,
        )
        .unwrap();

        let host_col = result
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let first_col = result
            .column_by_name("value_first")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let last_col = result
            .column_by_name("value_last")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..result.num_rows() {
            match host_col.value(i) {
                "a" => {
                    // ts=[100,200,500] vals=[10,20,50] → first=10, last=50
                    assert!((first_col.value(i) - 10.0).abs() < f64::EPSILON);
                    assert!((last_col.value(i) - 50.0).abs() < f64::EPSILON);
                }
                "b" => {
                    // ts=[300,400] vals=[30,40] → first=30, last=40
                    assert!((first_col.value(i) - 30.0).abs() < f64::EPSILON);
                    assert!((last_col.value(i) - 40.0).abs() < f64::EPSILON);
                }
                other => panic!("unexpected host: {other}"),
            }
        }
    }

    #[test]
    fn sort_aggregate_produces_sorted_output() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let ts = Int64Array::from(vec![100, 200, 300]);
        let regions = StringArray::from(vec!["eu", "us", "ap"]);
        let values = Float64Array::from(vec![1.0, 2.0, 3.0]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ts), Arc::new(regions), Arc::new(values)],
        )
        .unwrap();

        let result =
            aggregate_sorted(&batch, &[AggFn::Sum], &["value"], &["region"], None).unwrap();

        let region_col = result
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Sort-based aggregation should produce sorted group keys.
        let regions: Vec<&str> = (0..result.num_rows())
            .map(|i| region_col.value(i))
            .collect();
        assert_eq!(
            regions,
            vec!["ap", "eu", "us"],
            "output should be sorted by group key"
        );
    }

    #[test]
    fn choose_strategy_defaults_to_hash() {
        assert_eq!(choose_strategy(None), AggregationStrategy::Hash);
        assert_eq!(choose_strategy(Some(2000)), AggregationStrategy::Hash);
    }

    #[test]
    fn choose_strategy_low_cardinality_uses_sort() {
        assert_eq!(choose_strategy(Some(5)), AggregationStrategy::Sort);
        assert_eq!(choose_strategy(Some(1024)), AggregationStrategy::Sort);
    }

    #[test]
    fn sort_aggregate_no_group_by_delegates() {
        let batch = test_batch();
        let result = aggregate_sorted(&batch, &[AggFn::Sum], &["value"], &[], None).unwrap();
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 150.0).abs() < f64::EPSILON);
    }
}
