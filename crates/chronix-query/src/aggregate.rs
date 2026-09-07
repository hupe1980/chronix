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

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray, UInt64Array,
};
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

/// Extra fractional digits an average of a decimal column is computed to.
///
/// A mean is a division, and a division of exact decimals does not
/// generally terminate — `1/3` has no finite decimal form at any scale. So
/// `avg` is the one aggregate here that rounds, and the rule is stated
/// rather than left to a `double`: the result is carried to the column's
/// scale plus six digits and rounded half away from zero, the way
/// PostgreSQL's `NUMERIC` division extends its scale. Six digits is a
/// millionth of the column's last digit; `sum` and `count` are both exact,
/// so a caller who needs a different rounding can divide them itself.
pub const AVG_EXTRA_SCALE: u8 = 6;

/// One aggregate's result, in the type that keeps it exact.
///
/// Every column type but decimal reduces to an `f64`, which is what it
/// already was. A decimal column stays a decimal all the way to the output
/// array: `sum`, `min`, `max`, `first` and `last` over exact values are
/// themselves exact, and turning them into a `double` on the way out would
/// undo the whole point of the column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggResult {
    /// SQL `NULL` — an empty bucket, or a result that overflowed.
    Null,
    /// A floating-point result.
    F64(f64),
    /// An exact decimal result: `mantissa × 10⁻ˢᶜᵃˡᵉ`.
    Decimal {
        /// The unscaled result.
        mantissa: i128,
        /// Digits after the decimal point.
        scale: u8,
    },
}

impl AggResult {
    /// The result as an `f64`, for the callers that only deal in floats.
    ///
    /// Lossy for a decimal, by definition — see
    /// [`extract_f64`](crate::extract_f64) for where that is and is not the
    /// right thing to do.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn as_f64(self) -> Option<f64> {
        match self {
            Self::Null => None,
            Self::F64(v) => Some(v),
            Self::Decimal { mantissa, scale } => {
                Some(mantissa as f64 / chronix_core::pow10(u32::from(scale)).unwrap_or(1) as f64)
            }
        }
    }
}

/// Build one output column from a column of results.
///
/// Public because the rollup engine builds the same columns from the same
/// accumulator, and two functions that must agree on an output type are one
/// function.
///
/// The Arrow type follows the results: a decimal aggregate produces a
/// `Decimal128` column, everything else a `Float64` one. A run of results
/// is homogeneous by construction — they come from one accumulator over one
/// input column — so the first non-null one decides.
pub fn agg_result_column(name: &str, results: &[AggResult]) -> Result<(Field, ArrayRef)> {
    let scale = results.iter().find_map(|r| match r {
        AggResult::Decimal { scale, .. } => Some(*scale),
        _ => None,
    });
    match scale {
        Some(scale) => {
            let scale_i8 = i8::try_from(scale).map_err(|_| {
                QueryError::Validation(format!("decimal scale {scale} is out of range"))
            })?;
            let mantissas: Vec<Option<i128>> = results
                .iter()
                .map(|r| match r {
                    AggResult::Decimal { mantissa, .. } => Some(*mantissa),
                    _ => None,
                })
                .collect();
            let array = Decimal128Array::from(mantissas)
                .with_precision_and_scale(chronix_core::DECIMAL_PRECISION, scale_i8)?;
            let field = Field::new(
                name,
                DataType::Decimal128(chronix_core::DECIMAL_PRECISION, scale_i8),
                true,
            );
            Ok((field, Arc::new(array)))
        }
        None => {
            let values: Vec<Option<f64>> = results.iter().map(|r| r.as_f64()).collect();
            Ok((
                Field::new(name, DataType::Float64, true),
                Arc::new(Float64Array::from(values)),
            ))
        }
    }
}

/// A value pulled out of a column, in the type that keeps it exact.
#[derive(Clone, Copy, Debug)]
pub enum Num {
    /// Anything that was already a float, or was widened to one.
    F64(f64),
    /// A decimal mantissa together with its scale.
    Dec(i128, u8),
}

/// The exact half of an accumulator, allocated only for decimal columns.
///
/// Boxed behind an `Option` so a float column pays eight bytes for it and
/// nothing else — a group-by over a high-cardinality tag holds one
/// accumulator per (group, field), and making every one of them 128 bytes
/// wider to support a column type it does not have is a cost the gateway
/// would feel.
#[derive(Debug, Clone)]
pub struct DecimalTrack {
    /// The scale everything tracked here is expressed at.
    pub scale: u8,
    /// Running total.
    pub sum: i128,
    /// Smallest mantissa seen.
    pub min: i128,
    /// Largest mantissa seen.
    pub max: i128,
    /// Mantissa at the earliest timestamp.
    pub first: i128,
    /// Mantissa at the latest timestamp.
    pub last: i128,
    /// A sum or a rescale needed more than 38 significant digits. The
    /// affected results are `NULL`: a number that is not the answer is
    /// worse than no number.
    pub overflow: bool,
}

impl DecimalTrack {
    /// An empty track at `scale`.
    #[must_use]
    pub fn new(scale: u8) -> Self {
        Self {
            scale,
            sum: 0,
            min: i128::MAX,
            max: i128::MIN,
            first: 0,
            last: 0,
            overflow: false,
        }
    }

    /// Re-express everything tracked so far at a wider scale.
    pub fn widen_to(&mut self, scale: u8) {
        let Some(factor) = scale
            .checked_sub(self.scale)
            .and_then(|d| chronix_core::pow10(u32::from(d)))
        else {
            self.overflow = true;
            return;
        };
        let mut scaled = |v: i128| match v.checked_mul(factor) {
            Some(x) => x,
            None => {
                self.overflow = true;
                v
            }
        };
        self.sum = scaled(self.sum);
        // The sentinels stay sentinels: scaling `i128::MAX` overflows, and
        // it does not stand for a value anyway.
        if self.min != i128::MAX {
            self.min = scaled(self.min);
        }
        if self.max != i128::MIN {
            self.max = scaled(self.max);
        }
        self.first = scaled(self.first);
        self.last = scaled(self.last);
        self.scale = scale;
    }

    /// Bring an incoming mantissa to the track's scale, widening the track
    /// if the incoming value is finer. Both directions are exact.
    ///
    /// Returns `None` when the alignment would overflow 38 digits; the
    /// caller sets [`overflow`](Self::overflow).
    pub fn align(&mut self, mantissa: i128, scale: u8) -> Option<i128> {
        match scale.cmp(&self.scale) {
            std::cmp::Ordering::Equal => Some(mantissa),
            std::cmp::Ordering::Greater => {
                self.widen_to(scale);
                Some(mantissa)
            }
            std::cmp::Ordering::Less => chronix_core::pow10(u32::from(self.scale - scale))
                .and_then(|f| mantissa.checked_mul(f)),
        }
    }

    /// The sum, or `Null` if it overflowed 38 digits.
    #[must_use]
    pub fn sum_result(&self) -> AggResult {
        if self.overflow || chronix_core::Decimal::new(self.sum, self.scale).is_err() {
            return AggResult::Null;
        }
        AggResult::Decimal {
            mantissa: self.sum,
            scale: self.scale,
        }
    }

    /// The mean, carried to [`AVG_EXTRA_SCALE`] extra digits and rounded
    /// half away from zero. See that constant for why this is the one
    /// aggregate that rounds.
    #[must_use]
    pub fn avg_result(&self, count: u64) -> AggResult {
        if self.overflow || count == 0 {
            return AggResult::Null;
        }
        let target = self
            .scale
            .saturating_add(AVG_EXTRA_SCALE)
            .min(chronix_core::MAX_DECIMAL_SCALE);
        let extra = target - self.scale;
        let Some(numerator) =
            chronix_core::pow10(u32::from(extra)).and_then(|factor| self.sum.checked_mul(factor))
        else {
            return AggResult::Null;
        };
        let divisor = i128::from(count);
        let quotient = numerator / divisor;
        let remainder = numerator % divisor;
        // Half away from zero: `|r| * 2 >= divisor` rounds up in magnitude.
        let rounded = match remainder.unsigned_abs().checked_mul(2) {
            Some(twice) if twice >= divisor.unsigned_abs() => {
                quotient + if numerator < 0 { -1 } else { 1 }
            }
            _ => quotient,
        };
        if chronix_core::Decimal::new(rounded, target).is_err() {
            return AggResult::Null;
        }
        AggResult::Decimal {
            mantissa: rounded,
            scale: target,
        }
    }

    /// One tracked mantissa as a result.
    #[must_use]
    pub fn at(&self, mantissa: i128) -> AggResult {
        AggResult::Decimal {
            mantissa,
            scale: self.scale,
        }
    }
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
            let (field, array) = agg_result_column(&func_name, &[value])?;
            result_fields.push(field);
            result_columns.push(array);
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
            if let Some(v) = typed.value_num(row) {
                groups[gidx].1[fi].update(v, ts);
            }
        }
    }

    // Build result schema
    let mut result_fields: Vec<Field> = group_by
        .iter()
        .map(|name| Field::new(*name, DataType::Utf8, true))
        .collect();

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
    // produces results for any requested AggFn in O(1). The output column's
    // type follows the results, so an aggregate over a decimal column stays
    // a decimal.
    for (fi, &field_name) in field_columns.iter().enumerate() {
        for func in functions {
            let func_name = format!("{field_name}_{}", agg_fn_name(*func));
            let vals: Vec<AggResult> = (0..num_groups)
                .map(|gi| groups[gi].1[fi].finalize(*func))
                .collect();
            let (field, array) = agg_result_column(&func_name, &vals)?;
            result_fields.push(field);
            result_columns.push(array);
        }
    }
    debug_assert_eq!(num_fields, field_columns.len());

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
fn aggregate_column_ts(col: &dyn Array, func: AggFn, timestamps: Option<&Int64Array>) -> AggResult {
    // For First/Last with a timestamp column, find the index of the
    // min/max timestamp and return the value at that index.
    if let Some(ts) = timestamps {
        if matches!(func, AggFn::First | AggFn::Last) {
            return first_last_by_ts(col, ts, func);
        }
    }

    // A decimal column is folded exactly, through the same accumulator the
    // streaming path uses, rather than through the `f64` kernels.
    if let TypedColumn::Decimal(a, scale) = TypedColumn::from_array(col) {
        // With no timestamp column, First and Last mean array position —
        // the same fallback the `f64` path takes. The accumulator cannot
        // answer them here because it is fed no timestamps, and asking it
        // anyway returned its zero-initialised slot rather than a value.
        if matches!(func, AggFn::First | AggFn::Last) {
            let mut present = (0..a.len()).filter(|&i| !a.is_null(i));
            let idx = if func == AggFn::First {
                present.next()
            } else {
                present.next_back()
            };
            return idx.map_or(AggResult::Null, |i| AggResult::Decimal {
                mantissa: a.value(i),
                scale,
            });
        }
        let mut acc = IncrementalAccumulator::new();
        for i in 0..a.len() {
            if !a.is_null(i) {
                acc.update(Num::Dec(a.value(i), scale), None);
            }
        }
        return acc.finalize(func);
    }

    let value = if let Some(f64_col) = col.as_any().downcast_ref::<Float64Array>() {
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
    };
    value.map_or(AggResult::Null, AggResult::F64)
}

/// Return the value at the row with the minimum (First) or maximum (Last)
/// timestamp.  Works for any numeric column type.
#[allow(clippy::cast_precision_loss)]
fn first_last_by_ts(col: &dyn Array, ts: &Int64Array, func: AggFn) -> AggResult {
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
        _ => return AggResult::Null,
    };
    let Some(idx) = idx else {
        return AggResult::Null;
    };

    // Extract the value at the selected index.
    match TypedColumn::from_array(col).value_num(idx) {
        Some(Num::F64(v)) => AggResult::F64(v),
        Some(Num::Dec(mantissa, scale)) => AggResult::Decimal { mantissa, scale },
        None => AggResult::Null,
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
    /// An exact decimal column, with the scale off its Arrow type.
    Decimal(&'a Decimal128Array, u8),
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
        } else if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
            // A negative scale is legal Arrow and meaningless here; it reads
            // as 0 rather than aborting the whole aggregate.
            Self::Decimal(a, u8::try_from(a.scale()).unwrap_or(0))
        } else {
            Self::Other
        }
    }

    /// The value at `row`, in the type that keeps it exact.
    #[allow(clippy::cast_precision_loss)]
    fn value_num(&self, row: usize) -> Option<Num> {
        match self {
            Self::Decimal(a, scale) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(Num::Dec(a.value(row), *scale))
                }
            }
            _ => self.value_f64(row).map(Num::F64),
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
            // A decimal reaches an `f64` only through `value_num`, which
            // does not lose anything; this is the fallback for the callers
            // that genuinely want a float.
            Self::Decimal(a, scale) => {
                if a.is_null(row) {
                    None
                } else {
                    Some(
                        a.value(row) as f64
                            / chronix_core::pow10(u32::from(*scale)).unwrap_or(1) as f64,
                    )
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
    /// The exact track, present only once a decimal value has been seen.
    decimal: Option<Box<DecimalTrack>>,
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
            decimal: None,
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn update(&mut self, value: Num, timestamp: Option<i64>) {
        self.count += 1;
        match value {
            Num::F64(v) => {
                self.sum += v;
                if v < self.min {
                    self.min = v;
                }
                if v > self.max {
                    self.max = v;
                }
            }
            Num::Dec(mantissa, scale) => {
                let track = self
                    .decimal
                    .get_or_insert_with(|| Box::new(DecimalTrack::new(scale)));
                match track.align(mantissa, scale) {
                    Some(m) => {
                        match track.sum.checked_add(m) {
                            Some(sum) => track.sum = sum,
                            None => track.overflow = true,
                        }
                        if m < track.min {
                            track.min = m;
                        }
                        if m > track.max {
                            track.max = m;
                        }
                    }
                    None => track.overflow = true,
                }
            }
        }
        // Skip First/Last tracking when timestamp is null (consistent
        // with the non-grouped `first_last_by_ts` path which also skips
        // null-timestamp rows).
        if let Some(ts) = timestamp {
            let first = ts < self.first_ts || !self.first_set;
            // Use `>=` so that on ties, the last row encountered wins
            // (consistent with the non-grouped `first_last_by_ts` path).
            let last = ts >= self.last_ts || !self.last_set;
            if first {
                self.first_ts = ts;
                self.first_set = true;
            }
            if last {
                self.last_ts = ts;
                self.last_set = true;
            }
            match value {
                Num::F64(v) => {
                    if first {
                        self.first_val = v;
                    }
                    if last {
                        self.last_val = v;
                    }
                }
                Num::Dec(mantissa, scale) => {
                    if let Some(track) = self.decimal.as_deref_mut() {
                        if let Some(m) = track.align(mantissa, scale) {
                            if first {
                                track.first = m;
                            }
                            if last {
                                track.last = m;
                            }
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn finalize(&self, func: AggFn) -> AggResult {
        if self.count == 0 {
            return if func == AggFn::Count {
                AggResult::F64(0.0)
            } else {
                AggResult::Null
            };
        }
        // A count is a count whatever the column holds, so it stays an
        // `f64` for every type — it is the one aggregate whose result is
        // not of the column's own kind.
        if func == AggFn::Count {
            return AggResult::F64(self.count as f64);
        }
        if let Some(track) = self.decimal.as_deref() {
            return match func {
                AggFn::Sum => track.sum_result(),
                AggFn::Avg => track.avg_result(self.count),
                AggFn::Min => track.at(track.min),
                AggFn::Max => track.at(track.max),
                AggFn::First => track.at(track.first),
                AggFn::Last => track.at(track.last),
                AggFn::Count => unreachable!("handled above"),
            };
        }
        match func {
            AggFn::Sum => AggResult::F64(self.sum),
            AggFn::Min => AggResult::F64(self.min),
            AggFn::Max => AggResult::F64(self.max),
            AggFn::Avg => AggResult::F64(self.sum / self.count as f64),
            AggFn::First => AggResult::F64(self.first_val),
            AggFn::Last => AggResult::F64(self.last_val),
            AggFn::Count => unreachable!("handled above"),
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
                if let Some(v) = typed.value_num(row) {
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

    for (fi, &field_name) in field_columns.iter().enumerate() {
        for func in functions {
            let func_name = format!("{field_name}_{}", agg_fn_name(*func));
            let vals: Vec<AggResult> = (0..num_groups)
                .map(|gi| groups[gi].1[fi].finalize(*func))
                .collect();
            let (field, array) = agg_result_column(&func_name, &vals)?;
            result_fields.push(field);
            result_columns.push(array);
        }
    }
    debug_assert_eq!(num_fields, field_columns.len());

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
    /// The exact track per field, allocated only for decimal columns.
    decimals: Vec<Option<Box<DecimalTrack>>>,
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
                decimals: (0..num_fields).map(|_| None).collect(),
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
            if let Some(v) = typed.and_then(|tc| tc.value_num(row)) {
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
    for (fi, &field_name) in field_columns.iter().enumerate() {
        for func in functions {
            let name = format!("{field_name}_{}", agg_fn_name(*func));
            let vals: Vec<AggResult> = (0..num_groups)
                .map(|gi| groups[gi].1[fi].finalize(*func))
                .collect();
            let (field, array) = agg_result_column(&name, &vals)?;
            result_fields.push(field);
            result_columns.push(array);
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
                TypedColumn::Decimal(a, scale) => {
                    // No Arrow kernel here: `sum` over `Decimal128` wraps on
                    // overflow, and a wrapped total is a wrong answer rather
                    // than a slow one. One checked pass instead.
                    let track =
                        self.decimals[fi].get_or_insert_with(|| Box::new(DecimalTrack::new(scale)));
                    for i in 0..a.len() {
                        if a.is_null(i) {
                            continue;
                        }
                        match track.align(a.value(i), scale) {
                            Some(m) => {
                                match track.sum.checked_add(m) {
                                    Some(sum) => track.sum = sum,
                                    None => track.overflow = true,
                                }
                                if m < track.min {
                                    track.min = m;
                                }
                                if m > track.max {
                                    track.max = m;
                                }
                            }
                            None => track.overflow = true,
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
                        if let Some(v) = typed.value_num(i) {
                            let first = t < self.first_ts[fi] || !self.first_set[fi];
                            // `>=` so last-encountered wins on ties.
                            let last = t >= self.last_ts[fi] || !self.last_set[fi];
                            if first {
                                self.first_ts[fi] = t;
                                self.first_set[fi] = true;
                            }
                            if last {
                                self.last_ts[fi] = t;
                                self.last_set[fi] = true;
                            }
                            match v {
                                Num::F64(x) => {
                                    if first {
                                        self.first_val[fi] = x;
                                    }
                                    if last {
                                        self.last_val[fi] = x;
                                    }
                                }
                                Num::Dec(mantissa, scale) => {
                                    let track = self.decimals[fi]
                                        .get_or_insert_with(|| Box::new(DecimalTrack::new(scale)));
                                    if let Some(m) = track.align(mantissa, scale) {
                                        if first {
                                            track.first = m;
                                        }
                                        if last {
                                            track.last = m;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// One field's result for one function, exact where the column is.
    #[allow(clippy::cast_precision_loss)]
    fn finalize(&self, fi: usize, func: AggFn) -> AggResult {
        if self.counts[fi] == 0 {
            return if func == AggFn::Count {
                AggResult::F64(0.0)
            } else {
                AggResult::Null
            };
        }
        // A count is a count whatever the column holds.
        if func == AggFn::Count {
            return AggResult::F64(self.counts[fi] as f64);
        }
        if let Some(track) = self.decimals[fi].as_deref() {
            return match func {
                AggFn::Sum => track.sum_result(),
                AggFn::Avg => track.avg_result(self.counts[fi]),
                AggFn::Min => track.at(track.min),
                AggFn::Max => track.at(track.max),
                AggFn::First => {
                    if self.first_set[fi] {
                        track.at(track.first)
                    } else {
                        AggResult::Null
                    }
                }
                AggFn::Last => {
                    if self.last_set[fi] {
                        track.at(track.last)
                    } else {
                        AggResult::Null
                    }
                }
                AggFn::Count => unreachable!("handled above"),
            };
        }
        match func {
            AggFn::Sum => AggResult::F64(self.sums[fi]),
            AggFn::Min => AggResult::F64(self.mins[fi]),
            AggFn::Max => AggResult::F64(self.maxs[fi]),
            AggFn::Avg => AggResult::F64(self.sums[fi] / self.counts[fi] as f64),
            AggFn::First => {
                if self.first_set[fi] {
                    AggResult::F64(self.first_val[fi])
                } else {
                    AggResult::Null
                }
            }
            AggFn::Last => {
                if self.last_set[fi] {
                    AggResult::F64(self.last_val[fi])
                } else {
                    AggResult::Null
                }
            }
            AggFn::Count => unreachable!("handled above"),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn finish(&self, functions: &[AggFn], field_columns: &[&str]) -> Result<RecordBatch> {
        let mut result_fields = Vec::new();
        let mut result_columns: Vec<ArrayRef> = Vec::new();
        for (fi, &field_name) in field_columns.iter().enumerate() {
            for func in functions {
                let name = format!("{field_name}_{}", agg_fn_name(*func));
                let value = self.finalize(fi, *func);
                let (field, array) = agg_result_column(&name, &[value])?;
                result_fields.push(field);
                result_columns.push(array);
            }
        }

        let schema = Arc::new(Schema::new(result_fields));
        RecordBatch::try_new(schema, result_columns).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Exact decimals ─────────────────────────────────────────────

    #[test]
    fn first_and_last_over_a_decimal_column_without_timestamps() {
        // No timestamp column, so First and Last mean array position. The
        // accumulator is fed no timestamps on this path, and asking it for
        // First returned its zero-initialised slot — a `0.00` where the
        // first value was `1.11`.
        let col: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(111_i128), None, Some(333)])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        );
        assert_eq!(
            aggregate_column_ts(col.as_ref(), AggFn::First, None),
            AggResult::Decimal {
                mantissa: 111,
                scale: 2
            }
        );
        assert_eq!(
            aggregate_column_ts(col.as_ref(), AggFn::Last, None),
            AggResult::Decimal {
                mantissa: 333,
                scale: 2
            }
        );
        // And an all-null column has no first value at all.
        let empty: ArrayRef = Arc::new(
            Decimal128Array::from(vec![None::<i128>, None])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        );
        assert_eq!(
            aggregate_column_ts(empty.as_ref(), AggFn::First, None),
            AggResult::Null
        );
    }

    #[test]
    fn a_decimal_sum_that_overflows_38_digits_is_null_not_wrapped() {
        // A wrapped total is a wrong answer that looks like an answer.
        let widest = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
        let col: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(widest), Some(widest)])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        assert_eq!(
            aggregate_column_ts(col.as_ref(), AggFn::Sum, None),
            AggResult::Null
        );
        // Min and max are untouched by the sum's overflow.
        assert_eq!(
            aggregate_column_ts(col.as_ref(), AggFn::Max, None),
            AggResult::Decimal {
                mantissa: widest,
                scale: 0
            }
        );
    }

    #[test]
    fn a_decimal_column_at_two_scales_folds_at_the_wider_one() {
        // The write path normalises, so this only arises for batches that
        // never went through it — but folding them at different scales would
        // add a hundredth to a ten-thousandth.
        let mut acc = IncrementalAccumulator::new();
        acc.update(Num::Dec(150, 2), Some(1)); // 1.50
        acc.update(Num::Dec(15_000, 4), Some(2)); // 1.5000
        assert_eq!(
            acc.finalize(AggFn::Sum),
            AggResult::Decimal {
                mantissa: 30_000,
                scale: 4
            }
        );
        // …and in the other order, which widens the track after the fact.
        let mut acc = IncrementalAccumulator::new();
        acc.update(Num::Dec(15_000, 4), Some(1));
        acc.update(Num::Dec(150, 2), Some(2));
        assert_eq!(
            acc.finalize(AggFn::Sum),
            AggResult::Decimal {
                mantissa: 30_000,
                scale: 4
            }
        );
    }

    #[test]
    fn the_average_of_decimals_rounds_half_away_from_zero() {
        let mut acc = IncrementalAccumulator::new();
        for m in [100_i128, 100, 200] {
            acc.update(Num::Dec(m, 2), Some(1));
        }
        // (1 + 1 + 2) / 3 = 1.333333…, at scale 2 + AVG_EXTRA_SCALE.
        assert_eq!(
            acc.finalize(AggFn::Avg),
            AggResult::Decimal {
                mantissa: 133_333_333,
                scale: 8
            }
        );
        // A negative mean rounds away from zero too, not toward it.
        let mut acc = IncrementalAccumulator::new();
        for m in [-100_i128, -100, -200] {
            acc.update(Num::Dec(m, 2), Some(1));
        }
        assert_eq!(
            acc.finalize(AggFn::Avg),
            AggResult::Decimal {
                mantissa: -133_333_333,
                scale: 8
            }
        );
    }

    #[test]
    fn a_count_of_decimals_is_a_float_column_not_a_decimal_one() {
        let results = [AggResult::F64(3.0)];
        let (field, _) = agg_result_column("v_count", &results).unwrap();
        assert_eq!(field.data_type(), &DataType::Float64);
        // And a decimal result produces a decimal column at its own scale.
        let results = [AggResult::Decimal {
            mantissa: 30,
            scale: 2,
        }];
        let (field, array) = agg_result_column("v_sum", &results).unwrap();
        assert_eq!(field.data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(array.len(), 1);
    }

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
