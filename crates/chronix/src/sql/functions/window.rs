//! Window-function machinery for the order-dependent analytics functions.
//!
//! # Why these are window functions
//!
//! `diff`, `zscore`, `rolling_std`, `stl_trend` and the rest look scalar at a
//! call site and are not: their answer for a row depends on the rows around
//! it, on which rows belong to the same series, and on what order those rows
//! are in. DataFusion offers a scalar UDF none of the three — it hands over
//! whatever `RecordBatch` the plan produced — so as a scalar function the
//! answer varies with batch size, partition count and, here, how many segments
//! the data was flushed into.
//!
//! A window function is given exactly what these need: one partition, in a
//! stated order, as a single array.
//!
//! ```sql
//! SELECT _time, host, diff(usage, 1) OVER (PARTITION BY host ORDER BY _time)
//! FROM cpu
//! ```
//!
//! `PARTITION BY` keeps two hosts' samples out of each other's differences and
//! `ORDER BY` makes "previous row" mean something. Both are the caller's
//! explicit choice, and omitting `OVER` is a planning error rather than a
//! wrong number.
//!
//! # Frames
//!
//! Every kernel consumes the whole partition, so the window frame is ignored:
//! `uses_window_frame()` is `false` and `evaluate_all` receives all the
//! partition's rows at once. The rolling functions take their window length as
//! an argument instead.

use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDF, WindowUDFImpl,
};

/// A window function whose value for every row is computed from the whole
/// partition in one pass.
///
/// Implementors state their SQL name, their argument types and their result
/// type, and compute the partition. Everything else — the `WindowUDF`
/// plumbing, the evaluator, the equality and hashing DataFusion requires — is
/// supplied by [`PartitionWindowUdf`].
pub(super) trait PartitionKernel: std::fmt::Debug + Send + Sync + 'static {
    /// The name the function is called by in SQL.
    const NAME: &'static str;

    /// Exact argument types, in order.
    fn arg_types() -> Vec<DataType>;

    /// Result type. Defaults to `Float64`, which all but one kernel returns.
    fn return_type() -> DataType {
        DataType::Float64
    }

    /// Compute the whole partition.
    ///
    /// Every element of `args` has exactly `num_rows` entries: DataFusion
    /// broadcasts a literal argument to the partition length before calling,
    /// so a constant is read from row 0 with [`constant_usize`] or
    /// [`constant_f64`]. The returned array must also have `num_rows` entries.
    fn evaluate(args: &[ArrayRef], num_rows: usize) -> DFResult<ArrayRef>;
}

/// Generic [`WindowUDF`] over a [`PartitionKernel`].
pub(super) struct PartitionWindowUdf<K: PartitionKernel> {
    signature: Signature,
    kernel: PhantomData<fn() -> K>,
}

impl<K: PartitionKernel> std::fmt::Debug for PartitionWindowUdf<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionWindowUdf")
            .field("name", &K::NAME)
            .finish()
    }
}

// One instance per kernel type, so identity is the type. DataFusion uses these
// to deduplicate equal expressions in a plan.
impl<K: PartitionKernel> PartialEq for PartitionWindowUdf<K> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl<K: PartitionKernel> Eq for PartitionWindowUdf<K> {}
impl<K: PartitionKernel> Hash for PartitionWindowUdf<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        K::NAME.hash(state);
    }
}

impl<K: PartitionKernel> PartitionWindowUdf<K> {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(K::arg_types()),
                // The value depends on the partition, not only on the row, so
                // the planner must not constant-fold or cache across calls.
                Volatility::Volatile,
            ),
            kernel: PhantomData,
        }
    }

    /// The registrable [`WindowUDF`] for this kernel.
    pub(super) fn udf() -> WindowUDF {
        WindowUDF::new_from_impl(Self::new())
    }
}

impl<K: PartitionKernel> WindowUDFImpl for PartitionWindowUdf<K> {
    fn name(&self) -> &str {
        K::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DFResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(PartitionEval::<K>(PhantomData)))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            K::return_type(),
            true,
        )))
    }
}

struct PartitionEval<K: PartitionKernel>(PhantomData<fn() -> K>);

impl<K: PartitionKernel> std::fmt::Debug for PartitionEval<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PartitionEval").field(&K::NAME).finish()
    }
}

impl<K: PartitionKernel> PartitionEvaluator for PartitionEval<K> {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DFResult<ArrayRef> {
        let out = K::evaluate(values, num_rows)?;
        if out.len() != num_rows {
            return Err(DataFusionError::Internal(format!(
                "{} produced {} rows for a partition of {num_rows}",
                K::NAME,
                out.len()
            )));
        }
        Ok(out)
    }
}

// ── Argument extraction ─────────────────────────────────────────────────

/// Read a `Float64` argument as a dense `Vec<f64>`, mapping NULL to `NaN`.
///
/// The kernels are positional — row `i` of the output belongs to row `i` of
/// the input — so a NULL cannot be dropped without shifting everything after
/// it. `NaN` keeps the alignment and propagates through the arithmetic, which
/// is what a missing observation should do.
pub(super) fn column_f64(args: &[ArrayRef], idx: usize) -> DFResult<Vec<f64>> {
    let arr = args.get(idx).ok_or_else(|| {
        DataFusionError::Internal(format!("missing window function argument {idx}"))
    })?;
    if !matches!(arr.data_type(), DataType::Float64) {
        return Err(DataFusionError::Plan(format!(
            "argument {idx} must be Float64, got {}",
            arr.data_type()
        )));
    }
    let f = arr.as_primitive::<Float64Type>();
    Ok((0..f.len())
        .map(|i| if f.is_null(i) { f64::NAN } else { f.value(i) })
        .collect())
}

/// Read a constant `Int64` argument, requiring it to be positive.
pub(super) fn constant_usize(args: &[ArrayRef], idx: usize, name: &str) -> DFResult<usize> {
    let arr = args.get(idx).ok_or_else(|| {
        DataFusionError::Internal(format!("missing window function argument {idx}"))
    })?;
    let i = arr
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| DataFusionError::Plan(format!("{name} must be an integer")))?;
    if i.is_empty() || i.is_null(0) {
        return Err(DataFusionError::Plan(format!("{name} must not be NULL")));
    }
    let raw = i.value(0);
    usize::try_from(raw)
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| DataFusionError::Plan(format!("{name} must be positive, got {raw}")))
}

/// Read a constant `Float64` argument.
pub(super) fn constant_f64(args: &[ArrayRef], idx: usize, name: &str) -> DFResult<f64> {
    let arr = args.get(idx).ok_or_else(|| {
        DataFusionError::Internal(format!("missing window function argument {idx}"))
    })?;
    let f = arr.as_primitive::<Float64Type>();
    if f.is_empty() || f.is_null(0) {
        return Err(DataFusionError::Plan(format!("{name} must not be NULL")));
    }
    Ok(f.value(0))
}

/// Wrap a `Vec<f64>` as an `ArrayRef`, turning `NaN` back into NULL.
///
/// The kernels use `NaN` internally for "no value here" — a leading row a
/// rolling window cannot reach, a gap, a division by zero — and SQL spells
/// that NULL. Returning literal `NaN` instead would make `WHERE x IS NULL`
/// miss every one of them and `avg()` return `NaN` for the whole column.
pub(super) fn f64_array(values: Vec<f64>) -> ArrayRef {
    Arc::new(
        values
            .into_iter()
            .map(|v| v.is_finite().then_some(v))
            .collect::<Float64Array>(),
    )
}

/// Run a kernel over a series with the non-finite entries removed, then place
/// the results back on the rows they came from.
///
/// Several analytics routines (STL, the anomaly detectors) are not defined on
/// `NaN` and would otherwise poison a whole partition from one gap. The
/// contract is that a row whose input was missing gets a missing output, and
/// every other row gets the value computed from the dense series.
pub(super) fn on_dense<F>(values: &[f64], f: F) -> DFResult<Vec<f64>>
where
    F: FnOnce(&[f64]) -> DFResult<Vec<f64>>,
{
    let dense: Vec<f64> = values.iter().copied().filter(|v| v.is_finite()).collect();
    let computed = f(&dense)?;
    let mut out = vec![f64::NAN; values.len()];
    let mut k = 0;
    for (i, v) in values.iter().enumerate() {
        if v.is_finite() {
            if let Some(c) = computed.get(k) {
                out[i] = *c;
            }
            k += 1;
        }
    }
    Ok(out)
}

/// Map an analytics-crate error into a DataFusion execution error.
pub(super) fn exec_err<E: std::fmt::Display>(e: E) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}
