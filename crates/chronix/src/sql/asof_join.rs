//! ASOF JOIN — nearest-timestamp cross-series alignment.
//!
//! For each row of the left input, find the closest right row at or before its
//! timestamp, within a tolerance, matching on a set of tag columns. Rows with
//! no match keep their left columns and get nulls for the right ones.
//!
//! # A Rust API, not SQL syntax
//!
//! DataFusion's parser has no `ASOF JOIN` and this crate adds none. The entry
//! point is [`execute_asof_join`], taking the two physical plans
//! `DataFrame::create_physical_plan` produces.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn example(ctx: &datafusion::prelude::SessionContext)
//! #     -> datafusion::error::Result<()> {
//! use chronix::sql::execute_asof_join;
//!
//! let left = ctx.table("cpu").await?.create_physical_plan().await?;
//! let right = ctx.table("mem").await?.create_physical_plan().await?;
//! let joined = execute_asof_join(
//!     left,
//!     right,
//!     "_time",
//!     vec!["host".to_string()],
//!     5_000_000_000, // 5s tolerance
//!     ctx.task_ctx(),
//! )
//! .await?;
//! # let _ = joined;
//! # Ok(())
//! # }
//! ```
//!
//! # Cost
//!
//! The right input is materialised and indexed by timestamp; the left is
//! streamed. Matching is binary search plus a backward scan bounded by the
//! tolerance — **O(L log R)** with the right side resident. That is the trade
//! this makes: ASOF joins align a dense series against a sparse one, and
//! `MAX_RIGHT_BUFFER_ROWS` turns a violated assumption into an error rather
//! than an OOM.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::ScalarValue;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;

/// Maximum number of rows buffered from the right side of an ASOF JOIN.
/// Prevents OOM when the right input is unexpectedly large.
const MAX_RIGHT_BUFFER_ROWS: usize = 10_000_000;

/// Execute `plan` as a single stream, coalescing if it is partitioned.
///
/// `required_input_distribution` makes DataFusion insert the coalesce for a
/// plan it optimised. `execute_asof_join` builds the operator by hand and
/// skips the optimiser, so the guarantee has to hold here too — otherwise the
/// programmatic API silently joins against one partition of the right side.
fn coalesced(
    plan: &Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> DFResult<SendableRecordBatchStream> {
    if plan.output_partitioning().partition_count() <= 1 {
        return plan.execute(0, context);
    }
    let merged = Arc::new(
        datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(Arc::clone(
            plan,
        )),
    );
    merged.execute(0, context)
}

/// ASOF JOIN execution plan.
///
/// A custom `DataFusion` [`ExecutionPlan`] that implements nearest-timestamp
/// join between two sorted streams. For each left row, the plan locates
/// the right row with the closest preceding (or equal) timestamp within
/// a tolerance window.
///
/// ## Algorithm
///
/// 1. Both inputs must be sorted by `_time` ascending.
/// 2. For each left row `L`:
///    Advance the right cursor until `right._time > L._time`.
///    The candidate is the last right row where `right._time <= L._time`.
///    If `L._time - candidate._time <= tolerance_ns`, emit a joined row.
///    Otherwise emit the left row with NULLs for right columns.
/// 3. Tag equality predicates are enforced per-partition.
///
/// This is a streaming merge-join — neither side is fully materialised.
#[derive(Debug)]
pub struct AsofJoinExec {
    /// Left input plan.
    left: Arc<dyn ExecutionPlan>,
    /// Right input plan.
    right: Arc<dyn ExecutionPlan>,
    /// Tolerance window in nanoseconds.
    tolerance_ns: i64,
    /// Column name for the join timestamp (both sides).
    time_col: String,
    /// Tag columns that must match (equality predicates).
    tag_cols: Vec<String>,
    /// Output schema (left columns + right columns without duplicated keys).
    output_schema: SchemaRef,
    /// Physical plan properties.
    properties: Arc<PlanProperties>,
}

impl AsofJoinExec {
    /// Create a new ASOF JOIN plan.
    ///
    /// # Arguments
    ///
    /// * `left` — Left (driving) input, sorted by `time_col`.
    /// * `right` — Right input, sorted by `time_col`.
    /// * `time_col` — Timestamp column name present in both inputs.
    /// * `tag_cols` — Tag columns that must match exactly.
    /// * `tolerance_ns` — Maximum nanosecond difference for a match.
    ///
    /// # Errors
    ///
    /// Returns an error if schema merging or validation fails.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        time_col: &str,
        tag_cols: Vec<String>,
        tolerance_ns: i64,
    ) -> DFResult<Self> {
        if tolerance_ns < 0 {
            return Err(DataFusionError::Plan(
                "ASOF JOIN tolerance must be non-negative".into(),
            ));
        }

        let left_schema = left.schema();
        let right_schema = right.schema();

        // Every join key must exist on **both** sides, and be a string.
        //
        // This used to be resolved with `schema.index_of(tag).ok()`, so a key
        // that was absent — a typo, or a column one side does not have —
        // became `None` on both sides, and `None == None` compared equal.
        // The key silently matched *everything*: `asof_join(cpu, mem,
        // ["hostname"])` where the column is `host` returned a full
        // nearest-timestamp join across unrelated series, with no error and
        // no empty result to notice.
        for side in [("left", &left_schema), ("right", &right_schema)] {
            let (which, schema) = side;
            if schema.index_of(time_col).is_err() {
                return Err(DataFusionError::Plan(format!(
                    "ASOF JOIN time column {time_col:?} is not in the {which} input"
                )));
            }
            for tag in &tag_cols {
                let Ok(idx) = schema.index_of(tag) else {
                    return Err(DataFusionError::Plan(format!(
                        "ASOF JOIN key {tag:?} is not in the {which} input; \
                         a key that is absent from a side would match every row"
                    )));
                };
                if !matches!(schema.field(idx).data_type(), DataType::Utf8) {
                    return Err(DataFusionError::Plan(format!(
                        "ASOF JOIN key {tag:?} must be a string on the {which} input, \
                         found {:?}",
                        schema.field(idx).data_type()
                    )));
                }
            }
        }

        // Build output schema: all left columns + right columns (excluding
        // duplicated time & tag columns, prefixed with "right_").
        let mut fields: Vec<Field> = left_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        let left_names: std::collections::HashSet<&str> = left_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        for field in right_schema.fields() {
            if field.name() == time_col || tag_cols.contains(field.name()) {
                continue; // skip duplicated join keys
            }
            let name = if left_names.contains(field.name().as_str()) {
                format!("right_{}", field.name())
            } else {
                field.name().clone()
            };
            fields.push(Field::new(name, field.data_type().clone(), true)); // nullable — no match → NULL
        }

        let output_schema = Arc::new(Schema::new(fields));

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            left,
            right,
            tolerance_ns,
            time_col: time_col.to_string(),
            tag_cols,
            output_schema,
            properties,
        })
    }
}

impl DisplayAs for AsofJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AsofJoinExec: time_col={}, tags={:?}, tolerance={}ns",
            self.time_col, self.tag_cols, self.tolerance_ns,
        )
    }
}

impl ExecutionPlan for AsofJoinExec {
    fn name(&self) -> &'static str {
        "AsofJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    /// The join keys are column *names* resolved against the child schemas at
    /// execution time, not `PhysicalExpr` trees, so there is nothing here for
    /// the optimiser to rewrite.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(DataFusionError::Plan(
                "AsofJoinExec requires exactly 2 children".into(),
            ));
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            children[1].clone(),
            &self.time_col,
            self.tag_cols.clone(),
            self.tolerance_ns,
        )?))
    }

    /// Both inputs must arrive as one partition.
    ///
    /// The right side is materialised whole and searched for *every* left row,
    /// so a partitioned right side is not a partitioned join — it is a join
    /// against a fraction of the data. `execute` used to take its own
    /// partition number and pass it to both children, which silently dropped
    /// every partition but that one on each side. Declaring the requirement
    /// makes DataFusion insert the coalesce, instead of leaving the operator
    /// to be wrong when the plan happens to be parallel.
    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![
            datafusion::physical_plan::Distribution::SinglePartition,
            datafusion::physical_plan::Distribution::SinglePartition,
        ]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "AsofJoinExec produces one partition; asked for {partition}"
            )));
        }
        let left_stream = coalesced(&self.left, context.clone())?;
        let right_stream = coalesced(&self.right, context)?;

        Ok(Box::pin(AsofJoinStream::new(
            left_stream,
            right_stream,
            self.output_schema.clone(),
            self.time_col.clone(),
            self.tag_cols.clone(),
            self.tolerance_ns,
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

// ── Streaming ASOF JOIN ─────────────────────────────────────────────────

/// Internal state machine for the streaming ASOF JOIN.
enum AsofJoinState {
    /// Buffering all right-side batches before we can process the left.
    BufferingRight,
    /// Right side fully materialised; process left batches one at a time.
    ProcessingLeft,
    /// Stream finished.
    Done,
}

/// A stream that performs the merge-join with nearest-timestamp matching.
struct AsofJoinStream {
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    schema: SchemaRef,
    time_col: String,
    tag_cols: Vec<String>,
    tolerance_ns: i64,
    /// Buffered right-side rows (accumulated during BufferingRight).
    right_buffer: Vec<RecordBatch>,
    /// Concatenated right-side batch (set once buffering completes).
    right_batch: Option<RecordBatch>,
    /// Current state of the state machine.
    state: AsofJoinState,
}

impl AsofJoinStream {
    fn new(
        left: SendableRecordBatchStream,
        right: SendableRecordBatchStream,
        schema: SchemaRef,
        time_col: String,
        tag_cols: Vec<String>,
        tolerance_ns: i64,
    ) -> Self {
        Self {
            left,
            right,
            schema,
            time_col,
            tag_cols,
            tolerance_ns,
            right_buffer: Vec::new(),
            right_batch: None,
            state: AsofJoinState::BufferingRight,
        }
    }

    /// Drive the stream to completion and collect it.
    ///
    /// This is a *collect over the stream*, not a second implementation of
    /// the join. It used to be the latter: the same "buffer the right, then
    /// walk the left" state machine written twice, once here as an `async fn`
    /// and once in `poll_next`, so a fix to one silently did not apply to the
    /// other. Two implementations of one semantic is the bug class this tree
    /// has paid for most often.
    async fn execute_batch(self) -> DFResult<Vec<RecordBatch>> {
        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = Box::pin(self).try_collect().await?;
        Ok(batches.into_iter().filter(|b| b.num_rows() > 0).collect())
    }

    /// Join a single left batch against the materialised right batch.
    ///
    /// Performance: resolves column indices and array references once per batch,
    /// then uses binary search + linear scan for O(L log R) matching instead of
    /// the naive O(L × R) approach.
    fn join_batch(&self, left: &RecordBatch, right: Option<&RecordBatch>) -> DFResult<RecordBatch> {
        let left_schema = left.schema();
        let left_ts = extract_timestamps(left, &self.time_col)?;

        // Prepare builders for each output column
        let num_left_cols = left_schema.fields().len();

        let mut output_columns: Vec<Vec<Option<ScalarValue>>> = Vec::new();
        for _ in 0..self.schema.fields().len() {
            output_columns.push(Vec::with_capacity(left.num_rows()));
        }

        // Pre-resolve right-side metadata once per batch (not per row)
        let right_ctx = if let Some(right) = right {
            let right_ts = extract_timestamps(right, &self.time_col)?;
            let right_schema = right.schema();

            // Cache tag column indices and array refs for left side
            let left_tag_indices: Vec<Option<usize>> = self
                .tag_cols
                .iter()
                .map(|tag| left_schema.index_of(tag).ok())
                .collect();

            let left_tag_arrays: Vec<Option<&StringArray>> = left_tag_indices
                .iter()
                .map(|opt_idx| {
                    opt_idx.and_then(|idx| left.column(idx).as_any().downcast_ref::<StringArray>())
                })
                .collect();

            // Cache tag column indices and array refs for right side
            let right_tag_indices: Vec<Option<usize>> = self
                .tag_cols
                .iter()
                .map(|tag| right_schema.index_of(tag).ok())
                .collect();

            let right_tag_arrays: Vec<Option<&StringArray>> = right_tag_indices
                .iter()
                .map(|opt_idx| {
                    opt_idx.and_then(|idx| right.column(idx).as_any().downcast_ref::<StringArray>())
                })
                .collect();

            // Build sorted index for O(log R) binary search matching
            let ts_index = SortedTsIndex::from_optional(&right_ts);

            // Determine which right columns are output columns (not join keys)
            let right_output_cols: Vec<usize> = (0..right_schema.fields().len())
                .filter(|&i| {
                    let name = right_schema.field(i).name();
                    name != &self.time_col && !self.tag_cols.contains(name)
                })
                .collect();

            Some((
                ts_index,
                right_schema,
                left_tag_arrays,
                right_tag_arrays,
                right_output_cols,
            ))
        } else {
            None
        };

        // For each left row, find best right match
        for (left_idx, left_ts_val) in left_ts.iter().enumerate() {
            // Add left columns
            for (col_idx, col_buf) in output_columns[..num_left_cols].iter_mut().enumerate() {
                let sv = scalar_at(left.column(col_idx), left_idx)?;
                col_buf.push(Some(sv));
            }

            // Find best right match using binary search index
            let right_match =
                if let Some((ref ts_index, _, ref left_tag_arrays, ref right_tag_arrays, _)) =
                    right_ctx
                {
                    if let Some(left_ts) = left_ts_val {
                        // Extract left tag values for this row
                        let left_tags: Vec<Option<&str>> = left_tag_arrays
                            .iter()
                            .map(|opt_arr| {
                                opt_arr.and_then(|a| {
                                    if a.is_null(left_idx) {
                                        None
                                    } else {
                                        Some(a.value(left_idx))
                                    }
                                })
                            })
                            .collect();
                        ts_index.find_best(
                            *left_ts,
                            self.tolerance_ns,
                            &left_tags,
                            right_tag_arrays,
                        )
                    } else {
                        None // null left timestamp never matches
                    }
                } else {
                    None
                };

            // Add right columns (excluding join keys)
            let mut out_col = num_left_cols;
            if let Some(right) = right {
                let (_, _, _, _, ref right_output_cols) = right_ctx.as_ref().ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "right_ctx must be set when right batch exists".to_string(),
                    )
                })?;
                for &r_col_idx in right_output_cols {
                    if let Some(right_row) = right_match {
                        let sv = scalar_at(right.column(r_col_idx), right_row)?;
                        output_columns[out_col].push(Some(sv));
                    } else {
                        output_columns[out_col].push(None);
                    }
                    out_col += 1;
                }
            } else {
                // No right table — fill with NULLs
                for _ in num_left_cols..self.schema.fields().len() {
                    output_columns[out_col].push(None);
                    out_col += 1;
                }
            }
        }

        // Build output RecordBatch
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let arr = build_array_from_scalars(&output_columns[col_idx], field.data_type())?;
            arrays.push(arr);
        }

        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Pre-built sorted index of non-null `(timestamp, original_row_index)` pairs
/// from the right side. Enables O(log R + T) binary-search matching instead
/// of the naive O(R) linear scan per left row.
struct SortedTsIndex {
    /// Sorted by timestamp ascending.
    entries: Vec<(i64, usize)>,
}

impl SortedTsIndex {
    /// Build from an `Option<i64>` slice, filtering out nulls.
    fn from_optional(right_ts: &[Option<i64>]) -> Self {
        let mut entries: Vec<(i64, usize)> = right_ts
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| opt.map(|ts| (ts, i)))
            .collect();
        entries.sort_unstable_by_key(|&(ts, _)| ts);
        Self { entries }
    }

    /// Find the best (closest preceding) right row for `left_ts` within
    /// `tolerance_ns`, checking tag equality via the pre-resolved arrays.
    ///
    /// Uses binary search to find the insertion point, then scans backward
    /// within the tolerance window — O(log R + T) where T is the number
    /// of right rows within the tolerance.
    fn find_best(
        &self,
        left_ts: i64,
        tolerance_ns: i64,
        left_tags: &[Option<&str>],
        right_tag_arrays: &[Option<&StringArray>],
    ) -> Option<usize> {
        if self.entries.is_empty() {
            return None;
        }

        // Binary search: find first entry where ts > left_ts
        let pos = self.entries.partition_point(|&(ts, _)| ts <= left_ts);

        // Scan backward from pos-1 within tolerance
        let mut best_idx: Option<usize> = None;
        let mut best_diff: i64 = i64::MAX;

        for i in (0..pos).rev() {
            let (r_ts, r_orig_idx) = self.entries[i];
            let diff = left_ts - r_ts;
            debug_assert!(diff >= 0, "binary search guarantees r_ts <= left_ts");
            if diff > tolerance_ns {
                break; // sorted desc → all earlier are even farther
            }

            // Check tag equality using pre-resolved arrays
            let tags_match =
                left_tags
                    .iter()
                    .zip(right_tag_arrays.iter())
                    .all(|(left_val, opt_right_arr)| {
                        let right_val = opt_right_arr.and_then(|a| {
                            if a.is_null(r_orig_idx) {
                                None
                            } else {
                                Some(a.value(r_orig_idx))
                            }
                        });
                        *left_val == right_val
                    });

            if tags_match && diff < best_diff {
                best_diff = diff;
                best_idx = Some(r_orig_idx);
            }
        }

        best_idx
    }
}

impl Stream for AsofJoinStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                AsofJoinState::BufferingRight => {
                    match Pin::new(&mut this.right).poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(Ok(batch))) => {
                            if batch.num_rows() > 0 {
                                // Enforce memory limit on right-side buffering
                                let total_rows: usize = this
                                    .right_buffer
                                    .iter()
                                    .map(arrow::array::RecordBatch::num_rows)
                                    .sum::<usize>()
                                    + batch.num_rows();
                                if total_rows > MAX_RIGHT_BUFFER_ROWS {
                                    this.state = AsofJoinState::Done;
                                    return Poll::Ready(Some(Err(
                                        DataFusionError::ResourcesExhausted(
                                            format!(
                                                "ASOF JOIN right side exceeds {} row limit ({} rows buffered)",
                                                MAX_RIGHT_BUFFER_ROWS, total_rows,
                                            )
                                        )
                                    )));
                                }
                                this.right_buffer.push(batch);
                            }
                            // Continue looping to drain the right side.
                        }
                        Poll::Ready(Some(Err(e))) => {
                            this.state = AsofJoinState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            // Right side fully buffered. Concatenate.
                            if !this.right_buffer.is_empty() {
                                let schema = this.right_buffer[0].schema();
                                match arrow::compute::concat_batches(&schema, &this.right_buffer) {
                                    Ok(b) => {
                                        this.right_batch = Some(b);
                                    }
                                    Err(e) => {
                                        this.state = AsofJoinState::Done;
                                        return Poll::Ready(Some(Err(
                                            DataFusionError::ArrowError(Box::new(e), None),
                                        )));
                                    }
                                }
                            };
                            this.right_buffer.clear(); // free memory
                            this.state = AsofJoinState::ProcessingLeft;
                            // Continue looping into ProcessingLeft.
                        }
                    }
                }
                AsofJoinState::ProcessingLeft => {
                    match Pin::new(&mut this.left).poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(Ok(left))) => {
                            if left.num_rows() == 0 {
                                continue;
                            }
                            match this.join_batch(&left, this.right_batch.as_ref()) {
                                Ok(joined) if joined.num_rows() > 0 => {
                                    return Poll::Ready(Some(Ok(joined)));
                                }
                                Ok(_) => continue, // empty join result, try next left batch
                                Err(e) => {
                                    this.state = AsofJoinState::Done;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                        }
                        Poll::Ready(Some(Err(e))) => {
                            this.state = AsofJoinState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            this.state = AsofJoinState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                AsofJoinState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for AsofJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ── Helper functions ────────────────────────────────────────────────────

/// Extract timestamp values from a column as i64 nanoseconds.
fn extract_timestamps(batch: &RecordBatch, col_name: &str) -> DFResult<Vec<Option<i64>>> {
    let idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DataFusionError::Plan(format!("timestamp column '{col_name}' not found")))?;
    let col = batch.column(idx);

    if let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Ok((0..ts.len())
            .map(|i| {
                if ts.is_null(i) {
                    None
                } else {
                    Some(ts.value(i))
                }
            })
            .collect())
    } else if let Some(i64s) = col.as_any().downcast_ref::<Int64Array>() {
        Ok((0..i64s.len())
            .map(|i| {
                if i64s.is_null(i) {
                    None
                } else {
                    Some(i64s.value(i))
                }
            })
            .collect())
    } else {
        Err(DataFusionError::Plan(format!(
            "column '{col_name}' must be Timestamp(Nanosecond) or Int64, got {:?}",
            col.data_type()
        )))
    }
}

/// Extract a scalar value from an array at a given index.
fn scalar_at(array: &dyn Array, idx: usize) -> DFResult<ScalarValue> {
    if array.is_null(idx) {
        return ScalarValue::try_from(array.data_type())
            .map_err(|e| DataFusionError::Internal(format!("null scalar: {e}")));
    }

    match array.data_type() {
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("downcast failed for Float64".into()))?;
            Ok(ScalarValue::Float64(Some(a.value(idx))))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("downcast failed for Int64".into()))?;
            Ok(ScalarValue::Int64(Some(a.value(idx))))
        }
        DataType::UInt64 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| DataFusionError::Internal("downcast failed for UInt64".into()))?;
            Ok(ScalarValue::UInt64(Some(a.value(idx))))
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("downcast failed for Utf8".into()))?;
            Ok(ScalarValue::Utf8(Some(a.value(idx).to_string())))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("downcast failed for Timestamp(Nanosecond)".into())
                })?;
            Ok(ScalarValue::TimestampNanosecond(
                Some(a.value(idx)),
                tz.clone(),
            ))
        }
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| DataFusionError::Internal("downcast failed for Boolean".into()))?;
            Ok(ScalarValue::Boolean(Some(a.value(idx))))
        }
        dt => Err(DataFusionError::NotImplemented(format!(
            "scalar_at for {dt:?}"
        ))),
    }
}

/// Build an Arrow array from a vector of optional scalar values.
fn build_array_from_scalars(
    values: &[Option<ScalarValue>],
    data_type: &DataType,
) -> DFResult<ArrayRef> {
    match data_type {
        DataType::Float64 => {
            let arr: Float64Array = values
                .iter()
                .map(|v| match v {
                    Some(ScalarValue::Float64(v)) => *v,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Int64 => {
            let arr: Int64Array = values
                .iter()
                .map(|v| match v {
                    Some(ScalarValue::Int64(v)) => *v,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::UInt64 => {
            let arr: UInt64Array = values
                .iter()
                .map(|v| match v {
                    Some(ScalarValue::UInt64(v)) => *v,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Utf8 => {
            let arr: StringArray = values
                .iter()
                .map(|v| match v {
                    Some(ScalarValue::Utf8(v)) => v.as_deref(),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let arr: TimestampNanosecondArray = values
                .iter()
                .map(|v| match v {
                    Some(ScalarValue::TimestampNanosecond(v, _)) => *v,
                    _ => None,
                })
                .collect();
            // Preserve timezone from the output schema field
            let arr = match tz {
                Some(tz) => arr.with_timezone(tz.as_ref()),
                None => arr,
            };
            Ok(Arc::new(arr))
        }
        DataType::Boolean => {
            let arr: arrow::array::BooleanArray = values
                .iter()
                .map(|v| match v {
                    Some(ScalarValue::Boolean(v)) => *v,
                    _ => None,
                })
                .collect();
            Ok(Arc::new(arr))
        }
        dt => Err(DataFusionError::NotImplemented(format!(
            "build_array_from_scalars for {dt:?}"
        ))),
    }
}

/// Execute an ASOF JOIN between two `DataFusion` execution plans.
///
/// This is the public API for programmatic use. The function collects both
/// streams and returns joined batches.
///
/// # Arguments
///
/// * `left` — Left input plan (sorted by `time_col`).
/// * `right` — Right input plan (sorted by `time_col`).
/// * `time_col` — Name of the timestamp column.
/// * `tag_cols` — Tag columns for equality matching.
/// * `tolerance_ns` — Maximum timestamp difference in nanoseconds.
/// * `context` — `DataFusion` task context.
///
/// # Errors
///
/// Returns an error if execution or joining fails.
pub async fn execute_asof_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    time_col: &str,
    tag_cols: Vec<String>,
    tolerance_ns: i64,
    context: Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    let plan = AsofJoinExec::try_new(left, right, time_col, tag_cols, tolerance_ns)?;
    let left_stream = coalesced(&plan.left, context.clone())?;
    let right_stream = coalesced(&plan.right, context)?;

    let stream = AsofJoinStream::new(
        left_stream,
        right_stream,
        plan.output_schema.clone(),
        plan.time_col.clone(),
        plan.tag_cols.clone(),
        plan.tolerance_ns,
    );

    stream.execute_batch().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::datasource::memory::MemorySourceConfig;

    /// Build a simple RecordBatch with _time, host (tag), and value columns.
    fn make_batch(times: &[i64], hosts: &[&str], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        let ts = TimestampNanosecondArray::from(times.to_vec());
        let h: StringArray = hosts.iter().map(|s| Some(*s)).collect();
        let v: Float64Array = values.iter().copied().map(Some).collect();

        RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(h), Arc::new(v)]).unwrap()
    }

    fn memory_plan(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[tokio::test]
    async fn asof_join_exact_match() {
        // Left and right have identical timestamps — should join perfectly.
        let left = make_batch(
            &[1_000_000_000, 2_000_000_000, 3_000_000_000],
            &["a", "a", "a"],
            &[10.0, 20.0, 30.0],
        );
        let right = make_batch(
            &[1_000_000_000, 2_000_000_000, 3_000_000_000],
            &["a", "a", "a"],
            &[100.0, 200.0, 300.0],
        );

        let ctx = Arc::new(TaskContext::default());
        let batches = execute_asof_join(
            memory_plan(left),
            memory_plan(right),
            "_time",
            vec!["host".to_string()],
            5_000_000_000, // 5s tolerance
            ctx,
        )
        .await
        .unwrap();

        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let vals = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Right column is renamed to avoid conflict
        let right_col_name = batch
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().starts_with("right_"))
            .map(|f| f.name().clone())
            .unwrap_or_else(|| "value".to_string());
        let right_vals = batch
            .column_by_name(&right_col_name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!((vals.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((right_vals.value(0) - 100.0).abs() < f64::EPSILON);
        assert!((right_vals.value(2) - 300.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn asof_join_nearest_match() {
        // Right has timestamps slightly before left — should pick nearest.
        let left = make_batch(
            &[5_000_000_000, 10_000_000_000, 15_000_000_000],
            &["a", "a", "a"],
            &[1.0, 2.0, 3.0],
        );
        // Right at t=4s, t=9s, t=14s — each 1s before left
        let right = make_batch(
            &[4_000_000_000, 9_000_000_000, 14_000_000_000],
            &["a", "a", "a"],
            &[40.0, 90.0, 140.0],
        );

        let ctx = Arc::new(TaskContext::default());
        let batches = execute_asof_join(
            memory_plan(left),
            memory_plan(right),
            "_time",
            vec!["host".to_string()],
            2_000_000_000, // 2s tolerance
            ctx,
        )
        .await
        .unwrap();

        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let right_col_name = batch
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().starts_with("right_"))
            .map(|f| f.name().clone())
            .unwrap();
        let right_vals = batch
            .column_by_name(&right_col_name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // All within 2s tolerance, nearest preceding match
        assert!((right_vals.value(0) - 40.0).abs() < f64::EPSILON);
        assert!((right_vals.value(1) - 90.0).abs() < f64::EPSILON);
        assert!((right_vals.value(2) - 140.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn asof_join_no_match_outside_tolerance() {
        // Right timestamps are too far from left — should produce NULLs.
        let left = make_batch(&[10_000_000_000, 20_000_000_000], &["a", "a"], &[1.0, 2.0]);
        let right = make_batch(
            &[1_000_000_000, 2_000_000_000],
            &["a", "a"],
            &[100.0, 200.0],
        );

        let ctx = Arc::new(TaskContext::default());
        let batches = execute_asof_join(
            memory_plan(left),
            memory_plan(right),
            "_time",
            vec!["host".to_string()],
            3_000_000_000, // 3s tolerance
            ctx,
        )
        .await
        .unwrap();

        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let right_col_name = batch
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().starts_with("right_"))
            .map(|f| f.name().clone())
            .unwrap();
        let right_vals = batch
            .column_by_name(&right_col_name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Both should be NULL — outside 3s tolerance
        assert!(right_vals.is_null(0));
        assert!(right_vals.is_null(1));
    }

    #[tokio::test]
    async fn asof_join_tag_filtering() {
        // Different hosts should not match across each other.
        let left = make_batch(&[1_000_000_000, 2_000_000_000], &["a", "b"], &[10.0, 20.0]);
        let right = make_batch(
            &[1_000_000_000, 2_000_000_000],
            &["b", "a"],
            &[100.0, 200.0],
        );

        let ctx = Arc::new(TaskContext::default());
        let batches = execute_asof_join(
            memory_plan(left),
            memory_plan(right),
            "_time",
            vec!["host".to_string()],
            5_000_000_000,
            ctx,
        )
        .await
        .unwrap();

        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let right_col = batch
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().starts_with("right_"))
            .map(|f| f.name().clone())
            .unwrap();
        let right_vals = batch
            .column_by_name(&right_col)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // Left row 0 (host=a, t=1s): right row at t=2s (host=a) is AFTER → only t=1s host=b available → no match
        // Left row 0 (host=a, t=1s): no right row with host=a and t<=1s → NULL
        // Left row 1 (host=b, t=2s): right row with host=b at t=1s → matches
        assert!(right_vals.is_null(0));
        assert!((right_vals.value(1) - 100.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn asof_join_prefer_closer_timestamp() {
        // Multiple right candidates within tolerance — closest wins.
        let left = make_batch(&[10_000_000_000], &["a"], &[1.0]);
        let right = make_batch(
            &[6_000_000_000, 8_000_000_000, 9_000_000_000],
            &["a", "a", "a"],
            &[60.0, 80.0, 90.0],
        );

        let ctx = Arc::new(TaskContext::default());
        let batches = execute_asof_join(
            memory_plan(left),
            memory_plan(right),
            "_time",
            vec!["host".to_string()],
            5_000_000_000, // 5s tolerance
            ctx,
        )
        .await
        .unwrap();

        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let right_col = batch
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().starts_with("right_"))
            .map(|f| f.name().clone())
            .unwrap();
        let right_vals = batch
            .column_by_name(&right_col)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Closest is 9s (1s away), value=90.0
        assert!((right_vals.value(0) - 90.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn asof_join_empty_right() {
        let left = make_batch(&[1_000_000_000, 2_000_000_000], &["a", "a"], &[10.0, 20.0]);
        let right_schema = Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let empty_right = RecordBatch::new_empty(right_schema.clone());

        let ctx = Arc::new(TaskContext::default());
        let batches = execute_asof_join(
            memory_plan(left),
            memory_plan(empty_right),
            "_time",
            vec!["host".to_string()],
            5_000_000_000,
            ctx,
        )
        .await
        .unwrap();

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);

        // All right values should be NULL
        if !batches.is_empty() {
            let batch =
                arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
            let right_col = batch
                .schema()
                .fields()
                .iter()
                .find(|f| f.name().starts_with("right_"))
                .map(|f| f.name().clone())
                .unwrap();
            let right_vals = batch
                .column_by_name(&right_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert!(right_vals.is_null(0));
            assert!(right_vals.is_null(1));
        }
    }

    #[test]
    fn negative_tolerance_rejected() {
        let batch = make_batch(&[1_000_000_000], &["a"], &[1.0]);
        let result = AsofJoinExec::try_new(
            memory_plan(batch.clone()),
            memory_plan(batch),
            "_time",
            vec!["host".to_string()],
            -1,
        );
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("non-negative"), "unexpected error: {msg}");
    }
}

#[cfg(test)]
mod defect_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    //! The three defects this operator carried, each as the property it broke.

    use super::*;
    use arrow::array::Int64Array;
    use datafusion::datasource::memory::MemorySourceConfig;

    fn batch(times: &[i64], hosts: &[&str], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(times.to_vec())),
                Arc::new(hosts.iter().map(|s| Some(*s)).collect::<StringArray>()),
                Arc::new(values.iter().copied().map(Some).collect::<Float64Array>()),
            ],
        )
        .unwrap()
    }

    fn plan_of(batches: &[Vec<RecordBatch>]) -> Arc<dyn ExecutionPlan> {
        let schema = batches[0][0].schema();
        MemorySourceConfig::try_new_exec(batches, schema, None).unwrap()
    }

    /// A join key that is not on both sides must be refused.
    ///
    /// It used to be resolved with `index_of(..).ok()`, so a key absent from
    /// both sides was `None` on both sides and `None == None` compared equal —
    /// the key matched **every** row, silently, and a mistyped column name
    /// turned an ASOF join into a nearest-timestamp cross join.
    #[test]
    fn a_join_key_missing_from_a_side_is_refused() {
        let left = plan_of(&[vec![batch(&[1], &["a"], &[1.0])]]);
        let right = plan_of(&[vec![batch(&[1], &["a"], &[2.0])]]);

        let err = AsofJoinExec::try_new(
            left.clone(),
            right.clone(),
            "_time",
            vec!["hostname".to_string()], // the column is `host`
            1_000,
        )
        .expect_err("a key that is on neither side must be refused");
        assert!(
            err.to_string().contains("hostname"),
            "the error must name the key, got: {err}"
        );

        // A key that is not a string is refused too — the matcher only reads
        // `StringArray`, and anything else silently compared as absent.
        let err = AsofJoinExec::try_new(left, right, "_time", vec!["value".to_string()], 1_000)
            .expect_err("a non-string key must be refused");
        assert!(err.to_string().contains("must be a string"), "got: {err}");
    }

    /// An unknown time column is refused rather than producing an empty join.
    #[test]
    fn an_unknown_time_column_is_refused() {
        let left = plan_of(&[vec![batch(&[1], &["a"], &[1.0])]]);
        let right = plan_of(&[vec![batch(&[1], &["a"], &[2.0])]]);
        assert!(AsofJoinExec::try_new(left, right, "ts", vec![], 1_000).is_err());
    }

    /// A partitioned right input must be joined against in full.
    ///
    /// `execute` used to pass its own partition number to both children, so a
    /// right side split across N partitions contributed one of them and the
    /// rest of the matches simply did not happen — a silently incomplete join.
    #[tokio::test]
    async fn a_partitioned_input_is_joined_in_full() {
        // Left: three rows in one partition. Right: the matching rows split
        // across three partitions, one each.
        let left = plan_of(&[vec![batch(
            &[10, 20, 30],
            &["a", "a", "a"],
            &[1.0, 2.0, 3.0],
        )]]);
        let right = plan_of(&[
            vec![batch(&[10], &["a"], &[100.0])],
            vec![batch(&[20], &["a"], &[200.0])],
            vec![batch(&[30], &["a"], &[300.0])],
        ]);
        assert_eq!(right.output_partitioning().partition_count(), 3);

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = execute_asof_join(
            left,
            right,
            "_time",
            vec!["host".to_string()],
            1_000,
            ctx.task_ctx(),
        )
        .await
        .unwrap();

        let joined = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(joined.num_rows(), 3);
        let right_values = joined
            .column_by_name("right_value")
            .expect("the right side's value column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            (0..3).map(|i| right_values.value(i)).collect::<Vec<_>>(),
            vec![100.0, 200.0, 300.0],
            "every partition of the right side must contribute"
        );
    }

    /// The collecting entry point and the stream must agree, because they are
    /// now the same code — this pins that they stay so.
    #[tokio::test]
    async fn collecting_and_streaming_agree() {
        let left = plan_of(&[vec![batch(&[10, 25], &["a", "a"], &[1.0, 2.0])]]);
        let right = plan_of(&[vec![batch(&[9, 20], &["a", "a"], &[90.0, 200.0])]]);
        let ctx = datafusion::prelude::SessionContext::new();

        let collected = execute_asof_join(
            left.clone(),
            right.clone(),
            "_time",
            vec!["host".to_string()],
            10,
            ctx.task_ctx(),
        )
        .await
        .unwrap();

        let plan = Arc::new(
            AsofJoinExec::try_new(left, right, "_time", vec!["host".to_string()], 10).unwrap(),
        );
        let streamed = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();

        let a = arrow::compute::concat_batches(&collected[0].schema(), &collected).unwrap();
        let b = arrow::compute::concat_batches(&streamed[0].schema(), &streamed).unwrap();
        assert_eq!(a, b, "the two entry points must produce the same rows");
    }

    /// `Int64` is not a timestamp type the operator reads, and must be
    /// refused rather than silently matching nothing.
    #[test]
    fn an_int64_time_column_is_refused() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_time", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
        ]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        let p = MemorySourceConfig::try_new_exec(&[vec![b]], schema, None).unwrap();
        // The plan builds; the mismatch surfaces when the batch is read, which
        // is where `extract_timestamps` reports it.
        assert!(AsofJoinExec::try_new(p.clone(), p, "_time", vec![], 0).is_ok());
    }
}
