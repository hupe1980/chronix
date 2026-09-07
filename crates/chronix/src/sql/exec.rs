//! Custom `DataFusion` `ExecutionPlan` backed by the Chronix query engine.
//!
//! `ChronixExec` is a leaf node in the `DataFusion` physical plan tree.
//! When executed, it builds and runs a Chronix `QueryPlan::Scan` in a
//! blocking thread pool and streams the results back.
//!
//! Implements `statistics()` to expose segment-level column statistics
//! (min/max, null counts, distinct counts) to DataFusion's cost-based
//! query optimizer for join ordering, filter selectivity estimation,
//! and memory allocation planning.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::statistics::StatisticsArgs;
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::Stream;

use chronix_engine::segment::metadata::data_types;
use chronix_engine::segment::stats::ordered_i64_to_f64;

use crate::sql::batch::{align_batch_to_schema, convert_timestamp_column};
use crate::Chronix;

/// A `DataFusion` `ExecutionPlan` that scans a Chronix measurement.
///
/// This is a leaf node — it has no children. When `execute()` is called,
/// it builds a Chronix query plan from the stored predicates and runs
/// it on the Tokio blocking thread pool.
#[derive(Debug)]
pub struct ChronixExec {
    db: Arc<Chronix>,
    measurement: String,
    /// Full table schema (before projection). Retained for statistics
    /// and potential schema introspection at execution time.
    _schema: SchemaRef,
    projected_schema: SchemaRef,
    time_start: i64,
    time_end: i64,
    tag_filters: Vec<(String, String)>,
    field_predicates: Vec<chronix_engine::segment::FieldPredicate>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
    /// Execution metrics, so `EXPLAIN ANALYZE` says what the scan did.
    ///
    /// Without these the scan was a black box in an analysed plan: DataFusion
    /// printed timings for every operator above it and nothing for the one
    /// doing the I/O. `rows_scanned` is the count the storage engine handed
    /// up, *before* the limit and the projection, which is the number that
    /// says whether pruning and limit pushdown actually did anything.
    metrics: ExecutionPlanMetricsSet,
}

impl ChronixExec {
    /// Create a new scan execution plan.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Arc<Chronix>,
        measurement: String,
        schema: SchemaRef,
        projection: Option<&[usize]>,
        time_start: i64,
        time_end: i64,
        tag_filters: Vec<(String, String)>,
        field_predicates: Vec<chronix_engine::segment::FieldPredicate>,
        limit: Option<usize>,
    ) -> Result<Self, DataFusionError> {
        let projected_schema = match projection {
            Some(proj) => Arc::new(schema.project(proj).map_err(|e| {
                tracing::error!("invalid projection indices from SQL planner: {e}");
                e
            })?),
            None => schema.clone(),
        };

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            db,
            measurement,
            _schema: schema,
            projected_schema,
            time_start,
            time_end,
            tag_filters,
            field_predicates,
            limit,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// A chronix error crossing into DataFusion.
fn to_df_error(e: crate::error::DbError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

impl DisplayAs for ChronixExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ChronixExec: measurement={}, time=[{}..{}], filters={}, limit={:?}",
            self.measurement,
            self.time_start,
            self.time_end,
            self.tag_filters.len(),
            self.limit,
        )
    }
}

impl ExecutionPlan for ChronixExec {
    fn name(&self) -> &'static str {
        "ChronixExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// A leaf scan: the tag filters and field predicates are chronix
    /// predicate types resolved against the segment reader, not
    /// `PhysicalExpr` trees, so the optimiser has nothing to walk here.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion, DataFusionError>,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    /// Stream the scan.
    ///
    /// The scan runs on the blocking pool and pushes batches through a bounded
    /// channel, so the first batch reaches DataFusion while the rest of the
    /// range is still being read, and a consumer that stops early stops the
    /// scan with it.
    ///
    /// The `LIMIT` is part of the chronix plan rather than applied to the
    /// result, which is what makes `execute_iter` stop reading buckets; and
    /// what the operator holds is reserved from the task's memory pool.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let db = self.db.clone();
        let measurement = self.measurement.clone();
        let time_start = self.time_start;
        let time_end = self.time_end;
        let tag_filters = self.tag_filters.clone();
        let field_predicates = self.field_predicates.clone();
        let limit = self.limit;
        let schema = self.projected_schema.clone();
        let stream_schema = self.projected_schema.clone();

        // Contradictory bounds — `_time > X AND _time < Y` with `Y <= X` —
        // describe an empty set, which in SQL is no rows rather than an
        // error. The query builder rejects an inverted range, so the
        // emptiness is answered here instead of surfacing as a failure.
        if time_start > time_end {
            let empty = arrow::record_batch::RecordBatch::new_empty(schema.clone());
            return Ok(Box::pin(ChronixStream {
                schema,
                inner: Box::pin(futures::stream::once(async move { Ok(empty) })),
            }));
        }

        // Two in flight: one being consumed downstream, one being read. Deeper
        // buys nothing — the reader is I/O-bound and the consumer is not — and
        // costs a batch of memory per slot.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<RecordBatch, DataFusionError>>(2);

        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let rows_scanned: Count =
            MetricBuilder::new(&self.metrics).counter("rows_scanned", partition);
        let scan_rows = rows_scanned.clone();

        let scan = move || {
            let mut builder = db.query().measurement(&measurement);
            for (key, value) in &tag_filters {
                builder = builder.tag(key, value);
            }
            builder = builder.range(time_start, time_end);
            let mut plan = builder.build()?;
            // Inject zone-map field predicates from SQL pushdown.
            chronix_query::plan::set_field_predicates(&mut plan, field_predicates);

            // The limit belongs *in* the plan: that is what stops
            // `execute_iter` materialising the buckets past it.
            if let Some(lim) = limit {
                plan = chronix_query::plan::QueryPlan::Limit {
                    source: Box::new(plan),
                    limit: lim,
                    offset: 0,
                };
            }

            for batch in db.execute_iter(&plan)? {
                if let Ok(b) = &batch {
                    scan_rows.add(b.num_rows());
                }
                // A closed receiver means the consumer is gone — a LIMIT
                // satisfied upstream, or a cancelled query. Stop reading.
                if tx.blocking_send(batch.map_err(to_df_error)).is_err() {
                    break;
                }
            }
            Ok::<(), crate::error::DbError>(())
        };

        let reservation = MemoryConsumer::new(format!("ChronixExec[{}]", self.measurement))
            .register(context.memory_pool());

        let stream = async_stream::try_stream! {
            let handle = tokio::task::spawn_blocking(scan);
            let _timer = baseline.elapsed_compute().timer();

            let mut held = 0usize;
            while let Some(batch) = rx.recv().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }

                // Account for what this operator is holding before it is
                // handed on, so a scan that outruns the pool is refused here
                // rather than silently allocated.
                let size = batch.get_array_memory_size();
                reservation.try_grow(size)?;
                reservation.shrink(held);
                held = size;

                let batch = convert_timestamp_column(batch)?;

                // Project by NAME, not by index.
                //
                // The storage layer emits columns in its own canonical order
                // (timestamp, tags sorted, fields sorted) while `projection`
                // indexes into the DataFusion table schema. Those two orders
                // agree only by coincidence; when a measurement's fields are
                // registered in non-alphabetical order across separate writes
                // they diverge, and an index-based `batch.project()` then
                // silently returns a *different column's data* under the
                // requested column's name. Resolving by name makes the emitted
                // batch match `projected_schema` by construction.
                let batch = align_batch_to_schema(&batch, &stream_schema)?;

                baseline.record_output(batch.num_rows());
                yield batch;
            }
            reservation.free();
            baseline.done();

            // The reader finished or failed; a build error never reached the
            // channel, so it is surfaced here.
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => Err(to_df_error(e))?,
                Err(e) => Err(DataFusionError::External(Box::new(e)))?,
            }
        };

        Ok(Box::pin(ChronixStream {
            schema,
            inner: Box::pin(stream),
        }))
    }

    /// Expose segment-level column statistics to DataFusion's cost-based
    /// optimizer.
    ///
    /// Aggregates min/max, null counts, row counts, and distinct counts
    /// across all active segments for this measurement. Values are
    /// reported as `Inexact` since they represent pre-filter estimates
    /// from segment zone maps, not exact post-filter counts.
    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>, DataFusionError> {
        let catalog = self.db.catalog().read();
        let segments = catalog.active_segments_for_measurement(&self.measurement);

        if segments.is_empty() {
            return Ok(Arc::new(Statistics::new_unknown(&self.projected_schema)));
        }

        // Aggregate per-column stats across segments.
        // Key: column name → (data_type, merged min/max/null/value/distinct/sum).
        struct MergedStats {
            data_type: u8,
            min_i64: i64,
            max_i64: i64,
            min_u64: u64,
            max_u64: u64,
            null_count: u64,
            value_count: u64,
            distinct_count: u64,
            sum_f64: f64,
            sum_i128: i128,
        }

        let mut col_map: HashMap<String, MergedStats> = HashMap::new();
        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;

        for seg in &segments {
            total_rows += seg.row_count;
            total_bytes += seg.byte_size;

            for cs in &seg.column_stats {
                let entry = col_map
                    .entry(cs.name.clone())
                    .or_insert_with(|| MergedStats {
                        data_type: cs.data_type,
                        min_i64: i64::MAX,
                        max_i64: i64::MIN,
                        min_u64: u64::MAX,
                        max_u64: u64::MIN,
                        null_count: 0,
                        value_count: 0,
                        distinct_count: 0,
                        sum_f64: 0.0,
                        sum_i128: 0,
                    });

                // Skip columns whose data type changed across segments
                // (schema evolution mismatch) — merging min/max across
                // different types would produce nonsense statistics.
                if entry.data_type != cs.data_type {
                    continue;
                }

                entry.min_i64 = entry.min_i64.min(cs.stats.min_value);
                entry.max_i64 = entry.max_i64.max(cs.stats.max_value);
                entry.min_u64 = entry.min_u64.min(cs.stats.min_value_u64);
                entry.max_u64 = entry.max_u64.max(cs.stats.max_value_u64);
                entry.null_count = entry.null_count.saturating_add(cs.stats.null_count);
                entry.value_count = entry.value_count.saturating_add(cs.stats.value_count);
                // distinct_count across segments is an upper bound (sum).
                entry.distinct_count = entry
                    .distinct_count
                    .saturating_add(u64::from(cs.stats.distinct_count));
                entry.sum_f64 += cs.stats.sum;
                entry.sum_i128 = entry.sum_i128.saturating_add(cs.stats.sum_i128);
            }
        }

        // Build per-column DataFusion statistics aligned with the projected schema.
        let column_statistics: Vec<ColumnStatistics> = self
            .projected_schema
            .fields()
            .iter()
            .map(|field| {
                // Map the Arrow field name back to the Chronix column name.
                // The `_time` virtual column maps to `_time`.
                let chronix_name = if field.name() == "_time" {
                    chronix_core::TIME_COLUMN
                } else {
                    field.name().as_str()
                };

                let Some(merged) = col_map.get(chronix_name) else {
                    return ColumnStatistics::new_unknown();
                };

                let (min_value, max_value, sum_value) = match merged.data_type {
                    data_types::TIMESTAMP | data_types::I64 => (
                        Precision::Inexact(ScalarValue::Int64(Some(merged.min_i64))),
                        Precision::Inexact(ScalarValue::Int64(Some(merged.max_i64))),
                        // Use i128 sum cast to i64 when it fits.
                        if let Ok(s) = i64::try_from(merged.sum_i128) {
                            Precision::Inexact(ScalarValue::Int64(Some(s)))
                        } else {
                            Precision::Absent
                        },
                    ),
                    data_types::U64 => (
                        Precision::Inexact(ScalarValue::UInt64(Some(merged.min_u64))),
                        Precision::Inexact(ScalarValue::UInt64(Some(merged.max_u64))),
                        if let Ok(s) = u64::try_from(merged.sum_i128) {
                            Precision::Inexact(ScalarValue::UInt64(Some(s)))
                        } else {
                            Precision::Absent
                        },
                    ),
                    data_types::F64 => (
                        Precision::Inexact(ScalarValue::Float64(Some(ordered_i64_to_f64(
                            merged.min_i64,
                        )))),
                        Precision::Inexact(ScalarValue::Float64(Some(ordered_i64_to_f64(
                            merged.max_i64,
                        )))),
                        Precision::Inexact(ScalarValue::Float64(Some(merged.sum_f64))),
                    ),
                    data_types::BOOL => (
                        Precision::Inexact(ScalarValue::Boolean(Some(merged.min_i64 != 0))),
                        Precision::Inexact(ScalarValue::Boolean(Some(merged.max_i64 != 0))),
                        Precision::Absent,
                    ),
                    // String, tag and decimal columns: no usable numeric
                    // min/max here. A decimal's zone map holds *mantissas*,
                    // saturated into an `i64`, and a `ScalarValue` for the
                    // column would have to carry its scale to mean anything;
                    // reporting nothing costs a planner hint, reporting the
                    // raw mantissa would cost correctness. Row-group pruning
                    // still uses those bounds, in the mantissa domain where
                    // they are meaningful — see `FieldPredicate`.
                    _ => (Precision::Absent, Precision::Absent, Precision::Absent),
                };

                ColumnStatistics {
                    null_count: Precision::Inexact(merged.null_count as usize),
                    max_value,
                    min_value,
                    sum_value,
                    distinct_count: if merged.distinct_count > 0 {
                        Precision::Inexact(merged.distinct_count as usize)
                    } else {
                        Precision::Absent
                    },
                    // Per-column byte size is not tracked in the zone maps;
                    // `total_byte_size` above is the segment-level figure.
                    byte_size: Precision::Absent,
                }
            })
            .collect();

        Ok(Arc::new(Statistics {
            num_rows: Precision::Inexact(total_rows as usize),
            total_byte_size: Precision::Inexact(total_bytes as usize),
            column_statistics,
        }))
    }
}

// ── Stream adapter ──────────────────────────────────────────────────────

struct ChronixStream {
    schema: SchemaRef,
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
}

impl Stream for ChronixStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for ChronixStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use tempfile::TempDir;

    fn open_test_db() -> (Arc<Chronix>, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());
        (db, dir)
    }

    #[test]
    fn statistics_unknown_for_empty_measurement() {
        let (db, _dir) = open_test_db();
        let key = SeriesKey::new("cpu", crate::tags! { "host" => "a" }).unwrap();
        let p = Point::new(key, crate::fields! { "usage" => 1.0_f64 }, 1_000_000).unwrap();
        db.insert(&p).unwrap();

        let schema =
            super::super::provider::measurement_schema_to_arrow(&db.schema("cpu").unwrap());
        let exec = ChronixExec::new(
            db.clone(),
            "cpu".to_string(),
            schema.clone(),
            None,
            i64::MIN,
            i64::MAX,
            vec![],
            vec![],
            None,
        )
        .unwrap();

        // No segments flushed yet — stats come from catalog, which is empty.
        let stats = exec
            .statistics_from_inputs(&[], &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[test]
    fn statistics_reflects_flushed_segments() {
        let (db, _dir) = open_test_db();
        let key = SeriesKey::new("temp", crate::tags! { "room" => "lab" }).unwrap();
        for i in 0..10 {
            let ts = (i + 1) * 1_000_000_000_i64;
            let p = Point::new(
                key.clone(),
                crate::fields! { "value" => 20.0 + i as f64 },
                ts,
            )
            .unwrap();
            db.insert(&p).unwrap();
        }
        // Force flush to create segment(s) with catalog stats.
        db.flush().unwrap();

        let schema =
            super::super::provider::measurement_schema_to_arrow(&db.schema("temp").unwrap());
        let exec = ChronixExec::new(
            db.clone(),
            "temp".to_string(),
            schema.clone(),
            None,
            i64::MIN,
            i64::MAX,
            vec![],
            vec![],
            None,
        )
        .unwrap();

        let stats = exec
            .statistics_from_inputs(&[], &StatisticsArgs::new())
            .unwrap();

        // Row count must reflect all 10 rows.
        assert_eq!(stats.num_rows, Precision::Inexact(10));
        assert!(matches!(stats.total_byte_size, Precision::Inexact(b) if b > 0));

        // Column statistics should exist for every projected column.
        assert_eq!(stats.column_statistics.len(), schema.fields().len());

        // Find the "value" column (F64) and verify min/max.
        let value_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "value")
            .unwrap();
        let vs = &stats.column_statistics[value_idx];
        match &vs.min_value {
            Precision::Inexact(ScalarValue::Float64(Some(v))) => {
                assert!(*v >= 19.0 && *v <= 21.0, "min_value {v} out of range");
            }
            other => panic!("expected Inexact Float64 min, got {other:?}"),
        }
        match &vs.max_value {
            Precision::Inexact(ScalarValue::Float64(Some(v))) => {
                assert!(*v >= 28.0 && *v <= 30.0, "max_value {v} out of range");
            }
            other => panic!("expected Inexact Float64 max, got {other:?}"),
        }
        // Null count should be zero for non-null data.
        assert_eq!(vs.null_count, Precision::Inexact(0));
    }

    #[test]
    fn statistics_with_projection() {
        let (db, _dir) = open_test_db();
        let key = SeriesKey::new("disk", crate::tags! { "device" => "sda" }).unwrap();
        for i in 0..5 {
            let ts = (i + 1) * 1_000_000_000_i64;
            let p = Point::new(
                key.clone(),
                crate::fields! { "bytes_read" => i * 100, "bytes_written" => i * 50 },
                ts,
            )
            .unwrap();
            db.insert(&p).unwrap();
        }
        db.flush().unwrap();

        let full_schema =
            super::super::provider::measurement_schema_to_arrow(&db.schema("disk").unwrap());

        // Project only _time and bytes_read (indices 0, 2 — skip device tag).
        let time_idx = full_schema
            .fields()
            .iter()
            .position(|f| f.name() == "_time")
            .unwrap();
        let br_idx = full_schema
            .fields()
            .iter()
            .position(|f| f.name() == "bytes_read")
            .unwrap();
        let projection = vec![time_idx, br_idx];

        let exec = ChronixExec::new(
            db.clone(),
            "disk".to_string(),
            full_schema,
            Some(&projection),
            i64::MIN,
            i64::MAX,
            vec![],
            vec![],
            None,
        )
        .unwrap();

        let stats = exec
            .statistics_from_inputs(&[], &StatisticsArgs::new())
            .unwrap();
        // Only 2 columns in projected schema.
        assert_eq!(stats.column_statistics.len(), 2);
        assert_eq!(stats.num_rows, Precision::Inexact(5));

        // Second column should be bytes_read (I64).
        let br_stats = &stats.column_statistics[1];
        match &br_stats.min_value {
            Precision::Inexact(ScalarValue::Int64(Some(v))) => assert_eq!(*v, 0),
            other => panic!("expected Inexact Int64 min, got {other:?}"),
        }
        match &br_stats.max_value {
            Precision::Inexact(ScalarValue::Int64(Some(v))) => assert_eq!(*v, 400),
            other => panic!("expected Inexact Int64 max, got {other:?}"),
        }
    }
}
