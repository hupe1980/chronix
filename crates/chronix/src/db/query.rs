//! Query-path methods for [`Chronix`] — plan execution, segment reading, projection.

use std::collections::BTreeMap;
use std::sync::Arc;

use metrics::histogram;
use tracing::warn;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use chronix_core::{MeasurementSchema, Point, SegmentState, SeriesKey, ShardId, TombstoneSet};
use chronix_engine::index::{
    SegmentCatalog, SegmentCatalogEntry, SeriesBloomFilter, TagInvertedIndex, TimeIndex,
    TimeIndexEntry,
};
use chronix_engine::segment::reader::SegmentReader;
use chronix_query::plan::{
    extract_field_predicates, extract_max_series, extract_namespace, extract_scan, QueryPlan,
};
use chronix_query::QueryBuilder;

use crate::error::{DbError, Result};

/// Context for filtering segment reads — groups related parameters
/// to keep function signatures concise.
pub(super) struct SegmentFilterCtx<'a> {
    pub(super) scan_columns: Option<&'a Vec<String>>,
    pub(super) time_range: &'a chronix_query::plan::TimeRange,
    pub(super) tag_filter_refs: &'a [(&'a str, &'a str)],
    pub(super) field_predicates: &'a [chronix_engine::segment::FieldPredicate],
    pub(super) measurement: &'a str,
    pub(super) projection: &'a [String],
    pub(super) plan: &'a QueryPlan,
    pub(super) tombstones: &'a TombstoneSet,
    pub(super) tag_col_names: Option<&'a [&'a str]>,
}

impl super::Chronix {
    /// Start building a query using the fluent builder API.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use chronix::Chronix;
    /// # use chronix_core::ChronixConfig;
    /// # let config = ChronixConfig::builder().data_dir("/tmp/q").build().unwrap();
    /// # let db = Chronix::open(config).unwrap();
    /// let plan = db.query()
    ///     .measurement("cpu")
    ///     .tag("host", "server-01")
    ///     .field("usage_idle")
    ///     .range(0, i64::MAX)
    ///     .build()
    ///     .unwrap();
    /// let batch = db.execute(&plan).unwrap();
    /// ```
    #[must_use]
    pub fn query(&self) -> QueryBuilder {
        QueryBuilder::new()
    }

    /// Execute a query plan and return results as an Arrow [`RecordBatch`].
    ///
    /// # Execution pipeline
    ///
    /// 1. Extract scan parameters from the plan
    /// 2. Scan in-memory buffers (memtable) for the measurement
    /// 3. Find matching on-disk segments via the catalog
    /// 4. **Segment-level predicate pushdown:**
    ///    - Time-range pruning against each segment's `[min_ts, max_ts]`
    ///    - Inverted tag-index pre-filter (eliminates segments that don't
    ///      contain any of the requested tag key=value pairs)
    ///    - Bloom filter pruning (eliminates segments that definitely don't
    ///      contain the queried series key)
    /// 5. **Row-group-level predicate pushdown:**
    ///    - Each surviving segment is read via
    ///      [`read_projected_filtered_with_predicates`](chronix_engine::segment::SegmentReader::read_projected_filtered_with_predicates),
    ///      which skips row groups whose timestamp stats don't overlap the
    ///      query window and whose tag column stats prove they cannot match.
    /// 6. Sort-merge deduplicate all batches
    /// 7. Apply tag filters and time range filtering
    /// 8. Apply column projection
    /// 9. Apply aggregation or downsampling if specified
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, the plan is invalid,
    /// or any I/O operation fails during segment reads.
    #[must_use = "query errors must not be silently ignored"]
    pub fn execute(&self, plan: &QueryPlan) -> Result<RecordBatch> {
        // For Aggregate(Scan) and Downsample(Scan), prefer
        // the streaming path which uses incremental accumulators and avoids
        // materializing the full intermediate result set.
        if matches!(
            plan,
            QueryPlan::Aggregate { source, .. } | QueryPlan::Downsample { source, .. }
                if matches!(source.as_ref(), QueryPlan::Scan { .. })
        ) {
            let batches = self.execute_stream(plan)?;
            return if batches.is_empty() {
                let arrow_schema = extract_scan(plan)
                    .and_then(|(m, _, proj, _)| {
                        self.schema(m)
                            .map(|ms| Self::measurement_to_arrow_schema(&ms, proj))
                    })
                    .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
                Ok(RecordBatch::new_empty(arrow_schema))
            } else if batches.len() == 1 {
                Ok(batches.into_iter().next().expect("checked len"))
            } else {
                arrow::compute::concat_batches(&batches[0].schema(), &batches)
                    .map_err(|e| DbError::Internal(e.to_string()))
            };
        }
        let (batch, _stats) = self.execute_inner(plan)?;
        Ok(batch)
    }

    /// Execute a query plan and return results together with
    /// [`PruningStats`](chronix_query::pruning::PruningStats) that
    /// describe how many segments were eliminated by the pruning pipeline.
    pub fn execute_with_stats(
        &self,
        plan: &QueryPlan,
    ) -> Result<(RecordBatch, chronix_query::pruning::PruningStats)> {
        self.execute_inner(plan)
    }

    /// Shared implementation for `execute` and `execute_with_stats`.
    fn execute_inner(
        &self,
        plan: &QueryPlan,
    ) -> Result<(RecordBatch, chronix_query::pruning::PruningStats)> {
        let start = std::time::Instant::now();
        let query_timeout = self.config.query_timeout;
        self.check_open()?;

        // Per-query memory tracking.
        let mem_limit = self.config.per_query_memory_limit;
        let memory_tracker = if mem_limit > 0 {
            Some(std::sync::Arc::new(chronix_query::MemoryTracker::new(
                mem_limit,
            )))
        } else {
            None
        };

        let (measurement, tag_filters, projection, time_range) = extract_scan(plan)
            .ok_or_else(|| DbError::Internal("invalid query plan: no scan node found".into()))?;

        // Defense-in-depth — warn if no namespace scope is set.
        // This catches queries that bypass the HTTP layer's namespace injection.
        if extract_namespace(plan).is_none() {
            warn!(
                measurement,
                "query plan has no namespace_id — tenant isolation relies solely on tag filters"
            );
        }

        // 1. Scan memtable for this measurement, filtering tombstones
        // No tombstone filter here, and that is a property rather than an
        // omission: every delete flushes the memtables before it scans, so a
        // tombstone can only ever refer to data that is already in a segment.
        // What used to stand here asked "does this series have any tombstone",
        // ignoring the tombstone's time range, so deleting one hour of one
        // series hid that series entirely from this path while the segment
        // scan beside it — which does check the range — kept showing it.
        let memtable_points: Vec<chronix_core::Point> =
            self.shards
                .scan_measurement(measurement, time_range.start, time_range.end);
        let memtable_batch = chronix_query::points_to_record_batch(&memtable_points)?;

        // 2. Shared segment pruning pipeline (time + tag-index + bloom).
        let (matching_entries, pruning_stats) =
            self.prune_segments(measurement, tag_filters, time_range);

        // Limit scanning by *estimated series count*, not
        // raw segment count.  Each `SegmentCatalogEntry` carries a
        // `series_count` stored in the segment header.  We accumulate
        // the series counts and stop including segments once we reach
        // the limit.  This is still an upper-bound heuristic (segments
        // may share series), but it's far more accurate than truncating
        // the segment list at a fixed offset.
        let matching_entries = if let Some(limit) = extract_max_series(plan) {
            let mut accumulated_series: u64 = 0;
            let mut kept = Vec::with_capacity(matching_entries.len().min(limit));
            for entry in matching_entries {
                if accumulated_series >= limit as u64 {
                    break;
                }
                accumulated_series += u64::from(entry.series_count.max(1));
                kept.push(entry);
            }
            if accumulated_series > limit as u64 {
                tracing::debug!(
                    segments_kept = kept.len(),
                    estimated_series = accumulated_series,
                    max_series = limit,
                    "capping segment scan by estimated series count"
                );
            }
            kept
        } else {
            matching_entries
        };

        // Metadata-only aggregation fast path.
        // For simple COUNT / MIN(timestamp) / MAX(timestamp) without
        // group-by, answer directly from catalog metadata + memtable
        // points, avoiding all segment I/O.
        if let QueryPlan::Aggregate {
            functions,
            group_by,
            ..
        } = plan
        {
            if group_by.is_empty() && !functions.is_empty() {
                use chronix_query::aggregate::AggFn;
                let all_meta = functions
                    .iter()
                    .all(|f| matches!(f, AggFn::Count | AggFn::Min | AggFn::Max));
                if all_meta {
                    let mem_count = memtable_batch.num_rows() as u64;
                    let mem_min_ts: Option<i64> = if mem_count > 0 {
                        memtable_batch.column_by_name("timestamp").and_then(|c| {
                            arrow::compute::kernels::aggregate::min(
                                c.as_any().downcast_ref::<arrow::array::Int64Array>()?,
                            )
                        })
                    } else {
                        None
                    };
                    let mem_max_ts: Option<i64> = if mem_count > 0 {
                        memtable_batch.column_by_name("timestamp").and_then(|c| {
                            arrow::compute::kernels::aggregate::max(
                                c.as_any().downcast_ref::<arrow::array::Int64Array>()?,
                            )
                        })
                    } else {
                        None
                    };

                    let mut total_count: u64 = mem_count;
                    let mut global_min: Option<i64> = mem_min_ts;
                    let mut global_max: Option<i64> = mem_max_ts;

                    for entry in &matching_entries {
                        total_count += entry.row_count;
                        let ts_min = entry.min_timestamp;
                        let ts_max = entry.max_timestamp;
                        global_min = Some(global_min.map_or(ts_min, |v: i64| v.min(ts_min)));
                        global_max = Some(global_max.map_or(ts_max, |v: i64| v.max(ts_max)));
                    }

                    use arrow::array::ArrayRef;
                    use arrow::datatypes::{DataType, Field, Schema};
                    let mut fields = Vec::with_capacity(functions.len());
                    let mut columns: Vec<ArrayRef> = Vec::with_capacity(functions.len());
                    for func in functions {
                        match func {
                            AggFn::Count => {
                                fields.push(Field::new("count", DataType::UInt64, false));
                                columns.push(Arc::new(arrow::array::UInt64Array::from(vec![
                                    total_count,
                                ])));
                            }
                            AggFn::Min => {
                                fields.push(Field::new("min", DataType::Int64, true));
                                columns.push(Arc::new(arrow::array::Int64Array::from(vec![
                                    global_min,
                                ])));
                            }
                            AggFn::Max => {
                                fields.push(Field::new("max", DataType::Int64, true));
                                columns.push(Arc::new(arrow::array::Int64Array::from(vec![
                                    global_max,
                                ])));
                            }
                            _ => unreachable!(),
                        }
                    }
                    let schema = Arc::new(Schema::new(fields));
                    let batch = RecordBatch::try_new(schema, columns)
                        .map_err(|e| DbError::Internal(e.to_string()))?;
                    histogram!("chronix_query_duration_seconds")
                        .record(start.elapsed().as_secs_f64());
                    return Ok((batch, pruning_stats));
                }
            }
        }

        // 3. Compute the minimal set of columns needed from segments
        //    (timestamp is always included by read_projected)
        let scan_columns = Self::compute_scan_columns(plan, tag_filters, projection);

        // 4. Collect batches: memtable + on-disk segments (with projection + row-group time pushdown)
        // Push tag predicates down to row-group level in the segment reader.
        let tag_filter_refs_for_pushdown: Vec<(&str, &str)> = tag_filters
            .iter()
            .map(|f| (f.key.as_str(), f.value.as_str()))
            .collect();
        let field_preds = extract_field_predicates(plan);
        // Compute query deadline for cancellation during I/O.
        let query_deadline = if !query_timeout.is_zero() {
            Some(start + query_timeout)
        } else {
            None
        };
        // Pre-filter memtable data so that the post-merge
        // filter_batch (which is redundant for already-pushed-down segment
        // data) can be skipped.
        let tag_filter_refs: Vec<(&str, &str)> = tag_filters
            .iter()
            .map(|f| (f.key.as_str(), f.value.as_str()))
            .collect();
        let memtable_batch = if memtable_batch.num_rows() > 0 {
            chronix_query::filter::filter_batch(
                &memtable_batch,
                time_range.start,
                time_range.end,
                &tag_filter_refs,
            )?
        } else {
            memtable_batch
        };

        let batches = self.collect_batches(
            memtable_batch,
            &matching_entries,
            scan_columns.as_deref(),
            Some((time_range.start, time_range.end)),
            &tag_filter_refs_for_pushdown,
            field_preds,
            query_deadline,
        )?;

        // Check query timeout after collecting batches.
        if !query_timeout.is_zero() && start.elapsed() > query_timeout {
            return Err(DbError::QueryTimeout(query_timeout));
        }

        // Memory guard: abort if collected batches exceed the configured limit.
        let max_bytes = self.config.max_query_result_bytes;
        if max_bytes > 0 {
            let total: usize = batches
                .iter()
                .map(arrow::array::RecordBatch::get_array_memory_size)
                .sum();
            if total > max_bytes {
                return Err(DbError::Internal(format!(
                    "query result too large: {total} bytes exceeds limit of {max_bytes} bytes \
                     (config max_query_result_bytes). Narrow the time range or add filters."
                )));
            }
        }

        // Charge collected batch memory against the per-query tracker.
        if let Some(ref tracker) = memory_tracker {
            let total: usize = batches
                .iter()
                .map(arrow::array::RecordBatch::get_array_memory_size)
                .sum();
            tracker.try_allocate(total)?;
        }

        if batches.is_empty() {
            // Return an empty batch that preserves the measurement schema
            // so callers can introspect column names and types.
            let arrow_schema = self.schema(measurement).map_or_else(
                || Arc::new(arrow::datatypes::Schema::empty()),
                |ms| Self::measurement_to_arrow_schema(&ms, projection),
            );
            return Ok((RecordBatch::new_empty(arrow_schema), pruning_stats));
        }

        // 4. Sort-merge dedup
        let merged = chronix_query::dedup::sort_merge_dedup(
            batches,
            measurement,
            memory_tracker.as_deref(),
        )?;

        // 5. Both merge inputs are already trimmed — the memtable above,
        // segment batches inside `collect_batches` — so this is a backstop
        // over small data. It is unconditional: running it only when a tag
        // filter was present let a range-only query return every row of a
        // straddling row group.
        let merged = chronix_query::filter::filter_batch(
            &merged,
            time_range.start,
            time_range.end,
            &tag_filter_refs,
        )?;

        // 5b. Filter out tombstoned series from segment data
        let filtered = {
            let tombs = self.tombstones.read();
            if tombs.is_empty() {
                merged
            } else {
                let tag_names = self.schema(measurement).map(|ms| {
                    ms.tag_names()
                        .into_iter()
                        .map(String::from)
                        .collect::<Vec<_>>()
                });
                let tag_refs: Option<Vec<&str>> = tag_names
                    .as_ref()
                    .map(|v| v.iter().map(String::as_str).collect());
                chronix_query::filter::filter_tombstoned(
                    &merged,
                    measurement,
                    &tombs,
                    tag_refs.as_deref(),
                )?
            }
        };

        // 6. Apply column projection (preserving group-by columns for aggregation)
        let projected = if projection.is_empty() {
            filtered
        } else {
            // Merge projection with group-by columns so aggregation can
            // find them after column projection.
            let mut full_projection: Vec<String> = projection.to_vec();
            if let QueryPlan::Aggregate { group_by, .. } = plan {
                for gb in group_by {
                    if !full_projection.contains(gb) {
                        full_projection.push(gb.clone());
                    }
                }
            }
            chronix_query::filter::project_batch(&filtered, &full_projection)?
        };

        // Enrich aggregate plan with estimated cardinality
        // from catalog column statistics when the builder didn't supply one.
        let enriched_plan = if let QueryPlan::Aggregate {
            ref source,
            ref functions,
            ref group_by,
            estimated_cardinality: None,
        } = *plan
        {
            if !group_by.is_empty() {
                // Estimate cardinality as max distinct_count across group-by
                // columns from the matching catalog entries.
                let est = matching_entries
                    .iter()
                    .flat_map(|entry| &entry.column_stats)
                    .filter(|cs| group_by.iter().any(|g| g == &cs.name))
                    .map(|cs| cs.stats.distinct_count as usize)
                    .max();
                if est.is_some() {
                    Some(QueryPlan::Aggregate {
                        source: source.clone(),
                        functions: functions.clone(),
                        group_by: group_by.clone(),
                        estimated_cardinality: est,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let effective_plan = enriched_plan.as_ref().unwrap_or(plan);

        // 7. Apply aggregation or downsampling
        let result = Self::apply_post_processing(
            effective_plan,
            projected,
            tag_filters,
            memory_tracker.as_deref(),
        )?;

        // Check query timeout after Arrow aggregate/downsample
        // operations, ensuring large sort/group computations cannot exceed
        // the deadline.
        if !query_timeout.is_zero() && start.elapsed() > query_timeout {
            return Err(DbError::QueryTimeout(query_timeout));
        }

        // Release tracked memory after query completion.
        if let Some(ref tracker) = memory_tracker {
            tracker.deallocate(tracker.allocated());
        }

        histogram!("chronix_query_duration_seconds").record(start.elapsed().as_secs_f64());
        Ok((result, pruning_stats))
    }

    /// Execute a query plan and return results as a sequence of
    /// [`RecordBatch`]es for constant-memory consumption.
    ///
    /// # Streaming Architecture
    ///
    /// For `Scan` plans, each segment produces an independent batch so
    /// callers can process and drop batches incrementally. When multiple
    /// sources overlap, sort-merge dedup (with `interleave` — no
    /// `concat_batches`) produces a deduplicated result that is then
    /// chunked into row-group-sized batches (64 Ki rows each) so the
    /// caller never has to hold the full result set in memory.
    ///
    /// For `Aggregate` and `Downsample` plans, all data must be
    /// materialized for the computation, so a single batch is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, the plan is invalid,
    /// or any I/O operation fails during segment reads.
    ///
    /// # Deduplication
    ///
    /// Sort-merge deduplication is applied across all returned batches,
    /// ensuring last-write-wins semantics even when a data point exists
    /// in both the memtable and a segment (e.g. after WAL replay).
    #[must_use = "query errors must not be silently ignored"]
    pub fn execute_stream(&self, plan: &QueryPlan) -> Result<Vec<RecordBatch>> {
        self.check_open()?;

        // For Aggregate over Scan, use streaming aggregation to avoid
        // materializing the full result set.
        if let QueryPlan::Aggregate {
            source,
            functions,
            group_by,
            ..
        } = plan
        {
            if let QueryPlan::Scan {
                measurement,
                tag_filters,
                projection,
                time_range,
                max_series,
                field_predicates,
                namespace_id,
            } = source.as_ref()
            {
                // Augment the scan projection with
                // group-by columns so they are available for aggregation.
                let augmented_source = if !projection.is_empty() && !group_by.is_empty() {
                    let mut aug = projection.clone();
                    for gb in group_by {
                        if !aug.contains(gb) {
                            aug.push(gb.clone());
                        }
                    }
                    QueryPlan::Scan {
                        measurement: measurement.clone(),
                        tag_filters: tag_filters.clone(),
                        projection: aug,
                        time_range: *time_range,
                        max_series: *max_series,
                        field_predicates: field_predicates.clone(),
                        namespace_id: namespace_id.clone(),
                    }
                } else {
                    source.as_ref().clone()
                };

                // Fold the scan into accumulators batch by batch.
                // Aggregation state is O(groups × fields); holding the input
                // instead made `SELECT avg(x)` over a week of 1 s data cost
                // gigabytes to produce a single row.
                //
                // The budget has to be built and threaded here. It used
                // to be passed as `None`, so `per_query_memory_limit` bound on
                // `execute()` and not on this path — and this is the path SQL,
                // HTTP `/query`, gRPC, PromQL, remote read and Flight SQL all
                // take. Folding batch-by-batch bounds memory in the *row* count
                // but not in the *group* count, so a group-by on a
                // high-cardinality tag grew until the process died rather than
                // until the query failed.
                let mem_limit = self.config.per_query_memory_limit;
                let memory_tracker = if mem_limit > 0 {
                    Some(chronix_query::MemoryTracker::new(mem_limit))
                } else {
                    None
                };
                let tracker = memory_tracker.as_ref();

                let mut stream = self.execute_iter(&augmented_source)?;
                let Some(first) = stream.next().transpose()? else {
                    return Ok(vec![]);
                };

                // Field columns come from the first batch's schema; later
                // batches may add columns under schema evolution, which the
                // aggregator tolerates by ignoring names it was not given.
                let schema = first.schema();
                let field_cols: Vec<&str> = schema
                    .fields()
                    .iter()
                    .filter(|f| {
                        f.name() != "timestamp"
                            && !tag_filters.iter().any(|tf| tf.key == *f.name())
                            && !group_by.iter().any(|g| g == f.name())
                    })
                    .map(|f| f.name().as_str())
                    .collect();
                let group_refs: Vec<&str> = group_by.iter().map(String::as_str).collect();

                let mut agg = chronix_query::aggregate::StreamingAggregator::new(
                    functions,
                    &field_cols,
                    &group_refs,
                );
                agg.push(&first, tracker)?;
                drop(first);
                for batch in stream {
                    agg.push(&batch?, tracker)?;
                }

                let result = agg.finish(tracker)?;
                return if result.num_rows() == 0 {
                    Ok(vec![])
                } else {
                    Ok(vec![result])
                };
            }
        }

        // Streaming Limit(Scan) with early termination.
        // Stop reading segments once we have enough rows, avoiding full I/O.
        if let QueryPlan::Limit {
            source,
            limit,
            offset,
        } = plan
        {
            if matches!(source.as_ref(), QueryPlan::Scan { .. }) {
                // The scan iterator is lazy, so breaking out of this
                // loop genuinely stops reading segments. The previous version
                // collected the whole scan first and only skipped the
                // *processing* of the excess rows.
                let mut result = Vec::new();
                let mut skipped = 0usize;
                let mut taken = 0usize;
                for batch in self.execute_iter(source)? {
                    if taken >= *limit {
                        break;
                    }
                    let batch = batch?;
                    let rows = batch.num_rows();
                    if skipped < *offset {
                        let to_skip = (*offset - skipped).min(rows);
                        skipped += to_skip;
                        if to_skip < rows {
                            let remaining = rows - to_skip;
                            let take = remaining.min(*limit - taken);
                            result.push(batch.slice(to_skip, take));
                            taken += take;
                        }
                    } else {
                        let take = rows.min(*limit - taken);
                        result.push(batch.slice(0, take));
                        taken += take;
                    }
                }
                return Ok(result);
            }
        }

        // Streaming Downsample(Scan) — per-chunk downsampling.
        if let QueryPlan::Downsample {
            source,
            interval,
            function,
        } = plan
        {
            if matches!(source.as_ref(), QueryPlan::Scan { .. }) {
                // A bucket almost never lines up with a batch boundary,
                // so downsampling each batch independently emitted the
                // straddling interval twice. `StreamingDownsampler` keeps one
                // bucket open across batches and closes it only when a later
                // bucket arrives, so every interval is emitted exactly once.
                let mut stream = self.execute_iter(source)?;
                let Some(first) = stream.next().transpose()? else {
                    return Ok(vec![]);
                };

                // Downsample the first numeric column.
                let schema = first.schema();
                let value_col = schema
                    .fields()
                    .iter()
                    .find(|f| {
                        f.name() != "timestamp"
                            && matches!(
                                f.data_type(),
                                arrow::datatypes::DataType::Float64
                                    | arrow::datatypes::DataType::Int64
                                    | arrow::datatypes::DataType::UInt64
                            )
                    })
                    .map(|f| f.name().as_str())
                    .unwrap_or("value");

                let interval_ns = interval.as_nanos() as i64;
                let mut ds = chronix_query::downsample::StreamingDownsampler::new(
                    value_col,
                    interval_ns,
                    *function,
                )?;
                let mut results = ds.push(&first)?;
                drop(first);
                for batch in stream {
                    results.extend(ds.push(&batch?)?);
                }
                results.extend(ds.finish()?);
                results.retain(|b| b.num_rows() > 0);
                return Ok(results);
            }
        }

        // Other plan types (Window, non-Scan Aggregate/Downsample/Limit) need full materialization
        if !matches!(plan, QueryPlan::Scan { .. }) {
            let result = self.execute(plan)?;
            return if result.num_rows() == 0 {
                Ok(vec![])
            } else {
                Ok(vec![result])
            };
        }

        // Scan: delegate to the bounded-memory bucket stream and collect.
        // `execute_stream` holds every batch, so it is the caller that has to
        // honour `max_query_result_bytes`; `execute_iter` does not, because a
        // streaming consumer never accumulates.
        self.execute_iter(plan)?
            .with_max_total_bytes(self.config.max_query_result_bytes)
            .collect()
    }

    /// Read a single segment, applying filter/tombstone/projection.
    /// Returns `None` if the segment is empty after filtering.
    ///
    pub(super) fn read_segment_filtered(
        entry: &SegmentCatalogEntry,
        ctx: &SegmentFilterCtx<'_>,
    ) -> Result<Option<RecordBatch>> {
        let reader = match SegmentReader::open(&entry.path) {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    segment = %entry.path.display(),
                    error = %e,
                    "Skipping unreadable segment in stream"
                );
                return Ok(None);
            }
        };

        let time_range_ns = Some((ctx.time_range.start, ctx.time_range.end));
        let batch = if let Some(cols) = ctx.scan_columns {
            let col_refs: Vec<&str> = cols.iter().map(String::as_str).collect();
            let available: Vec<&str> = col_refs
                .into_iter()
                .filter(|c| reader.column_metadata().iter().any(|m| m.name == *c))
                .collect();
            if available.is_empty() {
                return Ok(None);
            }
            // Zone-map field predicate pushdown (matching collect_batches).
            reader.read_projected_with_zone_maps(
                &available,
                time_range_ns,
                ctx.tag_filter_refs,
                ctx.field_predicates,
            )?
        } else {
            reader.read_projected_with_zone_maps(
                &[],
                time_range_ns,
                ctx.tag_filter_refs,
                ctx.field_predicates,
            )?
        };

        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let filtered = chronix_query::filter::filter_batch(
            &batch,
            ctx.time_range.start,
            ctx.time_range.end,
            ctx.tag_filter_refs,
        )?;

        let filtered = Self::apply_projection_and_tombstone(
            &filtered,
            ctx.measurement,
            ctx.projection,
            ctx.plan,
            ctx.tombstones,
            ctx.tag_col_names,
        )?;

        if filtered.num_rows() > 0 {
            Ok(Some(filtered))
        } else {
            Ok(None)
        }
    }

    /// Load time indices, bloom filters, and tag index from existing catalog entries.
    pub(super) fn load_catalog_state(
        catalog: &SegmentCatalog,
    ) -> (
        BTreeMap<ShardId, TimeIndex>,
        BTreeMap<u64, SeriesBloomFilter>,
        TagInvertedIndex,
    ) {
        let mut time_indices: BTreeMap<ShardId, TimeIndex> = BTreeMap::new();
        let mut blooms: BTreeMap<u64, SeriesBloomFilter> = BTreeMap::new();
        let tag_index = TagInvertedIndex::new();

        for entry in catalog.all_segments() {
            // Only index active segments — soft-deleted ones are awaiting GC
            // and should not appear in query paths or inflate memory.
            if entry.state != SegmentState::Active {
                continue;
            }
            let idx = time_indices.entry(entry.shard_id).or_default();
            idx.add_segment(TimeIndexEntry {
                segment_id: entry.segment_id,
                min_ts: entry.min_timestamp,
                max_ts: entry.max_timestamp,
            });

            // Load bloom filter sidecar file if it exists
            let bloom_path = entry.path.with_extension("bloom");
            if bloom_path.exists() {
                match std::fs::read(&bloom_path) {
                    Ok(data) => match SeriesBloomFilter::from_bytes(&data) {
                        Ok(bf) => {
                            blooms.insert(entry.segment_id.0, bf);
                        }
                        Err(e) => {
                            warn!(
                                segment = %entry.path.display(),
                                error = %e,
                                "Skipping corrupt bloom filter"
                            );
                        }
                    },
                    Err(e) => {
                        warn!(
                            bloom = %bloom_path.display(),
                            error = %e,
                            "Failed to read bloom filter file"
                        );
                    }
                }
            }

            // Rebuild tag inverted index from segment data
            match SegmentReader::open(&entry.path) {
                Ok(reader) => {
                    if let Ok(batch) = reader.read_all() {
                        let series_keys =
                            Self::extract_series_keys_from_batch(&batch, &entry.measurement);
                        let tag_pairs: Vec<(&str, &str)> = series_keys
                            .iter()
                            .flat_map(|sk| sk.tags().iter().map(|(k, v)| (k.as_ref(), v.as_ref())))
                            .collect();
                        if !tag_pairs.is_empty() {
                            tag_index.add_segment(entry.segment_id, &tag_pairs);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        segment = %entry.path.display(),
                        error = %e,
                        "Could not load segment for tag index rebuild"
                    );
                }
            }
        }

        (time_indices, blooms, tag_index)
    }

    /// Apply tombstone filtering and column projection to a batch.
    pub(super) fn apply_projection_and_tombstone(
        batch: &RecordBatch,
        measurement: &str,
        projection: &[String],
        plan: &QueryPlan,
        tombstones: &TombstoneSet,
        tag_col_names: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        // Tombstone filtering
        let filtered = if tombstones.is_empty() {
            batch.clone()
        } else {
            chronix_query::filter::filter_tombstoned(batch, measurement, tombstones, tag_col_names)?
        };

        // Column projection
        if projection.is_empty() {
            Ok(filtered)
        } else {
            let mut full_projection: Vec<String> = projection.to_vec();
            if let QueryPlan::Aggregate { group_by, .. } = plan {
                for gb in group_by {
                    if !full_projection.contains(gb) {
                        full_projection.push(gb.clone());
                    }
                }
            }
            Ok(chronix_query::filter::project_batch(
                &filtered,
                &full_projection,
            )?)
        }
    }

    /// Return the most recent [`Point`] for a given series.
    ///
    /// This is a fast path: it scans the memtable first (newest data),
    /// then on-disk segments in reverse time order, and returns as soon
    /// as a matching point is found. No full query plan is needed.
    ///
    /// `tags` must name **every** tag of the series, not a subset: this
    /// answers "the newest point of *this* series", so a partial tag set
    /// identifies a different series and correctly yields `None`. Use
    /// [`query`](Self::query) with tag filters to ask about a *set* of series.
    ///
    /// Points masked by a tombstone are skipped at the timestamp level, so a
    /// ranged delete leaves the newest surviving point answerable rather than
    /// hiding the series.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or an I/O error occurs.
    pub fn last_value(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<Option<Point>> {
        self.check_open()?;

        let key = SeriesKey::new(measurement.to_string(), tags.clone())
            .map_err(|e| DbError::Internal(format!("Invalid series key: {e}")))?;

        // Tombstones are checked per timestamp, below, against every candidate
        // this function considers. A series-level check used to stand here
        // instead and return `None` outright, which made a delete of any single
        // hour of a series hide the series' newest point forever.
        let canonical = key.canonical_form().to_string();
        let masked = |ts: i64| self.tombstones.read().is_tombstoned(&canonical, ts);

        // 0. Check last-value cache (fastest path). The delete path evicts the
        // cache for every series it tombstones, so a hit here is live — but a
        // ranged delete leaves neighbouring points cached, so the entry is
        // still checked against the tombstone rather than trusted.
        if let Some(cached) = self.lvc.get_by_key(&key) {
            if !masked(cached.timestamp()) {
                return Ok(Some(cached));
            }
        }

        // 1. Take the memtable's newest point as a *candidate*.
        //
        // It is not automatically the answer. Writes are accepted up to
        // ±2 shards out of order, so a late arrival can sit in the memtable
        // behind an already-flushed newer point for the same series. Returning
        // the memtable point unconditionally reports a stale value in exactly
        // the case — late data — the engine advertises support for.
        //
        // Seeding the segment loop with it is also the fast path: that loop
        // walks segments newest-first and stops as soon as a segment's
        // `max_timestamp` cannot beat the best so far, so when the memtable
        // point really is the newest, no segment is opened at all.
        let memtable_best = self
            .shards
            .scan(&key, 0, i64::MAX)
            .into_iter()
            .rfind(|p| !masked(p.timestamp()));

        // 2. Check on-disk segments in reverse time order (with bloom pruning)
        let entries: Vec<SegmentCatalogEntry> = {
            let catalog = self.catalog.read();
            let blooms = self.blooms.read();
            let mut segs: Vec<SegmentCatalogEntry> = catalog
                .active_segments_for_measurement(measurement)
                .into_iter()
                .filter(|e| {
                    // Bloom filter pruning: skip segments that definitely
                    // don't contain this series key
                    blooms
                        .get(&e.segment_id.0)
                        .is_none_or(|bloom| bloom.may_contain(&key))
                })
                .cloned()
                .collect();
            // Sort by max_timestamp descending to check newest segments first
            segs.sort_by_key(|s| std::cmp::Reverse(s.max_timestamp));
            segs
        };

        // Track the best result across ALL segments. We cannot return after
        // the first hit because `entry.max_timestamp` is the max across ALL
        // series in the segment, not just the queried series. A segment with
        // a high overall max_timestamp might contain an old point for our
        // target series, while a later segment could have a newer one.
        let mut global_best_ts: Option<i64> = memtable_best.as_ref().map(Point::timestamp);
        let mut global_best_batch: Option<RecordBatch> = None;
        let mut global_best_idx: Option<usize> = None;

        for entry in &entries {
            // If this segment's max_timestamp can't beat our global best,
            // all remaining segments are also older (sorted desc) — stop.
            if let Some(best) = global_best_ts {
                if entry.max_timestamp <= best {
                    break;
                }
            }

            let reader = match SegmentReader::open(&entry.path) {
                Ok(r) => r,
                Err(e) => {
                    warn!(segment = %entry.path.display(), error = %e, "Skipping unreadable segment");
                    continue;
                }
            };

            let tag_refs: Vec<(&str, &str)> =
                tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

            // Read row groups in REVERSE order (newest first) and stop
            // as soon as we find a matching point — avoids reading the
            // entire segment file for the common case.
            let rg_count = reader.row_group_count();
            let mut seg_best_ts: Option<i64> = None;
            let mut seg_best_batch: Option<RecordBatch> = None;
            let mut seg_best_idx: Option<usize> = None;

            for rg_idx in (0..rg_count).rev() {
                let batch = reader.read_row_group(rg_idx)?;
                if batch.num_rows() == 0 {
                    continue;
                }

                let filtered = chronix_query::filter::filter_batch(&batch, 0, i64::MAX, &tag_refs)?;
                if filtered.num_rows() == 0 {
                    continue;
                }

                let filtered_ts = filtered
                    .column_by_name("timestamp")
                    .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>())
                    .ok_or_else(|| DbError::Internal("missing timestamp column".into()))?;

                // Find the newest timestamp in this row group that a
                // tombstone does not mask. Skipping masked rows here rather
                // than rejecting the series up front is what lets a ranged
                // delete leave the points outside its range answerable.
                let mut best: Option<(usize, i64)> = None;
                for i in 0..filtered_ts.len() {
                    let t = filtered_ts.value(i);
                    if best.is_some_and(|(_, b)| t <= b) || masked(t) {
                        continue;
                    }
                    best = Some((i, t));
                }
                let Some((local_max_idx, local_max_ts)) = best else {
                    continue;
                };

                match seg_best_ts {
                    Some(b) if local_max_ts <= b => {
                        // Earlier row groups within this segment can't beat
                        // this segment's best — stop scanning this segment.
                        break;
                    }
                    _ => {
                        seg_best_ts = Some(local_max_ts);
                        seg_best_batch = Some(filtered);
                        seg_best_idx = Some(local_max_idx);
                    }
                }
            }

            // Update global best if this segment has a newer point.
            if let Some(seg_ts) = seg_best_ts {
                if global_best_ts.is_none_or(|best| seg_ts > best) {
                    global_best_ts = Some(seg_ts);
                    global_best_batch = seg_best_batch;
                    global_best_idx = seg_best_idx;
                }
            }
        }

        // A segment row only replaces the memtable candidate if it is strictly
        // newer, which `global_best_ts` already enforced above.
        if let (Some(batch), Some(idx)) = (global_best_batch, global_best_idx) {
            return Self::row_to_point(&batch, idx, measurement, tags).map(Some);
        }

        Ok(memtable_best)
    }

    /// Extract a single row from a `RecordBatch` and reconstruct a `Point`.
    ///
    /// All type conversions are fallible — no `unwrap()` in library code.
    fn row_to_point(
        batch: &RecordBatch,
        row: usize,
        measurement: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<Point> {
        let schema = batch.schema();
        let ts_col = batch
            .column_by_name("timestamp")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>())
            .ok_or_else(|| DbError::Internal("missing timestamp".into()))?;
        let timestamp = ts_col.value(row);

        let key = SeriesKey::new(measurement.to_string(), tags.clone())
            .map_err(|e| DbError::Internal(format!("Invalid series key: {e}")))?;

        let mut fields = BTreeMap::new();
        for field_ref in schema.fields() {
            let name = field_ref.name();
            if name == "timestamp" || tags.contains_key(name.as_str()) {
                continue;
            }
            let Some(col) = batch.column_by_name(name) else {
                continue;
            };
            // Skip null cells — Arrow returns physical defaults (0, "", false)
            // for nulls, which would produce incorrect field values.
            if col.is_null(row) {
                continue;
            }
            let val = match field_ref.data_type() {
                arrow::datatypes::DataType::Float64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .ok_or_else(|| {
                            DbError::Internal(format!("column '{name}' is not Float64"))
                        })?;
                    chronix_core::FieldValue::F64(arr.value(row))
                }
                arrow::datatypes::DataType::Int64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| {
                            DbError::Internal(format!("column '{name}' is not Int64"))
                        })?;
                    chronix_core::FieldValue::I64(arr.value(row))
                }
                arrow::datatypes::DataType::UInt64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .ok_or_else(|| {
                            DbError::Internal(format!("column '{name}' is not UInt64"))
                        })?;
                    chronix_core::FieldValue::U64(arr.value(row))
                }
                arrow::datatypes::DataType::Boolean => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .ok_or_else(|| {
                            DbError::Internal(format!("column '{name}' is not Boolean"))
                        })?;
                    chronix_core::FieldValue::Bool(arr.value(row))
                }
                arrow::datatypes::DataType::Utf8 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| DbError::Internal(format!("column '{name}' is not Utf8")))?;
                    chronix_core::FieldValue::String(arr.value(row).to_string())
                }
                _ => continue,
            };
            fields.insert(name.clone(), val);
        }

        Point::new(key, fields, timestamp)
            .map_err(|e| DbError::Internal(format!("Failed to create point: {e}")))
    }

    /// Convert a `MeasurementSchema` to an Arrow `Schema`.
    ///
    /// If `projection` is non-empty, only the timestamp and projected fields
    /// (plus tags) are included.
    pub(super) fn measurement_to_arrow_schema(
        ms: &MeasurementSchema,
        projection: &[String],
    ) -> Arc<arrow::datatypes::Schema> {
        use arrow::datatypes::{DataType, Field};
        use chronix_core::schema::ColumnType;

        let fields: Vec<Field> = ms
            .columns()
            .iter()
            .filter(|col| {
                if projection.is_empty() {
                    return true;
                }
                col.name == "timestamp"
                    || col.name == "time"
                    || col.role == chronix_core::ColumnRole::Tag
                    || projection.iter().any(|p| p == &col.name)
            })
            .map(|col| {
                let dt = match col.column_type {
                    ColumnType::Timestamp | ColumnType::I64 => DataType::Int64,
                    ColumnType::U64 => DataType::UInt64,
                    ColumnType::F64 => DataType::Float64,
                    ColumnType::Bool => DataType::Boolean,
                    ColumnType::String => DataType::Utf8,
                };
                // Use "timestamp" as the canonical column name
                let name = if col.name == "time" {
                    "timestamp".to_string()
                } else {
                    col.name.clone()
                };
                Field::new(name, dt, true)
            })
            .collect();
        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    /// Collect record batches from memtable and on-disk segments.
    ///
    /// When `scan_columns` is `Some`, only those columns are read from
    /// on-disk segments (I/O-level column projection pushdown).
    /// The memtable batch is always included as-is because it is already
    /// in memory.
    ///
    /// When `max_bytes > 0`, an incremental memory budget is enforced:
    /// each rayon task stops producing batches once the aggregate memory
    /// exceeds the budget, preventing unbounded allocation before the
    /// post-collection check.
    ///
    /// # Predicate pushdown
    ///
    /// Both `time_range` and `tag_predicates` are forwarded to
    /// [`SegmentReader::read_projected_filtered_with_predicates`] so that
    /// predicate evaluation happens at **row-group granularity** inside
    /// each segment.  Row groups whose timestamp stats fall outside the
    /// query window, or whose tag columns are entirely NULL, are skipped
    /// before any column bytes are decompressed or decoded.
    #[allow(clippy::unused_self)]
    fn collect_batches(
        &self,
        memtable_batch: RecordBatch,
        entries: &[SegmentCatalogEntry],
        scan_columns: Option<&[String]>,
        time_range: Option<(i64, i64)>,
        tag_predicates: &[(&str, &str)],
        field_predicates: &[chronix_engine::segment::FieldPredicate],
        query_deadline: Option<std::time::Instant>,
    ) -> Result<Vec<RecordBatch>> {
        // Read segments in parallel using rayon — each segment's mmap +
        // decode is independent.  Latency is proportional to the slowest
        // segment rather than the sum of all segments.
        //
        // Mmap-based I/O relies on the kernel's virtual-memory
        // subsystem for read-ahead.  On Linux the default readahead
        // window (typically 128 KiB, tunable via
        // /sys/block/<dev>/queue/read_ahead_kb) handles sequential
        // segment scans efficiently.  Explicit `posix_fadvise(SEQUENTIAL)`
        // or `madvise(MADV_SEQUENTIAL)` could further hint the kernel
        // for large scans, but benchmarks show negligible gain because
        // the kernel's heuristic already detects sequential fault patterns
        // from the row-group iteration order.  Adding fadvise is a
        // potential future micro-optimisation for cold-cache workloads.
        use rayon::prelude::*;

        // Incremental memory budget: tracks running allocation total
        // across all rayon tasks.  Once exceeded, remaining segments
        // are skipped — the post-collection guard produces the error.
        let budget = self.config.max_query_result_bytes;
        let allocated = std::sync::atomic::AtomicUsize::new(0);
        let cancelled = std::sync::atomic::AtomicBool::new(false);

        let segment_batches: Vec<RecordBatch> = entries
            .par_iter()
            .filter_map(|entry| {
                // Check query deadline during segment I/O.
                if let Some(deadline) = query_deadline {
                    if std::time::Instant::now() > deadline {
                        cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
                        return None;
                    }
                }
                if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                    return None;
                }

                // Early exit: skip remaining segments once budget exceeded.
                if budget > 0 && allocated.load(std::sync::atomic::Ordering::Relaxed) > budget {
                    return None;
                }

                match SegmentReader::open(&entry.path) {
                    Ok(reader) => {
                        let batch = if let Some(cols) = scan_columns {
                            let col_refs: Vec<&str> = cols.iter().map(String::as_str).collect();
                            // Silently skip columns that don't exist in this
                            // particular segment (schema may have evolved).
                            let available: Vec<&str> = col_refs
                                .into_iter()
                                .filter(|c| reader.column_metadata().iter().any(|m| m.name == *c))
                                .collect();
                            if available.is_empty() {
                                return None;
                            }
                            // Push tag predicates down to row-group level.
                            reader.read_projected_with_zone_maps(
                                &available,
                                time_range,
                                tag_predicates,
                                field_predicates,
                            )
                        } else {
                            reader.read_projected_with_zone_maps(
                                &[],
                                time_range,
                                tag_predicates,
                                field_predicates,
                            )
                        };
                        match batch {
                            Ok(b) if b.num_rows() > 0 => {
                                // Zone maps prune whole row groups, never
                                // rows within one. Trimming here rather than
                                // after the merge keeps the surplus out of the
                                // k-way merge entirely.
                                let (start_ns, end_ns) = time_range.unwrap_or((i64::MIN, i64::MAX));
                                let b = match chronix_query::filter::filter_batch(
                                    &b,
                                    start_ns,
                                    end_ns,
                                    tag_predicates,
                                ) {
                                    Ok(trimmed) => trimmed,
                                    Err(e) => {
                                        warn!(
                                            segment = %entry.path.display(),
                                            error = %e,
                                            "Error filtering segment batch"
                                        );
                                        return None;
                                    }
                                };
                                if b.num_rows() == 0 {
                                    return None;
                                }
                                // Track memory incrementally.
                                if budget > 0 {
                                    allocated.fetch_add(
                                        b.get_array_memory_size(),
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                                Some(b)
                            }
                            Ok(_) => None,
                            Err(e) => {
                                warn!(
                                    segment = %entry.path.display(),
                                    error = %e,
                                    "Error reading segment"
                                );
                                None
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            segment = %entry.path.display(),
                            error = %e,
                            "Skipping unreadable segment"
                        );
                        None
                    }
                }
            })
            .collect();

        let mut batches = segment_batches;

        // Abort if query was cancelled during segment I/O.
        if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(DbError::QueryTimeout(self.config.query_timeout));
        }

        // Memtable batch appended last → highest write-order → wins dedup.
        if memtable_batch.num_rows() > 0 {
            batches.push(memtable_batch);
        }

        Ok(batches)
    }

    /// Shared segment pruning pipeline used by both `execute_inner`
    /// and `execute_stream` to avoid diverging logic.
    ///
    /// Collects entries under read locks, then releases locks
    /// before the bloom-filter scan pass. This narrows the lock scope so
    /// concurrent `flush()` operations are not blocked during pruning.
    pub(super) fn prune_segments(
        &self,
        measurement: &str,
        tag_filters: &[chronix_query::plan::TagFilter],
        time_range: &chronix_query::plan::TimeRange,
    ) -> (
        Vec<SegmentCatalogEntry>,
        chronix_query::pruning::PruningStats,
    ) {
        let (candidate_key, tag_keys) = Self::build_series_key(measurement, tag_filters);
        let tag_key_refs: Vec<&str> = tag_keys.iter().map(String::as_str).collect();

        // A series bloom holds **complete** series keys, so it may only be
        // consulted with a complete one. Filters that name a strict subset of
        // the measurement's tags describe a *set* of series, and the key built
        // from them matches none of them — so every segment was pruned and the
        // query returned nothing at all.
        //
        // That is the ordinary query shape: `WHERE host = 'x'` on a
        // measurement that also carries `region`. It stayed invisible because
        // every test filtered a measurement with exactly one tag, where a
        // one-tag filter *is* complete. On `chronixd` it was total — the
        // server injects a hidden `__namespace__` tag into every point and a
        // matching filter into every query, so no user query was ever
        // complete, and every one of them returned nothing once the data had
        // been flushed.
        //
        // Falling back to `None` costs selectivity and nothing else: bloom
        // pruning is an optimisation, and the filter still runs per row.
        let series_key = candidate_key.filter(|_| {
            self.schema(measurement).is_some_and(|ms| {
                ms.tag_names()
                    .iter()
                    .all(|t| tag_filters.iter().any(|f| f.key == *t))
            })
        });

        let tag_filter_pairs: Vec<(&str, &str)> = tag_filters
            .iter()
            .map(|f| (f.key.as_str(), f.value.as_str()))
            .collect();
        let index_seg_ids = if tag_filter_pairs.is_empty() {
            None
        } else {
            Some(self.tag_index.segments_for_tags(&tag_filter_pairs))
        };

        // Phase 1: Collect time+index-filtered entries under catalog read lock.
        let (time_filtered, segments_total) = {
            let catalog = self.catalog.read();
            let all_measurement: Vec<&SegmentCatalogEntry> = catalog
                .active_segments_for_measurement(measurement)
                .into_iter()
                .collect();
            let total = all_measurement.len();
            let filtered: Vec<SegmentCatalogEntry> = all_measurement
                .into_iter()
                .filter(|e| {
                    e.max_timestamp >= time_range.start && e.min_timestamp <= time_range.end
                })
                .filter(|e| {
                    index_seg_ids
                        .as_ref()
                        .is_none_or(|ids| ids.contains(&e.segment_id))
                })
                .cloned()
                .collect();
            (filtered, total)
            // catalog read lock dropped here
        };

        let pruned_by_time_and_index = segments_total - time_filtered.len();

        if pruned_by_time_and_index > 0 {
            metrics::counter!("chronix_segments_pruned_by_time_and_index_total")
                .increment(pruned_by_time_and_index as u64);
        }

        // Phase 2: Bloom-filter pruning under blooms read lock (catalog lock released).
        let pruned = {
            let blooms = self.blooms.read();
            let refs: Vec<&SegmentCatalogEntry> = time_filtered.iter().collect();
            chronix_query::pruning::prune_entries(
                refs,
                |seg_id| blooms.get(&seg_id),
                series_key.as_ref(),
                &tag_key_refs,
            )
            // blooms read lock dropped here
        };

        let mut stats = pruned.stats;
        stats.segments_total = segments_total;
        stats.pruned_by_time = pruned_by_time_and_index;

        let entries: Vec<SegmentCatalogEntry> =
            pruned.segments.into_iter().map(|ps| ps.entry).collect();
        (entries, stats)
    }

    /// Build a canonical series key and tag filter keys from tag filters.
    ///
    /// Returns `(Option<SeriesKey>, Vec<String>)` where the series key is
    /// `None` if no tag filters are specified, and the tag keys are the
    /// column names used in equality filters (for stats pruning).
    fn build_series_key(
        measurement: &str,
        tag_filters: &[chronix_query::plan::TagFilter],
    ) -> (Option<SeriesKey>, Vec<String>) {
        if tag_filters.is_empty() {
            return (None, Vec::new());
        }
        let tags: BTreeMap<String, String> = tag_filters
            .iter()
            .map(|f| (f.key.clone(), f.value.clone()))
            .collect();
        let tag_keys: Vec<String> = tag_filters.iter().map(|f| f.key.clone()).collect();
        let series_key = SeriesKey::new(measurement.to_string(), tags).ok();
        (series_key, tag_keys)
    }

    /// Extract unique series keys from a `RecordBatch` by inspecting tag columns.
    ///
    /// Identifies tag columns via Arrow field metadata (`role=tag`) or falls
    /// back to all `Utf8` columns except `timestamp`. Returns deduplicated
    /// `SeriesKey` instances.
    pub(super) fn extract_series_keys_from_batch(
        batch: &arrow::array::RecordBatch,
        measurement: &str,
    ) -> Vec<SeriesKey> {
        use arrow::array::StringArray;
        use std::collections::BTreeMap as StdBTreeMap;

        let schema = batch.schema();
        // Identify tag columns: fields with metadata role=tag, or all Utf8 except timestamp
        let tag_cols: Vec<(usize, &str)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                if f.name() == "timestamp" {
                    return false;
                }
                if let Some(meta) = f.metadata().get("role") {
                    return meta == "tag";
                }
                f.data_type() == &arrow::datatypes::DataType::Utf8
            })
            .map(|(i, f)| (i, f.name().as_str()))
            .collect();

        let mut seen = std::collections::HashSet::new();
        let mut keys = Vec::new();

        for row in 0..batch.num_rows() {
            let mut tags = StdBTreeMap::new();
            for &(col_idx, col_name) in &tag_cols {
                if let Some(arr) = batch.column(col_idx).as_any().downcast_ref::<StringArray>() {
                    if !arr.is_null(row) {
                        tags.insert(col_name.to_string(), arr.value(row).to_string());
                    }
                }
            }
            if let Ok(sk) = SeriesKey::new(measurement.to_string(), tags) {
                let canonical = sk.canonical_form().to_string();
                if seen.insert(canonical) {
                    keys.push(sk);
                }
            }
        }

        keys
    }

    /// Compute the minimal set of columns needed from segments.
    ///
    /// Returns `None` when all columns should be read (no projection),
    /// or `Some(columns)` listing exactly which columns the segment
    /// reader should decode. This pushes projection down to the I/O layer
    /// so that unused column data is never decompressed or decoded.
    pub(super) fn compute_scan_columns(
        plan: &QueryPlan,
        tag_filters: &[chronix_query::plan::TagFilter],
        projection: &[String],
    ) -> Option<Vec<String>> {
        if projection.is_empty() {
            return None; // No projection specified — read everything
        }

        let mut columns: Vec<String> = Vec::new();

        // Always need timestamp for dedup / filter / downsample
        columns.push("timestamp".to_string());

        // Need tag columns for filtering
        for tf in tag_filters {
            if !columns.iter().any(|c| c == &tf.key) {
                columns.push(tf.key.clone());
            }
        }

        // Need group-by columns for aggregation
        if let QueryPlan::Aggregate { group_by, .. } = plan {
            for g in group_by {
                if !columns.iter().any(|c| c == g) {
                    columns.push(g.clone());
                }
            }
        }

        // Need projected fields
        for f in projection {
            if !columns.contains(f) {
                columns.push(f.clone());
            }
        }

        Some(columns)
    }

    /// Apply aggregation or downsampling post-processing to the projected batch.
    ///
    /// When a `memory_tracker` is provided, the output batch memory is charged
    /// against the per-query budget, failing the query if the budget is exceeded.
    fn apply_post_processing(
        plan: &QueryPlan,
        batch: RecordBatch,
        tag_filters: &[chronix_query::plan::TagFilter],
        memory_tracker: Option<&chronix_query::MemoryTracker>,
    ) -> Result<RecordBatch> {
        let result = match plan {
            QueryPlan::Aggregate {
                functions,
                group_by,
                estimated_cardinality,
                ..
            } => {
                let schema = batch.schema();
                let field_cols: Vec<&str> = schema
                    .fields()
                    .iter()
                    .filter(|f| {
                        f.name() != "timestamp"
                            && !tag_filters.iter().any(|tf| tf.key == *f.name())
                            && !group_by.iter().any(|g| g == f.name())
                    })
                    .map(|f| f.name().as_str())
                    .collect();
                let group_refs: Vec<&str> = group_by.iter().map(String::as_str).collect();

                if group_refs.is_empty() {
                    chronix_query::aggregate::aggregate_batch(
                        &batch,
                        functions,
                        &field_cols,
                        memory_tracker,
                    )?
                } else {
                    // Choose aggregation strategy based on cardinality.
                    let strategy =
                        chronix_query::aggregate::choose_strategy(*estimated_cardinality);
                    match strategy {
                        chronix_query::aggregate::AggregationStrategy::Sort => {
                            chronix_query::aggregate::aggregate_sorted(
                                &batch,
                                functions,
                                &field_cols,
                                &group_refs,
                                memory_tracker,
                            )?
                        }
                        chronix_query::aggregate::AggregationStrategy::Hash => {
                            chronix_query::aggregate::aggregate_grouped(
                                &batch,
                                functions,
                                &field_cols,
                                &group_refs,
                                memory_tracker,
                            )?
                        }
                    }
                }
            }
            QueryPlan::Downsample {
                interval, function, ..
            } => {
                let schema = batch.schema();
                let value_col = schema
                    .fields()
                    .iter()
                    .find(|f| {
                        f.name() != "timestamp"
                            && matches!(
                                f.data_type(),
                                arrow::datatypes::DataType::Float64
                                    | arrow::datatypes::DataType::Int64
                                    | arrow::datatypes::DataType::UInt64
                            )
                    })
                    .map_or("value", |f| f.name().as_str());

                let interval_ns = i64::try_from(interval.as_nanos()).unwrap_or(i64::MAX);
                chronix_query::downsample::downsample(
                    &batch,
                    value_col,
                    interval_ns,
                    function,
                    memory_tracker,
                )?
            }
            QueryPlan::Scan { .. } => batch,
            QueryPlan::Window {
                source,
                functions,
                partition_by,
                value_column,
            } => {
                // First apply the inner plan's post-processing
                let inner =
                    Self::apply_post_processing(source, batch, tag_filters, memory_tracker)?;
                chronix_query::window::apply_window(
                    &inner,
                    functions,
                    partition_by,
                    value_column,
                    memory_tracker,
                )?
            }
            QueryPlan::Limit {
                source,
                limit,
                offset,
            } => {
                // First apply the inner plan's post-processing
                let inner =
                    Self::apply_post_processing(source, batch, tag_filters, memory_tracker)?;
                let total = inner.num_rows();
                let start = (*offset).min(total);
                let len = (*limit).min(total.saturating_sub(start));
                if start == 0 && len == total {
                    inner
                } else {
                    inner.slice(start, len)
                }
            }
        };

        // Charge output batch memory against the per-query tracker.
        if let Some(tracker) = memory_tracker {
            tracker.try_allocate(result.get_array_memory_size())?;
        }

        Ok(result)
    }

    /// Check whether the LVC is enabled for a given measurement.
    pub(super) fn lvc_enabled_for(&self, measurement: &str) -> bool {
        if !self.config.enable_last_value_cache {
            return false;
        }
        match &self.config.lvc_measurements {
            None => true,
            Some(set) => set.contains(measurement),
        }
    }
}
