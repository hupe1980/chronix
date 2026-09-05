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
use chronix_query::plan::{extract_scan, QueryPlan};
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

/// The Arrow field-metadata key that carries a column's role.
///
/// Read by `chronixd`'s HTTP, gRPC and Flight SQL result encoders, and by the
/// series-key extractor. Public so an embedded caller reading batches from
/// `execute_iter` can tell a tag from a string field without guessing.
pub const ROLE_KEY: &str = "role";

/// `role` metadata for one column.
#[must_use]
pub fn role_metadata(role: chronix_core::ColumnRole) -> std::collections::HashMap<String, String> {
    [(ROLE_KEY.to_string(), role.to_string())].into()
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

    /// Execute a query plan and return the result as one Arrow
    /// [`RecordBatch`].
    ///
    /// Every plan runs on the same engine: the scan at its root is read by
    /// [`execute_iter`](Self::execute_iter) — one time-disjoint bucket of
    /// segments at a time, deduplicated last-write-wins, tombstones and
    /// projection applied per source — and the nodes above it fold over
    /// that stream. Aggregates and downsamples fold incrementally; a limit
    /// stops the scan; a window function sees its whole input, so it is the
    /// one shape that materialises the scan first.
    ///
    /// One read path, so nothing can disagree about last-write-wins, the
    /// time range or tombstones; `execute` is a convenience over it.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, the plan is invalid, a
    /// segment cannot be read, the query exceeds its timeout or memory
    /// budget, or the collected result exceeds `max_query_result_bytes`.
    #[must_use = "query errors must not be silently ignored"]
    pub fn execute(&self, plan: &QueryPlan) -> Result<RecordBatch> {
        Ok(self.execute_inner(plan)?.0)
    }

    /// [`execute`](Self::execute), returning the
    /// [`PruningStats`](chronix_query::pruning::PruningStats) that describe
    /// how many segments the pruning pipeline eliminated.
    pub fn execute_with_stats(
        &self,
        plan: &QueryPlan,
    ) -> Result<(RecordBatch, chronix_query::pruning::PruningStats)> {
        self.execute_inner(plan)
    }

    /// Execute a query plan as a sequence of [`RecordBatch`]es.
    ///
    /// `Scan`, `Limit(Scan)`, `Aggregate(Scan)` and `Downsample(Scan)` are
    /// streamed off [`execute_iter`](Self::execute_iter) and collected here;
    /// the collection is bounded by `max_query_result_bytes`. Other plan
    /// shapes return a single batch, as [`execute`](Self::execute) would.
    /// Prefer [`execute_iter`](Self::execute_iter) when the consumer can
    /// process batches as they arrive — it never accumulates.
    ///
    /// # Errors
    ///
    /// As [`execute`](Self::execute).
    #[must_use = "query errors must not be silently ignored"]
    pub fn execute_stream(&self, plan: &QueryPlan) -> Result<Vec<RecordBatch>> {
        Ok(self.execute_stream_inner(plan)?.0)
    }

    fn execute_inner(
        &self,
        plan: &QueryPlan,
    ) -> Result<(RecordBatch, chronix_query::pruning::PruningStats)> {
        let (batches, stats) = self.execute_stream_inner(plan)?;
        let batch = if batches.is_empty() {
            let arrow_schema = extract_scan(plan)
                .and_then(|(m, _, proj, _)| {
                    self.schema(m)
                        .map(|ms| Self::measurement_to_arrow_schema(&ms, proj))
                })
                .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
            RecordBatch::new_empty(arrow_schema)
        } else if batches.len() == 1 {
            batches.into_iter().next().expect("checked len")
        } else {
            arrow::compute::concat_batches(&batches[0].schema(), &batches)
                .map_err(|e| DbError::Internal(e.to_string()))?
        };
        Ok((batch, stats))
    }

    /// The one read path. Everything is a fold over `execute_iter`.
    #[allow(clippy::too_many_lines)]
    fn execute_stream_inner(
        &self,
        plan: &QueryPlan,
    ) -> Result<(Vec<RecordBatch>, chronix_query::pruning::PruningStats)> {
        let start = std::time::Instant::now();
        self.check_open()?;
        let query_timeout = self.config.query_timeout;
        let deadline_passed = || !query_timeout.is_zero() && start.elapsed() > query_timeout;
        let finish = |batches: Vec<RecordBatch>,
                      stats: chronix_query::pruning::PruningStats|
         -> Result<(Vec<RecordBatch>, chronix_query::pruning::PruningStats)> {
            if deadline_passed() {
                return Err(DbError::QueryTimeout(query_timeout));
            }
            histogram!("chronix_query_duration_seconds").record(start.elapsed().as_secs_f64());
            Ok((
                batches.into_iter().filter(|b| b.num_rows() > 0).collect(),
                stats,
            ))
        };

        let mem_limit = self.config.per_query_memory_limit;
        let memory_tracker = (mem_limit > 0).then(|| chronix_query::MemoryTracker::new(mem_limit));
        let tracker = memory_tracker.as_ref();

        match plan {
            // ── Aggregate(Scan): fold batch by batch, O(groups × fields) ──
            QueryPlan::Aggregate {
                source,
                functions,
                group_by,
                ..
            } if matches!(source.as_ref(), QueryPlan::Scan { .. }) => {
                let scan = Self::scan_for(plan);
                let (_, tag_filters, _, _) = extract_scan(&scan)
                    .ok_or_else(|| DbError::Internal("invalid query plan: no scan node".into()))?;
                let mut stream = self.execute_iter(&scan)?;
                let stats = stream.pruning_stats();
                let Some(first) = stream.next().transpose()? else {
                    return finish(vec![], stats);
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
                    if deadline_passed() {
                        return Err(DbError::QueryTimeout(query_timeout));
                    }
                }
                finish(vec![agg.finish(tracker)?], stats)
            }

            // ── Downsample(Scan): one open bucket carried across batches ──
            QueryPlan::Downsample {
                source,
                interval,
                function,
            } if matches!(source.as_ref(), QueryPlan::Scan { .. }) => {
                let mut stream = self.execute_iter(source)?;
                let stats = stream.pruning_stats();
                let Some(first) = stream.next().transpose()? else {
                    return finish(vec![], stats);
                };
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
                    .map_or("value", |f| f.name().as_str());
                let interval_ns = i64::try_from(interval.as_nanos()).unwrap_or(i64::MAX);
                let mut ds = chronix_query::downsample::StreamingDownsampler::new(
                    value_col,
                    interval_ns,
                    *function,
                )?;
                let mut results = ds.push(&first)?;
                drop(first);
                for batch in stream {
                    results.extend(ds.push(&batch?)?);
                    if deadline_passed() {
                        return Err(DbError::QueryTimeout(query_timeout));
                    }
                }
                results.extend(ds.finish()?);
                finish(results, stats)
            }

            // ── Scan and Limit(Scan): the stream itself, collected ────────
            QueryPlan::Scan { .. } => {
                let stream = self
                    .execute_iter(plan)?
                    .with_max_total_bytes(self.config.max_query_result_bytes);
                let stats = stream.pruning_stats();
                let batches = stream.collect::<Result<Vec<_>>>()?;
                finish(batches, stats)
            }
            QueryPlan::Limit { source, .. }
                if matches!(source.as_ref(), QueryPlan::Scan { .. }) =>
            {
                let stream = self
                    .execute_iter(plan)?
                    .with_max_total_bytes(self.config.max_query_result_bytes);
                let stats = stream.pruning_stats();
                let batches = stream.collect::<Result<Vec<_>>>()?;
                finish(batches, stats)
            }

            // ── Everything else materialises its scan, then folds ────────
            _ => {
                let scan = Self::scan_for(plan);
                let (_, tag_filters, _, _) = extract_scan(&scan)
                    .ok_or_else(|| DbError::Internal("invalid query plan: no scan node".into()))?;
                let stream = self
                    .execute_iter(&scan)?
                    .with_max_total_bytes(self.config.max_query_result_bytes);
                let stats = stream.pruning_stats();
                let batches = stream.collect::<Result<Vec<_>>>()?;
                if batches.is_empty() {
                    return finish(vec![], stats);
                }
                let input = arrow::compute::concat_batches(&batches[0].schema(), &batches)
                    .map_err(|e| DbError::Internal(e.to_string()))?;
                drop(batches);
                if let Some(t) = tracker {
                    t.try_allocate(input.get_array_memory_size())?;
                }
                let result = Self::apply_post_processing(plan, input, tag_filters, tracker)?;
                finish(vec![result], stats)
            }
        }
    }

    /// The `Scan` at the root of `plan`, with its projection widened to the
    /// columns the nodes above it need — group-by keys, window partition
    /// keys and value columns — so a projected scan can still be folded.
    fn scan_for(plan: &QueryPlan) -> QueryPlan {
        let mut needed: Vec<String> = Vec::new();
        let mut node = plan;
        let scan = loop {
            match node {
                QueryPlan::Scan { .. } => break node,
                QueryPlan::Aggregate {
                    source, group_by, ..
                } => {
                    needed.extend(group_by.iter().cloned());
                    node = source;
                }
                QueryPlan::Window {
                    source,
                    partition_by,
                    value_column,
                    ..
                } => {
                    needed.extend(partition_by.iter().cloned());
                    needed.push(value_column.clone());
                    node = source;
                }
                QueryPlan::Downsample { source, .. } | QueryPlan::Limit { source, .. } => {
                    node = source;
                }
            }
        };
        let mut scan = scan.clone();
        if let QueryPlan::Scan { projection, .. } = &mut scan {
            if !projection.is_empty() {
                for col in needed {
                    if !projection.contains(&col) {
                        projection.push(col);
                    }
                }
            }
        }
        scan
    }

    /// Read a single segment, applying filter/tombstone/projection.
    /// Returns `None` if the segment is empty after filtering.
    ///
    pub(super) fn read_segment_filtered(
        entry: &SegmentCatalogEntry,
        ctx: &SegmentFilterCtx<'_>,
    ) -> Result<Option<RecordBatch>> {
        // A segment that cannot be opened fails the query. Skipping it with
        // a warning returned a confidently incomplete answer — and let
        // retention roll up a shard from whatever fraction of it was
        // readable and then drop the rest.
        let reader = SegmentReader::open(&entry.path)?;

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
            // Zone-map field predicate pushdown.
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
            Some(entry.segment_id.0),
        )?;

        if filtered.num_rows() > 0 {
            Ok(Some(filtered))
        } else {
            Ok(None)
        }
    }

    /// Rebuild the in-memory indexes from the catalog and the per-segment
    /// series sidecars: per-shard time indexes, series blooms, the inverted
    /// tag index, and the exact set of series the segments hold.
    ///
    /// No segment is decoded on this path. A segment whose sidecar is
    /// missing or corrupt has its series list rebuilt from its tag columns
    /// once, and the sidecar rewritten.
    pub(super) fn load_catalog_state(
        catalog: &SegmentCatalog,
    ) -> (
        BTreeMap<ShardId, TimeIndex>,
        BTreeMap<u64, SeriesBloomFilter>,
        TagInvertedIndex,
        std::collections::HashSet<String>,
    ) {
        use chronix_engine::index::series_index;

        let mut time_indices: BTreeMap<ShardId, TimeIndex> = BTreeMap::new();
        let mut blooms: BTreeMap<u64, SeriesBloomFilter> = BTreeMap::new();
        let tag_index = TagInvertedIndex::new();
        let mut known: std::collections::HashSet<String> = std::collections::HashSet::new();

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

            let keys = match series_index::SegmentStamp::read(&entry.path)
                .and_then(|stamp| series_index::read(&entry.path, stamp))
            {
                Ok(keys) => keys,
                Err(e) => {
                    warn!(
                        segment = %entry.path.display(),
                        error = %e,
                        "series index unreadable — rebuilding it from the segment"
                    );
                    match Self::rebuild_series_index(&entry.path, &entry.measurement) {
                        Some(keys) => keys,
                        None => continue,
                    }
                }
            };
            Self::index_series_keys(&mut blooms, &tag_index, entry.segment_id, &keys);
            for key in &keys {
                known.insert(key.canonical_form().to_string());
            }
        }

        (time_indices, blooms, tag_index, known)
    }

    /// Register a segment's series in the bloom map and the tag index.
    pub(super) fn index_series_keys(
        blooms: &mut BTreeMap<u64, SeriesBloomFilter>,
        tag_index: &TagInvertedIndex,
        segment_id: chronix_core::SegmentId,
        keys: &[SeriesKey],
    ) {
        use chronix_engine::index::series_index;
        if let Some(bloom) = series_index::bloom(keys) {
            blooms.insert(segment_id.0, bloom);
        }
        let pairs = series_index::tag_pairs(keys);
        if !pairs.is_empty() {
            tag_index.add_segment(segment_id, &pairs);
        }
    }

    /// Recover a segment's series list from its tag columns and rewrite the
    /// sidecar. Returns `None` if the segment itself cannot be read.
    fn rebuild_series_index(path: &std::path::Path, measurement: &str) -> Option<Vec<SeriesKey>> {
        use chronix_engine::index::series_index;
        use chronix_engine::segment::metadata::roles;

        let reader = match SegmentReader::open(path) {
            Ok(r) => r,
            Err(e) => {
                warn!(segment = %path.display(), error = %e, "Could not open segment to rebuild its series index");
                return None;
            }
        };
        let tag_cols: Vec<&str> = reader
            .column_metadata()
            .iter()
            .filter(|m| m.role == roles::TAG)
            .map(|m| m.name.as_str())
            .collect();
        let batch = match reader.read_projected(&tag_cols) {
            Ok(b) => b,
            Err(e) => {
                warn!(segment = %path.display(), error = %e, "Could not read tag columns to rebuild the series index");
                return None;
            }
        };
        let keys = Self::extract_series_keys_from_batch(&batch, measurement);
        let stamp = series_index::SegmentStamp::of(reader.header());
        if let Err(e) = series_index::write(path, &keys, stamp) {
            warn!(segment = %path.display(), error = %e, "Could not rewrite the series index");
        }
        Some(keys)
    }

    /// Apply tombstone filtering and column projection to a batch.
    ///
    /// `segment` is the segment the batch was read from; `None` means the
    /// memtable, whose rows postdate every tombstone and are never masked.
    pub(super) fn apply_projection_and_tombstone(
        batch: &RecordBatch,
        measurement: &str,
        projection: &[String],
        plan: &QueryPlan,
        tombstones: &TombstoneSet,
        tag_col_names: Option<&[&str]>,
        segment: Option<u64>,
    ) -> Result<RecordBatch> {
        // Tombstone filtering
        let filtered = match segment {
            Some(id) if !tombstones.is_empty() => chronix_query::filter::filter_tombstoned(
                batch,
                measurement,
                tombstones,
                tag_col_names,
                id,
            )?,
            _ => batch.clone(),
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
        // Segment rows only: the memtable's points postdate every tombstone.
        let masked = |ts: i64, segment: u64| {
            self.tombstones
                .read()
                .is_tombstoned_in(&canonical, ts, segment)
        };

        // 0. Check last-value cache (fastest path). The delete path evicts the
        // cache for every series it tombstones, so a hit here is live — but a
        // ranged delete leaves neighbouring points cached, so the entry is
        // still checked against the tombstone rather than trusted.
        if let Some(cached) = self.lvc.get_by_key(&key) {
            if !self
                .tombstones
                .read()
                .covers(&canonical, cached.timestamp())
            {
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
        let memtable_best = self.shards.scan(&key, 0, i64::MAX).into_iter().last();

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
                    if best.is_some_and(|(_, b)| t <= b) || masked(t, entry.segment_id.0) {
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
    /// Rewrite a scan batch's schema so every column says what it *is*.
    ///
    /// The producers — the memtable's builder, the segment reader, the
    /// compaction writer — build an Arrow schema from a column's *type*, and
    /// a role is not a type: a tag and a string field are both `Utf8`. The
    /// role was known at write time, dropped here, and then guessed again by
    /// four consumers with three different guesses. The HTTP and gRPC query
    /// APIs guessed "not tag", so every tag came back as a field and `tags`
    /// was always empty; Flight SQL guessed "not field", so a string *field*
    /// came back as a tag; and the series-key extractor guessed the same way,
    /// which puts a string field into a `SeriesKey`.
    ///
    /// So the role is stamped once, here, from the registry that owns it, and
    /// the consumers read it. It costs a `Schema` per batch — the column
    /// arrays are shared, not copied — and it makes the batch self-describing
    /// for an external Flight SQL client too.
    pub(super) fn stamp_roles(&self, measurement: &str, batch: RecordBatch) -> RecordBatch {
        let Some(ms) = self.schema(measurement) else {
            return batch;
        };
        let schema = batch.schema();
        if schema
            .fields()
            .iter()
            .all(|f| f.metadata().contains_key(ROLE_KEY))
        {
            return batch;
        }
        let fields: Vec<arrow::datatypes::Field> = schema
            .fields()
            .iter()
            .map(|f| {
                let role = if f.name() == "timestamp" || f.name() == "time" || f.name() == "_time" {
                    Some(chronix_core::ColumnRole::Timestamp)
                } else {
                    // A column the registry does not know is one a query node
                    // computed — an aggregate's output, say. It is a value,
                    // which is what `Field` means here.
                    Some(
                        ms.column(f.name())
                            .map_or(chronix_core::ColumnRole::Field, |c| c.role),
                    )
                };
                match role {
                    None => f.as_ref().clone(),
                    Some(role) => f.as_ref().clone().with_metadata(role_metadata(role)),
                }
            })
            .collect();
        let stamped = Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(stamped, batch.columns().to_vec()).unwrap_or(batch)
    }

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
                Field::new(name, dt, true).with_metadata(role_metadata(col.role))
            })
            .collect();
        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    /// The segment pruning pipeline — time index, inverted tag index, then
    /// series blooms — shared by every scan.
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
        // Tag columns say so. The fallback that stood here — "any `Utf8`
        // column that is not the timestamp" — put a **string field** into the
        // series key, because a tag and a string field are the same Arrow
        // type. `stamp_roles` puts the answer on the batch; a column that
        // still carries no role is one a query node computed, and a computed
        // column is not part of a series key either.
        let tag_cols: Vec<(usize, &str)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.metadata().get(super::ROLE_KEY).map(String::as_str) == Some("tag"))
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
