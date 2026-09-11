//! Bounded-memory streaming scan execution.
//!
//! # Why this exists
//!
//! [`Chronix::execute_stream`](super::Chronix::execute_stream) returns a
//! `Vec<RecordBatch>`: every surviving segment is read, then merged. Peak
//! memory is therefore `O(total surviving rows)` even though the *output* is
//! chunked. Exporting a 7-day window of 50 series at 1 Hz — ~30M points —
//! needs gigabytes before the first byte is written, which a 512 MB gateway
//! does not have.
//!
//! # The bound
//!
//! Segments are written **series-major** (tags in cardinality order, then
//! timestamp) because that is what makes the columnar encodings compress. A
//! segment's row groups are therefore *not* time-ordered, so a lazy
//! row-group-level time merge is not possible without re-sorting.
//!
//! What *is* available is that a time-partitioned LSM produces segments whose
//! `[min_timestamp, max_timestamp]` ranges rarely overlap: writes are routed
//! to 1-hour shards, and TWCS compacts within a shard. So instead of merging
//! every surviving segment at once, [`BatchStream`] sweeps the catalog entries
//! into **time-disjoint buckets** and merges one bucket at a time. Because the
//! buckets are disjoint and processed in ascending time order, concatenating
//! their output yields exactly the same globally sorted, deduplicated sequence
//! as a single k-way merge would.
//!
//! Peak memory becomes `O(rows in the largest bucket)` — one shard's worth,
//! ~180K points for the gateway workload above — instead of `O(total rows)`.
//! The bucketing is derived from the catalog rather than assumed from shard
//! semantics, so a pathologically overlapping set of segments degrades to a
//! single bucket (today's behaviour) rather than to a wrong answer.
//!
//! # Ordering
//!
//! Deduplication is last-write-wins by input position, so within a bucket the
//! sources are ordered oldest-first: catalog order for segments, then the
//! memtable slice last. The memtable holds the newest writes — a point that
//! was flushed and then overwritten must read back as the overwrite.

use std::collections::VecDeque;

use arrow::array::{Array, Int64Array, RecordBatch};
use chronix_engine::index::SegmentCatalogEntry;
use chronix_engine::segment::FieldPredicate;
use chronix_query::plan::{
    extract_field_predicates, extract_scan, QueryPlan, TagFilter, TimeRange,
};

use crate::error::{DbError, Result};

use super::query::SegmentFilterCtx;
use super::Chronix;

/// Rows per emitted `RecordBatch`.
pub(super) const CHUNK_ROWS: usize = 65_536;

/// Read segments in parallel once a bucket holds at least this many.
const PARALLEL_THRESHOLD: usize = 4;

/// One time-disjoint unit of work: the segments and memtable rows whose
/// timestamps fall in a single range that no other bucket covers.
struct Bucket {
    /// Segments in catalog order (oldest first) for last-write-wins.
    segments: Vec<SegmentCatalogEntry>,
    /// Memtable rows in this bucket's range, or `None`.
    memtable: Option<RecordBatch>,
}

/// A lazily evaluated, time-ordered, deduplicated stream of query results.
///
/// Produced by [`Chronix::execute_iter`]. Each `next()` yields at most
/// `CHUNK_ROWS` rows. Segment I/O for a bucket happens on the `next()` call
/// that first needs it, so a consumer that stops early — a `LIMIT`, a size-
/// bounded export — never pays for the buckets it did not reach.
///
/// The iterator borrows the database; it cannot outlive the [`Chronix`]
/// handle it was created from.
pub struct BatchStream<'a> {
    db: &'a Chronix,
    plan: QueryPlan,
    measurement: String,
    projection: Vec<String>,
    tag_filters: Vec<TagFilter>,
    time_range: TimeRange,
    field_predicates: Vec<FieldPredicate>,
    scan_columns: Option<Vec<String>>,
    tag_col_names: Option<Vec<String>>,
    buckets: VecDeque<Bucket>,
    pending: VecDeque<RecordBatch>,
    /// Cumulative byte budget across everything yielded, or 0 for unlimited.
    ///
    /// A consumer that *collects* needs the guard; one that processes and
    /// drops each batch does not, which is the whole point of streaming.
    max_total_bytes: usize,
    emitted_bytes: usize,
    /// `LIMIT` rows still to emit, when the plan carried one.
    ///
    /// A limit is the one plan node above a scan that does *not* need to see
    /// its whole input — it needs to see less of it — so it belongs here
    /// rather than in the collecting path. Applying it inside the stream is
    /// also what makes it terminate early: the remaining buckets are never
    /// materialised.
    remaining: Option<usize>,
    /// `OFFSET` rows still to skip.
    to_skip: usize,
    /// Set after an error so the iterator stops rather than retrying.
    done: bool,
    /// When this scan must stop, and the budget it was given.
    ///
    /// **The deadline belongs to the iterator, not to the fold above it.**
    /// `query_timeout` used to be checked only by `execute_stream_inner`,
    /// once the whole scan had already been collected — so the work was done
    /// and *then* the caller was told it had run out of time — and the
    /// streaming handlers, `/api/v1/chronix/query` among them, did not check
    /// it at all. Checking here, before each bucket's segment I/O, is what
    /// makes a deadline stop the reading rather than describe it.
    deadline: Option<(std::time::Instant, std::time::Duration)>,
    /// What segment pruning did for this scan.
    pruning_stats: chronix_query::pruning::PruningStats,
    /// Keeps the snapshot's segment files on disk until the stream is done
    /// with them — see `db::leases`. Never read; released on drop,
    /// including when a `LIMIT` abandons the remaining buckets.
    _segment_lease: super::leases::SegmentLease<'a>,
}

impl BatchStream<'_> {
    /// Apply a cumulative byte budget across all yielded batches.
    ///
    /// Once the total exceeds `max_bytes` the stream yields an error and
    /// stops. `0` disables the check. Used by
    /// [`Chronix::execute_stream`](super::Chronix::execute_stream), which
    /// collects everything and so must honour
    /// [`ChronixConfig::max_query_result_bytes`](chronix_core::ChronixConfig::max_query_result_bytes).
    #[must_use]
    pub fn with_max_total_bytes(mut self, max_bytes: usize) -> Self {
        self.max_total_bytes = max_bytes;
        self
    }

    /// Drop the deadline `config.query_timeout` gave this scan.
    ///
    /// **For work nobody is waiting on.** A query deadline exists so that a
    /// caller's request cannot cost the server more than the caller's
    /// patience; a maintenance pass has no caller and no patience. Rollup
    /// materialisation catching a gateway up after a week offline, the cold
    /// tier writing a segment window to object storage, and `export_parquet`
    /// over a window larger than RAM are all *supposed* to take longer than
    /// a request would — and a `QueryTimeout` in a maintenance pass is worse
    /// than slow, because the pass retries on the next interval and fails
    /// again for ever.
    ///
    /// Naming the exception here keeps the default the safe way round: a scan
    /// is bounded unless somebody said why it should not be.
    #[must_use]
    pub fn without_deadline(mut self) -> Self {
        self.deadline = None;
        self
    }

    /// Restrict the stream to `limit` rows after skipping `offset`.
    #[must_use]
    fn with_limit(mut self, limit: usize, offset: usize) -> Self {
        self.remaining = Some(limit);
        self.to_skip = offset;
        self
    }

    /// Apply the pending offset and limit to one batch.
    ///
    /// Returns `None` when the batch is entirely skipped or the limit is
    /// already spent, in which case the caller moves on to the next batch.
    fn clip(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        let Some(remaining) = self.remaining.as_mut() else {
            return Some(batch);
        };
        let rows = batch.num_rows();
        if self.to_skip >= rows {
            self.to_skip -= rows;
            return None;
        }
        let start = self.to_skip;
        self.to_skip = 0;
        let take = (rows - start).min(*remaining);
        *remaining -= take;
        if take == 0 {
            self.done = true;
            return None;
        }
        Some(batch.slice(start, take))
    }

    /// How many segments the pruning pipeline considered and eliminated
    /// before the first bucket was read.
    #[must_use]
    pub fn pruning_stats(&self) -> chronix_query::pruning::PruningStats {
        self.pruning_stats.clone()
    }

    /// Number of time buckets not yet read.
    ///
    /// Each bucket is one merge unit, so this is how much segment I/O the
    /// stream still has ahead of it. Useful for asserting that a consumer
    /// which stopped early really did avoid the rest of the scan.
    #[must_use]
    pub fn buckets_remaining(&self) -> usize {
        self.buckets.len()
    }

    /// Read and merge one bucket into `CHUNK_ROWS`-sized output batches.
    fn materialise(&self, bucket: Bucket) -> Result<Vec<RecordBatch>> {
        let tag_filter_refs: Vec<(&str, &str)> = self
            .tag_filters
            .iter()
            .map(|f| (f.key.as_str(), f.value.as_str()))
            .collect();
        let tag_refs: Option<Vec<&str>> = self
            .tag_col_names
            .as_ref()
            .map(|v| v.iter().map(String::as_str).collect());

        // Held only for this bucket, not for the life of the stream.
        let tombs = self.db.tombstones.read();

        let ctx = SegmentFilterCtx {
            scan_columns: self.scan_columns.as_ref(),
            time_range: &self.time_range,
            tag_filter_refs: &tag_filter_refs,
            field_predicates: &self.field_predicates,
            measurement: &self.measurement,
            projection: &self.projection,
            plan: &self.plan,
            tombstones: &tombs,
            tag_col_names: tag_refs.as_deref(),
        };

        // Segments first (oldest → newest), so the memtable can overwrite them.
        let mut sources: Vec<RecordBatch> = Vec::with_capacity(bucket.segments.len() + 1);
        if bucket.segments.len() >= PARALLEL_THRESHOLD {
            use rayon::prelude::*;
            let read: Vec<Result<Option<RecordBatch>>> = bucket
                .segments
                .par_iter()
                .map(|entry| Chronix::read_segment_filtered(entry, &ctx))
                .collect();
            for r in read {
                if let Some(batch) = r? {
                    sources.push(batch);
                }
            }
        } else {
            for entry in &bucket.segments {
                if let Some(batch) = Chronix::read_segment_filtered(entry, &ctx)? {
                    sources.push(batch);
                }
            }
        }

        // Memtable last → highest write order → wins dedup.
        if let Some(mem) = bucket.memtable {
            if mem.num_rows() > 0 {
                let filtered = chronix_query::filter::filter_batch(
                    &mem,
                    self.time_range.start,
                    self.time_range.end,
                    &tag_filter_refs,
                )?;
                let filtered = Chronix::apply_projection_and_tombstone(
                    &filtered,
                    &self.measurement,
                    &self.projection,
                    &self.plan,
                    &tombs,
                    tag_refs.as_deref(),
                    None,
                )?;
                if filtered.num_rows() > 0 {
                    sources.push(filtered);
                }
            }
        }

        if sources.is_empty() {
            return Ok(Vec::new());
        }

        Ok(chronix_query::dedup::sort_merge_dedup_chunked(
            sources,
            &self.measurement,
            CHUNK_ROWS,
            None,
        )?)
    }
}

impl Iterator for BatchStream<'_> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            if let Some(batch) = self.pending.pop_front() {
                let batch = match self.clip(batch) {
                    Some(b) => b,
                    // Wholly consumed by the offset, or the limit is spent.
                    None => continue,
                };
                if self.max_total_bytes > 0 {
                    self.emitted_bytes += batch.get_array_memory_size();
                    if self.emitted_bytes > self.max_total_bytes {
                        self.done = true;
                        let max = self.max_total_bytes;
                        let total = self.emitted_bytes;
                        return Some(Err(DbError::Internal(format!(
                            "streaming query result too large: {total} bytes exceeds limit of \
                             {max} bytes (config max_query_result_bytes). Narrow the time range \
                             or add filters."
                        ))));
                    }
                }
                // Stamped here rather than in each producer: the memtable
                // builder, the segment reader and the compaction writer all
                // build a schema from a column's *type*, and a role is not a
                // type — a tag and a string field are both `Utf8`. One place,
                // reading the registry that owns the answer.
                return Some(Ok(self.db.stamp_roles(&self.measurement, batch)));
            }
            if self.remaining == Some(0) {
                self.done = true;
                return None;
            }
            let bucket = self.buckets.pop_front()?;
            // Before the I/O, not after it: a bucket read is the unit of work
            // this deadline exists to stop.
            if let Some((expiry, budget)) = self.deadline {
                if std::time::Instant::now() >= expiry {
                    self.done = true;
                    return Some(Err(DbError::QueryTimeout(budget)));
                }
            }
            match self.materialise(bucket) {
                Ok(chunks) => self.pending.extend(chunks),
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

impl Chronix {
    /// Execute a `Scan` plan as a lazily evaluated, bounded-memory iterator of
    /// `RecordBatch`es.
    ///
    /// Results are globally sorted by timestamp and deduplicated
    /// (last-write-wins), exactly as
    /// [`execute_stream`](Self::execute_stream) returns them — but segments
    /// are read one time-disjoint bucket at a time, so peak memory is
    /// proportional to the busiest time bucket rather than to the whole
    /// result set. This is what makes `export_parquet` viable on a device
    /// whose RAM is smaller than its query window.
    ///
    /// `Scan` and `Limit(Scan)` plans stream; a limit also terminates the
    /// scan early, so the buckets past it are never read. Aggregations,
    /// downsampling and window functions have to see their whole input, so
    /// pass those to [`execute`](Self::execute) or
    /// [`execute_stream`](Self::execute_stream).
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or the plan is neither a
    /// `Scan` nor a `Limit` over one. I/O errors surface from the iterator,
    /// not from this call.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use chronix::Chronix;
    /// # use chronix_core::ChronixConfig;
    /// # let config = ChronixConfig::builder().data_dir("/tmp/q").build().unwrap();
    /// # let db = Chronix::open(config).unwrap();
    /// let plan = db.query().measurement("cpu").range(0, i64::MAX).build().unwrap();
    /// let mut rows = 0;
    /// for batch in db.execute_iter(&plan)? {
    ///     rows += batch?.num_rows(); // each batch is dropped before the next is read
    /// }
    /// # Ok::<(), chronix::DbError>(())
    /// ```
    pub fn execute_iter(&self, plan: &QueryPlan) -> Result<BatchStream<'_>> {
        self.check_open()?;

        // `LIMIT` over a scan is the one wrapper that streams: it needs *less*
        // of its input, not all of it, and applying it inside the stream is
        // what makes the scan terminate early instead of reading every bucket.
        if let QueryPlan::Limit {
            source,
            limit,
            offset,
        } = plan
        {
            if matches!(source.as_ref(), QueryPlan::Scan { .. }) {
                return Ok(self.execute_iter(source)?.with_limit(*limit, *offset));
            }
        }

        if !matches!(plan, QueryPlan::Scan { .. }) {
            return Err(DbError::Internal(
                "execute_iter requires a Scan or Limit(Scan) plan; aggregate, downsample and \
                 window plans must see their whole input — use execute() or execute_stream()"
                    .into(),
            ));
        }

        let (measurement, tag_filters, projection, time_range) = extract_scan(plan)
            .ok_or_else(|| DbError::Internal("invalid query plan: no scan node found".into()))?;

        // No warning here about a plan without a namespace, and that is
        // deliberate: the embedded API and every single-tenant server produce
        // one on **every** read, so the warning fired once per query in the
        // ordinary configuration — 400 identical `WARN` lines for 400 reads,
        // on a gateway logging to an SD card. A warning that cannot
        // distinguish "tenancy is off" from "a handler forgot to scope this"
        // is not evidence of either, and only the server knows which. It is
        // asked there, where the answer exists: `chronixd`'s read handlers
        // scope through one function, proved by a test that drives both of
        // them.

        // The memtable is bounded by its flush budget, so materialising it is
        // safe; it is the *segment* side that is unbounded.
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
        drop(memtable_points);

        let (matching_entries, pruning_stats, segment_lease) =
            self.prune_segments(measurement, tag_filters, time_range);

        tracing::debug!(
            measurement,
            total = pruning_stats.segments_total,
            pruned_time = pruning_stats.pruned_by_time,
            surviving = matching_entries.len(),
            "execute_iter segment pruning"
        );

        let buckets = build_buckets(matching_entries, memtable_batch)?;

        let tag_col_names: Option<Vec<String>> = self
            .schema(measurement)
            .map(|ms| ms.tag_names().into_iter().map(String::from).collect());

        Ok(BatchStream {
            db: self,
            scan_columns: Self::compute_scan_columns(plan, tag_filters, projection),
            measurement: measurement.to_string(),
            projection: projection.to_vec(),
            tag_filters: tag_filters.to_vec(),
            time_range: *time_range,
            field_predicates: extract_field_predicates(plan).to_vec(),
            tag_col_names,
            plan: plan.clone(),
            buckets: buckets.into(),
            pending: VecDeque::new(),
            max_total_bytes: 0,
            emitted_bytes: 0,
            remaining: None,
            to_skip: 0,
            done: false,
            deadline: {
                let budget = self.config.query_timeout;
                (!budget.is_zero()).then(|| (std::time::Instant::now() + budget, budget))
            },
            pruning_stats,
            _segment_lease: segment_lease,
        })
    }
}

/// Ensure a batch is ascending by timestamp, sorting only if it is not.
///
/// `ShardRouter::scan_measurement` already sorts by timestamp, but the
/// bucketing below slices on that order, so the invariant is checked here
/// rather than assumed across a crate boundary. The check is one linear pass
/// over an `i64` column; the sort never runs in practice.
fn ensure_ts_sorted(batch: RecordBatch) -> Result<RecordBatch> {
    let Some(col) = batch.column_by_name(chronix_core::TIME_COLUMN) else {
        return Ok(batch);
    };
    let Some(ts) = col.as_any().downcast_ref::<Int64Array>() else {
        return Ok(batch);
    };
    if ts.values().windows(2).all(|w| w[0] <= w[1]) {
        return Ok(batch);
    }
    let indices = arrow::compute::sort_to_indices(col, None, None)
        .map_err(|e| DbError::Internal(e.to_string()))?;
    let cols = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DbError::Internal(e.to_string()))?;
    RecordBatch::try_new(batch.schema(), cols).map_err(|e| DbError::Internal(e.to_string()))
}

/// Sweep segments into time-disjoint buckets and attach the memtable rows
/// that fall in each bucket's range.
///
/// Segments are sorted by `min_timestamp`; a segment starts a new bucket when
/// it begins after everything accumulated so far has ended. Within a bucket
/// the original catalog order is restored, because dedup resolves ties by
/// input position and catalog order is write order.
fn build_buckets(entries: Vec<SegmentCatalogEntry>, memtable: RecordBatch) -> Result<Vec<Bucket>> {
    /// A run of segments whose time ranges overlap, with the catalog position
    /// of each kept so write order can be restored for last-write-wins.
    struct Range {
        lo: i64,
        hi: i64,
        segments: Vec<(usize, SegmentCatalogEntry)>,
    }

    // Disjoint and ascending by construction.
    let mut ranges: Vec<Range> = Vec::new();
    let mut ordered: Vec<(usize, SegmentCatalogEntry)> = entries.into_iter().enumerate().collect();
    ordered.sort_by_key(|(idx, e)| (e.min_timestamp, *idx));

    for (idx, entry) in ordered {
        match ranges.last_mut() {
            Some(range) if entry.min_timestamp <= range.hi => {
                range.hi = range.hi.max(entry.max_timestamp);
                range.segments.push((idx, entry));
            }
            _ => ranges.push(Range {
                lo: entry.min_timestamp,
                hi: entry.max_timestamp,
                segments: vec![(idx, entry)],
            }),
        }
    }

    let memtable = ensure_ts_sorted(memtable)?;
    let ts: &[i64] = memtable
        .column_by_name(chronix_core::TIME_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map_or(&[][..], |a| a.values().as_ref());
    // Every row must have a readable timestamp, because the slicing below is
    // what places a memtable row in the bucket whose segments it may overwrite.
    // The old fallback set `n = 0`, which looks like "hand the memtable to the
    // first bucket" and is not: with `n = 0` every slice is empty and the
    // trailing `pos < num_rows` branch appends the *whole* memtable as a final
    // bucket, after every segment. Buckets dedup independently, so a memtable
    // point overwriting a flushed one would then be emitted as a second row —
    // out of timestamp order, with the stale value first. That is the
    // last-write-wins divergence this module exists to prevent, so the
    // broken invariant is reported rather than silently worked around.
    if memtable.num_rows() != ts.len() {
        return Err(DbError::Internal(format!(
            "memtable batch has {} rows but a timestamp column of {} readable values; \
             a scan cannot be bucketed without a timestamp per row",
            memtable.num_rows(),
            ts.len()
        )));
    }
    let n = ts.len();

    let mut buckets = Vec::with_capacity(ranges.len() + 1);
    let mut pos = 0usize;

    for range in ranges {
        // Memtable rows strictly before this bucket have no segment to merge
        // with, so they form their own (still disjoint) bucket.
        let gap_start = pos;
        while pos < n && ts[pos] < range.lo {
            pos += 1;
        }
        if pos > gap_start {
            buckets.push(Bucket {
                segments: Vec::new(),
                memtable: Some(memtable.slice(gap_start, pos - gap_start)),
            });
        }

        let in_start = pos;
        while pos < n && ts[pos] <= range.hi {
            pos += 1;
        }

        let mut segments = range.segments;
        segments.sort_by_key(|(idx, _)| *idx);
        buckets.push(Bucket {
            segments: segments.into_iter().map(|(_, e)| e).collect(),
            memtable: (pos > in_start).then(|| memtable.slice(in_start, pos - in_start)),
        });
    }

    if pos < memtable.num_rows() {
        buckets.push(Bucket {
            segments: Vec::new(),
            memtable: Some(memtable.slice(pos, memtable.num_rows() - pos)),
        });
    }

    Ok(buckets)
}
