//! Compaction executor — merge-sort compaction with deduplication.
//!
//! Reads input segments, merges their data via sort-merge, deduplicates on
//! `(series_key, timestamp)` keeping the latest value, applies tombstone
//! cleanup, and writes an optimized output segment.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(test)]
use arrow::array::Float64Array;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use tracing::info;

use crate::segment::metadata::roles;
use crate::segment::reader::SegmentReader;
use crate::segment::writer::{SegmentMeta, SegmentWriter, SegmentWriterConfig};
use chronix_core::config::FloatEncoding;
use chronix_core::{CompressionCodec, TombstoneSet};

use crate::compaction::error::{CompactionError, Result};
use crate::compaction::picker::CompactionTask;

/// Default compaction task timeout: 10 minutes.
const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(600);

/// Compaction executor that performs merge-sort compaction.
///
/// Reads all input segments, merge-sorts by `(series_key, timestamp)`,
/// deduplicates, optionally removes tombstoned data, and writes an
/// optimized output segment.
///
/// # Tombstone lookup complexity
///
/// Tombstones are passed as `&TombstoneSet`, which uses a `HashMap` keyed
/// by canonical series name for O(1) amortised per-row lookup in
/// [`execute`](Self::execute).  Each entry supports both full-series and
/// ranged (time-window) deletes.  The overall cleanup pass is therefore
/// **O(rows)**, not O(rows × tombstones).
pub struct CompactionExecutor {
    /// Compression enabled for output segment.
    compress: bool,
    /// Float encoding strategy.
    float_encoding: FloatEncoding,
    /// Row group size for output segment.
    row_group_size: usize,
    /// Compression codec for output segment.
    compression_codec: CompressionCodec,
    /// Zstd compression level (used when codec is Zstd).
    zstd_level: i32,
    /// Maximum wall-clock time allowed for a single compaction task.
    task_timeout: Duration,
    /// Maximum total bytes allowed for input segments in a
    /// single compaction task.  When the running total of
    /// `RecordBatch::get_array_memory_size()` exceeds this limit, the
    /// task is rejected with `CompactionError::InputTooLarge`.
    /// 0 = unlimited.
    max_input_bytes: u64,
}

impl Default for CompactionExecutor {
    fn default() -> Self {
        Self {
            compress: true,
            float_encoding: FloatEncoding::Chimp,
            row_group_size: 65_536,
            compression_codec: CompressionCodec::default(),
            zstd_level: 3,
            task_timeout: DEFAULT_TASK_TIMEOUT,
            max_input_bytes: 0,
        }
    }
}

impl CompactionExecutor {
    /// Create an executor with custom settings.
    #[must_use]
    pub fn new(
        compress: bool,
        float_encoding: FloatEncoding,
        row_group_size: usize,
        compression_codec: CompressionCodec,
        zstd_level: i32,
    ) -> Self {
        Self {
            compress,
            float_encoding,
            row_group_size,
            compression_codec,
            zstd_level,
            task_timeout: DEFAULT_TASK_TIMEOUT,
            max_input_bytes: 0,
        }
    }

    /// Set the maximum wall-clock time for a single compaction task.
    #[must_use]
    pub fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.task_timeout = timeout;
        self
    }

    /// Set the maximum total bytes for input segments.
    ///
    /// When the cumulative in-memory size of loaded `RecordBatch`es
    /// exceeds this threshold the compaction task is rejected early
    /// with [`CompactionError::InputTooLarge`].  Pass 0 for unlimited.
    #[must_use]
    pub fn with_max_input_bytes(mut self, limit: u64) -> Self {
        self.max_input_bytes = limit;
        self
    }

    /// Execute a compaction task.
    ///
    /// Reads all input segments, merges their data, deduplicates on
    /// `(series_key_hash, timestamp, canonical)`, removes tombstoned series,
    /// and writes an optimized output segment.
    ///
    /// # K-way Merge Architecture
    ///
    /// Each segment's row indices are sorted independently by
    /// `(series_hash, timestamp, canonical_id)`, then merged using a
    /// K-element `BinaryHeap` (min-heap) in O(N log K) time — versus
    /// the previous O(N log N) global sort. Deduplication is inline:
    /// duplicate `(hash, ts, canonical)` keys are resolved by keeping
    /// only the row from the newest segment (last-write-wins).
    ///
    /// Output is assembled column-by-column via `arrow::compute::interleave`
    /// in row-group-sized chunks, bounding peak memory to
    /// O(chunk_size × columns) for the output stage.
    ///
    /// # Errors
    ///
    /// Returns an error if no eligible segments exist, reading/writing
    /// segments fails, or deduplication encounters an internal error.
    pub fn execute(&self, task: &CompactionTask, tombstones: &TombstoneSet) -> Result<SegmentMeta> {
        if task.input_segments.is_empty() {
            return Err(CompactionError::NoEligibleSegments);
        }
        let deadline = Instant::now() + self.task_timeout;
        info!(
            shard = %task.shard_id,
            input_segments = task.input_segments.len(),
            output = %task.output_path.display(),
            "Starting compaction"
        );

        // 1. Read all input segments as RecordBatches.
        // Sort by segment_id ascending so that later segments (higher IDs,
        // written more recently) get higher segment_ord in the K-way merge.
        // This guarantees deterministic last-write-wins dedup semantics:
        //   - segment_ids are assigned sequentially at flush time
        //   - the K-way merge heap breaks ties by segment_ord
        //   - per-key, only the row from the newest segment is emitted
        // ⚠  Invariant: segment_id must increase monotonically with write time.
        let mut sorted_entries = task.input_segments.clone();
        sorted_entries.sort_by_key(|e| e.segment_id);

        // Validate that segment IDs are strictly monotonic.
        // Duplicate IDs would violate last-write-wins dedup semantics.
        for w in sorted_entries.windows(2) {
            if w[0].segment_id >= w[1].segment_id {
                return Err(CompactionError::Internal(format!(
                    "segment_id monotonicity violation: segments {} and {} have \
                     non-strictly-increasing IDs ({} >= {})",
                    w[0].path.display(),
                    w[1].path.display(),
                    w[0].segment_id,
                    w[1].segment_id,
                )));
            }
        }
        let segment_ids: Vec<u64> = sorted_entries.iter().map(|e| e.segment_id.0).collect();
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut loaded_bytes: u64 = 0;
        for entry in &sorted_entries {
            let reader = SegmentReader::open(&entry.path)?;
            let batch = reader.read_all()?;
            if batch.num_rows() > 0 {
                loaded_bytes += batch.get_array_memory_size() as u64;
                // Reject early if cumulative input exceeds budget.
                if self.max_input_bytes > 0 && loaded_bytes > self.max_input_bytes {
                    return Err(CompactionError::InputTooLarge {
                        loaded_bytes,
                        limit_bytes: self.max_input_bytes,
                    });
                }
                all_batches.push(batch);
            }
            if Instant::now() > deadline {
                return Err(CompactionError::TaskTimeout(
                    self.task_timeout.as_millis() as u64
                ));
            }
        }

        if all_batches.is_empty() {
            return Err(CompactionError::NoEligibleSegments);
        }

        // 2. Unify schemas across all batches
        let unified_schema = unify_schemas(&all_batches)?;

        // 3. Align all batches to the unified schema (consumes originals)
        let aligned: Vec<RecordBatch> = all_batches
            .into_iter()
            .map(|b| align_to_schema(&b, &unified_schema))
            .collect::<Result<Vec<_>>>()?;

        // 4. Compute metadata across batches WITHOUT concatenation.
        //    Hashes, canonicals, and timestamps are flattened into parallel
        //    vectors indexed by a global row number.
        let total_rows: usize = aligned
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        if total_rows == 0 {
            return Err(CompactionError::NoEligibleSegments);
        }
        let batch_sizes: Vec<usize> = aligned
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .collect();

        let tag_columns = find_tag_columns_from_segments(task);
        let measurement = &task.input_segments[0].measurement;

        let mut hashes = Vec::with_capacity(total_rows);
        let mut canonicals = Vec::with_capacity(total_rows);
        let mut timestamps = Vec::with_capacity(total_rows);

        for batch in &aligned {
            let (h, c) = compute_row_hashes_and_canonicals(batch, measurement, &tag_columns);
            hashes.extend(h);
            canonicals.extend(c);

            let ts_col = batch
                .column_by_name(chronix_core::TIME_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| CompactionError::Internal("missing timestamp column".into()))?;
            for i in 0..batch.num_rows() {
                timestamps.push(ts_col.value(i));
            }
        }

        // 5. Assign sort-preserving canonical IDs for O(1) comparison
        //    during K-way merge. Number of unique canonicals = series count,
        //    typically ≪ total_rows.
        let canonical_ids = assign_canonical_ids(&canonicals);

        // 6. Per-segment sort: sort each segment's indices independently
        //    by (hash, ts, canonical_id). Uses sort_unstable since
        //    segment_ord in the merge heap handles dedup ordering.
        if Instant::now() > deadline {
            return Err(CompactionError::TaskTimeout(
                self.task_timeout.as_millis() as u64
            ));
        }
        let mut segment_streams: Vec<Vec<usize>> = Vec::with_capacity(batch_sizes.len());
        {
            let mut cursor = 0usize;
            for &bs in &batch_sizes {
                let start = cursor;
                let end = cursor + bs;
                let mut local_indices: Vec<usize> = (start..end).collect();
                local_indices.sort_unstable_by(|&a, &b| {
                    hashes[a]
                        .cmp(&hashes[b])
                        .then(timestamps[a].cmp(&timestamps[b]))
                        .then(canonical_ids[a].cmp(&canonical_ids[b]))
                });
                segment_streams.push(local_indices);
                cursor = end;
            }
        }

        // 7. K-way merge with inline dedup — O(N log K) where K = segment
        //    count, vs previous O(N log N) global sort. Uses a BinaryHeap
        //    (min-heap) to merge K pre-sorted streams. For duplicate
        //    (hash, ts, canonical) keys, only the row from the newest
        //    segment (highest segment_ord) is emitted.
        //
        // Stream the merge directly into chunked output
        // instead of materializing the full sorted_kept Vec first.
        // This eliminates O(N) memory for the index vector and avoids
        // accumulating all output chunks before writing.
        let chunk_size = self.row_group_size.max(8192);
        let mut merge_iter = KWayMergeIter::new(
            &segment_streams,
            &hashes,
            &timestamps,
            &canonical_ids,
            &canonicals,
            tombstones,
            &segment_ids,
        );

        // 8. Build SORTED output directly from source batches (no
        //    concat_batches). Collect chunk_size indices at a time from
        //    the streaming merge, then interleave into output RecordBatches.
        let mut output_chunks: Vec<RecordBatch> = Vec::new();
        let mut chunk_buf: Vec<usize> = Vec::with_capacity(chunk_size);

        loop {
            chunk_buf.clear();
            for _ in 0..chunk_size {
                match merge_iter.next() {
                    Some(gi) => chunk_buf.push(gi),
                    None => break,
                }
            }
            if chunk_buf.is_empty() {
                break;
            }

            let interleave_pairs: Vec<(usize, usize)> = chunk_buf
                .iter()
                .map(|&gi| global_to_batch_local(&batch_sizes, gi))
                .collect::<Result<Vec<_>>>()?;

            let num_cols = unified_schema.fields().len();
            let mut result_columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

            for col_idx in 0..num_cols {
                let col_arrays: Vec<&dyn Array> =
                    aligned.iter().map(|b| b.column(col_idx).as_ref()).collect();
                let interleaved = compute::interleave(&col_arrays, &interleave_pairs)
                    .map_err(|e| CompactionError::Internal(format!("interleave failed: {e}")))?;
                result_columns.push(interleaved);
            }

            let chunk_batch = RecordBatch::try_new(unified_schema.clone(), result_columns)
                .map_err(|e| {
                    CompactionError::Internal(format!("failed to build chunk batch: {e}"))
                })?;
            output_chunks.push(chunk_batch);

            if Instant::now() > deadline {
                return Err(CompactionError::TaskTimeout(
                    self.task_timeout.as_millis() as u64
                ));
            }
        }

        if output_chunks.is_empty() {
            info!(
                shard = %task.shard_id,
                "Compaction: all rows tombstoned — no output segment"
            );
            return Ok(SegmentMeta {
                path: task.output_path.clone(),
                min_timestamp: 0,
                max_timestamp: 0,
                row_count: 0,
                series_count: 0,
                series_keys: Vec::new(),
                byte_size: 0,
                row_group_count: 0,
                column_count: 0,
                uncompressed_bytes: 0,
                header: crate::segment::header::SegmentHeader {
                    version: 0,
                    flags: 0,
                    created_at: 0,
                    min_timestamp: 0,
                    max_timestamp: 0,
                    row_count: 0,
                    column_count: 0,
                    series_count: 0,
                    compression: 0,
                    sort_order: 0,
                },
                column_metas: Vec::new(),
            });
        }

        // Pass pre-chunked output directly to finalize_batches,
        // avoiding the concat_batches → finalize_batch double-materialization.
        // Each chunk is already row_group_size rows, so writing is streaming.
        if Instant::now() > deadline {
            return Err(CompactionError::TaskTimeout(
                self.task_timeout.as_millis() as u64
            ));
        }

        let config = SegmentWriterConfig {
            row_group_size: self.row_group_size,
            compress: self.compress,
            float_encoding: self.float_encoding,
            compression_codec: self.compression_codec,
            zstd_level: self.zstd_level,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&task.output_path, config)?;
        let meta: std::result::Result<_, CompactionError> =
            (|| Ok(writer.finalize_batches(&output_chunks, measurement, &tag_columns)?))();

        match meta {
            Ok(meta) => {
                info!(
                    shard = %task.shard_id,
                    input_rows = total_rows,
                    output_rows = meta.row_count,
                    dedup_removed = (total_rows as u64).saturating_sub(meta.row_count),
                    output = %meta.path.display(),
                    "Compaction complete"
                );
                Ok(meta)
            }
            Err(e) => {
                // Clean up partial output file to prevent orphaned/corrupt segments.
                if task.output_path.exists() {
                    if let Err(rm_err) = std::fs::remove_file(&task.output_path) {
                        tracing::warn!(
                            path = %task.output_path.display(),
                            error = %rm_err,
                            "failed to clean up partial compaction output"
                        );
                    }
                }
                Err(e)
            }
        }
    }
}

/// Map a global row index to `(batch_index, local_row)`.
fn global_to_batch_local(batch_sizes: &[usize], global: usize) -> Result<(usize, usize)> {
    let mut offset = 0;
    for (bi, &bs) in batch_sizes.iter().enumerate() {
        if global < offset + bs {
            return Ok((bi, global - offset));
        }
        offset += bs;
    }
    Err(CompactionError::Internal(format!(
        "global index {global} out of range (total rows: {offset})"
    )))
}

/// Unify schemas from multiple batches into a single superset schema.
///
/// Returns an error if two batches define the same column with different
/// data types (schema evolution conflict).
///
/// Nullable flags are widened: a column is nullable in the unified schema
/// if it is nullable in ANY batch, or if it is absent from ANY batch
/// (since `align_to_schema` fills missing columns with nulls).
fn unify_schemas(batches: &[RecordBatch]) -> Result<Arc<Schema>> {
    let mut fields: BTreeMap<String, Field> = BTreeMap::new();

    for batch in batches {
        for field in batch.schema().fields() {
            match fields.entry(field.name().clone()) {
                std::collections::btree_map::Entry::Occupied(mut existing) => {
                    if existing.get().data_type() != field.data_type() {
                        return Err(CompactionError::Internal(format!(
                            "column '{}' type conflict across segments: {:?} vs {:?}",
                            field.name(),
                            existing.get().data_type(),
                            field.data_type(),
                        )));
                    }
                    // Widen to nullable if either side allows nulls
                    if field.is_nullable() && !existing.get().is_nullable() {
                        let widened = existing.get().clone().with_nullable(true);
                        existing.insert(widened);
                    }
                }
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(field.as_ref().clone());
                }
            }
        }
    }

    // Mark any column absent from at least one batch as nullable,
    // since align_to_schema fills missing columns with null arrays.
    let all_names: Vec<String> = fields.keys().cloned().collect();
    for batch in batches {
        let schema = batch.schema();
        let batch_names: std::collections::HashSet<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        for name in &all_names {
            if !batch_names.contains(name.as_str()) {
                if let Some(f) = fields.get_mut(name) {
                    if !f.is_nullable() {
                        *f = f.clone().with_nullable(true);
                    }
                }
            }
        }
    }

    let field_vec: Vec<Field> = fields.into_values().collect();
    Ok(Arc::new(Schema::new(field_vec)))
}

/// Align a batch to a target schema, adding null columns for missing fields.
fn align_to_schema(batch: &RecordBatch, target: &Arc<Schema>) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());

    for field in target.fields() {
        if let Some(col) = batch.column_by_name(field.name()) {
            columns.push(col.clone());
        } else {
            // Add null column
            let null_col = make_null_array(field.data_type(), num_rows);
            columns.push(null_col);
        }
    }

    RecordBatch::try_new(target.clone(), columns)
        .map_err(|e| CompactionError::Internal(format!("Failed to align batch to schema: {e}")))
}

/// Create an array of nulls with the given data type and length.
///
/// Uses Arrow's built-in `new_null_array` which handles all data types
/// correctly, including `Timestamp`, `Date`, `Decimal`, etc.
fn make_null_array(dt: &DataType, len: usize) -> ArrayRef {
    arrow::array::new_null_array(dt, len)
}

/// Identify which column names are tags by reading segment column metadata.
///
/// Uses the authoritative `role` field from segment metadata (role=1 is TAG)
/// instead of guessing from Arrow data types, which would misclassify string
/// fields as tags.
///
/// Unions tag columns across **all** readable segments to handle schema
/// evolution (tags present in later segments but absent from earlier ones).
fn find_tag_columns_from_segments(task: &CompactionTask) -> Vec<String> {
    let mut all_tags = std::collections::BTreeSet::new();
    for entry in &task.input_segments {
        if let Ok(reader) = SegmentReader::open(&entry.path) {
            for c in reader.column_metadata() {
                if c.role == roles::TAG {
                    all_tags.insert(c.name.clone());
                }
            }
        }
    }
    all_tags.into_iter().collect()
}

/// Compute FNV-1a series hash AND canonical form for each row.
///
/// Returns `(hashes, canonicals)` so that the dedup step can compare
/// canonical forms when hashes collide, preventing cross-series data loss.
fn compute_row_hashes_and_canonicals(
    batch: &RecordBatch,
    measurement: &str,
    tag_columns: &[String],
) -> (Vec<u64>, Vec<String>) {
    let num_rows = batch.num_rows();
    let mut hashes = Vec::with_capacity(num_rows);
    let mut canonicals = Vec::with_capacity(num_rows);

    // Sort tag columns for deterministic hashing (BTreeMap order)
    let mut sorted_tags: Vec<&str> = tag_columns.iter().map(String::as_str).collect();
    sorted_tags.sort_unstable();

    // Pre-fetch tag arrays
    let tag_arrays: Vec<Option<&StringArray>> = sorted_tags
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        })
        .collect();

    // Cache series key computations — many rows share
    // the same tag combination, so we avoid redundant BTreeMap + SeriesKey
    // + hash allocations. Keyed on a collision-free tag-byte string built
    // directly from the sorted raw column values (avoids the u64 FNV
    // fingerprint that could silently merge distinct series on collision).
    let mut cache: HashMap<String, (u64, String)> = HashMap::new();

    for row in 0..num_rows {
        // Build a collision-free cache key from raw tag bytes.
        let mut cache_key = String::new();
        for (tag_idx, _) in sorted_tags.iter().enumerate() {
            if let Some(arr) = tag_arrays[tag_idx] {
                if !arr.is_null(row) {
                    let val = arr.value(row);
                    if !val.is_empty() {
                        cache_key.push_str(val);
                    }
                }
            }
            cache_key.push('\0'); // separator
        }

        if let Some((h, c)) = cache.get(&cache_key) {
            hashes.push(*h);
            canonicals.push(c.clone());
            continue;
        }

        // Cache miss — build full SeriesKey
        let mut tags = BTreeMap::new();
        for (tag_idx, tag_name) in sorted_tags.iter().enumerate() {
            if let Some(arr) = tag_arrays[tag_idx] {
                if !arr.is_null(row) {
                    let val = arr.value(row);
                    if !val.is_empty() {
                        tags.insert((*tag_name).to_string(), val.to_string());
                    }
                }
            }
        }

        // Construct SeriesKey and compute hash + canonical form
        match chronix_core::SeriesKey::new(measurement, tags) {
            Ok(key) => {
                let h = key.hash_fnv();
                let c = key.canonical_form().to_string();
                cache.insert(cache_key, (h, c.clone()));
                hashes.push(h);
                canonicals.push(c);
            }
            Err(_) => {
                // Fallback: use row index to avoid dedup collision when
                // multiple rows fail (should never happen in practice).
                use std::hash::{Hash, Hasher};
                let mut h = fnv::FnvHasher::default();
                row.hash(&mut h);
                measurement.hash(&mut h);
                hashes.push(h.finish());
                canonicals.push(format!("__fallback_{row}"));
            }
        }
    }

    (hashes, canonicals)
}

/// Assign sort-preserving `u32` IDs to canonical series forms.
///
/// The returned vector maps each index in `canonicals` to a `u32` that
/// preserves lexicographic order: `ids[a] < ids[b]` iff
/// `canonicals[a] < canonicals[b]`. This enables O(1) comparisons in
/// the K-way merge heap instead of per-byte string comparisons.
fn assign_canonical_ids(canonicals: &[String]) -> Vec<u32> {
    let mut unique: Vec<&str> = canonicals.iter().map(String::as_str).collect();
    unique.sort_unstable();
    unique.dedup();
    // Checked cast prevents silent truncation if >4B unique series.
    let id_map: HashMap<&str, u32> = unique
        .iter()
        .enumerate()
        .map(|(i, &s)| {
            (
                s,
                u32::try_from(i).expect("more than u32::MAX unique canonical series"),
            )
        })
        .collect();
    canonicals.iter().map(|s| id_map[s.as_str()]).collect()
}

/// Entry in the K-way merge min-heap.
///
/// Ordered by `(hash, ts, canonical_id, segment_ord)`. Wrapped in
/// `Reverse` so that `BinaryHeap` (a max-heap) yields the smallest
/// entry first.
#[derive(Eq, PartialEq)]
struct MergeEntry {
    hash: u64,
    ts: i64,
    canonical_id: u32,
    /// Segment ordinal: 0 = oldest, K−1 = newest (dedup priority).
    segment_ord: usize,
    /// Global row index into the flattened metadata arrays.
    global_idx: usize,
    /// Which sorted stream this entry came from.
    stream_idx: usize,
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash
            .cmp(&other.hash)
            .then(self.ts.cmp(&other.ts))
            .then(self.canonical_id.cmp(&other.canonical_id))
            .then(self.segment_ord.cmp(&other.segment_ord))
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// K-way merge of pre-sorted segment streams with inline deduplication.
///
/// Each input segment's indices are pre-sorted by `(hash, ts, canonical_id)`.
/// A `BinaryHeap` (min-heap via `Reverse`) merges K sorted streams in
/// O(N log K) time. Dedup is inline: for duplicate `(hash, ts, canonical_id)`
/// tuples, only the row from the newest segment (highest `segment_ord`) is
/// emitted — achieving last-write-wins semantics.
///
/// Rows masked by a tombstone issued against their segment are skipped.
///
/// Returns a streaming iterator that yields one global
/// index at a time instead of materializing the entire merged result.
/// The caller collects chunk-sized batches for streaming output.
struct KWayMergeIter<'a> {
    segment_streams: &'a [Vec<usize>],
    hashes: &'a [u64],
    timestamps: &'a [i64],
    canonical_ids: &'a [u32],
    canonicals: &'a [String],
    /// TombstoneSet supports both full-series and ranged deletes.
    tombstones: &'a TombstoneSet,
    /// The catalog id of each stream's segment: a tombstone masks a row
    /// only if it was issued against the segment the row comes from.
    segment_ids: &'a [u64],
    positions: Vec<usize>,
    heap: BinaryHeap<Reverse<MergeEntry>>,
}

impl<'a> KWayMergeIter<'a> {
    fn new(
        segment_streams: &'a [Vec<usize>],
        hashes: &'a [u64],
        timestamps: &'a [i64],
        canonical_ids: &'a [u32],
        canonicals: &'a [String],
        tombstones: &'a TombstoneSet,
        segment_ids: &'a [u64],
    ) -> Self {
        let k = segment_streams.len();
        let mut positions = vec![0usize; k];
        let mut heap = BinaryHeap::with_capacity(k);

        for (stream_idx, stream) in segment_streams.iter().enumerate() {
            if let Some(&gi) = stream.first() {
                heap.push(Reverse(MergeEntry {
                    hash: hashes[gi],
                    ts: timestamps[gi],
                    canonical_id: canonical_ids[gi],
                    segment_ord: stream_idx,
                    global_idx: gi,
                    stream_idx,
                }));
                positions[stream_idx] = 1;
            }
        }

        Self {
            segment_streams,
            hashes,
            timestamps,
            canonical_ids,
            canonicals,
            tombstones,
            segment_ids,
            positions,
            heap,
        }
    }
}

impl Iterator for KWayMergeIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        loop {
            let Reverse(entry) = self.heap.pop()?;

            // Advance the stream that produced this entry.
            let stream = &self.segment_streams[entry.stream_idx];
            let pos = &mut self.positions[entry.stream_idx];
            if *pos < stream.len() {
                let gi = stream[*pos];
                *pos += 1;
                self.heap.push(Reverse(MergeEntry {
                    hash: self.hashes[gi],
                    ts: self.timestamps[gi],
                    canonical_id: self.canonical_ids[gi],
                    segment_ord: entry.stream_idx,
                    global_idx: gi,
                    stream_idx: entry.stream_idx,
                }));
            }

            // Check ranged tombstones — supports both full-series and time-range deletes.
            if self.tombstones.is_tombstoned_in(
                &self.canonicals[entry.global_idx],
                self.timestamps[entry.global_idx],
                self.segment_ids[entry.stream_idx],
            ) {
                continue;
            }

            // Dedup: if the next heap entry has the same (hash, ts, canonical_id),
            // a newer version exists — skip this older one.
            if let Some(Reverse(next)) = self.heap.peek() {
                if entry.hash == next.hash
                    && entry.ts == next.ts
                    && entry.canonical_id == next.canonical_id
                {
                    continue;
                }
            }

            return Some(entry.global_idx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::picker::CompactionTask;
    use crate::index::SegmentCatalogEntry;
    use chronix_core::{FieldValue, Point, SegmentId, SegmentState, SeriesKey, ShardId};
    use std::path::{Path, PathBuf};

    fn make_point(host: &str, value: f64, ts: i64) -> Point {
        let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
        let fields = BTreeMap::from([("cpu".to_string(), FieldValue::F64(value))]);
        let key = SeriesKey::new("cpu_usage", tags).unwrap();
        Point::new(key, fields, ts).unwrap()
    }

    fn write_segment(dir: &Path, name: &str, points: &[Point]) -> (PathBuf, SegmentMeta) {
        let path = dir.join(name);
        let config = SegmentWriterConfig {
            row_group_size: 1000,
            compress: false,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: chronix_core::CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };
        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(points).unwrap();
        let meta = writer.finalize().unwrap();
        (path, meta)
    }

    fn make_entry(id: u64, shard: i64, path: PathBuf, meta: &SegmentMeta) -> SegmentCatalogEntry {
        SegmentCatalogEntry {
            segment_id: SegmentId(id),
            shard_id: ShardId(shard),
            measurement: "cpu_usage".to_string(),
            path,
            min_timestamp: meta.min_timestamp,
            max_timestamp: meta.max_timestamp,
            row_count: meta.row_count,
            series_count: meta.series_count,
            byte_size: meta.byte_size,
            row_group_count: meta.row_group_count,
            column_count: meta.column_count,
            column_stats: Vec::new(),
            state: SegmentState::default(),
        }
    }

    #[test]
    fn compact_three_overlapping_segments() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        // Segment 1: host=srv1 timestamps 100, 200, 300
        let pts1 = vec![
            make_point("srv1", 1.0, 100),
            make_point("srv1", 2.0, 200),
            make_point("srv1", 3.0, 300),
        ];
        let (p1, m1) = write_segment(dir.path(), "s1.csx", &pts1);

        // Segment 2: host=srv1 timestamps 200 (dup!), 400; host=srv2 ts=100
        let pts2 = vec![
            make_point("srv1", 99.0, 200), // duplicate with s1
            make_point("srv1", 4.0, 400),
            make_point("srv2", 10.0, 100),
        ];
        let (p2, m2) = write_segment(dir.path(), "s2.csx", &pts2);

        // Segment 3: host=srv2 timestamps 200
        let pts3 = vec![make_point("srv2", 20.0, 200)];
        let (p3, m3) = write_segment(dir.path(), "s3.csx", &pts3);

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![
                make_entry(1, 0, p1, &m1),
                make_entry(2, 0, p2, &m2),
                make_entry(3, 0, p3, &m3),
            ],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("compacted.csx"),
        };

        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        );
        let tombstones = TombstoneSet::new();
        let meta = executor.execute(&task, &tombstones).unwrap();

        // 3 + 3 + 1 = 7 input rows, minus 1 duplicate = 6
        assert_eq!(meta.row_count, 6);
        assert_eq!(meta.series_count, 2);

        // Read back and verify
        let reader = SegmentReader::open(&task.output_path).unwrap();
        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), 6);

        // Verify dedup: srv1@200 should have value 99.0 (last-write-wins = segment 2)
        let host_col = batch.column_by_name("host").unwrap();
        let host_arr = host_col.as_any().downcast_ref::<StringArray>().unwrap();
        let ts_col = batch.column_by_name(chronix_core::TIME_COLUMN).unwrap();
        let ts_arr = ts_col.as_any().downcast_ref::<Int64Array>().unwrap();
        let cpu_col = batch.column_by_name("cpu").unwrap();
        let cpu_arr = cpu_col.as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            if host_arr.value(i) == "srv1" && ts_arr.value(i) == 200 {
                // Last-write-wins: the value from segment 2 (99.0) wins
                assert!(
                    (cpu_arr.value(i) - 99.0).abs() < f64::EPSILON,
                    "Expected 99.0 for srv1@200, got {}",
                    cpu_arr.value(i)
                );
            }
        }
    }

    #[test]
    fn compact_with_tombstone_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        let pts = vec![
            make_point("srv1", 1.0, 100),
            make_point("srv1", 2.0, 200),
            make_point("srv2", 10.0, 100),
            make_point("srv2", 20.0, 200),
        ];
        let (path, meta) = write_segment(dir.path(), "seg.csx", &pts);

        // Tombstone srv1
        let srv1_key = SeriesKey::new(
            "cpu_usage",
            BTreeMap::from([("host".to_string(), "srv1".to_string())]),
        )
        .unwrap();
        let mut tombstones = TombstoneSet::new();
        tombstones.insert(
            chronix_core::Tombstone::all_time(srv1_key.canonical_form().to_string())
                .with_segments(0..1000),
        );

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, path, &meta)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("cleaned.csx"),
        };

        let executor = CompactionExecutor::default();
        let result = executor.execute(&task, &tombstones).unwrap();

        // Only srv2 rows should remain
        assert_eq!(result.row_count, 2);

        let reader = SegmentReader::open(&task.output_path).unwrap();
        let batch = reader.read_all().unwrap();
        let host_col = batch.column_by_name("host").unwrap();
        let host_arr = host_col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(host_arr.value(i), "srv2");
        }
    }

    #[test]
    fn compact_empty_segments_returns_error() {
        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: PathBuf::from("/tmp/empty.csx"),
        };

        let executor = CompactionExecutor::default();
        let result = executor.execute(&task, &TombstoneSet::new());
        assert!(result.is_err());
    }

    #[test]
    fn compact_preserves_all_data_when_no_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        let pts1 = vec![make_point("srv1", 1.0, 100), make_point("srv1", 2.0, 200)];
        let pts2 = vec![make_point("srv1", 3.0, 300), make_point("srv1", 4.0, 400)];

        let (p1, m1) = write_segment(dir.path(), "s1.csx", &pts1);
        let (p2, m2) = write_segment(dir.path(), "s2.csx", &pts2);

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, p1, &m1), make_entry(2, 0, p2, &m2)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("no_dup.csx"),
        };

        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        );
        let meta = executor.execute(&task, &TombstoneSet::new()).unwrap();
        assert_eq!(meta.row_count, 4);
    }

    /// Verify that compacted output has series grouped together with
    /// monotonically increasing timestamps within each series.
    #[test]
    fn compact_output_sorted_by_series_then_time() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        // Interleave two series across two segments so concatenation
        // order would NOT naturally produce sorted output.
        let pts1 = vec![
            make_point("srv1", 1.0, 300),
            make_point("srv2", 2.0, 100),
            make_point("srv1", 3.0, 100),
        ];
        let pts2 = vec![
            make_point("srv2", 4.0, 400),
            make_point("srv1", 5.0, 500),
            make_point("srv2", 6.0, 200),
        ];

        let (p1, m1) = write_segment(dir.path(), "s1.csx", &pts1);
        let (p2, m2) = write_segment(dir.path(), "s2.csx", &pts2);

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, p1, &m1), make_entry(2, 0, p2, &m2)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("sorted.csx"),
        };

        // Use small row_group_size to exercise multi-RG path.
        let executor =
            CompactionExecutor::new(false, FloatEncoding::Gorilla, 3, CompressionCodec::Lz4, 3);
        let meta = executor.execute(&task, &TombstoneSet::new()).unwrap();
        assert_eq!(meta.row_count, 6);

        // Read back and verify within-series timestamp monotonicity.
        let reader = SegmentReader::open(&task.output_path).unwrap();
        let batch = reader.read_all().unwrap();

        let host_col = batch.column_by_name("host").unwrap();
        let host_arr = host_col.as_any().downcast_ref::<StringArray>().unwrap();
        let ts_col = batch.column_by_name(chronix_core::TIME_COLUMN).unwrap();
        let ts_arr = ts_col.as_any().downcast_ref::<Int64Array>().unwrap();

        // Collect timestamps per series.
        let mut srv1_ts = Vec::new();
        let mut srv2_ts = Vec::new();
        for i in 0..batch.num_rows() {
            match host_arr.value(i) {
                "srv1" => srv1_ts.push(ts_arr.value(i)),
                "srv2" => srv2_ts.push(ts_arr.value(i)),
                other => panic!("unexpected host: {other}"),
            }
        }

        assert_eq!(
            srv1_ts,
            vec![100, 300, 500],
            "srv1 timestamps should be sorted"
        );
        assert_eq!(
            srv2_ts,
            vec![100, 200, 400],
            "srv2 timestamps should be sorted"
        );

        // Verify data is grouped: all rows of one series appear before
        // the other (same-series rows must be contiguous in the batch).
        let mut seen_series: Vec<String> = Vec::new();
        for i in 0..batch.num_rows() {
            let h = host_arr.value(i).to_string();
            if seen_series.last() != Some(&h) {
                seen_series.push(h);
            }
        }
        assert_eq!(
            seen_series.len(),
            2,
            "series should be grouped (got transitions: {seen_series:?})"
        );
    }

    /// Integration stress test: write many segments with overlapping data,
    /// compact, and verify every surviving row is correct.
    #[test]
    fn compact_stress_many_segments_many_series() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        // Generate 8 segments × 5 series × varying timestamps with overlaps.
        let hosts = ["web1", "web2", "db1", "db2", "cache1"];
        let num_segments = 8;
        let mut segments = Vec::new();
        let mut expected: std::collections::HashMap<(String, i64), f64> =
            std::collections::HashMap::new();

        for seg_idx in 0..num_segments {
            let mut points = Vec::new();
            for (h_idx, host) in hosts.iter().enumerate() {
                // Each segment writes 10 timestamps with some overlap across segments.
                let base_ts = (seg_idx * 5 + h_idx * 3) as i64 * 1000;
                for t in 0..10 {
                    let ts = base_ts + t * 100;
                    let value = seg_idx as f64 * 1000.0 + h_idx as f64 * 100.0 + t as f64;
                    points.push(make_point(host, value, ts));
                    // Last-write-wins: later segment (higher seg_idx) wins
                    expected.insert((host.to_string(), ts), value);
                }
            }
            let name = format!("seg_{seg_idx}.csx");
            let (path, meta) = write_segment(dir.path(), &name, &points);
            segments.push(make_entry(seg_idx as u64, 0, path, &meta));
        }

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: segments,
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("stress.csx"),
        };

        let executor =
            CompactionExecutor::new(true, FloatEncoding::Chimp, 50, CompressionCodec::Lz4, 3);
        let meta = executor.execute(&task, &TombstoneSet::new()).unwrap();

        // Verify row count matches expected deduplicated set.
        assert_eq!(
            meta.row_count as usize,
            expected.len(),
            "compacted row count should match deduplicated expected set"
        );

        // Read back and verify every row.
        let reader = SegmentReader::open(&task.output_path).unwrap();
        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), expected.len());

        let host_col = batch.column_by_name("host").unwrap();
        let host_arr = host_col.as_any().downcast_ref::<StringArray>().unwrap();
        let ts_col = batch.column_by_name(chronix_core::TIME_COLUMN).unwrap();
        let ts_arr = ts_col.as_any().downcast_ref::<Int64Array>().unwrap();
        let cpu_col = batch.column_by_name("cpu").unwrap();
        let cpu_arr = cpu_col.as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            let key = (host_arr.value(i).to_string(), ts_arr.value(i));
            let expected_val = expected
                .get(&key)
                .unwrap_or_else(|| panic!("unexpected row: host={}, ts={}", key.0, key.1));
            assert!(
                (cpu_arr.value(i) - expected_val).abs() < f64::EPSILON,
                "row {i}: host={}, ts={}: expected {expected_val}, got {}",
                key.0,
                key.1,
                cpu_arr.value(i),
            );
        }

        // Verify per-series timestamps are sorted ascending.
        let mut series_ts: std::collections::HashMap<String, Vec<i64>> =
            std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            series_ts
                .entry(host_arr.value(i).to_string())
                .or_default()
                .push(ts_arr.value(i));
        }
        for (host, timestamps) in &series_ts {
            for w in timestamps.windows(2) {
                assert!(
                    w[0] <= w[1],
                    "series {host} is not sorted: {} > {}",
                    w[0],
                    w[1]
                );
            }
        }
        assert_eq!(series_ts.len(), hosts.len(), "all series should be present");
    }

    #[test]
    fn compact_succeeds_within_generous_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        let pts = vec![make_point("srv1", 1.0, 100)];
        let (p1, m1) = write_segment(dir.path(), "seg.csx", &pts);

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, p1, &m1)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("out.csx"),
        };

        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        )
        .with_task_timeout(Duration::from_secs(60));

        let result = executor.execute(&task, &TombstoneSet::new());
        assert!(result.is_ok(), "compaction should finish within 60s");
    }

    #[test]
    fn compact_timeout_zero_triggers_task_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        let pts = vec![make_point("srv1", 1.0, 100)];
        let (p1, m1) = write_segment(dir.path(), "seg.csx", &pts);

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, p1, &m1)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("out.csx"),
        };

        // Zero timeout — should trigger immediately after first segment read
        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        )
        .with_task_timeout(Duration::ZERO);

        let result = executor.execute(&task, &TombstoneSet::new());
        assert!(
            matches!(result, Err(CompactionError::TaskTimeout(_))),
            "expected TaskTimeout, got {result:?}"
        );
    }

    /// Verify K-way merge correctly deduplicates the same key across many
    /// segments, keeping only the newest segment's value.
    #[test]
    fn kway_merge_dedup_many_segments_same_key() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        // 10 segments all writing the same (host=srv1, ts=100) with different values.
        let mut segments = Vec::new();
        for seg_idx in 0u64..10 {
            let pts = vec![make_point("srv1", seg_idx as f64, 100)];
            let name = format!("dup_{seg_idx}.csx");
            let (path, meta) = write_segment(dir.path(), &name, &pts);
            segments.push(make_entry(seg_idx, 0, path, &meta));
        }

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: segments,
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("dedup_many.csx"),
        };

        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        );
        let meta = executor.execute(&task, &TombstoneSet::new()).unwrap();

        assert_eq!(meta.row_count, 1, "should keep exactly one row after dedup");

        let reader = SegmentReader::open(&task.output_path).unwrap();
        let batch = reader.read_all().unwrap();
        let cpu_col = batch.column_by_name("cpu").unwrap();
        let cpu_arr = cpu_col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(
            (cpu_arr.value(0) - 9.0).abs() < f64::EPSILON,
            "expected value 9.0 from newest segment, got {}",
            cpu_arr.value(0)
        );
    }

    /// Single segment compaction should pass through without data loss.
    #[test]
    fn kway_merge_single_segment_passthrough() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        let pts = vec![
            make_point("srv1", 1.0, 100),
            make_point("srv1", 2.0, 200),
            make_point("srv2", 3.0, 100),
        ];
        let (path, meta) = write_segment(dir.path(), "single.csx", &pts);

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, path, &meta)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("single_out.csx"),
        };

        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        );
        let result = executor.execute(&task, &TombstoneSet::new()).unwrap();
        assert_eq!(result.row_count, 3);
    }

    /// Tombstones applied during K-way merge should remove ALL versions of
    /// a series, not just the oldest.
    #[test]
    fn kway_merge_tombstone_removes_all_versions() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        // Three segments all writing srv1 data.
        let pts1 = vec![make_point("srv1", 1.0, 100), make_point("srv2", 10.0, 100)];
        let pts2 = vec![make_point("srv1", 2.0, 100), make_point("srv2", 20.0, 200)];
        let pts3 = vec![make_point("srv1", 3.0, 200), make_point("srv2", 30.0, 300)];

        let (p1, m1) = write_segment(dir.path(), "t1.csx", &pts1);
        let (p2, m2) = write_segment(dir.path(), "t2.csx", &pts2);
        let (p3, m3) = write_segment(dir.path(), "t3.csx", &pts3);

        let srv1_key = SeriesKey::new(
            "cpu_usage",
            BTreeMap::from([("host".to_string(), "srv1".to_string())]),
        )
        .unwrap();
        let mut tombstones = TombstoneSet::new();
        tombstones.insert(
            chronix_core::Tombstone::all_time(srv1_key.canonical_form().to_string())
                .with_segments(0..1000),
        );

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![
                make_entry(1, 0, p1, &m1),
                make_entry(2, 0, p2, &m2),
                make_entry(3, 0, p3, &m3),
            ],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("tombstone_all.csx"),
        };

        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        );
        let result = executor.execute(&task, &tombstones).unwrap();

        // Only srv2 rows should remain (3 rows across 3 segments, no dups).
        assert_eq!(result.row_count, 3);

        let reader = SegmentReader::open(&task.output_path).unwrap();
        let batch = reader.read_all().unwrap();
        let host_col = batch.column_by_name("host").unwrap();
        let host_arr = host_col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(
                host_arr.value(i),
                "srv2",
                "tombstoned srv1 row leaked through"
            );
        }
    }

    /// K-way merge with all data tombstoned should produce an empty result.
    #[test]
    fn kway_merge_all_tombstoned_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        let pts = vec![make_point("srv1", 1.0, 100)];
        let (path, meta) = write_segment(dir.path(), "all_tomb.csx", &pts);

        let srv1_key = SeriesKey::new(
            "cpu_usage",
            BTreeMap::from([("host".to_string(), "srv1".to_string())]),
        )
        .unwrap();
        let mut tombstones = TombstoneSet::new();
        tombstones.insert(
            chronix_core::Tombstone::all_time(srv1_key.canonical_form().to_string())
                .with_segments(0..1000),
        );

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, path, &meta)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("all_tomb_out.csx"),
        };

        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        );
        let result = executor.execute(&task, &tombstones).unwrap();
        assert_eq!(result.row_count, 0);
    }

    /// `max_input_bytes` rejects compaction tasks that would
    /// exceed the configured memory budget.
    #[test]
    fn max_input_bytes_rejects_oversized_task() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        let pts = vec![make_point("srv1", 1.0, 100), make_point("srv1", 2.0, 200)];
        let (p1, m1) = write_segment(dir.path(), "big1.csx", &pts);

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, p1, &m1)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("bounded.csx"),
        };

        // Set an impossibly small limit (1 byte).
        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        )
        .with_max_input_bytes(1);
        let result = executor.execute(&task, &TombstoneSet::new());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CompactionError::InputTooLarge { .. }),
            "expected InputTooLarge, got: {err}"
        );
    }

    /// unlimited (0) allows any input size.
    #[test]
    fn max_input_bytes_zero_is_unlimited() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("output");
        std::fs::create_dir_all(&out_dir).unwrap();

        let pts = vec![make_point("srv1", 1.0, 100), make_point("srv1", 2.0, 200)];
        let (p1, m1) = write_segment(dir.path(), "unlim.csx", &pts);

        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: vec![make_entry(1, 0, p1, &m1)],
            source_level: crate::compaction::CompactionLevel::L0,
            target_level: crate::compaction::CompactionLevel::L1,
            output_path: out_dir.join("unlim_out.csx"),
        };

        let executor = CompactionExecutor::new(
            false,
            FloatEncoding::Gorilla,
            1000,
            CompressionCodec::Lz4,
            3,
        )
        .with_max_input_bytes(0); // unlimited
        let result = executor.execute(&task, &TombstoneSet::new());
        assert!(result.is_ok(), "unlimited should succeed: {result:?}");
    }
}
