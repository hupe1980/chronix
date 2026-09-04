//! K-way merge deduplication for overlapping segments.
//!
//! When multiple segments for the same shard may overlap in time, their
//! `RecordBatch` results must be merged and deduplicated. The dedup strategy
//! is **last-write-wins**: on duplicate `(series_key, timestamp)`, the
//! value from the segment with the highest WAL sequence number is kept.
//!
//! # Streaming Merge Architecture
//!
//! The k-way merge uses a `BinaryHeap` of per-batch cursors and
//! materialises the final result via `arrow::compute::interleave`
//! instead of `concat_batches` + `take`. This avoids creating a full
//! concatenated backing batch, cutting peak memory roughly in half.
//!
//! ## Algorithm
//!
//! Instead of concatenating all batches and performing a full O(N log N)
//! lexsort, this module uses a streaming **k-way merge** with a min-heap
//! of segment cursors:
//!
//! 1. Each input `RecordBatch` is internally sorted by
//!    `(timestamp, series_hash, series_group_id)`.
//! 2. A `BinaryHeap` (used as a min-heap) maintains one cursor per batch,
//!    keyed on the current row's sort key.
//! 3. On each step, the minimum entry is popped. If subsequent entries share
//!    the same dedup key `(timestamp, group_id)`, they are also popped and
//!    the entry from the **latest** batch (highest `batch_idx`) wins
//!    (last-write-wins semantics).
//! 4. The merged result is collected as `(cursor_idx, row_idx)` pairs and
//!    materialised into a final `RecordBatch` via Arrow `take` on a
//!    concatenated backing batch.
//!
//! Complexity: O(N log K) where K = number of overlapping batches, vs the
//! previous O(N log N) full-sort approach. When segments are already sorted
//! by timestamp (the common case), intra-batch sorting is nearly free.
//!
//! Series identity is determined by the canonical form
//! (`measurement\0tag1=v1\0tag2=v2`) — the FNV-1a hash is used as a fast
//! grouping key for sort order, and unique canonical forms are mapped to
//! integer group IDs to avoid O(N) heap-allocated strings while
//! still preventing false-positive dedup from hash collisions.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fnv::FnvHasher;
use std::hash::Hasher;

use crate::error::{QueryError, Result};

/// Build a union schema from all batches, preserving field order from the
/// first batch and appending any new fields discovered in later batches.
fn build_union_schema(batches: &[RecordBatch]) -> Arc<Schema> {
    let mut fields: Vec<Field> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for batch in batches {
        for field in batch.schema().fields() {
            if seen.insert(field.name().clone()) {
                // Mark as nullable since not all batches may have this column
                let f = if batches.len() > 1 {
                    Field::new(field.name(), field.data_type().clone(), true)
                } else {
                    field.as_ref().clone()
                };
                fields.push(f);
            }
        }
    }

    Arc::new(Schema::new(fields))
}

/// Align a single batch to the target schema.
///
/// Columns present in the target but missing from the batch are filled
/// with typed null arrays of the batch's row count. Columns in the batch
/// but not in the target are dropped (the target schema is the authority).
fn align_batch(batch: &RecordBatch, target: &Arc<Schema>) -> Result<RecordBatch> {
    let cols: Vec<ArrayRef> = target
        .fields()
        .iter()
        .map(|field| {
            match batch.schema().index_of(field.name()) {
                Ok(idx) => Arc::clone(batch.column(idx)),
                Err(_) => {
                    // Column missing — create a typed null array matching the
                    // target field's data type so the RecordBatch constructor
                    // accepts it.
                    new_null_array(field.data_type(), batch.num_rows())
                }
            }
        })
        .collect();

    RecordBatch::try_new(target.clone(), cols).map_err(QueryError::Arrow)
}

/// Merge and deduplicate multiple `RecordBatch`es using k-way merge.
///
/// Deduplication key is `(series_canonical, timestamp)` where the series
/// canonical is `measurement\0tag1=v1\0tag2=v2`, matching
/// [`chronix_core::SeriesKey::canonical_form()`]. An FNV-1a hash is used
/// as a fast grouping key during sort, but the canonical string is the
/// authoritative identity — preventing hash-collision data loss.
///
/// Tag columns are identified by Arrow field metadata (`role=tag`), or as a
/// fallback, all `Utf8` columns except `"timestamp"`.
///
/// When multiple rows share the same `(series_canonical, timestamp)`, the
/// row from the batch appearing **later** in the input list wins
/// (last-write-wins semantics, since newer segments are listed last).
///
/// If only one batch is provided, it is returned as-is (fast path).
///
/// # Algorithm
///
/// Each input batch is internally sorted by `(timestamp, series_hash,
/// group_id)`, then a min-heap merges all batches in O(N log K) time
/// (K = batch count), compared to a O(N log N) full sort.
///
/// # Errors
///
/// Returns an error if schema merging or sorting fails, or if the
/// optional `memory_tracker` budget is exceeded.
pub fn sort_merge_dedup(
    batches: Vec<RecordBatch>,
    measurement: &str,
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(QueryError::Validation("no batches to deduplicate".into()));
    }

    // Fast path: single batch, no dedup needed.
    if batches.len() == 1 {
        return batches
            .into_iter()
            .next()
            .ok_or_else(|| QueryError::Validation("expected single batch".into()));
    }

    let (schema, plan) = plan_merge(batches, measurement, memory_tracker)?;
    let Some(plan) = plan else {
        return Ok(RecordBatch::new_empty(schema));
    };

    // `usize::MAX` chunking emits the whole merge as a single batch.
    Ok(merge_chunks(&plan, usize::MAX, memory_tracker)?
        .pop()
        .unwrap_or_else(|| RecordBatch::new_empty(schema)))
}

/// Streaming chunked variant of [`sort_merge_dedup`].
///
/// Instead of materialising a single giant `RecordBatch` from the k-way
/// merge, this emits output in row-group-sized chunks of `chunk_size` rows,
/// bounding peak output memory to `O(chunk_size × columns)` regardless of how
/// many rows survive dedup.
///
/// Merge and dedup semantics (last-write-wins, FNV + canonical identity) are
/// identical to [`sort_merge_dedup`].
///
/// # Errors
///
/// Returns an error if schema merging or sorting fails, or if the optional
/// `memory_tracker` budget is exceeded.
pub fn sort_merge_dedup_chunked(
    batches: Vec<RecordBatch>,
    measurement: &str,
    chunk_size: usize,
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Err(QueryError::Validation("no batches to deduplicate".into()));
    }

    // Fast path: one input, so nothing can be a duplicate of anything —
    // but the rows still have to come out in timestamp order, because that
    // is what this function promises and what every consumer assumes. A
    // segment is written series-major, so returning it as it lies handed
    // back rows that jumped backwards in time whenever a segment held more
    // than one series: the streaming rollup accumulator closed a bucket on
    // the first series and then saw the second, and last-write-wins kept
    // one series' worth of a twenty-series aggregate.
    if batches.len() == 1 {
        let batch = batches
            .into_iter()
            .next()
            .ok_or_else(|| QueryError::Validation("expected single batch".into()))?;
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }
        let sorted = sort_batch_by_timestamp(&batch)?;
        return Ok(chunk_batch(&sorted, chunk_size));
    }

    let (_schema, plan) = plan_merge(batches, measurement, memory_tracker)?;
    let Some(plan) = plan else {
        return Ok(vec![]);
    };
    merge_chunks(&plan, chunk_size, memory_tracker)
}

/// Sort a batch ascending by timestamp, stably, so equal timestamps keep
/// their relative order (which is write order, and so last-write-wins).
///
/// A no-op when the batch is already ordered, which is the common case:
/// the memtable and every merged output are ordered already.
fn sort_batch_by_timestamp(batch: &RecordBatch) -> Result<RecordBatch> {
    let Some(ts) = batch
        .column_by_name("timestamp")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
    else {
        // No timestamp column to order by (a projection that dropped it);
        // the caller gets what it asked for.
        return Ok(batch.clone());
    };
    if ts.values().windows(2).all(|w| w[0] <= w[1]) {
        return Ok(batch.clone());
    }
    let mut order: Vec<u32> = (0..batch.num_rows() as u32).collect();
    order.sort_by_key(|&i| (ts.value(i as usize), i));
    let indices = arrow::array::UInt32Array::from(order);
    let columns = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Metadata cursor for a batch participating in k-way merge.
struct MergeCursor {
    /// Original position in the input batch list (higher = newer for LWW).
    batch_idx: usize,
    /// Pre-extracted timestamps for O(1) comparison.
    timestamps: Vec<i64>,
    /// Pre-computed FNV-1a series hashes for sort ordering.
    series_hashes: Vec<u64>,
    /// Pre-computed collision-proof integer group IDs (shared across batches).
    group_ids: Vec<u32>,
    /// Total number of rows in the (sorted) batch.
    num_rows: usize,
}

/// Entry in the min-heap for k-way merge.
///
/// Ordering is reversed so that `BinaryHeap` (a max-heap) acts as a min-heap
/// over `(timestamp, series_hash, group_id, batch_idx)`.
#[derive(Eq, PartialEq)]
struct HeapEntry {
    timestamp: i64,
    series_hash: u64,
    group_id: u32,
    /// Original batch position (for last-write-wins tie-breaking).
    batch_idx: usize,
    /// Index into the `cursors` / `sorted_batches` arrays.
    cursor_idx: usize,
    /// Row position within the cursor's sorted batch.
    row: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed comparison → `BinaryHeap` (max-heap) becomes a min-heap.
        //
        // Primary sort: (timestamp, series_hash, group_id) ascending.
        // Tie-breaker: batch_idx ascending — earlier batches are popped first
        // so that later batches can overwrite during last-write-wins dedup.
        let self_key = (
            self.timestamp,
            self.series_hash,
            self.group_id,
            self.batch_idx,
        );
        let other_key = (
            other.timestamp,
            other.series_hash,
            other.group_id,
            other.batch_idx,
        );
        other_key.cmp(&self_key)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Everything the k-way merge needs: the union schema, one cursor per
/// non-empty input, and the inputs sorted into merge order.
struct MergePlan {
    schema: Arc<Schema>,
    cursors: Vec<MergeCursor>,
    sorted_batches: Vec<RecordBatch>,
}

/// Align the inputs to a union schema and sort each one into merge order.
///
/// Returns `Ok(None)` when every input is empty, in which case the caller
/// only needs the union schema (also returned) to build an empty result.
fn plan_merge(
    batches: Vec<RecordBatch>,
    measurement: &str,
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<(Arc<Schema>, Option<MergePlan>)> {
    let schema = build_union_schema(&batches);

    // Pre-check memory budget against estimated output size
    // (total input rows × schema byte-width).
    if let Some(tracker) = memory_tracker {
        let total_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        let estimated_row_bytes = schema.fields().len() * 8 + 16; // rough per-row estimate
        tracker.try_allocate(total_rows * estimated_row_bytes)?;
    }

    // Align all batches to the union schema by column name. Missing columns
    // are filled with null arrays, handling schema evolution (e.g. a field
    // added to a new segment but absent in older segments) and cardinality-
    // based column order differences.
    let aligned: Vec<(usize, RecordBatch)> = batches
        .into_iter()
        .map(|batch| {
            if batch.schema() == schema {
                Ok(batch)
            } else {
                align_batch(&batch, &schema)
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .enumerate()
        .filter(|(_, b)| b.num_rows() > 0)
        .collect();

    if aligned.is_empty() {
        return Ok((schema, None));
    }

    let time_idx = schema
        .index_of("timestamp")
        .map_err(|_| QueryError::Validation("'timestamp' column not found".into()))?;
    let tag_columns = resolve_tag_columns(&schema);

    // A shared group map ensures the same canonical series string receives
    // the same integer group ID across all batches, which is essential for
    // correct cross-batch dedup.
    let mut group_map: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut next_group_id: u32 = 0;
    let mut cursors: Vec<MergeCursor> = Vec::with_capacity(aligned.len());
    let mut sorted_batches: Vec<RecordBatch> = Vec::with_capacity(aligned.len());

    for (batch_idx, batch) in &aligned {
        let hashes = compute_series_hashes(batch, measurement, &tag_columns);
        let group_ids = compute_series_group_ids_into(
            batch,
            measurement,
            &tag_columns,
            &mut group_map,
            &mut next_group_id,
        );

        // Sort each batch internally by (timestamp, series_hash, group_id).
        // For the common case of pre-sorted single-series segments this is
        // essentially a no-op (already sorted).
        let hash_array: UInt64Array = hashes.iter().copied().collect();
        let gid_array: UInt32Array = group_ids.iter().copied().collect();
        let ascending = Some(compute::SortOptions {
            descending: false,
            nulls_first: false,
        });
        let sort_columns = vec![
            compute::SortColumn {
                values: Arc::clone(batch.column(time_idx)),
                options: ascending,
            },
            compute::SortColumn {
                values: Arc::new(hash_array) as ArrayRef,
                options: ascending,
            },
            compute::SortColumn {
                values: Arc::new(gid_array) as ArrayRef,
                options: ascending,
            },
        ];

        let sort_indices = compute::lexsort_to_indices(&sort_columns, None)?;
        let indices_values = sort_indices.values();

        let sorted_cols: std::result::Result<Vec<ArrayRef>, _> = batch
            .columns()
            .iter()
            .map(|col| compute::take(col.as_ref(), &sort_indices, None))
            .collect();
        let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_cols?)?;

        let sorted_timestamps: Vec<i64> = {
            let ts = sorted_batch
                .column(time_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Validation("timestamp column must be Int64".into()))?;
            ts.values().to_vec()
        };
        let sorted_hashes: Vec<u64> = indices_values.iter().map(|&i| hashes[i as usize]).collect();
        let sorted_gids: Vec<u32> = indices_values
            .iter()
            .map(|&i| group_ids[i as usize])
            .collect();

        cursors.push(MergeCursor {
            batch_idx: *batch_idx,
            timestamps: sorted_timestamps,
            series_hashes: sorted_hashes,
            group_ids: sorted_gids,
            num_rows: sorted_batch.num_rows(),
        });
        sorted_batches.push(sorted_batch);
    }

    Ok((
        schema.clone(),
        Some(MergePlan {
            schema,
            cursors,
            sorted_batches,
        }),
    ))
}

/// Run the k-way merge, emitting a `RecordBatch` every `chunk_size` rows.
///
/// `chunk_size == usize::MAX` yields at most one batch, which is how the
/// non-chunked [`sort_merge_dedup`] is expressed.
///
/// Surviving rows are recorded as `(cursor, row)` pairs, exactly the form
/// `arrow::compute::interleave` consumes — the merge already knows both, so
/// there is nothing to pack into a flat index and nothing to search back out
/// of one.
fn merge_chunks(
    plan: &MergePlan,
    chunk_size: usize,
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<Vec<RecordBatch>> {
    let MergePlan {
        schema,
        cursors,
        sorted_batches,
    } = plan;

    let mut heap = BinaryHeap::with_capacity(cursors.len());
    for (ci, cursor) in cursors.iter().enumerate() {
        if cursor.num_rows > 0 {
            heap.push(HeapEntry {
                timestamp: cursor.timestamps[0],
                series_hash: cursor.series_hashes[0],
                group_id: cursor.group_ids[0],
                batch_idx: cursor.batch_idx,
                cursor_idx: ci,
                row: 0,
            });
        }
    }

    // Track current row position per cursor (starts at 0, the row already
    // pushed to the heap).
    let mut cursor_pos: Vec<usize> = vec![0; cursors.len()];
    let mut chunks: Vec<RecordBatch> = Vec::new();
    let mut kept: Vec<(usize, usize)> = Vec::with_capacity(chunk_size.min(65_536));

    // Push the cursor's next row onto the heap, if it has one.
    macro_rules! advance {
        ($ci:expr) => {{
            let ci = $ci;
            cursor_pos[ci] += 1;
            let nr = cursor_pos[ci];
            if nr < cursors[ci].num_rows {
                heap.push(HeapEntry {
                    timestamp: cursors[ci].timestamps[nr],
                    series_hash: cursors[ci].series_hashes[nr],
                    group_id: cursors[ci].group_ids[nr],
                    batch_idx: cursors[ci].batch_idx,
                    cursor_idx: ci,
                    row: nr,
                });
            }
        }};
    }

    while let Some(entry) = heap.pop() {
        let dedup_key = (entry.timestamp, entry.group_id);
        advance!(entry.cursor_idx);

        // Determine the winner for this dedup key. The current entry is the
        // initial candidate; any subsequent entries with the same key are
        // popped and compared. Because the heap pops lower batch_idx first
        // for equal keys, the last one popped is the "newest" write.
        let mut best_cursor = entry.cursor_idx;
        let mut best_row = entry.row;
        let mut best_batch = entry.batch_idx;

        while let Some(next) = heap.peek() {
            if (next.timestamp, next.group_id) != dedup_key {
                break;
            }
            let next = heap
                .pop()
                .ok_or_else(|| QueryError::Validation("heap peek and pop disagreed".into()))?;
            advance!(next.cursor_idx);

            // Last-write-wins: higher batch_idx wins; within the same batch
            // the later occurrence wins.
            if next.batch_idx > best_batch || (next.batch_idx == best_batch && next.row > best_row)
            {
                best_cursor = next.cursor_idx;
                best_row = next.row;
                best_batch = next.batch_idx;
            }
        }

        kept.push((best_cursor, best_row));

        if kept.len() >= chunk_size {
            chunks.push(materialize_chunk(
                &kept,
                sorted_batches,
                schema,
                memory_tracker,
            )?);
            kept.clear();
        }
    }

    if !kept.is_empty() {
        chunks.push(materialize_chunk(
            &kept,
            sorted_batches,
            schema,
            memory_tracker,
        )?);
    }

    Ok(chunks)
}

/// Materialise `(cursor, row)` pairs into a `RecordBatch` via interleave.
fn materialize_chunk(
    pairs: &[(usize, usize)],
    sorted_batches: &[RecordBatch],
    schema: &Arc<Schema>,
    memory_tracker: Option<&crate::memory::MemoryTracker>,
) -> Result<RecordBatch> {
    let mut result_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for col_idx in 0..schema.fields().len() {
        let col_arrays: Vec<&dyn Array> = sorted_batches
            .iter()
            .map(|b| b.column(col_idx).as_ref())
            .collect();
        result_columns.push(compute::interleave(&col_arrays, pairs)?);
    }

    let batch = RecordBatch::try_new(schema.clone(), result_columns).map_err(QueryError::Arrow)?;
    if let Some(tracker) = memory_tracker {
        tracker.try_allocate(batch.get_array_memory_size())?;
    }
    Ok(batch)
}

/// Split a single `RecordBatch` into chunks of at most `chunk_size` rows.
fn chunk_batch(batch: &RecordBatch, chunk_size: usize) -> Vec<RecordBatch> {
    let total = batch.num_rows();
    if total <= chunk_size {
        return vec![batch.clone()];
    }
    let mut chunks = Vec::with_capacity(total.div_ceil(chunk_size));
    let mut offset = 0;
    while offset < total {
        let len = (total - offset).min(chunk_size);
        chunks.push(batch.slice(offset, len));
        offset += len;
    }
    chunks
}

/// Resolve which columns in the schema are tag columns.
///
/// Prefers the Arrow field metadata (`role=tag`) annotation when
/// available. Falls back to treating all `Utf8` columns except
/// `"timestamp"` as tags — this matches the heuristic used elsewhere
/// in the codebase.
fn resolve_tag_columns(schema: &Schema) -> Vec<(usize, String)> {
    // First try explicit metadata
    let explicit: Vec<(usize, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.metadata().get("role").map(std::string::String::as_str) == Some("tag"))
        .map(|(i, f)| (i, f.name().clone()))
        .collect();
    if !explicit.is_empty() {
        return explicit;
    }
    // Fallback: all Utf8 columns except timestamp
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.data_type() == &DataType::Utf8 && f.name() != "timestamp")
        .map(|(i, f)| (i, f.name().clone()))
        .collect()
}

/// Compute per-row FNV-1a series hashes matching `SeriesKey::hash_fnv()`.
///
/// The canonical form is `measurement\0tag1=v1\0tag2=v2` with tags sorted
/// by name.
fn compute_series_hashes(
    batch: &RecordBatch,
    measurement: &str,
    tag_columns: &[(usize, String)],
) -> Vec<u64> {
    let num_rows = batch.num_rows();
    let mut hashes = Vec::with_capacity(num_rows);

    // Sort tag columns by name for deterministic hashing (BTreeMap order)
    let mut sorted_tags: Vec<(usize, &str)> = tag_columns
        .iter()
        .map(|(idx, name)| (*idx, name.as_str()))
        .collect();
    sorted_tags.sort_unstable_by_key(|&(_, name)| name);

    // Pre-fetch tag arrays
    let tag_arrays: Vec<Option<&StringArray>> = sorted_tags
        .iter()
        .map(|&(col_idx, _)| batch.column(col_idx).as_any().downcast_ref::<StringArray>())
        .collect();

    for row in 0..num_rows {
        let mut hasher = FnvHasher::default();
        hasher.write(measurement.as_bytes());

        for (tag_idx, &(_, tag_name)) in sorted_tags.iter().enumerate() {
            if let Some(arr) = tag_arrays[tag_idx] {
                if !arr.is_null(row) {
                    hasher.write_u8(0);
                    hasher.write(tag_name.as_bytes());
                    hasher.write_u8(b'=');
                    hasher.write(arr.value(row).as_bytes());
                }
            }
        }

        hashes.push(hasher.finish());
    }

    hashes
}

/// Compute per-row integer group IDs for series identity.
///
/// Builds canonical strings using a single scratch buffer, assigns a
/// unique `u32` group ID to each distinct canonical via a shared `HashMap`,
/// and returns one group ID per row.  Only K `String` objects are allocated
/// (K = number of unique series), not N, making this O(N) in time but
/// O(K) in heap-allocation count.
///
/// The shared `group_map` / `next_id` parameters allow consistent group IDs
/// across multiple batches (essential for cross-batch dedup in the k-way
/// merge).
fn compute_series_group_ids_into(
    batch: &RecordBatch,
    measurement: &str,
    tag_columns: &[(usize, String)],
    group_map: &mut std::collections::HashMap<String, u32>,
    next_id: &mut u32,
) -> Vec<u32> {
    let num_rows = batch.num_rows();
    let mut group_ids = Vec::with_capacity(num_rows);

    // Sort tag columns by name for deterministic ordering (BTreeMap order)
    let mut sorted_tags: Vec<(usize, &str)> = tag_columns
        .iter()
        .map(|(idx, name)| (*idx, name.as_str()))
        .collect();
    sorted_tags.sort_unstable_by_key(|&(_, name)| name);

    // Pre-fetch tag arrays
    let tag_arrays: Vec<Option<&StringArray>> = sorted_tags
        .iter()
        .map(|&(col_idx, _)| batch.column(col_idx).as_any().downcast_ref::<StringArray>())
        .collect();

    let mut scratch = String::new();

    for row in 0..num_rows {
        scratch.clear();
        // Shares one canonical-format definition with `SeriesKey` — see
        // `chronix_core::push_canonical`.
        chronix_core::push_canonical(
            &mut scratch,
            measurement,
            sorted_tags
                .iter()
                .enumerate()
                .filter_map(|(tag_idx, &(_, tag_name))| {
                    let arr = tag_arrays[tag_idx]?;
                    (!arr.is_null(row)).then(|| (tag_name, arr.value(row)))
                }),
        );

        let id = if let Some(&id) = group_map.get(&*scratch) {
            id
        } else {
            let id = *next_id;
            *next_id += 1;
            group_map.insert(scratch.clone(), id);
            id
        };
        group_ids.push(id);
    }

    group_ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(times: Vec<i64>, values: Vec<f64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
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
    fn single_batch_passthrough() {
        let batch = make_batch(vec![100, 200, 300], vec![1.0, 2.0, 3.0]);
        let result = sort_merge_dedup(vec![batch.clone()], "cpu", None).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn merge_no_overlap() {
        let b1 = make_batch(vec![100, 200], vec![1.0, 2.0]);
        let b2 = make_batch(vec![300, 400], vec![3.0, 4.0]);

        let result = sort_merge_dedup(vec![b1, b2], "cpu", None).unwrap();
        assert_eq!(result.num_rows(), 4);

        let times = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(times.values(), &[100, 200, 300, 400]);
    }

    #[test]
    fn dedup_overlapping_last_write_wins() {
        // b1 is "older" (seg 1), b2 is "newer" (seg 2)
        let b1 = make_batch(vec![100, 200, 300], vec![1.0, 2.0, 3.0]);
        let b2 = make_batch(vec![200, 300, 400], vec![22.0, 33.0, 44.0]);

        // b2 listed LAST → it wins on conflict
        let result = sort_merge_dedup(vec![b1, b2], "cpu", None).unwrap();
        assert_eq!(result.num_rows(), 4); // 100, 200, 300, 400

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

        assert_eq!(times.values(), &[100, 200, 300, 400]);
        // ts=200 and ts=300 should have values from b2 (last-write-wins)
        assert!((values.value(0) - 1.0).abs() < f64::EPSILON); // ts=100 from b1
        assert!((values.value(1) - 22.0).abs() < f64::EPSILON); // ts=200 from b2
        assert!((values.value(2) - 33.0).abs() < f64::EPSILON); // ts=300 from b2
        assert!((values.value(3) - 44.0).abs() < f64::EPSILON); // ts=400 from b2
    }

    #[test]
    fn empty_batches_error() {
        let result = sort_merge_dedup(vec![], "cpu", None);
        assert!(result.is_err());
    }

    #[test]
    fn schema_evolution_missing_column_filled_with_nulls() {
        use arrow::array::StringArray;

        // b1 has (timestamp, value)
        let b1 = make_batch(vec![100, 200], vec![1.0, 2.0]);

        // b2 has (timestamp, value, extra_field) — schema evolution
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("extra_field", DataType::Utf8, true),
        ]));
        let b2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(Int64Array::from(vec![300, 400])),
                Arc::new(Float64Array::from(vec![3.0, 4.0])),
                Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
            ],
        )
        .unwrap();

        let result = sort_merge_dedup(vec![b1, b2], "cpu", None).unwrap();
        // Union schema should have 3 columns
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 4);

        let times = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(times.values(), &[100, 200, 300, 400]);
    }

    #[test]
    fn different_column_order_aligned() {
        // b1: (timestamp, host, region, value)
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]));
        let b1 = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(arrow::array::StringArray::from(vec!["srv1"])),
                Arc::new(arrow::array::StringArray::from(vec!["us-east"])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();

        // b2: (timestamp, region, host, value) — different order
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]));
        let b2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(Int64Array::from(vec![200])),
                Arc::new(arrow::array::StringArray::from(vec!["eu-west"])),
                Arc::new(arrow::array::StringArray::from(vec!["srv2"])),
                Arc::new(Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();

        let result = sort_merge_dedup(vec![b1, b2], "cpu", None).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 4);

        // Verify columns are in b1's order: timestamp, host, region, value
        assert_eq!(result.schema().field(1).name(), "host");
        assert_eq!(result.schema().field(2).name(), "region");

        let hosts = result
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(hosts.value(0), "srv1");
        assert_eq!(hosts.value(1), "srv2");
    }

    #[test]
    fn multi_series_same_timestamp_preserved() {
        // Two different series (host=srv1 vs host=srv2) at the SAME timestamp
        // must both be preserved after dedup.
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true)
                .with_metadata([("role".into(), "tag".into())].into()),
            Field::new("value", DataType::Float64, false),
        ]));

        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(StringArray::from(vec!["srv1", "srv1"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();

        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(StringArray::from(vec!["srv2", "srv2"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();

        let result = sort_merge_dedup(vec![b1, b2], "cpu", None).unwrap();
        // All 4 rows must survive — different series even though timestamps match
        assert_eq!(result.num_rows(), 4);

        let times = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Sorted by (timestamp, series_hash)
        assert_eq!(times.value(0), 100);
        assert_eq!(times.value(1), 100);
        assert_eq!(times.value(2), 200);
        assert_eq!(times.value(3), 200);

        let hosts = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Both srv1 and srv2 must appear at each timestamp
        let host_set: std::collections::HashSet<&str> = (0..4).map(|i| hosts.value(i)).collect();
        assert!(host_set.contains("srv1"));
        assert!(host_set.contains("srv2"));
    }

    #[test]
    fn dedup_respects_memory_tracker() {
        let b1 = make_batch(vec![100, 200], vec![1.0, 2.0]);
        let b2 = make_batch(vec![300, 400], vec![3.0, 4.0]);

        // Tracker with generous budget — should succeed.
        let tracker = crate::memory::MemoryTracker::new(1_000_000);
        let result = sort_merge_dedup(vec![b1.clone(), b2.clone()], "cpu", Some(&tracker)).unwrap();
        assert_eq!(result.num_rows(), 4);
        assert!(
            tracker.allocated() > 0,
            "tracker should have charged memory"
        );

        // Tracker with tiny budget — should fail.
        let tiny_tracker = crate::memory::MemoryTracker::new(1);
        let err = sort_merge_dedup(vec![b1, b2], "cpu", Some(&tiny_tracker));
        assert!(err.is_err(), "should fail when memory budget exceeded");
    }
}

#[cfg(test)]
mod ordering_tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// A single input is returned in timestamp order, not in the order it
    /// happens to lie on disk.
    ///
    /// A segment is written series-major, so its rows jump backwards in
    /// time at every series boundary. Handing that back unsorted broke
    /// every consumer that assumed the promised order — the streaming
    /// rollup accumulator closed a bucket on the first series and then saw
    /// the second, and last-write-wins kept one series' share of the
    /// aggregate.
    #[test]
    fn a_single_series_major_batch_comes_back_in_time_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("v", DataType::Float64, true),
        ]));
        // host a at t=1,2,3 then host b at t=1,2,3 — series-major.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "b"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])),
            ],
        )
        .unwrap();

        let out = sort_merge_dedup_chunked(vec![batch], "m", 1024, None).unwrap();
        assert_eq!(out.len(), 1);
        let ts = out[0]
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.values(), &[1, 1, 2, 2, 3, 3]);
        // Both series survive: this sorts, it does not deduplicate across
        // series that merely share a timestamp.
        assert_eq!(out[0].num_rows(), 6);
    }

    /// An already-ordered batch is passed through untouched.
    #[test]
    fn an_ordered_batch_is_not_reshuffled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("v", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        let out = sort_merge_dedup_chunked(vec![batch], "m", 1024, None).unwrap();
        let v = out[0]
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(v.values(), &[1.0, 2.0, 3.0]);
    }
}
