//! Segment reader — reads `.csx` segment files.
//!
//! The reader validates the file integrity (magic bytes, `CRC32c` checksum),
//! parses header/footer/metadata, and provides methods to read individual
//! columns or entire row groups as Apache Arrow arrays.
//!
//! # I/O strategy — memory-mapped prefetch
//!
//! The entire segment file is **memory-mapped** via [`memmap2::Mmap`].  No
//! per-column `seek()`/`read()` system calls are issued; instead, column data
//! is accessed by slicing the mapped byte range, and the OS page cache
//! transparently pages data in from disk on first access.
//!
//! Two advisory hints accelerate sequential scans:
//!
//! 1. **`madvise(MADV_SEQUENTIAL)`** — set once at open time, tells the kernel
//!    the file will be read front-to-back so it can perform aggressive
//!    read-ahead (typically 128–256 KiB on Linux).
//! 2. **`madvise(MADV_WILLNEED)`** — issued per-column in
//!    [`SegmentReader::read_columns`]: while decoding column *N*, the reader
//!    hints the kernel to begin paging in column *N+1* (and the same column
//!    in the next row group for multi-row-group scans).  This overlaps I/O
//!    with CPU decoding.
//!
//! Because the `Mmap` holds the entire file mapping, **there is no separate
//! read-ahead buffer or prefetch thread**; the OS page cache *is* the buffer.
//! This is the most efficient approach for columnar segment files that fit in
//! the page cache — it avoids double-copying into user-space buffers and lets
//! the kernel coalesce I/O for adjacent columns.
//!
//! # Predicate pushdown
//!
//! The reader supports multi-level predicate pushdown to minimise the amount
//! of data that is read and decoded:
//!
//! | Level | Mechanism | Method |
//! |---|---|---|
//! | **Segment** | Catalog-level time-range + tag-index + bloom filter | `Chronix::prune_segments()` |
//! | **Row group — time** | `min_ts`/`max_ts` from timestamp column stats | `read_projected_filtered` |
//! | **Row group — tags** | `value_count == 0` with a non-zero `null_count` (a genuinely all-null block → impossible match) | `read_projected_filtered_with_predicates` |
//! | **Column** | projection pushdown (only requested columns decoded) | `read_projected` |

use std::fs::File;
use std::path::{Path, PathBuf};
// The only bare `Arc` in this file is the key provider, which is gated; every
// other use is written out as `std::sync::Arc`. Without the same gate the
// import is unused under `--no-default-features`.
#[cfg(feature = "field-encryption")]
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use memmap2::{Advice, Mmap, UncheckedAdvice};

use chronix_encoding::{ColumnDecoder, DecodedColumn, EncodedBlock};

use crate::segment::compression::decompress_block_with_optional_dict;
use crate::segment::error::{Result, SegmentError};
use crate::segment::header::{SegmentFooter, SegmentHeader, FOOTER_SIZE, HEADER_SIZE};
use crate::segment::metadata::roles;
use crate::segment::metadata::{data_types, ColumnBlockMeta, SegmentMetadata};
use crate::segment::stats::ordered_i64_to_f64;

// ── Zone-map field predicates for late-materialisation ─────────────────

/// A numeric comparison operator for zone-map row-group pruning.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ZoneMapOp {
    /// Equals.
    Eq,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    GtEq,
    /// Less than.
    Lt,
    /// Less than or equal.
    LtEq,
}

/// A field predicate that can be evaluated against per-row-group
/// [`ColumnStats`](crate::segment::stats::ColumnStats) min/max to skip row groups
/// that cannot possibly match.
#[derive(Debug, Clone)]
pub struct FieldPredicate {
    /// Column name.
    pub column: String,
    /// Comparison operator.
    pub op: ZoneMapOp,
    /// Predicate value as `f64`.  Integer predicates are cast to `f64`.
    pub value: f64,
}

impl FieldPredicate {
    /// Test whether a row group with the given float min/max statistics
    /// could possibly contain rows matching this predicate.
    ///
    /// A NaN bound makes every comparison false, which would prune the
    /// group — so an unusable bound is reported as "may match" instead.
    /// **A pruning step may only ever be wrong in the direction of doing
    /// more work**; one that can drop rows is a wrong answer, not a slow one.
    fn may_match_f64(&self, rg_min: f64, rg_max: f64) -> bool {
        if rg_min.is_nan() || rg_max.is_nan() || rg_min > rg_max {
            return true;
        }
        match self.op {
            ZoneMapOp::Eq => self.value >= rg_min && self.value <= rg_max,
            ZoneMapOp::Gt => rg_max > self.value,
            ZoneMapOp::GtEq => rg_max >= self.value,
            ZoneMapOp::Lt => rg_min < self.value,
            ZoneMapOp::LtEq => rg_min <= self.value,
        }
    }

    /// Test whether a row group with i64 min/max statistics could
    /// possibly match.
    ///
    /// The predicate value is an `f64`, so it is compared as one: casting
    /// it to `i64` first truncated the bound, and `n < 1.5` then pruned a
    /// row group holding `{1, 2, 3}` because `1 < 1` is false. Above 2^53 an
    /// `i64` bound is not representable as an `f64` at all, and the group is
    /// kept rather than compared wrongly.
    fn may_match_i64(&self, rg_min: i64, rg_max: i64) -> bool {
        if rg_min > rg_max {
            return true;
        }
        const EXACT: i64 = 1 << 53;
        if rg_min <= -EXACT || rg_max >= EXACT {
            return true;
        }
        #[allow(clippy::cast_precision_loss)] // bounded above by 2^53
        let (lo, hi) = (rg_min as f64, rg_max as f64);
        match self.op {
            // The column is integral, so an equality against a fractional
            // value cannot match any row in it, whatever the range says.
            ZoneMapOp::Eq if self.value.fract() != 0.0 => false,
            ZoneMapOp::Eq => self.value >= lo && self.value <= hi,
            ZoneMapOp::Gt => hi > self.value,
            ZoneMapOp::GtEq => hi >= self.value,
            ZoneMapOp::Lt => lo < self.value,
            ZoneMapOp::LtEq => lo <= self.value,
        }
    }
}

/// Reads data from a `.csx` segment file.
///
/// # I/O model
///
/// The segment file is **memory-mapped** ([`Mmap`]) so all column block
/// reads are zero-copy slices into the OS page cache.  No per-column
/// `File::read()` calls are issued; instead the kernel pages data in on
/// first access and the `MADV_WILLNEED` advisory hint triggers
/// asynchronous prefetch of upcoming blocks while the current block is
/// being decoded.  This means sequential scans benefit from kernel-level
/// read-ahead without any user-space prefetch thread or buffer.
///
/// # Predicate pushdown
///
/// [`read_projected_filtered_with_predicates`](Self::read_projected_filtered_with_predicates)
/// evaluates predicates at row-group granularity using per-block
/// [`ColumnStats`](crate::segment::stats::ColumnStats) before touching any
/// column bytes.  This can skip entire row groups when the query's
/// time window doesn't overlap a row group's timestamp range, or when
/// a tag column's `value_count` proves it cannot match an equality
/// filter.
pub struct SegmentReader {
    /// Memory-mapped file data (zero-copy, backed by OS page cache).
    data: Mmap,
    /// Parsed segment header.
    header: SegmentHeader,
    /// Parsed segment metadata.
    metadata: SegmentMetadata,
    /// File path (for error messages).
    path: PathBuf,
    /// Optional field-level encryption key provider for decrypting
    /// encrypted column blocks at read time.
    #[cfg(feature = "field-encryption")]
    key_provider: Option<Arc<dyn crate::segment::field_encryption::FieldKeyProvider>>,
}

impl SegmentReader {
    /// Open a segment file, validate integrity, and parse metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the file doesn't exist, is corrupt, or has an
    /// invalid checksum.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        // SAFETY: The file is opened read-only and the mapping lives as long
        // as `SegmentReader`. External mutation of the underlying file while
        // mapped is undefined behaviour, but segment files are immutable
        // after write.
        #[allow(unsafe_code)] // audited: read-only mmap of an immutable segment file
        let data = unsafe { Mmap::map(&file)? };
        // Hint the OS that we will read sequentially through the
        // mapping.  On Linux this sets MADV_SEQUENTIAL which doubles the
        // default read-ahead window, reducing page faults on sequential scans.
        data.advise(Advice::Sequential)?;

        if data.len() < HEADER_SIZE + FOOTER_SIZE {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "file too small: {} bytes (need at least {})",
                    data.len(),
                    HEADER_SIZE + FOOTER_SIZE
                ),
            });
        }

        // Parse header
        let header = SegmentHeader::from_bytes(&data[..HEADER_SIZE])?;

        // Parse footer
        let footer_start = data.len() - FOOTER_SIZE;
        let footer = SegmentFooter::from_bytes(&data[footer_start..])?;

        // Verify CRC32c checksum (over everything except footer)
        let computed_checksum = crc32c::crc32c(&data[..footer_start]);
        if computed_checksum != footer.checksum {
            return Err(SegmentError::ChecksumMismatch {
                expected: footer.checksum,
                actual: computed_checksum,
            });
        }

        // Verify independent metadata checksum
        let metadata_start = footer.metadata_offset as usize;
        if metadata_start < footer_start {
            let metadata_bytes = &data[metadata_start..footer_start];
            let computed_meta_crc = crc32c::crc32c(metadata_bytes);
            if computed_meta_crc != footer.metadata_checksum {
                return Err(SegmentError::ChecksumMismatch {
                    expected: footer.metadata_checksum,
                    actual: computed_meta_crc,
                });
            }
        }

        // Parse metadata section
        if metadata_start < HEADER_SIZE {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "metadata_offset ({metadata_start}) is before end of header ({HEADER_SIZE})"
                ),
            });
        }
        if metadata_start > footer_start {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "metadata_offset ({metadata_start}) exceeds footer start ({footer_start})"
                ),
            });
        }
        let metadata = SegmentMetadata::from_bytes(&data[metadata_start..footer_start])?;

        // Validate that header column_count matches the actual metadata.
        if metadata.columns.len() != header.column_count as usize {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "column count mismatch: header says {}, metadata has {}",
                    header.column_count,
                    metadata.columns.len()
                ),
            });
        }

        Ok(Self {
            data,
            header,
            metadata,
            path,
            #[cfg(feature = "field-encryption")]
            key_provider: None,
        })
    }

    /// Returns the segment header.
    #[must_use]
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Returns the column metadata.
    #[must_use]
    pub fn column_metadata(&self) -> &[crate::segment::metadata::ColumnMeta] {
        &self.metadata.columns
    }

    /// Returns the number of row groups.
    #[must_use]
    pub fn row_group_count(&self) -> usize {
        self.metadata.row_group_blocks.len()
    }

    /// Returns the per-column block metadata for one row group.
    ///
    /// Each entry carries the codec that was actually used for that block —
    /// [`ColumnMeta::default_encoding`](crate::segment::metadata::ColumnMeta::default_encoding)
    /// is only the writer's initial guess — along with its on-disk length.
    /// Together they answer "which encoding won, and what did it cost", which
    /// is what compression regressions and codec benchmarks need to see.
    ///
    /// Returns `None` if `row_group` is out of range.
    #[must_use]
    pub fn row_group_blocks(&self, row_group: usize) -> Option<&[ColumnBlockMeta]> {
        self.metadata
            .row_group_blocks
            .get(row_group)
            .map(Vec::as_slice)
    }

    /// Returns the file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Set a field-level encryption key provider for decrypting encrypted
    /// column blocks.  Must be called before reading any encrypted columns.
    #[cfg(feature = "field-encryption")]
    pub fn set_key_provider(
        &mut self,
        provider: Arc<dyn crate::segment::field_encryption::FieldKeyProvider>,
    ) {
        self.key_provider = Some(provider);
    }

    /// Read specific columns from a specific row group as Arrow arrays.
    ///
    /// # Read-ahead / prefetch
    ///
    /// When reading column N, the reader issues an `madvise(MADV_WILLNEED)`
    /// hint for column N+1's byte range so the OS can begin paging it in
    /// from disk while the current column is being decoded.  Combined with
    /// the `Sequential` advice set at open time, this reduces I/O stalls
    /// on sequential scans.
    ///
    /// # Errors
    ///
    /// Returns an error if the row group index is out of range or a column
    /// is not found.
    pub fn read_columns(&self, column_names: &[&str], row_group: usize) -> Result<Vec<ArrayRef>> {
        if row_group >= self.metadata.row_group_blocks.len() {
            return Err(SegmentError::RowGroupOutOfRange {
                index: row_group,
                max: self.metadata.row_group_blocks.len().saturating_sub(1),
            });
        }

        let rg_blocks = &self.metadata.row_group_blocks[row_group];

        // Resolve all column indices and block metas upfront for prefetching.
        let resolved: Vec<(usize, &ColumnBlockMeta)> = column_names
            .iter()
            .map(|&name| {
                let col_idx = self
                    .metadata
                    .columns
                    .iter()
                    .position(|c| c.name == name)
                    .ok_or_else(|| SegmentError::ColumnNotFound {
                        name: name.to_string(),
                    })?;
                let block_meta = rg_blocks
                    .iter()
                    .find(|b| b.column_index == col_idx as u16)
                    .ok_or_else(|| SegmentError::ColumnNotFound {
                        name: name.to_string(),
                    })?;
                Ok((col_idx, block_meta))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut result = Vec::with_capacity(column_names.len());

        for (i, &(col_idx, block_meta)) in resolved.iter().enumerate() {
            // Prefetch the next column block via madvise(WillNeed)
            // so the OS starts paging it in while we decode the current one.
            if i + 1 < resolved.len() {
                let (_, next_block) = resolved[i + 1];
                self.prefetch_block(next_block);
            }

            // Also prefetch the same columns in the next row group if
            // we're doing a sequential multi-row-group scan.
            if row_group + 1 < self.metadata.row_group_blocks.len() {
                let next_rg_blocks = &self.metadata.row_group_blocks[row_group + 1];
                if let Some(next_rg_block) = next_rg_blocks
                    .iter()
                    .find(|b| b.column_index == col_idx as u16)
                {
                    self.prefetch_block(next_rg_block);
                }
            }

            let array = self.read_column_block(block_meta, col_idx)?;
            result.push(array);
        }

        Ok(result)
    }

    /// Issue an `madvise(MADV_WILLNEED)` hint for a column block's byte
    /// range, asking the OS to begin paging the data into the page cache.
    ///
    /// This is a non-blocking kernel hint — the data may already be in the
    /// page cache (in which case this is a no-op) or the kernel may begin
    /// an asynchronous read from disk.  Either way, the call returns
    /// immediately and never blocks the caller.
    ///
    /// Errors are silently ignored — prefetch is purely advisory.
    fn prefetch_block(&self, block_meta: &ColumnBlockMeta) {
        let start = block_meta.offset as usize;
        let len = block_meta.length as usize;
        if start.saturating_add(len) <= self.data.len() {
            // madvise(MADV_WILLNEED) — non-blocking hint to the kernel.
            if let Err(e) = self.data.advise_range(Advice::WillNeed, start, len) {
                tracing::trace!(
                    offset = start,
                    length = len,
                    error = %e,
                    "prefetch advise failed (non-fatal)"
                );
                metrics::counter!("chronix_segment_prefetch_errors_total").increment(1);
            } else {
                metrics::counter!("chronix_segment_prefetch_hints_total").increment(1);
                metrics::counter!("chronix_segment_prefetch_bytes_total").increment(len as u64);
            }
        }
    }

    /// Read all data as a single Arrow `RecordBatch`.
    ///
    /// This is a convenience wrapper around [`read_projected`](Self::read_projected)
    /// that passes every column name.  When only a subset of columns is
    /// needed, prefer calling [`read_projected`](Self::read_projected) or
    /// [`read_projected_filtered`](Self::read_projected_filtered) directly
    /// — they skip I/O and decompression for unrequested columns (
    /// column pruning).
    ///
    /// # Errors
    ///
    /// Returns an error if any column data is corrupt.
    pub fn read_all(&self) -> Result<RecordBatch> {
        let column_names: Vec<&str> = self
            .metadata
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        self.read_projected(&column_names)
    }

    /// Read all columns from a single row group as a `RecordBatch`.
    ///
    /// This is useful for reverse-order scanning (e.g. `last_value`)
    /// where only one row group at a time needs to be materialised.
    ///
    /// # Errors
    ///
    /// Returns an error if `row_group` is out of range or column data is corrupt.
    pub fn read_row_group(&self, row_group: usize) -> Result<RecordBatch> {
        let column_names: Vec<&str> = self
            .metadata
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        let arrays = self.read_columns(&column_names, row_group)?;
        let fields: Vec<Field> = column_names
            .iter()
            .map(|&name| {
                let col_meta = self
                    .metadata
                    .columns
                    .iter()
                    .find(|c| c.name == name)
                    .ok_or_else(|| SegmentError::CorruptFile {
                        detail: format!("column '{name}' not in segment metadata"),
                    })?;
                let dt = match col_meta.data_type {
                    data_types::TIMESTAMP | data_types::I64 => DataType::Int64,
                    data_types::U64 => DataType::UInt64,
                    data_types::F64 => DataType::Float64,
                    data_types::BOOL => DataType::Boolean,
                    _ => DataType::Utf8,
                };
                Ok(Field::new(name, dt, true).with_metadata(roles::arrow_metadata(col_meta.role)))
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = std::sync::Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| SegmentError::CorruptFile {
            detail: format!("failed to build row-group RecordBatch: {e}"),
        })
    }

    /// Read selected columns across all row groups as a single `RecordBatch`.
    ///
    /// Only the byte ranges for the requested columns are decoded and
    /// decompressed — all other column data on disk is skipped entirely.
    /// The `timestamp` column is always included even if not listed.
    ///
    /// # Errors
    ///
    /// Returns an error if any requested column name is not found in the
    /// segment metadata, or if column data is corrupt.
    pub fn read_projected(&self, column_names: &[&str]) -> Result<RecordBatch> {
        self.read_projected_filtered(column_names, None)
    }

    /// Read selected columns with optional row-group-level time-range
    /// predicate pushdown.
    ///
    /// When `time_range` is `Some((start_ns, end_ns))`, row groups whose
    /// timestamp range does not overlap `[start_ns, end_ns]` are skipped
    /// entirely — no I/O or decompression is performed for them.
    ///
    /// Both boundaries are **inclusive**, matching the semantics of
    /// `chronix_query::plan::TimeRange`.
    ///
    /// This is the primary entry point for query-time segment reads where
    /// the caller already knows the desired time window.
    pub fn read_projected_filtered(
        &self,
        column_names: &[&str],
        time_range: Option<(i64, i64)>,
    ) -> Result<RecordBatch> {
        self.read_projected_filtered_with_predicates(column_names, time_range, &[])
    }

    /// Read selected columns with row-group-level time-range and tag
    /// predicate pushdown.
    ///
    /// In addition to time-range pruning, this method also pushes tag
    /// equality predicates down to row-group level. A row group is
    /// skipped when a tag column's per-block statistics indicate that
    /// it has zero non-null values (`value_count == 0`), which means it
    /// cannot possibly match an equality filter on that tag.
    ///
    /// # Arguments
    ///
    /// - `column_names` — Columns to read (empty = all).
    /// - `time_range` — Optional inclusive time range `(start, end)`.
    /// - `tag_predicates` — Tag equality filters as `(column_name, value)` pairs.
    pub fn read_projected_filtered_with_predicates(
        &self,
        column_names: &[&str],
        time_range: Option<(i64, i64)>,
        tag_predicates: &[(&str, &str)],
    ) -> Result<RecordBatch> {
        self.read_projected_with_zone_maps(column_names, time_range, tag_predicates, &[])
    }

    /// Read selected columns with full predicate pushdown: time-range,
    /// tag equality, and zone-map field predicates (late materialisation).
    ///
    /// Zone-map field predicates evaluate per-row-group column min/max
    /// statistics to skip row groups that cannot possibly match numeric
    /// comparisons.  This avoids decompressing and decoding column data
    /// for entire row groups.
    ///
    /// # Arguments
    ///
    /// - `column_names` — Columns to read (empty = all).
    /// - `time_range` — Optional inclusive time range `(start, end)`.
    /// - `tag_predicates` — Tag equality filters as `(column_name, value)` pairs.
    /// - `field_predicates` — Numeric field predicates for zone-map pruning.
    pub fn read_projected_with_zone_maps(
        &self,
        column_names: &[&str],
        time_range: Option<(i64, i64)>,
        tag_predicates: &[(&str, &str)],
        field_predicates: &[FieldPredicate],
    ) -> Result<RecordBatch> {
        // If column_names is empty, read ALL columns (equivalent to read_all).
        let all_col_names: Vec<String> = if column_names.is_empty() {
            self.metadata
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect()
        } else {
            Vec::new()
        };

        // Ensure timestamp is always included
        let mut names: Vec<&str> = Vec::with_capacity(if column_names.is_empty() {
            all_col_names.len()
        } else {
            column_names.len() + 1
        });
        if column_names.is_empty() {
            // Read all columns
            for c in &all_col_names {
                names.push(c.as_str());
            }
        } else {
            if !column_names.contains(&"timestamp") {
                names.push("timestamp");
            }
            for &n in column_names {
                if !names.contains(&n) {
                    names.push(n);
                }
            }
        }

        // Find the timestamp column index for row-group pruning.
        let ts_col_idx = self
            .metadata
            .columns
            .iter()
            .position(|c| c.name == "timestamp");

        // Resolve tag predicate column indices for row-group pushdown.
        let tag_col_indices: Vec<usize> = tag_predicates
            .iter()
            .filter_map(|(col_name, _)| {
                self.metadata
                    .columns
                    .iter()
                    .position(|c| c.name == *col_name)
            })
            .collect();

        let mut all_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); names.len()];

        // Segment-level bloom filter check.
        // If any tag predicate value is definitely absent from the
        // segment's bloom filter, the entire segment cannot match —
        // return an empty RecordBatch immediately.
        if !tag_predicates.is_empty() {
            for &(col_name, value) in tag_predicates {
                if let Some(col_meta) = self.metadata.columns.iter().find(|c| c.name == col_name) {
                    if let Some(ref bf) = col_meta.bloom_filter {
                        if !crate::segment::bloom::bloom_filter_contains(bf, value) {
                            metrics::counter!("chronix_segment_pruned_by_bloom_filter_total")
                                .increment(1);
                            // Build empty RecordBatch with correct schema
                            let mut empty_arrays: Vec<ArrayRef> = Vec::with_capacity(names.len());
                            for &name in &names {
                                let cm = self
                                    .metadata
                                    .columns
                                    .iter()
                                    .find(|c| c.name == name)
                                    .ok_or_else(|| SegmentError::CorruptFile {
                                        detail: format!("column '{name}' not in metadata"),
                                    })?;
                                let ea: ArrayRef = match cm.data_type {
                                    data_types::TIMESTAMP | data_types::I64 => {
                                        std::sync::Arc::new(Int64Array::from(Vec::<i64>::new()))
                                    }
                                    data_types::U64 => {
                                        std::sync::Arc::new(UInt64Array::from(Vec::<u64>::new()))
                                    }
                                    data_types::F64 => {
                                        std::sync::Arc::new(Float64Array::from(Vec::<f64>::new()))
                                    }
                                    data_types::BOOL => {
                                        std::sync::Arc::new(BooleanArray::from(Vec::<bool>::new()))
                                    }
                                    _ => std::sync::Arc::new(StringArray::from(Vec::<&str>::new())),
                                };
                                empty_arrays.push(ea);
                            }
                            let fields: Vec<Field> = names
                                .iter()
                                .map(|&n| {
                                    let cm2 = self
                                        .metadata
                                        .columns
                                        .iter()
                                        .find(|c| c.name == n)
                                        .ok_or_else(|| SegmentError::CorruptFile {
                                            detail: format!("column '{n}' not in metadata"),
                                        })?;
                                    let dt = match cm2.data_type {
                                        data_types::TIMESTAMP | data_types::I64 => DataType::Int64,
                                        data_types::U64 => DataType::UInt64,
                                        data_types::F64 => DataType::Float64,
                                        data_types::BOOL => DataType::Boolean,
                                        _ => DataType::Utf8,
                                    };
                                    Ok(Field::new(n, dt, true)
                                        .with_metadata(roles::arrow_metadata(cm2.role)))
                                })
                                .collect::<Result<Vec<_>>>()?;
                            let schema = Schema::new(fields);
                            return RecordBatch::try_new(std::sync::Arc::new(schema), empty_arrays)
                                .map_err(|e| SegmentError::CorruptFile {
                                    detail: format!("failed to create empty RecordBatch: {e}"),
                                });
                        }
                    }
                }
            }
        }

        for rg_idx in 0..self.metadata.row_group_blocks.len() {
            let rg_blocks = &self.metadata.row_group_blocks[rg_idx];

            // Row-group-level time-range predicate pushdown:
            // Compare the query window [start_ns, end_ns] against the row
            // group's timestamp column stats [rg_min, rg_max].  If the
            // intervals don't overlap, no rows in this row group can match
            // so we skip it entirely — no I/O, no decompression, no
            // decoding.
            if let (Some((start_ns, end_ns)), Some(ts_idx)) = (time_range, ts_col_idx) {
                if let Some(ts_block) = rg_blocks.iter().find(|b| b.column_index == ts_idx as u16) {
                    let rg_min = ts_block.stats.min_value;
                    let rg_max = ts_block.stats.max_value;
                    // Skip if row group is entirely before or after query range.
                    // Both boundaries are inclusive: [start_ns, end_ns]
                    if rg_max < start_ns || rg_min > end_ns {
                        metrics::counter!("chronix_segment_rg_pruned_by_time_range_total")
                            .increment(1);
                        continue;
                    }
                }
            }

            // Row-group-level tag predicate pushdown.
            //
            // For each requested tag column, inspect the per-block stats.
            // If any tag column has `value_count == 0` (i.e. all values in
            // this row group are NULL for that tag), the row group cannot
            // contain a matching row — skip it without reading any data.
            //
            // This is conservative: a row group with non-zero value_count
            // may still not match the equality value, but skipping it
            // would require decoding the block (which is what we're trying
            // to avoid).  Full value-level filtering is done post-decode
            // by `filter_batch()`.
            //
            // An encrypted column's statistics are suppressed deliberately,
            // which leaves `value_count` at zero — indistinguishable from
            // an all-null block unless `null_count` is consulted too. Only
            // a genuinely all-null block is impossible to match.
            if !tag_predicates.is_empty() {
                let mut skip = false;
                for &tag_idx in &tag_col_indices {
                    if let Some(tag_block) =
                        rg_blocks.iter().find(|b| b.column_index == tag_idx as u16)
                    {
                        if tag_block.stats.value_count == 0 && !tag_block.stats.says_nothing() {
                            skip = true;
                            break;
                        }
                    }
                }
                if skip {
                    metrics::counter!("chronix_segment_rg_pruned_by_tag_stats_total").increment(1);
                    continue;
                }
            }

            // Per-row-group bloom filter pruning for tag columns.
            //
            // If a tag column has a per-RG bloom filter and the queried value
            // is definitely not present in this row group, skip it entirely.
            if !tag_predicates.is_empty() {
                let mut skip = false;
                for &(col_name, value) in tag_predicates {
                    if let Some(col_meta) =
                        self.metadata.columns.iter().find(|c| c.name == col_name)
                    {
                        if let Some(ref rg_blooms) = col_meta.row_group_blooms {
                            if let Some(bloom_bytes) = rg_blooms.get(rg_idx) {
                                if !bloom_bytes.is_empty()
                                    && !crate::segment::bloom::bloom_filter_contains(
                                        bloom_bytes,
                                        value,
                                    )
                                {
                                    skip = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                if skip {
                    metrics::counter!("chronix_segment_rg_pruned_by_bloom_total").increment(1);
                    continue;
                }
            }

            // Late materialisation — zone-map field predicate pushdown.
            //
            // For each numeric field predicate, retrieve the column's
            // per-row-group min/max from ColumnBlockMeta.stats and check
            // whether any row in this group could possibly match.  If the
            // zone-map test rules out all rows, skip the entire row group.
            if !field_predicates.is_empty() {
                let mut skip = false;
                for pred in field_predicates {
                    if let Some((col_idx, col_meta)) = self
                        .metadata
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, c)| c.name == pred.column)
                    {
                        if let Some(block) =
                            rg_blocks.iter().find(|b| b.column_index == col_idx as u16)
                        {
                            // Statistics that say nothing — an encrypted
                            // block, whose stats are suppressed so the zone
                            // map cannot leak its values — must not prune.
                            let matches = if block.stats.says_nothing() {
                                true
                            } else {
                                match col_meta.data_type {
                                    data_types::F64 => {
                                        let rg_min = ordered_i64_to_f64(block.stats.min_value);
                                        let rg_max = ordered_i64_to_f64(block.stats.max_value);
                                        pred.may_match_f64(rg_min, rg_max)
                                    }
                                    data_types::TIMESTAMP | data_types::I64 => pred.may_match_i64(
                                        block.stats.min_value,
                                        block.stats.max_value,
                                    ),
                                    _ => true, // non-numeric → can't prune
                                }
                            };
                            if !matches {
                                skip = true;
                                break;
                            }
                        }
                    }
                }
                if skip {
                    metrics::counter!("chronix_segment_rg_pruned_by_zone_map_total").increment(1);
                    continue;
                }
            }

            let arrays = self.read_columns(&names, rg_idx)?;
            for (col_idx, array) in arrays.into_iter().enumerate() {
                all_arrays[col_idx].push(array);
            }
        }

        // Concatenate row groups per column.
        // If all row groups were pruned, return an empty RecordBatch
        // with the correct schema.
        let all_pruned = all_arrays.iter().all(std::vec::Vec::is_empty);

        let mut final_arrays: Vec<ArrayRef> = Vec::with_capacity(names.len());

        if all_pruned {
            // Build empty arrays with correct types
            for &name in &names {
                let col_meta = self
                    .metadata
                    .columns
                    .iter()
                    .find(|c| c.name == name)
                    .ok_or_else(|| SegmentError::CorruptFile {
                        detail: format!("projected column '{name}' not in segment metadata"),
                    })?;
                let empty_array: ArrayRef = match col_meta.data_type {
                    data_types::TIMESTAMP | data_types::I64 => {
                        std::sync::Arc::new(Int64Array::from(Vec::<i64>::new()))
                    }
                    data_types::U64 => std::sync::Arc::new(UInt64Array::from(Vec::<u64>::new())),
                    data_types::F64 => std::sync::Arc::new(Float64Array::from(Vec::<f64>::new())),
                    data_types::BOOL => std::sync::Arc::new(BooleanArray::from(Vec::<bool>::new())),
                    _ => std::sync::Arc::new(StringArray::from(Vec::<&str>::new())),
                };
                final_arrays.push(empty_array);
            }
        } else {
            for (col_idx, arrays) in all_arrays.iter().enumerate() {
                let refs: Vec<&dyn arrow::array::Array> =
                    arrays.iter().map(std::convert::AsRef::as_ref).collect();
                let concatenated =
                    arrow::compute::concat(&refs).map_err(|e| SegmentError::CorruptFile {
                        detail: format!("failed to concatenate column {}: {e}", names[col_idx]),
                    })?;
                final_arrays.push(concatenated);
            }
        }

        // Build schema for only the projected columns
        let fields: Vec<Field> = names
            .iter()
            .map(|&name| {
                let col_meta = self
                    .metadata
                    .columns
                    .iter()
                    .find(|c| c.name == name)
                    .ok_or_else(|| SegmentError::CorruptFile {
                        detail: format!("projected column '{name}' not in segment metadata"),
                    })?;
                let dt = match col_meta.data_type {
                    data_types::TIMESTAMP | data_types::I64 => DataType::Int64,
                    data_types::U64 => DataType::UInt64,
                    data_types::F64 => DataType::Float64,
                    data_types::BOOL => DataType::Boolean,
                    _ => DataType::Utf8,
                };
                Ok(Field::new(name, dt, true).with_metadata(roles::arrow_metadata(col_meta.role)))
            })
            .collect::<Result<Vec<Field>>>()?;

        let schema = Schema::new(fields);
        let batch =
            RecordBatch::try_new(std::sync::Arc::new(schema), final_arrays).map_err(|e| {
                SegmentError::CorruptFile {
                    detail: format!("failed to create RecordBatch: {e}"),
                }
            })?;

        // All column data has been decoded into owned Arrow arrays.
        // Hint the kernel to release the mmap pages back to the page cache.
        // This reduces RSS during parallel scans where completed segments
        // would otherwise hold pages until SegmentReader is dropped.
        // SAFETY: We hold a read-only file-backed Mmap and all column data has
        // already been decoded into owned Arrow arrays above.  No references into
        // the mapped region remain, so releasing the pages is safe.
        #[allow(unsafe_code)]
        let _ = unsafe { self.data.unchecked_advise(UncheckedAdvice::DontNeed) };

        Ok(batch)
    }

    /// Read and decode a single column block.
    fn read_column_block(&self, block_meta: &ColumnBlockMeta, col_idx: usize) -> Result<ArrayRef> {
        let start = block_meta.offset as usize;
        let end = start
            .checked_add(block_meta.length as usize)
            .ok_or_else(|| SegmentError::CorruptFile {
                detail: format!(
                    "column block offset+length overflow: offset={start}, length={}",
                    block_meta.length,
                ),
            })?;

        if end > self.data.len() {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "column block extends beyond file: offset={start}, \
                     length={}, file_size={}",
                    block_meta.length,
                    self.data.len()
                ),
            });
        }

        let raw_data = &self.data[start..end];

        // Verify per-block CRC32c if present (block_crc == 0 means legacy/no checksum)
        if block_meta.block_crc != 0 {
            let actual_crc = crc32c::crc32c(raw_data);
            if actual_crc != block_meta.block_crc {
                return Err(SegmentError::CorruptFile {
                    detail: format!(
                        "block CRC mismatch for column {}: expected {:#010x}, got {:#010x}",
                        col_idx, block_meta.block_crc, actual_crc,
                    ),
                });
            }
        } else {
            tracing::warn!(
                column = col_idx,
                "block CRC is zero, skipping integrity check — this segment was written without per-block CRCs"
            );
            metrics::counter!("chronix_segment_crc_skip_total").increment(1);
        }

        // Field-level decryption: decrypt after CRC check (CRC covers the
        // encrypted on-disk bytes) but before decompression.
        #[cfg(feature = "field-encryption")]
        let raw_data: std::borrow::Cow<'_, [u8]> = if block_meta.encrypted {
            let col_meta = &self.metadata.columns[col_idx];
            let key_id = col_meta
                .key_id
                .as_deref()
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: format!(
                        "column {} is encrypted but has no key_id in metadata",
                        col_meta.name,
                    ),
                })?;
            let provider = self
                .key_provider
                .as_ref()
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: format!(
                        "column {} is encrypted but no key provider is configured",
                        col_meta.name,
                    ),
                })?;
            let key = provider
                .get_key(key_id)
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: format!(
                        "key '{}' not found in key provider for encrypted column {}",
                        key_id, col_meta.name,
                    ),
                })?;
            let decrypted = crate::segment::field_encryption::decrypt_block(
                raw_data,
                &key,
                crate::segment::field_encryption::BlockContext {
                    column: &col_meta.name,
                    segment_created_at: self.header.created_at,
                },
            )
            .map_err(|e| SegmentError::CorruptFile {
                detail: format!("failed to decrypt column {}: {e}", col_meta.name,),
            })?;
            std::borrow::Cow::Owned(decrypted)
        } else {
            std::borrow::Cow::Borrowed(raw_data)
        };

        #[cfg(not(feature = "field-encryption"))]
        let raw_data: &[u8] = raw_data;

        // Decompress if needed — the dictionary is passed for ZSTD_DICT blocks.
        let decompressed = if block_meta.compressed {
            decompress_block_with_optional_dict(
                &raw_data,
                self.metadata.zstd_dictionary.as_deref(),
            )?
        } else {
            raw_data.to_vec()
        };

        // Parse the encoded block (first byte is encoding tag)
        let encoded_block = EncodedBlock::from_bytes(&decompressed)?;

        // Decode to typed values
        let decoded = ColumnDecoder::decode(&encoded_block)?;

        // Verify decoded value count matches metadata expectation.
        let decoded_len = decoded.len();
        let expected = block_meta.value_count as usize;
        if decoded_len != expected {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "decoded value count mismatch for column {col_idx}: \
                     metadata says {expected} values but decoded {decoded_len}",
                ),
            });
        }

        // D-NULL: attach the block's validity bitmap so absent values read
        // back as SQL NULL rather than as a 0/""/false sentinel.
        let nulls = self.read_validity(block_meta)?;

        // Convert to Arrow array
        let col_meta = &self.metadata.columns[col_idx];
        decoded_to_arrow(decoded, col_meta.data_type, nulls)
    }

    /// Load a block's validity bitmap, if it has one (`.csx` v2).
    ///
    /// Returns `None` for blocks with no nulls, which store no bitmap.
    fn read_validity(&self, block_meta: &ColumnBlockMeta) -> Result<Option<NullBuffer>> {
        if block_meta.validity_length == 0 {
            return Ok(None);
        }
        let start = block_meta.validity_offset as usize;
        let end = start
            .checked_add(block_meta.validity_length as usize)
            .ok_or_else(|| SegmentError::CorruptFile {
                detail: "validity bitmap offset+length overflow".to_string(),
            })?;
        if end > self.data.len() {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "validity bitmap extends beyond file: offset={start}, \
                     length={}, file_size={}",
                    block_meta.validity_length,
                    self.data.len()
                ),
            });
        }
        let value_count = block_meta.value_count as usize;
        let bytes = &self.data[start..end];
        if bytes.len() < crate::segment::validity::bitmap_len(value_count) {
            return Err(SegmentError::CorruptFile {
                detail: format!(
                    "validity bitmap too short: {} bytes for {value_count} values",
                    bytes.len()
                ),
            });
        }
        Ok(crate::segment::validity::null_buffer_from_bytes(
            bytes,
            value_count,
        ))
    }
}

/// Re-attach a validity bitmap to a freshly decoded Arrow array.
///
/// The decoder always produces a dense array (nulls were stored as
/// sentinels in the encoded payload); this stamps the v2 validity bitmap
/// back on so the sentinels become real SQL NULLs.
fn apply_nulls<A>(array: A, nulls: Option<NullBuffer>) -> Result<A>
where
    A: arrow::array::Array + From<arrow::array::ArrayData>,
{
    let Some(nulls) = nulls else {
        return Ok(array);
    };
    let data = array.into_data();
    if nulls.len() != data.len() {
        return Err(SegmentError::CorruptFile {
            detail: format!(
                "validity bitmap length {} does not match column length {}",
                nulls.len(),
                data.len()
            ),
        });
    }
    // SAFETY-equivalent: `into_builder`/`build` revalidates offsets and
    // buffers, and the null buffer length is checked above.
    let data = data
        .into_builder()
        .nulls(Some(nulls))
        .build()
        .map_err(|e| SegmentError::CorruptFile {
            detail: format!("failed to attach validity bitmap: {e}"),
        })?;
    Ok(A::from(data))
}

/// Convert decoded column values to an Arrow `ArrayRef`.
///
/// Consumes the decoded column to avoid redundant cloning.
fn decoded_to_arrow(
    decoded: DecodedColumn,
    data_type: u8,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    match (decoded, data_type) {
        (DecodedColumn::I64(values), data_types::TIMESTAMP | data_types::I64) => Ok(
            std::sync::Arc::new(apply_nulls(Int64Array::from(values), nulls)?),
        ),
        (DecodedColumn::U64(values), data_types::U64) => Ok(std::sync::Arc::new(apply_nulls(
            UInt64Array::from(values),
            nulls,
        )?)),
        (DecodedColumn::F64(values), data_types::F64) => Ok(std::sync::Arc::new(apply_nulls(
            Float64Array::from(values),
            nulls,
        )?)),
        (DecodedColumn::Bool(values), data_types::BOOL) => Ok(std::sync::Arc::new(apply_nulls(
            BooleanArray::from(values),
            nulls,
        )?)),
        (DecodedColumn::String(values), data_types::STRING) => {
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            Ok(std::sync::Arc::new(apply_nulls(
                StringArray::from(refs),
                nulls,
            )?))
        }
        (decoded, _) => Err(SegmentError::CorruptFile {
            detail: format!(
                "type mismatch: decoded variant {:?} does not match expected data_type {data_type}",
                std::mem::discriminant(&decoded),
            ),
        }),
    }
}

#[cfg(test)]
mod zone_map_tests {
    use super::*;

    fn pred(op: ZoneMapOp, value: f64) -> FieldPredicate {
        FieldPredicate {
            column: "n".into(),
            op,
            value,
        }
    }

    /// A fractional bound must not be truncated to an integer. `n < 1.5`
    /// used to become `n < 1`, which prunes a block holding `{1, 2, 3}`
    /// even though 1 matches — a pruning step that loses rows.
    #[test]
    fn a_fractional_bound_does_not_prune_an_integer_block() {
        assert!(pred(ZoneMapOp::Lt, 1.5).may_match_i64(1, 3));
        assert!(pred(ZoneMapOp::LtEq, 1.5).may_match_i64(1, 3));
        assert!(pred(ZoneMapOp::Gt, 2.5).may_match_i64(1, 3));
        assert!(pred(ZoneMapOp::GtEq, 2.5).may_match_i64(1, 3));
        assert!(pred(ZoneMapOp::Eq, 2.0).may_match_i64(1, 3));
        // And it still prunes what it should.
        assert!(!pred(ZoneMapOp::Lt, 1.0).may_match_i64(1, 3));
        assert!(!pred(ZoneMapOp::Gt, 3.0).may_match_i64(1, 3));
        // An integer column cannot hold 2.5 at all.
        assert!(!pred(ZoneMapOp::Eq, 2.5).may_match_i64(1, 3));
    }

    /// Beyond 2^53 an `i64` bound cannot be compared as an `f64`, so the
    /// row group is kept rather than compared wrongly.
    #[test]
    fn a_bound_beyond_exact_float_range_never_prunes() {
        let big = (1_i64 << 53) + 1;
        assert!(pred(ZoneMapOp::Eq, big as f64).may_match_i64(big, big));
        assert!(pred(ZoneMapOp::Lt, 0.0).may_match_i64(big, big + 10));
    }

    /// Statistics that say nothing — the sentinel range an encrypted block
    /// carries — must read as "unknown", not as an empty range. As an empty
    /// range every comparison is false, so a predicate on an encrypted
    /// column pruned every row group and the query returned nothing.
    #[test]
    fn suppressed_statistics_never_prune() {
        let empty = crate::segment::stats::ColumnStats::empty();
        assert!(empty.says_nothing());
        assert!(
            pred(ZoneMapOp::Gt, 0.0).may_match_i64(empty.min_value, empty.max_value),
            "an inverted sentinel range must not prune"
        );
        assert!(pred(ZoneMapOp::Eq, 42.0).may_match_f64(f64::NAN, f64::NAN));

        // A genuinely all-null block *is* impossible to match, and pruning
        // it stays correct.
        let mut all_null = crate::segment::stats::ColumnStats::empty();
        all_null.record_null();
        assert!(!all_null.says_nothing());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::writer::{SegmentWriter, SegmentWriterConfig};
    use chronix_core::config::{CompressionCodec, FloatEncoding};
    use chronix_core::types::{FieldValue, Point, SeriesKey};
    use std::collections::BTreeMap;

    fn make_point(host: &str, cpu: f64, ts: i64) -> Point {
        let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
        let key = SeriesKey::new("cpu_usage", tags).unwrap();
        let fields = BTreeMap::from([("cpu".to_string(), FieldValue::F64(cpu))]);
        Point::new(key, fields, ts).unwrap()
    }

    #[test]
    fn write_and_read_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csx");

        // Write
        let points: Vec<Point> = (0..100)
            .map(|i| make_point("server-1", 50.0 + i as f64, 1_000_000 + i * 10_000))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 50,
            compress: false,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        let meta = writer.finalize().unwrap();

        assert_eq!(meta.row_count, 100);
        assert_eq!(meta.row_group_count, 2); // 100 rows / 50 per group

        // Read
        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.header().row_count, 100);
        assert_eq!(reader.row_group_count(), 2);

        // Read specific columns from first row group
        let columns = reader.read_columns(&["timestamp", "cpu"], 0).unwrap();
        assert_eq!(columns.len(), 2);

        // Read all data
        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 3); // timestamp, host, cpu
    }

    #[test]
    fn write_and_read_compressed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compressed.csx");

        let points: Vec<Point> = (0..200)
            .map(|i| make_point("server-1", 50.0 + (i % 10) as f64, 1_000_000 + i * 10_000))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 100,
            compress: true,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), 200);
    }

    #[test]
    fn checksum_validation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checksum.csx");

        let points = vec![make_point("s1", 1.0, 1000)];
        let mut writer = SegmentWriter::new(&path, SegmentWriterConfig::default()).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        // Corrupt one byte in the middle
        let mut data = std::fs::read(&path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        assert!(matches!(
            SegmentReader::open(&path),
            Err(SegmentError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn column_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("projection.csx");

        let points: Vec<Point> = (0..50)
            .map(|i| make_point("h1", i as f64, i * 1000))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 100,
            compress: false,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();

        // Read only the cpu column
        let cols = reader.read_columns(&["cpu"], 0).unwrap();
        assert_eq!(cols.len(), 1);

        let cpu_arr = cols[0].as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(cpu_arr.len(), 50);
    }

    #[test]
    fn row_group_out_of_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rg_oor.csx");

        let points = vec![make_point("h1", 1.0, 1000)];
        let mut writer = SegmentWriter::new(&path, SegmentWriterConfig::default()).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert!(reader.read_columns(&["timestamp"], 999).is_err());
    }

    #[test]
    fn column_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("col_404.csx");

        let points = vec![make_point("h1", 1.0, 1000)];
        let mut writer = SegmentWriter::new(&path, SegmentWriterConfig::default()).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert!(reader.read_columns(&["nonexistent"], 0).is_err());
    }

    #[test]
    fn read_projected_selects_subset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("projected.csx");

        // Write a segment with 3 columns: timestamp, host (tag), cpu (field)
        let points: Vec<Point> = (0..80)
            .map(|i| make_point("server-1", 50.0 + i as f64, 1_000_000 + i * 10_000))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 50,
            compress: false,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();

        // Project only "cpu" — timestamp is auto-included
        let batch = reader.read_projected(&["cpu"]).unwrap();
        assert_eq!(batch.num_rows(), 80);
        assert_eq!(batch.num_columns(), 2); // timestamp + cpu
        assert!(batch.column_by_name("timestamp").is_some());
        assert!(batch.column_by_name("cpu").is_some());
        assert!(batch.column_by_name("host").is_none());

        // Verify values are correct
        let cpu_col = batch
            .column_by_name("cpu")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((cpu_col.value(0) - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn read_projected_includes_timestamp_implicitly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proj_ts.csx");

        let points = vec![make_point("h1", 42.0, 9999)];
        let mut writer = SegmentWriter::new(&path, SegmentWriterConfig::default()).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();

        // Only request "host" — should still get timestamp
        let batch = reader.read_projected(&["host"]).unwrap();
        assert_eq!(batch.num_columns(), 2); // timestamp + host
        assert!(batch.column_by_name("timestamp").is_some());
    }

    #[test]
    fn read_projected_all_columns_matches_read_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proj_all.csx");

        let points: Vec<Point> = (0..30)
            .map(|i| make_point("h1", i as f64, i * 1000))
            .collect();

        let mut writer = SegmentWriter::new(&path, SegmentWriterConfig::default()).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();

        let all = reader.read_all().unwrap();
        let projected = reader
            .read_projected(&["timestamp", "host", "cpu"])
            .unwrap();

        assert_eq!(all.num_rows(), projected.num_rows());
        assert_eq!(all.num_columns(), projected.num_columns());
    }

    #[test]
    fn read_projected_unknown_column_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proj_err.csx");

        let points = vec![make_point("h1", 1.0, 1000)];
        let mut writer = SegmentWriter::new(&path, SegmentWriterConfig::default()).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert!(reader.read_projected(&["nonexistent"]).is_err());
    }

    #[test]
    fn read_projected_across_multiple_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proj_multi_rg.csx");

        let points: Vec<Point> = (0..200)
            .map(|i| make_point("server-1", i as f64, i * 1000))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 64,
            compress: true,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert!(reader.row_group_count() > 1);

        let batch = reader.read_projected(&["cpu"]).unwrap();
        assert_eq!(batch.num_rows(), 200);
        assert_eq!(batch.num_columns(), 2); // timestamp + cpu
    }

    #[test]
    fn multiple_series() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi_series.csx");

        let mut points = Vec::new();
        for i in 0..20 {
            points.push(make_point("server-1", i as f64, i * 1000));
            points.push(make_point("server-2", i as f64 + 100.0, i * 1000));
        }

        let config = SegmentWriterConfig {
            row_group_size: 100,
            compress: false,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        let meta = writer.finalize().unwrap();

        assert_eq!(meta.series_count, 2);
        assert_eq!(meta.row_count, 40);

        let reader = SegmentReader::open(&path).unwrap();
        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), 40);
    }

    // ── P1: time-range predicate pushdown tests ──────────────────────

    /// Helper: create a segment with `n` points for a single series,
    /// timestamps `0, step, 2*step, …` and a small `row_group_size`
    /// so we get multiple row groups to prune.
    fn write_segment_with_rg_size(path: &std::path::Path, n: usize, step: i64, rg_size: usize) {
        let points: Vec<Point> = (0..n)
            .map(|i| make_point("server-1", i as f64, i as i64 * step))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: rg_size,
            compress: false,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();
    }

    #[test]
    fn time_range_prunes_row_groups() {
        // 30 points, ts = 0, 10_000, …, 290_000.
        // row_group_size = 10 → 3 row groups:
        //   rg0: ts [0, 90_000]
        //   rg1: ts [100_000, 190_000]
        //   rg2: ts [200_000, 290_000]
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prune.csx");
        write_segment_with_rg_size(&path, 30, 10_000, 10);

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_group_count(), 3);

        // Query only the middle row group's time range.
        let batch = reader
            .read_projected_filtered(&["cpu"], Some((100_000, 190_000)))
            .unwrap();
        assert_eq!(batch.num_rows(), 10); // only rg1

        // Verify timestamps are in the expected range.
        let ts = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..ts.len() {
            assert!(ts.value(i) >= 100_000);
            assert!(ts.value(i) <= 190_000);
        }
    }

    #[test]
    fn time_range_inclusive_end_boundary() {
        // Same layout: 3 row groups.
        //   rg0: ts [0, 90_000]
        //   rg1: ts [100_000, 190_000]
        //   rg2: ts [200_000, 290_000]
        //
        // Query [0, 100_000] — end equals rg1.min.
        // With inclusive end, rg1 MUST be included (it contains ts=100_000).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("boundary.csx");
        write_segment_with_rg_size(&path, 30, 10_000, 10);

        let reader = SegmentReader::open(&path).unwrap();

        let batch = reader
            .read_projected_filtered(&["cpu"], Some((0, 100_000)))
            .unwrap();
        // rg0 (10 rows) + rg1 (10 rows) = 20 rows
        // (rg1 is included because rg1.min == 100_000 <= end 100_000)
        assert_eq!(batch.num_rows(), 20);
    }

    #[test]
    fn time_range_all_pruned_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.csx");
        // ts range [0, 290_000]
        write_segment_with_rg_size(&path, 30, 10_000, 10);

        let reader = SegmentReader::open(&path).unwrap();

        // Query range completely after segment data.
        let batch = reader
            .read_projected_filtered(&["cpu"], Some((500_000, 600_000)))
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
        // Schema should still have timestamp + cpu.
        assert_eq!(batch.num_columns(), 2);
        assert!(batch.column_by_name("timestamp").is_some());
        assert!(batch.column_by_name("cpu").is_some());
    }

    #[test]
    fn time_range_none_reads_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_filter.csx");
        write_segment_with_rg_size(&path, 30, 10_000, 10);

        let reader = SegmentReader::open(&path).unwrap();

        let batch = reader.read_projected_filtered(&["cpu"], None).unwrap();
        assert_eq!(batch.num_rows(), 30);
    }

    #[test]
    fn read_row_group_returns_single_group() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single_rg.csx");
        write_segment_with_rg_size(&path, 30, 10_000, 10);

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_group_count(), 3);

        let rg1 = reader.read_row_group(1).unwrap();
        assert_eq!(rg1.num_rows(), 10);

        // Verify timestamps are from the second row group.
        let ts = rg1
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 100_000);
        assert_eq!(ts.value(9), 190_000);
    }

    #[test]
    fn read_row_group_out_of_range_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rg_oor2.csx");
        write_segment_with_rg_size(&path, 10, 1000, 10);

        let reader = SegmentReader::open(&path).unwrap();
        assert!(reader.read_row_group(5).is_err());
    }

    // ── Bloom filter integration tests ─────────────────────────

    /// Helper: create a segment with multiple hosts (tag values) for
    /// bloom filter testing.
    fn write_segment_with_tags(path: &std::path::Path, hosts: &[&str], points_per_host: usize) {
        let mut points = Vec::new();
        for (h_idx, &host) in hosts.iter().enumerate() {
            for i in 0..points_per_host {
                points.push(make_point(
                    host,
                    (h_idx * 100 + i) as f64,
                    (h_idx * points_per_host + i) as i64 * 1000,
                ));
            }
        }

        let config = SegmentWriterConfig {
            row_group_size: 64,
            compress: false,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();
    }

    #[test]
    fn bloom_filter_present_in_tag_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom_tag.csx");
        write_segment_with_tags(&path, &["server-1", "server-2", "server-3"], 10);

        let reader = SegmentReader::open(&path).unwrap();

        // The "host" column (role=TAG) should have a bloom filter
        let host_meta = reader
            .column_metadata()
            .iter()
            .find(|c| c.name == "host")
            .unwrap();
        assert!(
            host_meta.bloom_filter.is_some(),
            "tag column should have a bloom filter"
        );

        // Non-tag columns should NOT have bloom filters
        let ts_meta = reader
            .column_metadata()
            .iter()
            .find(|c| c.name == "timestamp")
            .unwrap();
        assert!(ts_meta.bloom_filter.is_none());

        let cpu_meta = reader
            .column_metadata()
            .iter()
            .find(|c| c.name == "cpu")
            .unwrap();
        assert!(cpu_meta.bloom_filter.is_none());
    }

    #[test]
    fn bloom_filter_prunes_segment_for_unknown_tag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom_prune.csx");
        write_segment_with_tags(&path, &["server-1", "server-2"], 20);

        let reader = SegmentReader::open(&path).unwrap();

        // Query with a tag value that does NOT exist in the segment.
        // The bloom filter should prune the entire segment, returning
        // zero rows.
        let batch = reader
            .read_projected_filtered_with_predicates(&["cpu"], None, &[("host", "server-999")])
            .unwrap();
        assert_eq!(batch.num_rows(), 0, "bloom filter should prune unknown tag");
    }

    #[test]
    fn bloom_filter_passes_known_tag() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom_pass.csx");
        write_segment_with_tags(&path, &["server-1", "server-2"], 20);

        let reader = SegmentReader::open(&path).unwrap();

        // Query with a tag value that DOES exist — bloom filter must
        // not prune it (no false negatives).
        let batch = reader
            .read_projected_filtered_with_predicates(
                &["cpu", "host"],
                None,
                &[("host", "server-1")],
            )
            .unwrap();
        // Should return rows (bloom filter passes, all row groups read).
        // The batch has all rows (not just server-1) because row-group
        // level filtering doesn't do value-level matching.
        assert!(
            batch.num_rows() > 0,
            "bloom filter should not prune known tag"
        );
    }

    #[test]
    fn bloom_filter_roundtrip_metadata() {
        // Verify that bloom filter data survives the write → read
        // round-trip through segment metadata serialization.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom_rt.csx");
        write_segment_with_tags(&path, &["alpha", "beta", "gamma"], 5);

        let reader = SegmentReader::open(&path).unwrap();
        let host_meta = reader
            .column_metadata()
            .iter()
            .find(|c| c.name == "host")
            .unwrap();

        let bf = host_meta.bloom_filter.as_ref().unwrap();

        // All written tag values must be present
        assert!(crate::segment::bloom::bloom_filter_contains(bf, "alpha"));
        assert!(crate::segment::bloom::bloom_filter_contains(bf, "beta"));
        assert!(crate::segment::bloom::bloom_filter_contains(bf, "gamma"));

        // An unwritten value should (almost certainly) be absent
        assert!(!crate::segment::bloom::bloom_filter_contains(bf, "delta"));
    }

    #[test]
    #[cfg(feature = "field-encryption")]
    fn field_encryption_roundtrip() {
        use crate::segment::field_encryption::{
            FieldEncryptionConfig, FieldEncryptionKey, StaticKeyProvider,
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("encrypted.csx");

        let key_bytes: [u8; 32] = [0xAB; 32];
        let enc_key = FieldEncryptionKey::new("key-1".to_string(), key_bytes);

        let mut enc_config = FieldEncryptionConfig::default();
        enc_config.encrypt_column("cpu", enc_key);

        let points: Vec<Point> = (0..100)
            .map(|i| make_point("server-1", 50.0 + i as f64, 1_000_000 + i * 10_000))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 50,
            compress: true,
            field_encryption: enc_config,
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        let meta = writer.finalize().unwrap();
        assert_eq!(meta.row_count, 100);

        // Verify column metadata flags
        let reader_no_key = SegmentReader::open(&path).unwrap();
        let cpu_meta = reader_no_key
            .column_metadata()
            .iter()
            .find(|c| c.name == "cpu")
            .unwrap();
        assert!(cpu_meta.encrypted);
        assert_eq!(cpu_meta.key_id.as_deref(), Some("key-1"));

        // Stats should be suppressed for encrypted columns
        let ts_meta = reader_no_key
            .column_metadata()
            .iter()
            .find(|c| c.name == "timestamp")
            .unwrap();
        assert!(!ts_meta.encrypted);
        assert!(ts_meta.stats.min_value != i64::MAX || ts_meta.stats.max_value != i64::MIN);

        // Bloom filter should NOT exist for encrypted tag columns — but
        // "cpu" is a field, not a tag, so check that "host" still has a bloom.
        let host_meta = reader_no_key
            .column_metadata()
            .iter()
            .find(|c| c.name == "host")
            .unwrap();
        assert!(host_meta.bloom_filter.is_some());

        // Read back with a key provider
        let mut provider = StaticKeyProvider::new();
        provider.add_key("key-1", key_bytes);

        let mut reader = SegmentReader::open(&path).unwrap();
        reader.set_key_provider(Arc::new(provider));

        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), 100);

        let cpu_col = batch
            .column_by_name("cpu")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..100 {
            assert!(
                (cpu_col.value(i) - (50.0 + i as f64)).abs() < 1e-9,
                "cpu mismatch at row {i}"
            );
        }
    }

    /// The validity bitmap lives in the data region, so the whole-file CRC32c
    /// verified at `open()` must cover it — otherwise a flipped bit would
    /// silently turn a real value into a NULL (or vice versa) with no error.
    #[test]
    fn corrupted_validity_bitmap_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nulls.csx");

        // Sparse column => at least one block carries a validity bitmap.
        let tags = BTreeMap::from([("host".to_string(), "h".to_string())]);
        let key = SeriesKey::new("m", tags).unwrap();
        let points: Vec<Point> = (0..40i64)
            .map(|i| {
                let fields = if i % 2 == 0 {
                    BTreeMap::from([("v".to_string(), FieldValue::F64(i as f64))])
                } else {
                    BTreeMap::from([("w".to_string(), FieldValue::F64(1.0))])
                };
                Point::new(key.clone(), fields, 1_000 + i * 10).unwrap()
            })
            .collect();

        let mut writer = SegmentWriter::new(&path, SegmentWriterConfig::default()).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        // Locate a bitmap and flip a bit inside it.
        let reader = SegmentReader::open(&path).unwrap();
        let (off, len) = reader
            .metadata
            .row_group_blocks
            .iter()
            .flatten()
            .find(|b| b.validity_length > 0)
            .map(|b| (b.validity_offset as usize, b.validity_length))
            .expect("a sparse column must have written a validity bitmap");
        assert!(len > 0);
        drop(reader);

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[off] ^= 0b0000_0001;
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            matches!(
                SegmentReader::open(&path),
                Err(SegmentError::ChecksumMismatch { .. })
            ),
            "a corrupted validity bitmap must fail the file checksum"
        );
    }

    /// D-NULL + field encryption: the validity bitmap is stored *outside*
    /// the encrypted payload, so a reader without the key must still be
    /// blocked on values while a reader with the key sees correct nulls.
    #[test]
    #[cfg(feature = "field-encryption")]
    fn encrypted_column_preserves_nulls() {
        use crate::segment::field_encryption::{
            FieldEncryptionConfig, FieldEncryptionKey, StaticKeyProvider,
        };
        use arrow::array::Array;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("encrypted_nulls.csx");

        let enc_key = FieldEncryptionKey::new("key-1".to_string(), [0xCD; 32]);
        let mut enc_config = FieldEncryptionConfig::default();
        enc_config.encrypt_column("cpu", enc_key);

        // Every other point omits `cpu` entirely.
        let tags = BTreeMap::from([("host".to_string(), "server-1".to_string())]);
        let key = SeriesKey::new("cpu_usage", tags).unwrap();
        let points: Vec<Point> = (0..40i64)
            .map(|i| {
                let fields = if i % 2 == 0 {
                    BTreeMap::from([("cpu".to_string(), FieldValue::F64(i as f64))])
                } else {
                    BTreeMap::from([("other".to_string(), FieldValue::F64(1.0))])
                };
                Point::new(key.clone(), fields, 1_000_000 + i * 10_000).unwrap()
            })
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 16,
            compress: true,
            field_encryption: enc_config,
            ..Default::default()
        };
        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let mut provider = StaticKeyProvider::new();
        provider.add_key("key-1", [0xCD; 32]);
        let mut reader = SegmentReader::open(&path).unwrap();
        reader.set_key_provider(Arc::new(provider));
        let batch = reader.read_all().unwrap();

        let cpu = batch
            .column_by_name("cpu")
            .expect("cpu column")
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("f64");
        assert_eq!(cpu.null_count(), 20, "half the rows have no cpu value");
        // And the surviving values must be intact, not shifted by the bitmap.
        let present: Vec<f64> = (0..cpu.len())
            .filter(|i| cpu.is_valid(*i))
            .map(|i| cpu.value(i))
            .collect();
        assert_eq!(present.len(), 20);
        assert!(
            present.iter().all(|v| *v % 2.0 == 0.0),
            "values corrupted: {present:?}"
        );
    }

    #[test]
    #[cfg(feature = "field-encryption")]
    fn field_encryption_wrong_key_fails() {
        use crate::segment::field_encryption::{
            FieldEncryptionConfig, FieldEncryptionKey, StaticKeyProvider,
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enc_wrong_key.csx");

        let key_bytes: [u8; 32] = [0xCD; 32];
        let enc_key = FieldEncryptionKey::new("key-2".to_string(), key_bytes);

        let mut enc_config = FieldEncryptionConfig::default();
        enc_config.encrypt_column("cpu", enc_key);

        let points: Vec<Point> = (0..20)
            .map(|i| make_point("server-1", 10.0 + i as f64, 1_000_000 + i * 10_000))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 100,
            compress: false,
            field_encryption: enc_config,
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        // Wrong key — decryption should fail
        let mut wrong_provider = StaticKeyProvider::new();
        wrong_provider.add_key("key-2", [0xFF; 32]);

        let mut reader = SegmentReader::open(&path).unwrap();
        reader.set_key_provider(Arc::new(wrong_provider));

        let result = reader.read_all();
        assert!(result.is_err(), "reading with wrong key should fail");
    }

    #[test]
    #[cfg(feature = "field-encryption")]
    fn field_encryption_no_provider_fails() {
        use crate::segment::field_encryption::{FieldEncryptionConfig, FieldEncryptionKey};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enc_no_provider.csx");

        let key_bytes: [u8; 32] = [0xEF; 32];
        let enc_key = FieldEncryptionKey::new("key-3".to_string(), key_bytes);

        let mut enc_config = FieldEncryptionConfig::default();
        enc_config.encrypt_column("cpu", enc_key);

        let points: Vec<Point> = (0..10)
            .map(|i| make_point("server-1", 1.0, 1_000_000 + i * 10_000))
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 100,
            compress: false,
            field_encryption: enc_config,
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        // No key provider at all
        let reader = SegmentReader::open(&path).unwrap();
        let result = reader.read_all();
        assert!(
            result.is_err(),
            "reading encrypted column without key provider should fail"
        );
    }

    // ── Per-row-group bloom filter tests ─────────────────

    #[test]
    fn row_group_bloom_filters_present_for_tag_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rg_bloom.csx");

        // 30 points across 3 hosts, sorted by time → hosts interleave across
        // row groups when row_group_size=10.
        write_segment_with_tags(&path, &["alpha", "beta", "gamma"], 10);

        let reader = SegmentReader::open(&path).unwrap();
        let host_meta = reader
            .column_metadata()
            .iter()
            .find(|c| c.name == "host")
            .unwrap();

        // Per-RG blooms should exist for tag columns
        assert!(
            host_meta.row_group_blooms.is_some(),
            "tag column should have per-row-group bloom filters"
        );

        let rg_blooms = host_meta.row_group_blooms.as_ref().unwrap();
        assert_eq!(rg_blooms.len(), reader.row_group_count());

        // Each bloom should be non-empty
        for (i, bloom) in rg_blooms.iter().enumerate() {
            assert!(!bloom.is_empty(), "bloom for rg{i} should be non-empty");
        }
    }

    #[test]
    fn row_group_bloom_prunes_non_matching_row_groups() {
        // Create a segment where host "alpha" is ONLY in the first row group
        // and host "beta" is ONLY in the second row group.
        //
        // Points are written in order (alpha first, beta second) with
        // increasing timestamps, so the writer's sort will keep them in
        // their respective row groups.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rg_bloom_prune.csx");

        let mut points = Vec::new();
        // First 10 points: host=alpha, ts=[0, 9000]
        for i in 0..10 {
            points.push(make_point("alpha", i as f64, i as i64 * 1000));
        }
        // Next 10 points: host=beta, ts=[10000, 19000]
        for i in 0..10 {
            points.push(make_point("beta", (100 + i) as f64, (10 + i) as i64 * 1000));
        }

        let config = SegmentWriterConfig {
            row_group_size: 10,
            compress: false,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: std::collections::HashMap::new(),
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_group_count(), 2);

        // Verify per-RG blooms: rg0 should contain "alpha" but NOT "beta"
        let host_meta = reader
            .column_metadata()
            .iter()
            .find(|c| c.name == "host")
            .unwrap();
        let rg_blooms = host_meta.row_group_blooms.as_ref().unwrap();

        assert!(crate::segment::bloom::bloom_filter_contains(
            &rg_blooms[0],
            "alpha"
        ));
        assert!(!crate::segment::bloom::bloom_filter_contains(
            &rg_blooms[0],
            "beta"
        ));
        assert!(crate::segment::bloom::bloom_filter_contains(
            &rg_blooms[1],
            "beta"
        ));
        assert!(!crate::segment::bloom::bloom_filter_contains(
            &rg_blooms[1],
            "alpha"
        ));

        // Query for "alpha" — should only return rg0 (10 rows)
        let batch = reader
            .read_projected_filtered_with_predicates(&["cpu", "host"], None, &[("host", "alpha")])
            .unwrap();
        assert_eq!(
            batch.num_rows(),
            10,
            "only rg0 should be returned for alpha"
        );

        // Query for "beta" — should only return rg1 (10 rows)
        let batch = reader
            .read_projected_filtered_with_predicates(&["cpu", "host"], None, &[("host", "beta")])
            .unwrap();
        assert_eq!(batch.num_rows(), 10, "only rg1 should be returned for beta");

        // Query for "gamma" (non-existent) — should return 0 rows
        let batch = reader
            .read_projected_filtered_with_predicates(&["cpu"], None, &[("host", "gamma")])
            .unwrap();
        assert_eq!(batch.num_rows(), 0, "non-existent tag should prune all RGs");
    }
}
