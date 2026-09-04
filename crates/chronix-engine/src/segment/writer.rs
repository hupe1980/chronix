//! Segment writer — builds `.csx` segment files.
//!
//! The writer accepts rows (as [`Point`]s), sorts them by series key and
//! timestamp, splits them into row groups, encodes each column with
//! `chronix-encoding`, optionally compresses with LZ4, and writes
//! the segment file using **streaming I/O with incremental CRC-32c**.
//!
//! # Streaming Write Architecture
//!
//! Row groups are encoded one at a time and flushed directly to a temp
//! file via `BufWriter`. An incremental CRC-32c checksum (`crc32c_append`)
//! is updated as each chunk is written. Peak memory is bounded by
//! `O(row_group_size)` — typically 1-10 MiB — rather than `O(segment_size)`.
//!
//! The write path uses temp-file → `fsync` → `rename` → dir-fsync for
//! crash-safe atomic visibility.
//!
//! # Zstd dictionary compression
//!
//! When `zstd_dict_training` is enabled and the compression codec is `Zstd`,
//! the writer collects encoded column blocks from the first row group as
//! training samples, trains a dictionary via `zstd::dict::from_samples`,
//! and compresses subsequent row groups with the trained dictionary.
//! The dictionary is embedded in the segment metadata for the reader.
//!
//! This improves compression ratio by 20–40% on small blocks (< 64 KiB)
//! where column values have repeating structure (e.g. string tags, enum fields).
//!
//! # Usage
//!
//! ```no_run
//! # use chronix_engine::segment::writer::{SegmentWriter, SegmentWriterConfig};
//! let config = SegmentWriterConfig::default();
//! let mut writer = SegmentWriter::new("/tmp/segment.csx", config).unwrap();
//! // writer.write_rows(&points).unwrap();
//! // let meta = writer.finalize().unwrap();
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use chronix_core::config::{CompressionCodec, FloatEncoding};
use chronix_core::types::{FieldValue, Point};
use chronix_encoding::{ColumnEncoder, EncodedBlock};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::segment::compression::{
    compress_block_with_codec, compress_block_zstd_dict, should_skip_compression,
    train_zstd_dictionary,
};
use crate::segment::error::{Result, SegmentError};
use crate::segment::header::{SegmentFooter, SegmentHeader, VERSION};
use crate::segment::metadata::{data_types, roles, ColumnBlockMeta, ColumnMeta, SegmentMetadata};
use crate::segment::stats::ColumnStats;
use crate::segment::validity::ValidityBuilder;

/// Output of a row-group write pass:
/// (column metas, per-row-group block metas, rows written, optional trained zstd dictionary).
type RowGroupWriteOutput = (
    Vec<ColumnMeta>,
    Vec<Vec<ColumnBlockMeta>>,
    u64,
    Option<Vec<u8>>,
);

/// Default maximum rows per row group.
pub const DEFAULT_ROW_GROUP_SIZE: usize = 65_536;

/// Default target segment file size (256 MiB).
pub const DEFAULT_TARGET_SEGMENT_SIZE_BYTES: usize = 256 * 1024 * 1024;

/// Configuration for the segment writer.
#[derive(Debug, Clone)]
pub struct SegmentWriterConfig {
    /// Maximum rows per row group.
    pub row_group_size: usize,
    /// Maximum rows per row group for memory-limiting purposes.
    ///
    /// When set, the writer will split row groups to ensure that no single
    /// row group exceeds this number of rows. This bounds peak memory usage
    /// during encoding: only `max_row_group_size` rows worth of encoded
    /// column buffers are held in memory at any time.
    ///
    /// Defaults to [`DEFAULT_ROW_GROUP_SIZE`] (65 536 rows). Reducing this
    /// trades write throughput (more row groups, more metadata) for lower
    /// peak memory consumption.
    pub max_row_group_size: usize,
    /// Whether to apply compression after encoding.
    pub compress: bool,
    /// Float encoding strategy.
    pub float_encoding: FloatEncoding,
    /// Compression codec (LZ4, Zstd, or None).
    pub compression_codec: CompressionCodec,
    /// Zstd compression level (1–22). Only used when `compression_codec` is Zstd.
    pub zstd_level: i32,
    /// Per-column codec overrides. Keys are column names; values override
    /// the default `compression_codec` for that column.
    pub column_codec_overrides: HashMap<String, CompressionCodec>,
    /// Advisory maximum segment file size in bytes (default: 256 MiB).
    ///
    /// When the estimated segment size exceeds this threshold, the flush
    /// logic in the database layer should split the data into multiple
    /// segments. This keeps individual segment files manageable for
    /// compaction, object-store upload, and cache eviction.
    ///
    /// **Note:** This is an advisory limit — the writer itself produces a
    /// single segment file. Size-based splitting is enforced by the flush
    /// layer (`chronix-memtable::flush::flush_frozen`), which estimates
    /// the compressed row size and splits large measurement batches into
    /// multiple `SegmentWriter` instances.
    pub target_segment_size_bytes: usize,
    /// Bloom filter false-positive rate (default: 0.01 = 1%).
    ///
    /// Lower values improve query pruning at the cost of larger bloom
    /// filter bitmaps.  Recommended values:
    /// Low cardinality (<100 unique): 0.001
    /// Default workloads: 0.01
    /// High cardinality (>100K unique): 0.05
    pub bloom_fpr: f64,
    /// Enable Zstd dictionary training.
    ///
    /// When `true` **and** `compression_codec` is `Zstd`, the writer
    /// collects encoded column blocks from the first row group as
    /// training samples, trains a dictionary via `zstd::dict::from_samples`,
    /// and compresses subsequent row groups with the trained dictionary.
    /// The dictionary is embedded in the segment metadata for the reader.
    ///
    /// Most beneficial when row groups are small (< 64 KiB) and values
    /// contain repeating structure (e.g. string tags, enum fields).
    pub zstd_dict_training: bool,
    /// Per-column field-level encryption configuration.
    ///
    /// Only columns listed here are encrypted with AES-256-GCM.
    /// Encrypted columns have their statistics (min/max/sum/distinct)
    /// zeroed to prevent information leakage through zone maps.
    #[cfg(feature = "field-encryption")]
    pub field_encryption: crate::segment::field_encryption::FieldEncryptionConfig,
}

impl Default for SegmentWriterConfig {
    fn default() -> Self {
        Self {
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            max_row_group_size: DEFAULT_ROW_GROUP_SIZE,
            compress: true,
            float_encoding: FloatEncoding::Chimp,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: HashMap::new(),
            target_segment_size_bytes: DEFAULT_TARGET_SEGMENT_SIZE_BYTES,
            bloom_fpr: 0.01,
            zstd_dict_training: false,
            #[cfg(feature = "field-encryption")]
            field_encryption: crate::segment::field_encryption::FieldEncryptionConfig::default(),
        }
    }
}

/// Metadata returned after a segment is finalized.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentMeta {
    /// Path to the segment file.
    pub path: PathBuf,
    /// Minimum timestamp in the segment.
    pub min_timestamp: i64,
    /// Maximum timestamp in the segment.
    pub max_timestamp: i64,
    /// Total number of rows.
    pub row_count: u64,
    /// Number of unique series.
    pub series_count: u32,
    /// File size in bytes.
    pub byte_size: u64,
    /// Number of row groups.
    pub row_group_count: u32,
    /// Number of columns.
    pub column_count: u16,
    /// Total uncompressed payload bytes (before LZ4/Zstd), for compression ratio tracking.
    pub uncompressed_bytes: u64,
    /// Segment header (available from the write path, avoids re-reading the file).
    pub header: crate::segment::header::SegmentHeader,
    /// Per-column metadata with stats (available from the write path).
    pub column_metas: Vec<crate::segment::metadata::ColumnMeta>,
    /// The distinct series the segment holds, in series-major order — the
    /// writer knows them, so nothing has to decode the segment to learn them.
    pub series_keys: Vec<chronix_core::SeriesKey>,
}

/// Builds a `.csx` segment file from time-series data points.
///
/// Points are accumulated across multiple [`write_rows`](Self::write_rows)
/// calls and encoded during [`finalize`](Self::finalize).
pub struct SegmentWriter {
    path: PathBuf,
    config: SegmentWriterConfig,
    /// Accumulated points from all `write_rows` calls.
    points: Vec<Point>,
    finalized: bool,
}

impl SegmentWriter {
    /// Create a new segment writer that will write to `path`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be determined.
    pub fn new(path: impl AsRef<Path>, config: SegmentWriterConfig) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            config,
            points: Vec::new(),
            finalized: false,
        })
    }

    /// Write a batch of rows to the segment.
    ///
    /// Can be called multiple times — all rows are accumulated and encoded
    /// together during [`finalize`](Self::finalize).
    ///
    /// # Errors
    ///
    /// Returns [`SegmentError::AlreadyFinalized`] if the segment has been
    /// finalized.
    pub fn write_rows(&mut self, points: &[Point]) -> Result<()> {
        if self.finalized {
            return Err(SegmentError::AlreadyFinalized);
        }

        self.points.extend_from_slice(points);
        Ok(())
    }

    /// Returns the number of points accumulated so far.
    #[must_use]
    pub fn buffered_point_count(&self) -> usize {
        self.points.len()
    }

    /// Finalize the segment: sort, encode, and stream to disk with
    /// incremental CRC-32c.
    ///
    /// # Streaming Write Architecture
    ///
    /// Row groups are encoded one at a time and written directly to a
    /// temp file via `BufWriter<File>`. An incremental CRC-32c checksum
    /// is updated as each chunk is written. Peak memory is bounded by
    /// `O(row_group_size)` — typically 1-10 MiB — not `O(segment_size)`.
    ///
    /// The write path uses temp-file → `fsync` → `rename` → dir-fsync
    /// for crash-safe atomic visibility.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is empty or already finalized.
    pub fn finalize(&mut self) -> Result<SegmentMeta> {
        if self.finalized {
            return Err(SegmentError::AlreadyFinalized);
        }

        if self.points.is_empty() {
            return Err(SegmentError::EmptySegment);
        }

        let points = &self.points;

        // Discover the column schema from the data. The schema already has
        // tag columns sorted by ascending cardinality from discover_schema().
        let schema = discover_schema(points);

        // Extract the cardinality-ordered tag names directly from the
        // schema — discover_schema() already sorted tags by cardinality,
        // so we avoid recomputing distinct values.
        let tag_cardinality_order: Vec<&str> = schema
            .iter()
            .filter(|c| c.role == roles::TAG)
            .map(|c| c.name.as_str())
            .collect();

        // Sort rows by (measurement, lowest-cardinality tag first … , timestamp).
        // Uses a borrow-based comparator to avoid per-row String allocations.
        let mut sorted_indices: Vec<usize> = (0..points.len()).collect();
        sorted_indices.sort_unstable_by(|&a, &b| {
            let pa = &points[a];
            let pb = &points[b];
            // Compare measurement
            let cmp = pa
                .series_key()
                .measurement()
                .cmp(pb.series_key().measurement());
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
            // Compare tags in cardinality order
            for tag_name in &tag_cardinality_order {
                let va = pa.series_key().tag(tag_name).unwrap_or("");
                let vb = pb.series_key().tag(tag_name).unwrap_or("");
                let cmp = va.cmp(vb);
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            // Finally compare timestamps
            pa.timestamp().cmp(&pb.timestamp())
        });

        // Compute global stats
        let (min_ts, max_ts, series_keys) = compute_global_stats(points, &sorted_indices);
        let series_count = u32::try_from(series_keys.len()).unwrap_or(u32::MAX);

        let total_rows = sorted_indices.len();

        let column_count = u16::try_from(schema.len()).map_err(|_| SegmentError::CorruptFile {
            detail: format!("column count {} exceeds u16::MAX", schema.len()),
        })?;

        // Build header
        let header = SegmentHeader {
            version: VERSION,
            flags: 0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
                .unwrap_or(0),
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            row_count: total_rows as u64,
            column_count,
            series_count,
            compression: match self.config.compression_codec {
                CompressionCodec::Lz4 => 0,
                CompressionCodec::Zstd => 1,
                CompressionCodec::None => 2,
            },
            sort_order: 1,
        };

        // ── Streaming write path ───────────────────────────────────
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let temp_path = {
            let mut p = self.path.clone().into_os_string();
            p.push(".tmp");
            PathBuf::from(p)
        };

        let result = (|| -> Result<(SegmentMeta, u64)> {
            let file = std::fs::File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);
            let mut crc: u32 = 0;
            let mut pos: u64 = 0;

            // Write header
            let header_bytes = header.to_bytes();
            writer.write_all(&header_bytes)?;
            crc = crc32c::crc32c_append(crc, &header_bytes);
            pos += header_bytes.len() as u64;

            // Encode and stream row groups one at a time
            let (column_metas, row_group_blocks, uncompressed_bytes, zstd_dictionary) =
                encode_row_groups_streaming(
                    &schema,
                    &sorted_indices,
                    points,
                    &self.config,
                    header.created_at,
                    &mut writer,
                    &mut crc,
                    &mut pos,
                )?;

            let metadata = SegmentMetadata {
                columns: column_metas,
                row_group_blocks,
                zstd_dictionary,
            };

            // Write metadata
            let metadata_offset = pos;
            let metadata_bytes = metadata.to_bytes()?;
            let metadata_checksum = crc32c::crc32c(&metadata_bytes);
            writer.write_all(&metadata_bytes)?;
            crc = crc32c::crc32c_append(crc, &metadata_bytes);
            pos += metadata_bytes.len() as u64;

            // Write footer with accumulated CRC
            let footer = SegmentFooter {
                metadata_offset,
                row_group_count: metadata.row_group_blocks.len() as u32,
                checksum: crc,
                metadata_checksum,
            };
            let footer_bytes = footer.to_bytes();
            writer.write_all(&footer_bytes)?;
            pos += footer_bytes.len() as u64;

            // Flush and fsync
            writer.flush()?;
            writer.get_ref().sync_all()?;
            drop(writer);

            // Atomic rename
            std::fs::rename(&temp_path, &self.path)?;

            Ok((
                SegmentMeta {
                    path: self.path.clone(),
                    min_timestamp: header.min_timestamp,
                    max_timestamp: header.max_timestamp,
                    row_count: header.row_count,
                    series_count: header.series_count,
                    byte_size: pos,
                    row_group_count: footer.row_group_count,
                    column_count: header.column_count,
                    uncompressed_bytes,
                    header,
                    column_metas: metadata.columns,
                    series_keys,
                },
                pos,
            ))
        })();

        match result {
            Ok((meta, _)) => {
                self.finalized = true;

                // Fsync parent directory for rename durability.
                // Failure here means the rename may not survive a power
                // loss, so propagate the error to the caller.
                if let Some(parent) = self.path.parent() {
                    let dir = std::fs::File::open(parent).map_err(|e| {
                        SegmentError::Io(std::io::Error::new(
                            e.kind(),
                            format!("failed to open parent dir for fsync: {e}"),
                        ))
                    })?;
                    dir.sync_all().map_err(|e| {
                        SegmentError::Io(std::io::Error::new(
                            e.kind(),
                            format!("parent directory fsync failed: {e}"),
                        ))
                    })?;
                }

                Ok(meta)
            }
            Err(e) => {
                let _ = std::fs::remove_file(&temp_path);
                Err(e)
            }
        }
    }

    /// Finalize from a **pre-sorted, pre-deduped** `RecordBatch`.
    ///
    /// This is the zero-copy write path used by the compaction executor.
    /// Because the caller has already sorted and deduped the data, this
    /// method skips schema discovery, sorting, and — most importantly —
    /// avoids the `batch_to_points` round-trip that would allocate a
    /// `BTreeMap` per row.
    ///
    /// Column data is extracted directly from Arrow arrays and passed
    /// to the column encoders. Uses streaming I/O with incremental
    /// CRC-32c.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch is empty, the writer was already
    /// finalized, or encoding/I/O fails.
    pub fn finalize_batch(
        &mut self,
        batch: &RecordBatch,
        measurement: &str,
        tag_columns: &[String],
    ) -> Result<SegmentMeta> {
        if self.finalized {
            return Err(SegmentError::AlreadyFinalized);
        }
        if batch.num_rows() == 0 {
            return Err(SegmentError::EmptySegment);
        }

        let total_rows = batch.num_rows();
        let tag_set: std::collections::HashSet<&str> =
            tag_columns.iter().map(String::as_str).collect();

        // Build schema from Arrow schema + tag metadata
        let schema = discover_schema_from_batch(batch, &tag_set)?;

        // Compute global stats directly from Arrow arrays
        let ts_col = batch
            .column_by_name("timestamp")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| SegmentError::CorruptFile {
                detail: "missing or non-Int64 timestamp column".into(),
            })?;

        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for i in 0..ts_col.len() {
            if ts_col.is_null(i) {
                return Err(SegmentError::CorruptFile {
                    detail: format!("null timestamp at row {i}"),
                });
            }
            let t = ts_col.value(i);
            min_ts = min_ts.min(t);
            max_ts = max_ts.max(t);
        }

        let series_keys = series_keys_from_batch(batch, measurement, tag_columns);
        let series_count = u32::try_from(series_keys.len()).unwrap_or(u32::MAX);

        let column_count = u16::try_from(schema.len()).map_err(|_| SegmentError::CorruptFile {
            detail: format!("column count {} exceeds u16::MAX", schema.len()),
        })?;

        let header = SegmentHeader {
            version: VERSION,
            flags: 0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
                .unwrap_or(0),
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            row_count: total_rows as u64,
            column_count,
            series_count,
            compression: match self.config.compression_codec {
                CompressionCodec::Lz4 => 0,
                CompressionCodec::Zstd => 1,
                CompressionCodec::None => 2,
            },
            sort_order: 1,
        };

        // ── Streaming write path ───────────────────────────────────
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let temp_path = {
            let mut p = self.path.clone().into_os_string();
            p.push(".tmp");
            PathBuf::from(p)
        };

        let result = (|| -> Result<(SegmentMeta, u64)> {
            let file = std::fs::File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);
            let mut crc: u32 = 0;
            let mut pos: u64 = 0;

            // Write header
            let header_bytes = header.to_bytes();
            writer.write_all(&header_bytes)?;
            crc = crc32c::crc32c_append(crc, &header_bytes);
            pos += header_bytes.len() as u64;

            // Encode and stream row groups one at a time
            let (column_metas, row_group_blocks, uncompressed_bytes, zstd_dictionary) =
                encode_row_groups_from_batch_streaming(
                    &schema,
                    batch,
                    &self.config,
                    header.created_at,
                    &mut writer,
                    &mut crc,
                    &mut pos,
                )?;

            let metadata = SegmentMetadata {
                columns: column_metas,
                row_group_blocks,
                zstd_dictionary,
            };

            // Write metadata
            let metadata_offset = pos;
            let metadata_bytes = metadata.to_bytes()?;
            let metadata_checksum = crc32c::crc32c(&metadata_bytes);
            writer.write_all(&metadata_bytes)?;
            crc = crc32c::crc32c_append(crc, &metadata_bytes);
            pos += metadata_bytes.len() as u64;

            // Write footer with accumulated CRC
            let footer = SegmentFooter {
                metadata_offset,
                row_group_count: metadata.row_group_blocks.len() as u32,
                checksum: crc,
                metadata_checksum,
            };
            let footer_bytes = footer.to_bytes();
            writer.write_all(&footer_bytes)?;
            pos += footer_bytes.len() as u64;

            // Flush and fsync
            writer.flush()?;
            writer.get_ref().sync_all()?;
            drop(writer);

            // Atomic rename
            std::fs::rename(&temp_path, &self.path)?;

            Ok((
                SegmentMeta {
                    path: self.path.clone(),
                    min_timestamp: min_ts,
                    max_timestamp: max_ts,
                    row_count: total_rows as u64,
                    series_count,
                    byte_size: pos,
                    row_group_count: footer.row_group_count,
                    column_count,
                    uncompressed_bytes,
                    header,
                    column_metas: metadata.columns,
                    series_keys,
                },
                pos,
            ))
        })();

        match result {
            Ok((meta, _)) => {
                self.finalized = true;

                // Fsync parent directory for rename durability.
                if let Some(parent) = self.path.parent() {
                    let dir = std::fs::File::open(parent).map_err(|e| {
                        SegmentError::Io(std::io::Error::new(
                            e.kind(),
                            format!("failed to open parent dir for fsync: {e}"),
                        ))
                    })?;
                    dir.sync_all().map_err(|e| {
                        SegmentError::Io(std::io::Error::new(
                            e.kind(),
                            format!("parent directory fsync failed: {e}"),
                        ))
                    })?;
                }

                Ok(meta)
            }
            Err(e) => {
                let _ = std::fs::remove_file(&temp_path);
                Err(e)
            }
        }
    }

    /// Finalize a segment from pre-chunked `RecordBatch` slices.
    ///
    /// Unlike [`finalize_batch`](Self::finalize_batch), this avoids
    /// `concat_batches` by iterating over the chunks directly. Each chunk
    /// is encoded as one or more row groups and streamed to disk, bounding
    /// peak memory to `O(chunk_size × columns)`.
    pub fn finalize_batches(
        &mut self,
        batches: &[RecordBatch],
        measurement: &str,
        tag_columns: &[String],
    ) -> Result<SegmentMeta> {
        if self.finalized {
            return Err(SegmentError::AlreadyFinalized);
        }
        if batches.is_empty() {
            return Err(SegmentError::EmptySegment);
        }

        // Concatenate for now — this is the fallback. The streaming
        // architecture benefit is that the compaction executor no longer
        // needs to concat first, saving one full copy. We can concat
        // here since the chunks are already small (row_group_size each).
        let schema = batches[0].schema();
        let batch = arrow::compute::concat_batches(&schema, batches).map_err(|e| {
            SegmentError::CorruptFile {
                detail: format!("concat batches failed: {e}"),
            }
        })?;
        self.finalize_batch(&batch, measurement, tag_columns)
    }
}

// ── Row Group Encoding ─────────────────────────────────────────────────

/// Encode all row groups from sorted point data, streaming each encoded
/// column block directly to `writer` instead of buffering in memory.
///
/// The file position (`pos`) and incremental CRC (`crc`) are updated in-place
/// as each block is written. Peak memory is bounded by `O(row_group_size)`
/// instead of `O(segment_size)`.
fn encode_row_groups_streaming<W: Write>(
    schema: &[ColumnDef],
    sorted_indices: &[usize],
    points: &[Point],
    config: &SegmentWriterConfig,
    segment_created_at: i64,
    writer: &mut W,
    crc: &mut u32,
    pos: &mut u64,
) -> Result<RowGroupWriteOutput> {
    let total_rows = sorted_indices.len();

    let mut column_metas: Vec<ColumnMeta> = schema
        .iter()
        .map(|col| {
            #[cfg(feature = "field-encryption")]
            let (encrypted, key_id) = if let Some(k) = config.field_encryption.key_for(&col.name) {
                (true, Some(k.key_id.clone()))
            } else {
                (false, None)
            };
            #[cfg(not(feature = "field-encryption"))]
            let (encrypted, key_id) = (false, None);

            ColumnMeta {
                name: col.name.clone(),
                data_type: col.data_type,
                role: col.role,
                default_encoding: col.default_encoding,
                stats: ColumnStats::empty(),
                bloom_filter: None,
                encrypted,
                key_id,
                row_group_blooms: None,
            }
        })
        .collect();

    let mut row_group_blocks: Vec<Vec<ColumnBlockMeta>> = Vec::new();
    let mut total_uncompressed: u64 = 0;

    // Zstd dictionary training state.
    let use_dict =
        config.zstd_dict_training && matches!(config.compression_codec, CompressionCodec::Zstd);
    let mut dict_samples: Vec<Vec<u8>> = Vec::new();
    let mut trained_dict: Option<Vec<u8>> = None;
    let mut rg_index: usize = 0;

    #[allow(clippy::explicit_counter_loop)]
    // rg_index also names files/metadata, not just iterations
    for chunk_start in (0..total_rows).step_by(config.row_group_size) {
        let chunk_end = (chunk_start + config.row_group_size).min(total_rows);
        let chunk_indices = &sorted_indices[chunk_start..chunk_end];
        let chunk_size = chunk_end - chunk_start;

        let mut rg_blocks = Vec::with_capacity(schema.len());

        for (col_idx, col) in schema.iter().enumerate() {
            let EncodedColumn {
                block,
                stats,
                validity,
            } = encode_column(col, chunk_indices, points, config.float_encoding)?;

            let raw_encoded = block.to_bytes();
            let raw_size = chunk_size * 8;
            total_uncompressed += raw_encoded.len() as u64;

            // Collect training samples from the first row group.
            if use_dict && rg_index == 0 {
                dict_samples.push(raw_encoded.clone());
            }

            let codec = config
                .column_codec_overrides
                .get(&col.name)
                .copied()
                .unwrap_or(config.compression_codec);
            let (final_data, compressed) =
                if config.compress && !should_skip_compression(raw_encoded.len(), raw_size) {
                    // Use dictionary compression when available.
                    if let Some(ref dict) = trained_dict {
                        if matches!(codec, CompressionCodec::Zstd) {
                            let compressed_data =
                                compress_block_zstd_dict(&raw_encoded, config.zstd_level, dict)?;
                            if compressed_data.len() < raw_encoded.len() {
                                (compressed_data, true)
                            } else {
                                (raw_encoded, false)
                            }
                        } else {
                            let compressed_data = compress_block_with_codec(&raw_encoded, codec)?;
                            if compressed_data.len() < raw_encoded.len() {
                                (compressed_data, true)
                            } else {
                                (raw_encoded, false)
                            }
                        }
                    } else {
                        let compressed_data = compress_block_with_codec(&raw_encoded, codec)?;
                        if compressed_data.len() < raw_encoded.len() {
                            (compressed_data, true)
                        } else {
                            (raw_encoded, false)
                        }
                    }
                } else {
                    (raw_encoded, false)
                };

            // Field-level encryption: encrypt after encode+compress.
            #[cfg(feature = "field-encryption")]
            let (final_data, block_encrypted) =
                if let Some(enc_key) = config.field_encryption.key_for(&col.name) {
                    let encrypted = crate::segment::field_encryption::encrypt_block(
                        &final_data,
                        enc_key.key_bytes(),
                        crate::segment::field_encryption::BlockContext {
                            column: &col.name,
                            segment_created_at,
                        },
                    )?;
                    (encrypted, true)
                } else {
                    (final_data, false)
                };
            #[cfg(not(feature = "field-encryption"))]
            let block_encrypted = false;

            let offset = *pos;
            writer.write_all(&final_data)?;
            *crc = crc32c::crc32c_append(*crc, &final_data);
            *pos += final_data.len() as u64;

            let block_crc = crc32c::crc32c(&final_data);

            // Suppress statistics for encrypted columns to prevent
            // information leakage through zone-map predicates.
            let final_stats = if block_encrypted {
                ColumnStats::empty()
            } else {
                stats
            };

            merge_stats(&mut column_metas[col_idx].stats, &final_stats);

            // D-NULL: the validity bitmap follows the block payload. It is
            // stored uncompressed and unencrypted — it reveals only which
            // rows have values, never what they are — so the reader can turn
            // it straight into an Arrow NullBuffer.
            let (validity_offset, validity_length) =
                write_validity(writer, validity.as_deref(), pos, crc)?;

            rg_blocks.push(ColumnBlockMeta {
                column_index: col_idx as u16,
                encoding: block.encoding,
                compressed,
                offset,
                length: u32::try_from(final_data.len()).map_err(|_| SegmentError::CorruptFile {
                    detail: format!("block length {} exceeds u32::MAX", final_data.len()),
                })?,
                value_count: u32::try_from(chunk_size).map_err(|_| SegmentError::CorruptFile {
                    detail: format!("value_count {chunk_size} exceeds u32::MAX"),
                })?,
                block_crc,
                encrypted: block_encrypted,
                validity_offset,
                validity_length,
                stats: final_stats,
            });
        }

        row_group_blocks.push(rg_blocks);

        // Build per-row-group bloom filters for tag columns.
        for (col_idx, col) in schema.iter().enumerate() {
            if col.role == roles::TAG && !column_metas[col_idx].encrypted {
                let mut distinct: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                for &idx in chunk_indices {
                    let val = points[idx].series_key().tag(&col.name).unwrap_or("");
                    if !val.is_empty() {
                        distinct.insert(val);
                    }
                }
                let bloom = if distinct.is_empty() {
                    Vec::new()
                } else {
                    let vals: Vec<&str> = distinct.into_iter().collect();
                    crate::segment::bloom::bloom_filter_build(&vals, config.bloom_fpr)
                };
                column_metas[col_idx]
                    .row_group_blooms
                    .get_or_insert_with(Vec::new)
                    .push(bloom);
            }
        }

        // Train dictionary after the first row group.
        if use_dict && rg_index == 0 && !dict_samples.is_empty() {
            let sample_refs: Vec<&[u8]> =
                dict_samples.iter().map(std::vec::Vec::as_slice).collect();
            if let Ok(Some(dict)) = train_zstd_dictionary(&sample_refs) {
                tracing::debug!(
                    dict_size = dict.len(),
                    samples = dict_samples.len(),
                    "trained Zstd dictionary from first row group"
                );
                trained_dict = Some(dict);
            }
            dict_samples.clear();
        }
        rg_index += 1;
    }

    // Build bloom filters for tag columns.
    // Skip bloom filter for encrypted columns to prevent information leakage.
    for (col_idx, col) in schema.iter().enumerate() {
        if col.role == roles::TAG && !column_metas[col_idx].encrypted {
            let mut distinct: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for &idx in sorted_indices {
                let val = points[idx].series_key().tag(&col.name).unwrap_or("");
                if !val.is_empty() {
                    distinct.insert(val);
                }
            }
            if !distinct.is_empty() {
                let values: Vec<&str> = distinct.into_iter().collect();
                column_metas[col_idx].bloom_filter = Some(
                    crate::segment::bloom::bloom_filter_build(&values, config.bloom_fpr),
                );
            }
        }
    }

    Ok((
        column_metas,
        row_group_blocks,
        total_uncompressed,
        trained_dict,
    ))
}

// ── Schema Discovery ───────────────────────────────────────────────────

/// Discovered column definition from point data.
#[derive(Debug, Clone)]
struct ColumnDef {
    name: String,
    data_type: u8,
    role: u8,
    default_encoding: u8,
}

/// Discover the column schema by scanning all points.
///
/// Tag columns are ordered by ascending cardinality (fewest distinct
/// values first) so that the physical column layout is optimal for
/// run-length and dictionary encoding.
fn discover_schema(points: &[Point]) -> Vec<ColumnDef> {
    let mut columns: Vec<ColumnDef> = Vec::new();

    // Timestamp column is always first
    columns.push(ColumnDef {
        name: "timestamp".to_string(),
        data_type: data_types::TIMESTAMP,
        role: roles::TIMESTAMP,
        default_encoding: chronix_encoding::EncodingType::DeltaOfDelta.tag(),
    });

    // Discover tag columns and compute cardinality
    let mut tag_names = BTreeSet::new();
    for p in points {
        for key in p.series_key().tag_keys() {
            tag_names.insert(key.to_string());
        }
    }

    // Sort tags by cardinality (lowest first) for optimal compression
    let mut tag_with_card: Vec<(String, usize)> = tag_names
        .into_iter()
        .map(|name: String| {
            let distinct: std::collections::HashSet<&str> = points
                .iter()
                .filter_map(|p| p.series_key().tag(&name))
                .collect();
            (name, distinct.len())
        })
        .collect();
    tag_with_card.sort_by_key(|(_, card)| *card);

    for (name, _) in tag_with_card {
        columns.push(ColumnDef {
            name,
            data_type: data_types::STRING,
            role: roles::TAG,
            default_encoding: chronix_encoding::EncodingType::Dictionary.tag(),
        });
    }

    // Discover field columns
    let mut field_defs: BTreeMap<String, u8> = BTreeMap::new();
    for p in points {
        for (key, val) in p.fields() {
            field_defs
                .entry(key.to_string())
                .or_insert_with(|| match val {
                    FieldValue::F64(_) => data_types::F64,
                    FieldValue::I64(_) => data_types::I64,
                    FieldValue::U64(_) => data_types::U64,
                    FieldValue::Bool(_) => data_types::BOOL,
                    FieldValue::String(_) => data_types::STRING,
                });
        }
    }
    for (name, dt) in field_defs {
        let enc = match dt {
            data_types::F64 => chronix_encoding::EncodingType::Chimp.tag(),
            data_types::I64 => chronix_encoding::EncodingType::IntegerI64.tag(),
            data_types::U64 => chronix_encoding::EncodingType::IntegerU64.tag(),
            data_types::BOOL => chronix_encoding::EncodingType::Bitmap.tag(),
            _ => chronix_encoding::EncodingType::Dictionary.tag(),
        };
        columns.push(ColumnDef {
            name,
            data_type: dt,
            role: roles::FIELD,
            default_encoding: enc,
        });
    }

    columns
}

/// Append a block's validity bitmap to the segment file.
///
/// Returns the `(offset, length)` pair recorded in
/// [`ColumnBlockMeta::validity_offset`] / `validity_length`. A `None` bitmap
/// (a block with no nulls) writes nothing and yields `(0, 0)`.
fn write_validity<W: Write>(
    writer: &mut W,
    validity: Option<&[u8]>,
    pos: &mut u64,
    crc: &mut u32,
) -> Result<(u64, u32)> {
    let Some(bytes) = validity else {
        return Ok((0, 0));
    };
    let offset = *pos;
    writer.write_all(bytes)?;
    *crc = crc32c::crc32c_append(*crc, bytes);
    *pos += bytes.len() as u64;
    let length = u32::try_from(bytes.len()).map_err(|_| SegmentError::CorruptFile {
        detail: format!("validity bitmap length {} exceeds u32::MAX", bytes.len()),
    })?;
    Ok((offset, length))
}

/// An encoded column block together with its statistics and, when the block
/// contains nulls, its validity bitmap (`.csx` v2, D-NULL).
struct EncodedColumn {
    block: EncodedBlock,
    stats: ColumnStats,
    /// `None` when every row in the block holds a real value.
    validity: Option<Vec<u8>>,
}

impl EncodedColumn {
    fn new(block: EncodedBlock, stats: ColumnStats, validity: ValidityBuilder) -> Self {
        Self {
            block,
            stats,
            validity: validity.finish(),
        }
    }

    /// Wrap a column that cannot contain nulls (timestamps, tags).
    fn dense((block, stats): (EncodedBlock, ColumnStats)) -> Self {
        Self {
            block,
            stats,
            validity: None,
        }
    }
}

/// Encode a single column for a chunk of rows.
fn encode_column(
    col: &ColumnDef,
    indices: &[usize],
    points: &[Point],
    float_encoding: FloatEncoding,
) -> Result<EncodedColumn> {
    let mut stats = ColumnStats::empty();
    // D-NULL: record which rows actually carry a value so the reader can
    // distinguish an absent field from a stored zero.
    let mut validity = ValidityBuilder::with_capacity(indices.len());

    match (col.role, col.data_type) {
        // Timestamps are never null; tags are materialised as "" by design
        // (an absent tag is not part of the series key).
        (roles::TIMESTAMP, _) => encode_timestamp_column(indices, points).map(EncodedColumn::dense),
        (roles::TAG, _) => encode_tag_column(col, indices, points).map(EncodedColumn::dense),
        (_, data_types::F64) => {
            let mut values = Vec::with_capacity(indices.len());
            for &idx in indices {
                if let Some(FieldValue::F64(v)) = points[idx].field(&col.name) {
                    values.push(*v);
                    stats.update_f64(*v);
                    validity.push(true);
                } else {
                    values.push(0.0);
                    stats.record_null();
                    validity.push(false);
                }
            }
            let block = ColumnEncoder::encode_f64(&values, float_encoding)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        (_, data_types::I64) => {
            let mut values = Vec::with_capacity(indices.len());
            for &idx in indices {
                if let Some(FieldValue::I64(v)) = points[idx].field(&col.name) {
                    values.push(*v);
                    stats.update_i64(*v);
                    validity.push(true);
                } else {
                    values.push(0);
                    stats.record_null();
                    validity.push(false);
                }
            }
            let block = ColumnEncoder::encode_i64(&values)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        (_, data_types::U64) => {
            let mut values = Vec::with_capacity(indices.len());
            for &idx in indices {
                if let Some(FieldValue::U64(v)) = points[idx].field(&col.name) {
                    values.push(*v);
                    stats.update_u64(*v);
                    validity.push(true);
                } else {
                    values.push(0);
                    stats.record_null();
                    validity.push(false);
                }
            }
            let block = ColumnEncoder::encode_u64(&values)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        (_, data_types::BOOL) => {
            let mut values = Vec::with_capacity(indices.len());
            for &idx in indices {
                if let Some(FieldValue::Bool(v)) = points[idx].field(&col.name) {
                    values.push(*v);
                    stats.update_bool(*v);
                    validity.push(true);
                } else {
                    values.push(false);
                    stats.record_null();
                    validity.push(false);
                }
            }
            let block = ColumnEncoder::encode_bool(&values)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        (_, data_types::STRING) => {
            // Collect owned strings, then borrow as &str for encoding
            let mut owned: Vec<String> = Vec::with_capacity(indices.len());
            for &idx in indices {
                if let Some(FieldValue::String(v)) = points[idx].field(&col.name) {
                    owned.push(v.clone());
                    validity.push(true);
                } else {
                    owned.push(String::new());
                    stats.record_null();
                    validity.push(false);
                }
            }
            let values: Vec<&str> = owned.iter().map(String::as_str).collect();
            let distinct: std::collections::HashSet<&str> = values.iter().copied().collect();
            stats.distinct_count = distinct.len() as u32;
            // value_count should reflect non-null entries (null_count already tracked).
            stats.value_count = (values.len() as u64).saturating_sub(stats.null_count);
            let block = ColumnEncoder::encode_string(&values)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        _ => Err(SegmentError::CorruptFile {
            detail: format!("unknown column type: {}", col.data_type),
        }),
    }
}

/// Compute min/max timestamps and unique series count from sorted indices.
///
/// Since the indices are already sorted by `(canonical_form, timestamp)`,
/// we count distinct series by checking when the canonical form changes.
fn compute_global_stats(
    points: &[Point],
    sorted_indices: &[usize],
) -> (i64, i64, Vec<chronix_core::SeriesKey>) {
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut series_keys: Vec<chronix_core::SeriesKey> = Vec::new();

    for &idx in sorted_indices {
        let p = &points[idx];
        let ts = p.timestamp();
        min_ts = min_ts.min(ts);
        max_ts = max_ts.max(ts);
        // Series-major order: a new series starts wherever the canonical form
        // changes. Comparing the form rather than the hash means a hash
        // collision between adjacent series is counted as two, not one.
        let is_new = series_keys
            .last()
            .is_none_or(|prev| prev.canonical_form() != p.series_key().canonical_form());
        if is_new {
            series_keys.push(p.series_key().clone());
        }
    }
    (min_ts, max_ts, series_keys)
}

/// Encode a timestamp column from a set of points at the given indices.
fn encode_timestamp_column(
    indices: &[usize],
    points: &[Point],
) -> Result<(EncodedBlock, ColumnStats)> {
    let mut stats = ColumnStats::empty();
    let mut timestamps = Vec::with_capacity(indices.len());
    for &idx in indices {
        let ts = points[idx].timestamp();
        timestamps.push(ts);
        stats.update_i64(ts);
    }
    let block = ColumnEncoder::encode_timestamps(&timestamps)?;
    Ok((block, stats))
}

/// Encode a tag column by looking up each tag value from the series key.
fn encode_tag_column(
    col: &ColumnDef,
    indices: &[usize],
    points: &[Point],
) -> Result<(EncodedBlock, ColumnStats)> {
    let mut stats = ColumnStats::empty();
    let mut values = Vec::with_capacity(indices.len());
    for &idx in indices {
        let val = points[idx].series_key().tag(&col.name).unwrap_or("");
        values.push(val);
    }
    let distinct: std::collections::HashSet<&str> = values.iter().copied().collect();
    stats.distinct_count = distinct.len() as u32;
    stats.value_count = values.len() as u64;
    let block = ColumnEncoder::encode_string(&values)?;
    Ok((block, stats))
}

/// Merge source stats into destination.
///
/// **Note:** `distinct_count` uses `max()` when merging across row groups —
/// a lower bound on the true segment cardinality, and a tighter estimate than
/// summing. The segment-level per-column `distinct_count` computed over all
/// rows is the exact figure; this merge only applies to the row-group rollup.
fn merge_stats(dest: &mut ColumnStats, src: &ColumnStats) {
    dest.min_value = dest.min_value.min(src.min_value);
    dest.max_value = dest.max_value.max(src.max_value);
    // Also merge u64 min/max — previously these were silently
    // dropped, breaking predicate pushdown for u64 columns.
    dest.min_value_u64 = dest.min_value_u64.min(src.min_value_u64);
    dest.max_value_u64 = dest.max_value_u64.max(src.max_value_u64);
    dest.null_count += src.null_count;
    dest.value_count += src.value_count;
    dest.sum += src.sum;
    dest.sum_i128 += src.sum_i128;
    // Use max() instead of saturating_add for a tighter bound.
    // Row groups may share values, so summing grossly over-counts.
    dest.distinct_count = dest.distinct_count.max(src.distinct_count);
}

// ── Batch-based encoding (zero-copy compaction path) ───────────────────

/// Discover column schema from an Arrow `RecordBatch`.
///
/// Uses `tag_columns` to distinguish tags from fields (since Arrow
/// schema alone doesn't carry Chronix role information).
fn discover_schema_from_batch(
    batch: &RecordBatch,
    tag_columns: &std::collections::HashSet<&str>,
) -> Result<Vec<ColumnDef>> {
    let mut columns = Vec::new();

    // Timestamp first
    columns.push(ColumnDef {
        name: "timestamp".to_string(),
        data_type: data_types::TIMESTAMP,
        role: roles::TIMESTAMP,
        default_encoding: chronix_encoding::EncodingType::DeltaOfDelta.tag(),
    });

    // Tags (sorted for consistency)
    let mut tags: Vec<&str> = tag_columns.iter().copied().collect();
    tags.sort_unstable();
    for tag in tags {
        if batch.column_by_name(tag).is_some() {
            columns.push(ColumnDef {
                name: tag.to_string(),
                data_type: data_types::STRING,
                role: roles::TAG,
                default_encoding: chronix_encoding::EncodingType::Dictionary.tag(),
            });
        }
    }

    // Fields (everything else except timestamp and tags)
    for field in batch.schema().fields() {
        let name = field.name().as_str();
        if name == "timestamp" || tag_columns.contains(name) {
            continue;
        }
        let (dt, enc) = match field.data_type() {
            DataType::Float64 => (data_types::F64, chronix_encoding::EncodingType::Chimp.tag()),
            DataType::Int64 => (
                data_types::I64,
                chronix_encoding::EncodingType::IntegerI64.tag(),
            ),
            DataType::UInt64 => (
                data_types::U64,
                chronix_encoding::EncodingType::IntegerU64.tag(),
            ),
            DataType::Boolean => (
                data_types::BOOL,
                chronix_encoding::EncodingType::Bitmap.tag(),
            ),
            DataType::Utf8 | DataType::LargeUtf8 => (
                data_types::STRING,
                chronix_encoding::EncodingType::Dictionary.tag(),
            ),
            other => {
                return Err(SegmentError::CorruptFile {
                    detail: format!(
                        "unsupported Arrow data type {:?} for field '{}'",
                        other, name
                    ),
                });
            }
        };
        columns.push(ColumnDef {
            name: name.to_string(),
            data_type: dt,
            role: roles::FIELD,
            default_encoding: enc,
        });
    }

    Ok(columns)
}

/// Count distinct series in a RecordBatch by canonical series key form.
///
/// Uses the full canonical form (not just hash) to avoid undercounting
/// in the extremely rare case of FNV hash collisions.
fn series_keys_from_batch(
    batch: &RecordBatch,
    measurement: &str,
    tag_columns: &[String],
) -> Vec<chronix_core::SeriesKey> {
    let mut seen = std::collections::HashSet::new();
    let mut keys = Vec::new();
    let num_rows = batch.num_rows();

    let tag_arrays: Vec<Option<&StringArray>> = tag_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        })
        .collect();

    for row in 0..num_rows {
        let mut tags = BTreeMap::new();
        for (i, tag_name) in tag_columns.iter().enumerate() {
            if let Some(arr) = tag_arrays[i] {
                if !arr.is_null(row) {
                    tags.insert(tag_name.clone(), arr.value(row).to_string());
                }
            }
        }
        if let Ok(key) = chronix_core::SeriesKey::new(measurement, tags) {
            if seen.insert(key.canonical_form().to_string()) {
                keys.push(key);
            }
        }
    }
    keys
}

/// Encode all row groups from a pre-sorted `RecordBatch`, streaming each
/// encoded column block directly to `writer` instead of buffering in memory.
///
/// This is the batch-based counterpart of [`encode_row_groups_streaming`].
fn encode_row_groups_from_batch_streaming<W: Write>(
    schema: &[ColumnDef],
    batch: &RecordBatch,
    config: &SegmentWriterConfig,
    segment_created_at: i64,
    writer: &mut W,
    crc: &mut u32,
    pos: &mut u64,
) -> Result<RowGroupWriteOutput> {
    let total_rows = batch.num_rows();

    let mut column_metas: Vec<ColumnMeta> = schema
        .iter()
        .map(|col| {
            #[cfg(feature = "field-encryption")]
            let (encrypted, key_id) = if let Some(k) = config.field_encryption.key_for(&col.name) {
                (true, Some(k.key_id.clone()))
            } else {
                (false, None)
            };
            #[cfg(not(feature = "field-encryption"))]
            let (encrypted, key_id) = (false, None);

            ColumnMeta {
                name: col.name.clone(),
                data_type: col.data_type,
                role: col.role,
                default_encoding: col.default_encoding,
                stats: ColumnStats::empty(),
                bloom_filter: None,
                encrypted,
                key_id,
                row_group_blooms: None,
            }
        })
        .collect();

    let mut row_group_blocks: Vec<Vec<ColumnBlockMeta>> = Vec::new();
    let mut total_uncompressed: u64 = 0;

    // Zstd dictionary training state.
    let use_dict =
        config.zstd_dict_training && matches!(config.compression_codec, CompressionCodec::Zstd);
    let mut dict_samples: Vec<Vec<u8>> = Vec::new();
    let mut trained_dict: Option<Vec<u8>> = None;
    let mut rg_index: usize = 0;

    #[allow(clippy::explicit_counter_loop)]
    // rg_index also names files/metadata, not just iterations
    for chunk_start in (0..total_rows).step_by(config.row_group_size) {
        let chunk_end = (chunk_start + config.row_group_size).min(total_rows);
        let chunk_size = chunk_end - chunk_start;

        let mut rg_blocks = Vec::with_capacity(schema.len());

        for (col_idx, col) in schema.iter().enumerate() {
            let EncodedColumn {
                block,
                stats,
                validity,
            } = encode_column_from_batch(
                col,
                batch,
                chunk_start,
                chunk_end,
                config.float_encoding,
            )?;

            let raw_encoded = block.to_bytes();
            let raw_size = chunk_size * 8;
            total_uncompressed += raw_encoded.len() as u64;

            // Collect training samples from the first row group.
            if use_dict && rg_index == 0 {
                dict_samples.push(raw_encoded.clone());
            }

            let codec = config
                .column_codec_overrides
                .get(&col.name)
                .copied()
                .unwrap_or(config.compression_codec);
            let (final_data, compressed) =
                if config.compress && !should_skip_compression(raw_encoded.len(), raw_size) {
                    // Use dictionary compression when available.
                    if let Some(ref dict) = trained_dict {
                        if matches!(codec, CompressionCodec::Zstd) {
                            let compressed_data =
                                compress_block_zstd_dict(&raw_encoded, config.zstd_level, dict)?;
                            if compressed_data.len() < raw_encoded.len() {
                                (compressed_data, true)
                            } else {
                                (raw_encoded, false)
                            }
                        } else {
                            let compressed_data = compress_block_with_codec(&raw_encoded, codec)?;
                            if compressed_data.len() < raw_encoded.len() {
                                (compressed_data, true)
                            } else {
                                (raw_encoded, false)
                            }
                        }
                    } else {
                        let compressed_data = compress_block_with_codec(&raw_encoded, codec)?;
                        if compressed_data.len() < raw_encoded.len() {
                            (compressed_data, true)
                        } else {
                            (raw_encoded, false)
                        }
                    }
                } else {
                    (raw_encoded, false)
                };

            // Field-level encryption: encrypt after encode+compress.
            #[cfg(feature = "field-encryption")]
            let (final_data, block_encrypted) =
                if let Some(enc_key) = config.field_encryption.key_for(&col.name) {
                    let encrypted = crate::segment::field_encryption::encrypt_block(
                        &final_data,
                        enc_key.key_bytes(),
                        crate::segment::field_encryption::BlockContext {
                            column: &col.name,
                            segment_created_at,
                        },
                    )?;
                    (encrypted, true)
                } else {
                    (final_data, false)
                };
            #[cfg(not(feature = "field-encryption"))]
            let block_encrypted = false;

            let offset = *pos;
            writer.write_all(&final_data)?;
            *crc = crc32c::crc32c_append(*crc, &final_data);
            *pos += final_data.len() as u64;

            let block_crc = crc32c::crc32c(&final_data);

            // Suppress statistics for encrypted columns to prevent
            // information leakage through zone-map predicates.
            let final_stats = if block_encrypted {
                ColumnStats::empty()
            } else {
                stats
            };

            merge_stats(&mut column_metas[col_idx].stats, &final_stats);

            // D-NULL: validity bitmap follows the block payload.
            let (validity_offset, validity_length) =
                write_validity(writer, validity.as_deref(), pos, crc)?;

            rg_blocks.push(ColumnBlockMeta {
                column_index: col_idx as u16,
                encoding: block.encoding,
                compressed,
                offset,
                length: u32::try_from(final_data.len()).map_err(|_| SegmentError::CorruptFile {
                    detail: format!("block length {} exceeds u32::MAX", final_data.len()),
                })?,
                value_count: u32::try_from(chunk_size).map_err(|_| SegmentError::CorruptFile {
                    detail: format!("value_count {chunk_size} exceeds u32::MAX"),
                })?,
                block_crc,
                encrypted: block_encrypted,
                validity_offset,
                validity_length,
                stats: final_stats,
            });
        }

        row_group_blocks.push(rg_blocks);

        // Build per-row-group bloom filters for tag columns (batch path).
        for (col_idx, col) in schema.iter().enumerate() {
            if col.role == roles::TAG && !column_metas[col_idx].encrypted {
                if let Some(arr) = batch
                    .column_by_name(&col.name)
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                {
                    let mut distinct: std::collections::HashSet<&str> =
                        std::collections::HashSet::new();
                    for i in chunk_start..chunk_end {
                        if !arr.is_null(i) {
                            let val = arr.value(i);
                            if !val.is_empty() {
                                distinct.insert(val);
                            }
                        }
                    }
                    let bloom = if distinct.is_empty() {
                        Vec::new()
                    } else {
                        let vals: Vec<&str> = distinct.into_iter().collect();
                        crate::segment::bloom::bloom_filter_build(&vals, config.bloom_fpr)
                    };
                    column_metas[col_idx]
                        .row_group_blooms
                        .get_or_insert_with(Vec::new)
                        .push(bloom);
                }
            }
        }

        // Train dictionary after the first row group.
        if use_dict && rg_index == 0 && !dict_samples.is_empty() {
            let sample_refs: Vec<&[u8]> =
                dict_samples.iter().map(std::vec::Vec::as_slice).collect();
            if let Ok(Some(dict)) = train_zstd_dictionary(&sample_refs) {
                tracing::debug!(
                    dict_size = dict.len(),
                    samples = dict_samples.len(),
                    "trained Zstd dictionary from first row group (batch path)"
                );
                trained_dict = Some(dict);
            }
            dict_samples.clear();
        }
        rg_index += 1;
    }

    // Build bloom filters for tag columns from Arrow arrays.
    // Skip bloom filter for encrypted columns to prevent information leakage.
    for (col_idx, col) in schema.iter().enumerate() {
        if col.role == roles::TAG && !column_metas[col_idx].encrypted {
            if let Some(arr) = batch
                .column_by_name(&col.name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            {
                let mut distinct: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        let val = arr.value(i);
                        if !val.is_empty() {
                            distinct.insert(val);
                        }
                    }
                }
                if !distinct.is_empty() {
                    let values: Vec<&str> = distinct.into_iter().collect();
                    column_metas[col_idx].bloom_filter = Some(
                        crate::segment::bloom::bloom_filter_build(&values, config.bloom_fpr),
                    );
                }
            }
        }
    }

    Ok((
        column_metas,
        row_group_blocks,
        total_uncompressed,
        trained_dict,
    ))
}

/// Encode a single column from a RecordBatch slice [start..end).
fn encode_column_from_batch(
    col: &ColumnDef,
    batch: &RecordBatch,
    start: usize,
    end: usize,
    float_encoding: FloatEncoding,
) -> Result<EncodedColumn> {
    let mut stats = ColumnStats::empty();
    // D-NULL: track per-row presence alongside the encoded values.
    let mut validity = ValidityBuilder::with_capacity(end.saturating_sub(start));

    match (col.role, col.data_type) {
        (roles::TIMESTAMP, _) => {
            let arr = batch
                .column_by_name("timestamp")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: "missing timestamp".into(),
                })?;
            let mut timestamps = Vec::with_capacity(end - start);
            for i in start..end {
                let t = arr.value(i);
                timestamps.push(t);
                stats.update_i64(t);
            }
            let block = ColumnEncoder::encode_timestamps(&timestamps)?;
            // Timestamps are never null.
            Ok(EncodedColumn::dense((block, stats)))
        }
        (roles::TAG, _) | (_, data_types::STRING) => {
            let arr = batch
                .column_by_name(&col.name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: format!("missing string column '{}'", col.name),
                })?;
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                if arr.is_null(i) {
                    values.push("");
                    stats.record_null();
                    validity.push(false);
                } else {
                    values.push(arr.value(i));
                    validity.push(true);
                }
            }
            let distinct: std::collections::HashSet<&str> = values.iter().copied().collect();
            stats.distinct_count = distinct.len() as u32;
            stats.value_count = (values.len() as u64).saturating_sub(stats.null_count);
            let block = ColumnEncoder::encode_string(&values)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        (_, data_types::F64) => {
            let arr = batch
                .column_by_name(&col.name)
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: format!("missing f64 column '{}'", col.name),
                })?;
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                if arr.is_null(i) {
                    values.push(0.0);
                    stats.record_null();
                    validity.push(false);
                } else {
                    let v = arr.value(i);
                    values.push(v);
                    stats.update_f64(v);
                    validity.push(true);
                }
            }
            let block = ColumnEncoder::encode_f64(&values, float_encoding)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        (_, data_types::I64) => {
            let arr = batch
                .column_by_name(&col.name)
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: format!("missing i64 column '{}'", col.name),
                })?;
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                if arr.is_null(i) {
                    values.push(0);
                    stats.record_null();
                    validity.push(false);
                } else {
                    let v = arr.value(i);
                    values.push(v);
                    stats.update_i64(v);
                    validity.push(true);
                }
            }
            let block = ColumnEncoder::encode_i64(&values)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        (_, data_types::U64) => {
            let arr = batch
                .column_by_name(&col.name)
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: format!("missing u64 column '{}'", col.name),
                })?;
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                if arr.is_null(i) {
                    values.push(0);
                    stats.record_null();
                    validity.push(false);
                } else {
                    let v = arr.value(i);
                    values.push(v);
                    stats.update_u64(v);
                    validity.push(true);
                }
            }
            let block = ColumnEncoder::encode_u64(&values)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        (_, data_types::BOOL) => {
            let arr = batch
                .column_by_name(&col.name)
                .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                .ok_or_else(|| SegmentError::CorruptFile {
                    detail: format!("missing bool column '{}'", col.name),
                })?;
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                if arr.is_null(i) {
                    values.push(false);
                    stats.record_null();
                    validity.push(false);
                } else {
                    let v = arr.value(i);
                    values.push(v);
                    stats.update_bool(v);
                    validity.push(true);
                }
            }
            let block = ColumnEncoder::encode_bool(&values)?;
            Ok(EncodedColumn::new(block, stats, validity))
        }
        _ => Err(SegmentError::CorruptFile {
            detail: format!("unknown column type: {}", col.data_type),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::reader::SegmentReader;
    use chronix_core::config::{CompressionCodec, FloatEncoding};
    use chronix_core::types::{FieldValue, Point, SeriesKey};
    use std::collections::{BTreeMap, HashMap};

    fn make_point(host: &str, cpu: f64, ts: i64) -> Point {
        let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
        let key = SeriesKey::new("cpu_usage", tags).unwrap();
        let fields = BTreeMap::from([("cpu".to_string(), FieldValue::F64(cpu))]);
        Point::new(key, fields, ts).unwrap()
    }

    #[test]
    fn per_column_codec_mixed_lz4_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed_codec.csx");

        let points: Vec<Point> = (0..200)
            .map(|i| make_point("server-1", 50.0 + i as f64, 1_000_000 + i * 10_000))
            .collect();

        // Override the "cpu" column to use Zstd while default is Lz4
        let mut overrides = HashMap::new();
        overrides.insert("cpu".to_string(), CompressionCodec::Zstd);

        let config = SegmentWriterConfig {
            row_group_size: 100,
            compress: true,
            float_encoding: FloatEncoding::Gorilla,
            compression_codec: CompressionCodec::Lz4,
            zstd_level: 3,
            column_codec_overrides: overrides,
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        let meta = writer.finalize().unwrap();

        assert_eq!(meta.row_count, 200);

        // Read back and verify data integrity
        let reader = SegmentReader::open(&path).unwrap();
        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), 200);

        // Verify cpu values
        let cpu_col = batch
            .column_by_name("cpu")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        for i in 0..200 {
            assert!(
                (cpu_col.value(i) - (50.0 + i as f64)).abs() < 1e-9,
                "cpu mismatch at row {i}"
            );
        }
    }

    #[test]
    fn p09_zstd_dict_training_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dict_trained.csx");

        // Generate enough data for 3+ row groups so dictionary training
        // kicks in on the first and is used on subsequent groups.
        let points: Vec<Point> = (0..600)
            .map(|i| {
                let host = format!("host-{}", i % 5);
                make_point(&host, 10.0 + (i as f64) * 0.1, 1_000_000 + i * 10_000)
            })
            .collect();

        let config = SegmentWriterConfig {
            row_group_size: 100,
            compress: true,
            compression_codec: CompressionCodec::Zstd,
            zstd_level: 3,
            zstd_dict_training: true,
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(&path, config).unwrap();
        writer.write_rows(&points).unwrap();
        let meta = writer.finalize().unwrap();

        assert_eq!(meta.row_count, 600);
        assert!(meta.row_group_count >= 3);

        // Verify the dictionary was trained and embedded
        let reader = SegmentReader::open(&path).unwrap();
        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), 600);

        // Verify data integrity
        let cpu_col = batch
            .column_by_name("cpu")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(cpu_col.len(), 600);
    }

    /// Segment metadata must stay proportional to the data, not fixed.
    ///
    /// Every column used to carry a full-precision HyperLogLog sketch
    /// (16 KiB) plus an equi-depth histogram, unconditionally — roughly 49 KiB
    /// of fixed overhead per segment that nothing ever read: the DataFusion
    /// statistics provider works from the catalog's exact `distinct_count`.
    /// On an hourly-sharded gateway that dwarfed the data and burned flash for
    /// nothing, so the sketches were removed. This pins the result.
    #[test]
    fn segment_metadata_overhead_stays_proportional_to_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overhead.csx");

        let points: Vec<Point> = (0..500)
            .map(|i| {
                let key = SeriesKey::new(
                    "m".to_string(),
                    std::collections::BTreeMap::from([(
                        "host".to_string(),
                        format!("h{}", i % 10),
                    )]),
                )
                .unwrap();
                Point::new(
                    key,
                    std::collections::BTreeMap::from([(
                        "cpu".to_string(),
                        FieldValue::F64(f64::from(i % 100) / 10.0),
                    )]),
                    i64::from(i) * 1_000_000,
                )
                .unwrap()
            })
            .collect();

        let mut writer = SegmentWriter::new(&path, SegmentWriterConfig::default()).unwrap();
        writer.write_rows(&points).unwrap();
        writer.finalize().unwrap();

        let file_size = std::fs::metadata(&path).unwrap().len();
        let block_bytes: u64 = {
            let reader = SegmentReader::open(&path).unwrap();
            (0..reader.row_group_count())
                .filter_map(|rg| reader.row_group_blocks(rg))
                .flatten()
                .map(|b| u64::from(b.length))
                .sum()
        };
        let overhead = file_size - block_bytes;

        assert!(
            overhead < 8 * 1024,
            "segment carries {overhead} bytes of non-block overhead for {block_bytes} bytes of \
             data — fixed-size metadata has crept back in"
        );
    }
}
