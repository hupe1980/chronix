//! Warm storage tier — moves aging data to a separate mount point
//! and re-compresses segments from LZ4 to Zstd for higher compression
//! ratios on cold data.
//!
//! The [`WarmTierConfig`] defines the threshold (`warm_after` days) and
//! the target path.  [`shards_to_warm`] identifies shards eligible for
//! migration.  [`migrate_segment`] reads the source segment, re-encodes
//! it with Zstd compression, and writes the result to the warm path.
//! The original segment file remains until the catalog is updated.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow::array::Array;
use chronix_core::{CompressionCodec, FieldValue, Point, SeriesKey, ShardId};
use chronix_engine::segment::metadata::roles;
use chronix_engine::segment::reader::SegmentReader;
use chronix_engine::segment::writer::{SegmentWriter, SegmentWriterConfig};
use tracing::warn;

/// Warm-tier migration error.
#[derive(Debug, thiserror::Error)]
pub enum WarmTierError {
    /// I/O error during migration.
    #[error("warm tier I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Segment read/write error.
    #[error("segment error: {0}")]
    Segment(#[from] chronix_engine::segment::error::SegmentError),
    /// Data conversion error.
    #[error("data error: {0}")]
    Data(String),
}

/// Default warm tier age threshold: 7 days (in nanoseconds).
pub const DEFAULT_WARM_AFTER_NS: i64 = 7 * 24 * 3600 * 1_000_000_000;

/// Warm tier configuration.
#[derive(Debug, Clone)]
pub struct WarmTierConfig {
    /// Enable the warm tier.
    pub enabled: bool,
    /// Age threshold in nanoseconds — shards older than this are moved.
    pub warm_after_ns: i64,
    /// Target directory for warm data.
    pub warm_path: PathBuf,
    /// Zstd compression level for warm tier (default: 9 for maximum ratio).
    pub zstd_level: i32,
}

impl Default for WarmTierConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            warm_after_ns: DEFAULT_WARM_AFTER_NS,
            warm_path: PathBuf::from("data/warm"),
            zstd_level: 9,
        }
    }
}

/// Result of a warm-tier migration pass.
#[derive(Debug, Clone)]
pub struct WarmTierResult {
    /// Number of shards moved.
    pub shards_moved: usize,
    /// Total segments re-compressed.
    pub segments_recompressed: usize,
    /// Estimated bytes saved by re-compression.
    pub bytes_saved: u64,
}

/// Identify shards eligible for warm-tier migration.
///
/// A shard is eligible if its `max_timestamp` is older than
/// `now_ns - warm_after_ns` and it has not already been moved.
///
/// # Arguments
///
/// * `shard_bounds` — Map of shard ID → (`min_ts`, `max_ts`).
/// * `now_ns` — Current time in nanoseconds.
/// * `warm_after_ns` — Age threshold in nanoseconds.
///
/// # Returns
///
/// List of shard IDs eligible for warm-tier migration.
#[must_use]
pub fn shards_to_warm(
    shard_bounds: &std::collections::BTreeMap<ShardId, (i64, i64)>,
    now_ns: i64,
    warm_after_ns: i64,
) -> Vec<ShardId> {
    let cutoff = now_ns.saturating_sub(warm_after_ns).max(0);
    shard_bounds
        .iter()
        .filter(|&(_, &(_, max_ts))| max_ts < cutoff)
        .map(|(&shard_id, _)| shard_id)
        .collect()
}

/// Re-compress and migrate a single segment file to the warm tier.
///
/// Processes the segment **one row group at a time** (streaming migration)
/// so that only a single row group's data (~64Ki rows) is held in memory
/// at any point.  The re-compressed output is written directly to the warm
/// path using per-row-group `write_rows` calls, avoiding full-segment
/// materialisation.
///
/// The bloom sidecar (`.bloom`) is copied as-is since its content is
/// compression-independent.
///
/// # Arguments
///
/// * `source_path` — Path to the original segment file.
/// * `warm_dir` — Target directory for the warm-tier segment file.
/// * `zstd_level` — Zstd compression level (1–22, 9 recommended for warm).
///
/// # Returns
///
/// A tuple of `(warm_path, new_file_size, original_file_size)`.
///
/// # Errors
///
/// Returns an error if the source cannot be read or re-compression fails.
pub fn migrate_segment(
    source_path: &Path,
    warm_dir: &Path,
    zstd_level: i32,
) -> Result<(PathBuf, u64, u64), WarmTierError> {
    std::fs::create_dir_all(warm_dir)?;

    let file_name = source_path.file_name().ok_or(WarmTierError::Data(
        "source segment has no file name".into(),
    ))?;
    let warm_path = warm_dir.join(file_name);

    let original_size = std::fs::metadata(source_path)?.len();

    // Open segment reader — metadata is read but no row-group data yet.
    let reader = SegmentReader::open(source_path)?;
    let col_meta = reader.column_metadata();
    let num_row_groups = reader.row_group_count();

    // Identify tag columns by role
    let tag_col_names: Vec<&str> = col_meta
        .iter()
        .filter(|cm| cm.role == roles::TAG)
        .map(|cm| cm.name.as_str())
        .collect();

    // Determine measurement name (from column or path).
    let measurement = {
        // Try to read just the first row group to get the measurement name
        if num_row_groups > 0 {
            let first_batch = reader.read_row_group(0)?;
            if let Ok(idx) = first_batch.schema().index_of("measurement") {
                let arr = first_batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>();
                arr.and_then(|a| {
                    if a.len() > 0 {
                        Some(a.value(0).to_string())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| extract_measurement_from_path(source_path))
            } else {
                extract_measurement_from_path(source_path)
            }
        } else {
            extract_measurement_from_path(source_path)
        }
    };

    if num_row_groups == 0 {
        // Empty segment — just copy it
        std::fs::copy(source_path, &warm_path)?;
        let new_size = std::fs::metadata(&warm_path)?.len();
        copy_bloom_sidecar(source_path, &warm_path);
        return Ok((warm_path, new_size, original_size));
    }

    // Prepare warm-tier writer with Zstd compression.
    let config = SegmentWriterConfig {
        compress: true,
        compression_codec: CompressionCodec::Zstd,
        zstd_level,
        ..SegmentWriterConfig::default()
    };
    let mut writer = SegmentWriter::new(&warm_path, config)?;

    // Stream one row group at a time: read → convert → write, then drop.
    let mut total_rows = 0usize;
    for rg_idx in 0..num_row_groups {
        let batch = reader.read_row_group(rg_idx)?;
        if batch.num_rows() == 0 {
            continue;
        }

        let points = record_batch_to_points(&batch, &tag_col_names, &measurement)?;
        total_rows += points.len();

        if !points.is_empty() {
            writer.write_rows(&points)?;
        }
        // `batch` and `points` are dropped here — only one row group in memory.
    }

    if total_rows == 0 {
        // All row groups were empty — just copy the original
        drop(writer);
        std::fs::copy(source_path, &warm_path)?;
        let new_size = std::fs::metadata(&warm_path)?.len();
        copy_bloom_sidecar(source_path, &warm_path);
        return Ok((warm_path, new_size, original_size));
    }

    writer.finalize()?;

    let new_size = std::fs::metadata(&warm_path)?.len();

    // Copy bloom sidecar (compression-independent)
    copy_bloom_sidecar(source_path, &warm_path);

    Ok((warm_path, new_size, original_size))
}

/// Extract a measurement name from a segment file path.
///
/// Convention: segment files are named `<measurement>_<segment_id>.csx`.
fn extract_measurement_from_path(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.rsplit_once('_'))
        .map_or_else(
            || "unknown".to_string(),
            |(measurement, _id)| measurement.to_string(),
        )
}

/// Copy bloom sidecar file if it exists.
fn copy_bloom_sidecar(source_path: &Path, warm_path: &Path) {
    let bloom_src = source_path.with_extension("bloom");
    if bloom_src.exists() {
        let bloom_dst = warm_path.with_extension("bloom");
        if let Err(e) = std::fs::copy(&bloom_src, &bloom_dst) {
            warn!(
                src = %bloom_src.display(),
                dst = %bloom_dst.display(),
                error = %e,
                "failed to copy bloom sidecar to warm tier"
            );
        }
    }
}

/// Convert an Arrow `RecordBatch` back into `Vec<Point>`.
///
/// Extracts tags from columns identified by `tag_col_names` and treats
/// all other non-timestamp columns as fields.
fn record_batch_to_points(
    batch: &arrow::record_batch::RecordBatch,
    tag_col_names: &[&str],
    measurement: &str,
) -> Result<Vec<Point>, WarmTierError> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};

    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut points = Vec::with_capacity(num_rows);

    let ts_idx = schema
        .index_of("timestamp")
        .map_err(|e| WarmTierError::Data(format!("timestamp column: {e}")))?;
    let ts_arr = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(WarmTierError::Data("timestamp column is not Int64".into()))?;

    for row in 0..num_rows {
        let timestamp = ts_arr.value(row);

        // Extract tags
        let mut tags = BTreeMap::new();
        for &tag_name in tag_col_names {
            if let Ok(col_idx) = schema.index_of(tag_name) {
                if let Some(arr) = batch.column(col_idx).as_any().downcast_ref::<StringArray>() {
                    if !arr.is_null(row) {
                        tags.insert(tag_name.to_string(), arr.value(row).to_string());
                    }
                }
            }
        }

        // Extract fields
        let mut fields = BTreeMap::new();
        for (col_idx, field_ref) in schema.fields().iter().enumerate() {
            let name = field_ref.name().as_str();
            if name == "timestamp" || tag_col_names.contains(&name) || name == "measurement" {
                continue;
            }
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            let val = match field_ref.data_type() {
                arrow::datatypes::DataType::Float64 => col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|a| FieldValue::F64(a.value(row))),
                arrow::datatypes::DataType::Int64 => col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| FieldValue::I64(a.value(row))),
                arrow::datatypes::DataType::UInt64 => col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .map(|a| FieldValue::U64(a.value(row))),
                arrow::datatypes::DataType::Boolean => col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(|a| FieldValue::Bool(a.value(row))),
                arrow::datatypes::DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|a| FieldValue::String(a.value(row).to_string())),
                _ => None,
            };
            if let Some(v) = val {
                fields.insert(name.to_string(), v);
            }
        }

        let key = SeriesKey::new(measurement.to_string(), tags)
            .map_err(|e| WarmTierError::Data(format!("SeriesKey error: {e}")))?;
        let point = Point::new(key, fields, timestamp)
            .map_err(|e| WarmTierError::Data(format!("Point error: {e}")))?;
        points.push(point);
    }

    Ok(points)
}

/// Migrate multiple segments for a shard to the warm tier.
///
/// Each segment is read, re-compressed with Zstd, and written to the
/// warm directory. Migration is segment-by-segment (streaming) — only
/// one segment's data is held in memory at a time.
///
/// # Arguments
///
/// * `segment_paths` — Paths to the original segment files.
/// * `warm_dir` — Target directory for warm-tier files.
/// * `zstd_level` — Zstd compression level for the warm copy.
///
/// # Returns
///
/// A `WarmTierResult` summarizing the migration.
pub fn migrate_shard_segments(
    segment_paths: &[PathBuf],
    warm_dir: &Path,
    zstd_level: i32,
) -> WarmTierResult {
    let mut result = WarmTierResult {
        shards_moved: 0,
        segments_recompressed: 0,
        bytes_saved: 0,
    };

    for path in segment_paths {
        match migrate_segment(path, warm_dir, zstd_level) {
            Ok((_warm_path, new_size, original_size)) => {
                result.segments_recompressed += 1;
                if original_size > new_size {
                    result.bytes_saved += original_size - new_size;
                }
            }
            Err(e) => {
                tracing::warn!(
                    source = %path.display(),
                    error = %e,
                    "Failed to migrate segment to warm tier"
                );
            }
        }
    }

    if result.segments_recompressed > 0 {
        result.shards_moved = 1;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn default_config() {
        let config = WarmTierConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.zstd_level, 9);
        assert_eq!(config.warm_after_ns, DEFAULT_WARM_AFTER_NS);
    }

    #[test]
    fn shards_to_warm_identifies_old_shards() {
        let mut bounds = BTreeMap::new();
        let now = 1_000_000_000_000; // 1000s in ns
        let warm_after = 500_000_000_000; // 500s

        // Shard 1: max_ts = 200s → age = 800s > 500s → eligible
        bounds.insert(ShardId(1), (100_000_000_000, 200_000_000_000));
        // Shard 2: max_ts = 600s → age = 400s < 500s → not eligible
        bounds.insert(ShardId(2), (500_000_000_000, 600_000_000_000));
        // Shard 3: max_ts = 400s → age = 600s > 500s → eligible
        bounds.insert(ShardId(3), (300_000_000_000, 400_000_000_000));

        let eligible = shards_to_warm(&bounds, now, warm_after);
        assert_eq!(eligible.len(), 2);
        assert!(eligible.contains(&ShardId(1)));
        assert!(eligible.contains(&ShardId(3)));
    }

    #[test]
    fn no_shards_eligible_when_all_recent() {
        let mut bounds = BTreeMap::new();
        bounds.insert(ShardId(1), (900, 1000));
        let eligible = shards_to_warm(&bounds, 1100, 500);
        assert!(eligible.is_empty());
    }

    #[test]
    fn warm_tier_result_default() {
        let result = WarmTierResult {
            shards_moved: 0,
            segments_recompressed: 0,
            bytes_saved: 0,
        };
        assert_eq!(result.shards_moved, 0);
    }

    #[test]
    fn migrate_segment_recompresses_with_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("cpu_00001.csx");

        // Write a real segment with LZ4 compression
        let config = SegmentWriterConfig {
            compress: true,
            compression_codec: CompressionCodec::Lz4,
            ..SegmentWriterConfig::default()
        };
        let mut writer = SegmentWriter::new(&source_path, config).unwrap();
        for i in 0..500 {
            let point = Point::new(
                SeriesKey::new("cpu", tags! { "host" => "srv-1", "region" => "us-east" }).unwrap(),
                fields! { "usage" => 50.0 + (i as f64) * 0.1 },
                1_700_000_000_000_000_000 + i * 1_000_000_000,
            )
            .unwrap();
            writer.write_rows(&[point]).unwrap();
        }
        writer.finalize().unwrap();

        // Create a bloom sidecar
        let bloom_path = source_path.with_extension("bloom");
        std::fs::write(&bloom_path, b"bloom data").unwrap();

        let original_size = std::fs::metadata(&source_path).unwrap().len();

        let warm_dir = dir.path().join("warm");
        let (warm_path, new_size, reported_original) =
            migrate_segment(&source_path, &warm_dir, 9).unwrap();

        assert!(warm_path.exists());
        assert_eq!(reported_original, original_size);
        // Zstd should produce a different (likely smaller) file than LZ4
        assert!(new_size > 0);

        // Verify the warm copy is a valid segment with correct data
        let reader = SegmentReader::open(&warm_path).unwrap();
        let batch = reader.read_all().unwrap();
        assert_eq!(batch.num_rows(), 500);

        // Bloom sidecar also copied
        let warm_bloom = warm_path.with_extension("bloom");
        assert!(warm_bloom.exists());
        assert_eq!(std::fs::read_to_string(&warm_bloom).unwrap(), "bloom data");
    }

    #[test]
    fn migrate_shard_segments_batch() {
        let dir = tempfile::tempdir().unwrap();
        let paths: Vec<PathBuf> = (0..3)
            .map(|i| {
                let p = dir.path().join(format!("seg_{i}.csx"));
                let config = SegmentWriterConfig::default();
                let mut writer = SegmentWriter::new(&p, config).unwrap();
                let point = Point::new(
                    SeriesKey::new("cpu", tags! { "host" => format!("srv-{i}") }).unwrap(),
                    fields! { "usage" => 42.0 },
                    1_700_000_000_000 + i as i64,
                )
                .unwrap();
                writer.write_rows(&[point]).unwrap();
                writer.finalize().unwrap();
                p
            })
            .collect();

        let warm_dir = dir.path().join("warm");
        let result = migrate_shard_segments(&paths, &warm_dir, 9);

        assert_eq!(result.shards_moved, 1);
        assert_eq!(result.segments_recompressed, 3);

        // All files exist in warm dir
        for i in 0..3 {
            let warm_file = warm_dir.join(format!("seg_{i}.csx"));
            assert!(warm_file.exists());
        }
    }
}
