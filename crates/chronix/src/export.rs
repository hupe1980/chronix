//! Parquet export — converts Chronix segments to Apache Parquet files.
//!
//! This module provides the `export_parquet` function which reads
//! measurement data from the database and writes it as a Parquet file
//! for interoperability with data lakehouse ecosystems (Spark, `DuckDB`,
//! Polars, etc.).

use std::path::Path;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

use crate::error::{DbError, Result};

/// Parquet export configuration.
#[derive(Debug, Clone)]
pub struct ParquetExportConfig {
    /// Parquet compression codec.
    pub compression: ParquetCompression,
    /// Row group size (number of rows per row group).
    pub row_group_size: usize,
    /// Dictionary-encode string columns (tags).
    ///
    /// Tag columns are low-cardinality by construction, so dictionary
    /// encoding is a large win on export size — the dominant cost when a
    /// gateway uploads a window to a fleet backend. Enabled explicitly rather
    /// than relying on the Parquet writer's default so the guarantee is part
    /// of the configuration rather than an implementation detail.
    pub dictionary_tags: bool,
    /// Stop writing before the file exceeds this many bytes.
    ///
    /// `None` writes everything. When set, the writer finishes the current
    /// row group and stops, reporting
    /// [`truncated`](ParquetExportResult::truncated) — an export that would
    /// blow a device's flash budget or an upload limit should come back short
    /// and say so, rather than filling the disk.
    ///
    /// The bound is approximate: it is checked at row-group boundaries, so
    /// the final file may exceed it by up to one row group.
    pub max_bytes: Option<u64>,
}

/// Outcome of a Parquet export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetExportResult {
    /// Rows actually written.
    pub rows_written: u64,
    /// Size of the finished file in bytes.
    pub bytes_written: u64,
    /// Whether [`ParquetExportConfig::max_bytes`] cut the export short.
    pub truncated: bool,
}

/// Supported Parquet compression codecs.
#[derive(Debug, Clone, Copy)]
pub enum ParquetCompression {
    /// No compression.
    None,
    /// Snappy compression (fast, moderate ratio).
    Snappy,
    /// Zstd compression (balanced).
    Zstd,
    /// LZ4 compression (very fast).
    Lz4,
}

impl Default for ParquetExportConfig {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::Zstd,
            row_group_size: 65_536,
            dictionary_tags: true,
            max_bytes: None,
        }
    }
}

impl From<ParquetCompression> for Compression {
    fn from(c: ParquetCompression) -> Self {
        match c {
            ParquetCompression::None => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Zstd => Compression::ZSTD(parquet::basic::ZstdLevel::default()),
            ParquetCompression::Lz4 => Compression::LZ4,
        }
    }
}

/// Write a fallible sequence of `RecordBatch`es to a Parquet file.
///
/// The iterator is consumed lazily and each batch is dropped after it is
/// written, so the peak memory of an export is one batch plus the Parquet
/// writer's in-progress row group — never the whole result set. Pair it with
/// [`Chronix::execute_iter`](crate::Chronix::execute_iter) to export a window
/// larger than available RAM.
///
/// Items are `Result` so a query stream can surface an I/O error mid-export
/// rather than having to be collected first; the error aborts the write.
///
/// # Errors
///
/// Returns an error if the batch stream fails, if there is no data, or if
/// file creation or Parquet writing fails.
pub fn write_parquet<I>(
    batches: I,
    output_path: &Path,
    config: &ParquetExportConfig,
) -> Result<ParquetExportResult>
where
    I: IntoIterator<Item = Result<RecordBatch>>,
{
    let mut iter = batches.into_iter();
    let Some(first) = iter.next().transpose()? else {
        return Err(DbError::Internal("No data to export".into()));
    };

    let schema = first.schema();

    let mut props = WriterProperties::builder()
        .set_compression(config.compression.into())
        .set_max_row_group_row_count(Some(config.row_group_size))
        .set_dictionary_enabled(config.dictionary_tags);

    // Dictionary-encode string columns explicitly. Tags are the low-cardinality
    // columns in a time-series export, and being explicit per column keeps the
    // guarantee visible instead of depending on the writer's global default.
    if config.dictionary_tags {
        for field in schema.fields() {
            if matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _)
            ) {
                props = props
                    .set_column_dictionary_enabled(ColumnPath::from(field.name().as_str()), true);
            }
        }
    }

    // Ensure parent directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).map_err(DbError::Io)?;
    }

    let file = std::fs::File::create(output_path).map_err(DbError::Io)?;

    let mut writer = ArrowWriter::try_new(file, schema, Some(props.build()))
        .map_err(|e| DbError::Internal(format!("Failed to create Parquet writer: {e}")))?;

    let mut total_rows: u64 = 0;
    let mut truncated = false;

    for batch in std::iter::once(Ok(first)).chain(iter) {
        // Check the bound *before* writing so the limit is respected rather
        // than merely detected. `bytes_written` counts flushed row groups;
        // `in_progress_size` covers the row group still being built.
        //
        // Checked before pulling the next batch would be nicer still, but the
        // budget is a row-group-boundary approximation either way; stopping
        // here means the source stream is never asked for the buckets past
        // the budget.
        if let Some(limit) = config.max_bytes {
            let so_far = writer.bytes_written() as u64 + writer.in_progress_size() as u64;
            if so_far >= limit {
                truncated = true;
                break;
            }
        }

        let batch = batch?;
        total_rows += batch.num_rows() as u64;
        writer
            .write(&batch)
            .map_err(|e| DbError::Internal(format!("Failed to write Parquet batch: {e}")))?;
    }

    writer
        .close()
        .map_err(|e| DbError::Internal(format!("Failed to close Parquet writer: {e}")))?;

    let bytes_written = std::fs::metadata(output_path).map(|m| m.len()).unwrap_or(0);

    Ok(ParquetExportResult {
        rows_written: total_rows,
        bytes_written,
        truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1000, 2000, 3000, 4000, 5000])),
                Arc::new(StringArray::from(vec![
                    "srv-1", "srv-1", "srv-2", "srv-2", "srv-1",
                ])),
                Arc::new(Float64Array::from(vec![95.5, 96.0, 50.0, 51.0, 97.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn export_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let batch = make_test_batch();
        let config = ParquetExportConfig::default();
        let result = write_parquet([Ok(batch)], &path, &config).unwrap();
        assert_eq!(result.rows_written, 5);
        assert!(!result.truncated);
        assert!(result.bytes_written > 0);

        // Read back with parquet crate
        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut total = 0;
        for result in reader {
            let read_batch = result.unwrap();
            total += read_batch.num_rows();
            // Verify schema matches
            assert_eq!(read_batch.num_columns(), 3);
        }
        assert_eq!(total, 5);
    }

    #[test]
    fn export_uncompressed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uncompressed.parquet");

        let batch = make_test_batch();
        let config = ParquetExportConfig {
            compression: ParquetCompression::None,
            row_group_size: 100,
            ..Default::default()
        };
        let result = write_parquet([Ok(batch)], &path, &config).unwrap();
        assert_eq!(result.rows_written, 5);
    }

    #[test]
    fn export_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.parquet");

        let batch1 = make_test_batch();
        let batch2 = make_test_batch();
        let config = ParquetExportConfig::default();
        let result = write_parquet([Ok(batch1), Ok(batch2)], &path, &config).unwrap();
        assert_eq!(result.rows_written, 10);
    }

    #[test]
    fn export_empty_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.parquet");
        let config = ParquetExportConfig::default();
        assert!(write_parquet(Vec::<Result<RecordBatch>>::new(), &path, &config).is_err());
    }

    /// hems #4: tag columns must be dictionary-encoded so a fleet upload of a
    /// time window stays small.
    #[test]
    fn tag_columns_are_dictionary_encoded() {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dict.parquet");

        // Many rows, few distinct hosts — the shape dictionary encoding is for.
        let n = 4096;
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let hosts: Vec<&str> = (0..n)
            .map(|i| if i % 2 == 0 { "srv-1" } else { "srv-2" })
            .collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(hosts)),
                Arc::new(Float64Array::from(
                    (0..n).map(|i| i as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        let config = ParquetExportConfig {
            compression: ParquetCompression::None,
            ..Default::default()
        };
        write_parquet([Ok(batch)], &path, &config).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let rg = reader.metadata().row_group(0);
        let host_col = (0..rg.num_columns())
            .map(|i| rg.column(i))
            .find(|c| c.column_path().string() == "host")
            .expect("host column");
        assert!(
            host_col.dictionary_page_offset().is_some(),
            "tag column has no dictionary page: encodings = {:?}",
            host_col.encodings().collect::<Vec<_>>()
        );
    }

    /// hems #4: a size-bounded export must come back short and say so rather
    /// than filling the device.
    #[test]
    fn size_bound_truncates_and_reports() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bounded.parquet");

        let batches: Vec<RecordBatch> = (0..200).map(|_| make_test_batch()).collect();
        let unbounded = ParquetExportConfig {
            row_group_size: 8,
            compression: ParquetCompression::None,
            ..Default::default()
        };
        let full = write_parquet(batches.clone().into_iter().map(Ok), &path, &unbounded).unwrap();
        assert!(!full.truncated);
        assert_eq!(full.rows_written, 1000);

        let limit = full.bytes_written / 4;
        let bounded = ParquetExportConfig {
            max_bytes: Some(limit),
            ..unbounded
        };
        let capped = write_parquet(batches.into_iter().map(Ok), &path, &bounded).unwrap();

        assert!(
            capped.truncated,
            "expected the size bound to cut the export"
        );
        assert!(
            capped.rows_written < full.rows_written,
            "truncated export wrote as many rows as the full one"
        );
        assert!(
            capped.bytes_written < full.bytes_written,
            "truncated file is not smaller"
        );

        // The file must still be a valid, readable Parquet file.
        let file = std::fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let read: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(read as u64, capped.rows_written);
    }
}
