//! Parquet re-encoding for the cold tier.
//!
//! # Why the cold tier is not `.csx`
//!
//! The hot tier stays `.csx` because time-series-specific encodings, row-group
//! zone maps and series blooms measurably beat general Parquet on this
//! workload. That advantage is real, and it is also irrelevant to data nobody
//! queries hot. What matters about an archive is that something other than
//! chronix can read it: Spark, DuckDB, Polars and pandas all read Parquet and
//! none of them will ever read `.csx`.
//!
//! So the tiering engine **re-encodes** on the way out. A cold object is
//! an ordinary Parquet file with an ordinary Arrow schema — `SELECT * FROM
//! read_parquet('s3://…')` in DuckDB works with no chronix in the picture.
//!
//! # What is preserved
//!
//! The Arrow schema the `.csx` reader produces is written verbatim, so the
//! canonical column order — timestamp, then tags sorted, then fields sorted —
//! survives the round trip, and so do nulls: `.csx` v2 validity bitmaps become
//! Parquet definition levels, which is the same information in the format the
//! rest of the world uses.
//!
//! Tag columns are dictionary-encoded explicitly rather than left to the
//! writer's heuristics. Tags are the low-cardinality dimension by definition,
//! and a dictionary page is what makes an external reader's predicate pushdown
//! work on them.
//!
//! # What is lost, and why that is the trade
//!
//! Series blooms and the skip index do not survive: Parquet has no place to
//! put them. Cold reads therefore prune on row-group statistics alone, which
//! is weaker than the five-level pruning the hot tier gets
//! than a `.csx` read. That is the price of the archive being readable, and
//! it is charged on the data that is queried least.

use std::path::Path;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::objstore::error::{ObjStoreError, Result};
use crate::segment::SegmentReader;

/// Zstd level for cold objects.
///
/// Cold data is written once and read rarely, so the compression budget is
/// spent where it pays: level 9 is roughly the knee of the ratio/time curve
/// for this data, and the write happens on a background tiering task.
const COLD_ZSTD_LEVEL: i32 = 9;

/// Rows per Parquet row group in a cold object.
///
/// Matches the `.csx` default so the row-group boundaries — and therefore the
/// statistics an external reader prunes on — line up with the ones the segment
/// was written with.
const COLD_ROW_GROUP_ROWS: usize = 65_536;

/// The on-object-store format of a tiered segment.
///
/// Recorded per segment rather than inferred from configuration, because the
/// policy can change while objects written under the old policy are still in
/// the bucket. A reader must be told what it is opening, not guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum ColdFormat {
    /// Verbatim `.csx`. Smallest, and readable only by chronix.
    Csx,
    /// Re-encoded Parquet. Readable by any Arrow-ecosystem tool.
    #[default]
    Parquet,
}

impl ColdFormat {
    /// File extension used for objects in this format.
    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Csx => "csx",
            Self::Parquet => "parquet",
        }
    }
}

impl std::fmt::Display for ColdFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Csx => "csx",
            Self::Parquet => "parquet",
        })
    }
}

/// Writer properties for a cold Parquet object.
///
/// Dictionary encoding is enabled per column rather than globally: it is what
/// makes tag columns cheap, and it is actively counterproductive on a
/// high-cardinality float column, where the dictionary is as large as the data
/// it replaces.
fn cold_writer_properties(schema: &SchemaRef) -> Result<WriterProperties> {
    let mut props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(COLD_ZSTD_LEVEL).map_err(|e| ObjStoreError::InvalidConfig {
                detail: format!("invalid cold-tier zstd level: {e}"),
            })?,
        ))
        .set_max_row_group_row_count(Some(COLD_ROW_GROUP_ROWS))
        .set_dictionary_enabled(false);

    for field in schema.fields() {
        if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            let path = parquet::schema::types::ColumnPath::from(field.name().as_str());
            props = props.set_column_dictionary_enabled(path, true);
        }
    }

    Ok(props.build())
}

/// Re-encode a `.csx` segment file as a Parquet object.
///
/// Reads one row group at a time so peak memory is a row group rather than the
/// whole segment — the tiering task runs on the same box as the database, and
/// on the gateway that box has 512 MB.
///
/// # Errors
///
/// Returns an error if the segment cannot be opened or read, or if the Parquet
/// writer fails.
pub fn csx_file_to_parquet(csx_path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let reader = SegmentReader::open(csx_path).map_err(ObjStoreError::Segment)?;
    let row_groups = reader.row_group_count();

    if row_groups == 0 {
        return Err(ObjStoreError::InvalidConfig {
            detail: "cannot re-encode an empty segment".to_string(),
        });
    }

    // The first row group establishes the schema the writer commits to.
    let first = reader.read_row_group(0).map_err(ObjStoreError::Segment)?;
    let schema = first.schema();
    let props = cold_writer_properties(&schema)?;

    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema.clone(), Some(props))
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;

    writer
        .write(&first)
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;
    drop(first);

    for rg in 1..row_groups {
        let batch = reader.read_row_group(rg).map_err(ObjStoreError::Segment)?;
        if batch.schema() != schema {
            return Err(ObjStoreError::InvalidConfig {
                detail: format!(
                    "row group {rg} has a different schema from row group 0; \
                     the segment is not internally consistent"
                ),
            });
        }
        writer
            .write(&batch)
            .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;
    }

    writer
        .close()
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;
    Ok(out)
}

/// Read a cold Parquet object back into Arrow batches.
///
/// This is the path chronix's own queries take over cold data. It reproduces
/// the batches the `.csx` reader would have produced, so everything downstream
/// — dedup, tombstone filtering, aggregation — is unchanged.
///
/// # Errors
///
/// Returns an error if the bytes are not a readable Parquet file.
pub fn parquet_to_batches(data: Vec<u8>) -> Result<Vec<RecordBatch>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;
    let reader = builder
        .build()
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;

    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))
}

/// Read a cold Parquet object from a local file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or is not valid Parquet.
pub fn parquet_file_to_batches(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(path).map_err(ObjStoreError::Io)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))?
        .build()
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))
}

/// Concatenate cold batches into one, for callers that want a single batch.
///
/// # Errors
///
/// Returns an error if the batches do not share a schema.
pub fn concat_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch> {
    arrow::compute::concat_batches(schema, batches)
        .map_err(|e| ObjStoreError::Parquet(e.to_string()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("b"),
                ])),
                // A null must survive the round trip as a null, not a sentinel.
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    None,
                    Some(3.5),
                    Some(4.5),
                ])),
            ],
        )
        .unwrap()
    }

    /// A cold object must round-trip values, nulls and schema exactly.
    #[test]
    fn parquet_round_trip_preserves_values_and_nulls() {
        let batch = sample_batch();
        let schema = batch.schema();
        let props = cold_writer_properties(&schema).unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let back = parquet_to_batches(buf).unwrap();
        let merged = concat_batches(&schema, &back).unwrap();

        assert_eq!(merged.num_rows(), 4);
        assert_eq!(
            merged.schema().fields().len(),
            3,
            "column order and count must survive"
        );
        assert_eq!(merged.schema().field(0).name(), "timestamp");

        let values = merged
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            values.is_null(1),
            "an absent field must read back as NULL, not as 0.0"
        );
        assert!((values.value(0) - 1.5).abs() < f64::EPSILON);
        assert!((values.value(3) - 4.5).abs() < f64::EPSILON);
    }

    /// Tag columns must carry a dictionary page — that is what makes an
    /// external reader's predicate pushdown work on them.
    #[test]
    fn tag_columns_are_dictionary_encoded() {
        let batch = sample_batch();
        let schema = batch.schema();
        let props = cold_writer_properties(&schema).unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf)).unwrap();
        let meta = builder.metadata();
        let rg = meta.row_group(0);

        let host = rg
            .columns()
            .iter()
            .find(|c| c.column_path().string() == "host")
            .expect("host column present");
        assert!(
            host.dictionary_page_offset().is_some(),
            "tag column must be dictionary-encoded"
        );

        let value = rg
            .columns()
            .iter()
            .find(|c| c.column_path().string() == "value")
            .expect("value column present");
        assert!(
            value.dictionary_page_offset().is_none(),
            "a float measurement column must not be dictionary-encoded"
        );
    }

    #[test]
    fn cold_format_extensions_are_distinct() {
        assert_eq!(ColdFormat::Csx.extension(), "csx");
        assert_eq!(ColdFormat::Parquet.extension(), "parquet");
        assert_eq!(
            ColdFormat::default(),
            ColdFormat::Parquet,
            "the archive should be readable by default"
        );
    }
}
