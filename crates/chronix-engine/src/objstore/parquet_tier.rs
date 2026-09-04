//! Parquet encoding for the cold tier.
//!
//! # Why the cold tier is not `.csx`
//!
//! The hot tier stays `.csx` because time-series-specific encodings, row-group
//! zone maps and series blooms measurably beat general Parquet on this
//! workload — and that advantage is irrelevant to data nobody queries hot.
//! What matters about an archive is that something else can read it: Spark,
//! DuckDB, Polars and pandas all read Parquet and none of them will ever read
//! `.csx`.
//!
//! # The writer takes query output, not a segment file
//!
//! [`ParquetArchiveWriter`] streams [`RecordBatch`]es into one object. Nothing
//! here can open a segment, which is deliberate: a segment file is not what
//! the database would answer — it still holds rows a tombstone masks, and two
//! overlapping segments hold the same `(series, timestamp)` twice. The caller
//! feeds it the read path's output, which has resolved both.
//!
//! # What is preserved, and what is lost
//!
//! The canonical column order — `_time`, then tags sorted, then fields sorted
//! — and nulls, as Parquet definition levels. Tag columns are dictionary-
//! encoded explicitly, which is what makes an external reader's predicate
//! pushdown work on them; float columns are not, since there the dictionary is
//! as large as the data.
//!
//! Series blooms and the skip index do not survive — Parquet has nowhere to
//! put them — so a cold scan prunes on row-group statistics alone. That is the
//! price of being readable, charged on the data queried least.

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::objstore::error::{ObjStoreError, Result};

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

/// Streams query-output [`RecordBatch`]es into one cold Parquet object.
///
/// The writer holds the encoded object in memory because the upload is a
/// single `put` that needs the whole body; peak memory is therefore the
/// *compressed* object plus one row group, not the scan. The caller bounds
/// the scan by archiving one `(measurement, shard)` group at a time.
///
/// Every batch must match the schema the writer was opened with. The caller
/// aligns batches to a single schema before writing, which is what makes a
/// measurement whose fields were registered across separate writes produce
/// one consistent object rather than a schema error halfway through.
pub struct ParquetArchiveWriter {
    writer: ArrowWriter<Vec<u8>>,
    schema: SchemaRef,
    rows: u64,
}

impl ParquetArchiveWriter {
    /// Open a writer that will encode batches of `schema`.
    ///
    /// # Errors
    ///
    /// Returns an error if the compression level is invalid or the Parquet
    /// writer cannot be created for this schema.
    pub fn new(schema: SchemaRef) -> Result<Self> {
        let props = cold_writer_properties(&schema)?;
        let writer = ArrowWriter::try_new(Vec::new(), schema.clone(), Some(props))
            .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;
        Ok(Self {
            writer,
            schema,
            rows: 0,
        })
    }

    /// Append one batch.
    ///
    /// # Errors
    ///
    /// Returns an error if `batch` does not match the writer's schema, or if
    /// the Parquet writer fails.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if batch.schema() != self.schema {
            return Err(ObjStoreError::InvalidConfig {
                detail: format!(
                    "cold archive batch schema {:?} does not match the object schema {:?}",
                    batch.schema(),
                    self.schema
                ),
            });
        }
        self.writer
            .write(batch)
            .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    /// Rows written so far.
    #[must_use]
    pub const fn rows(&self) -> u64 {
        self.rows
    }

    /// Close the writer and return the encoded object and its row count.
    ///
    /// # Errors
    ///
    /// Returns an error if the Parquet footer cannot be written.
    pub fn finish(self) -> Result<(Vec<u8>, u64)> {
        let rows = self.rows;
        let bytes = self
            .writer
            .into_inner()
            .map_err(|e| ObjStoreError::Parquet(e.to_string()))?;
        Ok((bytes, rows))
    }
}

/// Read a cold Parquet object back into Arrow batches.
///
/// Chronix's own SQL reads the archive through DataFusion's listing table, not
/// through this: it exists so an embedder holding archive bytes can decode them
/// with the same reader the round-trip tests use. Nothing further is applied —
/// dedup and tombstones were resolved when the object was *written*, which is
/// the whole point of building it from the read path.
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
        let merged = arrow::compute::concat_batches(&schema, &back).unwrap();

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

    /// The writer streams several batches into one object and refuses a
    /// batch that does not match the schema it committed to.
    #[test]
    fn archive_writer_streams_batches_and_pins_the_schema() {
        let batch = sample_batch();
        let mut w = ParquetArchiveWriter::new(batch.schema()).unwrap();
        w.write(&batch).unwrap();
        w.write(&batch).unwrap();

        let mismatched = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        assert!(
            w.write(&mismatched).is_err(),
            "a batch of a different schema must be refused, not silently dropped"
        );

        let (bytes, rows) = w.finish().unwrap();
        assert_eq!(rows, 8, "both batches must be counted");
        let back = parquet_to_batches(bytes).unwrap();
        let total: usize = back.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 8);
    }

    /// An empty batch is a no-op, not a zero-row row group.
    #[test]
    fn archive_writer_ignores_empty_batches() {
        let schema = sample_batch().schema();
        let mut w = ParquetArchiveWriter::new(schema.clone()).unwrap();
        w.write(&RecordBatch::new_empty(schema)).unwrap();
        assert_eq!(w.rows(), 0);
        let (_, rows) = w.finish().unwrap();
        assert_eq!(rows, 0);
    }
}
