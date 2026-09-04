//! Registering the Parquet cold tier with DataFusion.
//!
//! The archiver writes one Parquet object per `(measurement, shard)` under
//! `measurement=<m>/shard=<n>/`. This module points DataFusion at the same
//! bucket, so the archive is queryable from chronix as well as from DuckDB.
//!
//! ```sql
//! -- after register_cold_tier(&ctx, "s3://bucket/chronix", "power", "power_cold")
//! SELECT date_trunc('day', _time) AS d, avg(watts)
//! FROM power_cold
//! WHERE _time >= '2024-01-01'
//! GROUP BY d;
//! ```
//!
//! # One table is one measurement
//!
//! A listing table has exactly one schema and two measurements do not share
//! one, so a registration names a measurement and points at that measurement's
//! partition — the same shape as the hot tier. Because the archive is written
//! from the read path with the hot tier's schema, the cold table's columns are
//! the hot table's: `_time` typed `Timestamp(ns)`, tags, fields. A query moved
//! from one to the other is the same query.
//!
//! # A separate table, not a union with the hot tier
//!
//! A cold object has no series bloom and no skip index, so a cold scan prunes
//! on row-group statistics alone. Unioning that into the hot table would let a
//! query change cost class by orders of magnitude whenever its range crossed
//! the tiering boundary, with nothing in the plan saying so.
//!
//! # Credentials
//!
//! Taken from the process environment by the `object_store` builders
//! (`AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`,
//! `AZURE_STORAGE_ACCOUNT`, …). Chronix does not read, store or log them.

use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;

use crate::error::{DbError, Result};

/// Register one measurement's cold archive as a queryable SQL table.
///
/// `url` is the archive root the tiering pass writes to — the same value as
/// [`ArchiveConfig::remote_url`](crate::cold_archive::ArchiveConfig::remote_url)
/// — for example `s3://bucket/chronix` or `file:///var/lib/chronix/archive`.
/// `measurement` selects the `measurement=<m>/` partition beneath it, and
/// `table_name` is the name the table takes in SQL.
///
/// The table is a Parquet listing over that partition, so objects added by
/// later archival passes are picked up without re-registering.
///
/// # Errors
///
/// Returns an error if `measurement` is not a usable path segment, if the URL
/// is not a supported object-store URL, if the store cannot be constructed
/// (usually missing credentials), or if the measurement has no archived
/// objects to infer a schema from.
pub async fn register_cold_tier(
    ctx: &SessionContext,
    url: &str,
    measurement: &str,
    table_name: &str,
) -> Result<()> {
    if measurement.is_empty()
        || measurement.contains('/')
        || measurement.contains('\\')
        || measurement.contains("..")
        || measurement.contains('=')
    {
        return Err(DbError::Internal(format!(
            "cold tier measurement {measurement:?} is not a usable path segment"
        )));
    }

    // The archive root may or may not carry a trailing slash; the partition
    // has to be joined onto it either way, and a missing slash would otherwise
    // splice the measurement onto the last path component of the bucket
    // prefix.
    let root = url.strip_suffix('/').unwrap_or(url);
    let table_url = format!("{root}/measurement={measurement}/");

    let parsed = url::Url::parse(&table_url)
        .map_err(|e| DbError::Internal(format!("cold tier URL {table_url} is not a URL: {e}")))?;

    let (store, _path) = object_store::parse_url(&parsed).map_err(|e| {
        DbError::Internal(format!(
            "cold tier URL {table_url} is not a supported object store: {e}"
        ))
    })?;

    let store_url = ObjectStoreUrl::parse(&parsed[..url::Position::BeforePath]).map_err(|e| {
        DbError::Internal(format!("cold tier URL {table_url} is not registrable: {e}"))
    })?;
    ctx.register_object_store(store_url.as_ref(), Arc::from(store));

    // `shard=<n>` beneath the measurement is a Hive-style partition directory,
    // which DataFusion's listing treats as a partition rather than as a
    // subdirectory — so this finds the objects with
    // `listing_table_ignore_subdirectory` left at its default, instead of
    // mutating the shared session to relax it.
    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");

    ctx.register_listing_table(table_name, &table_url, options, None, None)
        .await
        .map_err(|e| {
            DbError::Internal(format!(
                "failed to register cold tier {table_url} as `{table_name}`: {e}"
            ))
        })?;

    tracing::info!(
        url = %table_url,
        measurement,
        table = table_name,
        "cold tier registered for SQL"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use arrow::array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;

    /// Write a Parquet object shaped exactly like one the archiver produces:
    /// the hot tier's schema, `_time` included.
    fn write_cold_object(dir: &std::path::Path, name: &str) {
        std::fs::create_dir_all(dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("watts", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_i64, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(dir.join(name)).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    /// The archive root, laid out the way the archiver writes it.
    fn archive_with(root: &std::path::Path, measurement: &str, shard: i64, name: &str) {
        write_cold_object(
            &root
                .join(format!("measurement={measurement}"))
                .join(format!("shard={shard}")),
            name,
        );
    }

    /// The exit criterion for the cold tier: archived data queryable via SQL,
    /// found through the `measurement=`/`shard=` partitions the archiver
    /// writes. DataFusion skips plain subdirectories by default, so the layout
    /// being Hive-style is load-bearing rather than cosmetic.
    #[tokio::test]
    async fn archived_objects_are_queryable_through_sql() {
        let tmp = tempfile::tempdir().unwrap();
        archive_with(tmp.path(), "power", 1, "part-a.parquet");
        archive_with(tmp.path(), "power", 2, "part-b.parquet");

        let ctx = SessionContext::new();
        let url = format!("file://{}/", tmp.path().display());
        register_cold_tier(&ctx, &url, "power", "power_cold")
            .await
            .unwrap();

        let batches = ctx
            .sql("SELECT count(*) AS n, avg(watts) AS a FROM power_cold")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];

        let n = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 6, "both shards must be in the table");

        let a = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((a - 20.0).abs() < 1e-9, "values must survive, got {a}");
    }

    /// One table is one measurement. This is the property that a single
    /// listing over the whole archive root could not have: `power` and `cpu`
    /// do not share a schema, so each has to be its own table.
    #[tokio::test]
    async fn a_table_sees_only_its_own_measurement() {
        let tmp = tempfile::tempdir().unwrap();
        archive_with(tmp.path(), "power", 1, "part-a.parquet");
        archive_with(tmp.path(), "cpu", 1, "part-a.parquet");

        let ctx = SessionContext::new();
        let url = format!("file://{}/", tmp.path().display());
        register_cold_tier(&ctx, &url, "power", "power_cold")
            .await
            .unwrap();

        let batches = ctx
            .sql("SELECT count(*) AS n FROM power_cold")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3, "another measurement's objects must not be in scope");
    }

    /// An archive root given without a trailing slash must resolve to the same
    /// partition — the join is the caller's most likely slip.
    #[tokio::test]
    async fn the_root_may_omit_its_trailing_slash() {
        let tmp = tempfile::tempdir().unwrap();
        archive_with(tmp.path(), "power", 1, "part-a.parquet");

        let ctx = SessionContext::new();
        let url = format!("file://{}", tmp.path().display());
        register_cold_tier(&ctx, &url, "power", "power_cold")
            .await
            .unwrap();

        let batches = ctx
            .sql("SELECT count(*) AS n FROM power_cold")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            3
        );
    }

    /// Predicates must reach the table — an archive that has to be fully
    /// scanned for every query is not a usable archive — and `_time` must be a
    /// real timestamp, so the hot tier's time predicates work unchanged.
    #[tokio::test]
    async fn cold_tier_answers_filtered_queries_on_time_and_tags() {
        let tmp = tempfile::tempdir().unwrap();
        archive_with(tmp.path(), "power", 1, "part-a.parquet");

        let ctx = SessionContext::new();
        let url = format!("file://{}/", tmp.path().display());
        register_cold_tier(&ctx, &url, "power", "cold")
            .await
            .unwrap();

        let batches = ctx
            .sql(
                "SELECT sum(watts) AS s FROM cold \
                 WHERE host = 'a' AND _time < '1970-01-01T00:00:00.000000003Z'",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let s = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((s - 30.0).abs() < 1e-9, "filters must apply, got {s}");
    }

    /// A measurement name that would escape its partition must be refused
    /// before it is spliced into a URL.
    #[tokio::test]
    async fn a_traversing_measurement_name_is_refused() {
        let ctx = SessionContext::new();
        for bad in ["", "../etc", "a/b", "shard=1"] {
            let err = register_cold_tier(&ctx, "file:///tmp/archive/", bad, "x")
                .await
                .expect_err("must be refused: {bad}");
            assert!(
                err.to_string().contains("not a usable path segment"),
                "error should name the problem, got: {err}"
            );
        }
    }

    /// A URL that is not an object store must be refused with a clear message
    /// rather than producing an empty table.
    #[tokio::test]
    async fn an_unsupported_url_is_refused() {
        let ctx = SessionContext::new();
        let err = register_cold_tier(&ctx, "not a url", "power", "x")
            .await
            .expect_err("a malformed URL must be refused");
        assert!(
            err.to_string().contains("not a URL"),
            "error should name the problem, got: {err}"
        );
    }
}
