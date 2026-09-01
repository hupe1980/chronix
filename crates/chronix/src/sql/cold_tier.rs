//! Registering the Parquet cold tier with DataFusion.
//!
//! The tiering engine re-encodes segments to Parquet on the way to object
//! storage ([`chronix_engine::objstore::parquet_tier`], D6). This module is the
//! other half of that decision: it makes the archive queryable *from chronix*
//! as well as from DuckDB, by pointing DataFusion at the same bucket.
//!
//! ```sql
//! -- after register_cold_tier(&ctx, "s3://bucket/chronix", "power_cold")
//! SELECT date_trunc('day', timestamp) AS d, avg(watts)
//! FROM power_cold
//! WHERE timestamp >= '2024-01-01'
//! GROUP BY d;
//! ```
//!
//! # Why this is a separate table rather than a union with the hot tier
//!
//! Registering the cold prefix as its own table is a deliberate choice, not a
//! missing feature. A cold object has no series bloom and no skip index —
//! Parquet has nowhere to put them — so a cold scan prunes on row-group
//! statistics alone. Silently unioning that into the hot table would mean a
//! query whose time range happens to cross the tiering boundary quietly
//! changes cost class by orders of magnitude, with nothing in the plan saying
//! so. Naming the cold table makes the archive an explicit thing to query,
//! which is what an archive should be.
//!
//! # Credentials
//!
//! Taken from the process environment by the `object_store` builders
//! (`AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`,
//! `AZURE_STORAGE_ACCOUNT`, …). Chronix does not read, store or log them —
//! a database that persists cloud credentials is a database that leaks them.

use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;

use crate::error::{DbError, Result};

/// Register a Parquet cold-tier prefix as a queryable SQL table.
///
/// `url` is the object-store location the tiering engine writes to — the same
/// value as [`TieringConfig::remote_url`](chronix_engine::objstore::TieringConfig::remote_url)
/// — for example `s3://bucket/chronix` or `file:///var/lib/chronix/archive`.
/// `table_name` is the name the table takes in SQL.
///
/// The table is a Parquet listing over that prefix, so objects added by later
/// tiering runs are picked up without re-registering.
///
/// # Errors
///
/// Returns an error if the URL is not a supported object-store URL, if the
/// store cannot be constructed (usually missing credentials), or if the prefix
/// holds no Parquet objects to infer a schema from.
pub async fn register_cold_tier(ctx: &SessionContext, url: &str, table_name: &str) -> Result<()> {
    let parsed = url::Url::parse(url)
        .map_err(|e| DbError::Internal(format!("cold tier URL {url} is not a URL: {e}")))?;

    let (store, _path) = object_store::parse_url(&parsed).map_err(|e| {
        DbError::Internal(format!(
            "cold tier URL {url} is not a supported object store: {e}"
        ))
    })?;

    let store_url = ObjectStoreUrl::parse(&parsed[..url::Position::BeforePath])
        .map_err(|e| DbError::Internal(format!("cold tier URL {url} is not registrable: {e}")))?;
    ctx.register_object_store(store_url.as_ref(), Arc::from(store));

    // The tiering engine writes `namespace=<ns>/shard=<n>/<file>.parquet`.
    // Those are Hive-style partition directories, which DataFusion's listing
    // treats as partitions rather than as subdirectories — so this finds the
    // objects with `listing_table_ignore_subdirectory` left at its default,
    // instead of mutating the shared session to relax it (D25).
    //
    // Filtering on the extension also keeps `.csx` objects left by an earlier
    // `ColdFormat::Csx` policy out of the table, rather than failing the whole
    // registration on the first one.
    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");

    ctx.register_listing_table(table_name, url, options, None, None)
        .await
        .map_err(|e| {
            DbError::Internal(format!(
                "failed to register cold tier {url} as `{table_name}`: {e}"
            ))
        })?;

    tracing::info!(url, table = table_name, "cold tier registered for SQL");
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    /// Write a Parquet file shaped like a cold object.
    fn write_cold_object(dir: &std::path::Path, name: &str) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("watts", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
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

    /// The exit criterion for the cold tier: cold segments queryable via SQL.
    #[tokio::test]
    async fn cold_segments_are_queryable_through_sql() {
        let tmp = tempfile::tempdir().unwrap();
        write_cold_object(tmp.path(), "seg_0001.parquet");
        write_cold_object(tmp.path(), "seg_0002.parquet");

        let ctx = SessionContext::new();
        let url = format!("file://{}/", tmp.path().display());
        register_cold_tier(&ctx, &url, "power_cold").await.unwrap();

        let df = ctx
            .sql("SELECT count(*) AS n, avg(watts) AS a FROM power_cold")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];

        let n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 6, "both cold objects must be in the table");

        let a = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((a - 20.0).abs() < 1e-9, "values must survive, got {a}");
    }

    /// The tiering engine writes `namespace=<ns>/shard=<n>/<name>.parquet`.
    /// The flat-layout test above passed while this one failed against the old
    /// `ns_<x>/shard_<n>` layout, because DataFusion skips plain subdirectories
    /// by default — so the layout the engine actually produces gets its own
    /// test.
    #[tokio::test]
    async fn cold_tier_sees_the_nested_layout_the_tiering_engine_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("namespace=default").join("shard=1");
        std::fs::create_dir_all(&nested).unwrap();
        write_cold_object(&nested, "seg_0001.parquet");

        let ctx = SessionContext::new();
        let url = format!("file://{}/", tmp.path().display());
        register_cold_tier(&ctx, &url, "nested").await.unwrap();

        let batches = ctx
            .sql("SELECT count(*) AS n FROM nested")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 3, "objects nested under namespace/shard must be visible");
    }

    /// Predicates must reach the table — an archive that has to be fully
    /// scanned for every query is not a usable archive.
    #[tokio::test]
    async fn cold_tier_answers_filtered_queries() {
        let tmp = tempfile::tempdir().unwrap();
        write_cold_object(tmp.path(), "seg_0001.parquet");

        let ctx = SessionContext::new();
        let url = format!("file://{}/", tmp.path().display());
        register_cold_tier(&ctx, &url, "cold").await.unwrap();

        let batches = ctx
            .sql("SELECT sum(watts) AS s FROM cold WHERE host = 'a'")
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
        assert!((s - 30.0).abs() < 1e-9, "tag filter must apply, got {s}");
    }

    /// A URL that is not an object store must be refused with a clear message
    /// rather than producing an empty table.
    #[tokio::test]
    async fn an_unsupported_url_is_refused() {
        let ctx = SessionContext::new();
        let err = register_cold_tier(&ctx, "not a url", "x")
            .await
            .expect_err("a malformed URL must be refused");
        assert!(
            err.to_string().contains("not a URL"),
            "error should name the problem, got: {err}"
        );
    }
}
