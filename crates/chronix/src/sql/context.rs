//! `DataFusion` catalog and session context for Chronix.
//!
//! Provides a dynamic [`CatalogProvider`] that reflects Chronix measurements
//! as `DataFusion` tables. This means new measurements created via schema-on-write
//! are instantly queryable via SQL without manual registration.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use super::functions::register_udfs;
use super::provider::ChronixTableProvider;
use crate::Chronix;

/// A `DataFusion` [`CatalogProvider`] backed by Chronix.
///
/// Reports a single schema `"public"` containing all measurements as tables.
#[derive(Debug)]
pub(crate) struct ChronixCatalogProvider {
    db: Arc<Chronix>,
    namespace: Option<String>,
}

impl CatalogProvider for ChronixCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        vec!["public".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "public" {
            Some(Arc::new(ChronixSchemaProvider {
                db: self.db.clone(),
                namespace: self.namespace.clone(),
            }))
        } else {
            None
        }
    }
}

/// A `DataFusion` [`SchemaProvider`] backed by Chronix.
///
/// Each Chronix measurement is surfaced as a table. Tables are resolved
/// on demand so newly created measurements are immediately visible.
#[derive(Debug)]
pub(crate) struct ChronixSchemaProvider {
    db: Arc<Chronix>,
    namespace: Option<String>,
}

#[async_trait]
impl SchemaProvider for ChronixSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.db.measurement_names_in(self.namespace.as_deref())
    }

    fn table_exist(&self, name: &str) -> bool {
        // Scoped, because "the table resolves" is itself an answer. Both of
        // these used to consult the process-wide schema registry, so
        // `SELECT * FROM another_tenants_measurement` returned zero rows
        // where a name that genuinely does not exist errors — an existence
        // oracle a tenant could use to enumerate every other tenant's
        // measurement names.
        self.db.has_measurement_in(self.namespace.as_deref(), name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if !self.table_exist(name) {
            return Ok(None);
        }
        match ChronixTableProvider::try_new_scoped(self.db.clone(), name, self.namespace.clone()) {
            Ok(provider) => Ok(Some(Arc::new(provider))),
            Err(_) => Ok(None),
        }
    }
}

/// Create a `DataFusion` [`SessionContext`] fully wired to a Chronix database.
///
/// The returned context:
/// Has `chronix.public` as the default catalog and schema
/// Dynamically resolves measurement names to tables
/// Includes all custom Chronix SQL functions (`time_bucket`, `first`, `last`,
///   `rate`, `irate`)
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use chronix::prelude::*;
/// use chronix::sql::create_session_context;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ChronixConfig::builder().data_dir("/tmp/db").build()?;
/// let db = Arc::new(Chronix::open(config)?);
/// let ctx = create_session_context(db);
/// let df = ctx.sql("SELECT * FROM cpu LIMIT 10").await?;
/// df.show().await?;
/// # Ok(())
/// # }
/// ```
#[allow(clippy::needless_pass_by_value)] // Arc handed to the catalog provider by design
pub fn create_session_context(db: Arc<Chronix>) -> SessionContext {
    build_session_context(&db, None)
}

/// Create a [`SessionContext`] whose tables are scoped to one namespace.
///
/// Every table reached through the returned context carries a mandatory
/// `__namespace__` filter, and the tag itself is absent from every schema —
/// so no SQL text, however phrased, reads another namespace's rows, and no
/// result exposes the marker. This is the read-side counterpart of the tag
/// that `chronixd` stamps on every point it ingests.
///
/// Multi-tenant servers keep one context per namespace; the embedded API uses
/// [`create_session_context`] and sees everything.
#[allow(clippy::needless_pass_by_value)] // Arc handed to the catalog provider by design
pub fn create_namespaced_session_context(db: Arc<Chronix>, namespace: &str) -> SessionContext {
    build_session_context(&db, Some(namespace.to_string()))
}

fn build_session_context(db: &Arc<Chronix>, namespace: Option<String>) -> SessionContext {
    let config = db.config();
    let target_partitions = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1);

    // `information_schema` is on, which is what makes `SHOW TABLES`,
    // `SHOW COLUMNS` and the catalog views work — the first thing anybody
    // types in a SQL shell, and previously an error telling them to enable a
    // setting they cannot reach.
    //
    // It was off to prevent "catalog structure leakage", and that was the
    // right call while `table_names()` answered from the process-wide schema
    // registry: it would have listed every tenant's measurements. The
    // catalog is now scoped to the session's namespace, so what these views
    // enumerate is the caller's own data.
    let session_config = SessionConfig::new()
        .with_default_catalog_and_schema("chronix", "public")
        .with_information_schema(true)
        .with_target_partitions(target_partitions);

    // Configure RuntimeEnv with spill-to-disk and a bounded
    // memory pool so that large GROUP BY / ORDER BY queries spill instead
    // of OOM-killing the process.
    let spill_dir = config.data_dir.join("spill");
    let runtime_env = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![spill_dir])),
        )
        .with_memory_pool(Arc::new(FairSpillPool::new(config.per_query_memory_limit)))
        .build_arc()
        .expect("failed to build DataFusion RuntimeEnv");

    let state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();

    // `EpochLiteralRule` must run **before** DataFusion's `TypeCoercion`,
    // which is what rejects `_time >= 1700000000000000000`. Appending with
    // `with_analyzer_rule` puts it after, where the plan has already failed
    // — so the default list is rebuilt with ours in front.
    let state = {
        let mut builder = SessionStateBuilder::new_from_existing(state);
        let rules = builder
            .analyzer_rules()
            .take()
            .unwrap_or_else(|| datafusion::optimizer::Analyzer::new().rules);
        let mut with_ours: Vec<
            std::sync::Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>,
        > = vec![std::sync::Arc::new(super::epoch_literals::EpochLiteralRule)];
        with_ours.extend(rules);
        builder.with_analyzer_rules(with_ours).build()
    };

    let ctx = SessionContext::from(state);

    // Register Chronix catalog
    let catalog = Arc::new(ChronixCatalogProvider {
        db: db.clone(),
        namespace,
    });
    ctx.register_catalog("chronix", catalog);

    // Register custom SQL functions, bounded by the configured analytics
    // limits — which used to be read nowhere at all.
    register_udfs(
        &ctx,
        super::functions::ForecastLimits {
            max_horizon: db.config().analytics.max_forecast_horizon,
            max_training_points: db.config().analytics.max_training_points,
        },
    );

    // Register runtime UDFs added via db.register_udf() / db.register_udaf()
    for udf in db.custom_udfs() {
        ctx.register_udf((*udf).clone());
    }
    for udaf in db.custom_udafs() {
        ctx.register_udaf((*udaf).clone());
    }

    ctx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use arrow::array::Array;
    use tempfile::TempDir;

    fn open_test_db() -> (Arc<Chronix>, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap();
        let db = Arc::new(Chronix::open(config).unwrap());
        (db, dir)
    }

    #[tokio::test]
    async fn catalog_lists_measurements() {
        let (db, _dir) = open_test_db();

        // Insert a point to create measurement
        let key = SeriesKey::new("cpu", crate::tags! { "host" => "srv1" }).unwrap();
        let point = Point::new(key, crate::fields! { "usage" => 42.0_f64 }, 1_000_000_000).unwrap();
        db.insert(&point).unwrap();

        let provider = ChronixSchemaProvider {
            db,
            namespace: None,
        };
        let names = provider.table_names();
        assert!(names.contains(&"cpu".to_string()));
        assert!(provider.table_exist("cpu"));
        assert!(!provider.table_exist("nonexistent"));
    }

    #[tokio::test]
    async fn session_context_executes_sql() {
        let (db, _dir) = open_test_db();

        // Insert test data
        let key = SeriesKey::new("temp", crate::tags! { "room" => "office" }).unwrap();
        for i in 0..5 {
            let ts = (i + 1) * 1_000_000_000_i64;
            let point = Point::new(
                key.clone(),
                crate::fields! { "value" => 20.0 + i as f64 },
                ts,
            )
            .unwrap();
            db.insert(&point).unwrap();
        }

        let ctx = create_session_context(db);
        let df = ctx.sql("SELECT * FROM temp ORDER BY _time").await.unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn sql_with_where_clause() {
        let (db, _dir) = open_test_db();

        let key = SeriesKey::new("cpu", crate::tags! { "host" => "srv1" }).unwrap();
        for i in 0..10 {
            let ts = (i + 1) * 1_000_000_000_i64;
            let point = Point::new(
                key.clone(),
                crate::fields! { "usage" => i as f64 * 10.0 },
                ts,
            )
            .unwrap();
            db.insert(&point).unwrap();
        }

        let ctx = create_session_context(db);
        let df = ctx
            .sql("SELECT usage FROM cpu WHERE _time >= arrow_cast(5000000000, 'Timestamp(Nanosecond, None)')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert!(total_rows >= 6); // points 5-10
    }

    #[tokio::test]
    async fn sql_group_by_with_aggregation() {
        let (db, _dir) = open_test_db();

        for host in &["a", "b"] {
            let key = SeriesKey::new("cpu", crate::tags! { "host" => *host }).unwrap();
            for i in 0..3 {
                let ts = (i + 1) * 1_000_000_000_i64;
                let point =
                    Point::new(key.clone(), crate::fields! { "usage" => 10.0_f64 }, ts).unwrap();
                db.insert(&point).unwrap();
            }
        }

        let ctx = create_session_context(db);
        let df = ctx
            .sql("SELECT host, SUM(usage) as total FROM cpu GROUP BY host ORDER BY host")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2); // one row per host
    }

    #[tokio::test]
    async fn sql_order_by_limit() {
        let (db, _dir) = open_test_db();

        let key = SeriesKey::new("m", crate::tags! { "t" => "v" }).unwrap();
        for i in 0..20 {
            let point = Point::new(
                key.clone(),
                crate::fields! { "val" => i as f64 },
                (i + 1) * 1_000_000_000_i64,
            )
            .unwrap();
            db.insert(&point).unwrap();
        }

        let ctx = create_session_context(db);
        let df = ctx
            .sql("SELECT val FROM m ORDER BY _time DESC LIMIT 5")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 5);
    }

    // ── Window function tests ───────────────────────────────────────

    /// Helper to insert a series of (value, timestamp) points.
    async fn insert_series(db: &Arc<Chronix>, measurement: &str, tag: &str, values: &[f64]) {
        let key = SeriesKey::new(measurement, crate::tags! { "host" => tag }).unwrap();
        for (i, &v) in values.iter().enumerate() {
            let ts = (i as i64 + 1) * 1_000_000_000;
            let point = Point::new(key.clone(), crate::fields! { "value" => v }, ts).unwrap();
            db.insert(&point).unwrap();
        }
    }

    #[tokio::test]
    async fn window_row_number() {
        let (db, _dir) = open_test_db();
        insert_series(&db, "wf", "a", &[10.0, 20.0, 30.0, 40.0, 50.0]).await;

        let ctx = create_session_context(db);
        let df = ctx
            .sql(
                "SELECT value, ROW_NUMBER() OVER (ORDER BY _time) AS rn \
                 FROM wf ORDER BY rn",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 5);

        // Verify sequential numbering via SQL
        let df2 = ctx
            .sql(
                "SELECT COUNT(*) AS cnt FROM (\
                   SELECT ROW_NUMBER() OVER (ORDER BY _time) AS rn FROM wf\
                 ) WHERE rn BETWEEN 1 AND 5",
            )
            .await
            .unwrap();
        let batches2 = df2.collect().await.unwrap();
        let total: usize = batches2.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1); // one aggregation row
    }

    #[tokio::test]
    async fn window_lag_lead() {
        let (db, _dir) = open_test_db();
        insert_series(&db, "wlag", "a", &[100.0, 200.0, 300.0]).await;

        let ctx = create_session_context(db);
        let df = ctx
            .sql(
                "SELECT value, \
                        LAG(value, 1) OVER (ORDER BY _time) AS prev_val, \
                        LEAD(value, 1) OVER (ORDER BY _time) AS next_val \
                 FROM wlag ORDER BY _time",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3);

        // Flatten all batches into one for easier access
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();

        // LAG: first row should be NULL, second should be 100.0
        let prev = batch
            .column_by_name("prev_val")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(prev.is_null(0));
        assert!((prev.value(1) - 100.0).abs() < f64::EPSILON);

        // LEAD: last row should be NULL, first should be 200.0
        let nxt = batch
            .column_by_name("next_val")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((nxt.value(0) - 200.0).abs() < f64::EPSILON);
        assert!(nxt.is_null(2));
    }

    #[tokio::test]
    async fn window_rank_dense_rank() {
        let (db, _dir) = open_test_db();
        // Insert duplicate values to test ranking
        let key = SeriesKey::new("wrank", crate::tags! { "host" => "a" }).unwrap();
        for (i, &v) in [10.0, 20.0, 20.0, 30.0].iter().enumerate() {
            let ts = (i as i64 + 1) * 1_000_000_000;
            let point = Point::new(key.clone(), crate::fields! { "value" => v }, ts).unwrap();
            db.insert(&point).unwrap();
        }

        let ctx = create_session_context(db);
        let df = ctx
            .sql(
                "SELECT value, \
                        RANK() OVER (ORDER BY value) AS rnk, \
                        DENSE_RANK() OVER (ORDER BY value) AS drnk \
                 FROM wrank ORDER BY value",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();

        assert_eq!(batch.num_rows(), 4);

        let rnk = batch
            .column_by_name("rnk")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        // RANK: 1, 2, 2, 4
        assert_eq!(rnk.value(0), 1);
        assert_eq!(rnk.value(1), 2);
        assert_eq!(rnk.value(2), 2);
        assert_eq!(rnk.value(3), 4);

        let drnk = batch
            .column_by_name("drnk")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        // DENSE_RANK: 1, 2, 2, 3
        assert_eq!(drnk.value(0), 1);
        assert_eq!(drnk.value(1), 2);
        assert_eq!(drnk.value(2), 2);
        assert_eq!(drnk.value(3), 3);
    }

    #[tokio::test]
    async fn window_rolling_average() {
        let (db, _dir) = open_test_db();
        insert_series(&db, "wroll", "a", &[10.0, 20.0, 30.0, 40.0, 50.0]).await;

        let ctx = create_session_context(db);
        let df = ctx
            .sql(
                "SELECT value, \
                        AVG(value) OVER (\
                          ORDER BY _time \
                          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW\
                        ) AS rolling_avg \
                 FROM wroll ORDER BY _time",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 5);

        let avg = batch
            .column_by_name("rolling_avg")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        // Row 0: avg(10) = 10
        assert!((avg.value(0) - 10.0).abs() < f64::EPSILON);
        // Row 1: avg(10,20) = 15
        assert!((avg.value(1) - 15.0).abs() < f64::EPSILON);
        // Row 2: avg(10,20,30) = 20
        assert!((avg.value(2) - 20.0).abs() < f64::EPSILON);
        // Row 3: avg(20,30,40) = 30
        assert!((avg.value(3) - 30.0).abs() < f64::EPSILON);
        // Row 4: avg(30,40,50) = 40
        assert!((avg.value(4) - 40.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn window_first_last_nth_value() {
        let (db, _dir) = open_test_db();
        insert_series(&db, "wflv", "a", &[100.0, 200.0, 300.0, 400.0]).await;

        let ctx = create_session_context(db);
        let df = ctx
            .sql(
                "SELECT value, \
                        FIRST_VALUE(value) OVER (ORDER BY _time) AS fv, \
                        LAST_VALUE(value) OVER (\
                          ORDER BY _time \
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
                        ) AS lv, \
                        NTH_VALUE(value, 2) OVER (ORDER BY _time) AS nv2 \
                 FROM wflv ORDER BY _time",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 4);

        let fv = batch
            .column_by_name("fv")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // FIRST_VALUE is always 100.0
        assert!((fv.value(0) - 100.0).abs() < f64::EPSILON);
        assert!((fv.value(3) - 100.0).abs() < f64::EPSILON);

        let lv = batch
            .column_by_name("lv")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // LAST_VALUE over unbounded frame is always 400.0
        assert!((lv.value(0) - 400.0).abs() < f64::EPSILON);
        assert!((lv.value(3) - 400.0).abs() < f64::EPSILON);

        let nv2 = batch
            .column_by_name("nv2")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // NTH_VALUE(v, 2) is NULL for row 0, then 200.0 for the rest
        assert!(nv2.is_null(0));
        assert!((nv2.value(1) - 200.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn window_partition_by() {
        let (db, _dir) = open_test_db();
        // Two hosts with different data
        insert_series(&db, "wpart", "srv1", &[10.0, 20.0, 30.0]).await;
        insert_series(&db, "wpart", "srv2", &[100.0, 200.0, 300.0]).await;

        let ctx = create_session_context(db);
        let df = ctx
            .sql(
                "SELECT host, value, \
                        ROW_NUMBER() OVER (PARTITION BY host ORDER BY _time) AS rn, \
                        SUM(value) OVER (PARTITION BY host ORDER BY _time) AS running_sum \
                 FROM wpart ORDER BY host, _time",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 6);

        let rn = batch
            .column_by_name("rn")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        // ROW_NUMBER resets per partition: 1,2,3,1,2,3
        assert_eq!(rn.value(0), 1);
        assert_eq!(rn.value(2), 3);
        assert_eq!(rn.value(3), 1);
        assert_eq!(rn.value(5), 3);

        let running = batch
            .column_by_name("running_sum")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // srv1: 10, 30, 60 | srv2: 100, 300, 600
        assert!((running.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((running.value(2) - 60.0).abs() < f64::EPSILON);
        assert!((running.value(3) - 100.0).abs() < f64::EPSILON);
        assert!((running.value(5) - 600.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn window_ntile() {
        let (db, _dir) = open_test_db();
        insert_series(&db, "wntile", "a", &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).await;

        let ctx = create_session_context(db);
        let df = ctx
            .sql("SELECT value, NTILE(3) OVER (ORDER BY _time) AS tile FROM wntile ORDER BY _time")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 6);

        let tile = batch
            .column_by_name("tile")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        // 6 rows / 3 tiles → 2 per tile: 1,1,2,2,3,3
        assert_eq!(tile.value(0), 1);
        assert_eq!(tile.value(1), 1);
        assert_eq!(tile.value(2), 2);
        assert_eq!(tile.value(3), 2);
        assert_eq!(tile.value(4), 3);
        assert_eq!(tile.value(5), 3);
    }

    #[tokio::test]
    async fn runtime_udf_is_available_in_sql() {
        use arrow::array::Float64Array;
        use arrow::datatypes::DataType;
        use datafusion::logical_expr::create_udf;
        use datafusion::logical_expr::{ColumnarValue, Volatility};

        let (db, _dir) = open_test_db();

        // Register a simple "double_it(x) -> x * 2" scalar UDF at runtime
        let double_it = create_udf(
            "double_it",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| match &args[0] {
                ColumnarValue::Array(arr) => {
                    let input = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                    let output: Float64Array = input.iter().map(|v| v.map(|x| x * 2.0)).collect();
                    Ok(ColumnarValue::Array(Arc::new(output)))
                }
                ColumnarValue::Scalar(s) => {
                    let v = match s {
                        datafusion::common::ScalarValue::Float64(Some(x)) => x * 2.0,
                        _ => {
                            return Err(datafusion::common::DataFusionError::Internal(
                                "expected f64".into(),
                            ))
                        }
                    };
                    Ok(ColumnarValue::Scalar(
                        datafusion::common::ScalarValue::Float64(Some(v)),
                    ))
                }
            }),
        );
        db.register_udf(Arc::new(double_it));

        // Insert test data
        let key = SeriesKey::new("metrics", crate::tags! { "t" => "v" }).unwrap();
        for i in 1..=3 {
            let point = Point::new(
                key.clone(),
                crate::fields! { "val" => i as f64 },
                i * 1_000_000_000_i64,
            )
            .unwrap();
            db.insert(&point).unwrap();
        }

        // Query using the runtime UDF
        let ctx = create_session_context(db);
        let df = ctx
            .sql("SELECT double_it(val) AS doubled FROM metrics ORDER BY _time")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column_by_name("doubled")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(col.len(), 3);
        assert!((col.value(0) - 2.0).abs() < f64::EPSILON);
        assert!((col.value(1) - 4.0).abs() < f64::EPSILON);
        assert!((col.value(2) - 6.0).abs() < f64::EPSILON);
    }

    // ── Phase 0: RuntimeEnv configuration tests ─────────────────────

    #[tokio::test]
    async fn runtime_env_has_memory_pool() {
        let (db, _dir) = open_test_db();
        let ctx = create_session_context(db);
        let state = ctx.state();
        let runtime = state.runtime_env();
        // FairSpillPool is configured — a reservation should succeed for
        // a small amount, proving the pool is active.
        let consumer = datafusion::execution::memory_pool::MemoryConsumer::new("test");
        let reservation = consumer.register(&runtime.memory_pool);
        reservation.try_grow(1024).unwrap();
        assert_eq!(reservation.size(), 1024);
        // And the pool tracks it
        assert!(runtime.memory_pool.reserved() >= 1024);
    }

    #[tokio::test]
    async fn runtime_env_target_partitions_matches_cpus() {
        let (db, _dir) = open_test_db();
        let ctx = create_session_context(db);
        let expected = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        assert_eq!(
            ctx.state().config().target_partitions(),
            expected,
            "target_partitions should match available parallelism"
        );
    }

    #[tokio::test]
    async fn spill_dir_created_on_query() {
        // Verify that a GROUP BY query with spill-to-disk configured
        // still produces correct results (even if no actual spill occurs
        // during this small test).
        let (db, _dir) = open_test_db();
        let key = SeriesKey::new("spill_test", crate::tags! { "t" => "v" }).unwrap();
        for i in 0..50 {
            let point = Point::new(
                key.clone(),
                crate::fields! { "val" => i as f64 },
                (i + 1) * 1_000_000_000_i64,
            )
            .unwrap();
            db.insert(&point).unwrap();
        }

        let ctx = create_session_context(db);
        let df = ctx
            .sql("SELECT t, COUNT(*) AS cnt FROM spill_test GROUP BY t")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 1);
    }

    // ── Phase 1: Streaming chunked output tests ─────────────────────

    #[tokio::test]
    async fn streaming_scan_returns_all_rows() {
        let (db, _dir) = open_test_db();
        let key = SeriesKey::new("stream_test", crate::tags! { "host" => "s1" }).unwrap();
        for i in 0..100 {
            let point = Point::new(
                key.clone(),
                crate::fields! { "usage" => i as f64 },
                (i + 1) * 1_000_000_i64,
            )
            .unwrap();
            db.insert(&point).unwrap();
        }

        let ctx = create_session_context(db);
        let df = ctx
            .sql("SELECT * FROM stream_test ORDER BY _time")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 100);
    }

    #[tokio::test]
    async fn streaming_scan_with_limit() {
        let (db, _dir) = open_test_db();
        let key = SeriesKey::new("lim_test", crate::tags! { "host" => "s1" }).unwrap();
        for i in 0..200 {
            let point = Point::new(
                key.clone(),
                crate::fields! { "val" => i as f64 },
                (i + 1) * 1_000_000_i64,
            )
            .unwrap();
            db.insert(&point).unwrap();
        }

        let ctx = create_session_context(db);
        let df = ctx.sql("SELECT val FROM lim_test LIMIT 10").await.unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 10);
    }

    #[tokio::test]
    async fn streaming_scan_with_aggregation() {
        let (db, _dir) = open_test_db();
        for host in &["a", "b", "c"] {
            let key = SeriesKey::new("agg_stream", crate::tags! { "host" => *host }).unwrap();
            for i in 0..10 {
                let point = Point::new(
                    key.clone(),
                    crate::fields! { "val" => 1.0_f64 },
                    (i + 1) * 1_000_000_000_i64,
                )
                .unwrap();
                db.insert(&point).unwrap();
            }
        }

        let ctx = create_session_context(db);
        let df = ctx
            .sql("SELECT host, SUM(val) AS total FROM agg_stream GROUP BY host ORDER BY host")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let batch = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let totals = batch
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        for i in 0..3 {
            assert!((totals.value(i) - 10.0).abs() < f64::EPSILON);
        }
    }
}
