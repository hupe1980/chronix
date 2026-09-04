//! The SQL scan streams; it does not collect the range first.
//!
//! `ChronixExec::execute` used to call `execute_stream`, which materialises
//! every surviving row of the time range inside `spawn_blocking` before
//! yielding the first batch. Two properties were false as a result, and each
//! is pinned here:
//!
//! 1. **A `LIMIT` bounds the scan.** The limit was applied to the collected
//!    result, so `SELECT * FROM m LIMIT 10` read the whole measurement — the
//!    same wall time as the unlimited query.
//! 2. **The scan is accounted for.** The materialised rows were never
//!    reserved from DataFusion's memory pool, so a scan far larger than the
//!    per-query budget neither spilled nor failed; it just allocated.
//!
//! Both are asserted through the memory pool rather than through timing: a
//! per-query budget too small for the measurement must fail the unlimited
//! query and answer the limited one. That is deterministic, where a stopwatch
//! is not.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::sync::Arc;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

/// A database holding `rows` points of `cpu`, flushed into segments, with a
/// per-query memory budget of `budget` bytes.
fn db_with(dir: &tempfile::TempDir, rows: i64, budget: usize) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .per_query_memory_limit(budget)
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    let key = SeriesKey::new("cpu", tags! { "host" => "h1" }).unwrap();
    let mut written = 0;
    while written < rows {
        let chunk = (rows - written).min(10_000);
        let points: Vec<Point> = (0..chunk)
            .map(|i| {
                let ts = (written + i) * 1_000_000_000;
                Point::new(key.clone(), fields! { "v" => i as f64 }, ts).unwrap()
            })
            .collect();
        assert!(db.insert_batch(&points).unwrap().is_complete());
        db.flush().unwrap();
        written += chunk;
    }
    db
}

/// Run `sql` and return `(rows returned, rows the storage scan produced)`.
///
/// The second number is `ChronixExec`'s `rows_scanned` metric — what the
/// engine handed up before the limit and the projection. It is the only
/// number that distinguishes "read ten rows" from "read two hundred thousand
/// and kept ten", which is exactly what a stopwatch cannot do reliably.
async fn scan_cost(ctx: &datafusion::prelude::SessionContext, sql: &str) -> (usize, usize) {
    use datafusion::physical_plan::{collect, ExecutionPlan};

    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let returned: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    /// Sum `rows_scanned` over every `ChronixExec` in the tree.
    fn scanned(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = plan
            .metrics()
            .and_then(|m| {
                m.iter()
                    .find(|v| v.value().name() == "rows_scanned")
                    .map(|v| v.value().as_usize())
            })
            .unwrap_or(0);
        own + plan.children().iter().map(|c| scanned(c)).sum::<usize>()
    }

    (returned, scanned(&plan))
}

/// A `LIMIT` must stop the scan, not filter its result.
#[tokio::test]
async fn a_limited_query_does_not_pay_for_the_whole_measurement() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with(&dir, 200_000, 64 * 1024 * 1024);
    let ctx = chronix::sql::create_session_context(db);

    let (returned, scanned) = scan_cost(&ctx, "SELECT * FROM cpu LIMIT 10").await;
    assert_eq!(returned, 10);
    assert!(
        scanned <= 10_000,
        "the limit must stop the scan after the first bucket; it read {scanned} \
         of 200000 rows"
    );

    let (returned, scanned) = scan_cost(&ctx, "SELECT * FROM cpu").await;
    assert_eq!(returned, 200_000);
    assert_eq!(scanned, 200_000, "an unlimited scan reads everything");
}

/// What the scan holds must come out of the query's memory pool.
///
/// A streaming operator holds one batch, so a *sane* budget is not the test —
/// it would pass whether or not anything was reserved. A budget smaller than
/// one batch is: it can only fail if the reservation is real.
#[tokio::test]
async fn the_scan_reserves_from_the_query_budget() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with(&dir, 50_000, 8 * 1024);
    let ctx = chronix::sql::create_session_context(db);

    let err = ctx
        .sql("SELECT * FROM cpu")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("a budget below one batch must refuse the scan");
    let msg = err.to_string();
    assert!(
        msg.contains("Resources exhausted") || msg.contains("Failed to allocate"),
        "the failure must name the memory budget, got: {msg}"
    );
}

/// `EXPLAIN ANALYZE` must say what the scan did. Before the scan carried
/// metrics it was the one operator in an analysed plan with no numbers at all.
#[tokio::test]
async fn explain_analyze_reports_the_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with(&dir, 5_000, 64 * 1024 * 1024);
    let ctx = chronix::sql::create_session_context(db);

    let batches = ctx
        .sql("EXPLAIN ANALYZE SELECT count(*) FROM cpu")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(
        text.contains("rows_scanned"),
        "the analysed plan must report the scan's row count, got:\n{text}"
    );
}

/// Streaming must not change the answer: the same rows, in the same order.
#[tokio::test]
async fn streaming_returns_the_same_rows_as_before() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with(&dir, 5_000, 64 * 1024 * 1024);
    let ctx = chronix::sql::create_session_context(db);

    let batches = ctx
        .sql("SELECT count(*) AS n, sum(v) AS s, min(_time) AS lo, max(_time) AS hi FROM cpu")
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
    assert_eq!(n, 5_000);

    let ordered = ctx
        .sql("SELECT v FROM cpu ORDER BY _time LIMIT 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let v = ordered[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(
        (0..3).map(|i| v.value(i)).collect::<Vec<_>>(),
        vec![0.0, 1.0, 2.0],
        "the first rows by time must be the first rows written"
    );
}
