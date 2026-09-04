//! `rate()` must not depend on how DataFusion partitions the scan.
//!
//! DataFusion inserts `RepartitionExec: RoundRobinBatch(N)` below a partial
//! aggregate. With more than `N` scan batches a partition receives batches
//! 0, N, 2N… — a run that spans the whole time range with holes in it — and
//! every other partition's run overlaps it. `rate` folded those runs by
//! concatenating them in `first_time` order, which reads the second run's
//! first value as a counter reset.
//!
//! The failure is not subtle: on a perfect 1 sample/s counter that never
//! resets, `rate()` returned **13.5**.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::sync::Arc;

use arrow::array::Array;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

/// A counter incrementing by 1 every second, written in `flushes` segments so
/// the scan produces at least that many batches.
fn counter_db(dir: &tempfile::TempDir, flushes: i64, per_flush: i64) -> Arc<Chronix> {
    let db = Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let key = SeriesKey::new("ctr", tags! { "host" => "h" }).unwrap();
    for f in 0..flushes {
        let points: Vec<Point> = (0..per_flush)
            .map(|i| {
                let idx = f * per_flush + i;
                Point::new(
                    key.clone(),
                    fields! { "v" => idx as f64 },
                    idx * 1_000_000_000,
                )
                .unwrap()
            })
            .collect();
        assert!(db.insert_batch(&points).unwrap().is_complete());
        db.flush().unwrap();
    }
    db
}

async fn scalar(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Option<f64> {
    let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let col = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    (!col.is_null(0)).then(|| col.value(0))
}

/// The whole point: many batches, one group, no resets, answer must be 1.0/s.
#[tokio::test]
async fn rate_is_independent_of_scan_partitioning() {
    let dir = tempfile::tempdir().unwrap();
    // 40 flushes comfortably exceeds any plausible `target_partitions`, so at
    // least one partition receives non-adjacent batches.
    let db = counter_db(&dir, 40, 500);
    let ctx = chronix::sql::create_session_context(db);

    let r = scalar(&ctx, "SELECT rate(v, _time) AS r FROM ctr")
        .await
        .expect("rate over 20000 samples must be defined");
    assert!(
        (r - 1.0).abs() < 1e-9,
        "a 1/s counter that never resets must rate at 1.0/s, got {r}"
    );

    // Grouped, which adds a hash repartition above the round-robin one.
    let r = scalar(
        &ctx,
        "SELECT rate(v, _time) AS r FROM ctr GROUP BY host ORDER BY host",
    )
    .await
    .unwrap();
    assert!(
        (r - 1.0).abs() < 1e-9,
        "grouped rate must also be 1.0, got {r}"
    );
}

/// `irate` keeps the two globally latest samples, so it was already
/// partition-independent. Pinned so it stays that way.
#[tokio::test]
async fn irate_is_independent_of_scan_partitioning() {
    let dir = tempfile::tempdir().unwrap();
    let db = counter_db(&dir, 40, 500);
    let ctx = chronix::sql::create_session_context(db);

    let r = scalar(&ctx, "SELECT irate(v, _time) AS r FROM ctr")
        .await
        .unwrap();
    assert!((r - 1.0).abs() < 1e-9, "irate must be 1.0/s, got {r}");
}

/// A real counter reset must still be read as a restart, not as a negative
/// rate — the rule the ordering exists to serve.
#[tokio::test]
async fn a_counter_reset_is_still_detected() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let key = SeriesKey::new("ctr", tags! { "host" => "h" }).unwrap();

    // 0,1,2,3,4 then a restart at 0,1,2,3,4 — total increase 4 + 0 + 4 = 8
    // over 9 seconds.
    let values: Vec<f64> = (0..5).chain(0..5).map(f64::from).collect();
    let points: Vec<Point> = values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            Point::new(key.clone(), fields! { "v" => *v }, i as i64 * 1_000_000_000).unwrap()
        })
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    db.flush().unwrap();

    let ctx = chronix::sql::create_session_context(db);
    let r = scalar(&ctx, "SELECT rate(v, _time) AS r FROM ctr")
        .await
        .unwrap();
    let expected = 8.0 / 9.0;
    assert!(
        (r - expected).abs() < 1e-9,
        "a restart contributes its new value as the increase: expected {expected}, got {r}"
    );
}

/// Fewer than two samples has no rate, and neither does a single instant.
#[tokio::test]
async fn a_degenerate_series_has_no_rate() {
    let dir = tempfile::tempdir().unwrap();
    let db = counter_db(&dir, 1, 1);
    let ctx = chronix::sql::create_session_context(db);
    assert_eq!(
        scalar(&ctx, "SELECT rate(v, _time) AS r FROM ctr").await,
        None,
        "one sample cannot have a rate"
    );
}
