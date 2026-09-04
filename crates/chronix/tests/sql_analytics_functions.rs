#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! The analytics SQL surface: window functions and forecast aggregates.
//!
//! Written against the two properties a per-batch implementation cannot hold:
//! the same logical data laid out over a different number of segments must
//! give the same answer, and two series in one measurement must never share a
//! window.

use std::sync::Arc;

use arrow::array::Array;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use datafusion::prelude::SessionContext;

/// Write `hosts × per_host` points, split across `flushes` segments.
fn db_with(hosts: &[&str], per_host: usize, flushes: usize) -> (tempfile::TempDir, Arc<Chronix>) {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let chunk = per_host.div_ceil(flushes.max(1));
    for f in 0..flushes.max(1) {
        let mut points = Vec::new();
        for (h, host) in hosts.iter().enumerate() {
            let key = SeriesKey::new("m", tags! { "host" => *host }).unwrap();
            for i in (f * chunk)..((f + 1) * chunk).min(per_host) {
                // Each host has its own level, so mixing them is visible.
                let v = (h as f64) * 1000.0 + i as f64;
                points.push(
                    Point::new(key.clone(), fields! { "v" => v }, i as i64 * 1_000_000_000)
                        .unwrap(),
                );
            }
        }
        let _ = db.insert_batch(&points).unwrap();
        db.flush().unwrap();
    }
    (dir, db)
}

async fn f64s(ctx: &SessionContext, q: &str) -> Vec<Option<f64>> {
    let batches = ctx.sql(q).await.unwrap().collect().await.unwrap();
    batches
        .iter()
        .flat_map(|b| {
            let c = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            (0..c.len())
                .map(|i| (!c.is_null(i)).then(|| c.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

// ── The regressions ─────────────────────────────────────────────────────

/// The defect that motivated the redesign: identical data in four segments
/// instead of one produced four forecasts instead of one.
#[tokio::test]
async fn forecast_does_not_depend_on_segment_layout() {
    let mut answers = Vec::new();
    for flushes in [1usize, 4, 7] {
        let (_dir, db) = db_with(&["a"], 210, flushes);
        let ctx = chronix::sql::create_session_context(db);
        let got = f64s(
            &ctx,
            "SELECT unnest(forecast(v, _time, 3)) FROM m GROUP BY host",
        )
        .await;
        assert_eq!(
            got.len(),
            3,
            "{flushes} flushes produced {} values",
            got.len()
        );
        answers.push(got);
    }
    assert_eq!(answers[0], answers[1], "1 vs 4 segments");
    assert_eq!(answers[1], answers[2], "4 vs 7 segments");
}

/// A window function's partition is the series, so two hosts never share a
/// difference. As a scalar UDF this returned the 1000-unit jump between the
/// two hosts' levels as an ordinary sample-to-sample difference.
#[tokio::test]
async fn diff_does_not_cross_series() {
    let (_dir, db) = db_with(&["a", "b"], 50, 3);
    let ctx = chronix::sql::create_session_context(db);
    let diffs = f64s(
        &ctx,
        "SELECT diff(v, 1) OVER (PARTITION BY host ORDER BY _time) FROM m",
    )
    .await;
    assert_eq!(diffs.len(), 100);
    // One NULL per host — the first row of each partition — and every other
    // difference is exactly 1.
    assert_eq!(diffs.iter().filter(|d| d.is_none()).count(), 2);
    for d in diffs.into_iter().flatten() {
        assert!((d - 1.0).abs() < 1e-9, "difference crossed a series: {d}");
    }
}

/// `zscore` is defined against the partition, so each host is standardised
/// against its own distribution, not against the union of both.
#[tokio::test]
async fn zscore_is_scoped_to_its_partition() {
    let (_dir, db) = db_with(&["a", "b"], 40, 2);
    let ctx = chronix::sql::create_session_context(db);
    let z = f64s(&ctx, "SELECT zscore(v) OVER (PARTITION BY host) FROM m").await;
    assert_eq!(z.len(), 80);
    // Both hosts hold the same shape at different levels, so every z-score is
    // in the same narrow range. Standardising across both hosts together would
    // put half the rows near -1 and half near +1.
    let max = z.iter().flatten().fold(f64::MIN, |a, b| a.max(*b));
    let min = z.iter().flatten().fold(f64::MAX, |a, b| a.min(*b));
    assert!(max < 2.0 && min > -2.0, "z-score range [{min}, {max}]");
}

// ── Window function behaviour ───────────────────────────────────────────

#[tokio::test]
async fn rolling_functions_null_their_warm_up() {
    let (_dir, db) = db_with(&["a"], 20, 1);
    let ctx = chronix::sql::create_session_context(db);

    let means = f64s(
        &ctx,
        "SELECT rolling_mean(v, 5) OVER (PARTITION BY host ORDER BY _time) FROM m",
    )
    .await;
    assert_eq!(means.len(), 20);
    assert!(means[..4].iter().all(Option::is_none), "warm-up not NULL");
    // v = 0..19, so the mean of rows 0..=4 is 2.
    assert!((means[4].unwrap() - 2.0).abs() < 1e-9);
    assert!((means[19].unwrap() - 17.0).abs() < 1e-9);

    let stds = f64s(
        &ctx,
        "SELECT rolling_std(v, 5) OVER (PARTITION BY host ORDER BY _time) FROM m",
    )
    .await;
    assert!(stds[..4].iter().all(Option::is_none));
    // A window of five consecutive integers has sample sd sqrt(2.5).
    assert!((stds[4].unwrap() - 2.5_f64.sqrt()).abs() < 1e-9);
}

#[tokio::test]
async fn pct_change_and_ewm_follow_the_order_by() {
    let (_dir, db) = db_with(&["a"], 10, 2);
    let ctx = chronix::sql::create_session_context(db);

    let pct = f64s(
        &ctx,
        "SELECT pct_change(v) OVER (PARTITION BY host ORDER BY _time) FROM m",
    )
    .await;
    // v = 0..9: the first row has no predecessor and the second divides by
    // zero, so both are NULL; the rest are 1/(i-1).
    assert!(pct[0].is_none() && pct[1].is_none());
    assert!((pct[2].unwrap() - 1.0).abs() < 1e-9);
    assert!((pct[9].unwrap() - 1.0 / 8.0).abs() < 1e-9);

    let ewm = f64s(
        &ctx,
        "SELECT ewm(v, 1.0) OVER (PARTITION BY host ORDER BY _time) FROM m",
    )
    .await;
    // alpha = 1 keeps only the latest observation.
    for (i, v) in ewm.iter().enumerate() {
        assert!((v.unwrap() - i as f64).abs() < 1e-9);
    }
}

#[tokio::test]
async fn stl_decompose_returns_all_three_components() {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let points: Vec<Point> = (0..96)
        .map(|i| {
            let v = 10.0
                + 5.0 * (2.0 * std::f64::consts::PI * f64::from(i % 24) / 24.0).sin()
                + f64::from(i) * 0.01;
            Point::new(
                key.clone(),
                fields! { "v" => v },
                i64::from(i) * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    let ctx = chronix::sql::create_session_context(db);

    let batches = ctx
        .sql(
            "SELECT d.trend, d.seasonal, d.residual FROM (
               SELECT stl_decompose(v, 24) OVER (PARTITION BY host ORDER BY _time) AS d FROM m
             )",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 96);
    assert_eq!(batches[0].num_columns(), 3);

    // trend + seasonal + residual must reconstruct the input exactly.
    let sums = f64s(
        &ctx,
        "SELECT d.trend + d.seasonal + d.residual - v FROM (
           SELECT v, stl_decompose(v, 24) OVER (PARTITION BY host ORDER BY _time) AS d FROM m
         )",
    )
    .await;
    for s in sums.into_iter().flatten() {
        assert!(
            s.abs() < 1e-6,
            "components do not reconstruct the series: {s}"
        );
    }
}

#[tokio::test]
async fn correlation_summarises_its_partition() {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let points: Vec<Point> = (0..30)
        .map(|i| {
            Point::new(
                key.clone(),
                fields! { "a" => f64::from(i), "b" => f64::from(i) * 2.0 + 1.0 },
                i64::from(i) * 1_000_000_000,
            )
            .unwrap()
        })
        .collect();
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    let ctx = chronix::sql::create_session_context(db);

    let r = f64s(
        &ctx,
        "SELECT correlation(a, b) OVER (PARTITION BY host ORDER BY _time) FROM m",
    )
    .await;
    assert_eq!(r.len(), 30);
    for v in r.into_iter().flatten() {
        assert!(
            (v - 1.0).abs() < 1e-9,
            "perfectly correlated columns gave {v}"
        );
    }
}

// ── Errors the surface should give ──────────────────────────────────────

/// Calling a window function without `OVER` is a planning error, not a wrong
/// answer. That is the whole point of the shape change.
#[tokio::test]
async fn window_functions_require_an_over_clause() {
    let (_dir, db) = db_with(&["a"], 10, 1);
    let ctx = chronix::sql::create_session_context(db);
    let err = ctx.sql("SELECT diff(v, 1) FROM m").await.err();
    assert!(err.is_some(), "diff() without OVER planned successfully");
}

#[tokio::test]
async fn invalid_arguments_are_rejected() {
    let (_dir, db) = db_with(&["a"], 20, 1);
    let ctx = chronix::sql::create_session_context(db);

    for q in [
        "SELECT rolling_mean(v, 0) OVER (PARTITION BY host ORDER BY _time) FROM m",
        "SELECT rolling_mean(v, -3) OVER (PARTITION BY host ORDER BY _time) FROM m",
        "SELECT ewm(v, 0.0) OVER (PARTITION BY host ORDER BY _time) FROM m",
        "SELECT ewm(v, 2.0) OVER (PARTITION BY host ORDER BY _time) FROM m",
    ] {
        let outcome = match ctx.sql(q).await {
            Err(_) => Err(()),
            Ok(df) => df.collect().await.map(|_| ()).map_err(|_| ()),
        };
        assert!(outcome.is_err(), "accepted an invalid argument: {q}");
    }

    let err = ctx
        .sql("SELECT forecast(v, _time, 0) FROM m GROUP BY host")
        .await;
    let outcome = match err {
        Err(_) => Err(()),
        Ok(df) => df.collect().await.map(|_| ()).map_err(|_| ()),
    };
    assert!(outcome.is_err(), "accepted a zero horizon");
}

// ── Aggregates ──────────────────────────────────────────────────────────

#[tokio::test]
async fn forecast_is_grouped_per_series() {
    let (_dir, db) = db_with(&["a", "b"], 60, 3);
    let ctx = chronix::sql::create_session_context(db);
    let batches = ctx
        .sql("SELECT host, unnest(forecast(v, _time, 4)) AS p FROM m GROUP BY host ORDER BY host")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 8, "two hosts × horizon 4");

    // Host "a" sits around 0..59 and host "b" around 1000..1059, so each
    // forecast must land near its own host's level.
    for b in &batches {
        let hosts = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let preds = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            let expected = if hosts.value(i) == "a" { 59.0 } else { 1059.0 };
            assert!(
                (preds.value(i) - expected).abs() < 30.0,
                "host {} forecast {} is not near {expected}",
                hosts.value(i),
                preds.value(i)
            );
        }
    }
}

/// The aggregate sorts by the timestamp column it is given, so a plan that
/// delivers rows out of order still forecasts the series in time order.
#[tokio::test]
async fn forecast_orders_by_its_timestamp_argument() {
    let (_dir, db) = db_with(&["a"], 60, 4);
    let ctx = chronix::sql::create_session_context(db);
    let ordered = f64s(
        &ctx,
        "SELECT unnest(forecast(v, _time, 2)) FROM m GROUP BY host",
    )
    .await;
    let shuffled = f64s(
        &ctx,
        "SELECT unnest(forecast(v, _time, 2)) FROM (SELECT * FROM m ORDER BY v DESC) GROUP BY host",
    )
    .await;
    assert_eq!(ordered, shuffled, "row delivery order changed the forecast");
}

/// `auto_forecast` runs the cross-validated selector from SQL: on a clean
/// seasonal series it must beat the fixed-model `forecast` (SES) by a wide
/// margin, and — like `forecast` — its answer must not depend on how many
/// segments the data was flushed into.
#[tokio::test]
async fn auto_forecast_selects_a_better_model_than_ses_and_is_layout_independent() {
    use arrow::array::{Array, ListArray};

    let period = 12usize;
    let truth =
        |i: usize| 100.0 + (2.0 * std::f64::consts::PI * i as f64 / period as f64).sin() * 20.0;
    let horizon = 12usize;

    let mut answers = Vec::new();
    for flushes in [1usize, 4] {
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
        let key = SeriesKey::new("s", tags! { "h" => "a" }).unwrap();
        let n = 240usize;
        for chunk in (0..n).collect::<Vec<_>>().chunks(n / flushes) {
            let pts: Vec<Point> = chunk
                .iter()
                .map(|&i| {
                    Point::new(
                        key.clone(),
                        fields! { "v" => truth(i) },
                        (i as i64) * 60_000_000_000,
                    )
                    .unwrap()
                })
                .collect();
            assert!(db.insert_batch(&pts).unwrap().is_complete());
            db.flush().unwrap();
        }
        let ctx = chronix::sql::create_session_context(db.clone());
        let extract = |batches: Vec<arrow::record_batch::RecordBatch>| -> Vec<f64> {
            let list = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let vals = list.value(0);
            let f = vals
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            (0..f.len()).map(|i| f.value(i)).collect()
        };
        let auto = extract(
            ctx.sql(&format!("SELECT auto_forecast(v, _time, {horizon}) FROM s"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap(),
        );
        let ses = extract(
            ctx.sql(&format!("SELECT forecast(v, _time, {horizon}) FROM s"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap(),
        );
        let err = |f: &[f64]| -> f64 {
            f.iter()
                .enumerate()
                .map(|(h, v)| (v - truth(n + h)).abs())
                .sum::<f64>()
                / f.len() as f64
        };
        assert!(
            err(&auto) * 4.0 < err(&ses),
            "auto {:.2} should beat SES {:.2} on a seasonal series",
            err(&auto),
            err(&ses)
        );
        answers.push(auto);
    }
    assert_eq!(answers[0].len(), horizon);
    for (a, b) in answers[0].iter().zip(&answers[1]) {
        assert!(
            (a - b).abs() < 1e-9,
            "the forecast depends on segment layout: {a} vs {b}"
        );
    }
}
