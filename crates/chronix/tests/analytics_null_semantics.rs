#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! What a NULL means to an analytics window function.
//!
//! A NULL is a **missing sample, not a missing row**. It is excluded from
//! every statistic, and a row still gets an answer wherever that answer can
//! be computed from samples that do exist. Two consequences, and the split
//! between them is the whole design:
//!
//! - A function of the *window* — `rolling_mean`, `rolling_std`,
//!   `rolling_corr`, `ewm`, `correlation` — answers at a gap row too, from
//!   the real samples around it. `AVG(v) OVER (ROWS n PRECEDING)` beside it
//!   in the same `SELECT` does exactly this.
//! - A function of *that row's own value* — `diff`, `pct_change`, `zscore`,
//!   `anomaly_score`, `multivariate_anomaly`, the STL components — is NULL
//!   there, because the only way to produce a number would be to invent the
//!   sample.
//!
//! Before this was one rule it was four. `rolling_mean`, `rolling_std` and
//! `rolling_corr` folded the NaN into a sliding accumulator, so one NULL made
//! every later row of the partition NULL — until an exact recompute every
//! 1024 steps silently healed it, and a NULL at row 5 broke 1020 rows while
//! one at row 1500 broke 549. `ewm` did the same with no heal at all.
//! `zscore` summed the whole partition and returned NULL for all of it.
//! `correlation` deleted pairs. The STL kernels *removed* the row, which
//! moves every row after it into the previous season.
//!
//! The check that matters here is the last test: every window function the
//! session registers is named, and a new one cannot be added without saying
//! what it answers across a gap.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::Array;

use chronix::prelude::*;
use chronix::{Chronix, fields, tags};
use datafusion::prelude::SessionContext;

/// 120 rows for one host, with `v` missing at rows 7, 8 and 60 and `w`
/// present everywhere. A row exists at every timestamp; only the sample is
/// gone, which is the shape a device that skipped a reading produces.
const GAPS: [usize; 3] = [7, 8, 60];
const ROWS: usize = 120;

fn db_with_gaps() -> (tempfile::TempDir, Arc<Chronix>) {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let mut points = Vec::new();
    for i in 0..ROWS {
        let t = i as i64 * 1_000_000_000;
        // A clean 12-period seasonal so the STL kernels have something real
        // to decompose.
        let v = 100.0 + 0.5 * i as f64 + 10.0 * (i as f64 * std::f64::consts::TAU / 12.0).sin();
        let w = 50.0 + (i % 7) as f64;
        let f = if GAPS.contains(&i) {
            fields! { "w" => w }
        } else {
            fields! { "v" => v, "w" => w }
        };
        points.push(Point::new(key.clone(), f, t).unwrap());
    }
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    (dir, db)
}

async fn col(ctx: &SessionContext, expr: &str) -> Vec<Option<f64>> {
    let q = format!("SELECT {expr} OVER (PARTITION BY host ORDER BY _time) FROM m");
    let batches = ctx.sql(&q).await.unwrap().collect().await.unwrap();
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

/// Whether a statistic reads the value of the row it is reporting on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reads {
    /// Only the window around the row, so a gap row still gets an answer.
    TheWindow,
    /// The row's own sample, so a gap row cannot have one.
    ThisRow,
}
use Reads::{TheWindow, ThisRow};

/// Every windowed statistic, the call that exercises it, and what it reads.
///
/// `stl_decompose` and the two row-arithmetic functions are covered by their
/// own tests below, for the shape of their arguments and their results.
const STATISTICS: &[(&str, &str, Reads)] = &[
    ("rolling_mean", "rolling_mean(v, 4)", TheWindow),
    ("rolling_std", "rolling_std(v, 4)", TheWindow),
    ("rolling_corr", "rolling_corr(v, w, 5)", TheWindow),
    ("ewm", "ewm(v, 0.3)", TheWindow),
    ("correlation", "correlation(v, w)", TheWindow),
    ("cross_correlation", "cross_correlation(v, w, 1)", TheWindow),
    ("zscore", "zscore(v)", ThisRow),
    ("stl_trend", "stl_trend(v, 12)", ThisRow),
    ("stl_seasonal", "stl_seasonal(v, 12)", ThisRow),
    ("stl_residual", "stl_residual(v, 12)", ThisRow),
    ("anomaly_score", "anomaly_score(v, 3.0)", ThisRow),
    (
        "multivariate_anomaly",
        "multivariate_anomaly(v, w, 3.0)",
        ThisRow,
    ),
];

/// The rule, for every statistic at once: a gap nulls its own rows and
/// nothing else.
#[tokio::test]
async fn a_gap_nulls_its_own_rows_and_no_others() {
    let (_dir, db) = db_with_gaps();
    let ctx = chronix::sql::create_session_context(db);

    for &(name, expr, reads) in STATISTICS {
        let out = col(&ctx, expr).await;
        assert_eq!(out.len(), ROWS, "{name}: wrong row count");

        for &g in &GAPS {
            match reads {
                ThisRow => assert!(
                    out[g].is_none(),
                    "{name} reads row {g}'s own sample, which is missing, but reported {:?}",
                    out[g]
                ),
                TheWindow => assert!(
                    out[g].is_some(),
                    "{name} reads only the window, whose other rows have samples, \
                     yet row {g} is NULL"
                ),
            }
        }
        // The defect this file exists for: the gap must not reach past
        // itself. Row 9 is the first row whose window no longer holds the
        // gap at rows 7-8 for any of these calls.
        assert!(
            out[9].is_some(),
            "{name}: still NULL at row 9, after the gap at rows 7-8 closed"
        );
        assert!(
            out[ROWS - 1].is_some(),
            "{name}: the gap nulled the rest of the partition — row {} is NULL",
            ROWS - 1
        );
        let expected_nulls = if reads == ThisRow { GAPS.len() } else { 0 };
        let nulls = out.iter().filter(|v| v.is_none()).count();
        assert!(
            nulls <= expected_nulls + 4,
            "{name}: {nulls} NULL rows, expected at most {} (warm-up allows 4)",
            expected_nulls + 4
        );
    }
}

/// A NULL operand makes one row NULL, because that is what SQL arithmetic
/// does — `v - lag(v)` is NULL when either side is.
#[tokio::test]
async fn row_arithmetic_nulls_only_the_rows_that_read_a_gap() {
    let (_dir, db) = db_with_gaps();
    let ctx = chronix::sql::create_session_context(db);

    for expr in ["diff(v, 1)", "pct_change(v)"] {
        let out = col(&ctx, expr).await;
        // Rows 7, 8 read a missing value; so do 9 (predecessor 8) and 0
        // (no predecessor). Row 10 onwards is whole again.
        assert!(out[0].is_none(), "{expr}: row 0 has no predecessor");
        for &g in &GAPS {
            assert!(out[g].is_none(), "{expr}: row {g} is a gap");
            assert!(out[g + 1].is_none(), "{expr}: row {} reads the gap", g + 1);
        }
        assert!(out[10].is_some(), "{expr}: row 10 reads two present values");
        assert!(
            out[ROWS - 1].is_some(),
            "{expr}: partition poisoned to the end"
        );
    }
}

/// The STL kernels keep every row where it is.
///
/// `on_dense` removed the gap rows and shifted everything after them by one
/// position, which for a seasonal decomposition is a change of phase: on a
/// clean sine the seasonal value at the row after the gap moved from -0.866
/// to -0.943 and stayed wrong for the rest of the partition. Asserted
/// against the same series with no gap at all.
#[tokio::test]
async fn a_gap_does_not_shift_the_seasonal_phase() {
    let (_dir, gapped) = db_with_gaps();
    let ctx_gapped = chronix::sql::create_session_context(gapped);

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
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let mut points = Vec::new();
    for i in 0..ROWS {
        let v = 100.0 + 0.5 * i as f64 + 10.0 * (i as f64 * std::f64::consts::TAU / 12.0).sin();
        points.push(
            Point::new(
                key.clone(),
                fields! { "v" => v, "w" => 50.0 + (i % 7) as f64 },
                i as i64 * 1_000_000_000,
            )
            .unwrap(),
        );
    }
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    let ctx_whole = chronix::sql::create_session_context(db);

    let a = col(&ctx_gapped, "stl_seasonal(v, 12)").await;
    let b = col(&ctx_whole, "stl_seasonal(v, 12)").await;
    let mut worst = 0.0f64;
    for i in 0..ROWS {
        if let (Some(x), Some(y)) = (a[i], b[i]) {
            worst = worst.max((x - y).abs());
        }
    }
    // Interpolating the three gaps costs 0.67 on a seasonal amplitude of 10
    // — the chord-versus-arc error of a straight line across two steps of a
    // sine. Removing the rows instead, which is what `on_dense` did, costs
    // 4.79, because every row after a gap is then read against the previous
    // season. The threshold sits between the two.
    assert!(
        worst < 1.5,
        "three missing samples moved the seasonal component by {worst:.3}; \
         interpolating them costs 0.67 and dropping their rows costs 4.79"
    );
}

/// `stl_decompose` returns all three components on the same rows.
#[tokio::test]
async fn stl_decompose_survives_a_gap() {
    let (_dir, db) = db_with_gaps();
    let ctx = chronix::sql::create_session_context(db);
    let batches = ctx
        .sql(
            "SELECT d.trend, d.seasonal, d.residual FROM (SELECT \
             stl_decompose(v, 12) OVER (PARTITION BY host ORDER BY _time) AS d FROM m)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total, ROWS);
    for b in &batches {
        for c in 0..3 {
            let a = b
                .column(c)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            assert!(
                (0..a.len()).any(|i| !a.is_null(i)),
                "component {c} is entirely NULL"
            );
        }
    }
}

/// Every window function the session registers is named in this file.
///
/// The tests above are only as good as the list they iterate, and the two
/// kernels most likely to be missing from a hand-written list are the newest
/// one and the one whose author added it last week. This closes that by
/// asking the registry.
#[tokio::test]
async fn every_registered_window_function_has_a_null_answer_here() {
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
    let ctx = chronix::sql::create_session_context(db);

    // Everything the session has that a bare DataFusion context does not:
    // `row_number`, `rank`, `lead` and the rest are upstream's and carry
    // upstream's NULL rules.
    let builtin: BTreeSet<String> = SessionContext::new()
        .state()
        .window_functions()
        .keys()
        .map(ToString::to_string)
        .collect();
    let registered: BTreeSet<String> = ctx
        .state()
        .window_functions()
        .keys()
        .map(ToString::to_string)
        .filter(|n| !builtin.contains(n))
        .collect();
    assert!(
        registered.len() >= 15,
        "expected chronix's own window functions, found {registered:?}"
    );

    let mut covered: BTreeSet<String> = STATISTICS
        .iter()
        .map(|(name, _, _)| (*name).to_string())
        .collect();
    // Covered by their own tests above, for the shape of their arguments.
    covered.insert("diff".into());
    covered.insert("pct_change".into());
    covered.insert("stl_decompose".into());

    let missing: Vec<&String> = registered.difference(&covered).collect();
    assert!(
        missing.is_empty(),
        "these window functions have no NULL answer in this file: {missing:?}"
    );
}
