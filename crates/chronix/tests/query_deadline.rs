#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A read deadline stops the reading — and stops nothing else.
//!
//! `ChronixConfig::query_timeout` was checked in exactly one place: after
//! `execute_stream_inner` had already collected a whole `Scan`. So the work
//! was done and *then* the caller was told it had run out of time, which is
//! the worst of both — the resources are spent and the answer is thrown away.
//! The streaming path underneath it, which `/api/v1/chronix/query`, the
//! exports and the cold tier all take, did not check the deadline at all.
//!
//! Moving it into `BatchStream` raises the opposite question, and it is the
//! one that matters more: a deadline is a bound on a *caller's patience*, and
//! a maintenance pass has no caller. A `QueryTimeout` inside rollup
//! materialisation would not merely be slow — the pass would fail, and the
//! next pass, and every pass after it, on a gateway catching up after a week
//! offline. So the background scans say `without_deadline()` and this file
//! pins both halves.

use std::sync::Arc;
use std::time::Duration;

use chronix::prelude::*;
use chronix::Chronix;

/// A database whose every read is already out of time, holding `points`
/// points of one series spread over `points` seconds.
fn expired_db(points: i64) -> (Arc<Chronix>, tempfile::TempDir, i64) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = ChronixConfig::builder()
        .data_dir(tmp.path().to_path_buf())
        .query_timeout(Duration::from_nanos(1))
        .build()
        .expect("config");
    let db = Arc::new(Chronix::open(config).expect("open"));
    let base = 1_700_000_000_000_000_000i64;
    let batch: Vec<Point> = (0..points)
        .map(|i| {
            Point::new(
                SeriesKey::new("cpu", chronix::tags! { "host" => "a" }).expect("key"),
                chronix::fields! { "value" => i as f64 },
                base + i * 1_000_000_000,
            )
            .expect("point")
        })
        .collect();
    db.insert_batch(&batch)
        .expect("insert")
        .into_complete()
        .expect("all accepted");
    db.flush().expect("flush");
    (db, tmp, base)
}

/// The streaming read path refuses once the budget is spent.
///
/// This is the path `/api/v1/chronix/query` takes, and it had no deadline at
/// all: `query_timeout` was a field of `ChronixConfig` that the one endpoint
/// most people reach for first never consulted.
#[test]
fn a_spent_budget_stops_the_streaming_read() {
    let (db, _tmp, base) = expired_db(500);
    let plan = db
        .query()
        .measurement("cpu")
        .range(base, base + 500_000_000_000)
        .build()
        .expect("plan");

    let err = db
        .execute_iter(&plan)
        .expect("the stream is built")
        .collect::<Result<Vec<_>, _>>()
        .expect_err("a spent budget must refuse the scan");
    assert!(
        matches!(err, chronix::DbError::QueryTimeout(_)),
        "expected a query timeout, got {err}"
    );
}

/// …and the collecting path agrees, because it is a fold over the same
/// iterator.
#[test]
fn a_spent_budget_stops_the_collecting_read() {
    let (db, _tmp, base) = expired_db(500);
    let plan = db
        .query()
        .measurement("cpu")
        .range(base, base + 500_000_000_000)
        .build()
        .expect("plan");
    let err = db.execute(&plan).expect_err("a spent budget refuses");
    assert!(
        matches!(err, chronix::DbError::QueryTimeout(_)),
        "expected a query timeout, got {err}"
    );
}

/// A scan that opted out of the deadline answers, however long it takes.
///
/// The other half of the property, and the one a regression would be silent
/// about: without it a rollup materialisation, a cold-tier write and an
/// export would each start failing the moment they outran a bound that was
/// never meant for them.
#[test]
fn maintenance_work_is_not_bounded_by_a_query_deadline() {
    let (db, _tmp, base) = expired_db(500);
    let plan = db
        .query()
        .measurement("cpu")
        .range(base, base + 500_000_000_000)
        .build()
        .expect("plan");
    let rows: usize = db
        .execute_iter(&plan)
        .expect("stream")
        .without_deadline()
        .map(|b| {
            b.expect("a scan with no deadline cannot time out")
                .num_rows()
        })
        .sum();
    assert_eq!(rows, 500, "every row is still read");
}

/// The paths that opt out really do, driven through their own entry points.
///
/// Asserting on `without_deadline()` directly would only prove the method
/// works. This proves the three callers *use* it, which is the thing that
/// would rot.
#[test]
fn export_and_rollups_run_under_an_expired_deadline() {
    let (db, tmp, base) = expired_db(500);

    // Export — explicitly built for a window larger than RAM, so it is meant
    // to outlast any request deadline.
    let plan = db
        .query()
        .measurement("cpu")
        .range(base, base + 500_000_000_000)
        .build()
        .expect("plan");
    let out = tmp.path().join("export.parquet");
    db.export_parquet(&plan, &out, &chronix::ParquetExportConfig::default())
        .expect("an export is not bounded by query_timeout");
    assert!(out.exists(), "the export wrote its file");

    // Rollup materialisation — a gateway catching up after a week offline is
    // supposed to take longer than a request would, and a deadline here would
    // fail this pass and every pass after it.
    db.create_rollup(
        RollupBuilder::new()
            .name("cpu_1m")
            .source("cpu")
            .target("cpu_1m")
            .every("1m")
            .aggregation(RollupAggFn::Avg)
            .build()
            .expect("rollup config"),
    )
    .expect("create rollup");
    db.materialise_rollups()
        .expect("materialisation is not bounded by query_timeout");
}
