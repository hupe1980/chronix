#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! `LIMIT` over a scan must stream, and must agree with the collecting path.
//!
//! A limit is the one node above a scan that needs *less* of its input rather
//! than all of it, so it belongs in the stream, where it also stops the scan
//! early. `has_rows` — the server's existence probe behind
//! `/api/v1/prom/metadata` and `/label/__name__/values` — is a `LIMIT 1` plan,
//! so a limit that cannot stream makes a whole measurement invisible.

use std::sync::Arc;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

fn db_with(rows: usize, flushes: usize) -> (tempfile::TempDir, Arc<Chronix>) {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();
    let chunk = rows.div_ceil(flushes);
    for f in 0..flushes {
        let points: Vec<Point> = ((f * chunk)..((f + 1) * chunk).min(rows))
            .map(|i| {
                Point::new(
                    key.clone(),
                    fields! { "v" => i as f64 },
                    i as i64 * 1_000_000_000,
                )
                .unwrap()
            })
            .collect();
        if points.is_empty() {
            continue;
        }
        let _ = db.insert_batch(&points).unwrap();
        db.flush().unwrap();
    }
    (dir, db)
}

fn streamed(db: &Chronix, plan: &chronix_query::QueryPlan) -> Vec<f64> {
    db.execute_iter(plan)
        .unwrap()
        .flat_map(|b| {
            let b = b.unwrap();
            let c = b
                .column_by_name("v")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn execute_iter_accepts_a_limit_and_agrees_with_execute() {
    let (_dir, db) = db_with(100, 4);
    for limit in [1usize, 7, 100, 1000] {
        let plan = db
            .query()
            .measurement("m")
            .range(0, 200_000_000_000)
            .limit(limit)
            .build()
            .unwrap();
        let eager = db.execute(&plan).unwrap().num_rows();
        let lazy = streamed(&db, &plan);
        assert_eq!(eager, limit.min(100), "execute() with LIMIT {limit}");
        assert_eq!(
            lazy.len(),
            eager,
            "execute_iter() disagreed at LIMIT {limit}"
        );
        // Streaming preserves the global timestamp order, so the rows are the
        // first `limit` values and not an arbitrary `limit` of them.
        for (i, v) in lazy.iter().enumerate() {
            assert!((v - i as f64).abs() < 1e-9, "LIMIT {limit} row {i} = {v}");
        }
    }
}

#[test]
fn a_limit_stops_the_scan_early() {
    let (_dir, db) = db_with(400, 8);
    let plan = db
        .query()
        .measurement("m")
        .range(0, 1_000_000_000_000)
        .limit(1)
        .build()
        .unwrap();
    let mut stream = db.execute_iter(&plan).unwrap();
    let before = stream.buckets_remaining();
    let first = stream.next().transpose().unwrap().expect("a first batch");
    assert_eq!(first.num_rows(), 1);
    assert!(
        stream.next().is_none(),
        "the stream continued past the limit"
    );
    assert!(
        stream.buckets_remaining() < before,
        "no bucket was consumed, so the probe measured nothing"
    );
    assert!(
        stream.buckets_remaining() > 0,
        "LIMIT 1 read every bucket ({before} of them) instead of stopping"
    );
}

#[test]
fn offset_skips_from_the_front() {
    let (_dir, db) = db_with(50, 3);
    let plan = db
        .query()
        .measurement("m")
        .range(0, 200_000_000_000)
        .limit(5)
        .offset(20)
        .build()
        .unwrap();
    let lazy = streamed(&db, &plan);
    assert_eq!(lazy, vec![20.0, 21.0, 22.0, 23.0, 24.0]);
    assert_eq!(db.execute(&plan).unwrap().num_rows(), 5);
}

#[test]
fn an_offset_past_the_end_yields_nothing() {
    let (_dir, db) = db_with(20, 2);
    let plan = db
        .query()
        .measurement("m")
        .range(0, 200_000_000_000)
        .limit(5)
        .offset(100)
        .build()
        .unwrap();
    assert!(streamed(&db, &plan).is_empty());
    assert_eq!(db.execute(&plan).unwrap().num_rows(), 0);
}

/// Aggregates still cannot stream, and the error still says so.
#[test]
fn execute_iter_still_rejects_a_plan_that_needs_its_whole_input() {
    let (_dir, db) = db_with(20, 1);
    let plan = db
        .query()
        .measurement("m")
        .range(0, 200_000_000_000)
        .aggregate(chronix_query::AggFn::Sum)
        .build()
        .unwrap();
    let Err(err) = db.execute_iter(&plan) else {
        panic!("an aggregate plan streamed");
    };
    assert!(
        err.to_string().contains("whole input"),
        "unhelpful error: {err}"
    );
}
