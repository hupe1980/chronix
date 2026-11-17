//! `ORDER BY _time` is answered by the scan, in both directions.
//!
//! The scan declares that it emits `_time` ascending, and runs newest-first
//! for `ORDER BY _time DESC`, so DataFusion drops the sort and the `LIMIT`
//! stops the scan after the buckets it needs. A declared ordering the stream
//! does not keep is a wrong answer, so every claim here is checked against a
//! full read sorted independently — over disjoint segments, overlapping
//! segments, unflushed memtable rows and overwritten points.

#![cfg(feature = "sql")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use chronix::prelude::*;
use chronix::{Chronix, fields, tags};
use datafusion::prelude::SessionContext;

const MIN: i64 = 60_000_000_000;
const DAY: i64 = 1_440 * MIN;
const HOSTS: [&str; 3] = ["a", "b", "c"];
/// Points per host per day: one every ten minutes.
const PER_DAY: i64 = 144;

type Row = (i64, String, u64);

fn open() -> (tempfile::TempDir, Arc<Chronix>) {
    let dir = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    (dir, db)
}

/// Write through the backfill door — fixtures deliberately write behind the
/// newest timestamp — and refuse a partial write, so a fixture cannot
/// silently hold less than it says.
fn write(db: &Chronix, points: &[Point]) {
    assert!(
        db.backfill(points).unwrap().is_complete(),
        "fixture write was partial"
    );
}

fn day_points(day: i64, bump: f64) -> Vec<Point> {
    let mut points = Vec::new();
    for (h, host) in HOSTS.iter().enumerate() {
        let key = SeriesKey::new("m", tags! { "host" => *host }).unwrap();
        for i in 0..PER_DAY {
            let ts = day * DAY + i * 10 * MIN;
            let v = (h as f64) * 1_000.0 + (day * PER_DAY + i) as f64 + bump;
            points.push(Point::new(key.clone(), fields! { "v" => v }, ts).unwrap());
        }
    }
    points
}

/// (a) Time-disjoint segments: one flush per day.
fn disjoint() -> (tempfile::TempDir, Arc<Chronix>) {
    let (dir, db) = open();
    for day in 0..5 {
        write(&db, &day_points(day, 0.0));
        db.flush().unwrap();
    }
    (dir, db)
}

/// (b) Overlapping segments written out of order, with a rewritten day.
fn overlapping() -> (tempfile::TempDir, Arc<Chronix>) {
    let (dir, db) = open();
    for day in [2, 0, 1] {
        write(&db, &day_points(day, 0.0));
        db.flush().unwrap();
    }
    // Day 1 again with new values: last write wins, in a segment that
    // overlaps the first one's range.
    write(&db, &day_points(1, 0.5));
    db.flush().unwrap();
    (dir, db)
}

/// (c) Flushed days plus newer, unflushed memtable rows, one of which
/// overwrites a flushed point.
fn with_memtable() -> (tempfile::TempDir, Arc<Chronix>) {
    let (dir, db) = disjoint();
    write(&db, &day_points(5, 0.0));
    let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
    let ts = 4 * DAY + (PER_DAY - 1) * 10 * MIN;
    write(
        &db,
        &[Point::new(key, fields! { "v" => -1.0 }, ts).unwrap()],
    );
    (dir, db)
}

/// (d) The same `(series, _time)` written three times, in order.
fn duplicates() -> (tempfile::TempDir, Arc<Chronix>) {
    let (dir, db) = open();
    for bump in [0.0, 0.25, 0.75] {
        write(&db, &day_points(0, bump));
        db.flush().unwrap();
    }
    (dir, db)
}

async fn rows(ctx: &SessionContext, sql: &str) -> Vec<Row> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let t = arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Int64).unwrap();
        let t = t
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let h = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let v = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((t.value(i), h.value(i).to_string(), v.value(i).to_bits()));
        }
    }
    out
}

fn sorted(mut r: Vec<Row>) -> Vec<Row> {
    r.sort();
    r
}

async fn plan_text(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

const COLS: &str = "SELECT _time, host, v FROM m";

/// Every claim, on one fixture.
async fn check(name: &str, db: Arc<Chronix>) {
    let ctx = chronix::sql::create_session_context(db);
    let full = sorted(rows(&ctx, COLS).await);
    assert!(!full.is_empty(), "{name}: fixture is empty");

    for (dir, desc) in [("ASC", false), ("DESC", true)] {
        // Unlimited: the declared order holds row by row, and nothing is
        // lost, duplicated or resurrected.
        let all = rows(&ctx, &format!("{COLS} ORDER BY _time {dir}")).await;
        let monotone = all.windows(2).all(|w| {
            if desc {
                w[0].0 >= w[1].0
            } else {
                w[0].0 <= w[1].0
            }
        });
        assert!(
            monotone,
            "{name}: ORDER BY _time {dir} is not {dir} row by row"
        );
        assert_eq!(
            sorted(all),
            full,
            "{name}: ORDER BY _time {dir} returned different rows from a full read"
        );

        // Limited: the right timestamps, and only rows that exist.
        let top = rows(&ctx, &format!("{COLS} ORDER BY _time {dir} LIMIT 10")).await;
        assert_eq!(top.len(), 10, "{name}: {dir} LIMIT 10");
        let mut want: Vec<i64> = full.iter().map(|r| r.0).collect();
        want.sort_unstable();
        if desc {
            want.reverse();
        }
        let got: Vec<i64> = top.iter().map(|r| r.0).collect();
        assert_eq!(got, want[..10], "{name}: {dir} LIMIT 10 timestamps");
        for r in &top {
            assert!(
                full.contains(r),
                "{name}: {dir} LIMIT 10 returned {r:?}, which a full read does not hold"
            );
        }
    }
}

#[tokio::test]
async fn disjoint_segments() {
    let (_d, db) = disjoint();
    check("disjoint", db).await;
}

#[tokio::test]
async fn overlapping_segments() {
    let (_d, db) = overlapping();
    check("overlapping", db).await;
}

#[tokio::test]
async fn memtable_rows_and_overwrites() {
    let (_d, db) = with_memtable();
    check("memtable", db.clone()).await;
    // The overwrite must be what DESC returns for that point.
    let ctx = chronix::sql::create_session_context(db);
    let ts = 4 * DAY + (PER_DAY - 1) * 10 * MIN;
    let all = rows(&ctx, &format!("{COLS} ORDER BY _time DESC")).await;
    let hit: Vec<&Row> = all.iter().filter(|r| r.0 == ts && r.1 == "a").collect();
    assert_eq!(hit.len(), 1, "one row per (series, _time)");
    assert_eq!(
        f64::from_bits(hit[0].2),
        -1.0,
        "the memtable overwrite wins"
    );
}

#[tokio::test]
async fn overwritten_points() {
    let (_d, db) = duplicates();
    check("duplicates", db.clone()).await;
    let ctx = chronix::sql::create_session_context(db);
    let all = rows(&ctx, &format!("{COLS} ORDER BY _time DESC")).await;
    assert_eq!(all.len(), HOSTS.len() * PER_DAY as usize);
    assert!(
        all.iter().all(|r| f64::from_bits(r.2).fract() == 0.75),
        "the last of three writes wins in both directions"
    );
}

/// The sort is gone for `_time` in either direction, and stays for anything
/// the scan does not emit in order.
#[tokio::test]
async fn the_sort_is_dropped_only_where_the_scan_answers_it() {
    let (_d, db) = disjoint();
    let ctx = chronix::sql::create_session_context(db);
    for q in [
        format!("{COLS} ORDER BY _time ASC LIMIT 10"),
        format!("{COLS} ORDER BY _time DESC LIMIT 10"),
    ] {
        let plan = plan_text(&ctx, &q).await;
        assert!(!plan.contains("SortExec"), "{q} still sorts:\n{plan}");
    }
    let q = format!("{COLS} ORDER BY v DESC LIMIT 10");
    let plan = plan_text(&ctx, &q).await;
    assert!(plan.contains("SortExec"), "{q} must sort:\n{plan}");
    let q = format!("{COLS} ORDER BY _time DESC, host LIMIT 10");
    let plan = plan_text(&ctx, &q).await;
    assert!(
        plan.contains("SortExec"),
        "{q} needs a tie-break sort:\n{plan}"
    );
}

/// `LIMIT 10` in either direction reads one day, not five.
#[tokio::test]
async fn latest_n_reads_only_the_newest_bucket() {
    use datafusion::physical_plan::{ExecutionPlan, collect};

    fn scanned(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = plan
            .metrics()
            .and_then(|m| {
                m.iter()
                    .find(|v| v.value().name() == "rows_scanned")
                    .map(|v| v.value().as_usize())
            })
            .unwrap_or(0);
        own + plan.children().into_iter().map(scanned).sum::<usize>()
    }

    let (_d, db) = disjoint();
    let ctx = chronix::sql::create_session_context(db);
    let one_day = HOSTS.len() * PER_DAY as usize;
    for dir in ["ASC", "DESC"] {
        let plan = ctx
            .sql(&format!("{COLS} ORDER BY _time {dir} LIMIT 10"))
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let out = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        assert_eq!(
            out.iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            10
        );
        let read = scanned(&plan);
        assert!(
            read <= one_day,
            "{dir} LIMIT 10 read {read} rows; one day holds {one_day}"
        );
    }
}
