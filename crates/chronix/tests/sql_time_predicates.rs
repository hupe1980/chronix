#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Writing a time predicate in SQL, and finding out whether it pruned.
//!
//! Chronix's native API speaks epoch nanoseconds — `insert` takes an `i64`,
//! `range` takes two, every wire protocol carries them — so the first thing
//! a user writes in SQL is the comparison they already have the numbers
//! for. DataFusion rejected it: `_time` is `Timestamp(ns)`, the literal is
//! `Int64`, and there is no common type. `BETWEEN` failed worse, with an
//! internal "likely a bug in DataFusion" message.
//!
//! The provider's pushdown had always accepted `ScalarValue::Int64` — it was
//! written for exactly this predicate. It was unreachable, because the plan
//! never got that far.

use arrow::array::Array;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use std::sync::Arc;

const BASE: i64 = 1_700_000_000_000_000_000;
const SECOND: i64 = 1_000_000_000;

fn open(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Arc::new(Chronix::open(config).unwrap())
}

fn point(host: &str, ts: i64, v: f64) -> Point {
    Point::new(
        SeriesKey::new("m", tags! { "host" => host }).unwrap(),
        fields! { "v" => v },
        ts,
    )
    .unwrap()
}

/// Five points one second apart, flushed so the scan reads segments.
fn seeded(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let db = open(dir);
    for i in 0..5i64 {
        db.insert(&point("a", BASE + i * SECOND, i as f64)).unwrap();
    }
    db.flush().unwrap();
    db
}

/// Rows a query returns.
fn rows(db: &Chronix, sql: &str) -> usize {
    db.sql(sql)
        .unwrap_or_else(|e| panic!("{sql}\n  failed: {e}"))
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum()
}

/// The `ChronixExec` line from an `EXPLAIN`, which carries the time range
/// the scan will actually read.
fn scan_line(db: &Chronix, sql: &str) -> String {
    let batches = db
        .sql(&format!("EXPLAIN {sql}"))
        .unwrap_or_else(|e| panic!("EXPLAIN {sql}\n  failed: {e}"));
    for b in &batches {
        for c in 0..b.num_columns() {
            let Some(a) = b
                .column(c)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
            else {
                continue;
            };
            for i in 0..a.len() {
                for line in a.value(i).lines() {
                    if line.contains("ChronixExec") {
                        return line.trim().to_string();
                    }
                }
            }
        }
    }
    panic!("no ChronixExec in the plan for: {sql}");
}

// ── Epoch literals ─────────────────────────────────────────────────────

#[test]
fn a_time_column_compares_against_an_epoch_integer() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let t2 = BASE + 2 * SECOND;

    assert_eq!(
        rows(&db, &format!("SELECT * FROM m WHERE _time >= {BASE}")),
        5
    );
    assert_eq!(
        rows(&db, &format!("SELECT * FROM m WHERE _time <= {t2}")),
        3
    );
    assert_eq!(rows(&db, &format!("SELECT * FROM m WHERE _time > {t2}")), 2);
    assert_eq!(rows(&db, &format!("SELECT * FROM m WHERE _time < {t2}")), 2);
    assert_eq!(rows(&db, &format!("SELECT * FROM m WHERE _time = {t2}")), 1);
    assert_eq!(
        rows(&db, &format!("SELECT * FROM m WHERE _time <> {t2}")),
        4
    );
}

/// `BETWEEN` is its own plan node rather than sugar over two comparisons,
/// so it needed handling of its own — and it failed the worst of the three,
/// with an internal DataFusion bug message instead of a type error.
#[test]
fn between_and_in_accept_epoch_integers() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let t0 = BASE;
    let t2 = BASE + 2 * SECOND;

    assert_eq!(
        rows(
            &db,
            &format!("SELECT * FROM m WHERE _time BETWEEN {t0} AND {t2}")
        ),
        3
    );
    assert_eq!(
        rows(
            &db,
            &format!("SELECT * FROM m WHERE _time NOT BETWEEN {t0} AND {t2}")
        ),
        2
    );
    assert_eq!(
        rows(&db, &format!("SELECT * FROM m WHERE _time IN ({t0}, {t2})")),
        2
    );
}

/// The literal is read in the column's own unit, so a value this database
/// handed out compares equal to itself. Any other reading would silently
/// shift every timestamp by a factor of a thousand.
#[test]
fn an_epoch_literal_is_read_in_the_columns_unit() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);

    // The exact nanosecond of the third point, and nothing either side.
    let t2 = BASE + 2 * SECOND;
    let batches = db
        .sql(&format!("SELECT v FROM m WHERE _time = {t2}"))
        .unwrap();
    let values: Vec<f64> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(values, vec![2.0], "the literal must name the third point");
}

/// Both sides, so `1700000000000000000 <= _time` reads the same as
/// `_time >= 1700000000000000000`.
#[test]
fn the_literal_may_be_on_either_side() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let t2 = BASE + 2 * SECOND;
    assert_eq!(
        rows(&db, &format!("SELECT * FROM m WHERE {t2} <= _time")),
        3
    );
}

/// A float is not rewritten: `1.7e18` is not a value anybody types meaning
/// an instant, and it cannot represent one exactly.
#[test]
fn a_float_literal_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    assert!(
        db.sql("SELECT * FROM m WHERE _time >= 1.7e18").is_err(),
        "a float must not be silently read as an instant"
    );
}

// ── Pushdown ───────────────────────────────────────────────────────────

/// The point of writing the predicate at all: the scan must read the
/// narrowed range, not the whole measurement and then filter.
#[test]
fn an_epoch_predicate_narrows_the_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);
    let t2 = BASE + 2 * SECOND;

    let unfiltered = scan_line(&db, "SELECT * FROM m");
    assert!(
        unfiltered.contains(&i64::MIN.to_string()),
        "an unfiltered scan reads everything: {unfiltered}"
    );

    for sql in [
        format!("SELECT * FROM m WHERE _time >= {BASE}"),
        "SELECT * FROM m WHERE _time >= timestamp '2023-11-14T22:13:20Z'".to_string(),
        "SELECT * FROM m WHERE _time >= to_timestamp_seconds(1700000000)".to_string(),
        "SELECT * FROM m WHERE _time >= to_timestamp_millis(1700000000000)".to_string(),
    ] {
        let line = scan_line(&db, &sql);
        assert!(
            line.contains(&format!("time=[{BASE}..")),
            "the lower bound must reach the scan: {sql}\n  {line}"
        );
    }

    let line = scan_line(
        &db,
        &format!("SELECT * FROM m WHERE _time BETWEEN {BASE} AND {t2}"),
    );
    assert!(
        line.contains(&format!("time=[{BASE}..{t2}]")),
        "BETWEEN must narrow both ends: {line}"
    );
}

// ── EXPLAIN ────────────────────────────────────────────────────────────

/// `EXPLAIN` used to be refused as information disclosure, which left no way
/// to find out whether a time filter pushed down — the one question worth
/// asking about a query here. The scan's display is the caller's own query
/// echoed back: a measurement name, a time range, a filter count, a limit.
#[test]
fn explain_is_permitted_on_a_read() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);

    assert!(db.sql("EXPLAIN SELECT * FROM m").is_ok());
    assert!(db.sql("EXPLAIN ANALYZE SELECT count(*) FROM m").is_ok());
    assert!(
        db.sql("EXPLAIN SELECT * FROM m WHERE v > (SELECT avg(v) FROM m)")
            .is_ok(),
        "a subquery is still a read"
    );
}

/// And it is not a way around the admission check: `EXPLAIN ANALYZE`
/// executes, so the plan it wraps has to pass exactly the same rules.
#[test]
fn explain_does_not_smuggle_a_write_past_the_check() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(&dir);

    for sql in [
        "EXPLAIN INSERT INTO m SELECT * FROM m",
        "EXPLAIN ANALYZE INSERT INTO m SELECT * FROM m",
        "EXPLAIN SET datafusion.catalog.information_schema = true",
        "EXPLAIN CREATE EXTERNAL TABLE t STORED AS CSV LOCATION '/etc/passwd'",
    ] {
        assert!(
            db.sql(sql).is_err(),
            "{sql} must not be permitted by wrapping it in EXPLAIN"
        );
    }
}
