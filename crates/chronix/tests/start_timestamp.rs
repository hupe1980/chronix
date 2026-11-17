#![cfg(feature = "sql")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! The start timestamp (`_start`) through the whole engine: WAL replay, flush,
//! compaction, SQL, rollups — and what it costs.

use arrow::array::{Array, Int64Array, TimestampNanosecondArray};
use chronix::prelude::*;
use chronix::rollup::{RollupAggFn, RollupBuilder};
use chronix::{Chronix, fields, tags};

const SECOND: i64 = 1_000_000_000;

fn open(dir: &std::path::Path) -> Chronix {
    Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir)
            .shard_duration(std::time::Duration::from_secs(3600))
            .ooo_shard_tolerance(2)
            .maintenance_interval(std::time::Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap()
}

fn counter(ts: i64, start: Option<i64>) -> Point {
    let key = SeriesKey::new("boots", tags! { "box" => "a" }).unwrap();
    let p = Point::new(key, fields! { "total" => (ts / SECOND) as f64 }, ts).unwrap();
    match start {
        Some(s) => p.with_start_timestamp(s).unwrap(),
        None => p,
    }
}

/// `(_time, _start)` of every row of `boots`, ordered by time.
fn rows(db: &Chronix) -> Vec<(i64, Option<i64>)> {
    let batches = db
        .sql("SELECT CAST(_time AS BIGINT) AS t, _start FROM boots ORDER BY _time")
        .unwrap();
    let mut out = Vec::new();
    for b in batches {
        let t = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let s = b
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((t.value(i), s.is_valid(i).then(|| s.value(i))));
        }
    }
    out
}

/// A start timestamp survives a crash (WAL replay), a flush, and a
/// compaction, and reads back in SQL as a nullable `TIMESTAMP` beside `_time`.
#[test]
fn a_start_timestamp_survives_replay_flush_and_compaction() {
    let base = 1_700_000_000 * SECOND;
    let expected = vec![
        (base, Some(base - 10 * SECOND)),
        (base + SECOND, None),
        // A reset: the counter started again.
        (base + 2 * SECOND, Some(base + 2 * SECOND)),
    ];
    if let Ok(dir) = std::env::var("CHRONIX_START_CRASH_DIR") {
        let db = open(std::path::Path::new(&dir));
        assert!(
            db.insert_batch(&[
                counter(base, Some(base - 10 * SECOND)),
                counter(base + SECOND, None),
                counter(base + 2 * SECOND, Some(base + 2 * SECOND)),
            ])
            .unwrap()
            .is_complete()
        );
        assert_eq!(rows(&db), expected, "from the memtable");
        // A crash: no close, no flush. The next open replays the WAL.
        std::process::abort();
    }
    let tmp = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "a_start_timestamp_survives_replay_flush_and_compaction",
            "--exact",
            "--nocapture",
        ])
        .env("CHRONIX_START_CRASH_DIR", tmp.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort");
    let db = open(tmp.path());
    assert_eq!(rows(&db), expected, "after WAL replay");
    db.flush().unwrap();
    assert_eq!(rows(&db), expected, "from a segment");

    // A second segment, then a compaction that merges the two.
    db.insert(&counter(base + 3 * SECOND, Some(base + 2 * SECOND)))
        .unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    let mut all = expected.clone();
    all.push((base + 3 * SECOND, Some(base + 2 * SECOND)));
    assert_eq!(rows(&db), all, "after compaction");

    // It is not a value: the schema's field list does not offer it.
    let schema = db.schema("boots").unwrap();
    assert_eq!(schema.field_names(), ["total"]);
    assert_eq!(
        schema
            .column(chronix::chronix_core::START_COLUMN)
            .unwrap()
            .role,
        ColumnRole::StartTimestamp
    );
}

/// A constant start timestamp — the normal case between two resets — is
/// close to free: the column's cost is a fixed per-block overhead, not a
/// per-row one, because pco encodes a constant run as a zero delta. A
/// measurement that never receives one has no `_start` column at all.
#[test]
fn a_constant_start_timestamp_is_nearly_free() {
    let base = 1_700_000_000 * SECOND;
    let segment_bytes = |rows: i64, with_start: bool| {
        let tmp = tempfile::tempdir().unwrap();
        let db = open(tmp.path());
        let points: Vec<Point> = (0..rows)
            .map(|i| counter(base + i * SECOND, with_start.then_some(base - SECOND)))
            .collect();
        assert!(db.insert_batch(&points).unwrap().is_complete());
        db.flush().unwrap();
        let bytes: u64 = db.segments_of("boots").iter().map(|s| s.bytes).sum();
        let has_column = db
            .schema("boots")
            .unwrap()
            .column(chronix::chronix_core::START_COLUMN)
            .is_some();
        (bytes, has_column)
    };
    let (without, without_column) = segment_bytes(10_000, false);
    let (with, with_column) = segment_bytes(10_000, true);
    assert!(!without_column, "never sent, never stored");
    assert!(with_column);
    let per_row = (with as f64 - without as f64) / 10_000.0;
    assert!(
        per_row < 0.1,
        "a constant _start cost {per_row:.3} bytes per row ({without} → {with} bytes)"
    );
}

/// A rollup does not aggregate the start timestamp: a bucket has no single
/// start, and `avg(_start)` would be a number with no meaning.
#[test]
fn a_rollup_does_not_aggregate_the_start_timestamp() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open(tmp.path());
    db.create_rollup(
        RollupBuilder::new()
            .name("boots_1m")
            .source("boots")
            .target("boots_1m")
            .bucket(TimeBucket::fixed_ns(60 * SECOND))
            .aggregation(RollupAggFn::Max)
            .group_by("box")
            .build()
            .unwrap(),
    )
    .unwrap();
    // Five hours, so the first two are outside the out-of-order window and
    // final; one point a minute.
    let base = 1_700_000_000 / 3600 * 3600 * SECOND;
    let points: Vec<Point> = (0..300)
        .map(|i| counter(base + i * 60 * SECOND, Some(base - SECOND)))
        .collect();
    assert!(db.insert_batch(&points).unwrap().is_complete());
    db.flush().unwrap();
    assert!(db.materialise_rollups().unwrap() > 0);
    let schema = db.schema("boots_1m").expect("the rollup materialised");
    assert!(
        schema.columns().iter().all(|c| !c.name.contains("_start")),
        "{:?}",
        schema.columns()
    );
}
