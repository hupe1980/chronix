#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A query returns the rows inside its time range and no others.
//!
//! Segment pruning works at row-group granularity: a group whose zone map
//! overlaps the range is read whole, boundary rows included. Trimming those
//! rows is the query's job, and it was conditional on the query carrying a tag
//! filter — so a plain `range(start, end)` returned every row of every
//! straddling group. A 101-point window over a 20K-point segment came back
//! with 3600 rows. `execute_stream` filtered unconditionally and did not share
//! the bug, which meant the two paths silently disagreed; both are asserted
//! here so they cannot drift apart again.

use std::collections::BTreeMap;

use chronix::prelude::*;
use chronix::Chronix;

const BASE: i64 = 1_700_000_000_000_000_000;
const SEC: i64 = 1_000_000_000;

fn seeded() -> (tempfile::TempDir, Chronix) {
    let dir = tempfile::tempdir().unwrap();
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap(),
    )
    .unwrap();

    let points: Vec<Point> = (0..20_000i64)
        .map(|i| {
            let tags: BTreeMap<String, String> = [("host".to_string(), format!("h{}", i % 4))]
                .into_iter()
                .collect();
            let key = SeriesKey::new("m", tags).unwrap();
            let fields: BTreeMap<String, FieldValue> =
                [("v".to_string(), FieldValue::F64(i as f64))]
                    .into_iter()
                    .collect();
            Point::new(key, fields, BASE + i * SEC).unwrap()
        })
        .collect();
    let _ = db.insert_batch(&points).unwrap();
    db.flush().unwrap();
    (dir, db)
}

fn timestamps(batch: &arrow::record_batch::RecordBatch) -> Vec<i64> {
    batch
        .column_by_name(chronix_core::TIME_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect()
}

#[test]
fn a_range_without_tag_filters_returns_only_rows_in_range() {
    let (_dir, db) = seeded();

    // A window landing mid-row-group, so pruning cannot do the trimming.
    let start = BASE + 10_000 * SEC;
    let end = start + 100 * SEC;
    let plan = db
        .query()
        .measurement("m")
        .range(start, end)
        .build()
        .unwrap();

    let ts = timestamps(&db.execute(&plan).unwrap());
    assert!(
        ts.iter().all(|t| *t >= start && *t <= end),
        "rows outside [{start}, {end}] were returned"
    );
    assert_eq!(ts.len(), 101, "inclusive range over 1s points");
}

#[test]
fn a_range_with_tag_filters_returns_only_rows_in_range() {
    let (_dir, db) = seeded();

    let start = BASE + 10_000 * SEC;
    let end = start + 100 * SEC;
    let plan = db
        .query()
        .measurement("m")
        .tag("host", "h0")
        .range(start, end)
        .build()
        .unwrap();

    let ts = timestamps(&db.execute(&plan).unwrap());
    assert!(
        ts.iter().all(|t| *t >= start && *t <= end),
        "rows outside [{start}, {end}] were returned"
    );
    // Every 4th point belongs to h0: 10_000, 10_004, … 10_100.
    assert_eq!(ts.len(), 26);
}

#[test]
fn the_streaming_path_agrees_with_the_collecting_one() {
    let (_dir, db) = seeded();

    let start = BASE + 10_000 * SEC;
    let end = start + 100 * SEC;
    let plan = db
        .query()
        .measurement("m")
        .range(start, end)
        .build()
        .unwrap();

    let collected = timestamps(&db.execute(&plan).unwrap());

    let mut streamed: Vec<i64> = Vec::new();
    for batch in db.execute_iter(&plan).unwrap() {
        streamed.extend(timestamps(&batch.unwrap()));
    }

    let mut a = collected;
    let mut b = streamed;
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b, "execute and execute_iter disagreed on the same plan");
}
