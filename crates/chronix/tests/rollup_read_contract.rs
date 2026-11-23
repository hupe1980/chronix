#![allow(clippy::unwrap_used, clippy::expect_used)] // integration fixtures
//! The public embedded rollup view has one contract on both sides of its watermark.

use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, UInt64Array};
use arrow::datatypes::DataType;
use chronix::prelude::*;
use chronix::{fields, tags};

const MINUTE: i64 = 60_000_000_000;

fn insert(db: &Chronix, point: &str, at: i64, watts: f64) {
    let key = SeriesKey::new("power", tags! { "point" => point }).unwrap();
    db.insert(&Point::new(key, fields! { "watts" => watts }, at).unwrap())
        .unwrap();
}

fn open(dir: &tempfile::TempDir, width: &str) -> Chronix {
    let mut config = ChronixConfig::small(dir.path());
    config.maintenance_interval = Duration::from_secs(3600);
    let db = Chronix::open(config).unwrap();
    declare(&db, width);
    db
}

fn declare(db: &Chronix, width: &str) {
    db.create_rollup(
        RollupBuilder::new()
            .name("tier")
            .source("power")
            .target("summary")
            .every(width)
            .timezone("Europe/Berlin")
            .aggregation(RollupAggFn::Count)
            .aggregation(RollupAggFn::Avg)
            .group_by("point")
            .build()
            .unwrap(),
    )
    .unwrap();
}

fn grid(db: &Chronix, start: i64, end: i64) -> RecordBatch {
    db.rollup_where("tier", start, end, &[("point", "grid")])
        .unwrap()
}

fn check_row(batch: &RecordBatch, time: i64, mean: f64, count: u64) {
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name(chronix_core::TIME_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        time
    );
    assert_eq!(
        batch
            .column_by_name("watts_avg")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        mean
    );
    assert_eq!(
        batch
            .column_by_name("watts_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        count
    );
}

// 052-rollup-contract/SR-002: widening the source scan preserves the caller's
// tag predicate and the inclusive window on returned bucket starts.
#[test]
fn complete_buckets_and_typed_empty_batches_survive_materialisation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, "15m");
    let t = 1_700_000_100_000_000_000;
    let current = t + 30 * 60 * MINUTE;
    insert(&db, "grid", t, 0.0);
    insert(&db, "grid", t + 7 * MINUTE, 1000.0);
    insert(&db, "grid", current, 0.0);

    let schema = grid(&db, t, t).schema();
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            (
                f.name().as_str(),
                f.data_type().clone(),
                f.metadata()["role"].as_str(),
            )
        })
        .collect();
    assert_eq!(
        fields,
        [
            ("_time", DataType::Int64, "timestamp"),
            ("point", DataType::Utf8, "tag"),
            ("watts_avg", DataType::Float64, "field"),
            ("watts_count", DataType::UInt64, "field"),
        ]
    );

    for stored in [false, true] {
        if stored {
            assert_eq!(db.materialise_rollups().unwrap(), 1);
        }
        for end in [t, t + 1_000_000_000, t + 15 * MINUTE - 1] {
            let row = grid(&db, t, end);
            check_row(&row, t, 500.0, 2);
            assert_eq!(row.schema(), schema);
            let empty = db
                .rollup_where("tier", t, end, &[("point", "absent")])
                .unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.schema(), schema);
        }
        let between = grid(&db, t + 1, t + 15 * MINUTE - 1);
        assert_eq!(between.num_rows(), 0);
        assert_eq!(between.schema(), schema);
        check_row(&grid(&db, current, current), current, 0.0, 1);
    }
}

#[test]
fn a_single_bucket_read_includes_the_last_hour_of_a_long_calendar_day() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, "1d");
    // Berlin midnight at the beginning of the 25-hour fall transition day.
    let start = i64::try_from(
        "2024-10-26T22:00:00Z"
            .parse::<jiff::Timestamp>()
            .unwrap()
            .as_nanosecond(),
    )
    .unwrap();
    insert(&db, "grid", start, 0.0);
    insert(&db, "grid", start + 24 * 60 * MINUTE, 1000.0);
    insert(&db, "other", start + 24 * 60 * MINUTE, 9999.0);
    insert(&db, "grid", start + 25 * 60 * MINUTE, 77.0);
    check_row(&grid(&db, start, start), start, 500.0, 2);
}

#[test]
fn a_sparse_group_keeps_the_schema_of_the_whole_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, "15m");
    let t = 1_700_000_100_000_000_000;
    insert(&db, "grid", t, 0.0);
    let key = SeriesKey::new("power", tags! { "point" => "other" }).unwrap();
    let point = Point::new(key, fields! { "amps" => 3_i64, "label" => "aux" }, t).unwrap();
    db.insert(&point).unwrap();
    let batch = grid(&db, t, t);
    let schema = batch.schema();
    assert_eq!(batch.column_by_name("amps_avg").unwrap().null_count(), 1);
    assert_eq!(batch.column_by_name("amps_count").unwrap().null_count(), 1);
    assert!(batch.column_by_name("label_avg").is_none());
    let other = db
        .rollup_where("tier", t, t, &[("point", "other")])
        .unwrap();
    assert_eq!(other.schema(), schema);
    assert_eq!(other.column_by_name("watts_avg").unwrap().null_count(), 1);
}

#[test]
fn the_final_representable_bucket_includes_the_last_nanosecond() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ChronixConfig::small(dir.path());
    config.future_write_tolerance = Duration::MAX;
    let db = Chronix::open(config).unwrap();
    declare(&db, "15m");
    let t = i64::MAX - i64::MAX.rem_euclid(15 * MINUTE);
    insert(&db, "grid", t, 0.0);
    insert(&db, "grid", i64::MAX, 1000.0);
    check_row(&grid(&db, t, t), t, 500.0, 2);
}

#[test]
fn a_mixed_read_refuses_a_float_count_from_an_old_target() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, "15m");
    let t = 1_700_000_100_000_000_000;
    let current = t + 30 * 60 * MINUTE;
    let key = SeriesKey::new("summary", tags! { "point" => "grid" }).unwrap();
    let old = Point::new(
        key,
        fields! { "watts_avg" => 500.0, "watts_count" => 2.0 },
        t,
    )
    .unwrap();
    db.insert(&old).unwrap();
    insert(&db, "grid", current, 5.0);
    assert_eq!(db.materialise_rollups().unwrap(), 0);
    let error = db.rollup("tier", t, current).unwrap_err().to_string();
    assert!(
        error.contains("UInt64") && error.contains("Float64"),
        "{error}"
    );
}
