#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Aggregation, grouping and limits are folded over a lazy
//! scan, so they must produce the same answers as the materialising path even
//! though the input now arrives in several independently-read time buckets.
//!
//! `First`/`Last` are the interesting cases: their state depends on timestamps
//! seen so far, so an aggregator that resets or mis-orders across buckets
//! would quietly return a value from the wrong end of the range.

use arrow::array::{Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use chronix_query::aggregate::AggFn;

const SEC: i64 = 1_000_000_000;
const HOUR_NS: i64 = 3_600 * SEC;

fn open(dir: &tempfile::TempDir) -> Chronix {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .wal_fsync_policy(chronix_core::FsyncPolicy::Periodic(
            std::time::Duration::from_secs(1),
        ))
        .build()
        .unwrap();
    Chronix::open(config).unwrap()
}

/// Four hourly segments, each flushed separately so the scan yields four
/// independent time buckets. Values ascend so First/Last are unambiguous.
fn seed(db: &Chronix, hosts: &[&str]) {
    const PER_HOUR: i64 = 100;
    for hour in 0..4i64 {
        let mut batch = Vec::new();
        for i in 0..PER_HOUR {
            for (h, host) in hosts.iter().enumerate() {
                let key = SeriesKey::new("m", tags! { "host" => *host }).unwrap();
                let v = (hour * PER_HOUR + i) as f64 + (h as f64) * 10_000.0;
                batch
                    .push(Point::new(key, fields! { "v" => v }, hour * HOUR_NS + i * SEC).unwrap());
            }
        }
        db.insert_batch(&batch).unwrap().into_complete().unwrap();
        db.flush().unwrap();
    }
}

fn single_f64(batches: &[RecordBatch], col: &str) -> f64 {
    let b = &batches[0];
    let idx = b.schema().index_of(col).unwrap();
    b.column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn ungrouped_aggregates_fold_correctly_across_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, &["a"]);

    // 400 points, values 0..=399.
    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .aggregate(AggFn::Count)
        .aggregate(AggFn::Sum)
        .aggregate(AggFn::Min)
        .aggregate(AggFn::Max)
        .aggregate(AggFn::Avg)
        .aggregate(AggFn::First)
        .aggregate(AggFn::Last)
        .build()
        .unwrap();

    let batches = db.execute_stream(&plan).unwrap();
    assert_eq!(batches.len(), 1);

    let expected_sum: f64 = (0..400).map(f64::from).sum();
    assert_eq!(single_f64(&batches, "v_count"), 400.0);
    assert!((single_f64(&batches, "v_sum") - expected_sum).abs() < 1e-6);
    assert_eq!(single_f64(&batches, "v_min"), 0.0);
    assert_eq!(single_f64(&batches, "v_max"), 399.0);
    assert!((single_f64(&batches, "v_avg") - expected_sum / 400.0).abs() < 1e-9);
    // First/Last are by timestamp, and the buckets are read in time order.
    assert_eq!(single_f64(&batches, "v_first"), 0.0);
    assert_eq!(single_f64(&batches, "v_last"), 399.0);
}

#[test]
fn grouped_aggregates_fold_correctly_across_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, &["a", "b"]);

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .aggregate(AggFn::Count)
        .aggregate(AggFn::Max)
        .group_by(&["host"])
        .build()
        .unwrap();

    let batches = db.execute_stream(&plan).unwrap();
    let b = &batches[0];
    let hosts = b
        .column(b.schema().index_of("host").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = b
        .column(b.schema().index_of("v_count").unwrap())
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let maxes = b
        .column(b.schema().index_of("v_max").unwrap())
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(b.num_rows(), 2, "one row per host");
    for i in 0..b.num_rows() {
        assert_eq!(counts.value(i), 400.0, "host {}", hosts.value(i));
        let expected_max = if hosts.value(i) == "a" {
            399.0
        } else {
            10_399.0
        };
        assert_eq!(maxes.value(i), expected_max, "host {}", hosts.value(i));
    }
}

/// A group that only appears in a later bucket must still be discovered.
#[test]
fn groups_appearing_late_are_not_missed() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);

    // Hour 0: only host "a". Hour 1: host "b" appears for the first time.
    for (hour, host) in [(0i64, "a"), (1, "b")] {
        let key = SeriesKey::new("m", tags! { "host" => host }).unwrap();
        let batch: Vec<Point> = (0..50)
            .map(|i| {
                Point::new(
                    key.clone(),
                    fields! { "v" => 1.0 },
                    hour * HOUR_NS + i * SEC,
                )
                .unwrap()
            })
            .collect();
        db.insert_batch(&batch).unwrap().into_complete().unwrap();
        db.flush().unwrap();
    }

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .aggregate(AggFn::Count)
        .group_by(&["host"])
        .build()
        .unwrap();

    let b = &db.execute_stream(&plan).unwrap()[0];
    assert_eq!(b.num_rows(), 2, "both hosts must appear");
}

#[test]
fn limit_and_offset_span_bucket_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, &["a"]);

    // 100 rows per hourly bucket; take 150 starting at 50 — crosses two
    // buckets and must stop before reading the last two.
    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .offset(50)
        .limit(150)
        .build()
        .unwrap();

    let batches = db.execute_stream(&plan).unwrap();
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 150);

    // Values must be 50..=199, in order.
    let mut seen = Vec::new();
    for b in &batches {
        let v = b
            .column(b.schema().index_of("v").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            seen.push(v.value(i));
        }
    }
    let expected: Vec<f64> = (50..200).map(f64::from).collect();
    assert_eq!(seen, expected);
}

/// The streaming path and the materialising `execute()` path must agree.
#[test]
fn streaming_and_materialising_aggregates_agree() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    seed(&db, &["a", "b"]);

    for func in [
        AggFn::Count,
        AggFn::Sum,
        AggFn::Min,
        AggFn::Max,
        AggFn::Avg,
        AggFn::First,
        AggFn::Last,
    ] {
        let plan = db
            .query()
            .measurement("m")
            .range(0, i64::MAX)
            .aggregate(func)
            .build()
            .unwrap();

        let streamed = db.execute_stream(&plan).unwrap();
        let materialised = db.execute(&plan).unwrap();
        assert_eq!(
            streamed[0].num_rows(),
            materialised.num_rows(),
            "{func:?}: row count differs"
        );
        let col = format!("v_{}", chronix_query::aggregate::agg_fn_name(func));
        let a = single_f64(&streamed, &col);
        let b = single_f64(&[materialised], &col);
        assert!(
            (a - b).abs() < 1e-9,
            "{func:?}: streaming gave {a}, materialising gave {b}"
        );
    }
}
