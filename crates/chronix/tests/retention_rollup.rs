#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Rollup-aware retention: raw data must never be dropped before its
//! aggregate exists — and the aggregate must be right.

use chronix::prelude::*;
use chronix::rollup::{RollupAggFn, RollupBuilder};
use chronix::{fields, tags, Chronix};
use std::sync::Arc;
use std::time::Duration;

const HOUR: i64 = 3_600_000_000_000;
const MINUTE: i64 = 60_000_000_000;

fn open(dir: &tempfile::TempDir) -> Arc<Chronix> {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Arc::new(Chronix::open(config).unwrap())
}

fn rollup(db: &Chronix, name: &str, source: &str, target: &str, interval: i64) {
    db.create_rollup(
        RollupBuilder::new()
            .name(name)
            .source(source)
            .target(target)
            .bucket(chronix::timebucket::TimeBucket::fixed_ns(interval))
            // The tiers outlive the raw data — that is what a rollup is for.
            // Without this the 1 ns global retention below expires the
            // materialised tiers too, once the maintenance thread has
            // flushed them into segments of their own.
            .retention_ns(i64::MAX / 4)
            .aggregation(RollupAggFn::Avg)
            .aggregation(RollupAggFn::Count)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();
}

/// A live write far ahead closes the out-of-order window behind it, which
/// is what makes older buckets final. Written to its own measurement so it
/// never feeds a rollup.
fn advance_clock(db: &Chronix, to: i64) {
    let key = SeriesKey::new("clock", tags! { "h" => "x" }).unwrap();
    db.insert(&Point::new(key, fields! { "v" => 0.0 }, to).unwrap())
        .unwrap();
    db.flush().unwrap();
}

fn f64_col(batch: &arrow::record_batch::RecordBatch, name: &str, row: usize) -> f64 {
    batch
        .column_by_name(name)
        .expect("column present in the rollup batch")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(row)
}

fn scan(db: &Chronix, measurement: &str) -> arrow::record_batch::RecordBatch {
    let plan = db
        .query()
        .measurement(measurement)
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    db.execute(&plan).unwrap()
}

/// A segment that cannot be read cannot be rolled up — so retention must
/// preserve it. Dropping it destroys the raw data *and* the aggregate that
/// was supposed to replace it.
#[test]
fn unreadable_segment_is_not_dropped_by_rollup_aware_retention() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    for i in 0..50i64 {
        db.insert(
            &Point::new(key.clone(), fields! { "v" => i as f64 }, i * 1_000_000_000).unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();
    advance_clock(&db, 10 * HOUR);

    let segs: Vec<_> = {
        let cat = db.catalog().read();
        cat.active_segments_for_measurement("raw")
            .iter()
            .map(|e| e.path.clone())
            .collect()
    };
    assert_eq!(segs.len(), 1, "expected exactly one segment");
    std::fs::write(&segs[0], b"NOT A SEGMENT").unwrap();

    let result = db.enforce_retention(Duration::from_nanos(1)).unwrap();
    // The clock segment goes; the raw segment must stay.
    assert_eq!(result.segments_deleted, 1, "only the clock segment may go");
    assert!(
        segs[0].exists(),
        "the unreadable segment must be preserved, not dropped"
    );
    assert_eq!(
        db.catalog()
            .read()
            .active_segments_for_measurement("raw")
            .len(),
        1
    );
    db.close().unwrap();
}

/// The ordinary case: the rollup is materialised, then the raw data goes.
#[test]
fn readable_segment_is_dropped_after_rollup() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    for i in 0..50i64 {
        db.insert(
            &Point::new(key.clone(), fields! { "v" => i as f64 }, i * 1_000_000_000).unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();
    advance_clock(&db, 10 * HOUR);

    let result = db.enforce_retention(Duration::from_nanos(1)).unwrap();
    assert_eq!(result.segments_deleted, 2, "raw and clock");
    assert!(db
        .catalog()
        .read()
        .active_segments_for_measurement("raw")
        .is_empty());

    let batch = scan(&db, "raw_1m");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(f64_col(&batch, "v_count", 0), 50.0);
    assert!((f64_col(&batch, "v_avg", 0) - 24.5).abs() < 1e-9);
    db.close().unwrap();
}

/// A raw shard feeding a 1 s → 1 min → 15 min cascade is dropped only once
/// the 15 min tier — the one kept for years — exists.
#[test]
fn retention_materialises_the_whole_cascade_before_dropping_raw() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);
    rollup(&db, "1m_to_15m", "raw_1m", "raw_15m", 15 * MINUTE);

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    // 30 minutes of 1 s data with value = minute index, so every tier has
    // a closed-form answer.
    for i in 0..1800i64 {
        let v = (i / 60) as f64;
        db.insert(&Point::new(key.clone(), fields! { "v" => v }, i * 1_000_000_000).unwrap())
            .unwrap();
    }
    db.flush().unwrap();
    advance_clock(&db, 10 * HOUR);

    let result = db.enforce_retention(Duration::from_nanos(1)).unwrap();
    assert_eq!(result.segments_deleted, 2, "raw and clock");

    let m1 = scan(&db, "raw_1m");
    assert_eq!(m1.num_rows(), 30, "thirty 1-minute buckets");
    let m15 = scan(&db, "raw_15m");
    assert_eq!(m15.num_rows(), 2, "two 15-minute buckets");
    // The 15 min avg is the mean of its fifteen 1 min averages (0..15 → 7,
    // 15..30 → 22) and its count is the number of 1 min points (15).
    assert!(
        (f64_col(&m15, "v_avg_avg", 0) - 7.0).abs() < 1e-9,
        "{m15:?}"
    );
    assert!(
        (f64_col(&m15, "v_avg_avg", 1) - 22.0).abs() < 1e-9,
        "{m15:?}"
    );
    assert_eq!(f64_col(&m15, "v_avg_count", 0), 15.0);
    db.close().unwrap();
}

/// A rollup bucket is computed over *all* the raw data in it, not over one
/// segment at a time.
///
/// An expiring shard that was never compacted — below the compaction
/// trigger, which is the ordinary state of a gateway writing a few
/// megabytes an hour — holds several segments. Retention used to roll each
/// segment up on its own and insert the result, so a bucket spanning three
/// segments was written three times with three partial values and
/// last-write-wins kept whichever segment came last. The raw data was then
/// dropped, so the wrong aggregate was the only copy left.
#[test]
fn retention_rolls_a_bucket_up_over_every_segment_that_feeds_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    // Three flushes → three segments in one shard, all inside minute 0.
    // Values 1, 2, 3 per segment: the true mean over all sixty points is 2.
    for (seg, value) in [1.0f64, 2.0, 3.0].into_iter().enumerate() {
        for i in 0..20i64 {
            let ts = (seg as i64 * 20 + i) * 1_000_000_000;
            db.insert(&Point::new(key.clone(), fields! { "v" => value }, ts).unwrap())
                .unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(
        db.catalog()
            .read()
            .active_segments_for_measurement("raw")
            .len(),
        3,
        "the shard must hold three uncompacted segments"
    );
    advance_clock(&db, 10 * HOUR);

    let result = db.enforce_retention(Duration::from_nanos(1)).unwrap();
    assert_eq!(
        result.segments_deleted, 4,
        "three raw segments and the clock"
    );

    let batch = scan(&db, "raw_1m");
    assert_eq!(batch.num_rows(), 1, "one bucket: {batch:?}");
    assert_eq!(
        f64_col(&batch, "v_count", 0),
        60.0,
        "every point of every segment feeds the bucket"
    );
    let avg = f64_col(&batch, "v_avg", 0);
    assert!(
        (avg - 2.0).abs() < 1e-9,
        "mean over all segments is 2, got {avg}"
    );
    db.close().unwrap();
}

/// Raw data whose buckets are not yet final — inside the out-of-order
/// window — is never dropped, however old the clock says it is.
#[test]
fn retention_preserves_raw_data_whose_rollup_is_not_final() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    rollup(&db, "raw_to_1m", "raw", "raw_1m", MINUTE);

    let key = SeriesKey::new("raw", tags! { "h" => "a" }).unwrap();
    for i in 0..50i64 {
        db.insert(
            &Point::new(key.clone(), fields! { "v" => i as f64 }, i * 1_000_000_000).unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();
    // No clock advance: the window is still open over this shard.

    let result = db.enforce_retention(Duration::from_nanos(1)).unwrap();
    assert_eq!(result.segments_deleted, 0);
    assert_eq!(scan(&db, "raw_1m").num_rows(), 0, "nothing is final yet");
    assert_eq!(scan(&db, "raw").num_rows(), 50);
    db.close().unwrap();
}
