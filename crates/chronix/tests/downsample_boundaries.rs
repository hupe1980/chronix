#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Downsampling must produce exactly one row per interval, no matter
//! how the scan underneath it was chunked.
//!
//! `execute_stream` emits the scan in 64 Ki-row chunks and, before the fix,
//! downsampled each chunk independently — so an interval straddling a chunk
//! boundary came back as two partial buckets.

use arrow::array::{Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use chronix_query::aggregate::AggFn;

const SEC: i64 = 1_000_000_000;

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

fn buckets(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    for b in batches {
        let ts = b
            .column(b.schema().index_of(chronix_core::TIME_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // The aggregated value column is the one that is not the timestamp.
        let vi = (0..b.num_columns())
            .find(|i| b.schema().field(*i).name() != chronix_core::TIME_COLUMN)
            .unwrap();
        let v = b
            .column(vi)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((ts.value(i), v.value(i)));
        }
    }
    out
}

/// More than one 64 Ki chunk of 1 s points, downsampled to 1 min: the chunk
/// boundary lands in the middle of a minute.
#[test]
fn downsample_emits_one_row_per_interval_across_chunk_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    // 70_000 points at 1 s — comfortably past the 65_536-row chunk size.
    const POINTS: i64 = 70_000;
    let batch: Vec<Point> = (0..POINTS)
        .map(|i| Point::new(key.clone(), fields! { "v" => 1.0 }, i * SEC).unwrap())
        .collect();
    db.insert_batch(&batch).unwrap().into_complete().unwrap();

    let plan = db
        .query()
        .measurement("m")
        .range(0, i64::MAX)
        .downsample(
            TimeBucket::fixed(std::time::Duration::from_secs(60)),
            AggFn::Sum,
        )
        .build()
        .unwrap();

    let rows = buckets(&db.execute_stream(&plan).unwrap());

    // Every bucket key must appear exactly once.
    let mut seen: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for (ts, _) in &rows {
        *seen.entry(*ts).or_default() += 1;
    }
    let dupes: Vec<_> = seen.iter().filter(|(_, n)| **n > 1).collect();
    assert!(
        dupes.is_empty(),
        "{} interval(s) split across chunk boundaries, e.g. {:?}",
        dupes.len(),
        dupes.iter().take(3).collect::<Vec<_>>()
    );

    // Full minutes must sum to exactly 60 points.
    let full_minutes = POINTS / 60;
    for (ts, v) in &rows {
        if *ts / (60 * SEC) < full_minutes {
            assert!(
                (*v - 60.0).abs() < f64::EPSILON,
                "bucket at {ts} summed to {v}, expected 60"
            );
        }
    }

    // Total must be conserved.
    let total: f64 = rows.iter().map(|(_, v)| v).sum();
    assert!(
        (total - POINTS as f64).abs() < 1e-6,
        "downsample lost or duplicated points: {total} vs {POINTS}"
    );
}

/// The same invariant when the data spans several time buckets on disk.
#[test]
fn downsample_is_correct_across_segment_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);
    let key = SeriesKey::new("m", tags! { "h" => "a" }).unwrap();

    // Three hourly shards, flushed separately so they become three segments.
    const PER_HOUR: i64 = 3_600;
    for hour in 0..3i64 {
        let batch: Vec<Point> = (0..PER_HOUR)
            .map(|i| {
                Point::new(
                    key.clone(),
                    fields! { "v" => 1.0 },
                    hour * 3_600 * SEC + i * SEC,
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
        .downsample(
            TimeBucket::fixed(std::time::Duration::from_secs(60)),
            AggFn::Sum,
        )
        .build()
        .unwrap();

    let rows = buckets(&db.execute_stream(&plan).unwrap());
    assert_eq!(
        rows.len(),
        3 * 60,
        "expected one row per minute over 3 hours"
    );
    for (ts, v) in &rows {
        assert!(
            (*v - 60.0).abs() < f64::EPSILON,
            "bucket at {ts} summed to {v}, expected 60"
        );
    }
}

// ── The native plan and SQL must mean the same thing by a bucket ────────

/// Both surfaces bucket a DST transition day identically.
///
/// `downsample()` used to take a `Duration`, so `1d` on the native API was
/// 86 400 seconds of UTC while `1d` in SQL was the zone's day. On the spring
/// transition Europe/Berlin has a **23-hour** day, so the two answers
/// disagreed for every series that crossed it — silently, because both
/// produced plausible buckets.
#[cfg(feature = "sql")]
#[test]
fn the_native_plan_and_sql_agree_about_a_transition_day() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);

    // 2025-03-30 is the spring-forward day in Europe/Berlin: local 02:00
    // jumps to 03:00, so the local day is 23 hours long.
    // 2025-03-29T00:00:00Z .. 2025-03-31T00:00:00Z, hourly.
    let start = 1_743_206_400_i64 * SEC; // 2025-03-29T00:00:00Z
    let key = SeriesKey::new("dst", tags! { "host" => "a" }).unwrap();
    for h in 0..48 {
        let ts = start + h * 3600 * SEC;
        db.insert(&Point::new(key.clone(), fields! { "v" => 1.0_f64 }, ts).unwrap())
            .unwrap();
    }
    db.flush().unwrap();

    let bucket = TimeBucket::parse("1d", Some("Europe/Berlin")).unwrap();

    // The native plan.
    let plan = db
        .query()
        .measurement("dst")
        .downsample(bucket, AggFn::Count)
        .build()
        .unwrap();
    let native = buckets(&[db.execute(&plan).unwrap()]);

    // The same question in SQL.
    let sql = db
        .sql(
            "SELECT time_bucket('1d', _time, 'Europe/Berlin') AS b, COUNT(v) AS c \
             FROM dst GROUP BY b ORDER BY b",
        )
        .unwrap();
    let mut from_sql: Vec<(i64, f64)> = Vec::new();
    for batch in &sql {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        let c = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            #[allow(clippy::cast_precision_loss)]
            from_sql.push((ts.value(i), c.value(i) as f64));
        }
    }

    assert_eq!(
        native, from_sql,
        "the native downsample and time_bucket() must bucket identically"
    );

    // And the transition day really is short — otherwise this test would
    // pass on two implementations that are wrong in the same way.
    let counts: Vec<f64> = native.iter().map(|(_, c)| *c).collect();
    assert!(
        counts.contains(&23.0),
        "the Berlin day of the spring transition holds 23 hourly points, got {counts:?}"
    );
}

/// A calendar month is not thirty days, on either surface.
#[test]
fn a_native_monthly_bucket_follows_the_calendar() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir);

    // One point per day through February and March 2025 (28 + 31).
    let feb1 = 1_738_368_000_i64 * SEC; // 2025-02-01T00:00:00Z
    let key = SeriesKey::new("cal", tags! { "host" => "a" }).unwrap();
    for d in 0..59 {
        let ts = feb1 + d * 86_400 * SEC;
        db.insert(&Point::new(key.clone(), fields! { "v" => 1.0_f64 }, ts).unwrap())
            .unwrap();
    }
    db.flush().unwrap();

    let plan = db
        .query()
        .measurement("cal")
        .downsample(TimeBucket::parse("1mo", None).unwrap(), AggFn::Count)
        .build()
        .unwrap();
    let got = buckets(&[db.execute(&plan).unwrap()]);
    let counts: Vec<f64> = got.iter().map(|(_, c)| *c).collect();

    // February is 28 days in 2025 and March is 31 — a fixed width cannot
    // produce this pair at all.
    assert_eq!(counts, vec![28.0, 31.0], "got {got:?}");
}
