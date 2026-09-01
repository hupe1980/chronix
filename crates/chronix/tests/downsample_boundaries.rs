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
            .column(b.schema().index_of("timestamp").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // The aggregated value column is the one that is not the timestamp.
        let vi = (0..b.num_columns())
            .find(|i| b.schema().field(*i).name() != "timestamp")
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
        .downsample(std::time::Duration::from_secs(60), AggFn::Sum)
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
        .downsample(std::time::Duration::from_secs(60), AggFn::Sum)
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
