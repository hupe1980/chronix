//! A crash **inside** a rollup repair.
//!
//! The one interleaving the invalidation-log design is subtle about and did
//! not test: a repair writes its recomputed points, syncs them, and only then
//! clears the invalidation entry and persists the rollup state. A crash between the sync and the clear leaves a
//! pending entry whose work is already done, so the repair runs a second time
//! — and the argument that this is safe (the delete-then-rewrite is
//! idempotent) was an argument, not a test.
//!
//! The invariant is the one a user reads off a dashboard: **after recovery,
//! every rollup bucket equals the aggregate of the raw rows it covers** —
//! neither stale (a repair lost) nor doubled (a repair applied twice).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::collections::BTreeMap;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const SEC: i64 = 1_000_000_000;
const MINUTE: i64 = 60 * SEC;
/// Buckets of raw data written up front, one point per second.
const BUCKETS: i64 = 40;

fn config(dir: &std::path::Path) -> ChronixConfig {
    ChronixConfig::builder()
        .data_dir(dir)
        .shard_duration(Duration::from_secs(600))
        .ooo_shard_tolerance(1)
        .memtable_flush_threshold(16 * 1024)
        .wal_max_file_size(64 * 1024)
        // `Periodic` is the gateway preset, and the one under which an
        // unsynced WAL append is only in the operating system's buffers.
        .wal_fsync_policy(chronix_core::FsyncPolicy::Periodic(Duration::from_secs(1)))
        .build()
        .unwrap()
}

fn point(ts: i64, v: f64) -> Point {
    Point::new(
        SeriesKey::new("raw", tags! { "h" => "a" }).unwrap(),
        fields! { "v" => v },
        ts,
    )
    .unwrap()
}

fn rollup_config() -> chronix::RollupConfig {
    RollupBuilder::new()
        .name("raw_1m")
        .source("raw")
        .target("raw_1m")
        .bucket(TimeBucket::fixed_ns(MINUTE))
        .aggregation(RollupAggFn::Sum)
        .aggregation(RollupAggFn::Count)
        .group_by("h")
        .build()
        .unwrap()
}

/// Child: materialise a history, then backfill into already-final buckets in
/// a loop while materialising, and abort at an unpredictable moment.
fn child(dir: &std::path::Path) -> ! {
    let db = Chronix::open(config(dir)).unwrap();
    db.create_rollup(rollup_config()).unwrap();

    // A full history, plus a point far enough ahead that every bucket below
    // is past the live floor and therefore final.
    let mut points: Vec<Point> = Vec::new();
    for b in 0..BUCKETS {
        for s in 0..60 {
            points.push(point(b * MINUTE + s * SEC, 1.0));
        }
    }
    db.insert_batch(&points).unwrap().into_complete().unwrap();
    db.insert(&point((BUCKETS + 40) * MINUTE, 1.0)).unwrap();
    db.flush().unwrap();
    db.materialise_rollups().unwrap();

    // Now churn: each iteration adds one more point to an *already
    // materialised* bucket, which records an invalidation, and asks for the
    // repair. The abort lands wherever it lands.
    let mut backfilled: BTreeMap<i64, f64> = BTreeMap::new();
    let mut acks = std::fs::File::create(dir.join("acks")).unwrap();
    let mut round = 0i64;
    let deadline = std::time::Instant::now() + Duration::from_millis(1200);
    while std::time::Instant::now() < deadline {
        let bucket = round % BUCKETS;
        // A distinct timestamp per round, inside `bucket`, so nothing is
        // overwritten and the true aggregate is a pure function of the
        // rounds that were acknowledged.
        let ts = bucket * MINUTE + 59 * SEC - (round / BUCKETS);
        db.backfill(&[point(ts, 7.0)])
            .unwrap()
            .into_complete()
            .unwrap();
        *backfilled.entry(bucket).or_default() += 7.0;

        // Acknowledged means durable, and the parent has to be able to trust
        // the record of what was acknowledged.
        use std::io::{Seek, Write};
        let line = backfilled
            .iter()
            .map(|(b, v)| format!("{b}:{v}"))
            .collect::<Vec<_>>()
            .join(",");
        acks.set_len(0).unwrap();
        acks.seek(std::io::SeekFrom::Start(0)).unwrap();
        write!(acks, "{line}").unwrap();
        acks.sync_all().unwrap();

        let _ = db.materialise_rollups();
        round += 1;
    }
    std::process::abort();
}

/// The rollup's `(bucket_start → (sum, count))`, read through the public API.
fn rollup_buckets(db: &Chronix) -> BTreeMap<i64, (f64, f64)> {
    let m = db.rollup("raw_1m", i64::MIN, i64::MAX - 1).unwrap();
    let ts = m
        .column_by_name(chronix_core::TIME_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let sum = m
        .column_by_name("v_sum")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let count = m
        .column_by_name("v_count")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let mut out = BTreeMap::new();
    for i in 0..m.num_rows() {
        let prev = out.insert(ts.value(i), (sum.value(i), count.value(i)));
        assert!(prev.is_none(), "bucket {} appeared twice", ts.value(i));
    }
    out
}

/// The truth: aggregate the raw rows the same way, straight from storage.
fn raw_buckets(db: &Chronix) -> BTreeMap<i64, (f64, f64)> {
    let plan = db
        .query()
        .measurement("raw")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let mut out: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    for batch in db.execute_stream(&plan).unwrap() {
        let ts = batch
            .column_by_name(chronix_core::TIME_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let v = batch
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let bucket = ts.value(i).div_euclid(MINUTE) * MINUTE;
            let e = out.entry(bucket).or_default();
            e.0 += v.value(i);
            e.1 += 1.0;
        }
    }
    out
}

#[test]
fn a_crash_inside_a_repair_leaves_the_rollup_equal_to_its_source() {
    if let Ok(dir) = std::env::var("CHRONIX_ROLLUP_CRASH_DIR") {
        child(std::path::Path::new(&dir));
    }

    let tmp = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "a_crash_inside_a_repair_leaves_the_rollup_equal_to_its_source",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("CHRONIX_ROLLUP_CRASH_DIR", tmp.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort, not exit cleanly");

    let acks = std::fs::read_to_string(tmp.path().join("acks")).unwrap();
    assert!(!acks.trim().is_empty(), "the child barely ran");

    let db = Chronix::open(config(tmp.path())).unwrap();

    // A pending repair survives the crash and runs; running it again after
    // that must change nothing, which is what idempotence means here.
    db.materialise_rollups().unwrap();
    let after_first = rollup_buckets(&db);
    db.materialise_rollups().unwrap();
    let after_second = rollup_buckets(&db);
    assert_eq!(
        after_first, after_second,
        "a second materialisation changed the rollup, so a repair is not idempotent"
    );

    // Every materialised bucket equals the aggregate of the rows it covers.
    let raw = raw_buckets(&db);
    assert!(
        after_first.len() >= BUCKETS as usize,
        "only {} buckets materialised",
        after_first.len()
    );
    for (bucket, (sum, count)) in &after_first {
        let (raw_sum, raw_count) = raw
            .get(bucket)
            .copied()
            .unwrap_or_else(|| panic!("rollup has bucket {bucket} the raw data does not"));
        assert!(
            (sum - raw_sum).abs() < 1e-9 && (count - raw_count).abs() < 1e-9,
            "bucket {bucket}: rollup says (sum {sum}, count {count}), raw says \
             (sum {raw_sum}, count {raw_count})"
        );
    }
}
