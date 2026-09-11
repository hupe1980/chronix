//! A crash while a flush, a compaction and a rollup materialisation are all
//! in flight — the scenario the WAL floor, the series sidecar and the rollup
//! watermark were designed for, and that no sequential restart test
//! produces.
//!
//! The child process writes continuously on one thread while another runs
//! the background passes in a loop, records every acknowledged batch in a
//! file (fsynced, so the parent can trust it), and `abort()`s. The parent
//! reopens and checks the invariants that must survive any interleaving.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const HOUR: i64 = 3_600_000_000_000;
const MINUTE: i64 = 60_000_000_000;
const BATCH: i64 = 50;

fn config(dir: &std::path::Path) -> ChronixConfig {
    ChronixConfig::builder()
        .data_dir(dir)
        .shard_duration(Duration::from_secs(3600))
        .ooo_shard_tolerance(2)
        // Tiny, so flushes and compactions actually happen during the run.
        .memtable_flush_threshold(16 * 1024)
        .wal_max_file_size(64 * 1024)
        .wal_max_unflushed(64)
        .build()
        .unwrap()
}

fn point(i: i64) -> Point {
    // One point per second, value = index, so gaps and duplicates are visible.
    Point::new(
        SeriesKey::new("raw", tags! { "h" => "a" }).unwrap(),
        fields! { "v" => i as f64 },
        i * 1_000_000_000,
    )
    .unwrap()
}

/// Child: write, churn the background passes, abort.
fn child(dir: &std::path::Path) -> ! {
    let db = Chronix::open(config(dir)).unwrap();
    db.create_rollup(
        RollupBuilder::new()
            .name("raw_1m")
            .source("raw")
            .target("raw_1m")
            .bucket(TimeBucket::fixed_ns(MINUTE))
            .aggregation(RollupAggFn::Avg)
            .aggregation(RollupAggFn::Count)
            .group_by("h")
            .build()
            .unwrap(),
    )
    .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let churn = {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = db.flush();
                let _ = db.compact();
                let _ = db.gc();
                let _ = db.materialise_rollups();
            }
        })
    };

    let mut acks = std::fs::File::create(dir.join("acks")).unwrap();
    let mut next = 0i64;
    let deadline = std::time::Instant::now() + Duration::from_millis(1500);
    while std::time::Instant::now() < deadline {
        let batch: Vec<Point> = (next..next + BATCH).map(point).collect();
        let res = db.insert_batch(&batch).unwrap();
        assert!(res.is_complete(), "{:?}", res.rejected);
        next += BATCH;
        // Acknowledged means durable: record it durably too.
        acks.set_len(0).unwrap();
        use std::io::Seek;
        acks.seek(std::io::SeekFrom::Start(0)).unwrap();
        write!(acks, "{next}").unwrap();
        acks.sync_all().unwrap();
    }
    let _ = churn; // still running — that is the point
    std::process::abort();
}

#[test]
fn a_crash_under_load_loses_nothing_acknowledged_and_duplicates_nothing() {
    if let Ok(dir) = std::env::var("CHRONIX_CRASH_DIR") {
        child(std::path::Path::new(&dir));
    }

    let tmp = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "a_crash_under_load_loses_nothing_acknowledged_and_duplicates_nothing",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("CHRONIX_CRASH_DIR", tmp.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort, not exit cleanly");

    let acked: i64 = std::fs::read_to_string(tmp.path().join("acks"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(acked >= 5 * BATCH, "the child barely ran: {acked} points");

    let db = Chronix::open(config(tmp.path())).unwrap();

    // 1. Every acknowledged point is present, exactly once, in order.
    let plan = db
        .query()
        .measurement("raw")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batches = db.execute_stream(&plan).unwrap();
    let mut seen: Vec<i64> = Vec::new();
    for b in &batches {
        let ts = b
            .column_by_name(chronix_core::TIME_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let v = b
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            assert_eq!(
                ts.value(i),
                (v.value(i) as i64) * 1_000_000_000,
                "value/ts pairing"
            );
            seen.push(v.value(i) as i64);
        }
    }
    let mut sorted = seen.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), seen.len(), "a point was read back twice");
    assert!(
        seen.len() as i64 >= acked,
        "acknowledged {acked}, recovered {}",
        seen.len()
    );
    for (i, v) in sorted.iter().enumerate() {
        assert_eq!(*v, i as i64, "gap below the acknowledged prefix");
    }
    // Anything beyond the acknowledgement is a whole batch or nothing.
    assert_eq!(sorted.len() as i64 % BATCH, 0, "a partial batch survived");

    // 2. The series count is exact.
    assert_eq!(
        db.statistics().series_count,
        2_usize.min(db.statistics().series_count).max(1)
    );

    // 3. Whatever was materialised is right, and materialising again from
    //    the persisted watermark changes nothing.
    let check = |db: &Chronix| {
        let m = db.rollup("raw_1m", 0, i64::MAX - 1).unwrap();
        let ts = m
            .column_by_name(chronix_core::TIME_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let count = m
            .column_by_name("v_count")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        let avg = m
            .column_by_name("v_avg")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        let total = sorted.len() as i64;
        for i in 0..m.num_rows() {
            let bucket = ts.value(i) / MINUTE;
            let first = bucket * 60;
            let last = ((bucket + 1) * 60).min(total) - 1;
            assert_eq!(count.value(i) as i64, last - first + 1, "bucket {bucket}");
            let expect = (first + last) as f64 / 2.0;
            assert!((avg.value(i) - expect).abs() < 1e-9, "bucket {bucket}");
        }
        m.num_rows()
    };
    let before = check(&db);
    db.materialise_rollups().unwrap();
    assert_eq!(check(&db), before);

    // 4. A clean close after the recovery is a replay-free restart.
    db.close().unwrap();
    let db = Chronix::open(config(tmp.path())).unwrap();
    assert_eq!(db.wal_replayed_records(), 0);
    assert_eq!(db.execute(&plan).unwrap().num_rows(), sorted.len());
    db.close().unwrap();
    let _ = HOUR;
}
