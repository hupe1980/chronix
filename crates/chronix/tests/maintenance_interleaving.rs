#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Every destructive background pass, running at once, against live writes.
//!
//! `integration::concurrent_write_flush_compact_query_stress` covers writes,
//! flush, compaction and reads. The passes that *remove* data — delete,
//! retention, garbage collection, rollup materialisation — have only ever run
//! one at a time, with the database quiet around them, and they are exactly
//! the ones whose defects three earlier audit passes found: a derived value
//! computed from an input that kept moving — a watermark, a tombstone's
//! segment set, retention's view of which shards have expired.
//!
//! The invariant is the one a user has: **a point that was acknowledged and
//! not deleted is readable, and a point that was deleted stays deleted.**

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const SEC: i64 = 1_000_000_000;

/// A point of `load{host=…}` at `ts`.
fn point(host: &str, ts: i64, v: f64) -> Point {
    Point::new(
        SeriesKey::new("load", tags! { "host" => host }).unwrap(),
        fields! { "value" => v },
        ts,
    )
    .unwrap()
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn count_rows(db: &Chronix) -> usize {
    let plan = db
        .query()
        .measurement("load")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    db.execute(&plan).unwrap().num_rows()
}

/// Writers, a deleter, and every background pass, all at once.
///
/// Each writer owns a disjoint host and a disjoint timestamp range, so the
/// expected row count is exact: nothing overwrites anything, and the only
/// removals are the ones the deleter asks for.
#[test]
fn concurrent_maintenance_never_loses_an_acknowledged_write() {
    const WRITERS: usize = 4;
    const PER_WRITER: usize = 300;

    let tmp = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(8 * 1024)
        // The maintenance thread runs its own passes beside the ones the
        // test drives, which is the point: the interleavings a user gets are
        // not the ones a test schedules.
        .maintenance_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());

    // A base far enough in the past that nothing lands outside the
    // out-of-order window, and recent enough that retention can be given a
    // horizon that expires nothing.
    let base = now_nanos() - 3_600 * SEC;

    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for w in 0..WRITERS {
        let db = Arc::clone(&db);
        let written = Arc::clone(&written);
        handles.push(std::thread::spawn(move || {
            let host = format!("h{w}");
            for i in 0..PER_WRITER {
                let ts = base + (w as i64) * 1_000_000 * SEC / 1_000_000 + (i as i64) * SEC;
                // Counted *before* the insert, so `written` is an upper
                // bound on what any query can see. Counting after made the
                // reader's check `seen <= written` false by one whenever a
                // query ran between an insert returning and its increment —
                // the assertion was racy, not the database.
                written.fetch_add(1, Ordering::Relaxed);
                db.insert(&point(&host, ts, i as f64)).unwrap();
            }
        }));
    }

    // ── Background passes ────────────────────────────────────────────
    let mut background = Vec::new();
    for (name, pass) in [
        ("flush", 0u8),
        ("compact", 1),
        ("gc", 2),
        ("retention", 3),
        ("rollups", 4),
    ] {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        background.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let r: Result<(), chronix::DbError> = match pass {
                    0 => db.flush().map(|_| ()),
                    1 => db.compact().map(|_| ()),
                    2 => db.gc().map(|_| ()),
                    // A horizon of a year expires nothing, so retention is
                    // running its whole scan-and-decide path against a
                    // catalog that is changing underneath it without being
                    // licensed to remove anything.
                    3 => db
                        .enforce_retention(Duration::from_secs(365 * 24 * 3_600))
                        .map(|_| ()),
                    _ => db.materialise_rollups().map(|_| ()),
                };
                assert!(r.is_ok(), "{name} failed under load: {:?}", r.err());
                std::thread::sleep(Duration::from_millis(2));
            }
        }));
    }

    // A reader, asserting only that a query never fails and never sees more
    // rows than have been attempted.
    //
    // The ordering of these two reads is the whole invariant, and both ways of
    // getting it wrong were tried. A writer counts *before* it inserts, so a
    // row that exists has already been counted; the query must therefore
    // finish **before** the counter is read, or writers that landed during the
    // scan are visible in `seen` and missing from `attempted`. Count after
    // insert and read the counter first, and it fails the other way.
    {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let written = Arc::clone(&written);
        background.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let seen = count_rows(&db);
                let attempted = written.load(Ordering::Relaxed);
                assert!(
                    seen <= attempted,
                    "a query returned {seen} rows against {attempted} attempted writes"
                );
                std::thread::sleep(Duration::from_millis(3));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for h in background {
        h.join().unwrap();
    }

    db.flush().unwrap();
    let total = WRITERS * PER_WRITER;
    assert_eq!(
        count_rows(&db),
        total,
        "every acknowledged write must survive the background passes"
    );
}

/// A delete issued while every background pass is running stays deleted.
///
/// The tombstone lifecycle is the part three audit passes found defects in,
/// and each fix was verified with the database quiet. Compaction rewrites the
/// segments a tombstone names, garbage collection reclaims tombstones whose
/// segments have left the catalog, and retention drops segments outright —
/// so the three of them running *while* a delete is applied is the
/// interleaving the design is subtle about.
#[test]
fn a_delete_survives_concurrent_compaction_gc_and_retention() {
    const HOSTS: usize = 6;
    const PER_HOST: i64 = 200;

    let tmp = tempfile::tempdir().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        .memtable_flush_threshold(8 * 1024)
        .maintenance_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let base = now_nanos() - 3_600 * SEC;

    let mut points = Vec::new();
    for h in 0..HOSTS {
        for i in 0..PER_HOST {
            points.push(point(&format!("h{h}"), base + i * SEC, i as f64));
        }
    }
    db.insert_batch(&points).unwrap().into_complete().unwrap();
    db.flush().unwrap();
    assert_eq!(count_rows(&db), HOSTS * PER_HOST as usize);

    let stop = Arc::new(AtomicBool::new(false));
    let mut background = Vec::new();
    for pass in 0u8..4 {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        background.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let r: Result<(), chronix::DbError> = match pass {
                    0 => db.flush().map(|_| ()),
                    1 => db.compact().map(|_| ()),
                    2 => db.gc().map(|_| ()),
                    _ => db
                        .enforce_retention(Duration::from_secs(365 * 24 * 3_600))
                        .map(|_| ()),
                };
                assert!(r.is_ok(), "pass {pass} failed: {:?}", r.err());
                std::thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    // Delete every host's first half, one host at a time, while the passes run.
    for h in 0..HOSTS {
        let req = db
            .delete_builder()
            .measurement("load")
            .tag("host", &format!("h{h}"))
            .range(base, base + (PER_HOST / 2) * SEC)
            .build()
            .unwrap();
        let outcome = db.execute_delete(&req).unwrap();
        assert_eq!(
            outcome.segments_skipped, 0,
            "delete for h{h} skipped segments: {outcome:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    stop.store(true, Ordering::Relaxed);
    for h in background {
        h.join().unwrap();
    }

    // The delete range is **inclusive at both ends**, as `DeleteRequest`
    // documents and as `QueryBuilder::range` is: `[base, base + 100s]` covers
    // the points at offsets 0..=100, so 101 of each host's 200 go.
    db.flush().unwrap();
    db.compact().unwrap();
    let expected = HOSTS * (PER_HOST as usize - 101);
    assert_eq!(
        count_rows(&db),
        expected,
        "a delete must not be undone, and must not take more than it asked for"
    );

    // And it must still hold after a restart: a tombstone that only lives in
    // memory reads as a successful delete until the process ends.
    let dir = tmp.path().to_path_buf();
    drop(db);
    let db = Chronix::open(ChronixConfig::builder().data_dir(&dir).build().unwrap()).unwrap();
    assert_eq!(
        count_rows(&db),
        expected,
        "the delete must survive a restart"
    );
}
