#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! What a reader sees while the database changes underneath it.
//!
//! `maintenance_interleaving` asks whether a **write** survives the
//! background passes. This file asks the other half: whether an *answer*
//! does. Both defects it pins were invisible to a suite of three thousand
//! tests, because every one of them either quiets the database around the
//! query or asserts a bound the wrong way round — the reader in
//! `concurrent_maintenance_never_loses_an_acknowledged_write` asserts
//! `seen <= attempted`, which a result of zero rows satisfies.
//!
//! The two properties:
//!
//! 1. **A scan is answered from the segment set it was given.** Retention,
//!    a drop, archiving and a compaction all retire segment files; a scan
//!    holding one of those paths must still be able to open it.
//! 2. **A tag filter narrows an answer, it does not empty it.** The tag
//!    index is derived state updated *after* the catalog, so a segment can
//!    be queryable and unindexed. Reading "not in the index" as "cannot
//!    match" made a compaction's catalog swap a window in which every
//!    tag-filtered query returned no rows at all — and on a tenanted server
//!    every query carries a `__namespace__` filter.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const SEC: i64 = 1_000_000_000;
const HOUR: i64 = 3_600 * SEC;
const DAY: i64 = 24 * HOUR;

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

/// A database whose maintenance thread never fires, so each test drives the
/// pass it is about.
fn open(dir: &tempfile::TempDir, shard: Duration, retention: Option<Duration>) -> Arc<Chronix> {
    Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                .shard_duration(shard)
                .retention(retention)
                .maintenance_interval(Duration::from_secs(86_400 * 365))
                .build()
                .unwrap(),
        )
        .unwrap(),
    )
}

/// Retention runs while a scan is part-way through its buckets.
///
/// The scan snapshots the catalog once and opens each segment on the `next()`
/// that first needs it. Retention removed the entry and unlinked the file in
/// one step, so the scan's next bucket failed with a bare
/// `No such file or directory` — which reaches a client as
/// `500 an internal error occurred`, on a gateway where the maintenance pass
/// runs every thirty seconds and an export takes longer than that.
#[test]
fn a_scan_in_flight_outlives_a_retention_pass() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(
        &dir,
        Duration::from_secs(3600),
        Some(Duration::from_secs(2 * 86_400)),
    );

    let now = now_ns();
    let base = now - 5 * DAY;
    let key = SeriesKey::new("cpu", tags! { "host" => "h1" }).unwrap();
    for h in 0..10i64 {
        let pts: Vec<Point> = (0..60)
            .map(|m| {
                Point::new(
                    key.clone(),
                    fields! { "v" => 1.0 },
                    base + h * HOUR + m * 60 * SEC,
                )
                .unwrap()
            })
            .collect();
        assert!(db.insert_batch(&pts).unwrap().is_complete());
        db.flush().unwrap();
    }
    // One fresh point, so the retention reference is the wall clock rather
    // than the newest row this database holds — retention measures age from
    // `min(now, newest held)`, and without this every row would be equally
    // old and nothing would expire.
    let fresh = SeriesKey::new("cpu", tags! { "host" => "h2" }).unwrap();
    assert!(db
        .insert_batch(&[Point::new(fresh, fields! { "v" => 1.0 }, now).unwrap()])
        .unwrap()
        .is_complete());
    db.flush().unwrap();

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let mut stream = db.execute_iter(&plan).unwrap();
    let mut rows = stream.next().unwrap().unwrap().num_rows();

    // Everything but the fresh point is now past the cutoff.
    let result = db
        .enforce_retention(Duration::from_secs(2 * 86_400))
        .unwrap();
    assert!(
        result.segments_deleted > 0,
        "the pass has to remove something for this test to mean anything"
    );
    assert!(
        result.segments_awaiting_readers > 0,
        "the scan holds those segments, so their files cannot have gone yet"
    );
    assert_eq!(
        result.bytes_freed, 0,
        "nothing was unlinked, so no bytes were freed — a pass that says \
         otherwise is the one an operator reads when the disk will not shrink"
    );

    for batch in stream {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(
        rows, 601,
        "the scan must be answered from the set it was given"
    );

    // Once the reader has gone the files go, on the next collection.
    assert!(db.gc().unwrap() > 0);
    let after = db.execute(&plan).unwrap().num_rows();
    assert_eq!(after, 1, "a new query sees only what retention left");
}

/// Tag-filtered reads, running against a compaction.
///
/// Before the fix this returned **zero rows** for 36 % of the queries issued
/// during a compaction pass: the catalog swap makes the output segment
/// active before the tag index knows it exists, and a lookup that answers
/// "the segments that match" prunes everything it has not indexed.
#[test]
fn a_tag_filtered_read_stays_complete_across_a_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, Duration::from_secs(86_400), None);

    let key = SeriesKey::new("cpu", tags! { "host" => "h1", "region" => "eu" }).unwrap();
    let base = 1_700_000_000 * SEC;
    const SEGMENTS: i64 = 16;
    const PER_SEGMENT: i64 = 50;
    let expected = usize::try_from(SEGMENTS * PER_SEGMENT).unwrap();
    for seg in 0..SEGMENTS {
        let pts: Vec<Point> = (0..PER_SEGMENT)
            .map(|i| {
                let n = seg * PER_SEGMENT + i;
                Point::new(key.clone(), fields! { "v" => n as f64 }, base + n * SEC).unwrap()
            })
            .collect();
        assert!(db.insert_batch(&pts).unwrap().is_complete());
        db.flush().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let readers: Vec<_> = (0..3)
        .map(|_| {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let bad = Arc::clone(&bad);
            std::thread::spawn(move || {
                // A partial tag filter — one of the measurement's two tags —
                // because that is the shape every server query has.
                let plan = db
                    .query()
                    .measurement("cpu")
                    .tag("host", "h1")
                    .range(0, i64::MAX)
                    .build()
                    .unwrap();
                let mut issued = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    issued += 1;
                    match db.execute(&plan) {
                        Ok(b) if b.num_rows() == expected => {}
                        Ok(b) => bad
                            .lock()
                            .unwrap()
                            .push(format!("{} rows, expected {expected}", b.num_rows())),
                        Err(e) => bad.lock().unwrap().push(format!("failed: {e}")),
                    }
                }
                issued
            })
        })
        .collect();

    // Compact, and keep feeding it new L0 segments to compact.
    for _ in 0..40 {
        let _ = db.compact().unwrap();
        for seg in 0..4i64 {
            let pts: Vec<Point> = (0..10)
                .map(|i| {
                    let n = seg * PER_SEGMENT + i;
                    Point::new(key.clone(), fields! { "v" => n as f64 }, base + n * SEC).unwrap()
                })
                .collect();
            assert!(db.insert_batch(&pts).unwrap().is_complete());
            db.flush().unwrap();
        }
    }
    stop.store(true, Ordering::Relaxed);
    let issued: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();

    let bad = bad.lock().unwrap();
    assert!(
        bad.is_empty(),
        "{} of {issued} concurrent reads were wrong; first three: {:?}",
        bad.len(),
        &bad[..bad.len().min(3)]
    );
    assert!(
        issued > 100,
        "only {issued} reads — the race had no room to show"
    );
}

/// A `drop_measurement` under a running scan of that measurement.
///
/// The drop unlinks every segment file it can; the scan already holds them.
#[test]
fn a_scan_outlives_a_drop_of_its_own_measurement() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, Duration::from_secs(3600), None);

    let key = SeriesKey::new("cpu", tags! { "host" => "h1" }).unwrap();
    let base = 1_700_000_000 * SEC;
    for seg in 0..6i64 {
        let pts: Vec<Point> = (0..20)
            .map(|i| {
                Point::new(
                    key.clone(),
                    fields! { "v" => 1.0 },
                    base + seg * HOUR + i * SEC,
                )
                .unwrap()
            })
            .collect();
        assert!(db.insert_batch(&pts).unwrap().is_complete());
        db.flush().unwrap();
    }

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let mut stream = db.execute_iter(&plan).unwrap();
    let mut rows = stream.next().unwrap().unwrap().num_rows();

    db.drop_measurement("cpu").unwrap();

    for batch in stream {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(
        rows, 120,
        "the scan keeps the answer it was already reading"
    );

    // And the drop really dropped.
    assert!(db.schema("cpu").is_none());
    assert_eq!(db.statistics().leased_segments, 0, "the scan has finished");
    assert_eq!(
        db.gc().unwrap(),
        6,
        "every file the drop could not unlink goes on the next collection"
    );
}

/// No lease outlives the stream that took it.
///
/// A leaked lease is a segment file that is never reclaimed, so the count is
/// reported by `statistics()` and asserted here at both ends — including for
/// a `LIMIT` that abandons its remaining buckets.
#[test]
fn a_finished_or_abandoned_scan_holds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir, Duration::from_secs(3600), None);

    let key = SeriesKey::new("cpu", tags! { "host" => "h1" }).unwrap();
    let base = 1_700_000_000 * SEC;
    for seg in 0..4i64 {
        let pts: Vec<Point> = (0..20)
            .map(|i| {
                Point::new(
                    key.clone(),
                    fields! { "v" => 1.0 },
                    base + seg * HOUR + i * SEC,
                )
                .unwrap()
            })
            .collect();
        assert!(db.insert_batch(&pts).unwrap().is_complete());
        db.flush().unwrap();
    }
    assert_eq!(db.statistics().leased_segments, 0);

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();

    {
        let mut stream = db.execute_iter(&plan).unwrap();
        let _ = stream.next();
        assert!(
            db.statistics().leased_segments > 0,
            "a live stream holds its snapshot"
        );
    }
    assert_eq!(db.statistics().leased_segments, 0, "dropped mid-scan");

    // Run to completion.
    let _ = db.execute(&plan).unwrap();
    assert_eq!(db.statistics().leased_segments, 0);

    // A LIMIT stops early and never reaches the later buckets.
    let limited = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .limit(1)
        .build()
        .unwrap();
    let _ = db.execute(&limited).unwrap();
    assert_eq!(db.statistics().leased_segments, 0);
}
