//! Delete semantics: a tombstone must delete exactly what was asked for.
//!
//! Three separate properties, each of which was violated by a different part
//! of the delete path:
//!
//! 1. **A ranged delete deletes only its range.** `DeleteRequest` carries
//!    `time_start`/`time_end`, `Tombstone::ranged` exists, and both the
//!    segment filter and the compaction merge honour a time range — but
//!    nothing ever *constructed* a ranged tombstone, so every predicate
//!    delete erased the whole series regardless of the range asked for.
//!
//! 2. **A write after a delete is visible.** The write path never consulted
//!    the tombstone set, so re-creating a deleted series accepted the writes
//!    into the WAL and the memtable and then filtered every one of them out
//!    of every query — until an unrelated background GC pass happened to
//!    drop the tombstone, at which point the data reappeared.
//!
//! 3. **Both of the above survive a restart.** WAL replay reconstructed every
//!    tombstone as a full-series tombstone, so a correct ranged delete became
//!    a whole-series delete at the next open.
//!
//! These drive the public API, because the defect in each case was that two
//! code paths implemented one semantic and only one of them was right.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::collections::BTreeMap;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use tempfile::TempDir;

fn open_db(dir: &TempDir) -> Chronix {
    let config = ChronixConfig::builder()
        .data_dir(dir.path())
        .build()
        .unwrap();
    Chronix::open(config).unwrap()
}

fn point(host: &str, ts: i64) -> Point {
    let key = SeriesKey::new("cpu", tags! { "host" => host }).unwrap();
    Point::new(key, fields! { "v" => ts as f64 }, ts).unwrap()
}

/// Every timestamp still visible for `cpu`, sorted.
fn visible_timestamps(db: &Chronix) -> Vec<i64> {
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let Some(col) = batch.column_by_name(chronix_core::TIME_COLUMN) else {
        return Vec::new();
    };
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let mut out: Vec<i64> = (0..arr.len()).map(|i| arr.value(i)).collect();
    out.sort_unstable();
    out
}

fn host_tags(host: &str) -> BTreeMap<String, String> {
    tags! { "host" => host }
}

/// A predicate delete with a time range must delete only that range.
///
/// The request says "delete host=a between 2000 and 3000". Points at 1000 and
/// 4000 are outside that window and must survive.
#[test]
fn a_ranged_delete_leaves_points_outside_the_range() {
    let tmp = TempDir::new().unwrap();
    let db = open_db(&tmp);

    for ts in [1_000, 2_000, 3_000, 4_000] {
        db.insert(&point("a", ts)).unwrap();
    }
    db.flush().unwrap();

    let req = db
        .delete_builder()
        .measurement("cpu")
        .tag("host", "a")
        .range(2_000, 3_000)
        .build()
        .unwrap();
    let outcome = db.execute_delete(&req).unwrap();
    assert!(outcome.is_complete(), "delete must scan every segment");

    assert_eq!(
        visible_timestamps(&db),
        vec![1_000, 4_000],
        "a delete of [2000,3000] must not touch 1000 or 4000"
    );
}

/// The same property, but read back through `last_value`, which takes a
/// different code path from a range scan and had its own time-blind tombstone
/// check.
#[test]
fn a_ranged_delete_leaves_last_value_intact() {
    let tmp = TempDir::new().unwrap();
    let db = open_db(&tmp);

    for ts in [1_000, 2_000, 3_000, 4_000] {
        db.insert(&point("a", ts)).unwrap();
    }
    db.flush().unwrap();

    let req = db
        .delete_builder()
        .measurement("cpu")
        .tag("host", "a")
        .range(2_000, 3_000)
        .build()
        .unwrap();
    db.execute_delete(&req).unwrap();

    let last = db.last_value("cpu", &host_tags("a")).unwrap();
    assert_eq!(
        last.map(|p| p.timestamp()),
        Some(4_000),
        "the point at 4000 is outside the deleted range and is still the last value"
    );
}

/// A ranged delete must survive a restart as a *ranged* delete.
///
/// WAL replay rebuilt every tombstone as an unranged, whole-series tombstone,
/// so the points outside the deleted window came back deleted after reopening.
#[test]
fn a_ranged_delete_stays_ranged_across_a_restart() {
    let tmp = TempDir::new().unwrap();
    {
        let db = open_db(&tmp);
        for ts in [1_000, 2_000, 3_000, 4_000] {
            db.insert(&point("a", ts)).unwrap();
        }
        db.flush().unwrap();

        let req = db
            .delete_builder()
            .measurement("cpu")
            .tag("host", "a")
            .range(2_000, 3_000)
            .build()
            .unwrap();
        db.execute_delete(&req).unwrap();
        assert_eq!(visible_timestamps(&db), vec![1_000, 4_000]);
        db.close().unwrap();
    }

    let db = open_db(&tmp);
    assert_eq!(
        visible_timestamps(&db),
        vec![1_000, 4_000],
        "replaying the delete must restore the range, not a full-series tombstone"
    );
}

/// Writing to a series after deleting it must re-create it.
///
/// A decommissioned device that is re-provisioned under the same identity is
/// the ordinary case here. The write path never cleared the tombstone, so the
/// new points were accepted, written to the WAL, and then filtered out of
/// every read.
#[test]
fn writing_after_delete_series_recreates_the_series() {
    let tmp = TempDir::new().unwrap();
    let db = open_db(&tmp);

    db.insert(&point("a", 1_000)).unwrap();
    db.flush().unwrap();

    db.delete_series("cpu", &host_tags("a")).unwrap();
    assert!(
        visible_timestamps(&db).is_empty(),
        "the delete must take effect"
    );

    // Re-create the series.
    db.insert(&point("a", 5_000)).unwrap();
    assert_eq!(
        visible_timestamps(&db),
        vec![5_000],
        "a write after a delete must be visible, not silently swallowed"
    );

    assert_eq!(
        db.last_value("cpu", &host_tags("a"))
            .unwrap()
            .map(|p| p.timestamp()),
        Some(5_000),
        "last_value must see the re-created series too"
    );
}

/// The same, across a flush and a restart: the re-created data must be on
/// disk and must not be filtered out by a replayed tombstone.
#[test]
fn a_recreated_series_survives_flush_and_restart() {
    let tmp = TempDir::new().unwrap();
    {
        let db = open_db(&tmp);
        db.insert(&point("a", 1_000)).unwrap();
        db.flush().unwrap();
        db.delete_series("cpu", &host_tags("a")).unwrap();
        db.insert(&point("a", 5_000)).unwrap();
        db.flush().unwrap();
        assert_eq!(visible_timestamps(&db), vec![5_000]);
        db.close().unwrap();
    }

    let db = open_db(&tmp);
    assert_eq!(
        visible_timestamps(&db),
        vec![5_000],
        "the re-created point must survive replay; the tombstone predates it"
    );
}

/// A full-series delete with no range still deletes everything.
///
/// The regression guard for the fixes above: making deletes range-aware must
/// not weaken the unranged case.
#[test]
fn an_unranged_delete_still_removes_the_whole_series() {
    let tmp = TempDir::new().unwrap();
    let db = open_db(&tmp);

    for ts in [1_000, 2_000, 3_000, 4_000] {
        db.insert(&point("a", ts)).unwrap();
        db.insert(&point("b", ts)).unwrap();
    }
    db.flush().unwrap();

    let req = db
        .delete_builder()
        .measurement("cpu")
        .tag("host", "a")
        .build()
        .unwrap();
    db.execute_delete(&req).unwrap();

    assert_eq!(
        visible_timestamps(&db),
        vec![1_000, 2_000, 3_000, 4_000],
        "host=b is untouched and keeps all four points"
    );
    assert_eq!(
        db.last_value("cpu", &host_tags("a")).unwrap(),
        None,
        "host=a is fully deleted"
    );
}

/// A delete must survive the WAL being truncated.
///
/// This is the defect the other tests could not see. Tombstones lived only in
/// memory and in the data WAL, and the data WAL is truncated once the memtable
/// it covers has been flushed — so a delete followed by enough write traffic to
/// roll the WAL forward was simply gone at the next open, and every deleted row
/// came back. Tombstones are catalog state now, and the catalog is only ever
/// rewritten by a snapshot that carries them forward.
#[test]
fn a_delete_survives_wal_truncation_and_restart() {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfig::builder()
        .data_dir(tmp.path())
        // Small enough that the writes below roll the WAL past the delete.
        .wal_max_file_size(4096)
        .build()
        .unwrap();

    {
        let db = Chronix::open(config.clone()).unwrap();
        db.insert(&point("doomed", 1_000)).unwrap();
        db.flush().unwrap();
        db.delete_series("cpu", &host_tags("doomed")).unwrap();

        // Write and flush enough to roll the WAL past the delete's record.
        // Flushing as we go keeps the writer under its unflushed-file limit,
        // and is also what makes the truncation happen at all.
        for round in 0..20i64 {
            for ts in 0..20i64 {
                db.insert(&point("keeper", 10_000 + round * 20 + ts))
                    .unwrap();
            }
            db.flush().unwrap();
        }
        db.close().unwrap();
    }

    let db = Chronix::open(config).unwrap();
    assert_eq!(
        db.last_value("cpu", &host_tags("doomed")).unwrap(),
        None,
        "the delete must outlive the WAL record that carried it"
    );
    assert!(
        db.last_value("cpu", &host_tags("keeper"))
            .unwrap()
            .is_some(),
        "the surviving series must still be there — this is a delete, not a wipe"
    );
}

/// Reclaiming tombstones must not resurrect data.
///
/// The old rule dropped a tombstone as soon as its series left `known_series`,
/// which the delete itself had just arranged. It ran after every compaction
/// pass, including passes that never touched the segments holding the deleted
/// rows — so a delete could be undone by unrelated background work.
#[test]
fn reclaiming_tombstones_does_not_resurrect_deleted_rows() {
    let tmp = TempDir::new().unwrap();
    let db = open_db(&tmp);

    for ts in [1_000, 2_000, 3_000] {
        db.insert(&point("a", ts)).unwrap();
        db.insert(&point("b", ts)).unwrap();
    }
    db.flush().unwrap();

    db.delete_series("cpu", &host_tags("a")).unwrap();
    let before = visible_timestamps(&db);

    // Run the reclaim pass directly. It must be a no-op while the segments the
    // delete was issued against are still in the catalog.
    db.gc_tombstones();

    assert_eq!(
        visible_timestamps(&db),
        before,
        "reclaiming tombstones must not make deleted rows visible again"
    );
    assert_eq!(
        db.last_value("cpu", &host_tags("a")).unwrap(),
        None,
        "host=a stays deleted after a reclaim pass"
    );
}

/// The same, driven through a real compaction cycle rather than the reclaim
/// pass alone: compaction materialises the delete, so afterwards the rows are
/// gone from disk *and* stay invisible.
#[test]
fn a_delete_survives_compaction() {
    let tmp = TempDir::new().unwrap();
    let db = open_db(&tmp);

    for round in 0..4i64 {
        for ts in 0..50 {
            db.insert(&point("a", round * 1_000 + ts)).unwrap();
            db.insert(&point("b", round * 1_000 + ts)).unwrap();
        }
        db.flush().unwrap();
    }

    db.delete_series("cpu", &host_tags("a")).unwrap();
    db.compact().unwrap();
    db.gc_tombstones();

    assert_eq!(
        db.last_value("cpu", &host_tags("a")).unwrap(),
        None,
        "host=a must stay deleted through compaction and tombstone reclaim"
    );
    assert!(
        db.last_value("cpu", &host_tags("b")).unwrap().is_some(),
        "host=b is untouched"
    );
}

/// A ranged delete must not release the series from the cardinality budget.
///
/// The budget is the admission gate on the write path, so an under-count is as
/// much a correctness problem as an over-count: it lets a database hold more
/// live series than its limit allows. The series is still there — the delete
/// removed part of its history, not the series.
///
/// The bound that looks usable here and is not: the per-series maximum the
/// delete path collects is the maximum *inside the scanned window*, so
/// comparing it against the request's upper bound can never fail. Only an
/// absent bound proves the delete covered everything.
#[test]
fn a_ranged_delete_keeps_the_series_in_the_cardinality_budget() {
    let tmp = TempDir::new().unwrap();
    let db = open_db(&tmp);

    for ts in [1_000, 2_000, 3_000, 4_000] {
        db.insert(&point("a", ts)).unwrap();
    }
    db.flush().unwrap();
    assert_eq!(db.statistics().series_count, 1);

    // Delete an interval that leaves data on both sides.
    let req = db
        .delete_builder()
        .measurement("cpu")
        .tag("host", "a")
        .range(2_000, 3_000)
        .build()
        .unwrap();
    db.execute_delete(&req).unwrap();
    assert_eq!(
        db.statistics().series_count,
        1,
        "the series still holds data at 1000 and 4000"
    );

    // Delete everything up to 3000 — data remains at 4000, so the series is
    // still live even though the request named an upper bound at or above
    // every timestamp the scan saw.
    let req = db
        .delete_builder()
        .measurement("cpu")
        .tag("host", "a")
        .before(3_000)
        .build()
        .unwrap();
    db.execute_delete(&req).unwrap();
    assert_eq!(
        db.statistics().series_count,
        1,
        "a delete bounded above must not release a series that outlives it"
    );
    assert_eq!(
        db.last_value("cpu", &host_tags("a"))
            .unwrap()
            .map(|p| p.timestamp()),
        Some(4_000),
        "and the surviving point is still readable"
    );

    // An unbounded delete does release it.
    let req = db
        .delete_builder()
        .measurement("cpu")
        .tag("host", "a")
        .build()
        .unwrap();
    db.execute_delete(&req).unwrap();
    assert_eq!(
        db.statistics().series_count,
        0,
        "an unbounded delete returns the series to the budget"
    );
}

/// A point written after a delete is visible at once, even inside the
/// deleted interval: a tombstone masks the segments the delete was issued
/// against, never data written later. Backfilling into a deleted hour used
/// to be accepted, acknowledged and invisible until compaction happened to
/// reclaim the tombstone.
#[test]
fn a_write_after_a_delete_is_visible_inside_the_deleted_interval() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir);
    for ts in [1000, 2000, 3000] {
        db.insert(&point("a", ts)).unwrap();
    }
    db.flush().unwrap();
    db.execute_delete(&chronix::DeleteRequest {
        measurement: "cpu".into(),
        tag_filters: vec![("host".into(), "a".into())],
        time_start: Some(1500),
        time_end: Some(2500),
    })
    .unwrap();
    assert_eq!(visible_timestamps(&db), vec![1000, 3000]);

    // Re-write inside the deleted interval: visible from the memtable...
    db.backfill(&[point("a", 2000)])
        .unwrap()
        .into_complete()
        .unwrap();
    assert_eq!(visible_timestamps(&db), vec![1000, 2000, 3000]);
    // ...and from its own segment, before and after compaction reclaims
    // the tombstone.
    db.flush().unwrap();
    assert_eq!(visible_timestamps(&db), vec![1000, 2000, 3000]);
    db.compact().unwrap();
    db.gc().unwrap();
    assert_eq!(visible_timestamps(&db), vec![1000, 2000, 3000]);
    db.close().unwrap();
}

/// An unclean restart replays writes *over* a tombstone correctly.
///
/// The two halves of a restart meet here, and no other test puts them
/// together: replay re-inserts every write record above the WAL floor, and
/// the tombstone that must not mask them comes from the catalog. A series is
/// deleted, then written to again — which re-creates it, visible at once,
/// even inside the interval the delete covered — and the process aborts
/// before any of that reaches a segment.
///
/// **There is no WAL record for a delete, and there cannot usefully be one.**
/// `execute_delete` flushes first, so by the time a tombstone exists every
/// point it could cover is already in a segment and below the WAL floor —
/// which is never replayed. A `WalEntry::Delete` used to be written and
/// fsynced on every delete anyway; writing this test is what showed it had no
/// reachable consumer, because deleting the replay path that read it left
/// every assertion green. It is gone, and with it a second `fsync` per
/// delete.
///
/// The child aborts rather than returning, because `Drop` runs `close()` and
/// a clean close truncates the WAL past everything this needs replayed.
#[test]
fn an_unclean_restart_replays_writes_over_a_tombstone() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();

    let config = |path: &std::path::Path| {
        ChronixConfig::builder()
            .data_dir(path)
            // The maintenance thread must not flush behind this test's back:
            // the whole point is data that is still only in the WAL.
            .maintenance_interval(std::time::Duration::from_secs(86_400 * 365))
            .build()
            .unwrap()
    };

    if std::env::var("CHRONIX_TOMBSTONE_REPLAY_CHILD").is_ok() {
        let dir = std::path::PathBuf::from(std::env::var("CHRONIX_TOMBSTONE_REPLAY_DIR").unwrap());
        let db = Chronix::open(config(&dir)).unwrap();
        for ts in 0..20i64 {
            db.insert(&point("doomed", 1_000 + ts)).unwrap();
            db.insert(&point("keeper", 1_000 + ts)).unwrap();
        }
        // Deletes the whole series — and flushes on the way, which is why a
        // WAL record for the tombstone could never have been needed.
        db.delete_series("cpu", &host_tags("doomed")).unwrap();
        assert_eq!(db.last_value("cpu", &host_tags("doomed")).unwrap(), None);

        // Re-create the deleted series, and add to the survivor. Neither
        // reaches a segment: this is what replay has to bring back.
        for ts in 0..20i64 {
            db.insert(&point("doomed", 5_000 + ts)).unwrap();
            db.insert(&point("keeper", 5_000 + ts)).unwrap();
        }
        std::process::abort();
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "an_unclean_restart_replays_writes_over_a_tombstone",
            "--exact",
            "--nocapture",
        ])
        .env("CHRONIX_TOMBSTONE_REPLAY_CHILD", "1")
        .env("CHRONIX_TOMBSTONE_REPLAY_DIR", &dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort, not exit cleanly");

    let db = Chronix::open(config(&dir)).unwrap();
    assert!(
        db.wal_replayed_records() > 0,
        "the point of this test is that there was something to replay"
    );

    // The re-created series is back, at its new timestamps only.
    let doomed = db
        .last_value("cpu", &host_tags("doomed"))
        .unwrap()
        .expect("a write after a delete re-creates the series");
    assert!(
        doomed.timestamp() >= 5_000,
        "the replayed write is the newest point, not a resurrected one: {}",
        doomed.timestamp()
    );
    let doomed_seen = visible_timestamps_for(&db, "doomed");
    assert!(
        doomed_seen.iter().all(|ts| *ts >= 5_000),
        "the tombstone still masks the deleted range after replay: {doomed_seen:?}"
    );
    assert_eq!(doomed_seen.len(), 20);

    // And the survivor kept both halves.
    assert_eq!(visible_timestamps_for(&db, "keeper").len(), 40);
    db.close().unwrap();
}

/// Every visible timestamp of one host, oldest first — read from an
/// **unfiltered** scan.
///
/// Deliberately not `.tag("host", host)`: a whole-series delete rewrites each
/// segment's series sidecar, so the segment's bloom filter stops claiming the
/// deleted series and a tag-filtered query prunes the segment away before any
/// row is read. That is correct, and it is also a second mechanism that hides
/// the deleted rows — so a tag-filtered assertion passes whether or not the
/// tombstone survived, which is not what this file is for. Scanning
/// everything and splitting on the tag column leaves the tombstone as the
/// only thing that can mask a row.
fn visible_timestamps_for(db: &Chronix, host: &str) -> Vec<i64> {
    let plan = db
        .query()
        .measurement("cpu")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let (Some(time), Some(hosts)) = (
        batch.column_by_name(chronix_core::TIME_COLUMN),
        batch.column_by_name("host"),
    ) else {
        return Vec::new();
    };
    let time = time
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let hosts = hosts
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let mut out: Vec<i64> = (0..time.len())
        .filter(|i| hosts.value(*i) == host)
        .map(|i| time.value(i))
        .collect();
    out.sort_unstable();
    out
}
