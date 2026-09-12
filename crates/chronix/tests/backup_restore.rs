//! What an operator gets back.
//!
//! Every test here drives `backup()` → `restore()` → `open()` and asks the
//! restored database for the rows the original held. The suite's existing
//! round-trip never flushed, so it exercised a backup with **no segments at
//! all** — the WAL replayed and the 50 rows came back. Everything below
//! writes enough to flush first, which is the only shape a real backup has.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::collections::BTreeMap;
use std::path::Path;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};
use tempfile::TempDir;

fn open(dir: &Path) -> Chronix {
    Chronix::open(ChronixConfig::builder().data_dir(dir).build().unwrap()).unwrap()
}

fn point(i: i64) -> Point {
    Point::new(
        SeriesKey::new("cpu", tags! { "host" => "a" }).unwrap(),
        fields! { "v" => i as f64 },
        i * 1_000_000,
    )
    .unwrap()
}

fn count(db: &Chronix) -> usize {
    let plan = db
        .query()
        .measurement("cpu")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    db.execute(&plan).unwrap().num_rows()
}

/// Write `n` points and flush them into segments.
fn seed(db: &Chronix, n: i64) {
    let points: Vec<Point> = (0..n).map(point).collect();
    db.insert_batch(&points).unwrap().into_complete().unwrap();
    db.flush().unwrap();
}

/// The rows come back.
///
/// A backup whose segments are on disk rather than in the WAL used to restore
/// as an **empty** database: the catalog records each segment by absolute
/// path, so every entry still named the original data directory, and the open
/// sweep that removes files the catalog does not know deleted every restored
/// `.csx` before the first query.
#[test]
fn a_restored_backup_holds_the_rows_the_original_did() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");

    let db = open(&data);
    seed(&db, 200);
    assert_eq!(count(&db), 200, "the original holds its own rows");
    db.backup(&backup).unwrap();
    db.close().unwrap();

    Chronix::restore(&backup, &restored).unwrap();
    let db2 = open(&restored);
    assert_eq!(count(&db2), 200, "the restored database holds them too");
    db2.close().unwrap();
}

/// Disaster recovery: the directory the backup was taken from is gone.
///
/// This is the only shape that matters — a backup restored while its source
/// still exists can read the source's files and look like it worked.
#[test]
fn a_restore_does_not_depend_on_the_directory_it_was_taken_from() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");

    let db = open(&data);
    seed(&db, 200);
    db.backup(&backup).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(&data).unwrap();

    Chronix::restore(&backup, &restored).unwrap();
    let db2 = open(&restored);
    assert_eq!(count(&db2), 200);
    db2.close().unwrap();
}

/// A restored database reads its **own** files.
///
/// Restoring beside a live database must not give the copy a catalog that
/// points into the original's segments: the two then share files, and
/// whichever compacts first unlinks the other's data.
#[test]
fn a_restored_database_does_not_share_files_with_the_original() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");

    let db = open(&data);
    seed(&db, 200);
    db.backup(&backup).unwrap();

    Chronix::restore(&backup, &restored).unwrap();
    let copy = open(&restored);
    assert_eq!(count(&copy), 200);

    // The structural claim, checked directly: every path the copy's catalog
    // resolves is inside the copy. Segment files are hard-linked, so the two
    // databases may share *inodes* — that is safe precisely because a segment
    // is never rewritten, only unlinked, and an unlink drops one name.
    // Sharing a *path* is what is not safe.
    {
        let cat = copy.catalog().read();
        for entry in cat.all_segments() {
            let path = entry.file.resolve(&restored.join("segments"));
            assert!(
                path.starts_with(&restored),
                "the copy's catalog names {}, which is outside {}",
                path.display(),
                restored.display()
            );
            assert!(path.exists(), "{} must be there", path.display());
        }
    }

    // Delete everything from the copy and reclaim its files.
    let req = copy.delete_builder().measurement("cpu").build().unwrap();
    copy.execute_delete(&req).unwrap();
    copy.compact().unwrap();
    copy.gc().unwrap();
    copy.close().unwrap();

    assert_eq!(
        count(&db),
        200,
        "the original must be untouched by what the copy deleted"
    );
    db.close().unwrap();
}

/// Everything acknowledged before `backup()` was called is in the backup,
/// even though the database went on working throughout.
///
/// `backup()` used to walk `wal/`, `segments/` and `catalog/` with `read_dir`
/// while all three were live. Three ways that loses data, and none of them is
/// reported:
///
/// - a WAL file truncated by the flush that followed it vanished between the
///   directory listing and the copy, failing the whole backup with a bare
///   `ENOENT`;
/// - a catalog snapshot landing mid-copy replaces `manifest.snapshot.bin` and
///   **truncates** `manifest.wal`, so the copy can pair an old snapshot with
///   an emptied log;
/// - a segment flushed between the `segments/` walk and the `catalog/` walk is
///   named by the copied catalog and absent from the copied directory.
///
/// The second thread is a maintenance pass, and it is the one that needs the
/// [segment lease](chronix/src/db/leases.rs): `gc()` unlinks the files of
/// retired segments, and a backup that holds no lease is a reader nothing is
/// counting.
#[test]
fn a_backup_taken_under_load_contains_every_acknowledged_write() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let db = std::sync::Arc::new(open(&data));
    seed(&db, 500);

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let db = std::sync::Arc::clone(&db);
        let stop = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut i = 500i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let batch: Vec<Point> = (i..i + 50).map(point).collect();
                let _ = db.insert_batch(&batch);
                let _ = db.flush();
                i += 50;
            }
        })
    };
    let maintainer = {
        let db = std::sync::Arc::clone(&db);
        let stop = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = db.compact();
                let _ = db.gc();
            }
        })
    };

    for round in 0..8 {
        let before = count(&db);
        let target = dir.path().join(format!("backup-{round}"));
        db.backup(&target)
            .unwrap_or_else(|e| panic!("round {round}: backup failed: {e}"));
        let restored = dir.path().join(format!("restored-{round}"));
        Chronix::restore(&target, &restored)
            .unwrap_or_else(|e| panic!("round {round}: restore refused the backup: {e}"));
        let copy = open(&restored);
        let got = count(&copy);
        copy.close().unwrap();
        assert!(
            got >= before,
            "round {round}: {before} rows were acknowledged before the backup began, \
             the restored copy holds {got}"
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    maintainer.join().unwrap();
    db.close().unwrap();
}

/// A backup taken twice into the same directory is the newer database, not
/// the union of both.
#[test]
fn a_second_backup_into_the_same_directory_replaces_the_first() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");

    let db = open(&data);
    seed(&db, 100);
    db.backup(&backup).unwrap();

    let more: Vec<Point> = (100..300).map(point).collect();
    db.insert_batch(&more).unwrap().into_complete().unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    db.gc().unwrap();
    let manifest = db.backup(&backup).unwrap();
    db.close().unwrap();

    let restored = dir.path().join("restored");
    let read_back = Chronix::restore(&backup, &restored).unwrap();
    assert_eq!(read_back.file_count, manifest.file_count);
    let db2 = open(&restored);
    assert_eq!(count(&db2), 300);
    db2.close().unwrap();
}

/// A tombstone survives the round trip: a delete is not undone by a restore.
#[test]
fn a_delete_survives_a_backup_and_restore() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");

    let db = open(&data);
    seed(&db, 200);
    let req = db
        .delete_builder()
        .measurement("cpu")
        .range(0, 99 * 1_000_000)
        .build()
        .unwrap();
    db.execute_delete(&req).unwrap();
    assert_eq!(count(&db), 100);
    db.backup(&backup).unwrap();
    db.close().unwrap();

    Chronix::restore(&backup, &restored).unwrap();
    let db2 = open(&restored);
    assert_eq!(count(&db2), 100, "the delete must not be undone");
    db2.close().unwrap();
}

/// The schema travels with the data.
#[test]
fn the_restored_database_knows_its_columns() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");

    let db = open(&data);
    seed(&db, 200);
    let before: Vec<String> = db
        .schema("cpu")
        .unwrap()
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();
    db.backup(&backup).unwrap();
    db.close().unwrap();

    Chronix::restore(&backup, &restored).unwrap();
    let db2 = open(&restored);
    let after: Vec<String> = db2
        .schema("cpu")
        .expect("the restored database knows the measurement")
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(before, after);
    db2.close().unwrap();
}

/// A backup directory missing a file the catalog names is refused, rather
/// than opening as a database that fails at the first query.
#[test]
fn a_restore_refuses_an_incomplete_backup() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");

    let db = open(&data);
    seed(&db, 200);
    db.backup(&backup).unwrap();
    db.close().unwrap();

    // Remove one segment file from the backup.
    let mut removed = false;
    for shard in std::fs::read_dir(backup.join("segments"))
        .unwrap()
        .flatten()
    {
        if !shard.path().is_dir() {
            continue;
        }
        for f in std::fs::read_dir(shard.path()).unwrap().flatten() {
            if f.path().extension().is_some_and(|e| e == "csx") {
                std::fs::remove_file(f.path()).unwrap();
                removed = true;
                break;
            }
        }
        if removed {
            break;
        }
    }
    assert!(removed, "the backup must contain at least one segment");

    let restored = dir.path().join("restored");
    assert!(
        Chronix::restore(&backup, &restored).is_err(),
        "an incomplete backup must be refused"
    );
}

/// A backup can be checked without restoring it.
///
/// The standing advice for any backup scheme is to verify the backups you are
/// keeping *before* you need them, and a backup that can only be checked by
/// restoring it is one nobody checks. This is the same verification a restore
/// runs, offered on its own — no target directory, no running database.
#[test]
fn a_backup_can_be_verified_without_being_restored() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");

    let db = open(&data);
    seed(&db, 200);
    let taken = db.backup(&backup).unwrap();
    db.close().unwrap();

    let checked = Chronix::verify_backup(&backup).expect("a complete backup verifies");
    assert_eq!(checked.segments, taken.segments);
    assert_eq!(checked.created_at, taken.created_at);

    // Truncating one segment is caught, and the file is named.
    let seg = std::fs::read_dir(backup.join("segments"))
        .unwrap()
        .flatten()
        .find(|e| e.path().is_dir())
        .map(|shard| {
            std::fs::read_dir(shard.path())
                .unwrap()
                .flatten()
                .find(|f| f.path().extension().is_some_and(|e| e == "csx"))
                .unwrap()
                .path()
        })
        .expect("a segment");
    let short = std::fs::read(&seg).unwrap();
    std::fs::write(&seg, &short[..short.len() / 2]).unwrap();

    let err = Chronix::verify_backup(&backup).expect_err("a truncated segment is not a backup");
    let message = err.to_string();
    assert!(message.contains("truncated"), "{message}");
    assert!(
        message.contains(seg.file_name().unwrap().to_str().unwrap()),
        "the message has to name the file: {message}"
    );

    // And a directory that is not a backup at all.
    assert!(Chronix::verify_backup(dir.path()).is_err());
}

/// `BackupManifest` counts what the backup holds, not what a directory walk
/// happened to see.
#[test]
fn the_manifest_counts_the_segments_it_captured() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");

    let db = open(&data);
    seed(&db, 200);
    let expected = db.catalog().read().segment_count();
    let manifest = db.backup(&backup).unwrap();
    db.close().unwrap();

    assert_eq!(manifest.segments as usize, expected);
    assert!(manifest.total_bytes > 0);
}

/// Two decimals, because a restore that loses the column scale is a restore
/// that loses the number.
#[test]
fn a_decimal_column_survives_the_round_trip() {
    use chronix_core::{Decimal, FieldValue};

    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");

    let db = open(&data);
    let key = SeriesKey::new("meter", tags! { "id" => "m1" }).unwrap();
    let points: Vec<Point> = (0..200)
        .map(|i| {
            let mut f = BTreeMap::new();
            f.insert(
                "register".to_string(),
                FieldValue::Decimal(Decimal::new(1_234_500 + i128::from(i), 3).unwrap()),
            );
            Point::new(key.clone(), f, i * 1_000_000).unwrap()
        })
        .collect();
    db.insert_batch(&points).unwrap().into_complete().unwrap();
    db.flush().unwrap();
    db.backup(&backup).unwrap();
    db.close().unwrap();

    Chronix::restore(&backup, &restored).unwrap();
    let db2 = open(&restored);
    let plan = db2
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let batch = db2.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 200);
    let col = batch.column_by_name("register").unwrap();
    assert!(
        matches!(
            col.data_type(),
            arrow::datatypes::DataType::Decimal128(38, 3)
        ),
        "scale must survive: {:?}",
        col.data_type()
    );
    db2.close().unwrap();
}

/// A checkpoint into the database's own directory is refused.
///
/// `place()` unlinks the destination before linking, so a checkpoint whose
/// target overlaps `segments/` would destroy the first file it "copied".
#[test]
fn a_backup_refuses_to_overwrite_its_own_source() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let db = open(&data);
    seed(&db, 100);

    assert!(db.backup(&data).is_err(), "into the data directory itself");
    assert!(
        db.backup(&data.join("segments")).is_err(),
        "into the segment directory"
    );
    assert!(
        db.backup(&data.join("segments").join("shard_0")).is_err(),
        "into a shard directory"
    );

    // Everything still readable.
    assert_eq!(count(&db), 100);

    // And the default admin root — inside the data directory, outside
    // `segments/` — is allowed, because it cannot collide.
    db.backup(&data.join("backups").join("nightly")).unwrap();
    db.close().unwrap();
}

/// A checkpoint that fails over an earlier one leaves nothing restorable.
///
/// The manifest is the only thing that makes a directory a backup, and it is
/// removed before anything it describes is touched — so a half-finished
/// re-checkpoint cannot be restored, whether or not verification would have
/// caught it. The failure here is arranged by making the target's manifest
/// path a directory, which no write can replace.
#[test]
fn a_failed_recheckpoint_leaves_nothing_restorable() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backup = dir.path().join("backup");

    let db = open(&data);
    seed(&db, 100);
    db.backup(&backup).unwrap();
    assert!(Chronix::verify_backup(&backup).is_ok());

    // Make the manifest unwritable by putting a directory in its place, so
    // the next checkpoint gets as far as placing files and then fails.
    let manifest = backup.join("backup_manifest.json");
    std::fs::remove_file(&manifest).unwrap();
    std::fs::create_dir(&manifest).unwrap();

    let more: Vec<Point> = (100..300).map(point).collect();
    db.insert_batch(&more).unwrap().into_complete().unwrap();
    db.flush().unwrap();
    assert!(
        db.backup(&backup).is_err(),
        "the checkpoint cannot write its manifest"
    );
    db.close().unwrap();

    std::fs::remove_dir(&manifest).unwrap();
    assert!(
        Chronix::verify_backup(&backup).is_err(),
        "a directory with no manifest is not a backup"
    );
    assert!(
        Chronix::restore(&backup, &dir.path().join("restored")).is_err(),
        "and it cannot be restored"
    );
}

/// The database takes its own checkpoints.
///
/// Everything else the database needs doing for itself it does for itself —
/// flush, compaction, rollups, retention — because the product is
/// embedded-first and there is often nothing else running. A backup was the
/// one maintenance task that needed a cron the gateway does not have.
#[test]
fn the_maintenance_thread_takes_and_prunes_checkpoints() {
    use chronix_core::Checkpoints;

    let dir = TempDir::new().unwrap();
    let data = dir.path().join("db");
    let backups = dir.path().join("backups");

    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(&data)
            .checkpoints(Checkpoints {
                interval_secs: Some(1),
                directory: Some(backups.clone()),
                keep: 2,
            })
            .maintenance_interval(std::time::Duration::from_millis(50))
            .build()
            .unwrap(),
    )
    .unwrap();
    seed(&db, 100);

    // The first one is due immediately: the commonest reason a gateway has
    // no recent backup is that it restarts more often than the interval.
    assert!(
        wait_for(std::time::Duration::from_secs(15), || {
            !complete_checkpoints(&backups).is_empty()
        }),
        "the thread must take a checkpoint without being asked"
    );
    let first = complete_checkpoints(&backups)
        .first()
        .expect("just asserted")
        .clone();

    // It keeps taking them — observed by the *oldest* one being rotated out
    // rather than by counting, because `keep` caps the count by design.
    assert!(
        wait_for(std::time::Duration::from_secs(30), || {
            let now = complete_checkpoints(&backups);
            now.len() >= 2 && !now.contains(&first)
        }),
        "the oldest checkpoint must eventually be pruned, which is what proves \
         both that it keeps running and that `keep` is enforced"
    );

    let all = complete_checkpoints(&backups);
    assert!(
        all.len() <= 2,
        "at most `keep` complete checkpoints, found {}: {all:?}",
        all.len()
    );

    // And what it wrote is restorable.
    let newest = all.last().unwrap().clone();
    let manifest = Chronix::verify_backup(&newest).expect("a scheduled checkpoint verifies");
    assert!(manifest.segments > 0);
    db.close().unwrap();
}

/// Directories under `root` that hold a finished checkpoint, oldest first.
fn complete_checkpoints(root: &Path) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut out: Vec<std::path::PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir() && p.join("backup_manifest.json").exists())
        .collect();
    out.sort();
    out
}

/// Spin until `cond` or the deadline.
fn wait_for(deadline: std::time::Duration, mut cond: impl FnMut() -> bool) -> bool {
    let end = std::time::Instant::now() + deadline;
    while std::time::Instant::now() < end {
        if cond() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    false
}
