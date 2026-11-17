//! `ChronixConfig::read_only` opens a data directory for queries and writes
//! nothing to it.
//!
//! The case it exists for: a directory copied off a gateway, opened on a
//! laptop to be asked questions. Opening it normally would take the lock,
//! replay and flush the WAL, sweep orphan files and rewrite the catalog —
//! changing the evidence before anyone has read it.

#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chronix::prelude::*;
use chronix::{DbError, fields};

const T0: i64 = 1_700_000_000_000_000_000;
const MIN: i64 = 60_000_000_000;

fn config(dir: &Path) -> ChronixConfig {
    ChronixConfig::builder().data_dir(dir).build().unwrap()
}

fn read_only(dir: &Path) -> ChronixConfig {
    let mut c = config(dir);
    c.read_only = true;
    c
}

fn point(host: &str, ts: i64, v: f64) -> Point {
    let tags: BTreeMap<String, String> = [("host".to_owned(), host.to_owned())].into();
    Point::new(
        SeriesKey::new("cpu", tags).unwrap(),
        fields! { "v" => v },
        ts,
    )
    .unwrap()
}

fn rows(db: &Chronix) -> usize {
    let plan = db.query().measurement("cpu").build().unwrap();
    db.execute(&plan).unwrap().num_rows()
}

/// Ten points flushed into a segment, closed cleanly.
fn populated(dir: &Path) {
    let db = Chronix::open(config(dir)).unwrap();
    for i in 0..10 {
        db.insert(&point("a", T0 + i * MIN, i as f64)).unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();
}

/// Every file under `dir` with its size and modification time.
fn snapshot(dir: &Path) -> BTreeMap<PathBuf, (u64, std::time::SystemTime)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let md = e.metadata().unwrap();
            if md.is_dir() {
                stack.push(e.path());
            }
            out.insert(e.path(), (md.len(), md.modified().unwrap()));
        }
    }
    out
}

fn set_read_only(dir: &Path, read_only: bool) {
    use std::os::unix::fs::PermissionsExt;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let is_dir = e.metadata().unwrap().is_dir();
            if is_dir {
                stack.push(e.path());
            }
            let mode = match (is_dir, read_only) {
                (true, true) => 0o555,
                (true, false) => 0o755,
                (false, true) => 0o444,
                (false, false) => 0o644,
            };
            std::fs::set_permissions(e.path(), std::fs::Permissions::from_mode(mode)).unwrap();
        }
    }
    let top = if read_only { 0o555 } else { 0o755 };
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(top)).unwrap();
}

#[test]
fn read_only_open_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    populated(tmp.path());
    let before = snapshot(tmp.path());

    // A read-only filesystem: any write the open attempts is an error, so a
    // pass here is not luck.
    set_read_only(tmp.path(), true);
    let result = std::panic::catch_unwind(|| {
        let db = Chronix::open(read_only(tmp.path())).unwrap();
        assert_eq!(rows(&db), 10);
        db.close().unwrap();
    });
    set_read_only(tmp.path(), false);
    result.unwrap();

    assert_eq!(snapshot(tmp.path()), before, "nothing on disk changed");
}

#[test]
fn read_only_refuses_writes() {
    let tmp = tempfile::tempdir().unwrap();
    populated(tmp.path());
    let db = Chronix::open(read_only(tmp.path())).unwrap();

    let refused = |r: Result<(), DbError>, what: &str| match r {
        Err(DbError::ReadOnly { .. }) => {}
        other => panic!("{what}: expected ReadOnly, got {other:?}"),
    };
    refused(db.insert(&point("a", T0 + 99 * MIN, 1.0)), "insert");
    refused(db.flush().map(|_| ()), "flush");
    refused(db.compact().map(|_| ()), "compact");
    refused(db.drop_measurement("cpu").map(|_| ()), "drop_measurement");
    refused(db.check_writable(), "check_writable");
    assert_eq!(rows(&db), 10, "the refused insert left nothing behind");
}

#[test]
fn read_only_sees_unflushed_wal() {
    if let Ok(dir) = std::env::var("CHRONIX_READ_ONLY_CHILD_DIR") {
        let db = Chronix::open(config(Path::new(&dir))).unwrap();
        for i in 0..5 {
            db.insert(&point("b", T0 + i * MIN, 1.0)).unwrap();
        }
        // No flush, no close: the five points exist only in the WAL.
        std::process::abort();
    }

    let tmp = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["read_only_sees_unflushed_wal", "--exact", "--nocapture"])
        .env("CHRONIX_READ_ONLY_CHILD_DIR", tmp.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort");
    let before = snapshot(tmp.path());

    let db = Chronix::open(read_only(tmp.path())).unwrap();
    assert_eq!(rows(&db), 5, "the WAL is replayed into memory");
    db.close().unwrap();

    // Replayed, not consumed: a normal open after this still replays them.
    assert_eq!(snapshot(tmp.path()), before, "the WAL was not rewritten");
    let db = Chronix::open(config(tmp.path())).unwrap();
    assert_eq!(rows(&db), 5);
}

#[test]
fn read_only_refuses_a_live_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let live = Chronix::open(config(tmp.path())).unwrap();
    live.insert(&point("a", T0, 1.0)).unwrap();

    match Chronix::open(read_only(tmp.path())) {
        Err(DbError::DirectoryLive { pid, .. }) => assert_eq!(pid, std::process::id()),
        Err(other) => panic!("expected DirectoryLive, got {other}"),
        Ok(_) => panic!("a live directory must be refused"),
    }
    // The refusal did not disturb the holder.
    live.insert(&point("a", T0 + MIN, 2.0)).unwrap();
    assert_eq!(rows(&live), 2);
}

#[test]
fn read_only_refuses_a_directory_that_is_not_a_database() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("nope");
    assert!(Chronix::open(read_only(&missing)).is_err());
    assert!(!missing.exists(), "a read-only open creates nothing");
}

/// SQL spills and plans too: a query on a read-only open writes nothing to
/// the directory (its spill area is the handle's scratch directory).
#[cfg(feature = "sql")]
#[test]
fn read_only_sql_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    populated(tmp.path());
    let before = snapshot(tmp.path());

    let db = Chronix::open(read_only(tmp.path())).unwrap();
    let batches = db
        .sql("SELECT host, count(*) FROM cpu GROUP BY host ORDER BY host")
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    db.close().unwrap();

    assert_eq!(
        snapshot(tmp.path()),
        before,
        "a read-only SQL query wrote to the directory"
    );
}
