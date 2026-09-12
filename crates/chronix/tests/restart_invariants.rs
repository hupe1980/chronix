//! Properties that must hold across `close()` → `open()`.
//!
//! Every one of these was violated by a tree in which the durability suite
//! was green, because the suite's "crash" tests dropped the handle — which
//! runs `close()` — and never rotated a WAL file. Each test here drives the
//! public API only.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap

use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix, DbError};
use tempfile::TempDir;

const HOUR: i64 = 3_600_000_000_000;

fn point(measurement: &str, host: &str, ts: i64, v: f64) -> Point {
    Point::new(
        SeriesKey::new(measurement, tags! { "host" => host }).unwrap(),
        fields! { "v" => v },
        ts,
    )
    .unwrap()
}

fn count_rows(db: &Chronix, measurement: &str) -> usize {
    let plan = db
        .query()
        .measurement(measurement)
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    db.execute(&plan).unwrap().num_rows()
}

fn rows_on_disk(db: &Chronix, measurement: &str) -> (usize, u64) {
    let cat = db.catalog().read();
    let segs = cat.active_segments_for_measurement(measurement);
    (segs.len(), segs.iter().map(|e| e.row_count).sum())
}

/// A graceful close leaves nothing to replay, so a reopen must not
/// re-insert — and then re-flush — data that is already in a segment.
///
/// Before the fix `truncate_before` never removed the *active* WAL file, so
/// every restart replayed up to 32 MB of already-flushed points into the
/// memtable and wrote them to disk a second time on the next flush. Dedup
/// hid the duplicate rows from queries; the flash wear and the doubled
/// segment count were real.
#[test]
fn a_clean_close_is_a_replay_free_restart() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap()
    };

    {
        let db = Chronix::open(cfg()).unwrap();
        for i in 0..100 {
            db.insert(&point("m", "a", i * 1_000_000_000, i as f64))
                .unwrap();
        }
        db.close().unwrap();
    }
    for _ in 0..2 {
        let db = Chronix::open(cfg()).unwrap();
        assert_eq!(db.wal_replayed_records(), 0, "nothing should be replayed");
        db.close().unwrap();
    }
    let db = Chronix::open(cfg()).unwrap();
    let (segments, rows) = rows_on_disk(&db, "m");
    assert_eq!(rows, 100, "every reopen re-flushed the replayed points");
    assert_eq!(segments, 1);
    assert_eq!(count_rows(&db, "m"), 100);
    db.close().unwrap();
}

/// The cardinality budget is a property of the database, not of the WAL
/// file that happens to be active.
#[test]
fn the_cardinality_limit_survives_a_restart() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .max_series_cardinality(3)
            // Tiny WAL files so the series leave the active file.
            .wal_max_file_size(256)
            .build()
            .unwrap()
    };

    {
        let db = Chronix::open(cfg()).unwrap();
        for (i, host) in ["a", "b", "c"].iter().enumerate() {
            for j in 0..6 {
                db.insert(&point("m", host, (i as i64 * 100 + j) * 1_000_000_000, 1.0))
                    .unwrap();
                // Flush often: each write rotates a WAL file, and a flush is
                // what lets the files holding this series be truncated.
                db.flush().unwrap();
            }
        }
        assert_eq!(db.statistics().series_count, 3);
        db.close().unwrap();
    }

    let db = Chronix::open(cfg()).unwrap();
    assert_eq!(
        db.statistics().series_count,
        3,
        "the series count must be rebuilt from the segments, not from the WAL"
    );
    let err = db.insert(&point("m", "d", 1, 1.0)).unwrap_err();
    assert!(
        matches!(
            err,
            DbError::CardinalityExceeded {
                current: 3,
                limit: 3
            }
        ),
        "a fourth series must be rejected after a restart, got {err:?}"
    );
    db.close().unwrap();
}

/// A write the caller was told was rejected must stay rejected.
///
/// The shard-tolerance check used to run *after* the WAL append, so a point
/// outside the window was durable before it was refused — and WAL replay
/// bypasses the tolerance check, so the next open inserted it.
#[test]
fn a_rejected_write_does_not_reappear_after_a_restart() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .shard_duration(Duration::from_secs(3600))
            .ooo_shard_tolerance(1)
            .build()
            .unwrap()
    };

    {
        let db = Chronix::open(cfg()).unwrap();
        db.insert(&point("m", "a", 10 * HOUR, 10.0)).unwrap();
        let err = db.insert(&point("m", "a", HOUR, 1.0)).unwrap_err();
        assert!(
            matches!(err, DbError::Memtable(_)),
            "a point nine shards behind must be rejected, got {err:?}"
        );
        let res = db
            .insert_batch(&[
                point("m", "a", 11 * HOUR, 11.0),
                point("m", "a", 2 * HOUR, 2.0),
            ])
            .unwrap();
        assert_eq!(res.accepted, 1);
        assert_eq!(res.rejected.len(), 1);
        assert_eq!(res.rejected[0].0, 1, "the second point is the rejected one");
        assert_eq!(count_rows(&db, "m"), 2);
        db.close().unwrap();
    }

    let db = Chronix::open(cfg()).unwrap();
    assert_eq!(
        count_rows(&db, "m"),
        2,
        "the rejected points came back through WAL replay"
    );
    db.close().unwrap();
}

/// A column that reached the in-memory schema must reach the catalog too,
/// or a restart forgets it while the segments still carry it.
///
/// `register_point` mutated the registry point by point, so a batch rejected
/// on its *last* point had already added the columns of its first points —
/// and a later, valid write of those columns produced no schema action, so
/// nothing was ever persisted.
#[test]
fn a_column_added_by_a_rejected_batch_is_still_persisted() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap()
    };

    {
        let db = Chronix::open(cfg()).unwrap();
        db.insert(&point("m", "a", 1_000_000_000, 1.0)).unwrap();

        let key = SeriesKey::new("m", tags! { "host" => "a" }).unwrap();
        let with_new_col = Point::new(
            key.clone(),
            fields! { "v" => 2.0, "extra" => 5.0 },
            2_000_000_000,
        )
        .unwrap();
        let conflicting =
            Point::new(key.clone(), fields! { "v" => "oops" }, 3_000_000_000).unwrap();
        let err = db.insert_batch(&[with_new_col, conflicting]).unwrap_err();
        assert!(matches!(err, DbError::Schema(_)), "got {err:?}");

        // A rejected batch must not leave anything behind.
        assert_eq!(count_rows(&db, "m"), 1);

        // Now the column arrives for real.
        db.insert(&Point::new(key, fields! { "v" => 3.0, "extra" => 6.0 }, 4_000_000_000).unwrap())
            .unwrap();
        assert!(db.schema("m").unwrap().column("extra").is_some());
        db.close().unwrap();
    }

    let db = Chronix::open(cfg()).unwrap();
    let schema = db.schema("m").expect("measurement survives a restart");
    assert!(
        schema.column("extra").is_some(),
        "the column was in the segment but not in the persisted schema: {:?}",
        schema.columns()
    );
    let plan = db
        .query()
        .measurement("m")
        .field("extra")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert!(batch.column_by_name("extra").is_some());
    db.close().unwrap();
}

/// A whole-series delete releases the series from the cardinality budget,
/// and the release must survive a restart: the budget is rebuilt from the
/// segment sidecars, which used to still list the deleted series.
#[test]
fn a_whole_series_delete_stays_released_after_a_restart() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .max_series_cardinality(3)
            .build()
            .unwrap()
    };
    {
        let db = Chronix::open(cfg()).unwrap();
        for host in ["a", "b", "c"] {
            db.insert(&point("m", host, 1_000_000_000, 1.0)).unwrap();
        }
        db.flush().unwrap();
        db.delete_series("m", &tags! { "host" => "b" }).unwrap();
        assert_eq!(db.statistics().series_count, 2);
        db.close().unwrap();
    }
    let db = Chronix::open(cfg()).unwrap();
    assert_eq!(
        db.statistics().series_count,
        2,
        "the deleted series came back"
    );
    db.insert(&point("m", "d", 2_000_000_000, 1.0))
        .expect("the released slot must be usable after a restart");
    assert_eq!(count_rows(&db, "m"), 3);
    db.close().unwrap();
}

/// A flush that fails — here, the shard directory replaced by a file —
/// keeps its memtable, so the data reaches a segment once the disk is
/// back. It used to pop the memtable before writing, and the next
/// successful flush of any other shard truncated the WAL under it.
#[test]
fn a_failed_flush_loses_nothing() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .maintenance_interval(Duration::ZERO)
            .build()
            .unwrap()
    };
    {
        let db = Chronix::open(cfg()).unwrap();
        // Shard 0 and shard 1, so a second shard flushes while the first
        // cannot.
        for i in 0..20 {
            db.insert(&point("m", "a", i * 1_000_000_000, i as f64))
                .unwrap();
            db.insert(&point("m", "a", HOUR + i * 1_000_000_000, i as f64))
                .unwrap();
        }
        let shard0 = dir.path().join("segments").join("shard_0");
        std::fs::create_dir_all(&shard0).unwrap();
        std::fs::remove_dir_all(&shard0).unwrap();
        std::fs::write(&shard0, b"not a directory").unwrap();

        assert!(db.flush().is_err(), "shard 0 cannot be written");
        assert_eq!(
            count_rows(&db, "m"),
            40,
            "nothing is lost by a failed flush"
        );
        // A second flush attempt on the healthy shard must not truncate the
        // WAL under shard 0's records either.
        assert!(db.flush().is_err());

        std::fs::remove_file(&shard0).unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    let db = Chronix::open(cfg()).unwrap();
    assert_eq!(count_rows(&db, "m"), 40);
    assert_eq!(db.wal_replayed_records(), 0);
    db.close().unwrap();
}

/// A point far in the future is refused before it can anchor the
/// out-of-order window there — one device with a broken clock used to
/// lock every real write out until the next restart.
#[test]
fn a_future_timestamp_cannot_poison_the_window() {
    let dir = TempDir::new().unwrap();
    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(dir.path())
            .future_write_tolerance(Duration::from_secs(600))
            .build()
            .unwrap(),
    )
    .unwrap();
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap();
    db.insert(&point("m", "a", now, 1.0)).unwrap();
    let err = db
        .insert(&point("m", "clock", now + 365 * 24 * HOUR, 1.0))
        .unwrap_err();
    assert!(matches!(err, DbError::FutureTimestamp { .. }), "{err:?}");
    let res = db
        .backfill(&[point("m", "clock", now + 365 * 24 * HOUR, 1.0)])
        .unwrap();
    assert!(res.is_partial(), "backfill is for history, not the future");
    db.insert(&point("m", "a", now + 1_000_000_000, 2.0))
        .expect("a real write after the bad one must still be accepted");
    assert_eq!(count_rows(&db, "m"), 2);
    db.close().unwrap();
}

/// A segment file nothing registered — a crash between the write and the
/// catalog append — is removed at open. Segments live one directory down
/// (`segments/shard_<id>/`), where the sweep used to never look.
#[test]
fn orphaned_segment_files_are_removed_at_open() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap()
    };
    let orphan;
    {
        let db = Chronix::open(cfg()).unwrap();
        db.insert(&point("m", "a", 1_000_000_000, 1.0)).unwrap();
        db.flush().unwrap();
        let shard_dir = dir.path().join("segments").join("shard_0");
        let real = std::fs::read_dir(&shard_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|e| e == "csx"))
            .unwrap();
        orphan = shard_dir.join("seg_orphan.csx");
        std::fs::copy(&real, &orphan).unwrap();
        std::fs::copy(
            real.with_extension("series"),
            orphan.with_extension("series"),
        )
        .unwrap();
        db.close().unwrap();
    }
    let db = Chronix::open(cfg()).unwrap();
    assert!(!orphan.exists(), "the orphaned segment was not removed");
    assert!(!orphan.with_extension("series").exists());
    assert_eq!(count_rows(&db, "m"), 1);
    db.close().unwrap();
}

/// A delete's WAL record needs no replay — its tombstones are in the
/// catalog — so a clean close after a delete is still replay-free.
#[test]
fn a_clean_close_after_a_delete_is_replay_free() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap()
    };
    {
        let db = Chronix::open(cfg()).unwrap();
        db.insert(&point("m", "a", 1_000_000_000, 1.0)).unwrap();
        db.insert(&point("m", "b", 1_000_000_000, 1.0)).unwrap();
        db.delete_series("m", &tags! { "host" => "b" }).unwrap();
        db.close().unwrap();
    }
    let db = Chronix::open(cfg()).unwrap();
    assert_eq!(db.wal_replayed_records(), 0);
    assert_eq!(count_rows(&db, "m"), 1);
    db.close().unwrap();
}

/// The memtable cap is admission control for live writes, not for replay:
/// a record the caller was told was accepted is inserted at open whatever
/// the cap says. Here the cap is lowered between runs, so the WAL holds
/// more than one memtable's worth.
#[test]
fn replay_does_not_drop_records_over_the_memtable_cap() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    if let Ok(child_dir) = std::env::var("CHRONIX_REPLAY_CAP_DIR") {
        let db = Chronix::open(
            ChronixConfig::builder()
                .data_dir(&child_dir)
                .memtable_flush_threshold(64 * 1024 * 1024)
                .maintenance_interval(Duration::ZERO)
                .build()
                .unwrap(),
        )
        .unwrap();
        for i in 0..4000 {
            db.insert(&point("m", "a", i * 1_000_000_000, i as f64))
                .unwrap();
        }
        std::process::abort();
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "replay_does_not_drop_records_over_the_memtable_cap",
            "--exact",
            "--nocapture",
        ])
        .env("CHRONIX_REPLAY_CAP_DIR", &path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort");

    let db = Chronix::open(
        ChronixConfig::builder()
            .data_dir(&path)
            // Far below the ~180 bytes a point costs: the WAL cannot fit.
            .memtable_flush_threshold(64 * 1024)
            .max_memtable_memory(128 * 1024)
            .maintenance_interval(Duration::ZERO)
            .build()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(db.wal_replayed_records(), 4000);
    assert_eq!(count_rows(&db, "m"), 4000, "replayed records were dropped");
    let (segments, rows) = rows_on_disk(&db, "m");
    assert!(
        segments >= 1 && rows == 4000,
        "replay over the cap is flushed down at open"
    );
    db.close().unwrap();
}

/// **A damaged catalog refuses to open; it does not open smaller.**
///
/// The catalog is the durable record of *what may be deleted*, and `open()`
/// deletes every `.csx` the catalog does not name. So a replay that stops
/// early and returns `Ok` does not merely forget segments — it **deletes
/// them**, on the one pass that is supposed to be recovering the database.
///
/// Replay used to decide "this is the tail, stop here" from a byte count:
/// more than eight bytes left meant corruption, fewer meant a crash artefact.
/// A fragment followed by a real record could land on either side of that.
/// Now the question is asked directly — *is there a whole, CRC-valid record
/// after this?* — and a `yes` is a refusal.
///
/// What this pins is the consequence, in the order it matters: the open
/// fails, **and the segment files are still on disk** when it does.
#[test]
fn a_corrupt_catalog_refuses_to_open_and_deletes_nothing() {
    let dir = TempDir::new().unwrap();
    let cfg = || {
        ChronixConfig::builder()
            .data_dir(dir.path())
            .maintenance_interval(Duration::ZERO)
            .build()
            .unwrap()
    };

    {
        let db = Chronix::open(cfg()).unwrap();
        for i in 0..20 {
            db.insert(&point("m", "a", i * 1_000_000_000, i as f64))
                .unwrap();
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    let segments_before = segment_files(dir.path());
    assert!(
        !segments_before.is_empty(),
        "the fixture must have produced a segment, or this proves nothing"
    );

    // A fragment, then a whole record. `close()` snapshots and empties the
    // log, so this is exactly the shape a failed append used to leave behind:
    // bytes that are not a record, with a record after them.
    {
        use std::io::Write;
        let wal = dir.path().join("catalog").join("manifest.wal");
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal).unwrap();
        f.write_all(&64u32.to_le_bytes()).unwrap();
        f.write_all(b"fragment").unwrap();
        // A record the scanner will find: length, payload, matching CRC.
        let payload = b"a well-framed record".as_slice();
        f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
        f.write_all(payload).unwrap();
        f.write_all(&crc32c::crc32c(payload).to_le_bytes()).unwrap();
        f.sync_all().unwrap();
    }

    let err = Chronix::open(cfg()).expect_err("a damaged catalog must refuse");
    let msg = err.to_string();
    assert!(
        msg.contains("mid-stream corruption"),
        "the refusal must say what is wrong: {msg}"
    );

    assert_eq!(
        segment_files(dir.path()),
        segments_before,
        "a refused open must not have deleted anything — the orphan sweep runs \
         only on a catalog that loaded"
    );
}

/// Every `.csx` under `segments/`, sorted.
fn segment_files(data_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![data_dir.join("segments")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "csx") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}
