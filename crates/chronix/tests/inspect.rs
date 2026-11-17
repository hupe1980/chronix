//! `chronix::inspect` reads a data directory without opening it.
//!
//! Every test here checks a promise the support thread relies on: the report
//! is right, inspecting changes nothing on disk, a live process is not
//! disturbed, and a directory from another generation or an unclean stop is
//! named for what it is.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chronix::inspect::{DirState, InspectOptions, ProblemKind, inspect};
use chronix::prelude::*;
use chronix::{RollupAggFn, RollupBuilder, fields};

const T0: i64 = 1_700_000_000_000_000_000;
const MIN: i64 = 60_000_000_000;

fn open(dir: &Path) -> Chronix {
    Chronix::open(ChronixConfig::builder().data_dir(dir).build().unwrap()).unwrap()
}

fn write(db: &Chronix, measurement: &str, host: &str, ts: i64) {
    let tags: BTreeMap<String, String> = [("host".to_owned(), host.to_owned())].into();
    let key = SeriesKey::new(measurement, tags).unwrap();
    db.insert(&Point::new(key, fields! { "v" => 1.0 }, ts).unwrap())
        .unwrap();
}

/// Two measurements over two flushes, a rollup, closed cleanly.
fn populated(dir: &Path) {
    let db = open(dir);
    db.create_rollup(
        RollupBuilder::new()
            .name("cpu_1h")
            .source("cpu")
            .target("cpu_1h")
            .every("1h")
            .aggregation(RollupAggFn::Count)
            .group_by("host")
            .build()
            .unwrap(),
    )
    .unwrap();
    for i in 0..10 {
        write(&db, "cpu", "a", T0 + i * MIN);
        write(&db, "cpu", "b", T0 + i * MIN);
        write(&db, "mem", "a", T0 + i * MIN);
    }
    db.flush().unwrap();
    for i in 10..20 {
        write(&db, "cpu", "c", T0 + i * MIN);
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

#[cfg(unix)]
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
fn lock_records_holder_and_clean_close_empties_it() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open(tmp.path());
    let held = std::fs::read_to_string(tmp.path().join("LOCK")).unwrap();
    assert!(
        held.contains(&format!("pid={}", std::process::id())),
        "{held}"
    );
    assert!(held.contains("version="), "{held}");

    // A second open fails to lock — and must not wipe the holder record.
    assert!(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(tmp.path())
                .build()
                .unwrap()
        )
        .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("LOCK")).unwrap(),
        held
    );

    db.close().unwrap();
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("LOCK")).unwrap(),
        ""
    );
}

#[test]
fn inspect_reports_every_field() {
    let tmp = tempfile::tempdir().unwrap();
    populated(tmp.path());
    let before = snapshot(tmp.path());

    #[cfg(unix)]
    set_read_only(tmp.path(), true);
    let report = inspect(
        tmp.path(),
        InspectOptions::default().series_budget(Some(500)),
    );
    #[cfg(unix)]
    set_read_only(tmp.path(), false);
    let report = report.unwrap();

    assert_eq!(report.schema, chronix::inspect::SCHEMA);
    assert_eq!(report.state, DirState::ClosedCleanly);
    assert!(report.problems.is_empty(), "{:?}", report.problems);
    for f in &report.formats {
        assert!(f.readable, "{f:?}");
        assert_eq!(f.found, Some(f.supported), "{f:?}");
    }
    assert_eq!(report.series_total, Some(4)); // cpu a/b/c + mem a
    assert_eq!(report.series_budget, Some(500));

    let names: Vec<&str> = report
        .measurements
        .iter()
        .map(|m| m.name.as_str())
        .collect();
    assert!(
        names.contains(&"cpu") && names.contains(&"mem"),
        "{names:?}"
    );
    let cpu = report
        .measurements
        .iter()
        .find(|m| m.name == "cpu")
        .unwrap();
    assert_eq!(cpu.segments, 2);
    assert_eq!(cpu.series, Some(3));
    assert_eq!(cpu.oldest_ns, T0);
    assert_eq!(cpu.newest_ns, T0 + 19 * MIN);
    assert!(cpu.bytes > 0);
    assert!(cpu.oldest.ends_with('Z'), "{}", cpu.oldest);
    assert!(!cpu.pending_drop);

    assert_eq!(report.rollups.len(), 1);
    assert_eq!(report.rollups[0].name, "cpu_1h");
    assert_eq!(report.wal.records_above_floor, 0);

    // `--fast` skips the sidecars, and says so with `None` rather than 0.
    let fast = inspect(tmp.path(), InspectOptions::default().fast(true)).unwrap();
    assert_eq!(fast.series_total, None);
    assert!(fast.measurements.iter().all(|m| m.series.is_none()));

    assert_eq!(
        snapshot(tmp.path()),
        before,
        "inspect changed the directory"
    );
}

#[test]
fn inspect_does_not_block_a_concurrent_open() {
    let tmp = tempfile::tempdir().unwrap();
    let db = open(tmp.path());
    write(&db, "cpu", "a", T0);
    let held = std::fs::read_to_string(tmp.path().join("LOCK")).unwrap();

    let report = inspect(tmp.path(), InspectOptions::default()).unwrap();
    match &report.state {
        DirState::Live { holder } => assert_eq!(holder.pid, std::process::id()),
        other => panic!("expected Live, got {other:?}"),
    }
    // Unflushed: the WAL has the record a replay would read.
    assert!(report.wal.records_above_floor > 0);

    // Inspecting touched neither the lock nor its record, and the holder
    // keeps working.
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("LOCK")).unwrap(),
        held
    );
    write(&db, "cpu", "a", T0 + MIN);
    db.close().unwrap();
    let again = open(tmp.path());
    again.close().unwrap();
}

#[test]
fn inspect_names_a_foreign_generation() {
    let tmp = tempfile::tempdir().unwrap();
    populated(tmp.path());

    let segment = find(tmp.path(), "csx");
    let mut bytes = std::fs::read(&segment).unwrap();
    let supported = u16::from_le_bytes([bytes[4], bytes[5]]);
    bytes[4..6].copy_from_slice(&(supported + 7).to_le_bytes());
    std::fs::write(&segment, &bytes).unwrap();

    let report = inspect(tmp.path(), InspectOptions::default()).unwrap();
    let problem = report
        .problems
        .iter()
        .find(|p| p.kind == ProblemKind::ForeignGeneration)
        .unwrap_or_else(|| panic!("no foreign-generation problem: {:?}", report.problems));
    assert!(problem.file.ends_with(".csx"), "{problem:?}");
    let (found, wanted) = (supported + 7, supported);
    assert!(
        problem.message.contains(&found.to_string())
            && problem.message.contains(&wanted.to_string()),
        "the message must name both generations: {}",
        problem.message
    );
    let seg = report.formats.iter().find(|f| f.what == "segment").unwrap();
    assert!(!seg.readable);
    // A segment that cannot be read leaves its measurement's series unknown,
    // not undercounted.
    assert_eq!(report.series_total, None);
}

#[test]
fn inspect_reports_unclean_close() {
    if let Ok(dir) = std::env::var("CHRONIX_INSPECT_CHILD_DIR") {
        let db = open(Path::new(&dir));
        for i in 0..5 {
            write(&db, "cpu", "a", T0 + i * MIN);
        }
        std::process::abort();
    }

    let tmp = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["inspect_reports_unclean_close", "--exact", "--nocapture"])
        .env("CHRONIX_INSPECT_CHILD_DIR", tmp.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the child must abort");

    let report = inspect(tmp.path(), InspectOptions::default()).unwrap();
    match &report.state {
        DirState::NotClosedCleanly { holder: Some(h) } => assert_ne!(h.pid, std::process::id()),
        other => panic!("expected NotClosedCleanly with a holder, got {other:?}"),
    }
    assert_eq!(report.wal.records_above_floor, 5);
    assert_eq!(
        (report.wal.seq_first, report.wal.seq_last),
        (Some(1), Some(5))
    );
}

#[test]
fn a_directory_without_a_catalog_is_refused_by_name() {
    let tmp = tempfile::tempdir().unwrap();
    let err = inspect(tmp.path(), InspectOptions::default()).unwrap_err();
    assert!(
        err.to_string().contains("is not a chronix data directory"),
        "{err}"
    );
}

/// Inspection reads two small pieces per segment and nothing else. The
/// `inspect_10k_segments` bench holds the 1 s target at ten thousand; this
/// test keeps a loose bound at two thousand, because building the directory
/// (one fsynced sidecar per segment) is what dominates its run time.
#[test]
fn many_segments_inspect_quickly() {
    let tmp = tempfile::tempdir().unwrap();
    synthetic_dir(tmp.path(), 2_000);
    let start = std::time::Instant::now();
    let report = inspect(tmp.path(), InspectOptions::default()).unwrap();
    let took = start.elapsed();
    assert!(
        report.problems.is_empty(),
        "{:?}",
        &report.problems[..report.problems.len().min(3)]
    );
    assert_eq!(
        report.measurements.iter().map(|m| m.segments).sum::<u64>(),
        2_000
    );
    // `id % 6` and `id % 50` repeat together every lcm(6, 50) = 150 segments.
    assert_eq!(report.series_total, Some(150));
    assert!(
        took < std::time::Duration::from_secs(3),
        "inspect took {took:?}"
    );
}

/// A synthetic data directory of `n` segments: real catalog entries, segment
/// files that are a valid header and footer around an empty body, and real
/// `.series` sidecars — everything `inspect` reads, without `n` flushes.
fn synthetic_dir(root: &std::path::Path, n: u64) {
    use chronix::chronix_core::{SegmentFile, SegmentId, SegmentState, ShardId};
    use chronix::chronix_engine::index::{SegmentCatalog, SegmentCatalogEntry, series_index};
    use chronix::chronix_engine::segment::header::{SegmentFooter, SegmentHeader, VERSION};

    let seg_dir = root.join("segments");
    let mut cat = SegmentCatalog::new(root.join("catalog")).unwrap();
    cat.in_one_sync(|cat| {
        for id in 1..=n {
            let measurement = format!("m{}", id % 6);
            let file = SegmentFile::new(ShardId(0), &format!("seg_{id}.csx")).unwrap();
            let path = file.resolve(&seg_dir);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let header = SegmentHeader {
                version: VERSION,
                flags: 0,
                created_at: id as i64,
                min_timestamp: id as i64 * 1_000,
                max_timestamp: id as i64 * 1_000 + 999,
                row_count: 100,
                column_count: 3,
                series_count: 1,
                compression: 0,
                sort_order: 1,
            };
            let footer = SegmentFooter {
                metadata_offset: 48,
                row_group_count: 1,
                checksum: 0,
                metadata_checksum: 0,
            };
            let mut bytes = header.to_bytes();
            bytes.extend_from_slice(&footer.to_bytes());
            std::fs::write(&path, &bytes).unwrap();
            let tags: std::collections::BTreeMap<String, String> =
                [("unit".to_owned(), format!("u{}", id % 50))].into();
            let key = chronix::chronix_core::SeriesKey::new(measurement.clone(), tags).unwrap();
            series_index::write(&path, &[key], series_index::SegmentStamp::of(&header)).unwrap();
            cat.add_segment(SegmentCatalogEntry {
                segment_id: SegmentId(id),
                shard_id: ShardId(0),
                measurement,
                file,
                min_timestamp: header.min_timestamp,
                max_timestamp: header.max_timestamp,
                row_count: 100,
                series_count: 1,
                byte_size: bytes.len() as u64,
                row_group_count: 1,
                column_count: 3,
                column_stats: Vec::new(),
                state: SegmentState::Active,
            })?;
        }
        Ok(())
    })
    .unwrap();
    cat.force_snapshot().unwrap();
}

fn find(dir: &Path, ext: &str) -> PathBuf {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == ext) {
                return p;
            }
        }
    }
    panic!("no .{ext} file under {}", dir.display());
}
