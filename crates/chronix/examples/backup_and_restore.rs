#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Backup and Restore
//!
//! A backup is a **checkpoint**: the database as of the flush that starts it.
//! Every write acknowledged before `backup()` is in it; a write accepted
//! while it runs is not.
//!
//! What this shows, in the order an operator does it:
//!
//! 1. take a checkpoint of a live database;
//! 2. **verify** it, without restoring it — a backup that can only be checked
//!    by restoring it is one nobody checks;
//! 3. lose the original outright, and restore onto a fresh directory;
//! 4. confirm the restored database holds the same rows, and reads its *own*
//!    files.
//!
//! Step 3 is the only one that means anything: a restore taken while the
//! source still exists can read the source's files and look like it worked.
//!
//! ```sh
//! cargo run -p chronix --example backup_and_restore
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use chronix::prelude::*;
use chronix::{fields, tags, Chronix};

const HOUR_NS: i64 = 3_600_000_000_000;

fn rows(db: &Chronix, measurement: &str) -> usize {
    let plan = db
        .query()
        .measurement(measurement)
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    db.execute(&plan).unwrap().num_rows()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let data = root.path().join("chronix-data");
    let backup = root.path().join("backups").join("nightly");
    let restored = root.path().join("restored");

    let open = |path: &std::path::Path| -> Result<Chronix, chronix::DbError> {
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(path)
                .shard_duration(Duration::from_secs(3600))
                .build()?,
        )
    };

    // ── 1. A database with data on disk ────────────────────────
    //
    // The flush matters: a checkpoint of a database whose data is still in
    // the WAL exercises nothing, because the replay rebuilds it either way.
    let db = open(&data)?;
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )
    .unwrap_or(i64::MAX);

    let mut points = Vec::new();
    for hour in 0..3i64 {
        for minute in 0..60i64 {
            for meter in 0..4 {
                points.push(Point::new(
                    SeriesKey::new("power", tags! { "meter" => &format!("m{meter}") })?,
                    fields! { "watts" => 230.0 + minute as f64 },
                    now - (2 - hour) * HOUR_NS + minute * 60_000_000_000,
                )?);
            }
        }
    }
    db.insert_batch(&points)?.into_complete()?;
    db.flush()?;
    println!("── the live database ──");
    println!("  rows          {}", rows(&db, "power"));
    println!("  segments      {}", db.statistics().segment_count);

    // ── 2. The checkpoint ──────────────────────────────────────
    //
    // Segments are hard-linked when the target is on the same filesystem, so
    // this costs a directory entry per segment rather than their bytes. That
    // is safe because a segment is written once and then only unlinked.
    let manifest = db.backup(&backup)?;
    println!("\n── the checkpoint ──");
    println!("  segments      {}", manifest.segments);
    println!("  files         {}", manifest.file_count);
    println!("  bytes         {}", manifest.total_bytes);

    // ── 3. Verify it, without restoring it ─────────────────────
    let checked = Chronix::verify_backup(&backup)?;
    println!(
        "\n── verified ── {} segment(s), all present at the size the catalog records",
        checked.segments
    );

    // ── 4. Lose the original ───────────────────────────────────
    db.close()?;
    std::fs::remove_dir_all(&data)?;
    println!("\n── the original data directory is gone ──");

    // ── 5. Restore, and open the result ────────────────────────
    Chronix::restore(&backup, &restored)?;
    let recovered = open(&restored)?;
    println!("\n── the restored database ──");
    println!("  rows          {}", rows(&recovered, "power"));
    println!("  segments      {}", recovered.statistics().segment_count);

    // Every path its catalog names is inside the restored directory: the
    // catalog records each segment *relative* to `segments/`, which is what
    // makes a data directory movable at all.
    {
        let catalog = recovered.catalog().read();
        let outside = catalog
            .all_segments()
            .iter()
            .filter(|e| {
                !e.file
                    .resolve(&restored.join("segments"))
                    .starts_with(&restored)
            })
            .count();
        println!("  paths outside the restored directory: {outside}");
        assert_eq!(outside, 0);
    }

    // And the last value is the one that was written, not a default.
    let last = recovered
        .last_value("power", &BTreeMap::from([("meter".into(), "m0".into())]))?
        .expect("the restored database answers for a series it holds");
    println!("  newest m0     {:?}", last.field("watts"));

    assert_eq!(rows(&recovered, "power"), points.len());
    recovered.close()?;

    // ── 6. What a restore refuses ──────────────────────────────
    //
    // An incomplete backup is refused *before* anything is copied, rather
    // than restored into a database that opens and then fails at its first
    // query — which is what an unverified restore gives you, at the one
    // moment nobody has a second copy.
    let broken = root.path().join("broken");
    copy_tree(&backup, &broken)?;
    let victim = first_segment(&broken.join("segments")).expect("a segment");
    std::fs::remove_file(&victim)?;
    match Chronix::restore(&broken, &root.path().join("never")) {
        Ok(_) => return Err("an incomplete backup must be refused".into()),
        Err(e) => println!("\n── refused ──\n  {e}"),
    }

    Ok(())
}

fn copy_tree(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), &to)?;
        }
    }
    Ok(())
}

fn first_segment(segments: &std::path::Path) -> Option<std::path::PathBuf> {
    for shard in std::fs::read_dir(segments).ok()?.flatten() {
        if !shard.path().is_dir() {
            continue;
        }
        for file in std::fs::read_dir(shard.path()).ok()?.flatten() {
            if file.path().extension().is_some_and(|e| e == "csx") {
                return Some(file.path());
            }
        }
    }
    None
}
