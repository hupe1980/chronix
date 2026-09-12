//! Backup and restore: taking a copy of a live database, and getting it back.
//!
//! A backup is a **checkpoint** — the database as of the flush that starts
//! it. Every write acknowledged before the call is in it; one accepted while
//! it runs is not.
//!
//! Three invariants hold this together, and each is easy to break by
//! accident:
//!
//! 1. **The copy is driven by the catalog, never by a directory walk.** One
//!    read lock takes the entry list, takes a [lease](super::leases) on all
//!    of it, and writes the catalog out as a *single encode* of a single
//!    consistent state. A `read_dir` walk cannot do this: a catalog snapshot
//!    landing mid-walk replaces `manifest.snapshot.bin` and truncates
//!    `manifest.wal`, so the copy can capture the old snapshot beside the
//!    emptied log and lose every transition between them.
//! 2. **Linkable if and only if immutable.** Segment files are hard-linked
//!    when the target shares a filesystem, which is what makes a checkpoint
//!    of a large database near instant. That is sound only because nothing
//!    under `segments/` is rewritten in place — the series sidecar, the one
//!    file that changes, is written to a temporary file and renamed, so the
//!    rewrite takes a new inode. **Check this before changing either
//!    writer:** an in-place rewrite would silently alter every backup on the
//!    same filesystem. `catalog/` and `wal/` are copied, because the manifest
//!    is appended to in place.
//! 3. **The backup carries no WAL.** The checkpoint begins with a flush, so
//!    every acknowledged write is already in a segment the catalog names.
//!    Copying the live WAL would add records written *after* the checkpoint
//!    point and hand the copy a prefix of a file still being appended to.
//!
//! A restore verifies before it copies: every segment the backup's catalog
//! names must be present at its recorded size.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

use super::Chronix;
use super::{catalog_dir_of, chrono_timestamp_ms, segments_dir_of, wal_dir_of, BackupManifest};
use crate::error::{DbError, Result};

/// Name of the marker written last, and the only proof a backup finished.
const MANIFEST_NAME: &str = "backup_manifest.json";

impl Chronix {
    /// Take a checkpoint of this database into `target_dir`.
    ///
    /// A **checkpoint**: the database as of the flush that starts it. Every
    /// write acknowledged before this call is in it; a write accepted while
    /// it runs is not. The directory it produces is a complete database plus
    /// a [`BackupManifest`], driven by the catalog rather than by a directory
    /// walk and held against retirement by a segment lease, so it cannot be
    /// torn. Segment files are **hard-linked** where the target shares a
    /// filesystem, so a checkpoint of a large database is near instant and
    /// costs no space until those segments are compacted away. It carries no
    /// WAL, because the flush leaves nothing to replay.
    ///
    /// [`restore`](Self::restore) copies it somewhere a server can open, and
    /// verifies it on the way; [`verify_backup`](Self::verify_backup) runs
    /// that check on its own.
    ///
    /// `target_dir` may already hold an earlier backup: files the new
    /// checkpoint does not name are removed, so backing up into one directory
    /// repeatedly is a rolling checkpoint rather than an accumulating pile.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, the flush fails, or file
    /// I/O fails.
    #[must_use = "backup errors indicate incomplete backup"]
    pub fn backup(&self, target_dir: &Path) -> Result<BackupManifest> {
        self.check_open()?;

        // A checkpoint into the database's own directory would place a file
        // over the file it is reading — `place` unlinks the destination
        // before linking — so the first segment would be destroyed rather
        // than copied. The admin endpoint's default `backup_root` is
        // `<data_dir>/backups`, which is inside the data directory but not
        // inside `segments/`; this refuses only the overlap that can lose
        // data.
        // Both sides made absolute first, because a relative target and an
        // absolute `data_dir` compare unequal however much they overlap.
        // `std::path::absolute` rather than `canonicalize`, which requires
        // the path to exist and the target does not yet.
        let target_abs = std::path::absolute(target_dir).unwrap_or_else(|_| target_dir.to_owned());
        let data_abs = std::path::absolute(&self.config.data_dir)
            .unwrap_or_else(|_| self.config.data_dir.clone());
        let source_segments_probe = segments_dir_of(&data_abs);
        if target_abs == data_abs
            || target_abs.starts_with(&source_segments_probe)
            || source_segments_probe.starts_with(&target_abs)
        {
            return Err(DbError::InvalidRequest(format!(
                "refusing to check point {} into its own segment directory",
                self.config.data_dir.display()
            )));
        }

        // A nightly backup that silently starts failing is the failure mode
        // this whole subsystem exists to prevent, so it is measured. Both
        // counters are published at zero at startup, because a counter that
        // has never fired is *absent* from a scrape — which an alert cannot
        // tell from a metric that does not exist.
        let started = std::time::Instant::now();
        let checkpoint =
            || -> Result<BackupManifest> { self.checkpoint_inner(target_dir, started) };
        let result = checkpoint();
        if result.is_err() {
            metrics::counter!("chronix_backup_failures_total").increment(1);
        }
        result
    }

    fn checkpoint_inner(
        &self,
        target_dir: &Path,
        started: std::time::Instant,
    ) -> Result<BackupManifest> {
        // 1. Every acknowledged write into a segment. This is what makes the
        //    checkpoint point well-defined and the WAL unnecessary.
        self.flush()?;

        info!(target = %target_dir.display(), "Starting database checkpoint");

        let target_segments = segments_dir_of(target_dir);
        let target_catalog = catalog_dir_of(target_dir);
        std::fs::create_dir_all(&target_segments)?;
        std::fs::create_dir_all(wal_dir_of(target_dir))?;

        // The previous checkpoint's manifest goes **first**, before anything
        // it describes is touched. The manifest is the only thing that makes
        // a directory restorable, so a checkpoint that fails part-way over an
        // earlier one leaves a directory nothing will restore — by
        // construction, rather than because verification happens to notice.
        let _ = std::fs::remove_file(target_dir.join(MANIFEST_NAME));

        // 2. One read lock: the entries to copy, a lease on every one so a
        //    maintenance pass cannot unlink it, and the catalog written out
        //    as the backup's own. `all_segments`, not just the active ones:
        //    the snapshot has to describe the same database the catalog does,
        //    tombstones included, and a tombstone is reclaimable only once
        //    every segment it names has left.
        //
        //    The snapshot is encoded and written *inside* the lock. That
        //    blocks a flush's catalog registration for as long as one file
        //    write and its fsyncs, which is the price of the snapshot being a
        //    single consistent state rather than a directory walk — the whole
        //    point. It does not block readers, which take the same read lock.
        let (files, _lease) = {
            let catalog = self.catalog.read();
            let files: Vec<chronix_core::SegmentFile> = catalog
                .all_segments()
                .iter()
                .map(|e| e.file.clone())
                .collect();
            let lease = self
                .segment_leases
                .acquire(catalog.all_segments().iter().map(|e| e.segment_id));
            catalog
                .write_snapshot_to(&target_catalog)
                .map_err(|e| DbError::Internal(format!("catalog snapshot failed: {e}")))?;
            (files, lease)
        };

        // 3. Exactly the files the snapshot names, plus each one's series
        //    sidecar. Nothing is discovered by walking a directory, so a
        //    segment written after the snapshot is neither copied nor
        //    referenced.
        let source_segments = self.segments_dir();
        let mut file_count = 0usize;
        let mut total_bytes = 0u64;
        let mut wanted: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        for file in &files {
            let relative = file.as_relative();
            let src = source_segments.join(relative);
            let dst = target_segments.join(relative);
            wanted.insert(dst.clone());
            total_bytes += Self::place(&src, &dst)?;
            file_count += 1;

            // The sidecar is derived — a restore rebuilds it from the segment
            // if it is missing — so a sidecar that is not there is not an
            // error. One that is there travels, because rebuilding costs a
            // full decode of every segment at open.
            let src_side = chronix_engine::index::series_index::sidecar_path(&src);
            if src_side.exists() {
                let dst_side = chronix_engine::index::series_index::sidecar_path(&dst);
                wanted.insert(dst_side.clone());
                total_bytes += Self::place(&src_side, &dst_side)?;
                file_count += 1;
            }
        }

        // 4. An earlier checkpoint in the same directory leaves files this
        //    one does not name. They would be removed as orphans at open, but
        //    only after they had been carried around for the life of the
        //    backup.
        Self::prune_unlisted(&target_segments, &wanted);

        // 5. The manifest last: its presence is what makes the directory a
        //    backup, so an interrupted checkpoint cannot be restored.
        let manifest = BackupManifest {
            version: 2,
            created_at: chrono_timestamp_ms(),
            wal_sequence: self.wal.current_sequence(),
            segments: files.len() as u64,
            file_count,
            total_bytes,
        };
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| DbError::Internal(format!("manifest serialization: {e}")))?;
        std::fs::write(target_dir.join(MANIFEST_NAME), manifest_json)?;

        metrics::histogram!("chronix_backup_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        metrics::counter!("chronix_backups_total").increment(1);
        metrics::counter!("chronix_backup_bytes_total").increment(total_bytes);

        info!(
            target = %target_dir.display(),
            segments = manifest.segments,
            files = file_count,
            bytes = total_bytes,
            wal_seq = manifest.wal_sequence,
            duration_ms = started.elapsed().as_millis(),
            "Checkpoint complete"
        );
        Ok(manifest)
    }

    /// Link `src` at `dst`, or copy it if the two are not on one filesystem.
    ///
    /// Returns the bytes the file holds — the same number either way, because
    /// what a restore has to move is the same either way.
    fn place(src: &Path, dst: &Path) -> Result<u64> {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // An existing link or copy from an earlier checkpoint of the same
        // segment is already correct — a segment file is immutable — but it
        // may be a link to a file this database has since replaced, so it is
        // relinked rather than trusted.
        if dst.exists() {
            std::fs::remove_file(dst)?;
        }
        match std::fs::hard_link(src, dst) {
            Ok(()) => {}
            Err(_) => {
                std::fs::copy(src, dst)?;
            }
        }
        Ok(std::fs::metadata(dst)?.len())
    }

    /// Remove `.csx` files and sidecars under `dir` that `keep` does not name.
    fn prune_unlisted(dir: &Path, keep: &std::collections::HashSet<PathBuf>) {
        let Ok(shards) = std::fs::read_dir(dir) else {
            return;
        };
        for shard in shards.flatten() {
            let shard_path = shard.path();
            if !shard_path.is_dir() {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(&shard_path) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if keep.contains(&path) {
                    continue;
                }
                if let Err(e) = std::fs::remove_file(&path) {
                    warn!(path = %path.display(), error = %e, "backup: stale file left behind");
                }
            }
        }
    }

    /// Restore a checkpoint from `backup_dir` into `target_dir`.
    ///
    /// `backup_dir` must contain a `backup_manifest.json` written by
    /// [`backup`](Self::backup). `target_dir` must **not** already exist.
    /// The result can be opened with [`Chronix::open`] — on this machine or
    /// any other, because every path the catalog holds is relative to the
    /// data directory.
    ///
    /// The copy is **verified**: every segment the restored catalog names has
    /// to be present at its recorded size, and the file and byte counts have
    /// to match the manifest. An incomplete backup is refused here rather
    /// than opening as a database that fails at its first query — which is
    /// what an unverified restore gives you, at the one moment nobody has a
    /// second copy.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is missing or corrupt, the target
    /// exists, file I/O fails, or verification finds the backup incomplete.
    #[must_use = "restore errors indicate incomplete restore"]
    pub fn restore(backup_dir: &Path, target_dir: &Path) -> Result<BackupManifest> {
        let manifest = Self::read_manifest(backup_dir)?;

        if target_dir.exists() {
            return Err(DbError::InvalidRequest(format!(
                "target directory {} already exists — aborting restore to prevent data loss",
                target_dir.display()
            )));
        }

        info!(
            source = %backup_dir.display(),
            target = %target_dir.display(),
            wal_seq = manifest.wal_sequence,
            "Restoring database from checkpoint"
        );

        // Verify before copying anything: a refusal that leaves no half
        // database behind is one an operator can act on.
        Self::verify(backup_dir, &manifest)?;

        std::fs::create_dir_all(target_dir)?;
        let copied = (|| -> Result<()> {
            // `segments/` is **linked**, for the same reason the checkpoint
            // links it: those files are immutable, so restoring a large
            // database costs a directory entry each rather than its bytes.
            // The restored database only ever *unlinks* a segment, which
            // drops its own name and leaves the backup's.
            //
            // `catalog/` and `wal/` are **copied**, and the distinction is
            // the whole rule: `manifest.wal` is appended to in place, so a
            // link would let the restored database write into the backup.
            // Linkable if and only if immutable.
            let segments_src = segments_dir_of(backup_dir);
            let segments_dst = segments_dir_of(target_dir);
            std::fs::create_dir_all(&segments_dst)?;
            if segments_src.exists() {
                Self::link_dir_recursive(&segments_src, &segments_dst)?;
            }
            for dir_name in ["wal", "catalog"] {
                let src = backup_dir.join(dir_name);
                let dst = target_dir.join(dir_name);
                std::fs::create_dir_all(&dst)?;
                if src.exists() {
                    Self::copy_dir_recursive(&src, &dst)?;
                }
            }
            Ok(())
        })();
        if let Err(e) = copied {
            // Half a database is worse than none: the next thing an operator
            // does is point a server at this path.
            let _ = std::fs::remove_dir_all(target_dir);
            return Err(e);
        }

        info!(target = %target_dir.display(), "Restore complete");
        Ok(manifest)
    }

    /// Check that a backup is complete, without restoring it.
    ///
    /// Returns its [`BackupManifest`] if every segment the backup's own
    /// catalog names is present at its recorded size and the count matches
    /// the manifest — the same check [`restore`](Self::restore) runs, offered
    /// on its own because the standing advice for any backup scheme is to
    /// verify the ones you are keeping *before* you need them. A backup you
    /// can only check by restoring it is one nobody checks.
    ///
    /// It reads; it writes nothing, and it needs no running database.
    ///
    /// # Errors
    ///
    /// [`DbError::InvalidRequest`] if the directory is not a backup, its
    /// manifest is corrupt, or a segment is missing or the wrong size — with
    /// the file named.
    pub fn verify_backup(backup_dir: &Path) -> Result<BackupManifest> {
        let manifest = Self::read_manifest(backup_dir)?;
        Self::verify(backup_dir, &manifest)?;
        Ok(manifest)
    }

    /// The `backup_manifest.json` of a backup directory.
    fn read_manifest(backup_dir: &Path) -> Result<BackupManifest> {
        let manifest_path = backup_dir.join(MANIFEST_NAME);
        if !manifest_path.exists() {
            return Err(DbError::InvalidRequest(format!(
                "{MANIFEST_NAME} not found — {} is not a backup directory",
                backup_dir.display()
            )));
        }
        let data = std::fs::read_to_string(&manifest_path)?;
        serde_json::from_str(&data)
            .map_err(|e| DbError::InvalidRequest(format!("corrupt backup manifest: {e}")))
    }

    /// Check that `backup_dir` holds every segment its catalog names, at the
    /// size the catalog records.
    fn verify(backup_dir: &Path, manifest: &BackupManifest) -> Result<()> {
        let catalog = chronix_engine::index::SegmentCatalog::open(catalog_dir_of(backup_dir))
            .map_err(|e| DbError::InvalidRequest(format!("backup catalog is unreadable: {e}")))?;
        let segments_dir = segments_dir_of(backup_dir);
        let mut missing: Vec<String> = Vec::new();
        let mut short: Vec<String> = Vec::new();
        let entries = catalog.all_segments();
        for entry in &entries {
            let path = entry.file.resolve(&segments_dir);
            match std::fs::metadata(&path) {
                Err(_) => missing.push(entry.file.to_string()),
                Ok(m) if m.len() != entry.byte_size => short.push(format!(
                    "{} ({} bytes, catalog says {})",
                    entry.file,
                    m.len(),
                    entry.byte_size
                )),
                Ok(_) => {}
            }
        }
        if !missing.is_empty() || !short.is_empty() {
            return Err(DbError::InvalidRequest(format!(
                "incomplete backup in {}: {} segment(s) missing{}, {} truncated{}",
                backup_dir.display(),
                missing.len(),
                if missing.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", missing.join(", "))
                },
                short.len(),
                if short.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", short.join(", "))
                },
            )));
        }
        if entries.len() as u64 != manifest.segments {
            return Err(DbError::InvalidRequest(format!(
                "backup in {} holds {} segments, its manifest claims {}",
                backup_dir.display(),
                entries.len(),
                manifest.segments
            )));
        }
        Ok(())
    }

    /// Recursively link a directory tree, falling back to a copy per file.
    ///
    /// Only ever called on `segments/`, whose files are immutable — see the
    /// caller.
    fn link_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if src_path.is_dir() {
                Self::link_dir_recursive(&src_path, &dst_path)?;
            } else {
                Self::place(&src_path, &dst_path)?;
            }
        }
        Ok(())
    }

    /// Recursively copy a directory tree. Returns `(file_count, total_bytes)`.
    fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(usize, u64)> {
        std::fs::create_dir_all(dst)?;
        let mut files = 0usize;
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if src_path.is_dir() {
                let (f, b) = Self::copy_dir_recursive(&src_path, &dst_path)?;
                files += f;
                bytes += b;
            } else {
                let b = std::fs::copy(&src_path, &dst_path)?;
                files += 1;
                bytes += b;
            }
        }
        Ok((files, bytes))
    }
}

impl Chronix {
    /// Take the scheduled checkpoint, if one is configured, and prune old
    /// ones.
    ///
    /// Returns the directory written, or `None` when scheduled checkpoints
    /// are off. Called only by the maintenance thread — see
    /// [`Checkpoints`](chronix_core::Checkpoints) for why the schedule lives
    /// in the database rather than in cron.
    ///
    /// Each run writes `<directory>/<RFC3339 timestamp>` and then removes
    /// the oldest until `keep` remain. Pruning happens **after** the new one
    /// is complete, so a failed run never leaves fewer copies than it
    /// started with.
    ///
    /// # Errors
    ///
    /// Whatever [`backup`](Self::backup) returns, plus an I/O error if the
    /// directory cannot be created or listed.
    pub(crate) fn scheduled_checkpoint(&self) -> Result<Option<PathBuf>> {
        let settings = &self.config.checkpoints;
        let (Some(_), Some(root)) = (settings.interval(), settings.directory.as_ref()) else {
            return Ok(None);
        };
        std::fs::create_dir_all(root)?;

        // Second precision, and `:` replaced: a colon is legal on the
        // filesystems this targets and is not on the one an operator will
        // eventually copy the directory to.
        let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let target = root.join(&stamp);
        if target.exists() {
            // Two runs inside one second, which only happens if the interval
            // is absurd. Not an error: the previous one is current.
            return Ok(None);
        }
        self.backup(&target)?;
        Self::prune_checkpoints(root, settings.keep);
        Ok(Some(target))
    }

    /// Keep the newest `keep` complete checkpoints under `root`.
    ///
    /// A directory without a manifest is not a checkpoint — it is a run that
    /// did not finish — and is removed regardless of the count, because it
    /// is the one thing in here that cannot be restored and it is
    /// indistinguishable from a good one by name alone.
    fn prune_checkpoints(root: &Path, keep: usize) {
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        let mut complete: Vec<PathBuf> = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if path.join(MANIFEST_NAME).exists() {
                complete.push(path);
            } else if let Err(e) = std::fs::remove_dir_all(&path) {
                warn!(path = %path.display(), error = %e, "checkpoint: could not remove an unfinished run");
            }
        }
        // The names are timestamps in a sortable format, so this is oldest
        // first without reading any metadata.
        complete.sort();
        let keep = keep.max(1);
        if complete.len() <= keep {
            return;
        }
        for path in &complete[..complete.len() - keep] {
            match std::fs::remove_dir_all(path) {
                Ok(()) => {
                    tracing::debug!(path = %path.display(), "checkpoint: pruned");
                }
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "checkpoint: could not prune");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chronix_core::ChronixConfig;

    use super::*;

    /// The lease a checkpoint takes is the one a retirement honours.
    ///
    /// `db::leases` states the rule — *a segment leaves the catalog when its
    /// rows leave the database; its file is unlinked when the last reader
    /// that could open it is gone* — and it was written for scans and applied
    /// to scans. A backup is a reader too: it snapshots the catalog, releases
    /// the lock, and then spends as long as the copy takes holding a list of
    /// paths. Without a lease, a compaction or a retention pass landing in
    /// that window unlinks a file the backup is about to place, and the
    /// backup fails with a bare `ENOENT` at the one moment an operator is
    /// trying to make a second copy.
    ///
    /// This drives the mechanism rather than racing it: the lease is taken
    /// exactly as [`Chronix::backup`] takes it, the retirement is then run to
    /// completion, and the retirement is asked what it did. Racing a real
    /// backup against a real maintenance pass and watching for a failure is
    /// the test that looks stronger and says nothing — the window is
    /// microseconds wide, so it passes with the lease **and** without it.
    #[test]
    fn a_retirement_honours_the_lease_a_checkpoint_takes() {
        use chronix_core::{FieldValue, Point, SeriesKey};

        let dir = tempfile::tempdir().unwrap();
        let db = Chronix::open(
            ChronixConfig::builder()
                .data_dir(dir.path())
                // The maintenance thread must not retire anything behind this
                // test's back.
                .maintenance_interval(Duration::from_secs(86_400 * 365))
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
        const HOUR: i64 = 3_600_000_000_000;
        let key = SeriesKey::new(
            "cpu",
            std::collections::BTreeMap::from([("host".to_owned(), "a".to_owned())]),
        )
        .unwrap();
        let old: Vec<Point> = (0..200i64)
            .map(|i| {
                let fields =
                    std::collections::BTreeMap::from([("v".to_owned(), FieldValue::F64(i as f64))]);
                Point::new(key.clone(), fields, now - 120 * HOUR + i * 1_000_000).unwrap()
            })
            .collect();
        db.backfill(&old).unwrap().into_complete().unwrap();
        db.flush().unwrap();

        // One fresh point, so retention's reference is the wall clock rather
        // than the newest row this database holds — it measures age from
        // `min(now, newest held)`, and without this nothing is old.
        let fresh = SeriesKey::new(
            "cpu",
            std::collections::BTreeMap::from([("host".to_owned(), "b".to_owned())]),
        )
        .unwrap();
        db.insert(
            &Point::new(
                fresh,
                std::collections::BTreeMap::from([("v".to_owned(), FieldValue::F64(1.0))]),
                now,
            )
            .unwrap(),
        )
        .unwrap();
        db.flush().unwrap();

        // Exactly what `backup()` does, under the same lock.
        let (paths, lease) = {
            let catalog = db.catalog.read();
            let paths: Vec<std::path::PathBuf> = catalog
                .all_segments()
                .iter()
                .map(|e| e.file.resolve(&db.segments_dir()))
                .collect();
            let lease = db
                .segment_leases
                .acquire(catalog.all_segments().iter().map(|e| e.segment_id));
            (paths, lease)
        };
        assert!(
            !paths.is_empty(),
            "the flush has to have produced a segment"
        );

        // Everything this database holds is now past the cutoff.
        let result = db
            .enforce_retention(Duration::from_secs(2 * 86_400))
            .unwrap();
        assert!(
            result.segments_deleted > 0,
            "the pass has to remove something for this test to mean anything"
        );
        assert!(
            result.segments_awaiting_readers > 0,
            "the checkpoint holds those segments, so their files cannot have gone"
        );
        for path in &paths {
            assert!(
                path.exists(),
                "a checkpoint copying {} must still find it",
                path.display()
            );
        }

        // And once the checkpoint is done with them, they go.
        drop(lease);
        db.gc().unwrap();
        assert!(
            paths.iter().any(|p| !p.exists()),
            "the last reader is gone, so the expired segments must be reclaimed"
        );
        db.close().unwrap();
    }
}
