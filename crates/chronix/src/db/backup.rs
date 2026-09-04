//! Backup, restore, and point-in-time recovery (PITR) operations.

use std::path::Path;

use tracing::info;

use super::{chrono_timestamp_ms, BackupManifest, Chronix};
use crate::error::{DbError, Result};

impl Chronix {
    /// Create a point-in-time backup of the database.
    ///
    /// Flushes the WAL, then copies all on-disk state (`wal/`, `segments/`,
    /// `catalog/`) to `target_dir`. Rollup definitions and watermarks are
    /// part of the catalog, so they travel with it. A manifest
    /// file (`backup_manifest.json`) records backup metadata.
    ///
    /// The backup is *crash-consistent*: the WAL is synced before
    /// copying begins, and the manifest is written last.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed, flush fails, or
    /// file I/O fails.
    #[must_use = "backup errors indicate incomplete backup"]
    pub fn backup(&self, target_dir: &Path) -> Result<BackupManifest> {
        self.check_open()?;

        // 1. Flush all memtables so segment files are up to date.
        self.flush()?;
        // 2. Sync WAL to ensure all buffered records are on disk.
        self.wal
            .sync()
            .map_err(|e| DbError::Internal(format!("WAL sync failed: {e}")))?;

        info!(target = %target_dir.display(), "Starting database backup");

        std::fs::create_dir_all(target_dir)?;

        // 3. Copy sub-directories.
        let dirs = ["wal", "segments", "catalog"];
        let mut file_count: usize = 0;
        let mut total_bytes: u64 = 0;
        for dir_name in &dirs {
            let src = self.config.data_dir.join(dir_name);
            let dst = target_dir.join(dir_name);
            if src.exists() {
                let (files, bytes) = Self::copy_dir_recursive(&src, &dst)?;
                file_count += files;
                total_bytes += bytes;
            }
        }

        // 5. Write manifest last (atomic marker that backup completed).
        let manifest = BackupManifest {
            version: 1,
            created_at: chrono_timestamp_ms(),
            wal_sequence: self.wal.current_sequence(),
            file_count,
            total_bytes,
        };
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| DbError::Internal(format!("manifest serialization: {e}")))?;
        std::fs::write(target_dir.join("backup_manifest.json"), manifest_json)?;

        info!(
            target = %target_dir.display(),
            files = file_count,
            bytes = total_bytes,
            wal_seq = manifest.wal_sequence,
            "Backup complete"
        );
        Ok(manifest)
    }

    /// Restore a database from a backup directory.
    ///
    /// `backup_dir` must contain a `backup_manifest.json` created by
    /// [`backup()`](Self::backup). The contents are copied into `target_dir`,
    /// which can then be opened with `Chronix::open()`.
    ///
    /// `target_dir` must **not** already exist (to prevent accidental
    /// overwrites).
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is missing/corrupt, target
    /// already exists, or file I/O fails.
    #[must_use = "restore errors indicate incomplete restore"]
    pub fn restore(backup_dir: &Path, target_dir: &Path) -> Result<BackupManifest> {
        // Validate manifest exists.
        let manifest_path = backup_dir.join("backup_manifest.json");
        if !manifest_path.exists() {
            return Err(DbError::Internal(
                "backup_manifest.json not found — not a valid backup directory".into(),
            ));
        }
        let manifest_data = std::fs::read_to_string(&manifest_path)?;
        let manifest: BackupManifest = serde_json::from_str(&manifest_data)
            .map_err(|e| DbError::Internal(format!("corrupt backup manifest: {e}")))?;

        if target_dir.exists() {
            return Err(DbError::Internal(format!(
                "target directory {} already exists — aborting restore to prevent data loss",
                target_dir.display()
            )));
        }

        info!(
            source = %backup_dir.display(),
            target = %target_dir.display(),
            wal_seq = manifest.wal_sequence,
            "Restoring database from backup"
        );

        std::fs::create_dir_all(target_dir)?;

        // Copy sub-directories.
        let dirs = ["wal", "segments", "catalog"];
        for dir_name in &dirs {
            let src = backup_dir.join(dir_name);
            let dst = target_dir.join(dir_name);
            if src.exists() {
                Self::copy_dir_recursive(&src, &dst)?;
            }
        }

        info!(target = %target_dir.display(), "Restore complete");
        Ok(manifest)
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

    /// **Point-in-Time Recovery (PITR):** restore a database from a backup
    /// and replay WAL records up to a specific sequence number.
    ///
    /// This performs a standard restore from `backup_dir` into `target_dir`,
    /// then replays WAL records in the range
    /// `(backup_manifest.wal_sequence, target_sequence]` from the WAL
    /// directory inside the restored backup.
    ///
    /// If `wal_archive_dir` is provided, WAL records are sourced from the
    /// archive instead of the backup's WAL directory (for cases where WAL
    /// files have been archived separately).
    ///
    /// # Errors
    ///
    /// Returns an error if the backup is invalid, target exists, or
    /// the requested sequence is before the backup's WAL sequence.
    pub fn restore_pitr(
        backup_dir: &Path,
        target_dir: &Path,
        target_sequence: u64,
        wal_archive_dir: Option<&Path>,
    ) -> Result<(BackupManifest, usize)> {
        // 1. Perform standard restore
        let manifest = Self::restore(backup_dir, target_dir)?;

        if target_sequence < manifest.wal_sequence {
            return Err(DbError::Internal(format!(
                "PITR target sequence {target_sequence} is before backup's WAL sequence {}",
                manifest.wal_sequence
            )));
        }

        if target_sequence == manifest.wal_sequence {
            info!(
                target_seq = target_sequence,
                "PITR target equals backup sequence — no WAL replay needed"
            );
            return Ok((manifest, 0));
        }

        // 2. Determine WAL source directory
        let wal_dir = wal_archive_dir
            .map(Path::to_path_buf)
            .unwrap_or_else(|| target_dir.join("wal"));

        // 3. Replay WAL records in the PITR window
        info!(
            start_seq = manifest.wal_sequence,
            target_seq = target_sequence,
            wal_source = %wal_dir.display(),
            "Starting PITR WAL replay"
        );

        let records =
            chronix_engine::wal::replay_range(&wal_dir, manifest.wal_sequence, target_sequence)
                .map_err(|e| DbError::Internal(format!("PITR WAL replay failed: {e}")))?;

        let replayed = records.len();

        // 4. Write the replayed records into the target's WAL directory
        //    so that the db will pick them up on open().
        //    The records are already in the target WAL (from the backup copy),
        //    so this is only needed when sourcing from an external archive.
        if wal_archive_dir.is_some() {
            let target_wal_dir = target_dir.join("wal");
            std::fs::create_dir_all(&target_wal_dir)?;

            let wal_config = chronix_core::WalConfig::default();
            let writer = chronix_engine::wal::WalWriter::open(&target_wal_dir, wal_config)
                .map_err(|e| DbError::Internal(format!("PITR WAL writer open: {e}")))?;

            for record in &records {
                writer
                    .append(&record.payload)
                    .map_err(|e| DbError::Internal(format!("PITR WAL write: {e}")))?;
            }
            writer
                .sync()
                .map_err(|e| DbError::Internal(format!("PITR WAL sync: {e}")))?;
        }

        info!(
            replayed = replayed,
            target_seq = target_sequence,
            "PITR restore complete"
        );

        Ok((manifest, replayed))
    }
}
