//! Local filesystem storage backend.
//!
//! Implements [`StorageBackend`] for the local filesystem with:
//! - Atomic writes via temporary file + rename
//! - Positioned reads via `pread` (no seek/read race)
//! - File permissions `0o600` (owner read/write only)
//! - Namespace-scoped directory layout: `{data_dir}/ns_{namespace}/shard_{id}/`

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::fs;
use tracing::debug;

use chronix_core::{NamespaceId, ShardId};

use crate::storage::backend::{SegmentPath, StorageBackend};
use crate::storage::error::{Result, StorageError};

/// Local filesystem storage backend.
///
/// Stores segment files in a namespace-scoped, shard-based directory hierarchy:
/// ```text
/// {data_dir}/ns_{namespace}/shard_{id}/segment_name.csx
/// ```
///
/// All write operations are atomic (write to `.tmp` then rename) to prevent
/// partial segment files from being visible to readers.
#[derive(Debug, Clone)]
pub struct LocalFsBackend {
    data_dir: PathBuf,
}

impl LocalFsBackend {
    /// Create a new local filesystem backend rooted at `data_dir`.
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    /// Returns the data directory path.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Resolve a [`SegmentPath`] to an absolute filesystem path.
    #[must_use]
    pub fn resolve(&self, path: &SegmentPath) -> PathBuf {
        path.to_fs_path(&self.data_dir)
    }

    /// Ensure the namespace/shard directory exists.
    async fn ensure_shard_dir(&self, path: &SegmentPath) -> Result<()> {
        let dir = self
            .data_dir
            .join(format!("ns_{}", path.namespace().as_str()))
            .join(format!("shard_{}", path.shard_id().0));
        fs::create_dir_all(&dir).await?;
        Ok(())
    }

    /// Inner helper for [`put_segment`] — performs write-fsync-rename.
    /// Separated so the caller can clean up the temp file on error.
    async fn put_segment_inner(
        &self,
        temp_path: &Path,
        target: &Path,
        data: &[u8],
        path: &SegmentPath,
    ) -> Result<()> {
        fs::write(temp_path, data).await?;

        // Set file permissions (owner read/write only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            fs::set_permissions(temp_path, perms).await?;
        }

        // Fsync the temp file to ensure data is durable before rename.
        // Open with write access so fsync is POSIX-compliant even on
        // kernels that ignore sync on read-only fds.
        {
            let file = fs::OpenOptions::new().write(true).open(temp_path).await?;
            file.sync_all().await?;
        }

        fs::rename(temp_path, target).await?;

        // Fsync the parent directory to ensure the rename is durable.
        // Use spawn_blocking to avoid stalling the tokio worker on sync I/O.
        if let Some(parent) = target.parent() {
            let parent = parent.to_path_buf();
            tokio::task::spawn_blocking(move || -> std::result::Result<(), std::io::Error> {
                let dir = std::fs::File::open(&parent)?;
                dir.sync_all()?;
                Ok(())
            })
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
            .map_err(StorageError::Io)?;
        }

        debug!(path = %path, bytes = data.len(), "segment written");
        Ok(())
    }
}

impl StorageBackend for LocalFsBackend {
    async fn put_segment(&self, path: &SegmentPath, data: &[u8]) -> Result<()> {
        self.ensure_shard_dir(path).await?;
        let target = self.resolve(path);

        // Atomic write: write to temp file, fsync, then rename.
        // Use a process-wide atomic counter to guarantee uniqueness even
        // under NTP clock adjustments or same-nanosecond concurrent calls.
        //
        // Create temp file in the same directory as the target
        // so that `rename()` is always an atomic same-filesystem operation
        // (avoids EXDEV errors across mount points). The temp filename uses
        // only the file stem, not the full path, to stay within PATH_MAX.
        static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
        let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let file_name = target.file_name().unwrap_or_default().to_string_lossy();
        let temp_name = format!(".{file_name}.{seq}.{pid}.tmp");
        let temp_path = target
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(temp_name);

        match self
            .put_segment_inner(&temp_path, &target, data, path)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                // Clean up the temp file on any error to avoid leaking it.
                let _ = fs::remove_file(&temp_path).await;
                Err(e)
            }
        }
    }

    async fn get_segment(&self, path: &SegmentPath) -> Result<Vec<u8>> {
        let fs_path = self.resolve(path);
        match fs::read(&fs_path).await {
            Ok(data) => Ok(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::NotFound { path: fs_path })
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn get_range(&self, path: &SegmentPath, offset: u64, length: usize) -> Result<Vec<u8>> {
        // Cap read length to prevent OOM from corrupted headers.
        const MAX_RANGE_LEN: usize = 256 * 1024 * 1024; // 256 MiB
        if length > MAX_RANGE_LEN {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("get_range length {length} exceeds maximum {MAX_RANGE_LEN}"),
            )));
        }

        let fs_path = self.resolve(path);

        // Use positioned read (pread) to avoid seek/read race conditions.
        // tokio::task::spawn_blocking is used because pread is a sync syscall.
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let file = std::fs::File::open(&fs_path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::NotFound {
                        path: fs_path.clone(),
                    }
                } else {
                    StorageError::Io(e)
                }
            })?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; length];
                file.read_exact_at(&mut buf, offset)?;
                Ok(buf)
            }

            #[cfg(not(unix))]
            {
                use std::io::{Read, Seek, SeekFrom};
                let mut file = file;
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; length];
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;

        result
    }

    async fn delete_segment(&self, path: &SegmentPath) -> Result<()> {
        let fs_path = self.resolve(path);
        match fs::remove_file(&fs_path).await {
            Ok(()) => {
                debug!(path = %path, "segment deleted");
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!(path = %path, "segment already absent, treating as success");
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn list_segments(
        &self,
        namespace: &NamespaceId,
        shard_id: &ShardId,
    ) -> Result<Vec<SegmentPath>> {
        let shard_dir = self
            .data_dir
            .join(format!("ns_{}", namespace.as_str()))
            .join(format!("shard_{}", shard_id.0));

        match fs::read_dir(&shard_dir).await {
            Ok(mut entries) => {
                let mut segments = Vec::new();
                while let Some(entry) = entries.next_entry().await? {
                    let file_name = entry.file_name();
                    let name = file_name.to_string_lossy();
                    if name.ends_with(".csx") {
                        segments.push(SegmentPath::new(
                            namespace.clone(),
                            *shard_id,
                            name.into_owned(),
                        )?);
                    }
                }
                segments.sort_by(|a, b| a.segment_name().cmp(b.segment_name()));
                Ok(segments)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    async fn exists(&self, path: &SegmentPath) -> Result<bool> {
        let fs_path = self.resolve(path);
        match fs::metadata(&fs_path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_ns() -> NamespaceId {
        NamespaceId::default_namespace()
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "test_segment.csx").unwrap();

        let data = b"hello segment data";
        backend.put_segment(&path, data).await.unwrap();

        let read_back = backend.get_segment(&path).await.unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn put_get_range_partial_read() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(2), "range_test.csx").unwrap();

        let data = b"0123456789abcdef";
        backend.put_segment(&path, data).await.unwrap();

        // Read bytes 4..10
        let partial = backend.get_range(&path, 4, 6).await.unwrap();
        assert_eq!(partial, b"456789");

        // Read from start
        let start = backend.get_range(&path, 0, 4).await.unwrap();
        assert_eq!(start, b"0123");
    }

    #[tokio::test]
    async fn delete_then_exists_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "delete_me.csx").unwrap();

        backend.put_segment(&path, b"data").await.unwrap();
        assert!(backend.exists(&path).await.unwrap());

        backend.delete_segment(&path).await.unwrap();
        assert!(!backend.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn list_segments_scans_directory() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let ns = default_ns();
        let shard = ShardId(5);

        // Write 3 segments
        for i in 0..3 {
            let path = SegmentPath::new(ns.clone(), shard, format!("seg_{i:03}.csx")).unwrap();
            backend.put_segment(&path, b"data").await.unwrap();
        }

        // Also write a non-.csx file (should be ignored)
        let shard_dir = dir.path().join("ns_default").join("shard_5");
        tokio::fs::write(shard_dir.join("not_a_segment.txt"), b"ignore")
            .await
            .unwrap();

        let segments = backend.list_segments(&ns, &shard).await.unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].segment_name(), "seg_000.csx");
        assert_eq!(segments[1].segment_name(), "seg_001.csx");
        assert_eq!(segments[2].segment_name(), "seg_002.csx");
    }

    #[tokio::test]
    async fn list_segments_empty_shard() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let segments = backend
            .list_segments(&default_ns(), &ShardId(999))
            .await
            .unwrap();
        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn get_nonexistent_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "missing.csx").unwrap();

        let err = backend.get_segment(&path).await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound { .. }));
    }

    #[tokio::test]
    async fn delete_nonexistent_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "missing.csx").unwrap();

        // Deleting a non-existent segment should succeed (idempotent).
        backend.delete_segment(&path).await.unwrap();
    }

    #[tokio::test]
    async fn exists_nonexistent_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "missing.csx").unwrap();
        assert!(!backend.exists(&path).await.unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn put_segment_sets_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "perms.csx").unwrap();

        backend.put_segment(&path, b"data").await.unwrap();

        let fs_path = backend.resolve(&path);
        let metadata = std::fs::metadata(&fs_path).unwrap();
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[tokio::test]
    async fn put_segment_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "atomic.csx").unwrap();

        // Write segment
        backend.put_segment(&path, b"data").await.unwrap();

        // Verify no .tmp file remains
        let shard_dir = dir.path().join("ns_default").join("shard_1");
        let mut entries = tokio::fs::read_dir(&shard_dir).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let name = entry.file_name().to_string_lossy().to_string();
            assert!(
                !std::path::Path::new(&name)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("tmp")),
                "temp file should not remain: {name}"
            );
        }
    }

    // ── Additional edge-case tests ────────────────────────────────

    #[tokio::test]
    async fn put_segment_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "ow.csx").unwrap();

        backend.put_segment(&path, b"v1").await.unwrap();
        backend.put_segment(&path, b"v2").await.unwrap();
        let data = backend.get_segment(&path).await.unwrap();
        assert_eq!(data, b"v2");
    }

    #[tokio::test]
    async fn list_segments_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let ns = default_ns();

        for name in &["c.csx", "a.csx", "b.csx"] {
            let p = SegmentPath::new(ns.clone(), ShardId(1), *name).unwrap();
            backend.put_segment(&p, b"x").await.unwrap();
        }

        let segs = backend.list_segments(&ns, &ShardId(1)).await.unwrap();
        let names: Vec<_> = segs.iter().map(|s| s.segment_name().to_string()).collect();
        assert_eq!(names, vec!["a.csx", "b.csx", "c.csx"]);
    }

    #[tokio::test]
    async fn data_dir_accessor() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        assert_eq!(backend.data_dir(), dir.path());
    }

    #[tokio::test]
    async fn get_range_beyond_eof() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(dir.path());
        let path = SegmentPath::new(default_ns(), ShardId(1), "short.csx").unwrap();
        backend.put_segment(&path, b"tiny").await.unwrap();

        // Reading beyond EOF should return what's available or error
        let result = backend.get_range(&path, 0, 1024).await;
        // Either succeeds with partial data or errors — implementation defined
        if let Ok(data) = result {
            assert!(!data.is_empty());
        }
    }
}
