//! Storage backend trait and segment path types.
//!
//! The [`StorageBackend`] trait defines the pluggable interface for segment
//! file storage. [`SegmentPath`] encapsulates shard and segment identifiers
//! for portable path construction.

use std::fmt;
use std::path::{Path, PathBuf};

use chronix_core::{NamespaceId, ShardId};

use crate::storage::error::Result;

/// Identifies a segment within the storage layer.
///
/// Encapsulates namespace, shard ID, and segment file name for portable path
/// construction across different storage backends (local filesystem, S3, etc.).
///
/// # Tenant Isolation
///
/// Every segment path is scoped to a [`NamespaceId`], providing defense-in-depth
/// tenant isolation at the storage layer. Even if Cedar authorization policies
/// are misconfigured, the directory/object-key prefix prevents cross-tenant
/// data access.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentPath {
    /// Namespace (tenant) this segment belongs to.
    namespace: NamespaceId,
    /// The shard this segment belongs to.
    shard_id: ShardId,
    /// Segment filename (e.g., `"segment_001.csx"`).
    segment_name: String,
}

impl SegmentPath {
    /// Create a new segment path scoped to a namespace.
    ///
    /// Returns `InvalidPath` if `segment_name` contains path separators
    /// (`/`, `\`), parent-directory components (`..`), or is empty —
    /// preventing path-traversal attacks.
    pub fn new(
        namespace: NamespaceId,
        shard_id: ShardId,
        segment_name: impl Into<String>,
    ) -> crate::storage::error::Result<Self> {
        let name = segment_name.into();
        if name.is_empty()
            || name.contains('/')
            || name.contains('\\')
            || name.contains("..")
            || name == "."
        {
            return Err(crate::storage::error::StorageError::InvalidPath {
                detail: format!(
                    "invalid segment name '{name}': must not be empty or contain path separators / '..' components"
                ),
            });
        }
        Ok(Self {
            namespace,
            shard_id,
            segment_name: name,
        })
    }

    /// Returns the namespace (tenant) ID.
    #[must_use]
    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    /// Returns the shard ID.
    #[must_use]
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Returns the segment filename.
    #[must_use]
    pub fn segment_name(&self) -> &str {
        &self.segment_name
    }

    /// Resolve to a filesystem path relative to a data directory.
    ///
    /// Layout: `{data_dir}/ns_{namespace}/shard_{shard_id}/{segment_name}`
    #[must_use]
    pub fn to_fs_path(&self, data_dir: &Path) -> PathBuf {
        data_dir
            .join(format!("ns_{}", self.namespace.as_str()))
            .join(format!("shard_{}", self.shard_id.0))
            .join(&self.segment_name)
    }
}

impl fmt::Display for SegmentPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ns_{}/shard_{}/{}",
            self.namespace.as_str(),
            self.shard_id.0,
            self.segment_name
        )
    }
}

/// Pluggable storage backend for segment files.
///
/// Abstracts the physical storage of segment files, enabling local filesystem,
/// S3, and other backends. All operations are async for cloud compatibility.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync + 'static` for use across Tokio tasks.
///
/// # Atomicity
///
/// `put_segment` must write atomically (write-to-temp then rename) to prevent
/// partial/corrupt segment files from being visible.
pub trait StorageBackend: Send + Sync + 'static {
    /// Write a complete segment file to storage.
    ///
    /// Implementations must write atomically (write-to-temp → rename).
    fn put_segment(
        &self,
        path: &SegmentPath,
        data: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Read a complete segment file from storage.
    fn get_segment(
        &self,
        path: &SegmentPath,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send;

    /// Read a byte range from a segment file.
    ///
    /// Critical for column projection — reads only the required columns
    /// without loading the entire segment. Uses positioned reads (`pread`)
    /// to avoid seek/read race conditions.
    ///
    /// # Partial Read Semantics
    ///
    /// - If `offset + length` exceeds the file size, implementations MUST
    ///   return an error (not a short read). Callers rely on exact-length
    ///   results for correct column decoding.
    /// - `offset` is 0-based from the start of the segment file.
    /// - `length == 0` returns an empty `Vec<u8>`.
    /// - Implementations MUST NOT cache partial reads internally — the
    ///   segment cache layer handles caching at a higher level.
    /// - For object-store backends (S3, GCS), this maps to an HTTP Range
    ///   request: `Range: bytes={offset}-{offset+length-1}`.
    /// - Concurrent `get_range` calls on the same segment MUST be safe
    ///   (no file-level locking required — use positioned/pread I/O).
    fn get_range(
        &self,
        path: &SegmentPath,
        offset: u64,
        length: usize,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send;

    /// Delete a segment file from storage.
    fn delete_segment(
        &self,
        path: &SegmentPath,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// List all segment files in a namespace/shard.
    fn list_segments(
        &self,
        namespace: &NamespaceId,
        shard_id: &ShardId,
    ) -> impl std::future::Future<Output = Result<Vec<SegmentPath>>> + Send;

    /// Check whether a segment file exists.
    fn exists(&self, path: &SegmentPath) -> impl std::future::Future<Output = Result<bool>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_ns() -> NamespaceId {
        NamespaceId::default_namespace()
    }

    #[test]
    fn segment_path_display() {
        let path = SegmentPath::new(default_ns(), ShardId(42), "segment_001.csx").unwrap();
        assert_eq!(path.to_string(), "ns_default/shard_42/segment_001.csx");
    }

    #[test]
    fn segment_path_to_fs_path() {
        let path = SegmentPath::new(default_ns(), ShardId(7), "seg.csx").unwrap();
        let fs_path = path.to_fs_path(Path::new("/data"));
        assert_eq!(fs_path, PathBuf::from("/data/ns_default/shard_7/seg.csx"));
    }

    #[test]
    fn segment_path_accessors() {
        let path = SegmentPath::new(default_ns(), ShardId(3), "test.csx").unwrap();
        assert_eq!(path.namespace(), &default_ns());
        assert_eq!(path.shard_id(), ShardId(3));
        assert_eq!(path.segment_name(), "test.csx");
    }

    #[test]
    fn segment_path_equality() {
        let a = SegmentPath::new(default_ns(), ShardId(1), "seg.csx").unwrap();
        let b = SegmentPath::new(default_ns(), ShardId(1), "seg.csx").unwrap();
        let c = SegmentPath::new(default_ns(), ShardId(2), "seg.csx").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn segment_path_different_namespaces_differ() {
        let ns_a = NamespaceId::new("tenant-a").unwrap();
        let ns_b = NamespaceId::new("tenant-b").unwrap();
        let a = SegmentPath::new(ns_a, ShardId(1), "seg.csx").unwrap();
        let b = SegmentPath::new(ns_b, ShardId(1), "seg.csx").unwrap();
        assert_ne!(a, b);
        assert_ne!(
            a.to_fs_path(Path::new("/data")),
            b.to_fs_path(Path::new("/data"))
        );
    }

    #[test]
    fn segment_path_rejects_invalid_names() {
        assert!(SegmentPath::new(default_ns(), ShardId(1), "").is_err());
        assert!(SegmentPath::new(default_ns(), ShardId(1), "a/b").is_err());
        assert!(SegmentPath::new(default_ns(), ShardId(1), "a\\b").is_err());
        assert!(SegmentPath::new(default_ns(), ShardId(1), "a..b").is_err());
        assert!(SegmentPath::new(default_ns(), ShardId(1), ".").is_err());
        // Valid names should succeed
        assert!(SegmentPath::new(default_ns(), ShardId(1), "segment_001.csx").is_ok());
    }
}
