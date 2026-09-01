//! Object storage backend wrapping the [`object_store`] crate.
//!
//! Provides a [`ObjectStoreBackend`] that implements [`StorageBackend`]
//! for cloud object storage (S3, GCS, Azure Blob Storage) with an
//! optional local disk cache for frequently accessed segments.
//!
//! ## URL Scheme
//!
//! The backend is configured via URL:
//! - `s3://bucket/prefix` — Amazon S3 (or S3-compatible)
//! - `gs://bucket/prefix` — Google Cloud Storage
//! - `az://container/prefix` — Azure Blob Storage
//! - `file:///tmp/data` or `/tmp/data` — Local filesystem (for testing)

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use tokio::sync::Semaphore;
use tracing::{debug, warn};
use url::Url;

use crate::storage::{SegmentPath, StorageBackend};
use chronix_core::{NamespaceId, ShardId};

use crate::objstore::cache::DiskCache;
use crate::objstore::error::{ObjStoreError, Result};

/// Maximum number of retry attempts for transient object store errors.
const MAX_RETRIES: u32 = 3;

/// Initial backoff duration (doubles each retry, with jitter).
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);

/// Configurable backoff strategy for object store retries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackoffConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Initial backoff duration before the first retry.
    pub initial_backoff: Duration,
    /// Maximum backoff duration (caps exponential growth).
    pub max_backoff: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            max_retries: MAX_RETRIES,
            initial_backoff: INITIAL_BACKOFF,
            max_backoff: Duration::from_secs(30),
        }
    }
}

/// Retry an async operation with exponential backoff + jitter for
/// transient object store errors (429, 503, timeouts, connection resets).
///
/// Uses configurable backoff parameters with a cap to avoid
/// unbounded delays.
async fn retry_with_backoff<F, Fut, T>(
    op_name: &str,
    backoff: &BackoffConfig,
    f: F,
) -> std::result::Result<T, object_store::Error>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, object_store::Error>>,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) if is_retryable(&e) && attempt < backoff.max_retries => {
                attempt += 1;
                let base = backoff.initial_backoff * 2u32.saturating_pow(attempt - 1);
                let capped = base.min(backoff.max_backoff);
                // Add jitter: 50-150% of base
                let jitter_factor = 0.5 + rand_jitter();
                let delay = Duration::from_secs_f64(capped.as_secs_f64() * jitter_factor);
                warn!(
                    op = op_name,
                    attempt,
                    max = backoff.max_retries,
                    delay_ms = delay.as_millis(),
                    error = %e,
                    "retrying transient object store error"
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Determine if an object store error is retryable (transient).
fn is_retryable(err: &object_store::Error) -> bool {
    matches!(err, object_store::Error::Generic { .. })
        || err.to_string().contains("429")
        || err.to_string().contains("503")
        || err.to_string().contains("timeout")
        || err.to_string().contains("connection")
        || err.to_string().contains("reset")
}

/// Simple pseudo-random jitter in [0.0, 1.0] using thread-local state.
/// Avoids pulling in a full RNG crate for this one use case.
fn rand_jitter() -> f64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    Instant::now().hash(&mut hasher);
    (hasher.finish() % 1000) as f64 / 1000.0
}

/// Default multipart upload threshold: 8 MiB.
const DEFAULT_MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024;

/// Default maximum number of concurrent segment downloads.
const DEFAULT_MAX_CONCURRENT_DOWNLOADS: usize = 8;

/// Configuration for the object store backend.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ObjectStoreConfig {
    /// Object store URL (e.g., `s3://bucket/prefix`, `gs://bucket/prefix`).
    pub url: String,
    /// Optional local disk cache configuration.
    pub cache: Option<crate::objstore::cache::CacheConfig>,
    /// Segments larger than this threshold (in bytes) are uploaded via
    /// multipart upload instead of a single PUT. Defaults to 8 MiB.
    #[serde(default = "default_multipart_threshold")]
    pub multipart_threshold_bytes: usize,
    /// Maximum number of concurrent segment downloads. Prevents bandwidth
    /// saturation during recovery or replication bursts. Defaults to 8.
    #[serde(default = "default_max_concurrent_downloads")]
    pub max_concurrent_downloads: usize,
    /// Configurable retry backoff strategy.
    #[serde(default)]
    pub backoff: BackoffConfig,
}

fn default_max_concurrent_downloads() -> usize {
    DEFAULT_MAX_CONCURRENT_DOWNLOADS
}

fn default_multipart_threshold() -> usize {
    DEFAULT_MULTIPART_THRESHOLD
}

/// Object storage backend with optional local caching.
///
/// Delegates storage operations to a cloud object store (S3, GCS, Azure)
/// via the [`object_store`] crate. When a [`DiskCache`] is configured,
/// reads are served from the local cache when available.
///
/// ## Segment Path Mapping
///
/// `SegmentPath { namespace: "default", shard_id: 3, name: "seg_001.csx" }` maps to
/// the object key `ns_default/shard_3/seg_001.csx` relative to the configured prefix.
pub struct ObjectStoreBackend {
    /// The underlying object store implementation.
    store: Arc<dyn ObjectStore>,
    /// Optional local disk cache.
    cache: Option<DiskCache>,
    /// URL prefix for display/debug.
    url: String,
    /// Segments larger than this are uploaded via multipart put.
    multipart_threshold_bytes: usize,
    /// Semaphore limiting concurrent segment downloads to prevent
    /// bandwidth saturation during recovery or replication.
    download_semaphore: Arc<Semaphore>,
    /// Retry backoff configuration.
    backoff: BackoffConfig,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("url", &self.url)
            .field("cached", &self.cache.is_some())
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    /// Create a new object store backend from configuration.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` if the URL scheme is not supported.
    pub async fn new(config: ObjectStoreConfig) -> Result<Self> {
        let store = Self::build_store(&config.url)?;

        let cache = if let Some(cache_config) = config.cache {
            Some(DiskCache::new(cache_config).await?)
        } else {
            None
        };

        Ok(Self {
            store,
            cache,
            url: config.url,
            multipart_threshold_bytes: config.multipart_threshold_bytes,
            download_semaphore: Arc::new(Semaphore::new(config.max_concurrent_downloads)),
            backoff: config.backoff,
        })
    }

    /// Create a backend from an existing [`ObjectStore`] instance.
    ///
    /// Useful for testing with in-memory stores.
    /// Create a backend from an existing [`ObjectStore`] instance.
    ///
    /// Uses the default multipart threshold (8 MiB).
    pub fn from_store(store: Arc<dyn ObjectStore>, cache: Option<DiskCache>) -> Self {
        Self {
            url: "custom://".to_string(),
            store,
            cache,
            multipart_threshold_bytes: DEFAULT_MULTIPART_THRESHOLD,
            download_semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_DOWNLOADS)),
            backoff: BackoffConfig::default(),
        }
    }

    /// Create a backend from an existing [`ObjectStore`] with a custom
    /// multipart threshold.
    pub fn from_store_with_threshold(
        store: Arc<dyn ObjectStore>,
        cache: Option<DiskCache>,
        multipart_threshold_bytes: usize,
    ) -> Self {
        Self {
            url: "custom://".to_string(),
            store,
            cache,
            multipart_threshold_bytes,
            download_semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_DOWNLOADS)),
            backoff: BackoffConfig::default(),
        }
    }

    /// Build the underlying [`ObjectStore`] from a URL string.
    fn build_store(url_str: &str) -> Result<Arc<dyn ObjectStore>> {
        // Handle plain filesystem paths (no scheme)
        if url_str.starts_with('/') {
            return Ok(Arc::new(
                LocalFileSystem::new_with_prefix(url_str).map_err(|e| {
                    ObjStoreError::InvalidConfig {
                        detail: format!("invalid local path: {e}"),
                    }
                })?,
            ));
        }

        let url = Url::parse(url_str)?;
        match url.scheme() {
            "file" => {
                let path = url.path();
                Ok(Arc::new(
                    LocalFileSystem::new_with_prefix(path)
                        .map_err(|e| ObjStoreError::InvalidConfig {
                            detail: format!("invalid local path: {e}"),
                        })?,
                ))
            }
            "s3" | "gs" | "az" => {
                // For real cloud stores, we build from the URL.
                // The `object_store` crate reads credentials from
                // environment variables (AWS_ACCESS_KEY_ID, etc.)
                // or instance metadata automatically.
                let (store, _) = object_store::parse_url(&url)
                    .map_err(|e| ObjStoreError::InvalidConfig {
                        detail: format!("failed to parse object store URL '{url_str}': {e}"),
                    })?;
                Ok(Arc::new(store))
            }
            "memory" => {
                // In-memory store for testing
                Ok(Arc::new(object_store::memory::InMemory::new()))
            }
            scheme => Err(ObjStoreError::InvalidConfig {
                detail: format!(
                    "unsupported URL scheme '{scheme}' — use s3://, gs://, az://, file://, or memory://"
                ),
            }),
        }
    }

    /// Convert a `SegmentPath` to an object store path.
    /// Map a segment to its object key, using **Hive-style partition
    /// directories**.
    ///
    /// `namespace=<ns>/shard=<n>/<file>` rather than `ns_<ns>/shard_<n>/<file>`,
    /// because the `key=value` form is what the rest of the ecosystem reads as
    /// a partitioned dataset (D6, "standard edges"). Three things follow from
    /// it, and none from the underscore form:
    ///
    /// DuckDB, Spark and Polars expose `namespace` and `shard` as **columns**
    ///   and prune whole directories on a predicate over them.
    /// DataFusion's listing tables treat a `key=value` segment as a partition
    ///   rather than as a subdirectory, so the cold tier registers without
    ///   having to relax `listing_table_ignore_subdirectory` on the shared
    ///   session — which is the mutation D25 was written about.
    /// The archive layout is self-describing to somebody who has only the
    ///   bucket and no chronix.
    fn to_obj_path(path: &SegmentPath) -> ObjPath {
        ObjPath::from(format!(
            "namespace={}/shard={}/{}",
            path.namespace().as_str(),
            path.shard_id().0,
            path.segment_name()
        ))
    }

    /// Convert a namespace + shard to a prefix path for listing.
    fn shard_prefix(namespace: &NamespaceId, shard_id: ShardId) -> ObjPath {
        ObjPath::from(format!(
            "namespace={}/shard={}/",
            namespace.as_str(),
            shard_id.0
        ))
    }

    /// Cache key for a segment.
    fn cache_key(path: &SegmentPath) -> String {
        format!(
            "ns_{}/shard_{}/{}",
            path.namespace().as_str(),
            path.shard_id().0,
            path.segment_name()
        )
    }

    /// Return the size of a remote segment via a HEAD request.
    ///
    /// This avoids re-downloading the entire segment just to verify its
    /// size after upload. Returns `None` if the object does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the HEAD request fails (other than `NotFound`).
    pub async fn segment_size(&self, path: &SegmentPath) -> Result<Option<usize>> {
        let obj_path = Self::to_obj_path(path);
        let store = &self.store;
        match retry_with_backoff("segment_size", &self.backoff, || {
            let op = obj_path.clone();
            async move { store.head(&op).await }
        })
        .await
        {
            Ok(meta) => Ok(Some(usize::try_from(meta.size).unwrap_or(usize::MAX))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(ObjStoreError::Store(e)),
        }
    }

    /// Put an object into the store.
    ///
    /// # Errors
    ///
    /// Returns `ObjStoreError::Store` on object store failures.
    pub async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = ObjPath::from(key);
        let start = Instant::now();
        self.store
            .put(&path, PutPayload::from(data))
            .await
            .map_err(ObjStoreError::Store)?;

        let elapsed = start.elapsed();
        metrics::histogram!("chronix_objstore_put_duration_seconds").record(elapsed.as_secs_f64());
        debug!(key, elapsed_ms = elapsed.as_millis(), "object put");
        Ok(())
    }

    /// Get an object from the store.
    ///
    /// # Errors
    ///
    /// Returns `ObjStoreError::NotFound` if the object does not exist,
    /// or `ObjStoreError::Store` on other failures.
    pub async fn get(&self, key: &str) -> Result<Bytes> {
        let path = ObjPath::from(key);
        let start = Instant::now();

        let result = self.store.get(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => ObjStoreError::NotFound {
                path: key.to_string(),
            },
            other => ObjStoreError::Store(other),
        })?;

        let data = result.bytes().await.map_err(ObjStoreError::Store)?;

        let elapsed = start.elapsed();
        metrics::histogram!("chronix_objstore_get_duration_seconds").record(elapsed.as_secs_f64());
        debug!(
            key,
            size = data.len(),
            elapsed_ms = elapsed.as_millis(),
            "object get"
        );
        Ok(data)
    }

    /// Delete an object from the store.
    ///
    /// # Errors
    ///
    /// Returns `ObjStoreError::Store` on object store failures.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let path = ObjPath::from(key);
        self.store
            .delete(&path)
            .await
            .map_err(ObjStoreError::Store)?;
        debug!(key, "object deleted");
        Ok(())
    }

    /// List objects under a prefix.
    ///
    /// # Errors
    ///
    /// Returns `ObjStoreError::Store` on object store failures.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        use futures::TryStreamExt;
        let path = ObjPath::from(prefix);
        let objects: Vec<_> = self
            .store
            .list(Some(&path))
            .try_collect()
            .await
            .map_err(ObjStoreError::Store)?;

        Ok(objects
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect())
    }
}

impl StorageBackend for ObjectStoreBackend {
    async fn put_segment(
        &self,
        path: &SegmentPath,
        data: &[u8],
    ) -> crate::storage::error::Result<()> {
        let obj_path = Self::to_obj_path(path);
        let start = Instant::now();
        let store = &self.store;

        if data.len() >= self.multipart_threshold_bytes {
            // Use multipart upload for large segments so that a
            // network interruption at 99 % doesn't force starting over.
            //
            // On failure, orphaned parts may remain in cloud storage
            // (e.g., S3 charges for incomplete multipart uploads).
            // Operators should configure lifecycle rules to auto-expire
            // incomplete multipart uploads after 24–48h.
            let multipart_result =
                retry_with_backoff("put_segment_multipart", &self.backoff, || {
                    let op = obj_path.clone();
                    let data_ref = data;
                    async move {
                        let upload = store.put_multipart(&op).await?;
                        let mut writer = WriteMultipart::new(upload);
                        writer.write(data_ref);
                        writer.finish().await?;
                        Ok(())
                    }
                })
                .await;

            if let Err(e) = multipart_result {
                // Track orphaned multipart uploads
                metrics::counter!("chronix_objstore_multipart_orphaned_total").increment(1);
                warn!(
                    path = %path,
                    error = %e,
                    "multipart upload failed — orphaned parts may exist; \
                     configure S3 lifecycle rules to auto-expire incomplete uploads"
                );
                return Err(crate::storage::StorageError::InvalidPath {
                    detail: format!("object store multipart put failed: {e}"),
                });
            }

            debug!(
                path = %path,
                size = data.len(),
                "segment put via multipart upload"
            );
        } else {
            // Small segment — single PUT with retry.
            let payload_data = Bytes::copy_from_slice(data);
            retry_with_backoff("put_segment", &self.backoff, || {
                let p = payload_data.clone();
                let op = obj_path.clone();
                async move { store.put(&op, PutPayload::from(p)).await }
            })
            .await
            .map_err(|e| crate::storage::StorageError::InvalidPath {
                detail: format!("object store put failed: {e}"),
            })?;
        }

        // Also cache the data locally
        if let Some(ref cache) = self.cache {
            let key = Self::cache_key(path);
            if let Err(e) = cache.put(&key, data).await {
                warn!(key, error = %e, "failed to cache segment after put");
            }
        }

        let elapsed = start.elapsed();
        metrics::histogram!("chronix_objstore_put_duration_seconds").record(elapsed.as_secs_f64());
        // Track bytes transferred for throughput/cost accounting.
        metrics::counter!("chronix_objstore_put_bytes_total").increment(data.len() as u64);
        debug!(
            path = %path,
            size = data.len(),
            elapsed_ms = elapsed.as_millis(),
            "segment put to object store"
        );
        Ok(())
    }

    async fn get_segment(&self, path: &SegmentPath) -> crate::storage::error::Result<Vec<u8>> {
        let key = Self::cache_key(path);

        // Try cache first
        if let Some(ref cache) = self.cache {
            if let Some(data) = cache.get(&key).await {
                metrics::counter!("chronix_objstore_cache_hits_total").increment(1);
                return Ok(data);
            }
            metrics::counter!("chronix_objstore_cache_misses_total").increment(1);
        }

        // Acquire download semaphore permit to throttle concurrent
        // downloads during recovery/replication bursts.
        let _permit = self.download_semaphore.acquire().await.map_err(|_| {
            crate::storage::StorageError::InvalidPath {
                detail: "download semaphore closed".to_string(),
            }
        })?;

        // Cache miss — fetch from remote with retry
        let obj_path = Self::to_obj_path(path);
        let start = Instant::now();
        let store = &self.store;
        let path_str = path.to_string();

        // Retry with exponential backoff for transient errors
        let result = retry_with_backoff("get_segment", &self.backoff, || {
            let op = obj_path.clone();
            async move { store.get(&op).await }
        })
        .await
        .map_err(|e| match e {
            object_store::Error::NotFound { .. } => crate::storage::StorageError::NotFound {
                path: std::path::PathBuf::from(&path_str),
            },
            other => crate::storage::StorageError::InvalidPath {
                detail: format!("object store get failed: {other}"),
            },
        })?;

        let data = result
            .bytes()
            .await
            .map_err(|e| crate::storage::StorageError::InvalidPath {
                detail: format!("object store read failed: {e}"),
            })?;

        let elapsed = start.elapsed();
        metrics::histogram!("chronix_objstore_get_duration_seconds").record(elapsed.as_secs_f64());
        // Track bytes transferred for throughput/cost accounting.
        metrics::counter!("chronix_objstore_get_bytes_total").increment(data.len() as u64);

        // Populate cache
        if let Some(ref cache) = self.cache {
            if let Err(e) = cache.put(&key, &data).await {
                warn!(%key, error = %e, "failed to cache segment after fetch");
            }
        }

        Ok(data.to_vec())
    }

    async fn get_range(
        &self,
        path: &SegmentPath,
        offset: u64,
        length: usize,
    ) -> crate::storage::error::Result<Vec<u8>> {
        // For range reads, try to serve from cache if the full object is cached
        let key = Self::cache_key(path);

        if let Some(ref cache) = self.cache {
            if let Some(data) = cache.get(&key).await {
                #[allow(clippy::cast_possible_truncation)]
                let start = offset as usize;
                let end = start.checked_add(length).ok_or_else(|| {
                    crate::storage::StorageError::InvalidPath {
                        detail: "offset + length overflow".to_string(),
                    }
                })?;
                if end <= data.len() {
                    return Ok(data[start..end].to_vec());
                }
                // Cache entry is too small (corrupt?) — fall through to remote
            }
        }

        // Fetch the range directly from remote with retry for transient errors.
        // object_store 0.13 ranges are `u64`, so this no longer narrows the
        // offset through `usize` on a 32-bit host.
        let obj_path = Self::to_obj_path(path);
        let end = offset.checked_add(length as u64).ok_or_else(|| {
            crate::storage::StorageError::InvalidPath {
                detail: "offset + length overflow".to_string(),
            }
        })?;
        let range = offset..end;

        // Retry with exponential backoff for transient errors
        let store = &self.store;
        let data = retry_with_backoff("get_range", &self.backoff, || {
            let op = obj_path.clone();
            let r = range.clone();
            async move { store.get_range(&op, r).await }
        })
        .await
        .map_err(|e| match e {
            object_store::Error::NotFound { .. } => crate::storage::StorageError::NotFound {
                path: std::path::PathBuf::from(path.to_string()),
            },
            other => crate::storage::StorageError::InvalidPath {
                detail: format!("object store get_range failed: {other}"),
            },
        })?;

        Ok(data.to_vec())
    }

    async fn delete_segment(&self, path: &SegmentPath) -> crate::storage::error::Result<()> {
        let obj_path = Self::to_obj_path(path);
        // Retry with exponential backoff for transient errors
        let store = &self.store;
        retry_with_backoff("delete_segment", &self.backoff, || {
            let op = obj_path.clone();
            async move { store.delete(&op).await }
        })
        .await
        .map_err(|e| crate::storage::StorageError::InvalidPath {
            detail: format!("object store delete failed: {e}"),
        })?;

        // Remove from cache too
        if let Some(ref cache) = self.cache {
            let key = Self::cache_key(path);
            if let Err(e) = cache.remove(&key).await {
                warn!(key, error = %e, "failed to remove cached segment");
            }
        }

        Ok(())
    }

    async fn list_segments(
        &self,
        namespace: &NamespaceId,
        shard_id: &ShardId,
    ) -> crate::storage::error::Result<Vec<SegmentPath>> {
        use futures::TryStreamExt;
        let prefix = Self::shard_prefix(namespace, *shard_id);
        let ns = namespace.clone();
        let shard = *shard_id;

        // Retry with exponential backoff for transient errors
        let store = &self.store;
        let objects: Vec<_> = retry_with_backoff("list_segments", &self.backoff, || {
            let p = prefix.clone();
            async move { store.list(Some(&p)).try_collect::<Vec<_>>().await }
        })
        .await
        .map_err(|e| crate::storage::StorageError::InvalidPath {
            detail: format!("object store list failed: {e}"),
        })?;

        Ok(objects
            .into_iter()
            .filter_map(|meta| {
                let name = meta.location.filename()?;
                SegmentPath::new(ns.clone(), shard, name.to_string()).ok()
            })
            .collect())
    }

    async fn exists(&self, path: &SegmentPath) -> crate::storage::error::Result<bool> {
        // Check cache first
        if let Some(ref cache) = self.cache {
            if cache.contains(&Self::cache_key(path)) {
                return Ok(true);
            }
        }

        let obj_path = Self::to_obj_path(path);
        // Retry with exponential backoff for transient errors
        let store = &self.store;
        match retry_with_backoff("exists", &self.backoff, || {
            let op = obj_path.clone();
            async move { store.head(&op).await }
        })
        .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(crate::storage::StorageError::InvalidPath {
                detail: format!("object store exists check failed: {e}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronix_core::ShardId;
    use tempfile::TempDir;

    /// Create a backend backed by in-memory object store (no cache).
    fn make_in_memory() -> ObjectStoreBackend {
        let store = Arc::new(object_store::memory::InMemory::new());
        ObjectStoreBackend::from_store(store, None)
    }

    /// Create a backend backed by in-memory object store with cache.
    async fn make_in_memory_cached() -> (ObjectStoreBackend, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cache_config = crate::objstore::cache::CacheConfig {
            cache_dir: tmp.path().to_path_buf(),
            max_size_bytes: 1024 * 1024,
        };
        let cache = DiskCache::new(cache_config).await.unwrap();
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::from_store(store, Some(cache));
        (backend, tmp)
    }

    // ── Raw put/get/delete/list API tests ──────────────────────────

    #[tokio::test]
    async fn raw_put_and_get() {
        let backend = make_in_memory();
        backend
            .put("test/object", Bytes::from_static(b"hello"))
            .await
            .unwrap();

        let data = backend.get("test/object").await.unwrap();
        assert_eq!(&*data, b"hello");
    }

    #[tokio::test]
    async fn raw_get_not_found() {
        let backend = make_in_memory();
        let err = backend.get("nonexistent").await.unwrap_err();
        assert!(matches!(err, ObjStoreError::NotFound { .. }));
    }

    #[tokio::test]
    async fn raw_delete() {
        let backend = make_in_memory();
        backend
            .put("to_delete", Bytes::from_static(b"data"))
            .await
            .unwrap();
        backend.delete("to_delete").await.unwrap();
        assert!(backend.get("to_delete").await.is_err());
    }

    #[tokio::test]
    async fn raw_list() {
        let backend = make_in_memory();
        backend
            .put("prefix/a", Bytes::from_static(b"1"))
            .await
            .unwrap();
        backend
            .put("prefix/b", Bytes::from_static(b"2"))
            .await
            .unwrap();
        backend
            .put("other/c", Bytes::from_static(b"3"))
            .await
            .unwrap();

        let items = backend.list("prefix").await.unwrap();
        assert_eq!(items.len(), 2);
    }

    fn default_ns() -> NamespaceId {
        NamespaceId::default_namespace()
    }

    // ── StorageBackend trait tests ─────────────────────────────────

    #[tokio::test]
    async fn storage_put_and_get_segment() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(1), "seg_001.csx").unwrap();

        backend.put_segment(&path, b"segment-data").await.unwrap();
        let data = backend.get_segment(&path).await.unwrap();
        assert_eq!(data, b"segment-data");
    }

    #[tokio::test]
    async fn storage_get_range() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(2), "seg.csx").unwrap();
        backend
            .put_segment(&path, b"0123456789abcdef")
            .await
            .unwrap();

        let range = backend.get_range(&path, 4, 6).await.unwrap();
        assert_eq!(range, b"456789");
    }

    #[tokio::test]
    async fn storage_delete_segment() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(3), "seg.csx").unwrap();
        backend.put_segment(&path, b"data").await.unwrap();

        backend.delete_segment(&path).await.unwrap();
        assert!(!backend.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn storage_list_segments() {
        let backend = make_in_memory();
        let ns = default_ns();
        let p1 = SegmentPath::new(ns.clone(), ShardId(5), "a.csx").unwrap();
        let p2 = SegmentPath::new(ns.clone(), ShardId(5), "b.csx").unwrap();
        let p3 = SegmentPath::new(ns.clone(), ShardId(6), "c.csx").unwrap();

        backend.put_segment(&p1, b"1").await.unwrap();
        backend.put_segment(&p2, b"2").await.unwrap();
        backend.put_segment(&p3, b"3").await.unwrap();

        let segments = backend.list_segments(&ns, &ShardId(5)).await.unwrap();
        assert_eq!(segments.len(), 2);

        let names: Vec<_> = segments
            .iter()
            .map(|s| s.segment_name().to_string())
            .collect();
        assert!(names.contains(&"a.csx".to_string()));
        assert!(names.contains(&"b.csx".to_string()));
    }

    #[tokio::test]
    async fn storage_exists() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(1), "test.csx").unwrap();

        assert!(!backend.exists(&path).await.unwrap());
        backend.put_segment(&path, b"data").await.unwrap();
        assert!(backend.exists(&path).await.unwrap());
    }

    // ── Cache integration tests ───────────────────────────────────

    #[tokio::test]
    async fn cached_backend_serves_from_cache_on_hit() {
        let (backend, _tmp) = make_in_memory_cached().await;
        let path = SegmentPath::new(default_ns(), ShardId(1), "cached.csx").unwrap();

        backend.put_segment(&path, b"cached-data").await.unwrap();

        // First get populates cache, second serves from it
        let data1 = backend.get_segment(&path).await.unwrap();
        let data2 = backend.get_segment(&path).await.unwrap();
        assert_eq!(data1, data2);
        assert_eq!(data1, b"cached-data");
    }

    #[tokio::test]
    async fn cached_backend_invalidates_on_delete() {
        let (backend, _tmp) = make_in_memory_cached().await;
        let path = SegmentPath::new(default_ns(), ShardId(1), "to-delete.csx").unwrap();

        backend.put_segment(&path, b"data").await.unwrap();
        // Populate cache
        let _ = backend.get_segment(&path).await.unwrap();

        backend.delete_segment(&path).await.unwrap();
        assert!(!backend.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn cached_backend_get_range_from_cache() {
        let (backend, _tmp) = make_in_memory_cached().await;
        let path = SegmentPath::new(default_ns(), ShardId(2), "ranged.csx").unwrap();

        backend
            .put_segment(&path, b"0123456789abcdef")
            .await
            .unwrap();

        // Populate cache via full get
        let _ = backend.get_segment(&path).await.unwrap();

        // Range served from cache
        let range = backend.get_range(&path, 10, 4).await.unwrap();
        assert_eq!(range, b"abcd");
    }

    // ── Multipart upload tests ────────────────────────────────────

    #[tokio::test]
    async fn small_segment_uses_single_put() {
        // Threshold set high — small data goes through single PUT path.
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::from_store_with_threshold(store, None, 1024);
        let path = SegmentPath::new(default_ns(), ShardId(1), "small.csx").unwrap();
        let data = vec![0u8; 512]; // below 1024 threshold

        backend.put_segment(&path, &data).await.unwrap();
        let got = backend.get_segment(&path).await.unwrap();
        assert_eq!(got.len(), 512);
    }

    #[tokio::test]
    async fn large_segment_uses_multipart_upload() {
        // Threshold set low so our test data exceeds it.
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::from_store_with_threshold(store, None, 256);
        let path = SegmentPath::new(default_ns(), ShardId(1), "large.csx").unwrap();
        let data = vec![42u8; 1024]; // above 256 threshold

        backend.put_segment(&path, &data).await.unwrap();
        let got = backend.get_segment(&path).await.unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn multipart_threshold_boundary_exact() {
        // Data exactly at threshold should use multipart.
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::from_store_with_threshold(store, None, 512);
        let path = SegmentPath::new(default_ns(), ShardId(1), "exact.csx").unwrap();
        let data = vec![7u8; 512]; // exactly at threshold

        backend.put_segment(&path, &data).await.unwrap();
        let got = backend.get_segment(&path).await.unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn default_threshold_is_8mib() {
        let backend = make_in_memory();
        assert_eq!(
            backend.multipart_threshold_bytes,
            DEFAULT_MULTIPART_THRESHOLD
        );
        assert_eq!(DEFAULT_MULTIPART_THRESHOLD, 8 * 1024 * 1024);
    }

    // ── URL parsing tests ─────────────────────────────────────────

    #[tokio::test]
    async fn build_store_memory() {
        let store = ObjectStoreBackend::build_store("memory://test").unwrap();
        // Verify it's usable
        store
            .put(
                &ObjPath::from("test"),
                PutPayload::from(Bytes::from_static(b"ok")),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn build_store_local_path() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        let store = ObjectStoreBackend::build_store(path).unwrap();
        store
            .put(
                &ObjPath::from("test"),
                PutPayload::from(Bytes::from_static(b"ok")),
            )
            .await
            .unwrap();
    }

    #[test]
    fn build_store_unsupported_scheme() {
        let err = ObjectStoreBackend::build_store("ftp://host/path").unwrap_err();
        assert!(matches!(err, ObjStoreError::InvalidConfig { .. }));
    }

    // ── Debug / Display ───────────────────────────────────────────

    #[test]
    fn debug_format() {
        let backend = make_in_memory();
        let d = format!("{backend:?}");
        assert!(d.contains("ObjectStoreBackend"));
        assert!(d.contains("custom://"));
    }

    // ── Additional edge-case tests ────────────────────────────────

    #[tokio::test]
    async fn get_segment_not_found() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(99), "missing.csx").unwrap();
        let err = backend.get_segment(&path).await.unwrap_err();
        assert!(
            matches!(err, crate::storage::StorageError::NotFound { .. }),
            "expected NotFound, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn list_segments_empty_shard() {
        let backend = make_in_memory();
        let segments = backend
            .list_segments(&default_ns(), &ShardId(42))
            .await
            .unwrap();
        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn delete_segment_idempotent() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(1), "gone.csx").unwrap();
        // Deleting a non-existent segment should not error.
        backend.delete_segment(&path).await.unwrap();
    }

    #[tokio::test]
    async fn segment_size_returns_correct_len() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(1), "sized.csx").unwrap();

        // Not found
        assert_eq!(backend.segment_size(&path).await.unwrap(), None);

        // After put
        backend.put_segment(&path, b"twelve bytes").await.unwrap();
        assert_eq!(backend.segment_size(&path).await.unwrap(), Some(12));
    }

    #[tokio::test]
    async fn exists_nonexistent_returns_false() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(1), "nope.csx").unwrap();
        assert!(!backend.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn build_store_file_scheme() {
        let tmp = TempDir::new().unwrap();
        let url = format!("file://{}", tmp.path().to_str().unwrap());
        let store = ObjectStoreBackend::build_store(&url).unwrap();
        store
            .put(
                &ObjPath::from("test"),
                PutPayload::from(Bytes::from_static(b"ok")),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_segment_overwrite() {
        let backend = make_in_memory();
        let path = SegmentPath::new(default_ns(), ShardId(1), "overwrite.csx").unwrap();

        backend.put_segment(&path, b"version-1").await.unwrap();
        backend.put_segment(&path, b"version-2").await.unwrap();

        let data = backend.get_segment(&path).await.unwrap();
        assert_eq!(data, b"version-2");
    }
}
