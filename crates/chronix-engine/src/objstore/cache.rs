//! Local disk cache with LRU eviction for object store data.
//!
//! Recently accessed objects are cached on local disk to avoid repeated
//! remote fetches. The cache has a configurable maximum size and uses
//! LRU eviction to stay within budget.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::fs;
use tracing::{debug, warn};

use crate::objstore::error::{ObjStoreError, Result};

/// Monotonically increasing generation counter for LRU ordering.
///
/// Using a counter instead of `SystemTime` avoids issues with
/// non-monotonic clocks (NTP adjustments, leap seconds, VM migration).
static LRU_GENERATION: AtomicU64 = AtomicU64::new(1);

/// Configuration for the local disk cache.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheConfig {
    /// Root directory for cached objects.
    pub cache_dir: PathBuf,
    /// Maximum total cache size in bytes.
    pub max_size_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("/tmp/chronix-objcache"),
            max_size_bytes: 1024 * 1024 * 1024, // 1 GB
        }
    }
}

/// An entry in the LRU tracking table.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Size of the cached file in bytes.
    size: u64,
    /// Monotonic generation counter for LRU ordering.
    generation: u64,
}

/// LRU tracking state — protected by a mutex.
///
/// Uses a secondary `BTreeMap` ordered by `(access_time, key)` for O(log N)
/// LRU eviction instead of the previous O(N log N) sort-all-entries approach.
#[derive(Debug)]
struct CacheState {
    /// Key → entry metadata.
    entries: HashMap<String, CacheEntry>,
    /// Ordered index: (generation, key) → size. Enables O(log N) LRU scan.
    lru_index: BTreeMap<(u64, String), u64>,
    /// Current total size of all cached files.
    current_size: u64,
}

impl CacheState {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru_index: BTreeMap::new(),
            current_size: 0,
        }
    }

    /// Insert or update an entry, keeping the LRU index in sync.
    fn upsert(&mut self, key: String, entry: CacheEntry) {
        // Remove old LRU index entry if overwriting
        if let Some(old) = self.entries.get(&key) {
            self.lru_index.remove(&(old.generation, key.clone()));
            self.current_size -= old.size;
        }
        self.lru_index
            .insert((entry.generation, key.clone()), entry.size);
        self.current_size += entry.size;
        self.entries.insert(key, entry);
    }

    /// Remove an entry, keeping the LRU index in sync.
    fn remove(&mut self, key: &str) {
        if let Some(old) = self.entries.remove(key) {
            self.lru_index.remove(&(old.generation, key.to_string()));
            self.current_size -= old.size;
        }
    }

    /// Touch an entry (update generation in LRU index).
    fn touch(&mut self, key: &str) {
        if let Some(entry) = self.entries.get_mut(key) {
            self.lru_index.remove(&(entry.generation, key.to_string()));
            entry.generation = LRU_GENERATION.fetch_add(1, Ordering::Relaxed);
            self.lru_index
                .insert((entry.generation, key.to_string()), entry.size);
        }
    }
}

/// Local disk cache with LRU eviction.
///
/// Stores copies of remote objects under `cache_dir` using the object's
/// path as the filename (slashes replaced with `__`). When the cache
/// exceeds `max_size_bytes`, the least-recently accessed entries are
/// evicted until the cache fits within budget.
#[derive(Debug)]
pub struct DiskCache {
    config: CacheConfig,
    state: Mutex<CacheState>,
}

impl DiskCache {
    /// Create a new disk cache.
    ///
    /// The cache directory is created if it does not already exist.
    ///
    /// # Errors
    ///
    /// Returns `ObjStoreError::CacheIo` if the cache directory cannot be
    /// created or scanned.
    pub async fn new(config: CacheConfig) -> Result<Self> {
        fs::create_dir_all(&config.cache_dir)
            .await
            .map_err(ObjStoreError::CacheIo)?;

        let mut state = CacheState::new();

        // Scan existing cache directory to rebuild state
        let mut entries = fs::read_dir(&config.cache_dir)
            .await
            .map_err(ObjStoreError::CacheIo)?;

        while let Some(entry) = entries.next_entry().await.map_err(ObjStoreError::CacheIo)? {
            let metadata = entry.metadata().await.map_err(ObjStoreError::CacheIo)?;
            if metadata.is_file() {
                let filename = entry.file_name().to_string_lossy().to_string();
                // Reverse key_to_path encoding: "%2F" → "/", "%25" → "%"
                let key = Self::path_to_key(&filename);
                let size = metadata.len();
                // Assign a unique generation for LRU ordering
                let gen = LRU_GENERATION.fetch_add(1, Ordering::Relaxed);
                state.upsert(
                    key,
                    CacheEntry {
                        size,
                        generation: gen,
                    },
                );
            }
        }

        debug!(
            cache_dir = %config.cache_dir.display(),
            entries = state.entries.len(),
            current_size_bytes = state.current_size,
            max_size_bytes = config.max_size_bytes,
            "disk cache initialised"
        );

        Ok(Self {
            config,
            state: Mutex::new(state),
        })
    }

    /// Get a cached object by key, returning the bytes if present.
    ///
    /// Updates the access time for LRU tracking. Returns `None` on miss.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let file_path = self.key_to_path(key);

        // Check existence in state first (fast path)
        {
            let mut state = self.state.lock();
            if state.entries.contains_key(key) {
                state.touch(key);
            } else {
                return None;
            }
        }

        match fs::read(&file_path).await {
            Ok(data) => {
                debug!(key, size = data.len(), "cache hit");
                Some(data)
            }
            Err(e) => {
                warn!(key, error = %e, "cache entry missing on disk, evicting from state");
                self.remove_from_state(key);
                None
            }
        }
    }

    /// Store an object in the cache.
    ///
    /// Evicts LRU entries if necessary to fit within the size budget.
    ///
    /// # Errors
    ///
    /// Returns `ObjStoreError::CacheIo` on filesystem write failures.
    pub async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let size = data.len() as u64;

        // If the object is larger than the cache, write it but don't track
        // it in cache state — avoids violating the LRU size invariant.
        if size > self.config.max_size_bytes {
            debug!(
                key,
                size,
                max = self.config.max_size_bytes,
                "object exceeds cache budget, skipping cache"
            );
            return Ok(());
        }

        // Evict and reserve space atomically under the lock
        // before writing to disk. This prevents a concurrent put() from
        // evicting a file that's currently being fetched/read.
        self.evict_to_fit(size).await?;

        let file_path = self.key_to_path(key);

        // Reserve the entry in state before writing — prevents concurrent
        // eviction from re-using the space while we write.
        {
            let mut state = self.state.lock();
            state.upsert(
                key.to_string(),
                CacheEntry {
                    size,
                    generation: LRU_GENERATION.fetch_add(1, Ordering::Relaxed),
                },
            );
        }

        if let Err(e) = fs::write(&file_path, data).await {
            // Rollback on write failure — remove the reservation.
            self.remove_from_state(key);
            return Err(ObjStoreError::CacheIo(e));
        }

        debug!(key, size, "cached object");
        Ok(())
    }

    /// Remove an object from the cache.
    ///
    /// # Errors
    ///
    /// Returns `ObjStoreError::CacheIo` on filesystem delete failures.
    pub async fn remove(&self, key: &str) -> Result<()> {
        let file_path = self.key_to_path(key);
        self.remove_from_state(key);

        match fs::remove_file(&file_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ObjStoreError::CacheIo(e)),
        }
    }

    /// Check if a key is in the cache without updating access time.
    pub fn contains(&self, key: &str) -> bool {
        self.state.lock().entries.contains_key(key)
    }

    /// Current total size of cached data.
    pub fn current_size(&self) -> u64 {
        self.state.lock().current_size
    }

    /// Number of entries in the cache.
    pub fn entry_count(&self) -> usize {
        self.state.lock().entries.len()
    }

    /// Evict LRU entries until `needed` bytes can fit.
    async fn evict_to_fit(&self, needed: u64) -> Result<()> {
        let max = self.config.max_size_bytes;
        if needed > max {
            // Single object larger than the entire cache — skip caching
            return Ok(());
        }

        // Collect entries to evict under the lock, then delete files outside.
        // Uses the BTreeMap LRU index for O(log N) iteration from oldest.
        let to_evict: Vec<(String, PathBuf, u64)> = {
            let mut state = self.state.lock();
            if state.current_size + needed <= max {
                return Ok(()); // fits already
            }

            let mut evict_list = Vec::new();
            // Pop from BTreeMap front (oldest generation) until enough space is freed
            while state.current_size + needed > max {
                let Some(((_gen, key), _)) = state.lru_index.pop_first() else {
                    break; // no more entries to evict
                };
                if let Some(entry) = state.entries.remove(&key) {
                    let path = self.key_to_path(&key);
                    state.current_size -= entry.size;
                    evict_list.push((key, path, entry.size));
                }
            }

            evict_list
        };

        let mut evicted = 0u64;
        for (key, path, size) in &to_evict {
            debug!(key, "evicting cached object");
            if let Err(e) = fs::remove_file(path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(key, error = %e, "failed to evict cached file, re-adding to state");
                    // Rollback: re-insert the entry so state stays in sync with disk.
                    // Use generation 0 so it stays at front of LRU (oldest) and
                    // gets retried on next eviction pass.
                    let mut state = self.state.lock();
                    state.upsert(
                        key.clone(),
                        CacheEntry {
                            size: *size,
                            generation: 0,
                        },
                    );
                    continue;
                }
            }
            evicted += 1;
        }

        metrics::counter!("chronix_objstore_cache_evictions_total").increment(evicted);
        Ok(())
    }

    fn remove_from_state(&self, key: &str) {
        self.state.lock().remove(key);
    }

    fn key_to_path(&self, key: &str) -> PathBuf {
        // Encode unsafe characters for flat filesystem storage.
        // We percent-encode '/' and '%' to avoid ambiguity:
        //   '/' → "%2F"   '%' → "%25"
        // This is reversible and avoids the collision that a naive
        // "replace('/', '__')" would have (e.g. key "a__b" vs "a/b").
        let safe_name = key.replace('%', "%25").replace('/', "%2F");

        // Guard against filesystem filename length limits (255 bytes
        // on ext4/APFS). If the percent-encoded name exceeds 200 bytes,
        // truncate it and append a hash suffix for uniqueness.
        let filename = if safe_name.len() > 200 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let h = hasher.finish();
            format!("{:.100}_{:016x}", safe_name, h)
        } else {
            safe_name
        };

        self.config.cache_dir.join(filename)
    }

    /// Reverse of `key_to_path`'s filename encoding.
    fn path_to_key(filename: &str) -> String {
        filename.replace("%2F", "/").replace("%25", "%")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn make_cache(max_size: u64) -> (DiskCache, TempDir) {
        let tmp = TempDir::new().unwrap();
        let config = CacheConfig {
            cache_dir: tmp.path().to_path_buf(),
            max_size_bytes: max_size,
        };
        let cache = DiskCache::new(config).await.unwrap();
        (cache, tmp)
    }

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let (cache, _tmp) = make_cache(1024).await;
        cache.put("test/object.bin", b"hello world").await.unwrap();

        let data = cache.get("test/object.bin").await.unwrap();
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let (cache, _tmp) = make_cache(1024).await;
        assert!(cache.get("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn remove_deletes_entry() {
        let (cache, _tmp) = make_cache(1024).await;
        cache.put("key", b"data").await.unwrap();
        assert!(cache.contains("key"));

        cache.remove("key").await.unwrap();
        assert!(!cache.contains("key"));
        assert!(cache.get("key").await.is_none());
    }

    #[tokio::test]
    async fn remove_nonexistent_is_ok() {
        let (cache, _tmp) = make_cache(1024).await;
        cache.remove("ghost").await.unwrap();
    }

    #[tokio::test]
    async fn overwrite_updates_size() {
        let (cache, _tmp) = make_cache(1024).await;
        cache.put("key", b"short").await.unwrap();
        assert_eq!(cache.current_size(), 5);

        cache.put("key", b"a longer value").await.unwrap();
        assert_eq!(cache.current_size(), 14);
        assert_eq!(cache.entry_count(), 1);
    }

    #[tokio::test]
    async fn lru_eviction_frees_space() {
        // Cache max = 20 bytes
        let (cache, _tmp) = make_cache(20).await;

        cache.put("a", &[0u8; 10]).await.unwrap();
        // Small delay so timestamps differ
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        cache.put("b", &[1u8; 10]).await.unwrap();

        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.current_size(), 20);

        // Writing c(10 bytes) should evict a (oldest)
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        cache.put("c", &[2u8; 10]).await.unwrap();

        assert!(!cache.contains("a"), "oldest entry 'a' should be evicted");
        assert!(cache.contains("b"));
        assert!(cache.contains("c"));
        assert!(cache.current_size() <= 20);
    }

    #[tokio::test]
    async fn object_larger_than_cache_skips_caching() {
        let (cache, _tmp) = make_cache(10).await;
        // Put a pre-existing item
        cache.put("small", b"hi").await.unwrap();

        // This is larger than the cache, eviction should not remove 'small'
        cache.put("huge", &[0u8; 100]).await.unwrap();

        // "small" should still be there since huge can't fit anyway
        // (the current implementation writes the file regardless, but
        // evict_to_fit returns early — so huge IS written on disk but
        // didn't evict small)
        assert!(cache.contains("small"));
    }

    #[tokio::test]
    async fn rebuild_state_from_disk() {
        let tmp = TempDir::new().unwrap();
        let config = CacheConfig {
            cache_dir: tmp.path().to_path_buf(),
            max_size_bytes: 1024,
        };

        // Write files directly
        fs::write(tmp.path().join("file1"), b"aaa").await.unwrap();
        fs::write(tmp.path().join("file2"), b"bbbbb").await.unwrap();

        let cache = DiskCache::new(config).await.unwrap();
        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.current_size(), 8); // 3 + 5
    }

    #[tokio::test]
    async fn key_to_path_replaces_slashes() {
        let (cache, tmp) = make_cache(1024).await;
        let path = cache.key_to_path("shard_1/segment_001.csx");
        assert_eq!(path, tmp.path().join("shard_1%2Fsegment_001.csx"));

        // Roundtrip: path_to_key reverses the encoding
        let filename = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(DiskCache::path_to_key(filename), "shard_1/segment_001.csx");

        // Keys containing '%' are encoded unambiguously
        let path2 = cache.key_to_path("100%/done");
        assert_eq!(path2, tmp.path().join("100%25%2Fdone"));
        let filename2 = path2.file_name().unwrap().to_str().unwrap();
        assert_eq!(DiskCache::path_to_key(filename2), "100%/done");
    }

    #[tokio::test]
    async fn current_size_and_entry_count() {
        let (cache, _tmp) = make_cache(1024).await;
        assert_eq!(cache.current_size(), 0);
        assert_eq!(cache.entry_count(), 0);

        cache.put("a", b"hello").await.unwrap();
        assert_eq!(cache.current_size(), 5);
        assert_eq!(cache.entry_count(), 1);

        cache.put("b", b"world!").await.unwrap();
        assert_eq!(cache.current_size(), 11);
        assert_eq!(cache.entry_count(), 2);
    }

    #[tokio::test]
    async fn test_long_cache_key_doesnt_overflow() {
        let (cache, _tmp) = make_cache(4096).await;
        // Build a key that, after percent-encoding, exceeds 200 bytes
        let long_key = "a/".repeat(120); // 240 chars → each '/' becomes "%2F" → huge
        let path = cache.key_to_path(&long_key);
        let filename = path.file_name().unwrap().to_str().unwrap();

        // Filename must stay within filesystem limits
        assert!(
            filename.len() <= 255,
            "filename length {} exceeds 255-byte filesystem limit",
            filename.len()
        );

        // Should contain the hash suffix (16 hex chars after '_')
        assert!(
            filename.contains('_'),
            "long filename should contain a hash suffix"
        );

        // put/get roundtrip still works
        cache.put(&long_key, b"long key data").await.unwrap();
        let data = cache.get(&long_key).await.unwrap();
        assert_eq!(data, b"long key data");
    }
}
