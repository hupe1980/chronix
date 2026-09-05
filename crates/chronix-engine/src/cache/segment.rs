//! Segment Cache — scan-resistant cache for decoded Arrow arrays from segment files.
//!
//! Caches decoded column data keyed by `(SegmentId, row_group, column_name)`.
//! Uses **W-TinyLFU** admission control with LRU eviction and a configurable
//! memory bound. The frequency sketch (Count-Min Sketch) prevents sequential
//! scan patterns from thrashing the cache by rejecting cold newcomers when
//! they would evict hotter items.
//!
//! Also features singleflight dedup (only one loader per cache key), O(1)
//! eviction via an LRU linked list, and probabilistic promotion to minimise
//! write-lock contention on the hot read path.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::ArrayRef;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use chronix_core::SegmentId;

// ── Frequency sketch (Count-Min Sketch for TinyLFU) ───────────────────

/// Number of rows in the Count-Min Sketch.
const SKETCH_DEPTH: usize = 4;
/// Default sketch width, used when no capacity hint is given.
const DEFAULT_SKETCH_WIDTH: usize = 2048;

/// A Count-Min Sketch for estimating access frequency.
///
/// Used by the TinyLFU admission filter to decide whether a newly loaded
/// item is "hot" enough to evict the current LRU victim. Each key is
/// hashed to `SKETCH_DEPTH` rows; the minimum counter across rows is the
/// estimated frequency.
///
/// Width scales with cache capacity to reduce hash collisions
/// in large caches.
struct FrequencySketch {
    /// Counter matrix: `SKETCH_DEPTH` rows × `width` columns.
    table: Vec<Vec<u16>>,
    /// Number of columns (must be power of 2).
    width: usize,
    /// Total number of increment operations (for periodic reset).
    additions: u64,
    /// Reset all counters after this many increments.
    reset_interval: u64,
}

impl FrequencySketch {
    /// Sketch with the default width (test-only convenience).
    #[cfg(test)]
    fn new() -> Self {
        Self::with_width(DEFAULT_SKETCH_WIDTH)
    }

    /// Create a sketch with a specific width (rounded up to power of 2).
    fn with_width(width: usize) -> Self {
        let width = width.next_power_of_two().max(64);
        Self {
            table: vec![vec![0u16; width]; SKETCH_DEPTH],
            width,
            additions: 0,
            reset_interval: 10 * width as u64,
        }
    }

    /// Increment the frequency estimate for a key.
    fn increment(&mut self, hash: u64) {
        for row in 0..SKETCH_DEPTH {
            let col = self.index(hash, row);
            self.table[row][col] = self.table[row][col].saturating_add(1);
        }
        self.additions += 1;
        if self.additions >= self.reset_interval {
            self.reset();
        }
    }

    /// Estimate the frequency of a key (minimum across all rows).
    fn estimate(&self, hash: u64) -> u16 {
        let mut min = u16::MAX;
        for row in 0..SKETCH_DEPTH {
            let col = self.index(hash, row);
            min = min.min(self.table[row][col]);
        }
        min
    }

    /// Halve all counters (aging / decay) and reset the addition counter.
    fn reset(&mut self) {
        for row in &mut self.table {
            for cell in row.iter_mut() {
                *cell >>= 1;
            }
        }
        self.additions = 0;
    }

    /// Compute the column index for a given hash and row, using
    /// hash mixing to produce independent indices per row.
    fn index(&self, hash: u64, row: usize) -> usize {
        let mixed = hash.wrapping_mul(0x9E37_79B9_7F4A_7C15_u64.wrapping_add(row as u64));
        (mixed >> 48) as usize & (self.width - 1)
    }
}

/// Compute a 64-bit hash for a cache key (used by the frequency sketch).
fn key_hash(key: &SegmentCacheKey) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

/// Error returned to waiter threads when a singleflight cache load fails.
///
/// The original typed error cannot be transferred across threads (the
/// loader's error type `E` is not available to waiters), so the error
/// message is preserved as a `String`.
#[derive(Debug, Clone)]
pub struct CacheLoadError(pub String);

impl fmt::Display for CacheLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cache load failed: {}", self.0)
    }
}

impl std::error::Error for CacheLoadError {}

impl From<CacheLoadError> for String {
    fn from(e: CacheLoadError) -> Self {
        e.0
    }
}

/// Key for segment cache lookups.
///
/// `column` uses `Arc<str>` instead of `String` to make
/// LRU promotion clones cheap (atomic refcount bump vs heap alloc).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentCacheKey {
    /// The segment this data came from.
    pub segment_id: SegmentId,
    /// Row group index within the segment.
    pub row_group: u32,
    /// Column name (cheaply clonable via Arc).
    pub column: Arc<str>,
}

/// An entry in the segment cache with LRU tracking.
struct CacheEntry {
    /// The cached Arrow array.
    array: ArrayRef,
    /// Byte size of this array (for eviction accounting).
    byte_size: usize,
    /// Previous key in LRU order (toward most-recently-used).
    prev: Option<SegmentCacheKey>,
    /// Next key in LRU order (toward least-recently-used).
    next: Option<SegmentCacheKey>,
}

/// Singleflight state for a key currently being loaded.
///
/// Uses [`tokio::sync::Notify`] instead of `parking_lot::Condvar` so
/// that waiters yield the tokio worker thread instead of blocking it.
struct InflightEntry {
    /// The load result, set by the loader thread.
    result: Mutex<Option<Result<ArrayRef, String>>>,
    /// Async notification channel — wakes all `.notified()` futures.
    notify: Notify,
}

/// Inner mutable state protected by a single mutex.
struct CacheInner {
    /// Cached entries in a doubly-linked LRU list threaded through the HashMap.
    entries: HashMap<SegmentCacheKey, CacheEntry>,
    /// Head of LRU list — most recently used.
    lru_head: Option<SegmentCacheKey>,
    /// Tail of LRU list — least recently used (eviction candidate).
    lru_tail: Option<SegmentCacheKey>,
    /// In-flight loads for singleflight dedup.
    inflight: HashMap<SegmentCacheKey, Arc<InflightEntry>>,
}

/// LRU segment cache with configurable memory bound.
///
/// Stores decoded Arrow arrays from segment files. When the cache exceeds
/// its memory bound, the least-recently-used entries are evicted via an
/// O(1) linked-list removal.
///
/// **Singleflight:** When multiple threads request the same uncached key
/// simultaneously, only one thread invokes the loader while the others
/// wait. This prevents thundering-herd cache stampedes.
///
/// Thread-safe via `RwLock<CacheInner>` with atomic counters for stats.
pub struct SegmentCache {
    /// Mutable cache state.
    inner: RwLock<CacheInner>,
    /// TinyLFU frequency sketch — behind its own Mutex so hits on the
    /// read-lock fast path can still record frequency.
    sketch: Mutex<FrequencySketch>,
    /// Maximum cache size in bytes.
    max_bytes: usize,
    /// Current cache size in bytes.
    current_bytes: AtomicU64,
    /// Cache hit counter.
    hits: AtomicU64,
    /// Cache miss counter.
    misses: AtomicU64,
}

impl SegmentCache {
    /// Create a new segment cache with the given maximum size in bytes.
    #[must_use]
    pub fn new(max_bytes: usize) -> Self {
        // Scale sketch width with cache capacity. Assume ~64KB
        // average block size, then use 10× estimated max entries as width.
        let estimated_entries = (max_bytes / (64 * 1024)).max(1);
        let sketch_width = (estimated_entries * 10).clamp(DEFAULT_SKETCH_WIDTH, 1 << 20);
        Self {
            inner: RwLock::new(CacheInner {
                entries: HashMap::new(),
                lru_head: None,
                lru_tail: None,
                inflight: HashMap::new(),
            }),
            sketch: Mutex::new(FrequencySketch::with_width(sketch_width)),
            max_bytes,
            current_bytes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Promote a key to the head (most-recently-used) of the LRU list.
    fn promote(inner: &mut CacheInner, key: &SegmentCacheKey) {
        // Already at head?
        if inner.lru_head.as_ref() == Some(key) {
            return;
        }
        // Detach from current position
        Self::detach(inner, key);
        // Push to head
        Self::push_head(inner, key);
    }

    /// Detach a key from the LRU doubly-linked list.
    fn detach(inner: &mut CacheInner, key: &SegmentCacheKey) {
        let (prev, next) = {
            let entry = match inner.entries.get(key) {
                Some(e) => e,
                None => return,
            };
            (entry.prev.clone(), entry.next.clone())
        };

        if let Some(ref prev_key) = prev {
            if let Some(prev_entry) = inner.entries.get_mut(prev_key) {
                prev_entry.next = next.clone();
            }
        } else {
            // This was the head
            inner.lru_head = next.clone();
        }

        if let Some(ref next_key) = next {
            if let Some(next_entry) = inner.entries.get_mut(next_key) {
                next_entry.prev = prev;
            }
        } else {
            // This was the tail
            inner.lru_tail = prev;
        }

        if let Some(entry) = inner.entries.get_mut(key) {
            entry.prev = None;
            entry.next = None;
        }
    }

    /// Push a key to the head of the LRU list. The key must already exist
    /// in `entries` but must NOT be linked in the list.
    fn push_head(inner: &mut CacheInner, key: &SegmentCacheKey) {
        let old_head = inner.lru_head.take();
        if let Some(ref old_head_key) = old_head {
            if let Some(old_entry) = inner.entries.get_mut(old_head_key) {
                old_entry.prev = Some(key.clone());
            }
        }
        if let Some(entry) = inner.entries.get_mut(key) {
            entry.prev = None;
            entry.next = old_head.clone();
        }
        inner.lru_head = Some(key.clone());
        if inner.lru_tail.is_none() {
            inner.lru_tail = Some(key.clone());
        }
    }

    /// Get a cached array, or load it using the provided closure on miss.
    ///
    /// Uses singleflight dedup: if another task is already loading the
    /// same key, this task awaits the result instead of invoking the
    /// loader again.  The wait uses [`tokio::sync::Notify`] so the
    /// calling tokio worker thread is **not** blocked.
    ///
    /// Cache hits use a read lock (no contention).  LRU promotion
    /// is probabilistic — only every 16th hit acquires a write lock
    /// to reorder the LRU list, amortising lock contention on the
    /// hot read path.
    ///
    /// # Errors
    ///
    /// Returns an error if the loader function fails.
    pub async fn get_or_load<F, E>(
        &self,
        key: SegmentCacheKey,
        loader: F,
    ) -> std::result::Result<ArrayRef, E>
    where
        F: FnOnce() -> std::result::Result<ArrayRef, E>,
        E: std::fmt::Display + Clone + From<CacheLoadError>,
    {
        // Decision from the locked critical section — no MutexGuard
        // escapes this block, so the future remains Send.
        enum Action {
            Hit(ArrayRef),
            Wait(Arc<InflightEntry>),
            Load(Arc<InflightEntry>),
        }

        // Try read lock first for cache hits — avoids write
        // lock contention on the hot path.
        {
            let inner = self.inner.read();
            if let Some(entry) = inner.entries.get(&key) {
                let arr = entry.array.clone();
                drop(inner);
                self.sketch.lock().increment(key_hash(&key));
                let n = self.hits.fetch_add(1, Ordering::Relaxed);
                // Probabilistic LRU promotion: only every 16th hit takes a
                // write lock to reorder the list.  This trades a small LRU
                // accuracy loss for dramatically reduced lock contention.
                if n.is_multiple_of(16) {
                    let mut w = self.inner.write();
                    Self::promote(&mut w, &key);
                }
                return Ok(arr);
            }
        }

        let action = {
            let mut inner = self.inner.write();

            // Re-check under write lock (another thread may have inserted)
            if let Some(entry) = inner.entries.get(&key) {
                let arr = entry.array.clone();
                Self::promote(&mut inner, &key);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Action::Hit(arr)
            } else if let Some(waiter) = inner.inflight.get(&key).cloned() {
                self.misses.fetch_add(1, Ordering::Relaxed);
                Action::Wait(waiter)
            } else {
                let entry = Arc::new(InflightEntry {
                    result: Mutex::new(None),
                    notify: Notify::new(),
                });
                inner.inflight.insert(key.clone(), entry.clone());
                self.misses.fetch_add(1, Ordering::Relaxed);
                Action::Load(entry)
            }
            // MutexGuard dropped here
        };

        match action {
            Action::Hit(arr) => Ok(arr),
            Action::Wait(waiter) => self.wait_inflight(&waiter).await.map_err(E::from),
            Action::Load(entry) => {
                // We are the loader — call the closure outside of any lock
                let load_result = loader();

                // Publish result
                let array = match load_result {
                    Ok(arr) => {
                        *entry.result.lock() = Some(Ok(arr.clone()));
                        entry.notify.notify_waiters();
                        arr
                    }
                    Err(e) => {
                        *entry.result.lock() = Some(Err(e.to_string()));
                        entry.notify.notify_waiters();
                        self.inner.write().inflight.remove(&key);
                        return Err(e);
                    }
                };

                let byte_size = array.get_array_memory_size();

                // Combine cache insertion + inflight removal
                // under a single lock acquisition to avoid a window where
                // the entry is in the cache but still marked as inflight.
                {
                    let mut inner = self.inner.write();
                    let new_hash = key_hash(&key);
                    let mut sketch = self.sketch.lock();
                    sketch.increment(new_hash);

                    // TinyLFU admission gate: only admit the newcomer if it
                    // is at least as popular as the LRU victim it would
                    // evict. This prevents sequential scan patterns from
                    // thrashing the cache.
                    let admit = if byte_size > self.max_bytes {
                        false // single item exceeds entire budget
                    } else if self.current_bytes.load(Ordering::Relaxed) as usize + byte_size
                        > self.max_bytes
                    {
                        // Cache is full — compare newcomer freq vs victim freq
                        if let Some(ref victim_key) = inner.lru_tail {
                            let victim_freq = sketch.estimate(key_hash(victim_key));
                            let new_freq = sketch.estimate(new_hash);
                            new_freq >= victim_freq
                        } else {
                            true // empty cache, always admit
                        }
                    } else {
                        true // cache has room, always admit
                    };
                    drop(sketch);

                    if admit {
                        self.evict_to_fit_locked(&mut inner, byte_size);
                        inner.entries.insert(
                            key.clone(),
                            CacheEntry {
                                array: array.clone(),
                                byte_size,
                                prev: None,
                                next: None,
                            },
                        );
                        Self::push_head(&mut inner, &key);
                        self.current_bytes
                            .fetch_add(byte_size as u64, Ordering::Relaxed);
                    }
                    inner.inflight.remove(&key);
                }

                Ok(array)
            }
        }
    }

    /// Wait for an in-flight load to complete and return its result.
    ///
    /// Uses [`tokio::sync::Notify`] so the caller yields the tokio worker
    /// thread rather than blocking it with a `Condvar::wait()`.
    async fn wait_inflight(
        &self,
        entry: &Arc<InflightEntry>,
    ) -> std::result::Result<ArrayRef, CacheLoadError> {
        loop {
            // Register the `Notified` future BEFORE checking
            // the result. `Notify::notify_waiters()` is not sticky — if we
            // check-then-register, the notification can fire in between
            // and the waiter hangs forever.
            let notified = entry.notify.notified();

            // Now check if the result is already available.
            if let Some(ref result) = *entry.result.lock() {
                return match result {
                    Ok(arr) => Ok(arr.clone()),
                    Err(msg) => Err(CacheLoadError(msg.clone())),
                };
            }
            // Yield to tokio runtime until notified — safe because we
            // registered the future before the result check.
            notified.await;
        }
    }

    /// Evict LRU entries until there's room for `needed_bytes`.
    /// Must be called with `inner` already locked.
    fn evict_to_fit_locked(&self, inner: &mut CacheInner, needed_bytes: usize) {
        let target = self.max_bytes.saturating_sub(needed_bytes);

        loop {
            #[allow(clippy::cast_possible_truncation)]
            let current = self.current_bytes.load(Ordering::Relaxed) as usize;
            if current <= target {
                break;
            }

            // Evict from the tail (least-recently-used) — O(1)
            let tail_key = match inner.lru_tail.clone() {
                Some(k) => k,
                None => break,
            };
            Self::detach(inner, &tail_key);
            if let Some(removed) = inner.entries.remove(&tail_key) {
                self.current_bytes
                    .fetch_sub(removed.byte_size as u64, Ordering::Relaxed);
            }
        }
    }

    /// Invalidate all entries for a given segment.
    pub fn invalidate_segment(&self, segment_id: SegmentId) {
        let mut inner = self.inner.write();
        let keys_to_remove: Vec<SegmentCacheKey> = inner
            .entries
            .keys()
            .filter(|k| k.segment_id == segment_id)
            .cloned()
            .collect();

        for key in keys_to_remove {
            Self::detach(&mut inner, &key);
            if let Some(removed) = inner.entries.remove(&key) {
                self.current_bytes
                    .fetch_sub(removed.byte_size as u64, Ordering::Relaxed);
            }
        }
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.entries.clear();
        inner.lru_head = None;
        inner.lru_tail = None;
        self.current_bytes.store(0, Ordering::Relaxed);
    }

    /// Returns the current cache size in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Returns the number of cached entries.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.inner.read().entries.len()
    }

    /// Returns the total cache hits.
    #[must_use]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Returns the total cache misses.
    #[must_use]
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Returns the maximum cache size in bytes.
    #[must_use]
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
}

impl std::fmt::Debug for SegmentCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentCache")
            .field("max_bytes", &self.max_bytes)
            .field("current_bytes", &self.current_bytes.load(Ordering::Relaxed))
            .field("entries", &self.inner.read().entries.len())
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use std::sync::Arc;

    fn make_key(seg_id: u64, rg: u32, col: &str) -> SegmentCacheKey {
        SegmentCacheKey {
            segment_id: SegmentId(seg_id),
            row_group: rg,
            column: Arc::from(col),
        }
    }

    fn make_array(size: usize) -> ArrayRef {
        let values: Vec<i64> = (0..size as i64).collect();
        Arc::new(Int64Array::from(values))
    }

    #[tokio::test]
    async fn cache_hit_returns_same_data() {
        let cache = SegmentCache::new(1024 * 1024);
        let key = make_key(1, 0, chronix_core::TIME_COLUMN);
        let arr = make_array(100);

        let loaded = cache
            .get_or_load(key.clone(), || Ok::<_, String>(arr.clone()))
            .await
            .unwrap();
        assert_eq!(loaded.len(), 100);
        assert_eq!(cache.misses(), 1);

        // Second access should be a hit
        let hit = cache
            .get_or_load(key, || Ok::<_, String>(make_array(999)))
            .await
            .unwrap();
        assert_eq!(hit.len(), 100); // Same data, not the 999-element one
        assert_eq!(cache.hits(), 1);
    }

    /// The byte budget is a hard bound, not a target. Admission, eviction and
    /// the `current_bytes` update all happen under one write lock, so the
    /// invariant must hold after every insert — including concurrent ones.
    #[tokio::test]
    async fn never_exceeds_byte_budget() {
        let entry_size = make_array(64).get_array_memory_size();
        let max_bytes = entry_size * 4;
        let cache = SegmentCache::new(max_bytes);

        for i in 0..200u64 {
            let arr = make_array(64);
            cache
                .get_or_load(make_key(i, 0, "v"), || Ok::<_, String>(arr))
                .await
                .unwrap();
            assert!(
                cache.size_bytes() as usize <= max_bytes,
                "cache overshot its budget after insert {i}: {} > {max_bytes}",
                cache.size_bytes()
            );
        }
    }

    /// Concurrent loads must not let the accounting drift past the budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_loads_respect_byte_budget() {
        let entry_size = make_array(64).get_array_memory_size();
        let max_bytes = entry_size * 8;
        let cache = Arc::new(SegmentCache::new(max_bytes));

        let mut handles = Vec::new();
        for t in 0..8u64 {
            let cache = Arc::clone(&cache);
            handles.push(tokio::spawn(async move {
                for i in 0..50u64 {
                    let arr = make_array(64);
                    let _ = cache
                        .get_or_load(make_key(t * 1000 + i, 0, "v"), || Ok::<_, String>(arr))
                        .await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert!(
            cache.size_bytes() as usize <= max_bytes,
            "cache overshot its budget under concurrency: {} > {max_bytes}",
            cache.size_bytes()
        );
    }

    #[tokio::test]
    async fn eviction_removes_lru_entry() {
        // Small cache: fits ~2 small arrays
        let small_arr_size = make_array(10).get_array_memory_size();
        let cache = SegmentCache::new(small_arr_size * 2 + 10);

        cache
            .get_or_load(make_key(1, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        cache
            .get_or_load(make_key(2, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        assert_eq!(cache.entry_count(), 2);

        // Adding a third should evict the LRU (key 1)
        cache
            .get_or_load(make_key(3, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        assert!(cache.entry_count() <= 2);
    }

    #[tokio::test]
    async fn invalidate_segment_clears_matching() {
        let cache = SegmentCache::new(1024 * 1024);

        cache
            .get_or_load(make_key(1, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        cache
            .get_or_load(make_key(1, 1, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        cache
            .get_or_load(make_key(2, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        assert_eq!(cache.entry_count(), 3);

        cache.invalidate_segment(SegmentId(1));
        assert_eq!(cache.entry_count(), 1);
    }

    #[tokio::test]
    async fn clear_empties_cache() {
        let cache = SegmentCache::new(1024 * 1024);
        cache
            .get_or_load(make_key(1, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        cache.clear();
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(cache.size_bytes(), 0);
    }

    #[tokio::test]
    async fn lru_order_evicts_oldest() {
        // Cache fits exactly 2 entries
        let arr_size = make_array(10).get_array_memory_size();
        let cache = SegmentCache::new(arr_size * 2 + 10);

        // Insert keys 1, 2
        cache
            .get_or_load(make_key(1, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        cache
            .get_or_load(make_key(2, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();

        // Access key 1 to promote it (now LRU order: 2 is tail)
        cache
            .get_or_load(make_key(1, 0, "ts"), || Ok::<_, String>(make_array(999)))
            .await
            .unwrap();
        assert_eq!(cache.hits(), 1); // key 1 was a hit

        // Insert key 3 — should evict key 2 (LRU tail), not key 1
        cache
            .get_or_load(make_key(3, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();

        // Key 1 should still be cached
        let r = cache
            .get_or_load(make_key(1, 0, "ts"), || Ok::<_, String>(make_array(999)))
            .await
            .unwrap();
        assert_eq!(cache.hits(), 2);
        assert_eq!(r.len(), 10); // Original data, not the 999-element reload

        // Key 2 should have been evicted
        let r2 = cache
            .get_or_load(make_key(2, 0, "ts"), || Ok::<_, String>(make_array(5)))
            .await
            .unwrap();
        assert_eq!(r2.len(), 5); // Reloaded
        assert_eq!(cache.misses(), 4); // keys 1,2,3 initial + key 2 re-miss
    }

    #[tokio::test]
    async fn singleflight_dedup() {
        use std::sync::atomic::AtomicUsize;

        let cache = Arc::new(SegmentCache::new(1024 * 1024));
        let load_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let load_count = load_count.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_load(make_key(42, 0, "val"), || {
                        load_count.fetch_add(1, Ordering::SeqCst);
                        // Simulate slow load
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        Ok::<_, String>(make_array(100))
                    })
                    .await
                    .unwrap()
            }));
        }

        for h in handles {
            let arr = h.await.unwrap();
            assert_eq!(arr.len(), 100);
        }

        // Only one task should have called the loader
        assert_eq!(load_count.load(Ordering::SeqCst), 1);
        assert_eq!(cache.entry_count(), 1);
    }

    #[tokio::test]
    async fn tinylfu_rejects_cold_scan() {
        // Cache fits exactly 2 entries. Pre-heat keys A and B, then
        // scan through many cold keys. TinyLFU should reject the cold
        // newcomers because A and B have higher frequency.
        let arr_size = make_array(10).get_array_memory_size();
        let cache = SegmentCache::new(arr_size * 2 + 10);

        let key_a = make_key(1, 0, "ts");
        let key_b = make_key(2, 0, "ts");

        // Insert and access A and B multiple times to build frequency
        for _ in 0..5 {
            cache
                .get_or_load(key_a.clone(), || Ok::<_, String>(make_array(10)))
                .await
                .unwrap();
            cache
                .get_or_load(key_b.clone(), || Ok::<_, String>(make_array(10)))
                .await
                .unwrap();
        }

        // Now scan through 20 cold keys (each accessed only once)
        for i in 100..120 {
            cache
                .get_or_load(make_key(i, 0, "ts"), || Ok::<_, String>(make_array(10)))
                .await
                .unwrap();
        }

        // Hot keys A and B should still be in cache — TinyLFU admission
        // gate should have rejected the cold scan keys.
        let r_a = cache
            .get_or_load(key_a.clone(), || Ok::<_, String>(make_array(999)))
            .await
            .unwrap();
        assert_eq!(r_a.len(), 10, "hot key A should still be cached");

        let r_b = cache
            .get_or_load(key_b.clone(), || Ok::<_, String>(make_array(999)))
            .await
            .unwrap();
        assert_eq!(r_b.len(), 10, "hot key B should still be cached");
    }

    #[tokio::test]
    async fn tinylfu_admits_hot_newcomer() {
        // TinyLFU should admit a newcomer that is hotter than the victim.
        let arr_size = make_array(10).get_array_memory_size();
        let cache = SegmentCache::new(arr_size * 2 + 10);

        // Fill cache with cold keys (accessed once each)
        cache
            .get_or_load(make_key(1, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        cache
            .get_or_load(make_key(2, 0, "ts"), || Ok::<_, String>(make_array(10)))
            .await
            .unwrap();
        assert_eq!(cache.entry_count(), 2);

        // Build frequency for a new key by accessing it several times
        // (it will miss each time, load, get rejected, but sketch still
        // records the frequency).
        let hot_key = make_key(99, 0, "ts");
        for _ in 0..5 {
            cache
                .get_or_load(hot_key.clone(), || Ok::<_, String>(make_array(10)))
                .await
                .unwrap();
        }

        // The hot key should now be cached (admitted after building freq)
        let inner = cache.inner.read();
        assert!(
            inner.entries.contains_key(&hot_key),
            "hot newcomer should have been admitted"
        );
    }

    #[tokio::test]
    async fn sketch_reset_prevents_count_bloat() {
        let mut sketch = FrequencySketch::new();
        let reset_interval = sketch.reset_interval;

        // Increment the same key many times — should trigger periodic reset
        let key_hash = 42u64;
        for _ in 0..(reset_interval * 3) {
            sketch.increment(key_hash);
        }

        // After multiple resets, the estimate should be bounded (not
        // accumulated to SKETCH_RESET_INTERVAL * 3).
        let est = sketch.estimate(key_hash);
        assert!(
            est < 25000,
            "sketch estimate should be bounded after resets, got {est}"
        );
    }
}
