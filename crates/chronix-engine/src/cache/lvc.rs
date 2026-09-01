//! Last Value Cache (LVC) — O(1) lookup of the most recent point per series.
//!
//! The LVC stores the most recent `Point` for each series key, keyed by
//! the full `SeriesKey` (measurement + tags). It is populated on every
//! `insert()` and provides sub-microsecond lookups for dashboard-style
//! "current value" queries.
//!
//! Thread-safe via `DashMap` — lock-free concurrent reads and sharded writes.
//!
//! # LRU Eviction
//!
//! When `max_entries > 0`, the cache uses a `HashMap<SeriesKey, usize>` +
//! doubly-linked intrusive list for **O(1)** LRU eviction.  Previous
//! implementation used `VecDeque::retain()` which was O(N) per update —
//! a critical bottleneck at >100K series.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use chronix_core::{Point, SeriesKey};

// ── O(1) LRU order tracker ──────────────────────────────────────────

/// Doubly-linked list node for O(1) LRU tracking.
///
/// Uses `Arc<SeriesKey>` so the same allocation is shared
/// between the node and the lookup `HashMap`, avoiding a per-touch clone.
struct LruNode {
    key: Arc<SeriesKey>,
    prev: Option<usize>,
    next: Option<usize>,
}

/// O(1) LRU order tracker backed by a HashMap + doubly-linked list.
///
/// All operations (`touch`, `push_back`, `pop_front`, `remove`) are O(1).
/// Uses a slab-style `Vec<LruNode>` with free-list recycling.
struct LruOrder {
    /// Slab of linked-list nodes (indexed by handle).
    nodes: Vec<LruNode>,
    /// Map from SeriesKey → node index for O(1) lookup.
    /// `Arc<SeriesKey>` avoids double-cloning.
    map: HashMap<Arc<SeriesKey>, usize>,
    /// Free indices for recycling removed nodes.
    free: Vec<usize>,
    /// Head of the doubly-linked list (least recently used).
    head: Option<usize>,
    /// Tail of the doubly-linked list (most recently used).
    tail: Option<usize>,
}

impl LruOrder {
    /// Number of entries tracked (test-only observability).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }

    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            map: HashMap::new(),
            free: Vec::new(),
            head: None,
            tail: None,
        }
    }

    fn with_capacity(cap: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(cap),
            map: HashMap::with_capacity(cap),
            free: Vec::new(),
            head: None,
            tail: None,
        }
    }

    /// Allocate or recycle a node index.
    fn alloc(&mut self, key: Arc<SeriesKey>) -> usize {
        if let Some(idx) = self.free.pop() {
            self.nodes[idx] = LruNode {
                key,
                prev: None,
                next: None,
            };
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(LruNode {
                key,
                prev: None,
                next: None,
            });
            idx
        }
    }

    /// Unlink a node from the doubly-linked list (does NOT free it).
    fn unlink(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        match prev {
            Some(p) => self.nodes[p].next = next,
            None => self.head = next,
        }
        match next {
            Some(n) => self.nodes[n].prev = prev,
            None => self.tail = prev,
        }

        self.nodes[idx].prev = None;
        self.nodes[idx].next = None;
    }

    /// Append a node index to the tail (most recently used).
    fn link_tail(&mut self, idx: usize) {
        self.nodes[idx].prev = self.tail;
        self.nodes[idx].next = None;

        if let Some(old_tail) = self.tail {
            self.nodes[old_tail].next = Some(idx);
        }
        self.tail = Some(idx);
        if self.head.is_none() {
            self.head = Some(idx);
        }
    }

    /// Insert or move `key` to the most-recently-used position. O(1).
    fn touch(&mut self, key: &SeriesKey) {
        if let Some(&idx) = self.map.get(key) {
            // Already present — unlink and re-link at tail.
            self.unlink(idx);
            self.link_tail(idx);
        } else {
            // New entry — single Arc allocation shared between node + map.
            let arc = Arc::new(key.clone());
            let idx = self.alloc(Arc::clone(&arc));
            self.map.insert(arc, idx);
            self.link_tail(idx);
        }
    }

    /// Remove and return the least-recently-used key. O(1).
    fn pop_front(&mut self) -> Option<Arc<SeriesKey>> {
        let idx = self.head?;
        self.unlink(idx);
        let key = self.nodes[idx].key.clone();
        self.map.remove(&key);
        self.free.push(idx);
        Some(key)
    }

    /// Remove a specific key from the tracker. O(1).
    fn remove(&mut self, key: &SeriesKey) {
        if let Some(idx) = self.map.remove(key) {
            self.unlink(idx);
            self.free.push(idx);
        }
    }

    /// Remove all entries whose key is NOT in the DashMap cache.
    fn retain_present(&mut self, cache: &DashMap<SeriesKey, Point>) {
        let to_remove: Vec<Arc<SeriesKey>> = self
            .map
            .keys()
            .filter(|k| !cache.contains_key(k.as_ref()))
            .cloned()
            .collect();
        for key in to_remove {
            self.remove(&key);
        }
    }

    /// Clear everything.
    fn clear(&mut self) {
        self.nodes.clear();
        self.map.clear();
        self.free.clear();
        self.head = None;
        self.tail = None;
    }
}

// ── LastValueCache ──────────────────────────────────────────────────

/// Last Value Cache — stores the most recent point per series.
///
/// # Thread Safety
///
/// Uses `DashMap` for lock-free concurrent reads and sharded writes.
/// Multiple threads can read and update the cache simultaneously.
///
/// # Memory
///
/// Memory usage scales linearly with the number of unique series.
/// Each entry stores one `Point` (typically 100-500 bytes).
///
/// # LRU Eviction
///
/// An optional `max_entries` cap prevents unbounded memory growth in
/// high-cardinality workloads.  When full, the least recently used
/// entries are evicted in **O(1)** time using a HashMap-backed
/// doubly-linked list (replaces an O(N)
/// `VecDeque::retain` approach).
pub struct LastValueCache {
    /// Map from `SeriesKey` → most recent `Point`.
    cache: DashMap<SeriesKey, Point>,
    /// Total number of updates applied.
    update_count: AtomicUsize,
    /// Maximum number of entries allowed (0 = unlimited).
    max_entries: usize,
    /// O(1) LRU order tracker. Only actively maintained when `max_entries > 0`.
    order: Mutex<LruOrder>,
}

impl LastValueCache {
    /// Create a new, empty LVC with no entry limit.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
            update_count: AtomicUsize::new(0),
            max_entries: 0,
            order: Mutex::new(LruOrder::new()),
        }
    }

    /// Create an LVC with a maximum number of entries.
    ///
    /// When full, the least recently used (LRU) entry is evicted to
    /// make room for the new one.  Eviction is O(1).
    #[must_use]
    pub fn with_max_entries(max: usize) -> Self {
        Self {
            cache: DashMap::with_capacity(max.min(1024)),
            update_count: AtomicUsize::new(0),
            max_entries: max,
            order: Mutex::new(LruOrder::with_capacity(max.min(1024))),
        }
    }

    /// Create an LVC with pre-allocated capacity for `n` series.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            cache: DashMap::with_capacity(n),
            update_count: AtomicUsize::new(0),
            max_entries: 0,
            order: Mutex::new(LruOrder::new()),
        }
    }

    /// Update the cache with a new point. Only overwrites if the new
    /// point has a timestamp ≥ the existing cached point.
    ///
    /// When `max_entries > 0`, the key is moved to the most-recently-used
    /// position in O(1) time.  If the cache is full and the key is new,
    /// the least-recently-used entry is evicted — also O(1).
    pub fn update(&self, point: &Point) {
        let key = point.series_key().clone();

        // Enforce entry cap with O(1) LRU eviction.
        if self.max_entries > 0 {
            let mut order = self.order.lock();
            let is_new = !self.cache.contains_key(&key);

            // Move existing key to tail (most recently used) — O(1).
            // For new keys, this inserts at tail — also O(1).
            order.touch(&key);

            // Evict LRU entries (from the head) while over capacity — O(1) each.
            while is_new && self.cache.len() >= self.max_entries {
                if let Some(evict_key) = order.pop_front() {
                    self.cache.remove(&evict_key);
                } else {
                    break;
                }
            }
        }

        self.cache
            .entry(key)
            .and_modify(|existing| {
                if point.timestamp() >= existing.timestamp() {
                    *existing = point.clone();
                }
            })
            .or_insert_with(|| point.clone());

        self.update_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Look up the most recent point for a series.
    ///
    /// Returns `None` if the series has never been written to, or has
    /// been evicted.
    #[must_use]
    pub fn get(&self, measurement: &str, tags: &BTreeMap<String, String>) -> Option<Point> {
        let key = SeriesKey::new(measurement, tags.clone()).ok()?;
        self.cache.get(&key).map(|entry| entry.value().clone())
    }

    /// Look up by `SeriesKey`.
    #[must_use]
    pub fn get_by_key(&self, key: &SeriesKey) -> Option<Point> {
        self.cache.get(key).map(|entry| entry.value().clone())
    }

    /// Remove a series from the cache.
    pub fn evict(&self, measurement: &str, tags: &BTreeMap<String, String>) {
        if let Ok(key) = SeriesKey::new(measurement, tags.clone()) {
            self.cache.remove(&key);
            if self.max_entries > 0 {
                self.order.lock().remove(&key);
            }
        }
    }

    /// Remove a series by its `SeriesKey`.
    pub fn evict_by_key(&self, key: &SeriesKey) {
        self.cache.remove(key);
        if self.max_entries > 0 {
            self.order.lock().remove(key);
        }
    }

    /// Remove all entries for a measurement.
    pub fn evict_measurement(&self, measurement: &str) {
        self.cache
            .retain(|key, _point| key.measurement() != measurement);
        if self.max_entries > 0 {
            self.order.lock().retain_present(&self.cache);
        }
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.cache.clear();
        if self.max_entries > 0 {
            self.order.lock().clear();
        }
    }

    /// Returns the number of cached series.
    #[must_use]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns `true` if the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Returns the total number of updates applied.
    #[must_use]
    pub fn update_count(&self) -> usize {
        self.update_count.load(Ordering::Relaxed)
    }
}

impl Default for LastValueCache {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for LastValueCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LastValueCache")
            .field("entries", &self.cache.len())
            .field("updates", &self.update_count.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronix_core::FieldValue;

    fn make_point(host: &str, value: f64, ts: i64) -> Point {
        let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
        let fields = BTreeMap::from([("cpu".to_string(), FieldValue::F64(value))]);
        let key = SeriesKey::new("cpu", tags).unwrap();
        Point::new(key, fields, ts).unwrap()
    }

    #[test]
    fn update_and_get() {
        let lvc = LastValueCache::new();
        let p = make_point("srv1", 42.0, 1000);
        lvc.update(&p);

        let tags = BTreeMap::from([("host".to_string(), "srv1".to_string())]);
        let result = lvc.get("cpu", &tags);
        assert!(result.is_some());
        assert_eq!(result.unwrap().timestamp(), 1000);
    }

    #[test]
    fn newer_overwrites_older() {
        let lvc = LastValueCache::new();
        lvc.update(&make_point("srv1", 10.0, 100));
        lvc.update(&make_point("srv1", 20.0, 200));

        let tags = BTreeMap::from([("host".to_string(), "srv1".to_string())]);
        let result = lvc.get("cpu", &tags).unwrap();
        assert_eq!(result.timestamp(), 200);
    }

    #[test]
    fn older_does_not_overwrite() {
        let lvc = LastValueCache::new();
        lvc.update(&make_point("srv1", 20.0, 200));
        lvc.update(&make_point("srv1", 10.0, 100));

        let tags = BTreeMap::from([("host".to_string(), "srv1".to_string())]);
        let result = lvc.get("cpu", &tags).unwrap();
        assert_eq!(result.timestamp(), 200);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let lvc = LastValueCache::new();
        let tags = BTreeMap::from([("host".to_string(), "ghost".to_string())]);
        assert!(lvc.get("cpu", &tags).is_none());
    }

    #[test]
    fn evict_removes_entry() {
        let lvc = LastValueCache::new();
        lvc.update(&make_point("srv1", 1.0, 100));
        assert_eq!(lvc.len(), 1);

        let tags = BTreeMap::from([("host".to_string(), "srv1".to_string())]);
        lvc.evict("cpu", &tags);
        assert!(lvc.get("cpu", &tags).is_none());
        assert_eq!(lvc.len(), 0);
    }

    #[test]
    fn evict_measurement_removes_all_series() {
        let lvc = LastValueCache::new();
        lvc.update(&make_point("srv1", 1.0, 100));
        lvc.update(&make_point("srv2", 2.0, 100));
        assert_eq!(lvc.len(), 2);

        lvc.evict_measurement("cpu");
        assert_eq!(lvc.len(), 0);
    }

    #[test]
    fn clear_empties_cache() {
        let lvc = LastValueCache::new();
        lvc.update(&make_point("srv1", 1.0, 100));
        lvc.update(&make_point("srv2", 2.0, 100));
        lvc.clear();
        assert!(lvc.is_empty());
    }

    #[test]
    fn update_count_tracks_operations() {
        let lvc = LastValueCache::new();
        assert_eq!(lvc.update_count(), 0);
        lvc.update(&make_point("srv1", 1.0, 100));
        lvc.update(&make_point("srv1", 2.0, 200));
        assert_eq!(lvc.update_count(), 2);
    }

    #[test]
    fn multiple_series_independent() {
        let lvc = LastValueCache::new();
        lvc.update(&make_point("srv1", 1.0, 100));
        lvc.update(&make_point("srv2", 2.0, 200));

        let tags1 = BTreeMap::from([("host".to_string(), "srv1".to_string())]);
        let tags2 = BTreeMap::from([("host".to_string(), "srv2".to_string())]);

        assert_eq!(lvc.get("cpu", &tags1).unwrap().timestamp(), 100);
        assert_eq!(lvc.get("cpu", &tags2).unwrap().timestamp(), 200);
    }

    #[test]
    fn get_by_key() {
        let lvc = LastValueCache::new();
        let p = make_point("srv1", 42.0, 1000);
        let key = p.series_key().clone();
        lvc.update(&p);

        assert!(lvc.get_by_key(&key).is_some());
        let ghost_key = SeriesKey::new(
            "cpu",
            BTreeMap::from([("host".to_string(), "ghost".to_string())]),
        )
        .unwrap();
        assert!(lvc.get_by_key(&ghost_key).is_none());
    }

    // ── LRU O(1) eviction tests ──────────────────────────────

    #[test]
    fn lru_eviction_respects_max_entries() {
        let lvc = LastValueCache::with_max_entries(2);
        lvc.update(&make_point("srv1", 1.0, 100));
        lvc.update(&make_point("srv2", 2.0, 200));
        assert_eq!(lvc.len(), 2);

        // Adding a 3rd entry should evict srv1 (LRU).
        lvc.update(&make_point("srv3", 3.0, 300));
        assert_eq!(lvc.len(), 2);

        let tags1 = BTreeMap::from([("host".to_string(), "srv1".to_string())]);
        let tags3 = BTreeMap::from([("host".to_string(), "srv3".to_string())]);
        assert!(lvc.get("cpu", &tags1).is_none(), "srv1 should be evicted");
        assert!(lvc.get("cpu", &tags3).is_some(), "srv3 should be present");
    }

    #[test]
    fn lru_touch_moves_to_back() {
        let lvc = LastValueCache::with_max_entries(2);
        lvc.update(&make_point("srv1", 1.0, 100));
        lvc.update(&make_point("srv2", 2.0, 200));

        // Touch srv1 again — it should now be MRU, srv2 is LRU.
        lvc.update(&make_point("srv1", 1.5, 300));

        // Adding srv3 should evict srv2 (now LRU), not srv1.
        lvc.update(&make_point("srv3", 3.0, 400));

        let tags1 = BTreeMap::from([("host".to_string(), "srv1".to_string())]);
        let tags2 = BTreeMap::from([("host".to_string(), "srv2".to_string())]);
        let tags3 = BTreeMap::from([("host".to_string(), "srv3".to_string())]);
        assert!(
            lvc.get("cpu", &tags1).is_some(),
            "srv1 was touched, should survive"
        );
        assert!(
            lvc.get("cpu", &tags2).is_none(),
            "srv2 should be evicted (LRU)"
        );
        assert!(lvc.get("cpu", &tags3).is_some(), "srv3 should be present");
    }

    #[test]
    fn lru_evict_by_key_updates_order() {
        let lvc = LastValueCache::with_max_entries(3);
        lvc.update(&make_point("srv1", 1.0, 100));
        lvc.update(&make_point("srv2", 2.0, 200));
        lvc.update(&make_point("srv3", 3.0, 300));

        // Explicitly evict srv2. Order should have only srv1, srv3.
        let key2 = make_point("srv2", 0.0, 0).series_key().clone();
        lvc.evict_by_key(&key2);
        assert_eq!(lvc.len(), 2);
        assert_eq!(lvc.order.lock().len(), 2);
    }

    #[test]
    fn lru_order_internal_consistency() {
        let lvc = LastValueCache::with_max_entries(100);
        // Insert 50 entries, update 25 of them, evict 10.
        for i in 0..50 {
            lvc.update(&make_point(&format!("s{i}"), i as f64, i as i64));
        }
        assert_eq!(lvc.len(), 50);
        assert_eq!(lvc.order.lock().len(), 50);

        // Touch first 25 again.
        for i in 0..25 {
            lvc.update(&make_point(&format!("s{i}"), i as f64, 1000 + i as i64));
        }
        assert_eq!(lvc.len(), 50);
        assert_eq!(lvc.order.lock().len(), 50);

        // Evict by measurement clears everything.
        lvc.evict_measurement("cpu");
        assert_eq!(lvc.len(), 0);
        assert_eq!(lvc.order.lock().len(), 0);
    }
}
