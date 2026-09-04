//! Thread-safe string interner for memtable data deduplication.
//!
//! Time-series workloads insert millions of points that share identically
//! named measurements, tag keys, and tag values.  Without interning every
//! insert allocates a fresh `String` on the heap, wasting both memory and
//! allocator bandwidth.
//!
//! [`StringInterner`] maps each unique string to a single `Arc<str>`.
//! Subsequent inserts that encounter the same byte sequence receive a
//! cheap `Arc::clone` instead of a new allocation.

use std::sync::Arc;

use dashmap::DashMap;

/// A thread-safe interner that deduplicates strings into `Arc<str>`.
///
/// Internally backed by a [`DashMap`] which provides lock-free concurrent
/// reads with per-shard locking for writes, eliminating the global
/// `RwLock` contention bottleneck.
///
/// # Example
///
/// ```no_run
/// use chronix_engine::memtable::interner::StringInterner;
///
/// let interner = StringInterner::new();
/// let a = interner.intern("cpu");
/// let b = interner.intern("cpu");
/// assert!(std::sync::Arc::ptr_eq(&a, &b));
/// ```
#[derive(Debug)]
pub struct StringInterner {
    map: DashMap<Arc<str>, ()>,
}

impl StringInterner {
    /// Create an empty interner.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    /// Intern a string, returning a shared `Arc<str>`.
    ///
    /// If the string has been seen before, the existing `Arc` is cloned
    /// (lock-free read via `DashMap` shard).  Otherwise, a new `Arc<str>`
    /// is allocated and inserted (per-shard write lock only).
    pub fn intern(&self, s: &str) -> Arc<str> {
        // Fast path: already interned — only acquires a read guard on
        // the relevant shard.
        if let Some(entry) = self.map.get(s) {
            return Arc::clone(entry.key());
        }

        // Slow path: allocate and insert.  `entry` API ensures
        // at-most-once insertion without a separate double-check.
        // Always return the *map's* key (not our local Arc) to
        // guarantee pointer equality across concurrent callers.
        let arc: Arc<str> = Arc::from(s);
        let entry = self.map.entry(arc).or_insert(());
        Arc::clone(entry.key())
    }

    /// Look up a previously interned string without inserting.
    ///
    /// Returns `Some(Arc<str>)` if the string was already interned,
    /// `None` otherwise.
    #[must_use]
    pub fn lookup(&self, s: &str) -> Option<Arc<str>> {
        self.map.get(s).map(|entry| Arc::clone(entry.key()))
    }

    /// Number of unique strings interned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if no strings have been interned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Approximate heap bytes held by the interner.
    ///
    /// The interned strings themselves plus the map's per-entry overhead: an
    /// `Arc<str>` header (two `usize` counts), the fat pointer the map stores,
    /// and a hash slot. Interned strings are tag keys, tag values and
    /// measurement names, so on a high-cardinality workload this is the term
    /// that grows, and on the gateway it is one of the three that the memtable
    /// budget does not count.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();
        const SLOT: usize = std::mem::size_of::<(usize, usize)>() + std::mem::size_of::<usize>();

        self.map
            .iter()
            .map(|e| e.key().len() + ARC_HEADER + SLOT)
            .sum()
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_deduplicates() {
        let interner = StringInterner::new();
        let a = interner.intern("cpu");
        let b = interner.intern("cpu");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn intern_distinct_strings() {
        let interner = StringInterner::new();
        let a = interner.intern("cpu");
        let b = interner.intern("mem");
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn intern_empty_string() {
        let interner = StringInterner::new();
        let a = interner.intern("");
        let b = interner.intern("");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(&*a, "");
    }

    #[test]
    fn intern_concurrent() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let interner = StdArc::new(StringInterner::new());
        let mut handles = vec![];

        for _ in 0..8 {
            let interner = StdArc::clone(&interner);
            handles.push(thread::spawn(move || {
                let mut arcs = vec![];
                for _ in 0..1_000 {
                    arcs.push(interner.intern("host"));
                    arcs.push(interner.intern("dc"));
                    arcs.push(interner.intern("region"));
                }
                arcs
            }));
        }

        let mut all_arcs: Vec<Arc<str>> = vec![];
        for h in handles {
            all_arcs.extend(h.join().expect("thread panicked"));
        }

        assert_eq!(interner.len(), 3);
        // All "host" arcs point to the same allocation.
        let first_host = interner.intern("host");
        for arc in &all_arcs {
            if &**arc == "host" {
                assert!(Arc::ptr_eq(arc, &first_host));
            }
        }
    }
}
