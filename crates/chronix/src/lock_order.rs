//! Compile-time lock ordering enforcement.
//!
//! Wraps [`parking_lot::RwLock`] with a `const LEVEL: usize` parameter
//! that encodes the position in the total lock order.  In **debug builds**
//! a thread-local tracks the highest lock level currently held and panics
//! on out-of-order acquisition.  In **release builds** the wrapper is
//! zero-cost — all checks compile away.
//!
//! # Lock levels
//!
//! | Level | Field              | Type                                     |
//! |-------|--------------------|------------------------------------------|
//! | 1     | `catalog`          | `RwLock<SegmentCatalog>`                 |
//! | 2     | `time_index`       | `RwLock<BTreeMap<ShardId, TimeIndex>>`    |
//! | 3     | `blooms`           | `RwLock<BTreeMap<u64, SeriesBloomFilter>>`|
//! | 4     | `tombstones`       | `RwLock<TombstoneSet>`                   |
//! | 5     | `rollup_registry`  | `RwLock<RollupRegistry>`                 |

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Lock level constants — must match the documented total order.
pub mod level {
    /// Level 1: segment catalog.
    pub const CATALOG: usize = 1;
    /// Level 2: per-shard time indices.
    pub const TIME_INDEX: usize = 2;
    /// Level 3: per-segment bloom filters.
    pub const BLOOMS: usize = 3;
    /// Level 4: tombstoned series set.
    pub const TOMBSTONES: usize = 4;
    /// Level 5: rollup definitions.
    pub const ROLLUP_REGISTRY: usize = 5;
}

// ── Thread-local ordering tracker (debug builds only) ───────────────

#[cfg(debug_assertions)]
std::thread_local! {
    /// Stack of currently held lock levels on this thread.
    /// We use a `Vec` rather than a single max because a thread can
    /// acquire multiple locks and must pop them in LIFO order.
    static HELD_LEVELS: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(debug_assertions)]
fn push_level(level: usize) {
    HELD_LEVELS.with(|h| {
        let mut stack = h.borrow_mut();
        if let Some(&top) = stack.last() {
            assert!(
                level > top,
                "lock ordering violation — attempted to acquire level {level} \
                 while holding level {top}. Required order: catalog(1) → \
                 time_index(2) → blooms(3) → tombstones(4) → rollup_registry(5)"
            );
        }
        stack.push(level);
    });
}

#[cfg(debug_assertions)]
fn pop_level(level: usize) {
    HELD_LEVELS.with(|h| {
        let mut stack = h.borrow_mut();
        if let Some(&top) = stack.last() {
            if top == level {
                stack.pop();
            }
            // If top != level, the guard was moved across threads or
            // dropped out of order — we silently skip rather than panic
            // to avoid double-panic on unwind.
        }
    });
}

// ── OrderedRwLock ───────────────────────────────────────────────────

/// A [`RwLock`] annotated with a compile-time lock level.
///
/// In debug builds, acquiring the lock asserts that no higher-numbered
/// lock is already held on the current thread (i.e. locks must be
/// acquired in ascending level order).  In release builds the assertions
/// are compiled away entirely.
pub struct OrderedRwLock<const LEVEL: usize, T: ?Sized> {
    inner: RwLock<T>,
}

impl<const LEVEL: usize, T> OrderedRwLock<LEVEL, T> {
    /// Create a new `OrderedRwLock` wrapping `value`.
    pub fn new(value: T) -> Self {
        Self {
            inner: RwLock::new(value),
        }
    }
}

impl<const LEVEL: usize, T: ?Sized> OrderedRwLock<LEVEL, T> {
    /// Acquire a read lock, asserting correct ordering in debug builds.
    #[inline]
    pub fn read(&self) -> OrderedReadGuard<'_, LEVEL, T> {
        #[cfg(debug_assertions)]
        push_level(LEVEL);
        OrderedReadGuard {
            guard: self.inner.read(),
        }
    }

    /// Acquire a write lock, asserting correct ordering in debug builds.
    #[inline]
    pub fn write(&self) -> OrderedWriteGuard<'_, LEVEL, T> {
        #[cfg(debug_assertions)]
        push_level(LEVEL);
        OrderedWriteGuard {
            guard: self.inner.write(),
        }
    }
}

// ── Guards ──────────────────────────────────────────────────────────

/// Read guard that pops the level on drop (debug builds).
pub struct OrderedReadGuard<'a, const LEVEL: usize, T: ?Sized> {
    guard: RwLockReadGuard<'a, T>,
}

impl<const LEVEL: usize, T: ?Sized> std::ops::Deref for OrderedReadGuard<'_, LEVEL, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.guard
    }
}

#[cfg(debug_assertions)]
impl<const LEVEL: usize, T: ?Sized> Drop for OrderedReadGuard<'_, LEVEL, T> {
    fn drop(&mut self) {
        pop_level(LEVEL);
    }
}

/// Write guard that pops the level on drop (debug builds).
pub struct OrderedWriteGuard<'a, const LEVEL: usize, T: ?Sized> {
    guard: RwLockWriteGuard<'a, T>,
}

impl<const LEVEL: usize, T: ?Sized> std::ops::Deref for OrderedWriteGuard<'_, LEVEL, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<const LEVEL: usize, T: ?Sized> std::ops::DerefMut for OrderedWriteGuard<'_, LEVEL, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

#[cfg(debug_assertions)]
impl<const LEVEL: usize, T: ?Sized> Drop for OrderedWriteGuard<'_, LEVEL, T> {
    fn drop(&mut self) {
        pop_level(LEVEL);
    }
}

// ── Type aliases for the five ordered locks ─────────────────────────

/// Level-1 lock protecting the segment catalog.
pub type CatalogLock<T> = OrderedRwLock<{ level::CATALOG }, T>;
/// Level-2 lock protecting per-shard time indices.
pub type TimeIndexLock<T> = OrderedRwLock<{ level::TIME_INDEX }, T>;
/// Level-3 lock protecting per-segment bloom filters.
pub type BloomsLock<T> = OrderedRwLock<{ level::BLOOMS }, T>;
/// Level-4 lock protecting tombstoned series.
pub type TombstonesLock<T> = OrderedRwLock<{ level::TOMBSTONES }, T>;
/// Level-5 lock protecting rollup definitions.
pub type RollupRegistryLock<T> = OrderedRwLock<{ level::ROLLUP_REGISTRY }, T>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascending_order_allowed() {
        let a = OrderedRwLock::<1, _>::new(1u32);
        let b = OrderedRwLock::<2, _>::new(2u32);
        let ga = a.read();
        let gb = b.read();
        assert_eq!(*ga, 1);
        assert_eq!(*gb, 2);
        drop(gb);
        drop(ga);
    }

    #[test]
    fn single_lock_allowed() {
        let a = OrderedRwLock::<3, _>::new("hello");
        let g = a.write();
        assert_eq!(*g, "hello");
        drop(g);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "lock ordering violation")]
    fn descending_order_panics() {
        let a = OrderedRwLock::<2, _>::new(());
        let b = OrderedRwLock::<1, _>::new(());
        let _ga = a.read();
        let _gb = b.read(); // should panic
    }

    #[test]
    fn write_guard_deref_mut() {
        let a = OrderedRwLock::<1, _>::new(0u32);
        {
            let mut g = a.write();
            *g = 42;
        }
        assert_eq!(*a.read(), 42);
    }
}
