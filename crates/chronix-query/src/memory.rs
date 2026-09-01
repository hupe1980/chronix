//! Per-query memory tracking and budget enforcement.
//!
//! The [`MemoryTracker`] provides a lightweight mechanism to enforce
//! per-query memory limits. Query operators call `try_allocate` before
//! materializing intermediate results; if the cumulative allocation
//! exceeds the configured budget, the query fails with
//! [`QueryError::QueryMemoryExceeded`] rather than causing OOM.
//!
//! # Incremental budget enforcement
//!
//! All query operators (aggregate, dedup, window) perform pre-allocation
//! memory estimates before building results. For grouped aggregation,
//! the budget is checked incrementally as new groups are created.
//! For dedup and window, the estimated output size is checked before
//! materialization. Full spill-to-disk is in the backlog.
//!
//! # Usage
//!
//! ```no_run
//! use chronix_query::memory::MemoryTracker;
//!
//! let tracker = MemoryTracker::new(1024 * 1024); // 1 MiB budget
//! tracker.try_allocate(512 * 1024).unwrap();      // OK — 512 KiB used
//! tracker.try_allocate(256 * 1024).unwrap();      // OK — 768 KiB used
//! assert_eq!(tracker.allocated(), 768 * 1024);
//! tracker.deallocate(512 * 1024);                  // release 512 KiB
//! assert_eq!(tracker.allocated(), 256 * 1024);
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{QueryError, Result};

/// Tracks memory allocations against a per-query budget.
///
/// Thread-safe via atomic operations — can be shared across parallel
/// query operators via `Arc<MemoryTracker>`.
#[derive(Debug)]
pub struct MemoryTracker {
    /// Maximum allowed allocation in bytes.
    budget: usize,
    /// Current allocation in bytes.
    allocated: AtomicUsize,
}

impl MemoryTracker {
    /// Create a new tracker with the given memory budget in bytes.
    #[must_use]
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            budget: budget_bytes,
            allocated: AtomicUsize::new(0),
        }
    }

    /// Try to allocate `bytes` from the budget.
    ///
    /// Returns `Ok(())` if the allocation succeeds, or
    /// [`QueryError::QueryMemoryExceeded`] if the budget would be
    /// exceeded.
    ///
    /// # Errors
    ///
    /// Returns an error if `allocated + bytes > budget`.
    pub fn try_allocate(&self, bytes: usize) -> Result<()> {
        // CAS loop to atomically check-and-increment.
        loop {
            let current = self.allocated.load(Ordering::Relaxed);
            let new =
                current
                    .checked_add(bytes)
                    .ok_or_else(|| QueryError::QueryMemoryExceeded {
                        requested: bytes,
                        allocated: current,
                        budget: self.budget,
                    })?;

            if new > self.budget {
                return Err(QueryError::QueryMemoryExceeded {
                    requested: bytes,
                    allocated: current,
                    budget: self.budget,
                });
            }

            if self
                .allocated
                .compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
            // CAS failed — another thread updated concurrently; retry.
        }
    }

    /// Release `bytes` back to the budget.
    ///
    /// # Panics
    ///
    /// Debug-asserts that `bytes` does not exceed the current allocation.
    pub fn deallocate(&self, bytes: usize) {
        let prev = self.allocated.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(
            prev >= bytes,
            "deallocate({bytes}) but only {prev} bytes were allocated"
        );
    }

    /// Returns the current number of allocated bytes.
    #[must_use]
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Returns the configured budget in bytes.
    #[must_use]
    pub fn budget(&self) -> usize {
        self.budget
    }

    /// Returns the number of bytes remaining before the budget is
    /// exhausted.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.budget
            .saturating_sub(self.allocated.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_allocate_deallocate() {
        let t = MemoryTracker::new(1000);
        assert_eq!(t.budget(), 1000);
        assert_eq!(t.allocated(), 0);
        assert_eq!(t.remaining(), 1000);

        t.try_allocate(400).unwrap();
        assert_eq!(t.allocated(), 400);
        assert_eq!(t.remaining(), 600);

        t.try_allocate(600).unwrap();
        assert_eq!(t.allocated(), 1000);
        assert_eq!(t.remaining(), 0);

        t.deallocate(300);
        assert_eq!(t.allocated(), 700);
        assert_eq!(t.remaining(), 300);
    }

    #[test]
    fn exceeds_budget() {
        let t = MemoryTracker::new(100);
        t.try_allocate(60).unwrap();

        let err = t.try_allocate(50).unwrap_err();
        match err {
            QueryError::QueryMemoryExceeded {
                requested,
                allocated,
                budget,
            } => {
                assert_eq!(requested, 50);
                assert_eq!(allocated, 60);
                assert_eq!(budget, 100);
            }
            other => panic!("unexpected error: {other}"),
        }

        // Original allocation unchanged after failure.
        assert_eq!(t.allocated(), 60);
    }

    #[test]
    fn exact_budget() {
        let t = MemoryTracker::new(100);
        t.try_allocate(100).unwrap();
        assert_eq!(t.allocated(), 100);
        assert_eq!(t.remaining(), 0);

        // One more byte fails.
        assert!(t.try_allocate(1).is_err());
    }

    #[test]
    fn zero_budget() {
        let t = MemoryTracker::new(0);
        assert!(t.try_allocate(1).is_err());
        // Zero allocation always succeeds.
        t.try_allocate(0).unwrap();
    }

    #[test]
    fn deallocate_full() {
        let t = MemoryTracker::new(500);
        t.try_allocate(500).unwrap();
        t.deallocate(500);
        assert_eq!(t.allocated(), 0);
        assert_eq!(t.remaining(), 500);

        // Can allocate again.
        t.try_allocate(500).unwrap();
    }

    #[test]
    fn concurrent_allocations() {
        use std::sync::Arc;
        use std::thread;

        let t = Arc::new(MemoryTracker::new(10_000));
        let mut handles = vec![];

        for _ in 0..10 {
            let tracker = Arc::clone(&t);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    // Each thread tries to allocate 1 byte.
                    let _ = tracker.try_allocate(1);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // At most 1000 bytes allocated (10 threads × 100 allocations × 1 byte).
        assert!(t.allocated() <= 1000);
    }

    #[test]
    fn error_display() {
        let t = MemoryTracker::new(100);
        t.try_allocate(80).unwrap();
        let err = t.try_allocate(30).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("30"), "should mention requested bytes");
        assert!(msg.contains("80"), "should mention allocated bytes");
        assert!(msg.contains("100"), "should mention budget");
    }
}
