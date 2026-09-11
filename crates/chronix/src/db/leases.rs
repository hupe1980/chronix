//! Which segment files a live reader may still open.
//!
//! # Why
//!
//! A scan is lazy: [`execute_iter`](super::Chronix::execute_iter) snapshots the
//! catalog once and opens each segment on the `next()` call that first needs
//! it, so a large export holds a list of paths for as long as the consumer
//! takes. Every pass that *retires* a segment — retention, a measurement drop,
//! cold-tier archiving, a compaction's inputs — used to remove the catalog
//! entry and then unlink the file, which is crash-safe and says nothing about
//! the reader already holding the path. A retention pass landing inside a scan
//! made the scan fail with a bare `No such file or directory`.
//!
//! # The rule
//!
//! **A segment leaves the catalog when its rows leave the database; its file
//! is unlinked when the last reader that could open it is gone.** This is the
//! reference count RocksDB keeps per `Version` and the `pendingReaders` wait
//! Prometheus keeps per block, at the size this engine needs: a count per
//! segment id, incremented under the catalog *read* lock the reader already
//! takes to snapshot, consulted under the catalog *write* lock the retirement
//! already takes. Those two locks make the pair atomic, so there is no window
//! where a reader has a path nobody is counting.
//!
//! A segment that is still leased when it is retired is marked
//! [`SegmentState::SoftDeleted`](chronix_core::SegmentState) — invisible to
//! every query from that moment — and its file is unlinked by the next
//! [`gc`](super::Chronix::gc), which is a step of every maintenance pass.
//!
//! This replaces a five-minute grace period, which was a wall clock standing
//! in for the question the count answers exactly: a scan shorter than the
//! grace was safe by luck and a longer one was not safe at all.

use std::collections::HashMap;

use chronix_core::SegmentId;
use parking_lot::Mutex;

/// Per-segment reader counts.
///
/// A leaf lock: nothing is acquired while it is held, so it takes no level in
/// [`crate::lock_order`].
#[derive(Debug, Default)]
pub(crate) struct SegmentLeases {
    held: Mutex<HashMap<SegmentId, u32>>,
}

impl SegmentLeases {
    /// Take a lease on every segment in `ids`.
    ///
    /// The caller must hold the catalog read lock, so that the segments are
    /// still registered at the moment they are counted.
    pub(crate) fn acquire<'a>(
        &'a self,
        ids: impl IntoIterator<Item = SegmentId>,
    ) -> SegmentLease<'a> {
        let ids: Vec<SegmentId> = ids.into_iter().collect();
        if !ids.is_empty() {
            let mut held = self.held.lock();
            for id in &ids {
                *held.entry(*id).or_insert(0) += 1;
            }
        }
        SegmentLease { leases: self, ids }
    }

    /// Whether any reader may still open this segment.
    ///
    /// The caller must hold the catalog write lock, so that no reader can
    /// take a lease between this answer and the unlink it decides.
    pub(crate) fn is_leased(&self, id: SegmentId) -> bool {
        self.held.lock().contains_key(&id)
    }

    /// How many segments are leased right now — reported by `statistics()`,
    /// because a count that only grows is how a leaked lease looks.
    pub(crate) fn len(&self) -> usize {
        self.held.lock().len()
    }
}

/// A reader's claim on the segment files it was handed.
///
/// Released on drop, including when a stream is abandoned part-way through or
/// dropped by a panic.
#[derive(Debug)]
pub(crate) struct SegmentLease<'a> {
    leases: &'a SegmentLeases,
    ids: Vec<SegmentId>,
}

impl Drop for SegmentLease<'_> {
    fn drop(&mut self) {
        if self.ids.is_empty() {
            return;
        }
        let mut held = self.leases.held.lock();
        for id in &self.ids {
            if let Some(n) = held.get_mut(id) {
                *n -= 1;
                if *n == 0 {
                    held.remove(id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_is_released_on_drop() {
        let leases = SegmentLeases::default();
        {
            let _a = leases.acquire([SegmentId(1), SegmentId(2)]);
            assert!(leases.is_leased(SegmentId(1)));
            assert_eq!(leases.len(), 2);
        }
        assert!(!leases.is_leased(SegmentId(1)));
        assert_eq!(leases.len(), 0);
    }

    #[test]
    fn two_readers_of_one_segment_both_have_to_finish() {
        let leases = SegmentLeases::default();
        let a = leases.acquire([SegmentId(7)]);
        let b = leases.acquire([SegmentId(7)]);
        drop(a);
        assert!(
            leases.is_leased(SegmentId(7)),
            "the second reader still holds the path"
        );
        drop(b);
        assert!(!leases.is_leased(SegmentId(7)));
    }

    #[test]
    fn an_empty_lease_costs_nothing() {
        let leases = SegmentLeases::default();
        let _l = leases.acquire([]);
        assert_eq!(leases.len(), 0);
    }
}
