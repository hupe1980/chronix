//! Virtual clock for deterministic time control.
//!
//! Provides a monotonic clock that advances only when explicitly told to,
//! enabling deterministic testing of time-dependent distributed protocols
//! such as leader election timeouts and heartbeat intervals.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// A virtual clock for deterministic simulation.
///
/// Time advances only via `advance` or `advance_ms`. All nodes in a simulation share
/// a single `VirtualClock`, but per-node skew can be applied via
/// [`skew_for_node`](Self::skew_for_node).
#[derive(Debug)]
pub struct VirtualClock {
    /// Current virtual time in milliseconds since epoch 0.
    now_ms: AtomicU64,
    /// Per-node clock skew in milliseconds (indexed by node_id - 1).
    skews: Vec<AtomicI64>,
}

impl VirtualClock {
    /// Create a new virtual clock starting at time 0 for `node_count` nodes.
    #[must_use]
    pub fn new(node_count: usize) -> Self {
        let skews = (0..node_count).map(|_| AtomicI64::new(0)).collect();
        Self {
            now_ms: AtomicU64::new(0),
            skews,
        }
    }

    /// Current virtual time in milliseconds.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::Acquire)
    }

    /// Advance the clock by `ms` milliseconds.
    pub fn advance_ms(&self, ms: u64) {
        self.now_ms.fetch_add(ms, Ordering::Release);
    }

    /// Set clock skew for a specific node.
    ///
    /// Positive values make the node's clock run ahead; negative values
    /// make it run behind. Skew is additive to the global virtual time.
    pub fn set_skew(&self, node_id: u64, skew_ms: i64) {
        if let Some(s) = self.skews.get(node_id.saturating_sub(1) as usize) {
            s.store(skew_ms, Ordering::Release);
        }
    }

    /// Get the skew-adjusted time for a specific node.
    #[must_use]
    pub fn skew_for_node(&self, node_id: u64) -> u64 {
        let base = self.now_ms() as i64;
        let skew = self
            .skews
            .get(node_id.saturating_sub(1) as usize)
            .map_or(0, |s| s.load(Ordering::Acquire));
        (base + skew).max(0) as u64
    }

    /// Reset all skews to zero.
    pub fn clear_skews(&self) {
        for s in &self.skews {
            s.store(0, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_advances_deterministically() {
        let clock = VirtualClock::new(3);
        assert_eq!(clock.now_ms(), 0);

        clock.advance_ms(100);
        assert_eq!(clock.now_ms(), 100);

        clock.advance_ms(50);
        assert_eq!(clock.now_ms(), 150);
    }

    #[test]
    fn per_node_skew() {
        let clock = VirtualClock::new(3);
        clock.advance_ms(1000);

        // Node 1: +50ms ahead
        clock.set_skew(1, 50);
        assert_eq!(clock.skew_for_node(1), 1050);

        // Node 2: -30ms behind
        clock.set_skew(2, -30);
        assert_eq!(clock.skew_for_node(2), 970);

        // Node 3: no skew
        assert_eq!(clock.skew_for_node(3), 1000);
    }

    #[test]
    fn clear_skews_resets_all() {
        let clock = VirtualClock::new(2);
        clock.advance_ms(500);
        clock.set_skew(1, 100);
        clock.set_skew(2, -100);

        clock.clear_skews();

        assert_eq!(clock.skew_for_node(1), 500);
        assert_eq!(clock.skew_for_node(2), 500);
    }

    #[test]
    fn skew_clamps_to_zero() {
        let clock = VirtualClock::new(1);
        clock.advance_ms(10);
        clock.set_skew(1, -100);
        // Negative time clamped to 0
        assert_eq!(clock.skew_for_node(1), 0);
    }
}
