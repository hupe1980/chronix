//! Request-ID deduplication cache.
//!
//! Provides a bounded, thread-safe cache of recently-processed write
//! request IDs. When a write is retried after leader failover, the
//! receiver can detect the duplicate request ID and return the cached
//! response instead of re-applying the write.
//!
//! # Design
//!
//! Uses a `DashMap` with LRU-style eviction. Each entry stores the
//! request ID, the cached response (points written), and the insertion
//! timestamp. A background sweep or inline eviction removes entries
//! older than the configured TTL.
//!
//! # Cluster-Wide Dedup (FIXED)
//!
//! **Two-tier deduplication** ensures exactly-once write semantics:
//!
//! 1. **Node-local cache** (this module) — fast-path dedup for
//!    same-leader retries. Avoids Raft round-trips entirely.
//! 2. **Raft-replicated dedup** (in `RegionSmStore::apply()`) — the
//!    `request_id` is embedded in `RegionWriteCommand::WritePoints`
//!    and persisted in the Raft log. Every replica checks an
//!    `applied_requests` cache at apply time, so duplicate writes
//!    are detected even after leader failover.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

/// Default maximum number of tracked request IDs.
const DEFAULT_MAX_ENTRIES: usize = 100_000;
/// Default time-to-live for dedup entries.
const DEFAULT_TTL: Duration = Duration::from_secs(300); // 5 minutes

/// A cached write result keyed by request ID.
#[derive(Debug, Clone)]
struct DeduplicationEntry {
    /// Number of points written by the original request.
    written: u64,
    /// Unix timestamp (seconds) when this entry was inserted.
    inserted_at: u64,
}

/// Bounded, thread-safe deduplication cache for write request IDs.
///
/// Callers should:
/// 1. Call `check` before processing a write. If it returns `Some(n)`,
///    the request was already processed — return `n` without re-writing.
/// 2. Call `record` after successfully processing a write to cache the
///    result for future duplicate detection.
#[derive(Debug)]
pub struct DeduplicationCache {
    entries: DashMap<String, DeduplicationEntry>,
    max_entries: usize,
    ttl_secs: u64,
    /// Monotonic counter for inline eviction scheduling.
    ops_since_sweep: AtomicU64,
}

impl DeduplicationCache {
    /// Create a new dedup cache with default settings (100K entries, 5min TTL).
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(DEFAULT_MAX_ENTRIES, DEFAULT_TTL)
    }

    /// Create a dedup cache with custom capacity and TTL.
    #[must_use]
    pub fn with_config(max_entries: usize, ttl: Duration) -> Self {
        Self {
            entries: DashMap::with_capacity(max_entries.min(1024)),
            max_entries,
            ttl_secs: ttl.as_secs(),
            ops_since_sweep: AtomicU64::new(0),
        }
    }

    /// Check if a request ID has already been processed.
    ///
    /// Returns `Some(written)` if the request was already handled,
    /// `None` if this is a new request.
    pub fn check(&self, request_id: &str) -> Option<u64> {
        let now = now_secs();
        let entry = self.entries.get(request_id)?;
        if now.saturating_sub(entry.inserted_at) >= self.ttl_secs {
            // Expired entry — remove and treat as new.
            drop(entry);
            self.entries.remove(request_id);
            return None;
        }
        Some(entry.written)
    }

    /// Record a successfully processed request for future dedup.
    pub fn record(&self, request_id: String, written: u64) {
        let now = now_secs();
        self.entries.insert(
            request_id,
            DeduplicationEntry {
                written,
                inserted_at: now,
            },
        );

        // Inline eviction: every 1000 operations, sweep expired entries
        // and trim to max capacity.
        let ops = self.ops_since_sweep.fetch_add(1, Ordering::Relaxed);
        if ops % 1000 == 0 {
            self.sweep(now);
        }
    }

    /// Number of entries currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove expired entries and trim to capacity.
    fn sweep(&self, now: u64) {
        // Remove expired
        self.entries
            .retain(|_, entry| now.saturating_sub(entry.inserted_at) < self.ttl_secs);

        // If still over capacity, remove oldest entries
        if self.entries.len() > self.max_entries {
            let excess = self.entries.len() - self.max_entries;
            // Collect the oldest `excess` keys
            let mut entries_by_age: Vec<(String, u64)> = self
                .entries
                .iter()
                .map(|r| (r.key().clone(), r.value().inserted_at))
                .collect();
            entries_by_age.sort_by_key(|(_, ts)| *ts);
            for (key, _) in entries_by_age.into_iter().take(excess) {
                self.entries.remove(&key);
            }
        }
    }
}

impl Default for DeduplicationCache {
    fn default() -> Self {
        Self::new()
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_request_returns_none() {
        let cache = DeduplicationCache::new();
        assert!(cache.check("req-1").is_none());
    }

    #[test]
    fn recorded_request_returns_written() {
        let cache = DeduplicationCache::new();
        cache.record("req-1".into(), 42);
        assert_eq!(cache.check("req-1"), Some(42));
    }

    #[test]
    fn different_ids_independent() {
        let cache = DeduplicationCache::new();
        cache.record("req-1".into(), 10);
        cache.record("req-2".into(), 20);
        assert_eq!(cache.check("req-1"), Some(10));
        assert_eq!(cache.check("req-2"), Some(20));
        assert!(cache.check("req-3").is_none());
    }

    #[test]
    fn expired_entry_treated_as_new() {
        let cache = DeduplicationCache::with_config(100, Duration::from_secs(1));
        cache.record("req-1".into(), 5);
        // TTL=1s, sleep >1s to ensure expiry
        std::thread::sleep(Duration::from_millis(1100));
        assert!(cache.check("req-1").is_none());
    }

    #[test]
    fn sweep_removes_expired() {
        let cache = DeduplicationCache::with_config(100, Duration::from_secs(1));
        cache.record("req-1".into(), 1);
        cache.record("req-2".into(), 2);
        std::thread::sleep(Duration::from_millis(1100));
        cache.sweep(now_secs());
        assert!(cache.is_empty());
    }

    #[test]
    fn capacity_limit_enforced() {
        let cache = DeduplicationCache::with_config(5, Duration::from_secs(3600));
        for i in 0..10 {
            cache.record(format!("req-{i}"), i as u64);
        }
        cache.sweep(now_secs());
        assert!(cache.len() <= 5);
    }
}
