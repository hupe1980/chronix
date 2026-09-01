//! Chaos agent — manages active fault injections with RAII guards.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tracing::{info, warn};

use crate::error::ChaosError;
use crate::fault::{Fault, FaultConfig};

// ── Injection Metadata ────────────────────────────────────────

/// Information about an active fault injection.
#[derive(Debug, Clone)]
pub struct InjectionInfo {
    /// Unique injection ID.
    pub id: u64,
    /// The injected fault.
    pub fault: Fault,
    /// When the injection was created.
    pub started_at: Instant,
    /// Maximum duration before auto-expiry.
    pub duration: Duration,
    /// Description from the configuration.
    pub description: String,
}

impl InjectionInfo {
    /// Check whether this injection has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.started_at.elapsed() >= self.duration
    }

    /// Remaining time before expiry.
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.duration.saturating_sub(self.started_at.elapsed())
    }
}

// ── ChaosAgent ───────────────────────────────────────────────

/// Per-node chaos agent that manages fault injections.
///
/// The `ChaosAgent` is designed to be injected into server components
/// via `Arc<ChaosAgent>`. Application code checks the agent before
/// I/O operations to simulate faults.
///
/// All injections are time-limited and automatically expire. Dropping
/// a [`FaultGuard`] immediately clears the corresponding injection.
///
/// # Production Safety
///
/// In release builds, [`inject`](Self::inject) requires the environment
/// variable `CHRONIX_CHAOS_ENABLED=true`. This prevents accidental fault
/// injection in production from leaked agent references.
///
/// # Thread Safety
///
/// `ChaosAgent` is `Send + Sync` and designed for concurrent access
/// from multiple async tasks. Read-path queries (`should_drop_write`,
/// `is_disk_full`, etc.) use a read lock and never trigger GC, keeping
/// the hot path contention-free.
#[derive(Debug)]
pub struct ChaosAgent {
    injections: RwLock<BTreeMap<u64, InjectionInfo>>,
    next_id: AtomicU64,
    max_concurrent: usize,
    /// Tracks when the last GC was performed (monotonic nanos since
    /// `epoch`) to avoid taking a write lock on every read-path query
    /// Uses a monotonic `Instant` rather than `SystemTime`.
    last_gc: AtomicU64,
    /// Monotonic reference instant for GC timing.
    epoch: Instant,
}

impl Default for ChaosAgent {
    fn default() -> Self {
        Self::new()
    }
}

impl ChaosAgent {
    /// Create a new chaos agent with default limits.
    #[must_use]
    pub fn new() -> Self {
        Self::with_max_concurrent(16)
    }

    /// Create a chaos agent with a custom max concurrent fault limit.
    #[must_use]
    pub fn with_max_concurrent(max: usize) -> Self {
        Self {
            injections: RwLock::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
            max_concurrent: max,
            last_gc: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    /// Inject a fault. Returns a [`FaultGuard`] that will clean up on drop.
    ///
    /// # Safety Guard
    ///
    /// In release builds (`cfg(not(debug_assertions))`), fault injection is
    /// **only** permitted when the environment variable
    /// `CHRONIX_CHAOS_ENABLED=true` is set. This prevents accidental data
    /// corruption in production from leaked agent references.
    ///
    /// # Errors
    ///
    /// Returns [`ChaosError::MaxFaultsExceeded`] if the maximum number
    /// of concurrent injections is reached.
    /// Returns [`ChaosError::InvalidConfig`] if a ratio parameter is out of
    /// range, or if chaos is not enabled in a release build.
    pub fn inject(self: &Arc<Self>, config: &FaultConfig) -> Result<FaultGuard, ChaosError> {
        // Block injection in release builds unless explicitly enabled.
        #[cfg(not(debug_assertions))]
        {
            match std::env::var("CHRONIX_CHAOS_ENABLED") {
                Ok(val) if val == "true" || val == "1" => {}
                _ => {
                    return Err(ChaosError::InvalidConfig(
                        "chaos injection blocked in release build: set CHRONIX_CHAOS_ENABLED=true to enable".to_string(),
                    ));
                }
            }
        }

        // Validate all fault parameters (comprehensive
        // config validation in a single place).
        config.validate().map_err(ChaosError::InvalidConfig)?;

        self.gc_expired();

        let mut injections = self.injections.write();

        if injections.len() >= self.max_concurrent {
            return Err(ChaosError::MaxFaultsExceeded(self.max_concurrent));
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let info = InjectionInfo {
            id,
            fault: config.fault.clone(),
            started_at: Instant::now(),
            duration: config.duration,
            description: config.description.clone(),
        };

        info!(
            injection_id = id,
            fault = %config.fault,
            duration = ?config.duration,
            description = %config.description,
            "Chaos fault injected"
        );

        metrics::counter!("chronix_chaos_injections_total").increment(1);
        injections.insert(id, info);

        Ok(FaultGuard {
            agent: Arc::clone(self),
            id,
        })
    }

    /// Remove a specific injection by ID.
    ///
    /// # Errors
    ///
    /// Returns [`ChaosError::NotFound`] if the injection ID does not exist.
    pub fn clear(&self, id: u64) -> Result<(), ChaosError> {
        let mut injections = self.injections.write();
        if injections.remove(&id).is_some() {
            info!(injection_id = id, "Chaos fault cleared");
            Ok(())
        } else {
            Err(ChaosError::NotFound(id))
        }
    }

    /// Remove all active injections.
    pub fn clear_all(&self) {
        let mut injections = self.injections.write();
        let count = injections.len();
        injections.clear();
        if count > 0 {
            info!(count, "All chaos faults cleared");
        }
    }

    /// List all active (non-expired) injections.
    #[must_use]
    pub fn active_injections(&self) -> Vec<InjectionInfo> {
        self.maybe_gc();
        self.injections
            .read()
            .values()
            .filter(|info| !info.is_expired())
            .cloned()
            .collect()
    }

    /// Check if any faults are currently active (non-expired).
    #[must_use]
    pub fn has_active_faults(&self) -> bool {
        self.maybe_gc();
        self.injections
            .read()
            .values()
            .any(|info| !info.is_expired())
    }

    /// Number of active faults.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.maybe_gc();
        self.injections
            .read()
            .values()
            .filter(|info| !info.is_expired())
            .count()
    }

    // ── Fault Queries ─────────────────────────────────────────

    /// Check if a specific fault type is currently active.
    ///
    /// Uses read lock only — skips expired entries inline without
    /// taking a write lock on the hot path.
    #[must_use]
    pub fn is_fault_active(&self, matcher: &dyn Fn(&Fault) -> bool) -> bool {
        self.maybe_gc();
        self.injections
            .read()
            .values()
            .any(|info| !info.is_expired() && matcher(&info.fault))
    }

    /// Get the latency to inject (sum of all active latency faults).
    #[must_use]
    pub fn injected_latency(&self) -> Duration {
        self.maybe_gc();
        self.injections
            .read()
            .values()
            .filter(|info| !info.is_expired())
            .filter_map(|info| match &info.fault {
                Fault::LatencySpike { delay } | Fault::SlowDisk { latency: delay } => Some(*delay),
                _ => None,
            })
            .sum()
    }

    /// Check if writes should be dropped (any active `WriteDropper`).
    ///
    /// Returns `true` if a random sample falls within the drop ratio.
    /// Uses read lock only — no write-lock GC on hot path.
    #[must_use]
    pub fn should_drop_write(&self) -> bool {
        self.maybe_gc();
        let injections = self.injections.read();
        for info in injections.values() {
            if info.is_expired() {
                continue;
            }
            if let Fault::WriteDropper { drop_ratio } = &info.fault {
                let sample: f64 = rand::random();
                if sample < *drop_ratio {
                    return true;
                }
            }
        }
        false
    }

    /// Check if disk-full should be simulated.
    #[must_use]
    pub fn is_disk_full(&self) -> bool {
        self.is_fault_active(&|f| matches!(f, Fault::DiskFull))
    }

    /// Check if network partition is active for a given node.
    #[must_use]
    pub fn is_partitioned(&self, node_id: u64) -> bool {
        self.is_fault_active(&|f| {
            matches!(f, Fault::NetworkPartition { isolated_nodes } if isolated_nodes.contains(&node_id))
        })
    }

    /// Check if reads should be corrupted (any active `ReadCorruption`).
    ///
    /// Returns `Some(corruption_ratio)` when a random sample falls within the
    /// corruption probability, `None` otherwise.
    /// Uses read lock only — no write-lock GC on hot path.
    #[must_use]
    pub fn should_corrupt_read(&self) -> Option<f64> {
        self.maybe_gc();
        let injections = self.injections.read();
        for info in injections.values() {
            if info.is_expired() {
                continue;
            }
            if let Fault::ReadCorruption { corruption_ratio } = &info.fault {
                let sample: f64 = rand::random();
                if sample < *corruption_ratio {
                    return Some(*corruption_ratio);
                }
            }
        }
        None
    }

    // ── Internal ──────────────────────────────────────────────

    /// Only run GC if at least 1 second has passed since the last GC
    /// (Avoids a write lock on every read-path query.)
    fn maybe_gc(&self) {
        // Use monotonic `Instant` via the epoch stored at
        // construction. Avoids NTP clock-jump issues with SystemTime.
        let now_ns = u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let last = self.last_gc.load(Ordering::Relaxed);
        // 1 second = 1_000_000_000 ns
        if now_ns.saturating_sub(last) > 1_000_000_000
            && self
                .last_gc
                .compare_exchange(last, now_ns, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            self.gc_expired();
        }
    }

    /// Garbage-collect expired injections.
    fn gc_expired(&self) {
        let mut injections = self.injections.write();
        let before = injections.len();
        injections.retain(|id, info| {
            if info.is_expired() {
                warn!(
                    injection_id = id,
                    fault = %info.fault,
                    "Chaos fault expired"
                );
                false
            } else {
                true
            }
        });
        let removed = before - injections.len();
        if removed > 0 {
            metrics::counter!("chronix_chaos_expirations_total").increment(removed as u64);
        }
    }
}

// ── FaultGuard ───────────────────────────────────────────────

/// RAII guard that clears a fault injection when dropped.
///
/// Created by [`ChaosAgent::inject`]. Holding this guard keeps the
/// fault active; dropping it removes the injection immediately.
#[derive(Debug)]
pub struct FaultGuard {
    agent: Arc<ChaosAgent>,
    id: u64,
}

impl FaultGuard {
    /// Get the injection ID managed by this guard.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get information about the guarded injection.
    #[must_use]
    pub fn info(&self) -> Option<InjectionInfo> {
        self.agent.injections.read().get(&self.id).cloned()
    }
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        let _ = self.agent.clear(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_agent() -> Arc<ChaosAgent> {
        Arc::new(ChaosAgent::new())
    }

    #[test]
    fn inject_and_clear() {
        let agent = make_agent();
        let guard = agent
            .inject(&FaultConfig {
                fault: Fault::DiskFull,
                duration: Duration::from_secs(60),
                description: "test".into(),
            })
            .unwrap();

        assert!(agent.has_active_faults());
        assert_eq!(agent.active_count(), 1);

        let id = guard.id();
        drop(guard);

        assert!(!agent.has_active_faults());
        assert!(agent.clear(id).is_err()); // Already removed
    }

    #[test]
    fn guard_auto_cleanup() {
        let agent = make_agent();
        {
            let _guard = agent
                .inject(&FaultConfig {
                    fault: Fault::LatencySpike {
                        delay: Duration::from_millis(100),
                    },
                    duration: Duration::from_secs(60),
                    description: "spike".into(),
                })
                .unwrap();
            assert_eq!(agent.active_count(), 1);
        }
        // Guard dropped → injection cleared.
        assert_eq!(agent.active_count(), 0);
    }

    #[test]
    fn clear_all() {
        let agent = make_agent();
        let _g1 = agent
            .inject(&FaultConfig {
                fault: Fault::DiskFull,
                duration: Duration::from_secs(60),
                description: "a".into(),
            })
            .unwrap();
        let _g2 = agent
            .inject(&FaultConfig {
                fault: Fault::SlowDisk {
                    latency: Duration::from_millis(10),
                },
                duration: Duration::from_secs(60),
                description: "b".into(),
            })
            .unwrap();

        assert_eq!(agent.active_count(), 2);
        agent.clear_all();
        assert_eq!(agent.active_count(), 0);
    }

    #[test]
    fn max_concurrent_enforcement() {
        let agent = Arc::new(ChaosAgent::with_max_concurrent(2));

        let _g1 = agent
            .inject(&FaultConfig {
                fault: Fault::DiskFull,
                duration: Duration::from_secs(60),
                description: "a".into(),
            })
            .unwrap();
        let _g2 = agent
            .inject(&FaultConfig {
                fault: Fault::SlowDisk {
                    latency: Duration::from_millis(10),
                },
                duration: Duration::from_secs(60),
                description: "b".into(),
            })
            .unwrap();

        let result = agent.inject(&FaultConfig {
            fault: Fault::DiskFull,
            duration: Duration::from_secs(60),
            description: "c".into(),
        });
        assert!(matches!(result, Err(ChaosError::MaxFaultsExceeded(2))));
    }

    #[test]
    fn injected_latency_sums() {
        let agent = make_agent();
        let _g1 = agent
            .inject(&FaultConfig {
                fault: Fault::LatencySpike {
                    delay: Duration::from_millis(100),
                },
                duration: Duration::from_secs(60),
                description: "a".into(),
            })
            .unwrap();
        let _g2 = agent
            .inject(&FaultConfig {
                fault: Fault::SlowDisk {
                    latency: Duration::from_millis(50),
                },
                duration: Duration::from_secs(60),
                description: "b".into(),
            })
            .unwrap();

        let total = agent.injected_latency();
        assert_eq!(total, Duration::from_millis(150));
    }

    #[test]
    fn disk_full_check() {
        let agent = make_agent();
        assert!(!agent.is_disk_full());

        let _guard = agent
            .inject(&FaultConfig {
                fault: Fault::DiskFull,
                duration: Duration::from_secs(60),
                description: "test".into(),
            })
            .unwrap();

        assert!(agent.is_disk_full());
    }

    #[test]
    fn network_partition_check() {
        let agent = make_agent();
        assert!(!agent.is_partitioned(1));

        let _guard = agent
            .inject(&FaultConfig {
                fault: Fault::NetworkPartition {
                    isolated_nodes: vec![1, 2],
                },
                duration: Duration::from_secs(60),
                description: "test".into(),
            })
            .unwrap();

        assert!(agent.is_partitioned(1));
        assert!(agent.is_partitioned(2));
        assert!(!agent.is_partitioned(3));
    }

    #[test]
    fn read_corruption_check() {
        let agent = make_agent();
        // No active corruption → always None.
        assert!(agent.should_corrupt_read().is_none());

        // Inject with ratio = 1.0 so every read is corrupted.
        let _guard = agent
            .inject(&FaultConfig {
                fault: Fault::ReadCorruption {
                    corruption_ratio: 1.0,
                },
                duration: Duration::from_secs(60),
                description: "test read corruption".into(),
            })
            .unwrap();

        // With ratio 1.0 the method must return Some.
        let result = agent.should_corrupt_read();
        assert!(result.is_some());
        assert!((result.unwrap() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn read_corruption_not_triggered_at_zero_ratio() {
        let agent = make_agent();
        let _guard = agent
            .inject(&FaultConfig {
                fault: Fault::ReadCorruption {
                    corruption_ratio: 0.0,
                },
                duration: Duration::from_secs(60),
                description: "zero ratio".into(),
            })
            .unwrap();

        // ratio 0.0 → random < 0.0 is never true → always None
        for _ in 0..100 {
            assert!(agent.should_corrupt_read().is_none());
        }
    }

    #[test]
    fn active_injections_list() {
        let agent = make_agent();
        let _g1 = agent
            .inject(&FaultConfig {
                fault: Fault::DiskFull,
                duration: Duration::from_secs(60),
                description: "disk".into(),
            })
            .unwrap();
        let _g2 = agent
            .inject(&FaultConfig {
                fault: Fault::SlowDisk {
                    latency: Duration::from_millis(10),
                },
                duration: Duration::from_secs(60),
                description: "gpu".into(),
            })
            .unwrap();

        let active = agent.active_injections();
        assert_eq!(active.len(), 2);
    }

    #[test]
    fn expired_faults_gc() {
        let agent = make_agent();
        // Inject with tiny duration → immediately expired.
        let guard = agent
            .inject(&FaultConfig {
                fault: Fault::DiskFull,
                duration: Duration::from_nanos(1),
                description: "instant".into(),
            })
            .unwrap();

        // The fault is technically injected...
        let id = guard.id();
        std::mem::forget(guard); // Don't drop the guard.

        // ...but the fault is expired so it should not be considered active.
        std::thread::sleep(Duration::from_millis(1));

        assert!(!agent.has_active_faults());

        // GC may not have run yet (1-second cooldown), but the entry
        // is expired, so force-clear succeeds or entry is already gone.
        let _ = agent.clear(id);
    }

    #[test]
    fn injection_info_remaining() {
        let info = InjectionInfo {
            id: 1,
            fault: Fault::DiskFull,
            started_at: Instant::now(),
            duration: Duration::from_secs(60),
            description: "test".into(),
        };
        assert!(!info.is_expired());
        assert!(info.remaining() > Duration::from_secs(59));
    }

    #[test]
    fn fault_guard_info() {
        let agent = make_agent();
        let guard = agent
            .inject(&FaultConfig {
                fault: Fault::DiskFull,
                duration: Duration::from_secs(60),
                description: "test".into(),
            })
            .unwrap();

        let info = guard.info().unwrap();
        assert_eq!(info.description, "test");
        assert!(matches!(info.fault, Fault::DiskFull));
    }

    #[test]
    fn error_display() {
        assert!(ChaosError::NotFound(42).to_string().contains("42"));
        assert!(ChaosError::MaxFaultsExceeded(16).to_string().contains("16"));
        assert!(ChaosError::InvalidConfig("bad".into())
            .to_string()
            .contains("bad"));
    }

    #[test]
    fn default_agent() {
        let agent = ChaosAgent::default();
        assert!(!agent.has_active_faults());
    }

    #[test]
    fn inject_invalid_drop_ratio_rejected() {
        let agent = make_agent();
        let err = agent
            .inject(&FaultConfig {
                fault: Fault::WriteDropper { drop_ratio: 1.5 },
                duration: Duration::from_secs(10),
                description: "bad ratio".into(),
            })
            .unwrap_err();
        assert!(matches!(err, ChaosError::InvalidConfig(_)));

        let err = agent
            .inject(&FaultConfig {
                fault: Fault::WriteDropper { drop_ratio: -0.1 },
                duration: Duration::from_secs(10),
                description: "negative".into(),
            })
            .unwrap_err();
        assert!(matches!(err, ChaosError::InvalidConfig(_)));
    }

    #[test]
    fn inject_invalid_corruption_ratio_rejected() {
        let agent = make_agent();
        let err = agent
            .inject(&FaultConfig {
                fault: Fault::ReadCorruption {
                    corruption_ratio: 2.0,
                },
                duration: Duration::from_secs(10),
                description: "too high".into(),
            })
            .unwrap_err();
        assert!(matches!(err, ChaosError::InvalidConfig(_)));

        let err = agent
            .inject(&FaultConfig {
                fault: Fault::ReadCorruption {
                    corruption_ratio: -0.5,
                },
                duration: Duration::from_secs(10),
                description: "negative".into(),
            })
            .unwrap_err();
        assert!(matches!(err, ChaosError::InvalidConfig(_)));
    }
}
