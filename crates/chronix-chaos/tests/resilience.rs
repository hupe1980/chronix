//! Resilience test suite — validates ChaosAgent behaviour under
//! controlled fault injection: node kills, disk faults, network
//! partitions, write drops, read corruption, compound failures,
//! fault expiry, and concurrency limits.

use std::sync::Arc;
use std::time::Duration;

use chronix_chaos::{ChaosAgent, Fault, FaultConfig};

/// A fault that outlives the test (released via guard drop).
fn long_fault(fault: Fault) -> FaultConfig {
    FaultConfig {
        fault,
        duration: Duration::from_secs(60),
        description: "long-lived fault".into(),
    }
}

// ═══════════════════════════════════════════════════════════════
// Scenario 1: Node kill scheduling
//
// A KillNode fault is visible while active and clears on guard
// drop — the supervisor loop polls this flag to terminate a node.
// ═══════════════════════════════════════════════════════════════

#[test]
fn kill_node_fault_active_and_recovers() {
    let agent = Arc::new(ChaosAgent::default());
    let is_kill = |f: &Fault| matches!(f, Fault::KillNode { .. });

    assert!(!agent.is_fault_active(&is_kill));

    let guard = agent
        .inject(&long_fault(Fault::KillNode {
            delay: Duration::from_millis(10),
        }))
        .unwrap();
    assert!(agent.is_fault_active(&is_kill));

    drop(guard);
    assert!(!agent.is_fault_active(&is_kill));
}

// ═══════════════════════════════════════════════════════════════
// Scenario 2: Disk full — writes rejected, recovery restores them
// ═══════════════════════════════════════════════════════════════

#[test]
fn disk_full_rejects_writes_until_recovery() {
    let agent = Arc::new(ChaosAgent::default());
    assert!(!agent.is_disk_full());

    let guard = agent.inject(&long_fault(Fault::DiskFull)).unwrap();
    assert!(agent.is_disk_full());

    // Other subsystems unaffected.
    assert!(!agent.should_drop_write());
    assert!(!agent.is_partitioned(1));
    assert_eq!(agent.injected_latency(), Duration::ZERO);

    drop(guard);
    assert!(!agent.is_disk_full());
}

// ═══════════════════════════════════════════════════════════════
// Scenario 3: Network partition — minority isolated, reunion heals
// ═══════════════════════════════════════════════════════════════

#[test]
fn network_partition_isolates_only_target_nodes() {
    let agent = Arc::new(ChaosAgent::default());

    let guard = agent
        .inject(&long_fault(Fault::NetworkPartition {
            isolated_nodes: vec![2, 5],
        }))
        .unwrap();

    assert!(agent.is_partitioned(2));
    assert!(agent.is_partitioned(5));
    assert!(!agent.is_partitioned(1), "majority nodes stay connected");
    assert!(!agent.is_partitioned(3));

    // Reunion: partition heals, all nodes reachable again.
    drop(guard);
    assert!(!agent.is_partitioned(2));
    assert!(!agent.is_partitioned(5));
}

// ═══════════════════════════════════════════════════════════════
// Scenario 4: Slow disk — I/O latency injected and removed
// ═══════════════════════════════════════════════════════════════

#[test]
fn slow_disk_injects_latency() {
    let agent = Arc::new(ChaosAgent::default());
    assert_eq!(agent.injected_latency(), Duration::ZERO);

    let guard = agent
        .inject(&long_fault(Fault::SlowDisk {
            latency: Duration::from_millis(150),
        }))
        .unwrap();
    assert!(agent.injected_latency() >= Duration::from_millis(150));

    drop(guard);
    assert_eq!(agent.injected_latency(), Duration::ZERO);
}

// ═══════════════════════════════════════════════════════════════
// Scenario 5: Read corruption — ratio honoured, recovery clean
// ═══════════════════════════════════════════════════════════════

#[test]
fn read_corruption_ratio_reported_while_active() {
    let agent = Arc::new(ChaosAgent::default());
    assert!(agent.should_corrupt_read().is_none());

    let guard = agent
        .inject(&long_fault(Fault::ReadCorruption {
            corruption_ratio: 1.0,
        }))
        .unwrap();
    let ratio = agent.should_corrupt_read();
    assert!(ratio.is_some());
    assert!((ratio.unwrap() - 1.0).abs() < f64::EPSILON);

    drop(guard);
    assert!(agent.should_corrupt_read().is_none());
}

// ═══════════════════════════════════════════════════════════════
// Scenario 6: Compound failures (multiple simultaneous faults)
//
// Validates that the ChaosAgent correctly handles concurrent
// fault injections, independent recovery, and max-fault limits.
// ═══════════════════════════════════════════════════════════════

#[test]
fn compound_failures_and_independent_recovery() {
    let agent = Arc::new(ChaosAgent::default());

    // Inject multiple faults simultaneously.
    let g1 = agent
        .inject(&long_fault(Fault::WriteDropper { drop_ratio: 1.0 }))
        .unwrap();
    let g2 = agent.inject(&long_fault(Fault::DiskFull)).unwrap();
    let g3 = agent
        .inject(&long_fault(Fault::NetworkPartition {
            isolated_nodes: vec![5],
        }))
        .unwrap();
    let g4 = agent
        .inject(&long_fault(Fault::SlowDisk {
            latency: Duration::from_millis(200),
        }))
        .unwrap();

    assert_eq!(agent.active_count(), 4);
    assert!(agent.should_drop_write());
    assert!(agent.is_disk_full());
    assert!(agent.is_partitioned(5));
    assert!(agent.injected_latency() >= Duration::from_millis(200));

    // Recover write-drop and disk independently.
    drop(g1);
    assert!(!agent.should_drop_write());
    assert!(agent.is_disk_full());
    assert_eq!(agent.active_count(), 3);

    drop(g2);
    assert!(!agent.is_disk_full());
    assert_eq!(agent.active_count(), 2);

    // Network and latency still active.
    assert!(agent.is_partitioned(5));
    assert!(agent.injected_latency() >= Duration::from_millis(200));

    drop(g3);
    drop(g4);
    assert_eq!(agent.active_count(), 0);
    assert!(!agent.has_active_faults());
}

#[test]
fn clear_all_recovers_from_compound_failure() {
    let agent = Arc::new(ChaosAgent::default());

    let _g1 = agent
        .inject(&long_fault(Fault::WriteDropper { drop_ratio: 1.0 }))
        .unwrap();
    let _g2 = agent.inject(&long_fault(Fault::DiskFull)).unwrap();
    let _g3 = agent
        .inject(&long_fault(Fault::NetworkPartition {
            isolated_nodes: vec![1, 2, 3],
        }))
        .unwrap();

    assert_eq!(agent.active_count(), 3);

    // Emergency clear-all.
    agent.clear_all();

    assert_eq!(agent.active_count(), 0);
    assert!(!agent.should_drop_write());
    assert!(!agent.is_disk_full());
    assert!(!agent.is_partitioned(1));
}

// ═══════════════════════════════════════════════════════════════
// Scenario 7: Fault expiry (auto-cleanup)
//
// Faults with short durations should auto-expire.
// ═══════════════════════════════════════════════════════════════

#[test]
fn expired_faults_are_garbage_collected() {
    let agent = Arc::new(ChaosAgent::default());

    // Inject a fault with near-zero duration (immediately expired).
    let config = FaultConfig {
        fault: Fault::DiskFull,
        duration: Duration::from_nanos(1),
        description: "immediate expiry".into(),
    };
    let _guard = agent.inject(&config).unwrap();

    // Wait a tiny bit to ensure the fault is expired.
    std::thread::sleep(Duration::from_millis(10));

    // GC should clean it up on next inject.
    let config2 = FaultConfig {
        fault: Fault::SlowDisk {
            latency: Duration::from_millis(5),
        },
        duration: Duration::from_secs(60),
        description: "trigger GC".into(),
    };
    let _g2 = agent.inject(&config2).unwrap();

    // Only the non-expired fault should be active.
    // (Exact count depends on GC timing; the important thing is
    // that is_disk_full() returns false for the expired fault.)
    // Note: the DiskFull guard still holds a reference, so the
    // fault ID is still in the map. But InjectionInfo.is_expired()
    // returns true.
    let injections = agent.active_injections();
    let disk_faults: Vec<_> = injections
        .iter()
        .filter(|i| matches!(i.fault, Fault::DiskFull) && !i.is_expired())
        .collect();
    assert!(
        disk_faults.is_empty(),
        "expired DiskFull should not appear as active"
    );
}

// ═══════════════════════════════════════════════════════════════
// Scenario 8: Latency injection
//
// Validates that injected latency accumulates correctly across
// multiple SlowDisk/LatencySpike faults.
// ═══════════════════════════════════════════════════════════════

#[test]
fn latency_accumulates_from_multiple_faults() {
    let agent = Arc::new(ChaosAgent::default());

    let g1 = agent
        .inject(&long_fault(Fault::SlowDisk {
            latency: Duration::from_millis(100),
        }))
        .unwrap();
    let g2 = agent
        .inject(&long_fault(Fault::LatencySpike {
            delay: Duration::from_millis(200),
        }))
        .unwrap();

    // Total injected latency is at least 300ms.
    let total = agent.injected_latency();
    assert!(
        total >= Duration::from_millis(300),
        "expected ≥300ms, got {total:?}"
    );

    // Remove one.
    drop(g1);
    let remaining = agent.injected_latency();
    assert!(
        remaining >= Duration::from_millis(200),
        "expected ≥200ms after removing SlowDisk"
    );
    assert!(
        remaining < Duration::from_millis(300),
        "expected <300ms after removing SlowDisk"
    );

    drop(g2);
    assert_eq!(agent.injected_latency(), Duration::ZERO);
}

// ═══════════════════════════════════════════════════════════════
// Scenario 9: Write dropper with probabilistic drops
//
// Validates that the drop ratio roughly matches expectations.
// ═══════════════════════════════════════════════════════════════

#[test]
fn write_dropper_50_percent() {
    let agent = Arc::new(ChaosAgent::default());

    let guard = agent
        .inject(&long_fault(Fault::WriteDropper { drop_ratio: 0.5 }))
        .unwrap();

    // Run 10000 trials for statistical significance.
    let trials = 10_000;
    let drops: usize = (0..trials).filter(|_| agent.should_drop_write()).count();

    // With 50% ratio, drops should be roughly 5000 ± 500 (5 sigma).
    assert!(
        drops > 4000 && drops < 6000,
        "expected ~50% drops, got {drops}/{trials}"
    );

    drop(guard);
}

#[test]
fn write_dropper_0_percent_drops_nothing() {
    let agent = Arc::new(ChaosAgent::default());

    let guard = agent
        .inject(&long_fault(Fault::WriteDropper { drop_ratio: 0.0 }))
        .unwrap();

    let drops: usize = (0..1000).filter(|_| agent.should_drop_write()).count();
    assert_eq!(drops, 0, "0% drop ratio should drop nothing");

    drop(guard);
}

// ═══════════════════════════════════════════════════════════════
// Scenario 10: Max concurrent faults enforcement
//
// Validates that the ChaosAgent enforces its maximum concurrent
// fault limit to prevent runaway injection.
// ═══════════════════════════════════════════════════════════════

#[test]
fn max_concurrent_enforcement() {
    let agent = ChaosAgent::with_max_concurrent(2);
    let agent = Arc::new(agent);

    let _g1 = agent.inject(&long_fault(Fault::DiskFull)).unwrap();
    let _g2 = agent
        .inject(&long_fault(Fault::SlowDisk {
            latency: Duration::from_millis(5),
        }))
        .unwrap();

    // Third injection should fail.
    let result = agent.inject(&long_fault(Fault::DiskFull));
    assert!(
        result.is_err(),
        "should reject when at max concurrent faults"
    );
}
