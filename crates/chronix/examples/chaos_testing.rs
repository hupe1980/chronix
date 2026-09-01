#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Chaos Testing
//!
//! Demonstrates fault injection with RAII guards, multiple fault types,
//! and introspection of active injections.
//!
//! ```bash
//! cargo run --example chaos_testing
//! ```

use chronix_chaos::{ChaosAgent, Fault, FaultConfig};
use std::sync::Arc;
use std::time::Duration;

fn main() {
    println!("=== Chronix Chaos Testing ===\n");

    // ── 1. Create Agent ───────────────────────────────────────────
    println!("--- Chaos Agent ---");
    let agent = Arc::new(ChaosAgent::with_max_concurrent(8));
    println!("Created ChaosAgent (max_concurrent=8)");
    println!(
        "  Active faults: {} (has_active: {})\n",
        agent.active_count(),
        agent.has_active_faults()
    );

    // ── 2. Inject Latency Spike ───────────────────────────────────
    println!("--- Latency Spike ---");
    let latency_config = FaultConfig {
        fault: Fault::LatencySpike {
            delay: Duration::from_millis(250),
        },
        duration: Duration::from_secs(300),
        description: "Simulate network latency".to_string(),
    };
    latency_config.validate().expect("Validation failed");

    let guard_latency = agent
        .inject(&latency_config)
        .expect("Inject latency failed");
    println!(
        "  Injected latency spike (id={}, 250ms delay)",
        guard_latency.id()
    );
    println!("  Injected latency: {:?}", agent.injected_latency());
    println!("  Active faults: {}", agent.active_count());

    // ── 3. Inject Disk Full ───────────────────────────────────────
    println!("\n--- Disk Full ---");
    let disk_config = FaultConfig {
        fault: Fault::DiskFull,
        duration: Duration::from_secs(60),
        description: "Simulate disk full condition".to_string(),
    };
    let guard_disk = agent.inject(&disk_config).expect("Inject disk full failed");
    println!("  Injected disk full (id={})", guard_disk.id());
    println!("  is_disk_full: {}", agent.is_disk_full());

    // ── 4. Inject Write Dropper ───────────────────────────────────
    println!("\n--- Write Dropper ---");
    let dropper_config = FaultConfig {
        fault: Fault::WriteDropper { drop_ratio: 0.3 },
        duration: Duration::from_secs(120),
        description: "Drop 30% of writes".to_string(),
    };
    let guard_dropper = agent
        .inject(&dropper_config)
        .expect("Inject write dropper failed");
    println!(
        "  Injected write dropper (id={}, drop_ratio=0.3)",
        guard_dropper.id()
    );

    // Simulate checking if writes should be dropped
    let mut dropped = 0;
    let trials = 100;
    for _ in 0..trials {
        if agent.should_drop_write() {
            dropped += 1;
        }
    }
    println!("  Write drop test: {dropped}/{trials} dropped (~30% expected)");

    // ── 5. Read Corruption ────────────────────────────────────────
    println!("\n--- Read Corruption ---");
    let corrupt_config = FaultConfig {
        fault: Fault::ReadCorruption {
            corruption_ratio: 0.25,
        },
        duration: Duration::from_secs(60),
        description: "Simulate read corruption".to_string(),
    };
    let guard_corrupt = agent
        .inject(&corrupt_config)
        .expect("Inject read corruption failed");
    println!("  Injected read corruption (id={})", guard_corrupt.id());
    println!("  should_corrupt_read: {:?}", agent.should_corrupt_read());

    // ── 6. Network Partition ──────────────────────────────────────
    println!("\n--- Network Partition ---");
    let partition_config = FaultConfig {
        fault: Fault::NetworkPartition {
            isolated_nodes: vec![2, 5, 7],
        },
        duration: Duration::from_secs(60),
        description: "Isolate nodes 2, 5, 7".to_string(),
    };
    let guard_partition = agent
        .inject(&partition_config)
        .expect("Inject partition failed");
    println!(
        "  Injected network partition (id={}, nodes=[2,5,7])",
        guard_partition.id()
    );
    println!("  Node 2 partitioned: {}", agent.is_partitioned(2));
    println!("  Node 3 partitioned: {}", agent.is_partitioned(3));
    println!("  Node 5 partitioned: {}", agent.is_partitioned(5));

    // ── 7. Slow Disk ─────────────────────────────────────────────
    println!("\n--- Slow Disk ---");
    let slow_config = FaultConfig {
        fault: Fault::SlowDisk {
            latency: Duration::from_millis(100),
        },
        duration: Duration::from_secs(60),
        description: "100ms disk latency".to_string(),
    };
    let guard_slow = agent.inject(&slow_config).expect("Inject slow disk failed");
    println!(
        "  Injected slow disk (id={}, 100ms latency)",
        guard_slow.id()
    );

    // ── 8. Introspection ──────────────────────────────────────────
    println!("\n--- Active Injections ---");
    let active = agent.active_injections();
    println!("  {} active fault injections:", active.len());
    for info in &active {
        println!(
            "    id={}: {:?} — '{}' (remaining: {:?}, expired: {})",
            info.id,
            info.fault,
            info.description,
            info.remaining(),
            info.is_expired()
        );
    }

    // ── 9. Targeted Fault Query ───────────────────────────────────
    println!("\n--- Fault Queries ---");
    let has_latency = agent.is_fault_active(&|f| matches!(f, Fault::LatencySpike { .. }));
    let has_disk = agent.is_fault_active(&|f| matches!(f, Fault::DiskFull));
    let has_corruption = agent.is_fault_active(&|f| matches!(f, Fault::ReadCorruption { .. }));
    println!("  Has LatencySpike: {has_latency}");
    println!("  Has DiskFull:     {has_disk}");
    println!("  Has ReadCorruption: {has_corruption}");

    // ── 10. Manual Clear ──────────────────────────────────────────
    println!("\n--- Manual Clearing ---");
    let disk_id = guard_disk.id();
    agent.clear(disk_id).expect("Clear disk failed");
    println!(
        "  Cleared disk fault (id={disk_id}), is_disk_full: {}",
        agent.is_disk_full()
    );
    println!("  Active count: {}", agent.active_count());

    // ── 11. RAII Guard Cleanup ────────────────────────────────────
    println!("\n--- RAII Guard Cleanup ---");
    println!("  Active before drop: {}", agent.active_count());
    drop(guard_latency);
    println!("  After dropping latency guard: {}", agent.active_count());
    drop(guard_dropper);
    println!("  After dropping dropper guard: {}", agent.active_count());
    drop(guard_corrupt);
    drop(guard_partition);
    drop(guard_slow);
    println!("  After dropping all guards: {}", agent.active_count());
    println!("  has_active_faults: {}", agent.has_active_faults());

    // ── 12. Clear All ─────────────────────────────────────────────
    println!("\n--- Clear All ---");
    // Inject a few more for demonstration
    let _g1 = agent
        .inject(&FaultConfig {
            fault: Fault::DiskFull,
            duration: Duration::from_secs(60),
            description: "temp disk full".to_string(),
        })
        .expect("Inject failed");
    let _g2 = agent
        .inject(&FaultConfig {
            fault: Fault::SlowDisk {
                latency: Duration::from_millis(20),
            },
            duration: Duration::from_secs(60),
            description: "temp slow disk".to_string(),
        })
        .expect("Inject failed");
    println!("  Injected 2 more faults: active={}", agent.active_count());

    agent.clear_all();
    println!(
        "  After clear_all: active={}, has_active={}",
        agent.active_count(),
        agent.has_active_faults()
    );

    // ── 13. Validation ────────────────────────────────────────────
    println!("\n--- Config Validation ---");
    let bad_config = FaultConfig {
        fault: Fault::WriteDropper { drop_ratio: 1.5 }, // invalid: > 1.0
        duration: Duration::from_secs(60),
        description: "bad dropper".to_string(),
    };
    match bad_config.validate() {
        Ok(()) => println!("  Unexpectedly valid"),
        Err(e) => println!("  Invalid config caught: {e}"),
    }

    let bad_partition = FaultConfig {
        fault: Fault::NetworkPartition {
            isolated_nodes: vec![],
        }, // empty nodes
        duration: Duration::from_secs(60),
        description: "empty partition".to_string(),
    };
    match bad_partition.validate() {
        Ok(()) => println!("  Unexpectedly valid"),
        Err(e) => println!("  Invalid config caught: {e}"),
    }

    println!("\n✓ Chaos testing complete");
}
