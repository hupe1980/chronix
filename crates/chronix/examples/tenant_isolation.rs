#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Multi-Tenant Isolation
//!
//! Demonstrates namespace management, quota enforcement,
//! and resource usage tracking for multi-tenant deployments.
//!
//! ```bash
//! cargo run --example tenant_isolation
//! ```

use chronix::chronix_core::{NamespaceId, NamespaceQuota, NamespaceUsage};
use chronix_security::tenant::{NamespaceRegistry, QuotaEnforcer};

fn main() {
    println!("=== Chronix Multi-Tenant Isolation ===\n");

    // ── 1. Create Namespace Registry ──────────────────────────────
    println!("--- Namespace Registry ---");
    let registry = NamespaceRegistry::new();
    println!(
        "Registry created with default namespace (count={})",
        registry.count()
    );

    // ── 2. Create Namespaces ──────────────────────────────────────
    println!("\n--- Creating Namespaces ---");

    let team_a_id = NamespaceId::new("team-alpha").expect("Invalid namespace ID");
    let team_a_quota = NamespaceQuota {
        max_series_count: 10_000,
        max_ingestion_rate: 50_000,
        max_storage_bytes: 1_073_741_824, // 1 GB
        max_measurements: 100,
        max_request_rps: 0,
        max_request_burst: 0,
    };
    let team_a = registry
        .create_namespace(
            team_a_id,
            "Alpha team metrics",
            "alice@example.com",
            team_a_quota,
        )
        .expect("Failed to create team-alpha");
    println!(
        "Created: {} (owner: {}, max_series: {})",
        team_a.id, team_a.owner, team_a.quota.max_series_count
    );

    let team_b_id = NamespaceId::new("team-beta").expect("Invalid namespace ID");
    let team_b_quota = NamespaceQuota {
        max_series_count: 5_000,
        max_ingestion_rate: 20_000,
        max_storage_bytes: 536_870_912, // 512 MB
        max_measurements: 50,
        max_request_rps: 0,
        max_request_burst: 0,
    };
    let team_b = registry
        .create_namespace(
            team_b_id,
            "Beta team metrics",
            "bob@example.com",
            team_b_quota,
        )
        .expect("Failed to create team-beta");
    println!(
        "Created: {} (owner: {}, max_series: {})",
        team_b.id, team_b.owner, team_b.quota.max_series_count
    );

    let staging_id = NamespaceId::new("staging").expect("Invalid namespace ID");
    let staging_quota = NamespaceQuota {
        max_series_count: 1_000,
        max_ingestion_rate: 5_000,
        max_storage_bytes: 104_857_600, // 100 MB
        max_measurements: 20,
        max_request_rps: 0,
        max_request_burst: 0,
    };
    registry
        .create_namespace(
            staging_id,
            "Staging environment",
            "ops@example.com",
            staging_quota,
        )
        .expect("Failed to create staging");
    println!("Created: staging");

    // ── 3. List Namespaces ────────────────────────────────────────
    println!("\n--- All Namespaces ---");
    let names = registry.list_names();
    println!("Registered namespaces ({}):", names.len());
    for name in &names {
        let state = registry.get(name).expect("Namespace not found");
        println!(
            "  {:<15} owner={:<25} series_limit={:<8} storage_limit={} bytes",
            name,
            state.info.owner,
            state.info.quota.max_series_count,
            state.info.quota.max_storage_bytes,
        );
    }

    // ── 4. Simulate Usage ─────────────────────────────────────────
    println!("\n--- Simulating Usage ---");

    // Team Alpha: moderate usage
    let alpha_usage = NamespaceUsage {
        series_count: 3_500,
        ingestion_rate: 15_000.0,
        storage_bytes: 400_000_000,
        measurements: 25,
        ingestion_rate_window_s: 0,
    };
    registry
        .update_usage("team-alpha", alpha_usage)
        .expect("Failed to update usage");
    println!("  team-alpha: 3,500 series, 400 MB storage");

    // Team Beta: near limits
    let beta_usage = NamespaceUsage {
        series_count: 4_800,
        ingestion_rate: 19_000.0,
        storage_bytes: 500_000_000,
        measurements: 48,
        ingestion_rate_window_s: 0,
    };
    registry
        .update_usage("team-beta", beta_usage)
        .expect("Failed to update usage");
    println!("  team-beta:  4,800 series, 500 MB storage (near limits!)");

    // Increment series atomically
    registry
        .increment_series("team-alpha", 100)
        .expect("Failed to increment");
    println!("  team-alpha: incremented by 100 series");

    // ── 5. Usage Ratios ───────────────────────────────────────────
    println!("\n--- Usage Ratios ---");
    let ratios = registry.usage_ratios();
    for ratio in &ratios {
        let pct = ratio.ratio * 100.0;
        let warning = if pct > 90.0 {
            " ⚠ CRITICAL"
        } else if pct > 75.0 {
            " ⚡ WARNING"
        } else {
            ""
        };
        println!(
            "  {:<15} {:<20} {}/{} ({:.1}%){warning}",
            ratio.namespace, ratio.resource, ratio.current, ratio.limit, pct
        );
    }

    // ── 6. Quota Enforcement ──────────────────────────────────────
    println!("\n--- Quota Enforcement ---");
    let enforcer = QuotaEnforcer::new(&registry);

    // Team Alpha: should succeed (well under limits)
    #[allow(deprecated)]
    match enforcer.check_write("team-alpha", 100, 5000) {
        Ok(()) => println!("  team-alpha: write 100 series, 5000 points → ALLOWED"),
        Err(e) => println!("  team-alpha: write → DENIED: {e}"),
    }

    // Team Beta: should fail (would exceed series limit)
    #[allow(deprecated)]
    match enforcer.check_write("team-beta", 300, 1000) {
        Ok(()) => println!("  team-beta: write 300 series → ALLOWED"),
        Err(e) => println!("  team-beta: write 300 series → DENIED: {e}"),
    }

    // Team Beta: check measurement creation
    match enforcer.check_create_measurement("team-beta") {
        Ok(()) => println!("  team-beta: create measurement → ALLOWED"),
        Err(e) => println!("  team-beta: create measurement → DENIED: {e}"),
    }

    // Staging: normal write
    #[allow(deprecated)]
    match enforcer.check_write("staging", 10, 100) {
        Ok(()) => println!("  staging: write 10 series → ALLOWED"),
        Err(e) => println!("  staging: write → DENIED: {e}"),
    }

    // ── 7. Update Quota ───────────────────────────────────────────
    println!("\n--- Updating Quotas ---");
    let new_beta_quota = NamespaceQuota {
        max_series_count: 10_000, // doubled
        max_ingestion_rate: 40_000,
        max_storage_bytes: 1_073_741_824,
        max_measurements: 100,
        max_request_rps: 0,
        max_request_burst: 0,
    };
    registry
        .update_quota("team-beta", new_beta_quota)
        .expect("Failed to update quota");
    println!("  team-beta quota doubled: max_series=10,000");

    // Now the same write should succeed
    #[allow(deprecated)]
    match enforcer.check_write("team-beta", 300, 1000) {
        Ok(()) => println!("  team-beta: write 300 series → ALLOWED (after quota increase)"),
        Err(e) => println!("  team-beta: write → DENIED: {e}"),
    }

    // ── 8. Delete Namespace ───────────────────────────────────────
    println!("\n--- Deleting Namespace ---");
    let deleted = registry
        .delete_namespace("staging")
        .expect("Failed to delete staging");
    println!("Deleted '{}' (owner: {})", deleted.id, deleted.owner);
    println!(
        "Remaining namespaces: {} ({:?})",
        registry.count(),
        registry.list_names()
    );

    // Verify deleted namespace is gone
    assert!(!registry.exists("staging"));
    println!("Verified: 'staging' no longer exists");

    println!("\n✓ Multi-tenant isolation complete");
}
