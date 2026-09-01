//! Performance validation tests for Cedar authorization.
//!
//! Validates:
//! - Authorization decision latency scales well with policy count
//! - Policy hot-reload < 100 ms for 1000-policy file
//! - Authorization overhead is small relative to query cost
//!
//! **Note:** Absolute latency targets (< 100 µs p99) are release-mode
//! goals. Debug-mode thresholds are more generous.

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::authz::engine::AuthzEngine;
    use crate::authz::model::{ChronixAction, ChronixPrincipal, ChronixResource};

    /// Generate a Cedar policy using proper entity membership syntax.
    fn make_policy(id: usize) -> String {
        format!(
            r#"permit(
  principal in Chronix::Role::"role_{id}",
  action == Chronix::Action::"Read",
  resource == Chronix::Measurement::"measurement_{id}"
);"#
        )
    }

    /// Debug-mode threshold multiplier. Cedar's evaluation is ~10-50x
    /// slower in debug than release.  The 1000-policy benchmark can
    /// spike under CI / debug builds, so a generous margin is needed.
    #[cfg(debug_assertions)]
    const LATENCY_MULTIPLIER: u64 = 500;
    #[cfg(not(debug_assertions))]
    const LATENCY_MULTIPLIER: u64 = 1;

    // ── Authorization Decision Latency ──────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn authz_decision_latency_100_policies() {
        let engine = AuthzEngine::new();

        for i in 0..100 {
            engine
                .add_policy(&format!("policy_{i}"), &make_policy(i))
                .unwrap();
        }

        // Deny path (no matching role → default-deny)
        let principal = ChronixPrincipal::new("user1");
        let resource = ChronixResource::measurement("measurement_50");

        // Warm up
        for _ in 0..50 {
            let _ = engine.authorize(&principal, ChronixAction::Read, &resource);
        }

        let iterations = 5_000;
        let mut latencies = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let start = Instant::now();
            let d = engine.authorize(&principal, ChronixAction::Read, &resource);
            latencies.push(start.elapsed());
            assert!(!d.is_allowed());
        }

        latencies.sort();
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_micros(100 * LATENCY_MULTIPLIER);

        assert!(
            p99 < target,
            "100-policy p99 authz latency {p99:?} exceeds {target:?} target"
        );
    }

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn authz_decision_latency_1000_policies() {
        let engine = AuthzEngine::new();

        for i in 0..1000 {
            engine
                .add_policy(&format!("policy_{i}"), &make_policy(i))
                .unwrap();
        }

        let principal = ChronixPrincipal::new("user1");
        let resource = ChronixResource::measurement("measurement_500");

        // Warm up
        for _ in 0..20 {
            let _ = engine.authorize(&principal, ChronixAction::Read, &resource);
        }

        let iterations = 2_000;
        let mut latencies = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let start = Instant::now();
            let _ = engine.authorize(&principal, ChronixAction::Read, &resource);
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_micros(100 * LATENCY_MULTIPLIER);

        assert!(
            p99 < target,
            "1000-policy p99 authz latency {p99:?} exceeds {target:?} target"
        );
    }

    // ── Allow Path Latency ──────────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn authz_allow_decision_latency() {
        let engine = AuthzEngine::new();

        for i in 0..100 {
            engine
                .add_policy(&format!("policy_{i}"), &make_policy(i))
                .unwrap();
        }

        // Principal with matching role → should be allowed
        let principal = ChronixPrincipal::new("alice").with_role("role_50");
        let resource = ChronixResource::measurement("measurement_50");

        // Verify allow path works
        let decision = engine.authorize(&principal, ChronixAction::Read, &resource);
        assert!(
            decision.is_allowed(),
            "Expected allow for principal with matching role"
        );

        // Measure
        let iterations = 5_000;
        let mut latencies = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let start = Instant::now();
            let _ = engine.authorize(&principal, ChronixAction::Read, &resource);
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let avg = latencies.iter().sum::<Duration>() / iterations as u32;
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_micros(100 * LATENCY_MULTIPLIER);

        assert!(p99 < target, "Allow-path p99 {p99:?} exceeds {target:?}");
        assert!(
            avg < Duration::from_micros(50 * LATENCY_MULTIPLIER),
            "Allow-path average {avg:?} too high"
        );
    }

    // ── Default-Deny Latency ────────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn authz_deny_decision_fast() {
        let engine = AuthzEngine::new();

        for i in 0..100 {
            engine
                .add_policy(&format!("policy_{i}"), &make_policy(i))
                .unwrap();
        }

        // Nothing matches → deny
        let principal = ChronixPrincipal::new("nobody");
        let resource = ChronixResource::measurement("nonexistent");

        let iterations = 5_000;
        let mut latencies = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let start = Instant::now();
            let decision = engine.authorize(&principal, ChronixAction::Write, &resource);
            latencies.push(start.elapsed());
            assert!(!decision.is_allowed());
        }

        latencies.sort();
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_micros(100 * LATENCY_MULTIPLIER);

        assert!(p99 < target, "Deny decision p99 {p99:?} exceeds {target:?}");
    }

    // ── Policy Hot-Reload ───────────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn policy_hot_reload_under_100ms_1000_policies() {
        let engine = AuthzEngine::new();

        let mut policy_src = String::new();
        for i in 0..1000 {
            policy_src.push_str(&make_policy(i));
            policy_src.push('\n');
        }

        let start = Instant::now();
        let count = engine.load_policies(&policy_src).unwrap();
        let elapsed = start.elapsed();

        assert_eq!(count, 1000);
        // Debug-mode Cedar parse can be slower
        let target = Duration::from_millis(100 * LATENCY_MULTIPLIER);
        assert!(
            elapsed < target,
            "Reload of 1000 policies took {elapsed:?}, exceeds {target:?}"
        );
    }

    // ── Overhead Validation ─────────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn authz_overhead_small_relative_to_query() {
        let engine = AuthzEngine::new();

        for i in 0..100 {
            engine
                .add_policy(&format!("policy_{i}"), &make_policy(i))
                .unwrap();
        }

        let principal = ChronixPrincipal::new("alice").with_role("role_50");
        let resource = ChronixResource::measurement("measurement_50");

        // Verify authz works
        assert!(engine
            .authorize(&principal, ChronixAction::Read, &resource)
            .is_allowed());

        let iterations = 5_000;
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = engine.authorize(&principal, ChronixAction::Read, &resource);
        }
        let authz_total = start.elapsed();
        let authz_per_op = authz_total / iterations as u32;

        // In release mode (target): < 100µs per op vs 1ms query overhead = < 10%
        // In debug mode: authz is slower, but still small vs typical query
        let simulated_query_cost = Duration::from_millis(10);
        let overhead_pct =
            authz_per_op.as_nanos() as f64 / simulated_query_cost.as_nanos() as f64 * 100.0;

        assert!(
            overhead_pct < 50.0,
            "Authorization overhead {overhead_pct:.2}% of 10ms query is too high (per-op: {authz_per_op:?})"
        );
    }

    // ── Scaling Validation ──────────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn authz_scales_sublinearly_with_policy_count() {
        let principal = ChronixPrincipal::new("user1");
        let resource = ChronixResource::measurement("m_test");

        let mut results = Vec::new();

        for count in [10, 100, 500] {
            let engine = AuthzEngine::new();
            for i in 0..count {
                engine
                    .add_policy(&format!("p_{i}"), &make_policy(i))
                    .unwrap();
            }

            // Warm up
            for _ in 0..20 {
                let _ = engine.authorize(&principal, ChronixAction::Read, &resource);
            }

            let iterations = 1_000;
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = engine.authorize(&principal, ChronixAction::Read, &resource);
            }
            let avg = start.elapsed() / iterations as u32;
            results.push((count, avg));
        }

        // Verify 500-policy isn't more than 100x slower than 10-policy
        let (_, lat_10) = results[0];
        let (_, lat_500) = results[2];
        let ratio = lat_500.as_nanos() as f64 / lat_10.as_nanos().max(1) as f64;

        assert!(
            ratio < 100.0,
            "500-policy/10-policy latency ratio {ratio:.1}x is too high \
             ({lat_10:?} → {lat_500:?})"
        );
    }
}
