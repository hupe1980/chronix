#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Authorization (Cedar RBAC)
//!
//! Demonstrates the embedded Cedar-based authorization engine:
//! creating policies, principals, resources, and evaluating
//! allow/deny decisions.
//!
//! ```sh
//! cargo run -p chronix --example authz
//! ```

use chronix::chronix_security::authz::{
    AuthzEngine, ChronixAction, ChronixPrincipal, ChronixResource,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = AuthzEngine::new();

    // ── 1. Load Cedar policies ─────────────────────────────────
    println!("─── 1. Loading Cedar policies ───");
    // Chronix uses Cedar entity types: Chronix::Role::"<role>",
    // Chronix::Action::"<action>", Chronix::Measurement::"<name>"
    let policies = r#"
        // Operators can read and write all measurements
        permit(
            principal in Chronix::Role::"operator",
            action in [Chronix::Action::"Read", Chronix::Action::"Write"],
            resource
        );

        // Analysts can only read
        permit(
            principal in Chronix::Role::"analyst",
            action == Chronix::Action::"Read",
            resource
        );

        // Admins can do everything
        permit(
            principal in Chronix::Role::"admin",
            action,
            resource
        );

        // Deny deletes on secret_metrics for non-admins
        forbid(
            principal,
            action == Chronix::Action::"Delete",
            resource == Chronix::Measurement::"secret_metrics"
        );
    "#;

    let loaded = engine.load_policies(policies)?;
    println!("   Loaded {loaded} policies\n");

    // ── 2. Define principals ───────────────────────────────────
    println!("─── 2. Principals ───");
    let alice = ChronixPrincipal::new("alice").with_role("operator");
    let bob = ChronixPrincipal::new("bob").with_role("analyst");
    let carol = ChronixPrincipal::new("carol").with_role("admin");

    println!("   alice: operator");
    println!("   bob  : analyst");
    println!("   carol: admin\n");

    // ── 3. Define resources ────────────────────────────────────
    println!("─── 3. Resources ───");
    let cpu = ChronixResource::measurement("cpu")
        .with_namespace("production")
        .with_tag("host", "web-1");

    let secret_metrics = ChronixResource::measurement("secret_metrics").with_namespace("internal");

    println!("   cpu (production, host=web-1)");
    println!("   secret_metrics (internal)\n");

    // ── 4. Evaluate access decisions ───────────────────────────
    println!("─── 4. Access decisions ───");

    let checks: &[(&str, &ChronixPrincipal, ChronixAction, &ChronixResource)] = &[
        ("alice Read cpu", &alice, ChronixAction::Read, &cpu),
        ("alice Write cpu", &alice, ChronixAction::Write, &cpu),
        ("alice Delete cpu", &alice, ChronixAction::Delete, &cpu),
        ("bob Read cpu", &bob, ChronixAction::Read, &cpu),
        ("bob Write cpu", &bob, ChronixAction::Write, &cpu),
        (
            "carol Delete secret",
            &carol,
            ChronixAction::Delete,
            &secret_metrics,
        ),
        (
            "carol Admin secret",
            &carol,
            ChronixAction::Admin,
            &secret_metrics,
        ),
        ("bob Forecast cpu", &bob, ChronixAction::Forecast, &cpu),
        (
            "alice Subscribe cpu",
            &alice,
            ChronixAction::Subscribe,
            &cpu,
        ),
    ];

    for &(label, principal, ref action, resource) in checks {
        let decision = engine.authorize(principal, *action, resource);
        let icon = if decision.is_allowed() { "✅" } else { "❌" };
        print!("   {icon} {label:<30}");
        if decision.is_denied() {
            println!(" DENIED");
        } else {
            println!(" ALLOWED");
        }
    }

    // ── 5. Dynamic policy management ───────────────────────────
    println!("\n─── 5. Dynamic policy update ───");
    println!("   Adding policy: analysts can forecast");
    engine.add_policy(
        "analyst_forecast",
        r#"permit(principal in Chronix::Role::"analyst", action == Chronix::Action::"Forecast", resource);"#,
    )?;

    let decision = engine.authorize(&bob, ChronixAction::Forecast, &cpu);
    println!(
        "   bob Forecast cpu: {}",
        if decision.is_allowed() {
            "ALLOWED ✅"
        } else {
            "DENIED ❌"
        }
    );

    println!("   Removing policy: analyst_forecast");
    engine.remove_policy("analyst_forecast");

    let decision = engine.authorize(&bob, ChronixAction::Forecast, &cpu);
    println!(
        "   bob Forecast cpu: {}",
        if decision.is_allowed() {
            "ALLOWED ✅"
        } else {
            "DENIED ❌"
        }
    );

    println!("\n✅ Done");
    Ok(())
}
