#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Authorization (Cedar)
//!
//! The two decisions `chronixd` puts to Cedar, driven directly: may this
//! principal touch this tenant's data, and may it administer the server.
//!
//! ```sh
//! cargo run -p chronix --example authz
//! ```

use chronix::chronix_security::authz::{
    AuthzEngine, ChronixAction, ChronixNamespace, ChronixPrincipal,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = AuthzEngine::new();

    // ── 1. Policies ────────────────────────────────────────────
    println!("─── 1. Loading Cedar policies ───");
    // Every entity type is namespaced under `Chronix::`. `Action::"Read"` is
    // a *different type* and would match nothing — which is why the schema is
    // compiled in and every policy is validated against it, rather than
    // accepted and never consulted.
    let policies = r#"
        // Operators read and write production.
        permit(
            principal in Chronix::Role::"operator",
            action in [Chronix::Action::"Read", Chronix::Action::"Write"],
            resource == Chronix::Namespace::"production"
        );

        // Analysts read, anywhere.
        permit(
            principal in Chronix::Role::"analyst",
            action == Chronix::Action::"Read",
            resource
        );

        // Nobody deletes the internal namespace.
        forbid(
            principal,
            action == Chronix::Action::"Delete",
            resource == Chronix::Namespace::"internal"
        );

        // One administrative capability, and only that one. `Admin` is an
        // action *group*: `action in Chronix::Action::"Admin"` would grant
        // every capability at once.
        permit(
            principal == Chronix::User::"backup-runner",
            action == Chronix::Action::"ManageBackups",
            resource
        );
    "#;
    println!("   Loaded {} policies\n", engine.load_policies(policies)?);

    // A typo is refused rather than accepted and never matched.
    let typo = r#"permit(principal, action == Chronix::Action::"Query", resource);"#;
    match AuthzEngine::check_policies(typo) {
        Ok(_) => println!("   (unreachable: a bad action was accepted)"),
        Err(e) => println!("   A misspelt action is refused at load:\n     {e}\n"),
    }

    // ── 2. Principals ──────────────────────────────────────────
    println!("─── 2. Principals ───");
    let alice = ChronixPrincipal::new("alice").with_role("operator");
    let bob = ChronixPrincipal::new("bob").with_role("analyst");
    let backup = ChronixPrincipal::new("backup-runner");
    println!("   alice: operator");
    println!("   bob  : analyst");
    println!("   backup-runner: no role, named directly\n");

    // ── 3. Data-plane decisions ────────────────────────────────
    println!("─── 3. Namespace decisions ───");
    let production = ChronixNamespace::new("production");
    let internal = ChronixNamespace::new("internal");

    let checks: &[(&str, &ChronixPrincipal, ChronixAction, &ChronixNamespace)] = &[
        (
            "alice Read production",
            &alice,
            ChronixAction::Read,
            &production,
        ),
        (
            "alice Write production",
            &alice,
            ChronixAction::Write,
            &production,
        ),
        (
            "alice Write internal",
            &alice,
            ChronixAction::Write,
            &internal,
        ),
        ("bob Read internal", &bob, ChronixAction::Read, &internal),
        (
            "bob Write production",
            &bob,
            ChronixAction::Write,
            &production,
        ),
        (
            "bob Delete internal",
            &bob,
            ChronixAction::Delete,
            &internal,
        ),
    ];
    for &(label, principal, action, namespace) in checks {
        let decision = engine.authorize_namespace(principal, action, namespace);
        let icon = if decision.is_allowed() { "✅" } else { "❌" };
        let verdict = if decision.is_allowed() {
            "ALLOWED"
        } else {
            "DENIED"
        };
        println!("   {icon} {label:<26} {verdict}");
    }

    // ── 4. Control-plane decisions ─────────────────────────────
    println!("\n─── 4. Administrative decisions ───");
    for action in [ChronixAction::ManageBackups, ChronixAction::ManageKeys] {
        let decision = engine.authorize_system(&backup, action);
        let icon = if decision.is_allowed() { "✅" } else { "❌" };
        let verdict = if decision.is_allowed() {
            "ALLOWED"
        } else {
            "DENIED"
        };
        println!("   {icon} backup-runner {action:<16} {verdict}");
    }
    println!("   (least privilege: the capability granted, and nothing else)");

    // ── 5. Policies change; the active set is swapped atomically ─
    println!("\n─── 5. Adding and removing a policy ───");
    engine.add_policy(
        "analyst_writes_staging",
        r#"permit(principal in Chronix::Role::"analyst",
                  action == Chronix::Action::"Write",
                  resource == Chronix::Namespace::"staging");"#,
    )?;
    let staging = ChronixNamespace::new("staging");
    println!(
        "   bob Write staging: {}",
        verdict(&engine, &bob, ChronixAction::Write, &staging)
    );
    engine.remove_policy("analyst_writes_staging");
    println!(
        "   bob Write staging: {} (policy removed)",
        verdict(&engine, &bob, ChronixAction::Write, &staging)
    );

    println!("\n   Active policies: {}", engine.policy_count());
    Ok(())
}

fn verdict(
    engine: &AuthzEngine,
    principal: &ChronixPrincipal,
    action: ChronixAction,
    namespace: &ChronixNamespace,
) -> &'static str {
    if engine
        .authorize_namespace(principal, action, namespace)
        .is_allowed()
    {
        "ALLOWED ✅"
    } else {
        "DENIED ❌"
    }
}
