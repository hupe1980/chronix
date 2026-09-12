#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Every audit category the server declares is one the server emits.
//!
//! `AuditAction` had twenty-five variants and the server constructed **four**:
//! `Delete`, `LoginFailure`, `KeyRotation` and `Admin`. `NamespaceDelete`,
//! `DataExport`, `PolicyLoad`, `ApiKeyCreate`, `SchemaChange`,
//! `TriggerCreate`, `DropRollup` — all of them existed as categories and were
//! produced by nothing, while the hardening checklist told an operator to
//! "review audit event coverage (26 built-in action types)".
//!
//! That is the same defect as the authorization model's, in a sibling enum,
//! and the same question finds it: **count what the subsystem is asked to
//! record and compare it to the vocabulary it offers.** A category nothing
//! emits is not coverage; it is a gap in the shape of one — worse here than
//! in most places, because the *absence* of an event is what an auditor reads
//! as "it did not happen".
//!
//! The enum is the inventory; this walks it.

use chronix_security::audit::AuditAction;

/// Where an action may legitimately be constructed.
///
/// `chronixd` does not log one event per *successful* data request — that
/// would make the trail a second request log and bury what it exists for. It
/// logs the **refusals**, on every protocol, plus the operations that cannot
/// be undone. An embedded consumer with an erasure obligation records its own
/// successes through the public `AuditLogger`, which is what
/// `examples/audit_logging.rs` shows.
const SOURCES: &[(&str, &str)] = &[
    ("chronixd", include_str!("../src/auth.rs")),
    ("chronixd", include_str!("../src/admin.rs")),
    ("chronixd", include_str!("../src/namespace.rs")),
    ("chronixd", include_str!("../src/server.rs")),
    ("chronixd", include_str!("../src/http/management.rs")),
    ("chronixd", include_str!("../src/http/triggers.rs")),
    ("chronix", include_str!("../../chronix/src/pipeline.rs")),
    (
        "example",
        include_str!("../../chronix/examples/audit_logging.rs"),
    ),
];

#[test]
fn every_audit_action_has_a_producer() {
    let mut unproduced = Vec::new();
    for action in AuditAction::ALL {
        let needle = format!("AuditAction::{action:?}");
        let produced = SOURCES.iter().any(|(_, src)| src.contains(&needle));
        if !produced {
            unproduced.push(format!("{action:?}"));
        }
    }
    assert!(
        unproduced.is_empty(),
        "audit categories nothing constructs — each is a gap in the shape of \
         coverage:\n  {}",
        unproduced.join("\n  ")
    );
}

/// The trail's whole purpose is the events nobody watches, and a refusal is
/// the first of them. `Deny` must appear.
#[test]
fn a_refusal_is_recorded_somewhere() {
    let denials = SOURCES
        .iter()
        .filter(|(origin, _)| *origin == "chronixd")
        .filter(|(_, src)| src.contains("AuditDecision::Deny"))
        .count();
    assert!(
        denials >= 2,
        "only {denials} server modules record a denial; a trail that holds \
         only successes answers the one question it exists for with silence"
    );
}
