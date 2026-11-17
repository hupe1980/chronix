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
fn sources() -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    // Every chronixd source file, read at run time: this was a list of six,
    // and a producer in a seventh (`http/detectors.rs`) was invisible.
    let mut out = Vec::new();
    let mut stack = vec![root.join("src")];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d)
            .expect("readable source dir")
            .flatten()
        {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(std::fs::read_to_string(&path).expect("readable source"));
            }
        }
    }
    out.push(include_str!("../../chronix/src/pipeline.rs").to_string());
    out.push(include_str!("../../chronix/examples/audit_logging.rs").to_string());
    out
}

#[test]
fn every_audit_action_has_a_producer() {
    let sources = sources();
    let mut unproduced = Vec::new();
    for action in AuditAction::ALL {
        let needle = format!("AuditAction::{action:?}");
        let produced = sources.iter().any(|src| src.contains(&needle));
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
    // The last two sources are the embedded crate and its example.
    let all = sources();
    let denials = all[..all.len() - 2]
        .iter()
        .filter(|src| src.contains("AuditDecision::Deny"))
        .count();
    assert!(
        denials >= 2,
        "only {denials} server modules record a denial; a trail that holds \
         only successes answers the one question it exists for with silence"
    );
}
