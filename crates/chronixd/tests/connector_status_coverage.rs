#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Every state a connector can report is a state something produces.
//!
//! `ConnectorStatus` has five variants and only `Stopped` and `Running` were
//! ever constructed — both from the flags that record whether `start()` was
//! *called*. `Idle`, `Reconnecting` and `Failed(String)` came from nowhere,
//! so a connector whose task had died reported `Running` with zero messages.
//!
//! Same question `audit_coverage.rs` asks of `AuditAction`, in a sibling
//! enum: count the vocabulary a subsystem offers against what it utters.
//!
//! The enum is the inventory; this walks it.

use std::path::{Path, PathBuf};

/// Every `ConnectorStatus` variant, spelled as the source spells it.
///
/// Written out rather than derived: the point is to fail when somebody adds
/// a sixth variant and reports it from nowhere, and a list derived from the
/// enum would grow silently with it.
const VARIANTS: &[&str] = &["Stopped", "Running", "Idle", "Reconnecting", "Failed"];

/// Files that may construct a status, and what each is.
const PRODUCERS: &[&str] = &["src/kafka.rs", "src/mqtt.rs", "src/connector.rs"];

fn crate_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

/// The producing sources, with their `#[cfg(test)]` modules removed.
///
/// A variant constructed only by a mock in a test module is exactly the
/// defect this test is for: `connector.rs`'s own mock connector reported
/// `Running` and `Stopped` long before either was true of a real one.
fn production_source() -> String {
    let mut all = String::new();
    for rel in PRODUCERS {
        let body = std::fs::read_to_string(crate_dir().join(rel))
            .unwrap_or_else(|e| panic!("cannot read {rel}: {e}"));
        let body = match body.find("\n#[cfg(test)]\nmod tests {") {
            Some(at) => body[..at].to_string(),
            None => body,
        };
        all.push_str(&body);
        all.push('\n');
    }
    all
}

#[test]
fn every_connector_status_variant_is_produced_by_a_connector() {
    let source = production_source();

    let unused: Vec<&str> = VARIANTS
        .iter()
        .copied()
        .filter(|v| !source.contains(&format!("ConnectorStatus::{v}")))
        .collect();

    assert!(
        unused.is_empty(),
        "ConnectorStatus variant(s) {unused:?} are declared and constructed by no \
         connector outside a test module.\n\
         A state nothing reports is a gap in the shape of observability: `Failed` \
         carries a reason string, and while nothing wrote one, a connector that \
         could not reach its broker reported `Running` for ever.\n\
         Either report the state from the code that reaches it, or delete the variant."
    );
}

#[test]
fn a_connector_that_cannot_connect_does_not_report_running() {
    // The narrow claim, as a string search rather than a live broker: the
    // status must not be computed from the `running` flag alone, which is
    // what made a dead task indistinguishable from a working one.
    for rel in ["src/kafka.rs", "src/mqtt.rs"] {
        let body = std::fs::read_to_string(crate_dir().join(rel)).unwrap();
        let status = body
            .split("async fn status(&self) -> ConnectorStatus {")
            .nth(1)
            .unwrap_or_else(|| panic!("{rel} has no status() implementation"));
        let status = &status[..status.find("\n    }").expect("a closing brace")];

        assert!(
            status.contains("self.state"),
            "{rel}'s status() does not read the observed state.\n\
             Computing it from `running`/`stopped` reports the caller's intent: \
             `start()` was called, so `Running` — whether or not the task behind \
             it is alive."
        );
        assert!(
            !status.contains("ConnectorStatus::Running"),
            "{rel}'s status() names Running directly; only the task that has \
             actually connected should produce it."
        );
    }
}
