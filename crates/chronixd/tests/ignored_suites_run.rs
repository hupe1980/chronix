#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! A suite that is `#[ignore]`d everywhere is a suite that does not run.
//!
//! `kafka_integration.rs` and `mqtt_integration.rs` are the only tests that
//! drive the connectors against a real broker, and all of them are
//! `#[ignore]`d because they need Docker. That is the right call — `cargo
//! test` must not require a daemon — but it is a claim that something else
//! runs them, and nothing checked it: the CI job for the connectors
//! reported `ok. 0 passed; 5 ignored`.
//!
//! So: an integration suite that ignores its tests must be named by a
//! workflow line passing `--ignored`.
//!
//! Scoped to `tests/`. An `#[ignore]`d wall-clock assertion in a `src/perf`
//! module is a benchmark for an idle machine; running those in CI would
//! measure the runner.

use std::path::{Path, PathBuf};

/// The repository root, from this crate's manifest directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/<name> has a grandparent")
        .to_path_buf()
}

/// Every line of every workflow, joined — a `run:` may wrap.
fn workflow_text() -> String {
    let dir = repo_root().join(".github/workflows");
    let mut all = String::new();
    for entry in std::fs::read_dir(&dir).expect("the workflows directory exists") {
        let path = entry.expect("a readable directory entry").path();
        if path.extension().is_some_and(|e| e == "yml" || e == "yaml") {
            all.push_str(&std::fs::read_to_string(&path).expect("a readable workflow"));
            all.push('\n');
        }
    }
    assert!(
        !all.is_empty(),
        "no workflows found under {}",
        dir.display()
    );
    all
}

#[test]
fn an_integration_suite_that_ignores_its_tests_is_run_with_ignored_somewhere() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let workflows = workflow_text();

    let mut checked = 0;
    for entry in std::fs::read_dir(&tests_dir).expect("the tests directory exists") {
        let path = entry.expect("a readable directory entry").path();
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let body = std::fs::read_to_string(&path).expect("a readable test file");

        // `#[ignore]` on a test, not the word in a doc comment.
        let ignores = body
            .lines()
            .filter(|l| l.trim_start().starts_with("#[ignore"))
            .count();
        if ignores == 0 {
            continue;
        }

        let target = path
            .file_stem()
            .expect("a file name")
            .to_str()
            .expect("utf-8")
            .to_string();

        // The workflow must both name this target and pass `--ignored`.
        let runs_it = workflows
            .lines()
            .any(|line| line.contains(&format!("--test {target}")) && line.contains("--ignored"));

        assert!(
            runs_it,
            "tests/{target}.rs has {ignores} #[ignore]d test(s) and no workflow line runs \
             `--test {target} -- --ignored`.\n\
             An ignored suite is a claim that something else runs it. Either add the line \
             to .github/workflows/ci.yml, or remove the #[ignore] if the test no longer \
             needs what it was avoiding."
        );
        checked += 1;
    }

    assert!(
        checked >= 2,
        "expected to check at least the kafka and mqtt suites, checked {checked} — \
         has the #[ignore] spelling or the tests/ layout changed?"
    );
}
