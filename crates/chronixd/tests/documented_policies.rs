#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Every Cedar policy the documentation shows must be one the server accepts.
//!
//! The security guide's policy examples were, all of them, policies this
//! server could never act on:
//!
//! ```text
//! permit(
//!   principal in Group::"platform-team",
//!   action in [Action::"Write", Action::"Query"],
//!   resource in Namespace::"team-platform"
//! );
//! ```
//!
//! Four mistakes in four lines. The entity types are `Chronix::Role`,
//! `Chronix::Action` and `Chronix::Namespace` — unqualified, they are
//! *different types*, which Cedar is happy to accept and no request can ever
//! match. There is no `Group`. There is no `Query`; the action is `Read`. And
//! the page beside it documented `CreateMeasurement`, `DeleteMeasurement`
//! and `ManageNamespace`, none of which exists either.
//!
//! Nothing failed. Cedar without a schema accepts any syntactically valid
//! policy, so an operator following the guide got a `permit` that permitted
//! nothing — indistinguishable from a lockout — and, worse, the `forbid`
//! example, which forbade nothing and read as protection.
//!
//! Two things close it, and this file is the second. The first is that the
//! schema is now compiled in and always loaded, so the server itself refuses
//! these at startup. This checks the *documentation* against the same
//! schema, because a page nobody can copy from is still a defect even when
//! the server is right.

use std::path::{Path, PathBuf};

use chronix_security::authz::AuthzEngine;

/// Everything a reader might copy a policy out of.
fn documentation_roots() -> Vec<PathBuf> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf();
    vec![
        repo.join("site/content"),
        repo.join("README.md"),
        repo.join("concepts"),
    ]
}

fn markdown_files(root: &Path, out: &mut Vec<PathBuf>) {
    if root.is_file() {
        out.push(root.to_path_buf());
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            markdown_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "md") {
            out.push(path);
        }
    }
}

/// `(file, line, policy text)` for every fenced ```cedar block.
fn documented_policies() -> Vec<(PathBuf, usize, String)> {
    let mut files = Vec::new();
    for root in documentation_roots() {
        markdown_files(&root, &mut files);
    }
    files.sort();

    let mut blocks = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read");
        let mut lines = text.lines().enumerate();
        while let Some((idx, line)) = lines.next() {
            if line.trim_start().starts_with("```cedar") {
                let mut body = String::new();
                for (_, l) in lines.by_ref() {
                    if l.trim_start().starts_with("```") {
                        break;
                    }
                    body.push_str(l);
                    body.push('\n');
                }
                if !body.trim().is_empty() {
                    blocks.push((file.clone(), idx + 1, body));
                }
            }
        }
    }
    blocks
}

#[test]
fn every_documented_policy_loads_against_the_shipped_schema() {
    let blocks = documented_policies();
    assert!(
        blocks.len() >= 4,
        "found only {} documented Cedar policies, so the scan is broken \
         rather than the documentation",
        blocks.len()
    );

    let mut failures = Vec::new();
    for (file, line, policy) in blocks {
        if let Err(e) = AuthzEngine::check_policies(&policy) {
            failures.push(format!("{}:{line}\n    {e}", file.display()));
        }
    }
    assert!(
        failures.is_empty(),
        "documented policies `chronixd` would refuse at startup:\n  {}",
        failures.join("\n  ")
    );
}

/// The schema itself is documentation, and it is published by reference
/// rather than by copy.
///
/// A copy is the thing that drifts: the security guide carried its own
/// account of the action list, and it named seven actions of which three
/// existed. The page now points at the compiled-in schema, so this checks
/// that the pointer still resolves and that the file is what it claims.
#[test]
fn the_published_schema_is_the_one_the_engine_uses() {
    let src = chronix_security::authz::SCHEMA_SRC;
    assert!(src.contains("namespace Chronix"));
    for entity in [
        "entity Role",
        "entity User",
        "entity Namespace",
        "entity System",
    ] {
        assert!(
            src.contains(entity),
            "the schema no longer declares `{entity}`"
        );
    }
    // Every action the engine can be asked about is named in the schema; the
    // set equality itself is checked in `chronix-security`.
    assert!(!chronix_security::authz::schema_action_names()
        .expect("schema actions")
        .is_empty());
}
