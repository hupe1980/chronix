//! The bundled Grafana dashboards must query metrics this build emits.
//!
//! A dashboard naming a metric nothing exports renders an empty panel with
//! no error anywhere: Prometheus answers "no data" for an unknown series
//! exactly as it does for a quiet one. So the failure is invisible from
//! both sides, and the only place it can be caught is here. Twenty-six of
//! the forty-six metrics the dashboards referenced did not exist.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// The repository root, from this crate's manifest directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repository root")
}

/// Every `chronix_*` identifier appearing in a file.
fn chronix_names(text: &str) -> HashSet<String> {
    let mut names = HashSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while let Some(pos) = text[i..].find("chronix_") {
        let start = i + pos;
        // Not a name if it continues a longer identifier to the left.
        let preceded =
            start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_');
        let mut end = start;
        while end < bytes.len()
            && (bytes[end].is_ascii_lowercase()
                || bytes[end].is_ascii_digit()
                || bytes[end] == b'_')
        {
            end += 1;
        }
        if !preceded {
            names.insert(text[start..end].to_string());
        }
        i = end.max(start + 1);
    }
    names
}

/// Every metric name the crates mention in a string literal.
fn emitted_metric_names(root: &Path) -> HashSet<String> {
    let mut names = HashSet::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    names.extend(chronix_names(&text));
                }
            }
        }
    }
    names
}

/// Prometheus exposes a histogram as `_bucket`, `_count` and `_sum`, so a
/// panel may legitimately name a suffix of a metric the code emits.
fn resolves(name: &str, emitted: &HashSet<String>) -> bool {
    if emitted.contains(name) {
        return true;
    }
    ["_bucket", "_count", "_sum"].iter().any(|suffix| {
        name.strip_suffix(suffix)
            .is_some_and(|b| emitted.contains(b))
    })
}

#[test]
fn every_dashboard_metric_is_one_the_server_emits() {
    let root = repo_root();
    let emitted = emitted_metric_names(&root);
    assert!(
        emitted.len() > 50,
        "the scan found only {} metric names, so it is not working",
        emitted.len()
    );

    let mut problems: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for entry in std::fs::read_dir(root.join("dashboards")).expect("dashboards directory") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        checked += 1;
        let text = std::fs::read_to_string(&path).expect("dashboard json");
        let file = path
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        for name in chronix_names(&text) {
            if !resolves(&name, &emitted) {
                problems.push(format!("{file}: {name}"));
            }
        }
    }

    assert!(checked > 0, "no dashboards were checked");
    problems.sort();
    assert!(
        problems.is_empty(),
        "these dashboard panels query metrics nothing emits, so they render \
         empty with no error anywhere:\n  {}",
        problems.join("\n  ")
    );
}
