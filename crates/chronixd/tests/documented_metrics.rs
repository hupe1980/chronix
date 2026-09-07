#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Every metric name the documentation promises is one this tree emits.
//!
//! A metric name in a table, a runbook or a dashboard panel is a promise, and
//! it is the one kind of promise that cannot fail loudly: Prometheus answers
//! "no data" for a name nothing exports exactly as it does for a quiet one, so
//! an alert built on a misspelled metric is silently dead and a panel is
//! silently empty. Eleven documented names were in that state at once —
//! including `chronix_auth_failures_total`, which the security checklist tells
//! an operator to watch for brute-force attempts.
//!
//! This replaces `suite::dashboards`, which checked the bundled dashboards
//! the same way but counted a name as emitted if it appeared **anywhere** in a
//! `.rs` file — so a doc comment naming a metric satisfied it, and twelve
//! panels and tables passed while nothing exported what they queried. Here a
//! name is emitted only if it appears as a string literal in non-test source,
//! which is what a metrics macro, or a `const` fed to one, produces. The
//! dashboards are scanned alongside the prose, so nothing was lost.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repository root")
}

fn walk(root: &Path, ext: &str, f: &mut impl FnMut(&Path, &str)) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if path.is_dir() {
                // `target` is build output; `tests`, `benches` and `examples`
                // are not the server, and a metric named only there is not
                // emitted by anything an operator runs.
                if matches!(&*name, "target" | "tests" | "benches" | "examples") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == ext) {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    f(&path, &text);
                }
            }
        }
    }
}

/// Every `chronix_*` name that appears as a string literal in the server's
/// source — a metrics macro's argument, or a `const` fed to one.
fn emitted(root: &Path) -> BTreeSet<String> {
    let re = regex_lite_matches;
    let mut names = BTreeSet::new();
    walk(&root.join("crates"), "rs", &mut |_, text| {
        for line in text.lines() {
            let trimmed = line.trim_start();
            // A doc comment naming a metric is prose, not an emission.
            if trimmed.starts_with("//") {
                continue;
            }
            names.extend(re(line));
        }
    });
    names
}

/// `"chronix_…"` string literals on one line.
fn regex_lite_matches(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    while let Some(pos) = line[i..].find("\"chronix_") {
        let start = i + pos + 1;
        let mut end = start;
        while end < bytes.len()
            && (bytes[end].is_ascii_lowercase()
                || bytes[end].is_ascii_digit()
                || bytes[end] == b'_')
        {
            end += 1;
        }
        if end < bytes.len() && bytes[end] == b'"' {
            out.push(line[start..end].to_string());
        }
        i = end.max(start + 1);
    }
    out
}

/// Every `chronix_*` identifier the published documentation names.
fn documented(root: &Path) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut collect = |path: &Path, text: &str| {
        let origin = path
            .strip_prefix(root)
            .unwrap_or(path)
            .display()
            .to_string();
        let bytes = text.as_bytes();
        let mut i = 0;
        while let Some(pos) = text[i..].find("chronix_") {
            let start = i + pos;
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
                out.push((text[start..end].to_string(), origin.clone()));
            }
            i = end.max(start + 1);
        }
    };
    walk(&root.join("site").join("content"), "md", &mut collect);
    walk(&root.join("dashboards"), "json", &mut collect);
    let readme = root.join("README.md");
    if let Ok(text) = std::fs::read_to_string(&readme) {
        collect(&readme, &text);
    }
    out
}

/// Names that look like metrics but are not, each with a reason.
fn not_a_metric(name: &str) -> bool {
    // The workspace's own crate names, which are spelled with underscores in
    // Rust paths and appear throughout the prose.
    const CRATES: &[&str] = &[
        "chronix_analytics",
        "chronix_cluster",
        "chronix_core",
        "chronix_dsim",
        "chronix_encoding",
        "chronix_engine",
        "chronix_meta",
        "chronix_query",
        "chronix_security",
        "chronix_streaming",
    ];
    CRATES.contains(&name)
        // The Python package, and its module.
        || name.starts_with("chronix_client")
        // A bare prefix in prose ("every `chronix_` metric…").
        || name == "chronix_"
        // The Criterion bench target, named in `cargo bench --bench …`.
        || name == "chronix_bench"
}

/// Prometheus renders a histogram as three series.
fn resolves(name: &str, emitted: &BTreeSet<String>) -> bool {
    emitted.contains(name)
        || ["_bucket", "_count", "_sum"].iter().any(|suffix| {
            name.strip_suffix(suffix)
                .is_some_and(|base| emitted.contains(base))
        })
}

#[test]
fn every_documented_metric_is_one_the_tree_emits() {
    let root = repo_root();
    let emitted = emitted(&root);
    assert!(
        emitted.len() > 100,
        "the source scan found only {} metric names, so it is broken",
        emitted.len()
    );

    let mut problems: BTreeSet<String> = BTreeSet::new();
    let mut checked = 0usize;
    for (name, origin) in documented(&root) {
        if not_a_metric(&name) {
            continue;
        }
        checked += 1;
        if !resolves(&name, &emitted) {
            problems.insert(format!("{name}  ({origin})"));
        }
    }
    assert!(checked > 50, "only {checked} names were checked");

    assert!(
        problems.is_empty(),
        "the documentation promises {} metric name(s) nothing emits — an alert \
         built on one of these is silently dead:\n  {}",
        problems.len(),
        problems.into_iter().collect::<Vec<_>>().join("\n  ")
    );
}

/// Every metric this tree records is a name Prometheus will keep, inside the
/// `chronix_` namespace.
///
/// The exposition format allows `[a-zA-Z_:][a-zA-Z0-9_:]*`, and
/// `metrics-exporter-prometheus` rewrites anything else at render time —
/// silently, and only on the scrape. Three metrics were declared with **dots**
/// while 236 used underscores, so they reached an operator under a name that
/// was not the one in the source, and one of them
/// (`chronix.auth.env_key.grace_hit`) was written that way in the doc comment
/// telling operators to watch it. `documented_metrics` above could not see
/// this: it looks for `"chronix_…"` literals, which a dotted name is not.
///
/// The rewrite is proved rather than assumed, in
/// `chronixd --test metrics_exported::a_dotted_metric_name_is_rewritten_before_it_reaches_a_scrape`.
#[test]
fn every_metric_name_is_prometheus_safe() {
    let root = repo_root();
    let mut problems: Vec<String> = Vec::new();
    let mut checked = 0usize;

    walk(&root.join("crates"), "rs", &mut |path, text| {
        for (lineno, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            for name in metric_macro_literals(line) {
                checked += 1;
                // Two rules, and the second is why the first was not enough
                // on its own: `signal.condition.type_mismatch` was both
                // invalid *and* outside the namespace, so it rendered as
                // `signal_condition_type_mismatch` — a metric no operator
                // filtering on `chronix_` would ever see.
                let valid_identifier = !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
                    && !name.starts_with(|c: char| c.is_ascii_digit());
                if !valid_identifier || !name.starts_with("chronix_") {
                    problems.push(format!(
                        "{}:{}  {name}",
                        path.strip_prefix(&root).unwrap_or(path).display(),
                        lineno + 1,
                    ));
                }
            }
        }
    });

    assert!(
        checked > 100,
        "the macro scan found only {checked} metric names, so it is broken",
    );
    assert!(
        problems.is_empty(),
        "{} metric name(s) are not a valid Prometheus identifier under the \
         `chronix_` namespace. The exporter rewrites an invalid one silently, so \
         an operator greps the scrape for a name that is not in it; a valid one \
         outside the namespace is invisible to anyone filtering by prefix:\n  {}",
        problems.len(),
        problems.join("\n  "),
    );
}

/// The first string-literal argument of a `counter!`/`gauge!`/`histogram!`
/// call on this line, if it is a literal rather than a `const`.
///
/// A `const` fed to the macro is covered by the same rule at its definition,
/// which is itself a `"chronix_…"` literal the other scan sees.
fn metric_macro_literals(line: &str) -> Vec<String> {
    const MACROS: [&str; 3] = ["counter!(", "gauge!(", "histogram!("];
    let mut out = Vec::new();
    for m in MACROS {
        let mut from = 0usize;
        while let Some(pos) = line[from..].find(m) {
            let after = from + pos + m.len();
            from = after;
            let rest = line[after..].trim_start();
            let Some(rest) = rest.strip_prefix('"') else {
                continue; // a `const`, or a multi-line call
            };
            if let Some(end) = rest.find('"') {
                out.push(rest[..end].to_string());
            }
        }
    }
    out
}
