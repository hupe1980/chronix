#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code may unwrap
//! Every route the server mounts is described by the OpenAPI document, and
//! every path the document describes is a route.
//!
//! The document is the API's published inventory: the Python SDK checks its
//! own call sites against it, and it is what a user generates a client from.
//! It was hand-maintained beside the router and had drifted to describing
//! roughly half of it — including omitting the entire Prometheus surface at
//! the paths a Grafana datasource derives. Nothing failed; the document was
//! simply quietly incomplete, which is the failure mode of every inventory
//! kept in two places.
//!
//! The router's side is read out of `server.rs` because axum does not expose
//! its route table. That is a source scan, and it is deliberately strict: a
//! `.route(` whose path is not a literal will not be seen, so it must not
//! exist.

use std::collections::BTreeSet;

const SERVER_SRC: &str = include_str!("../src/server.rs");

/// Paths mounted by `build_router`, with `nest` prefixes applied.
fn mounted_paths() -> BTreeSet<String> {
    let body = {
        let start = SERVER_SRC
            .find("pub fn build_router(")
            .expect("build_router");
        &SERVER_SRC[start..]
    };

    let mut out = BTreeSet::new();
    // (prefix, brace depth at which the nest block opened)
    let mut nests: Vec<(String, i32)> = Vec::new();
    let mut depth = 0i32;
    for (idx, ch) in body.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                while nests.last().is_some_and(|(_, d)| *d > depth) {
                    nests.pop();
                }
                if depth <= 0 {
                    break;
                }
            }
            '.' => {
                let rest = &body[idx..];
                if let Some(path) = literal_after(rest, ".route(") {
                    let prefix: String = nests.iter().map(|(p, _)| p.as_str()).collect();
                    let full = if path == "/" && !prefix.is_empty() {
                        prefix
                    } else {
                        format!("{prefix}{path}")
                    };
                    out.insert(full);
                } else if let Some(prefix) = literal_after(rest, ".nest(") {
                    // The block opens on the next `{`; record the depth it
                    // will be at so the prefix pops with it.
                    nests.push((prefix.to_string(), depth + 1));
                }
            }
            _ => {}
        }
    }
    out
}

/// The string literal that follows `call` at the start of `rest`, if any.
fn literal_after<'a>(rest: &'a str, call: &str) -> Option<&'a str> {
    let after = rest.strip_prefix(call)?;
    let after = after.trim_start();
    let after = after.strip_prefix('"')?;
    let end = after.find('"')?;
    Some(&after[..end])
}

fn documented_paths() -> BTreeSet<String> {
    let spec = chronixd::openapi::spec_json("http://localhost:8086");
    let value: serde_json::Value = serde_json::from_str(&spec).expect("valid JSON");
    value["paths"]
        .as_object()
        .expect("a paths object")
        .keys()
        .cloned()
        .collect()
}

/// Routes that exist but are deliberately not published, each with a reason.
///
/// The list is short on purpose: an exemption is a decision, and one without a
/// reason beside it is an omission wearing a costume.
fn undocumented_by_design(path: &str) -> Option<&'static str> {
    Some(match path {
        // Kubernetes probe aliases of `/health` and `/ready`. Documenting the
        // alias as well as the canonical path doubles the surface a generated
        // client offers with no new capability.
        "/healthz" | "/readyz" => "alias of /health and /ready",
        // Feature-gated on `cluster` / `chaos`, which the published binary
        // does not build. A document that describes routes the server does
        // not serve is worse than one that omits them.
        p if p.starts_with("/api/v1/admin/chaos") => "requires --features chaos",
        "/api/v1/admin/nodes"
        | "/api/v1/admin/nodes/{id}/decommission"
        | "/api/v1/admin/heartbeat"
        | "/api/v1/admin/regions"
        | "/api/v1/admin/regions/{id}/state" => "requires --features cluster",
        _ => return None,
    })
}

#[test]
fn every_route_is_documented() {
    let mounted = mounted_paths();
    assert!(
        mounted.len() > 40,
        "the source scan found only {} routes, so it is broken rather than the router",
        mounted.len()
    );
    let documented = documented_paths();

    let missing: Vec<&String> = mounted
        .iter()
        .filter(|p| !documented.contains(*p) && undocumented_by_design(p).is_none())
        .collect();
    assert!(
        missing.is_empty(),
        "routes the OpenAPI document does not describe: {missing:#?}"
    );
}

#[test]
fn every_documented_path_is_a_route() {
    let mounted = mounted_paths();
    let documented = documented_paths();

    let phantom: Vec<&String> = documented
        .iter()
        .filter(|p| !mounted.contains(*p))
        .collect();
    assert!(
        phantom.is_empty(),
        "the OpenAPI document describes paths the server does not serve: {phantom:#?}"
    );
}
