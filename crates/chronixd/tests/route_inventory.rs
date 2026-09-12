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

#[path = "suite/route_scan.rs"]
mod route_scan;

use route_scan::mounted_paths;

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
        // Feature-gated on `cluster`, which the published binary
        // does not build. A document that describes routes the server does
        // not serve is worse than one that omits them.
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
