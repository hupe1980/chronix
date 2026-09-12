#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Every route that touches tenant data resolves a namespace.
//!
//! Authorization and **scoping** are different questions, and only the first
//! is answered by a middleware. `every_route_refuses_under_a_deny_all_policy`
//! proves no route escapes the gate; it says nothing about whether the
//! handler behind the gate filters the rows it returns, or confines the rows
//! it deletes. That is per-handler work, and a handler that forgets it reads
//! every tenant — which is the property this tree has broken most often:
//! *two of five write paths tagged the point and one of seven read paths
//! filtered on it*.
//!
//! `tenancy_test` drives the surfaces that existed when it was written. This
//! asks the question of the router, so a route added tomorrow is covered the
//! day it is mounted — the same argument the deny-all walk is built on, for
//! the other half of the property.
//!
//! The check is a source scan: a data handler must either name
//! `NamespaceContext` (so it can resolve a scope) or be one of the
//! exemptions below, each with its reason.

use std::collections::BTreeMap;

use crate::route_scan;

const SERVER_SRC: &str = include_str!("../../src/server.rs");

/// Every module a route handler can live in.
const HANDLER_SOURCES: &[(&str, &str)] = &[
    ("management", include_str!("../../src/http/management.rs")),
    ("query", include_str!("../../src/http/query.rs")),
    ("write", include_str!("../../src/http/write.rs")),
    ("prom", include_str!("../../src/http/prom.rs")),
    ("streaming", include_str!("../../src/http/streaming.rs")),
    ("triggers", include_str!("../../src/http/triggers.rs")),
    ("prometheus", include_str!("../../src/wire/prometheus.rs")),
    ("otlp", include_str!("../../src/wire/otlp.rs")),
    ("openapi", include_str!("../../src/openapi.rs")),
];

/// Handlers that need no namespace, each with the reason.
///
/// Short on purpose: an exemption is a decision, and one without a reason
/// beside it is an omission wearing a costume.
fn needs_no_namespace(handler: &str) -> Option<&'static str> {
    Some(match handler {
        "health_handler" | "ready_handler" => "a probe reads no tenant data",
        "openapi_handler" => "the API's own description, identical for every caller",
        "prom_buildinfo_handler" => "the server's version",
        "prom_empty_rules_handler"
        | "prom_empty_alerts_handler"
        | "prom_empty_exemplars_handler" => {
            "a constant empty Prometheus envelope; chronix has no rules engine"
        }
        "export_dashboards_handler" => "the bundled dashboard JSON, which is not data",
        "update_log_level_handler" => "a process-wide setting, behind ManageConfig",
        "list_connectors_handler" => {
            "connector configuration, which is the operator's not a tenant's"
        }
        _ => return None,
    })
}

/// `path -> handler function name`, read out of the router.
fn route_handlers() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut rest = SERVER_SRC;
    while let Some(at) = rest.find(".route(") {
        rest = &rest[at + ".route(".len()..];
        let Some(path) = quoted(rest) else { continue };
        // Every `name_handler` mentioned before the next `.route(`.
        let end = rest.find(".route(").unwrap_or(rest.len());
        let segment = &rest[..end];
        if let Some(handler) = segment
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .find(|w| w.ends_with("_handler"))
        {
            out.insert(path.to_string(), handler.to_string());
        }
    }
    out
}

fn quoted(s: &str) -> Option<&str> {
    let s = s.trim_start().strip_prefix('"')?;
    let end = s.find('"')?;
    Some(&s[..end])
}

/// The body of `pub async fn <name>(` up to its opening brace.
fn handler_signature(name: &str) -> Option<&'static str> {
    let needle = format!("pub async fn {name}(");
    for (_, src) in HANDLER_SOURCES {
        if let Some(at) = src.find(&needle) {
            let rest = &src[at..];
            let end = rest.find(" {")?;
            return Some(&rest[..end]);
        }
    }
    None
}

#[test]
fn every_data_route_resolves_a_namespace() {
    let handlers = route_handlers();
    assert!(
        handlers.len() > 30,
        "the source scan found only {} handlers, so it is broken rather than \
         the router",
        handlers.len()
    );

    let mut unscoped = Vec::new();
    for path in route_scan::mounted_paths() {
        if chronixd::namespace::is_control_plane(&path) {
            continue;
        }
        let Some(handler) = handlers.get(&path) else {
            continue;
        };
        if needs_no_namespace(handler).is_some() {
            continue;
        }
        let Some(sig) = handler_signature(handler) else {
            // A handler the scan cannot find is a hole in the scan, and a
            // hole in a guard is worse than no guard.
            unscoped.push(format!("{path} → {handler} (signature not found)"));
            continue;
        };
        if !sig.contains("NamespaceContext") {
            unscoped.push(format!("{path} → {handler}"));
        }
    }

    assert!(
        unscoped.is_empty(),
        "routes whose handler cannot resolve a namespace — each reads or writes \
         across every tenant, and each is a decision somebody has to make:\n  {}",
        unscoped.join("\n  ")
    );
}
