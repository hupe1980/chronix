#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Authorization reaches **every** route, or this file says which one it does
//! not.
//!
//! The pass that wrote these tests found Cedar wired into two decisions and
//! modelled for twenty. Policies validated, were loaded, were reported in the
//! startup log — and nine granular administrative capabilities existed, were
//! unit tested, and were requested by nothing, because every admin route
//! asked for `Admin`. gRPC and Flight SQL had no gate at all: they do not
//! pass through the axum middleware the gate lives in. And a configured
//! engine *replaced* the credential's `admin` capability rather than joining
//! it, so a permissive policy file widened what an ordinary key could reach.
//!
//! The gate also returned early when `SharedState::namespace_registry` was
//! `None` — a branch the product cannot produce, and one **every test in
//! this tree took**, because they all set `None`. So the branch the suite
//! exercised was the one that skips the gate. That is why this file builds
//! its state with a real registry, and why the field is no longer an
//! `Option`.
//!
//! None of that is visible from a test that authorizes one request. The test
//! that sees it walks the router: **under a policy set that permits nothing,
//! every route must refuse.** A route that answers `200` is a route no policy
//! can govern, and it fails here rather than in somebody's deployment.

use std::net::SocketAddr;
use std::sync::Arc;

use reqwest::StatusCode;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronix_security::authz::{AuthzEngine, ChronixAction};
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

use crate::route_scan;

const ADMIN_KEY: &str = "admin-key-for-authz-tests";

/// A server with authentication, an `admin` credential, and `policies`.
///
/// The credential is marked `admin` on purpose: it satisfies the *capability*
/// half of every administrative gate, so whatever these tests observe is the
/// **policy** half. A test that used a non-admin key would pass with Cedar
/// deleted.
async fn start_server(policies: &str, roles: Vec<String>) -> (String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());

    let auth_config = chronixd::config::AuthConfig {
        api_keys: vec![chronixd::config::ApiKeyEntry {
            name: "tester".to_string(),
            key: ADMIN_KEY.to_string(),
            namespaces: Vec::new(),
            admin: true,
            roles,
        }],
        jwt: None,
        exempt_paths: vec!["/health".to_string()],
    };
    let auth_state = chronixd::auth::AuthState::from_config(&auth_config).expect("auth state");

    let engine = AuthzEngine::new();
    if !policies.trim().is_empty() {
        engine.load_policies(policies).expect("policies load");
    }

    let state: AppState = Arc::new(SharedState {
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: Some(auth_state.clone()),
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: Some(Arc::new(engine)),
        audit_logger: None,
        config: chronixd::config::ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
    });

    let metrics_handle = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder()
        .handle();
    let app = build_router(
        state,
        "/metrics",
        metrics_handle,
        10 * 1024 * 1024,
        Some(auth_state),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base, tmp)
}

fn client() -> reqwest::Client {
    chronixd::tls::ensure_crypto_provider();
    reqwest::Client::new()
}

/// A concrete request path, with `{param}` filled in.
fn concrete(path: &str) -> String {
    path.replace("{name}", "cpu")
        .replace("{measurement}", "cpu")
        .replace("{id}", "1")
}

/// Routes the gate deliberately does not cover, each with its reason.
///
/// Kept short, and kept *here* rather than in the middleware, so adding one
/// is a decision somebody writes down. `is_control_plane` names the same set
/// in the server; `the_exemptions_match_the_servers_own` compares them.
fn ungated_by_design(path: &str) -> Option<&'static str> {
    Some(match path {
        "/health" | "/healthz" | "/ready" | "/readyz" => {
            "a liveness probe must not fail because a policy file changed"
        }
        "/metrics" => "the scrape is server-level and carries no tenant rows",
        "/api/v1/openapi.json" => "the API's own description, identical for every caller",
        _ => return None,
    })
}

/// **The test this whole design exists for.**
///
/// Every mounted route, driven under a policy set that permits nothing. A
/// `200` means that route reaches the database with no decision made about
/// it. Four did before this ran, and one of them was every write surface.
#[tokio::test]
async fn every_route_refuses_under_a_deny_all_policy() {
    let (base, _tmp) = start_server("", Vec::new()).await;
    let mut reachable: Vec<String> = Vec::new();

    for path in route_scan::mounted_paths() {
        if ungated_by_design(&path).is_some() {
            continue;
        }
        let url = format!("{base}{}", concrete(&path));
        // The router answers `405` for a method a path does not take, so try
        // each in turn and judge the first one that is not a method error.
        let mut verdict = None;
        for method in [
            reqwest::Method::GET,
            reqwest::Method::POST,
            reqwest::Method::PUT,
            reqwest::Method::DELETE,
        ] {
            let resp = client()
                .request(method.clone(), &url)
                .header("authorization", format!("Bearer {ADMIN_KEY}"))
                .header("content-type", "application/json")
                .body("{}")
                .send()
                .await
                .expect("request");
            if resp.status() != StatusCode::METHOD_NOT_ALLOWED {
                verdict = Some((method, resp.status()));
                break;
            }
        }
        let Some((method, status)) = verdict else {
            panic!("no method reached {path}; the route scan is wrong, not the router");
        };
        if status != StatusCode::FORBIDDEN {
            reachable.push(format!("{method} {path} → {status}"));
        }
    }

    assert!(
        reachable.is_empty(),
        "these routes answered with no policy permitting anything:\n  {}",
        reachable.join("\n  ")
    );
}

/// **Every data route is classified, and the classification is the route's
/// rather than the method's.**
///
/// A method mapping is wrong in this server in both directions, and a live
/// drive is what showed it. `POST /api/v1/chronix/sql` is a *read* — as are
/// `POST /api/v1/query` and `/query_range`, which is how Grafana sends PromQL
/// by default — so a read-only policy could not read, and granting a
/// datasource what it needed meant granting `Write`. In the other direction
/// `POST /api/v1/delete` and `/delete_batch` are *deletes*, so a
/// `forbid Delete` policy — the example this project's own guide shows —
/// forbade nothing, because any principal that could write could delete.
#[test]
fn every_data_route_is_classified() {
    let methods = [
        axum::http::Method::GET,
        axum::http::Method::POST,
        axum::http::Method::PUT,
        axum::http::Method::DELETE,
    ];
    let mut unclassified = Vec::new();
    for path in route_scan::mounted_paths() {
        if chronixd::namespace::is_control_plane(&path) {
            continue;
        }
        for method in &methods {
            if chronixd::namespace::action_for_route(&path, method).is_none() {
                unclassified.push(format!("{method} {path}"));
                break;
            }
        }
    }
    assert!(
        unclassified.is_empty(),
        "data routes with no authorization classification — each is refused \
         outright, and each is a decision somebody has to make:\n  {}",
        unclassified.join("\n  ")
    );
}

/// The two directions the method mapping got wrong, pinned as facts about
/// the routes rather than as a property of `POST`.
#[test]
fn a_posted_read_asks_to_read_and_a_posted_delete_asks_to_delete() {
    use axum::http::Method;

    for read in [
        "/api/v1/chronix/sql",
        "/api/v1/chronix/query",
        "/api/v1/query",
        "/api/v1/query_range",
        "/api/v1/series",
        "/api/v1/labels",
    ] {
        assert_eq!(
            chronixd::namespace::action_for_route(read, &Method::POST),
            Some(ChronixAction::Read),
            "{read} is a read whichever method carries it"
        );
    }
    for delete in ["/api/v1/delete", "/api/v1/delete_batch"] {
        assert_eq!(
            chronixd::namespace::action_for_route(delete, &Method::POST),
            Some(ChronixAction::Delete),
            "{delete} deletes data; a policy forbidding Delete must reach it"
        );
    }
    // And the method still decides where it *is* the effect.
    assert_eq!(
        chronixd::namespace::action_for_route("/api/v1/rollups", &Method::GET),
        Some(ChronixAction::Read)
    );
    assert_eq!(
        chronixd::namespace::action_for_route("/api/v1/rollups", &Method::POST),
        Some(ChronixAction::Write)
    );
}

/// A read-only policy reads — through the surface Grafana actually uses.
#[tokio::test]
async fn a_read_only_policy_can_use_the_query_apis() {
    let (base, _tmp) = start_server(
        r#"permit(principal in Chronix::Role::"reader",
                  action == Chronix::Action::"Read",
                  resource == Chronix::Namespace::"default");"#,
        vec!["reader".to_string()],
    )
    .await;

    for (path, body) in [
        ("/api/v1/chronix/sql", json!({"query": "SELECT 1"})),
        ("/api/v1/chronix/query", json!({"measurement": "cpu"})),
    ] {
        let resp = client()
            .post(format!("{base}{path}"))
            .header("authorization", format!("Bearer {ADMIN_KEY}"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "{path} is a read and must be reachable with a Read policy"
        );
    }

    // Grafana's own shape: a form body on the Prometheus instant query.
    let resp = client()
        .post(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("query=up")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // And it still cannot delete, which is the other half.
    let resp = client()
        .post(format!("{base}/api/v1/delete"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .json(&json!({"measurement": "cpu"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

/// A policy granting `Write` must not thereby grant `Delete`.
#[tokio::test]
async fn a_write_policy_does_not_grant_a_delete() {
    let (base, _tmp) = start_server(
        r#"permit(principal in Chronix::Role::"ingest",
                  action in [Chronix::Action::"Read", Chronix::Action::"Write"],
                  resource == Chronix::Namespace::"default");"#,
        vec!["ingest".to_string()],
    )
    .await;

    let write = client()
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .json(&json!([{"measurement": "cpu", "fields": {"usage": 1.0}}]))
        .send()
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::NO_CONTENT, "Write is permitted");

    for path in ["/api/v1/delete", "/api/v1/delete_batch"] {
        let resp = client()
            .post(format!("{base}{path}"))
            .header("authorization", format!("Bearer {ADMIN_KEY}"))
            .json(&json!({"measurement": "cpu"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "{path} deletes data and must need Delete"
        );
    }
}

/// The exemption list here and the predicate in the server are one decision.
#[test]
fn the_exemptions_match_the_servers_own() {
    for path in route_scan::mounted_paths() {
        let exempt_here = ungated_by_design(&path).is_some();
        let control_plane = chronixd::namespace::is_control_plane(&path);
        if exempt_here {
            assert!(
                control_plane,
                "{path} is exempt in this test but the server gates it as data"
            );
        }
        if control_plane && !exempt_here {
            assert!(
                path.starts_with("/api/v1/admin") || path.starts_with("/api/v1/namespaces"),
                "{path} is control plane in the server but neither admin nor exempt here"
            );
        }
    }
}

/// **A namespace-bound administrative key still reaches the admin routes.**
///
/// `validate_tenancy` requires every key to name its namespaces under
/// multi-tenancy, administrative keys included — and the data gate ran on
/// every path, so a key bound to `tenant-a` was refused on every
/// administrative endpoint unless it *also* listed `default`. That is the
/// namespace an administrative request does not have, and nothing told an
/// operator to add it: the request is about the server, not about a tenant.
#[tokio::test]
async fn a_namespace_bound_admin_key_reaches_the_admin_routes() {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());

    let auth_config = chronixd::config::AuthConfig {
        api_keys: vec![chronixd::config::ApiKeyEntry {
            name: "tenant-admin".to_string(),
            key: ADMIN_KEY.to_string(),
            // Bound to one tenant, and deliberately *not* to `default`.
            namespaces: vec!["tenant-a".to_string()],
            admin: true,
            roles: Vec::new(),
        }],
        jwt: None,
        exempt_paths: vec![],
    };
    let auth_state = chronixd::auth::AuthState::from_config(&auth_config).expect("auth state");

    let state: AppState = Arc::new(SharedState {
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: Some(auth_state.clone()),
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        // No engine: this is about the credential's namespace binding, which
        // is checked whether or not policies are configured.
        authz_engine: None,
        audit_logger: None,
        config: chronixd::config::ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
    });
    let metrics_handle = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder()
        .handle();
    let app = build_router(
        state,
        "/metrics",
        metrics_handle,
        1024 * 1024,
        Some(auth_state),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("addr");
    let base = format!("http://{addr}");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let resp = client()
        .get(format!("{base}/api/v1/admin/auth/keys"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .send()
        .await
        .unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "an administrative request is about the server, not about a namespace"
    );

    // The data plane still binds, which is the half that must not loosen.
    let data = client()
        .get(format!("{base}/api/v1/measurements"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        data.status(),
        StatusCode::FORBIDDEN,
        "a key bound to tenant-a must not read the default namespace"
    );
}

/// A read policy permits reads and nothing else — on the *data* plane only.
#[tokio::test]
async fn a_read_policy_permits_reads_and_refuses_writes() {
    let (base, _tmp) = start_server(
        r#"permit(principal in Chronix::Role::"reader",
                  action == Chronix::Action::"Read",
                  resource == Chronix::Namespace::"default");"#,
        vec!["reader".to_string()],
    )
    .await;

    let read = client()
        .get(format!("{base}/api/v1/measurements"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::OK, "Read was permitted");

    let write = client()
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .json(&json!([{"measurement": "cpu", "fields": {"v": 1.0}}]))
        .send()
        .await
        .unwrap();
    assert_eq!(
        write.status(),
        StatusCode::FORBIDDEN,
        "Write was not permitted"
    );
}

/// **The roles have to come from the credential.**
///
/// The namespace gate built a bare principal with no roles, so this exact
/// policy — the shape every policy in the security guide has — matched
/// nothing on a data request. An API key could not carry a role at all.
#[tokio::test]
async fn a_role_on_an_api_key_matches_a_role_policy() {
    let with_role = start_server(
        r#"permit(principal in Chronix::Role::"reader",
                  action == Chronix::Action::"Read", resource);"#,
        vec!["reader".to_string()],
    )
    .await;
    let resp = client()
        .get(format!("{}/api/v1/measurements", with_role.0))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Same policy, same key, no role: the negative half, so the test above
    // cannot pass because everything is permitted.
    let without_role = start_server(
        r#"permit(principal in Chronix::Role::"reader",
                  action == Chronix::Action::"Read", resource);"#,
        Vec::new(),
    )
    .await;
    let resp = client()
        .get(format!("{}/api/v1/measurements", without_role.0))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

/// Least privilege on the control plane: one capability is one capability.
#[tokio::test]
async fn a_capability_grants_its_own_routes_and_no_others() {
    let (base, _tmp) = start_server(
        r#"permit(principal in Chronix::Role::"backup_operator",
                  action == Chronix::Action::"ManageBackups", resource);"#,
        vec!["backup_operator".to_string()],
    )
    .await;

    // Backups: permitted. A bad path is a 400 from the handler, not a 403 —
    // what matters is that the gate let it through.
    let backup = client()
        .post(format!("{base}/api/v1/admin/backup/verify"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .json(&json!({"path": "does-not-exist"}))
        .send()
        .await
        .unwrap();
    assert_ne!(
        backup.status(),
        StatusCode::FORBIDDEN,
        "ManageBackups must reach the backup routes"
    );

    // Keys: the same credential, a different capability.
    let keys = client()
        .get(format!("{base}/api/v1/admin/auth/keys"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        keys.status(),
        StatusCode::FORBIDDEN,
        "a backup credential must not manage API keys"
    );
}

/// The `Admin` group is how an operator grants everything at once.
#[tokio::test]
async fn the_admin_group_reaches_every_administrative_route() {
    let (base, _tmp) = start_server(
        r#"permit(principal in Chronix::Role::"root",
                  action in Chronix::Action::"Admin", resource);"#,
        vec!["root".to_string()],
    )
    .await;

    for path in ["/api/v1/admin/auth/keys", "/api/v1/namespaces"] {
        let resp = client()
            .get(format!("{base}{path}"))
            .header("authorization", format!("Bearer {ADMIN_KEY}"))
            .send()
            .await
            .unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "the Admin group should cover {path}"
        );
    }
}

/// A policy engine can only ever narrow what a credential carries.
///
/// It used to replace the capability check rather than join it, so pointing
/// `authz_policy_dir` at a permissive file *widened* what a non-admin key
/// could do.
#[tokio::test]
async fn a_permissive_policy_does_not_grant_the_admin_capability() {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());

    let auth_config = chronixd::config::AuthConfig {
        api_keys: vec![chronixd::config::ApiKeyEntry {
            name: "ordinary".to_string(),
            key: ADMIN_KEY.to_string(),
            namespaces: Vec::new(),
            admin: false, // ← the whole point
            roles: vec!["root".to_string()],
        }],
        jwt: None,
        exempt_paths: vec![],
    };
    let auth_state = chronixd::auth::AuthState::from_config(&auth_config).expect("auth state");
    let engine = AuthzEngine::new();
    engine
        .load_policies(r#"permit(principal, action, resource);"#)
        .expect("policies");

    let state: AppState = Arc::new(SharedState {
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: Some(auth_state.clone()),
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: Some(Arc::new(engine)),
        audit_logger: None,
        config: chronixd::config::ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
    });
    let metrics_handle = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder()
        .handle();
    let app = build_router(
        state,
        "/metrics",
        metrics_handle,
        1024 * 1024,
        Some(auth_state),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("addr");
    let base = format!("http://{addr}");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let resp = client()
        .get(format!("{base}/api/v1/admin/auth/keys"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "a `permit(principal, action, resource)` must not confer the admin capability"
    );

    // The data plane, though, is exactly what that policy grants.
    let read = client()
        .get(format!("{base}/api/v1/measurements"))
        .header("authorization", format!("Bearer {ADMIN_KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::OK);
}

/// Every action `ChronixAction` can name is asked for by some route.
///
/// The reverse of the schema's own inventory check: that one proves the
/// schema and the enum agree, this proves the enum and the *server* do. An
/// action nothing asks for is a policy clause that silently never fires,
/// which is what nine of the twelve were.
#[test]
fn every_action_is_issued_by_some_surface() {
    const SERVER: &str = include_str!("../../src/server.rs");
    const NAMESPACE: &str = include_str!("../../src/namespace.rs");
    const GRPC: &str = include_str!("../../src/grpc.rs");
    const FLIGHT: &str = include_str!("../../src/flight.rs");

    let mut unissued = Vec::new();
    for action in ChronixAction::ALL {
        let needle = format!("ChronixAction::{}", action.name());
        let issued = [SERVER, NAMESPACE, GRPC, FLIGHT]
            .iter()
            .any(|src| src.contains(&needle));
        // The cluster capabilities are behind `--features cluster`; their
        // route groups are in `server.rs` either way, so the scan sees them.
        if !issued {
            unissued.push(action.name());
        }
    }
    assert!(
        unissued.is_empty(),
        "actions no request path ever asks for: {unissued:?}"
    );
}
