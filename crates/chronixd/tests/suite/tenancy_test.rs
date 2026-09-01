#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Namespace isolation across *every* server entry point.
//!
//! The invariant these tests pin: a point written through any ingestion
//! surface carries the namespace of the request that wrote it, and a query
//! issued through any read surface sees exactly the namespaces it is entitled
//! to — no more (a cross-tenant read) and no less (data that silently
//! disappears because one writer forgot the tag).
//!
//! Both halves had failed. Two of five write paths tagged the point and one of
//! seven read paths filtered on it, so OTLP and Prometheus remote-write data
//! was invisible to `/api/v1/query`, and `/api/v1/sql` read every tenant's
//! rows.

use std::net::SocketAddr;
use std::sync::Arc;

use reqwest::StatusCode;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

/// Start a server whose namespace registry knows `tenant-a` and `tenant-b`,
/// with `multi_tenancy` on.
async fn start_multi_tenant_server() -> (String, TempDir) {
    start_server_with(true).await
}

/// The ordinary single-tenant deployment: no namespace stamping, no scoping.
async fn start_server() -> (String, TempDir) {
    start_server_with(false).await
}

async fn start_server_with(multi_tenancy: bool) -> (String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());

    let registry = chronix_security::tenant::NamespaceRegistry::new();
    for ns in ["tenant-a", "tenant-b"] {
        registry
            .create_namespace(
                chronix_core::NamespaceId::new(ns).unwrap(),
                "test",
                "test",
                chronix_core::NamespaceQuota::default(),
            )
            .expect("create namespace");
    }

    let state: AppState = Arc::new(SharedState {
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: None,
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: Some(Arc::new(registry)),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        #[cfg(feature = "chaos")]
        chaos_agent: None,
        authz_engine: None,
        audit_logger: None,
        config: chronixd::config::ServerConfig {
            multi_tenancy,
            ..Default::default()
        },
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        #[cfg(feature = "chaos")]
        chaos_guards: parking_lot::Mutex::new(Vec::new()),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        openapi_json: std::sync::OnceLock::new(),
    });

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base, tmp)
}

/// A client, with the rustls provider installed.
///
/// These suites never call `server::run`, so nothing else installs it — and
/// `reqwest` panics inside `Client::builder().build()` rather than returning
/// an error when it is missing.
fn client() -> reqwest::Client {
    chronixd::tls::ensure_crypto_provider();
    reqwest::Client::new()
}

/// Write one JSON point into `ns`.
async fn write_json(base: &str, ns: &str, measurement: &str, host: &str, value: f64, ts: i64) {
    let resp = client()
        .post(format!("{base}/api/v1/write"))
        .header("X-Namespace", ns)
        .json(&json!({
            "measurement": measurement,
            "tags": {"host": host},
            "fields": {"value": value},
            "timestamp": ts
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NO_CONTENT,
        "write to {ns} failed"
    );
}

/// Rows returned by `/api/v1/query` for `measurement` in `ns`.
async fn query_json(base: &str, ns: &str, measurement: &str) -> Vec<Value> {
    let resp = client()
        .post(format!("{base}/api/v1/query"))
        .header("X-Namespace", ns)
        .json(&json!({ "measurement": measurement }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "query in {ns} failed");
    resp.json().await.unwrap()
}

// ── Ingestion surfaces must tag ────────────────────────────────────────

/// OTLP is one of the two protocols an operator is most likely to point at a
/// new time-series database, and its points were reaching storage with no
/// namespace tag — so `/api/v1/query`, which filters on that tag, returned
/// nothing for them. The data was in the database and unreachable through the
/// database's own query API.
#[tokio::test]
async fn otlp_written_points_are_visible_to_the_query_api() {
    let (base, _tmp) = start_server().await;

    let body = json!({
        "resourceMetrics": [{
            "resource": {"attributes": [{"key": "host", "value": {"stringValue": "srv1"}}]},
            "scopeMetrics": [{
                "metrics": [{
                    "name": "power_watts",
                    "gauge": {"dataPoints": [{
                        "timeUnixNano": "1700000000000000000",
                        "asDouble": 42.0
                    }]}
                }]
            }]
        }]
    });

    let resp = client()
        .post(format!("{base}/api/v1/otlp/metrics"))
        .header("X-Namespace", "tenant-a")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::OK, "OTLP write failed: {text}");

    let rows = query_json(&base, "tenant-a", "power_watts").await;
    assert_eq!(
        rows.len(),
        1,
        "OTLP-written data must be queryable in the namespace that wrote it"
    );
}

/// The same hole, through the other standard ingestion protocol.
#[tokio::test]
async fn influx_written_points_are_visible_in_their_namespace() {
    let (base, _tmp) = start_multi_tenant_server().await;

    let resp = client()
        .post(format!("{base}/api/v1/write/influx"))
        .header("X-Namespace", "tenant-a")
        .body("mem,host=srv1 used=1024 1700000000000000000\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    assert_eq!(query_json(&base, "tenant-a", "mem").await.len(), 1);
    assert_eq!(
        query_json(&base, "tenant-b", "mem").await.len(),
        0,
        "a line-protocol write must not be visible to another namespace"
    );
}

/// A client must not be able to place a point in a namespace it did not
/// address. The JSON write path merged the namespace tag with
/// `entry().or_insert_with()`, so a caller-supplied `__namespace__` tag won.
#[tokio::test]
async fn a_client_cannot_spoof_the_namespace_tag() {
    let (base, _tmp) = start_multi_tenant_server().await;

    let resp = client()
        .post(format!("{base}/api/v1/write"))
        .header("X-Namespace", "tenant-a")
        .json(&json!({
            "measurement": "spoof",
            "tags": {"host": "h", "__namespace__": "tenant-b"},
            "fields": {"value": 1.0},
            "timestamp": 1_700_000_000_000_000_000_i64
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::NO_CONTENT || resp.status() == StatusCode::BAD_REQUEST,
        "unexpected status {}",
        resp.status()
    );

    assert_eq!(
        query_json(&base, "tenant-b", "spoof").await.len(),
        0,
        "a write addressed to tenant-a must never land in tenant-b"
    );
}

// ── Read surfaces must filter ──────────────────────────────────────────

/// `/api/v1/sql` used the namespace only as a plan-cache key: the query itself
/// ran against every tenant's rows.
#[tokio::test]
async fn sql_only_sees_the_requesting_namespace() {
    let (base, _tmp) = start_multi_tenant_server().await;

    write_json(
        &base,
        "tenant-a",
        "power",
        "a1",
        1.0,
        1_700_000_000_000_000_000,
    )
    .await;
    write_json(
        &base,
        "tenant-b",
        "power",
        "b1",
        2.0,
        1_700_000_000_000_000_001,
    )
    .await;

    let resp = client()
        .post(format!("{base}/api/v1/sql"))
        .header("X-Namespace", "tenant-a")
        .json(&json!({"query": "SELECT count(*) AS n FROM power"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let n = body["rows"][0][0].as_i64().unwrap_or(-1);
    assert_eq!(n, 1, "SQL in tenant-a must see only tenant-a's row: {body}");
}

/// The same, for the PromQL surface Grafana talks to.
#[tokio::test]
async fn promql_only_sees_the_requesting_namespace() {
    let (base, _tmp) = start_multi_tenant_server().await;

    let now_ns = 1_700_000_000_000_000_000_i64;
    write_json(&base, "tenant-a", "power", "a1", 1.0, now_ns).await;
    write_json(&base, "tenant-b", "power", "b1", 2.0, now_ns).await;

    let resp = client()
        .get(format!("{base}/api/v1/prom/query"))
        .header("X-Namespace", "tenant-a")
        .query(&[("query", "power"), ("time", "1700000000")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let series = body["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        series.len(),
        1,
        "PromQL in tenant-a must see only tenant-a's series: {body}"
    );
}

/// Label endpoints leak the *shape* of another tenant's data even when the
/// samples themselves are scoped.
#[tokio::test]
async fn promql_label_values_are_namespace_scoped() {
    let (base, _tmp) = start_multi_tenant_server().await;

    let now_ns = 1_700_000_000_000_000_000_i64;
    write_json(&base, "tenant-a", "power", "a1", 1.0, now_ns).await;
    write_json(&base, "tenant-b", "power", "b1", 2.0, now_ns).await;

    let resp = client()
        .get(format!("{base}/api/v1/prom/label/host/values"))
        .header("X-Namespace", "tenant-a")
        .query(&[("start", "1699999999"), ("end", "1700000001")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let values: Vec<String> = body["data"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        values.contains(&"a1".to_string()) && !values.contains(&"b1".to_string()),
        "label values must not cross namespaces: {values:?}"
    );
}

/// The hidden tag is an implementation detail of tenancy and must not appear
/// in a tenant's own label listing.
#[tokio::test]
async fn the_namespace_tag_is_not_exposed_as_a_label() {
    let (base, _tmp) = start_multi_tenant_server().await;
    write_json(
        &base,
        "tenant-a",
        "power",
        "a1",
        1.0,
        1_700_000_000_000_000_000,
    )
    .await;

    let resp = client()
        .get(format!("{base}/api/v1/prom/labels"))
        .header("X-Namespace", "tenant-a")
        .query(&[("start", "1699999999"), ("end", "1700000001")])
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let labels: Vec<String> = body["data"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        !labels.iter().any(|l| l == "__namespace__"),
        "internal tag leaked into the label list: {labels:?}"
    );
}

// ── Prometheus API shape ───────────────────────────────────────────────

/// `/labels`, `/label/<name>/values` and `/series` answer a **bare array**
/// under `data`; only `/query` and `/query_range` wrap their result in
/// `{resultType, result}`. Grafana reads the array directly, so wrapping
/// these three left every variable dropdown and the metric browser empty.
#[tokio::test]
async fn prometheus_list_endpoints_answer_a_bare_array() {
    let (base, _tmp) = start_server().await;
    write_json(
        &base,
        "tenant-a",
        "power",
        "a1",
        1.0,
        1_700_000_000_000_000_000,
    )
    .await;

    for path in [
        "/api/v1/prom/labels",
        "/api/v1/prom/label/host/values",
        "/api/v1/prom/series",
    ] {
        let resp = client()
            .get(format!("{base}{path}"))
            .header("X-Namespace", "tenant-a")
            .query(&[
                ("start", "1699999999"),
                ("end", "1700000001"),
                ("match[]", "power"),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path}");
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success", "{path}");
        assert!(
            body["data"].is_array(),
            "{path} must answer a bare array under `data`, got {body}"
        );
    }
}

// ── Authenticated reads ────────────────────────────────────────────────

/// A server with authentication on must still answer queries.
///
/// It did not. `/api/v1/query` consulted a row-level-security engine that was
/// constructed empty, had no configuration path and no admin API, and was
/// deny-by-default — so an authenticated principal matched no policy and
/// every query returned `403 Forbidden`. Turning on authentication turned off
/// the query endpoint, and no test exercised the two together.
#[tokio::test]
async fn an_authenticated_query_returns_data() {
    let (base, _tmp) = start_authenticated_server().await;

    let resp = client()
        .post(format!("{base}/api/v1/write"))
        .header("authorization", "Bearer test-key")
        .json(&json!({
            "measurement": "power",
            "tags": {"host": "h"},
            "fields": {"value": 1.0},
            "timestamp": 1_700_000_000_000_000_000_i64
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT, "authenticated write");

    let resp = client()
        .post(format!("{base}/api/v1/query"))
        .header("authorization", "Bearer test-key")
        .json(&json!({"measurement": "power"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "an authenticated query must not be refused"
    );
    let rows: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(rows.len(), 1, "authenticated query must return the row");
}

/// Server with API-key authentication enabled.
async fn start_authenticated_server() -> (String, TempDir) {
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
            key: "test-key".to_string(),
        }],
        jwt: None,
        exempt_paths: vec!["/health".to_string()],
    };
    let auth_state = chronixd::auth::AuthState::from_config(&auth_config).expect("auth state");

    let state: AppState = Arc::new(SharedState {
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: Some(auth_state),
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: None,
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        #[cfg(feature = "chaos")]
        chaos_agent: None,
        authz_engine: None,
        audit_logger: None,
        config: chronixd::config::ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        #[cfg(feature = "chaos")]
        chaos_guards: parking_lot::Mutex::new(Vec::new()),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        openapi_json: std::sync::OnceLock::new(),
    });

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base, tmp)
}
