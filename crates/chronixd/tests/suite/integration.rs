#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Integration tests for the chronixd REST API.
//!
//! Spawns a real HTTP server (with an in-memory temp database) and exercises
//! every REST endpoint using `reqwest`.

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

// ── Helpers ────────────────────────────────────────────────────────────

/// Spin up a test HTTP server backed by a fresh temp database.
/// Returns the base URL and temp dir handle (must be kept alive).
async fn start_test_server() -> (String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));

    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());

    let state: AppState = Arc::new(SharedState {
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: None,
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

    // Use the metrics builder without installing a global recorder to avoid
    // conflicts across parallel tests.
    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();

    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let base = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    // Give the server a moment to start
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

// ── Health & readiness ─────────────────────────────────────────────────

#[tokio::test]
async fn health_endpoint() {
    let (base, _tmp) = start_test_server().await;
    let resp = client().get(format!("{base}/health")).send().await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn ready_endpoint() {
    let (base, _tmp) = start_test_server().await;
    let resp = client().get(format!("{base}/ready")).send().await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], true);
}

// ── Write + query round-trip ───────────────────────────────────────────

#[tokio::test]
async fn write_and_query_json() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    // Write a single point
    let write_body = json!({
        "measurement": "cpu",
        "tags": {"host": "server01", "region": "us-east"},
        "fields": {"usage_idle": 95.5, "usage_system": 1.2},
        "timestamp": 1_609_459_200_000_000_000_i64
    });

    let resp = c
        .post(format!("{base}/api/v1/write"))
        .json(&write_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Query it back
    let query_body = json!({
        "measurement": "cpu",
        "range": {"start": 1_609_459_199_000_000_000_i64, "end": 1_609_459_201_000_000_000_i64},
        "tags": {"host": "server01"}
    });

    let resp = c
        .post(format!("{base}/api/v1/query"))
        .json(&query_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let rows: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(rows.len(), 1, "expected 1 row, got {}", rows.len());
    assert_eq!(rows[0]["timestamp"], 1_609_459_200_000_000_000_i64);
}

#[tokio::test]
async fn write_batch() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    let write_body = json!([
        {
            "measurement": "cpu",
            "tags": {"host": "a"},
            "fields": {"usage": 10.0},
            "timestamp": 1000_i64
        },
        {
            "measurement": "cpu",
            "tags": {"host": "b"},
            "fields": {"usage": 20.0},
            "timestamp": 2000_i64
        }
    ]);

    let resp = c
        .post(format!("{base}/api/v1/write"))
        .json(&write_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Query all
    let query_body = json!({
        "measurement": "cpu"
    });
    let resp = c
        .post(format!("{base}/api/v1/query"))
        .json(&query_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(rows.len(), 2);
}

// ── InfluxDB Line Protocol ─────────────────────────────────────────────

#[tokio::test]
async fn write_influx_line_protocol() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    let line = "mem,host=srv1 used=1024i,free=2048i 1609459200000000000\n\
                mem,host=srv2 used=512i,free=4096i 1609459200000000000";

    let resp = c
        .post(format!("{base}/api/v1/write/influx"))
        .header("Content-Type", "text/plain")
        .body(line)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Query it back
    let query_body = json!({"measurement": "mem"});
    let resp = c
        .post(format!("{base}/api/v1/query"))
        .json(&query_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(rows.len(), 2);
}

// ── Measurements list & schema ─────────────────────────────────────────

#[tokio::test]
async fn list_measurements_and_schema() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    // Write something first
    let write_body = json!({
        "measurement": "temperature",
        "tags": {"location": "office"},
        "fields": {"value": 23.5},
        "timestamp": 1000_i64
    });
    c.post(format!("{base}/api/v1/write"))
        .json(&write_body)
        .send()
        .await
        .unwrap();

    // List measurements
    let resp = c
        .get(format!("{base}/api/v1/measurements"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let measurements = body["items"].as_array().expect("items should be an array");
    assert!(
        !measurements.is_empty(),
        "should have at least one measurement"
    );
    let names: Vec<&str> = measurements
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"temperature"),
        "should contain 'temperature'"
    );

    // Get schema
    let resp = c
        .get(format!("{base}/api/v1/measurements/temperature/schema"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let schema: Value = resp.json().await.unwrap();
    assert_eq!(schema["name"], "temperature");
    let columns = schema["columns"].as_array().unwrap();
    assert!(!columns.is_empty());
}

// ── Drop measurement ───────────────────────────────────────────────────

#[tokio::test]
async fn drop_measurement() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    // Write
    let write_body = json!({
        "measurement": "to_drop",
        "fields": {"val": 1.0},
        "timestamp": 1000_i64
    });
    c.post(format!("{base}/api/v1/write"))
        .json(&write_body)
        .send()
        .await
        .unwrap();

    // Drop
    let resp = c
        .delete(format!("{base}/api/v1/measurements/to_drop"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify it's gone — schema lookup should 404
    let resp = c
        .get(format!("{base}/api/v1/measurements/to_drop/schema"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── Delete endpoint ────────────────────────────────────────────────────

#[tokio::test]
async fn predicate_delete() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    // Write points
    for i in 0..5 {
        let body = json!({
            "measurement": "sensor",
            "tags": {"id": "s1"},
            "fields": {"temp": 20.0 + i as f64},
            "timestamp": (1000 + i * 100) as i64
        });
        c.post(format!("{base}/api/v1/write"))
            .json(&body)
            .send()
            .await
            .unwrap();
    }

    // Delete with time range
    let delete_body = json!({
        "measurement": "sensor",
        "tags": {"id": "s1"},
        "range": {"start": 1100_i64, "end": 1400_i64}
    });
    let resp = c
        .post(format!("{base}/api/v1/delete"))
        .json(&delete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert!(body["deleted"].is_number());
}

// ── Query not found ────────────────────────────────────────────────────

#[tokio::test]
async fn query_nonexistent_measurement() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    let query_body = json!({"measurement": "nonexistent"});
    let resp = c
        .post(format!("{base}/api/v1/query"))
        .json(&query_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── Empty write is rejected ────────────────────────────────────────────

#[tokio::test]
async fn empty_write_rejected() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    let resp = c
        .post(format!("{base}/api/v1/write"))
        .json(&json!([]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn empty_influx_write_rejected() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    let resp = c
        .post(format!("{base}/api/v1/write/influx"))
        .header("Content-Type", "text/plain")
        .body("# just a comment\n\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── Rollups list ───────────────────────────────────────────────────────

#[tokio::test]
async fn list_rollups_empty() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    let resp = c
        .get(format!("{base}/api/v1/rollups"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let rollups = body["items"].as_array().expect("items should be an array");
    assert!(rollups.is_empty());
}

// ── Metrics endpoint ───────────────────────────────────────────────────

#[tokio::test]
async fn metrics_endpoint() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    let resp = c.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // Prometheus metrics are text/plain
    let body = resp.text().await.unwrap();
    // Even if empty, should be valid (possibly just header comments)
    assert!(body.is_empty() || body.starts_with('#') || body.contains('\n'));
}

// ── Query with limit & offset ──────────────────────────────────────────

#[tokio::test]
async fn query_with_limit_and_offset() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    // Write 10 points
    for i in 0..10 {
        let body = json!({
            "measurement": "series",
            "tags": {"id": "a"},
            "fields": {"val": i as f64},
            "timestamp": (1000 + i * 100) as i64
        });
        c.post(format!("{base}/api/v1/write"))
            .json(&body)
            .send()
            .await
            .unwrap();
    }

    // Query with limit=3, offset=2
    let query_body = json!({
        "measurement": "series",
        "limit": 3,
        "offset": 2
    });
    let resp = c
        .post(format!("{base}/api/v1/query"))
        .json(&query_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(rows.len(), 3, "expected 3 rows with limit=3");
}

// ── read-only SQL admission control ────────────────────────────

/// `POST /api/v1/sql` must reject mutating statements *without applying
/// them*. Regression test for the pre-execution admission check: the old
/// code called `SessionContext::sql()` (which eagerly applies DDL and `SET`
/// side effects to the process-wide shared session) and only inspected the
/// plan afterwards.
#[tokio::test]
async fn sql_endpoint_rejects_mutations_without_side_effects() {
    let (base, _tmp) = start_test_server().await;
    let c = client();

    let sql = |q: &str| {
        let c = c.clone();
        let base = base.clone();
        let q = q.to_string();
        async move {
            c.post(format!("{base}/api/v1/sql"))
                .json(&json!({ "query": q }))
                .send()
                .await
                .unwrap()
        }
    };

    // `information_schema` is disabled at context construction.
    let before = sql("SELECT * FROM information_schema.tables").await;
    assert_ne!(
        before.status(),
        StatusCode::OK,
        "information_schema must be disabled to begin with"
    );

    for stmt in [
        "SET datafusion.catalog.information_schema = true",
        "CREATE VIEW pwned AS SELECT 1",
        "CREATE TABLE t (x INT)",
        "CREATE EXTERNAL TABLE ext STORED AS CSV LOCATION '/etc/passwd'",
        "EXPLAIN SELECT 1",
        "PREPARE p AS SELECT 1",
    ] {
        let resp = sql(stmt).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "statement should have been rejected: {stmt}"
        );
    }

    // The decisive assertion: the rejected `SET` did not mutate the shared
    // session. Repeated twice to also cover the plan-cache path.
    for _ in 0..2 {
        let after = sql("SELECT * FROM information_schema.tables").await;
        assert_ne!(
            after.status(),
            StatusCode::OK,
            "a rejected SET re-enabled information_schema process-wide"
        );
    }

    // Ordinary reads still work.
    let ok = sql("SELECT 1 AS x").await;
    assert_eq!(ok.status(), StatusCode::OK);
}
