#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Every error this server returns is the same JSON envelope.
//!
//! `ServerError` renders `{"error":…,"code":…}` and has done for a long time.
//! The framework answers *before* a handler runs and did not: a body that
//! failed to deserialise came back as the plain sentence "Failed to
//! deserialize the JSON body into the target type: missing field
//! `measurement`", an unknown path as a **404 with no body at all**, and a
//! wrong method as a bare 405. A client that parses `error` and branches on
//! `code` — the Python SDK does, and so does anything generated from the
//! OpenAPI document — got a JSON parse failure instead of an error message.
//!
//! So this drives the shapes a handler never sees, through the real router.

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

async fn server() -> (String, TempDir) {
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
        namespace_registry: std::sync::Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
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

/// Every failing request answers `{"error": <string>, "code": <string>}`.
#[tokio::test]
async fn every_error_carries_the_standard_envelope() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server().await;
    let c = reqwest::Client::new();

    let cases: Vec<(&str, reqwest::RequestBuilder, &str)> = vec![
        (
            "a path no route matches",
            c.get(format!("{base}/api/v1/nope")),
            "NOT_FOUND",
        ),
        (
            "a method no route accepts",
            c.delete(format!("{base}/api/v1/measurements")),
            "METHOD_NOT_ALLOWED",
        ),
        (
            "a body that is not JSON",
            c.post(format!("{base}/api/v1/chronix/sql"))
                .header("content-type", "application/json")
                .body("not json"),
            "BAD_REQUEST",
        ),
        (
            "valid JSON of the wrong shape",
            c.post(format!("{base}/api/v1/delete"))
                .header("content-type", "application/json")
                .body("{}"),
            "INVALID_BODY",
        ),
        (
            "a missing content type",
            c.post(format!("{base}/api/v1/chronix/sql"))
                .body(r#"{"query":"SELECT 1"}"#),
            "UNSUPPORTED_MEDIA_TYPE",
        ),
        (
            "a handler's own error",
            c.get(format!("{base}/api/v1/measurements/nope/schema")),
            "NOT_FOUND",
        ),
    ];

    for (what, req, expected_code) in cases {
        let resp = req.send().await.unwrap();
        let status = resp.status();
        assert!(status.is_client_error(), "{what}: status {status}");
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            content_type.starts_with("application/json"),
            "{what}: content-type {content_type}"
        );
        let body: Value = resp.json().await.unwrap_or_else(|e| panic!("{what}: {e}"));
        assert!(
            body["error"].as_str().is_some_and(|s| !s.is_empty()),
            "{what}: {body}"
        );
        assert_eq!(body["code"], expected_code, "{what}: {body}");
    }
}

/// The Prometheus endpoints keep their own error shape, which clients branch
/// on just as hard — `{"status":"error","errorType":…,"error":…}`. It is JSON,
/// so the envelope layer passes it through untouched.
#[tokio::test]
async fn the_prometheus_error_shape_is_left_alone() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server().await;
    let c = reqwest::Client::new();

    for (query, status, error_type) in [
        ("rate(", reqwest::StatusCode::BAD_REQUEST, "bad_data"),
        ("nope(x)", reqwest::StatusCode::BAD_REQUEST, "bad_data"),
        ("{}", reqwest::StatusCode::BAD_REQUEST, "bad_data"),
    ] {
        let resp = c
            .get(format!("{base}/api/v1/query"))
            .query(&[("query", query)])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status, "{query}");
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "error", "{query}: {body}");
        assert_eq!(body["errorType"], error_type, "{query}: {body}");
        assert!(body["code"].is_null(), "{query}: envelope leaked in {body}");
    }
}

/// Every column the schema endpoint reports can be selected.
///
/// The endpoint invites exactly one workflow — read the schema, then write a
/// query — and it reported the timestamp column as `timestamp`, which is the
/// *storage* name. SQL knows it as `_time`, so following the invitation
/// answered `No field named timestamp`. This types every name it hands out
/// back in.
#[tokio::test]
async fn every_column_the_schema_reports_can_be_selected() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server().await;
    let c = reqwest::Client::new();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let resp = c
        .post(format!("{base}/api/v2/write?bucket=default&precision=ns"))
        .body(format!(
            "cpu,host=a,region=eu usage=1,load=2,note=\"boot\" {now}"
        ))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "{}", resp.status());

    let schema: Value = c
        .get(format!("{base}/api/v1/measurements/cpu/schema"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let columns = schema["columns"].as_array().expect("columns");
    assert!(columns.len() >= 6, "{schema}");

    for col in columns {
        let name = col["name"].as_str().unwrap();
        let body: Value = c
            .post(format!("{base}/api/v1/chronix/sql"))
            .json(&serde_json::json!({
                "query": format!("SELECT \"{name}\" FROM cpu LIMIT 1")
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            body["rows"].is_array(),
            "the schema reports a column SQL does not have: {name} → {body}"
        );
    }

    // And a row's `tags` and `fields` follow the same schema: a tag is a tag,
    // and a *string field* is a field rather than being mistaken for one.
    let rows: Value = c
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&serde_json::json!({
            "measurement": "cpu",
            "range": {"start": 0, "end": i64::MAX}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = &rows.as_array().expect("rows")[0];
    assert_eq!(row["tags"]["host"], "a", "{row}");
    assert_eq!(row["tags"]["region"], "eu", "{row}");
    assert_eq!(row["fields"]["note"], "boot", "{row}");
    assert!(
        row["tags"]["note"].is_null(),
        "a string field is not a tag: {row}"
    );
    assert!(
        row["fields"]["host"].is_null(),
        "a tag is not a field: {row}"
    );
}

/// The analytics aggregates answer over the JSON API, and a query the caller
/// got wrong is a `400` with the reason in it.
///
/// Two things this test would have caught the moment they shipped: every
/// forecast aggregate returns a `LIST(DOUBLE)`, and the JSON row encoder had
/// no arm for a list, so the documented headline feature answered
/// `"<unsupported: List(Float64)>"` on the surface most callers use. And an
/// aggregate that rejects its own argument does so at **execution**, where
/// every error was mapped to `Internal` — a 500 whose body is redacted, so
/// the message naming the setting to raise never reached anyone.
#[tokio::test]
async fn an_analytics_aggregate_answers_and_explains_itself() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server().await;
    let c = reqwest::Client::new();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let body: String = (0..60)
        .map(|i| format!("m,h=a v={i} {}\n", now - (60 - i) * 1_000_000_000))
        .collect();
    let resp = c
        .post(format!("{base}/api/v2/write?bucket=default&precision=ns"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "{}", resp.status());

    // A list column is a JSON array, not a placeholder string.
    let out: Value = c
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&serde_json::json!({ "query": "SELECT forecast(v, _time, 3) AS f FROM m" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let cell = &out["rows"][0][0];
    assert!(cell.is_array(), "a forecast must be a JSON array: {out}");
    assert_eq!(cell.as_array().unwrap().len(), 3, "{out}");
    assert!(cell[0].is_number(), "{out}");

    // Over the configured bound: a 400 that names the setting.
    let resp = c
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&serde_json::json!({ "query": "SELECT forecast(v, _time, 2000000) AS f FROM m" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let out: Value = resp.json().await.unwrap();
    assert_eq!(out["code"], "BAD_REQUEST", "{out}");
    assert!(
        out["error"]
            .as_str()
            .is_some_and(|e| e.contains("max_forecast_horizon")),
        "the reason must survive to the client: {out}"
    );
}

/// A successful non-JSON response is not touched — `/metrics` is Prometheus
/// text, and wrapping it would take the scrape endpoint out.
#[tokio::test]
async fn a_successful_non_json_body_is_untouched() {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server().await;
    let body = reqwest::get(format!("{base}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.starts_with('#') || body.is_empty(), "{body:.120}");
}

/// A refused backup or restore says *why*, with the status a caller can act
/// on.
///
/// Every one of these was a redacted `500 DATABASE_ERROR: an internal error
/// occurred`, because they were raised as `DbError::Internal` and an internal
/// error is deliberately opaque on the wire. They are not internal: the
/// target already exists, the directory is not a backup, a segment is
/// missing. An operator restoring a backup is the last person who should be
/// told "an internal error occurred" — found by running the finished server
/// and reading what it said, not by a test.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_restore_names_its_reason() {
    let (base, tmp) = server().await;
    let client = super::integration::client();

    // Not a backup directory at all.
    let resp = client
        .post(format!("{base}/api/v1/admin/restore"))
        .json(&serde_json::json!({ "backup_dir": "nope", "target_dir": "t" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.expect("json");
    assert_eq!(body["code"], "INVALID_REQUEST");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("backup_manifest.json"),
        "the reason has to survive to the client: {body}"
    );

    // A real backup, then a restore onto a directory that exists.
    let resp = client
        .post(format!("{base}/api/v1/admin/backup"))
        .json(&serde_json::json!({ "target_dir": "b1" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
    let manifest: Value = resp.json().await.expect("json");
    assert!(manifest["segments"].is_number(), "manifest: {manifest}");

    let resp = client
        .post(format!("{base}/api/v1/admin/restore"))
        .json(&serde_json::json!({ "backup_dir": "b1", "target_dir": "b1" }))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.expect("json");
    assert_eq!(body["code"], "INVALID_REQUEST");
    assert!(
        body["error"].as_str().unwrap().contains("already exists"),
        "{body}"
    );

    drop(tmp);
}
