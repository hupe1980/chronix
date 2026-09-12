#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Signal triggers over HTTP.
//!
//! The whole subsystem was unreachable from this server: the trigger engine,
//! the delivery router and the signal store all worked, and nothing ever
//! constructed the `Pipeline` that owns them, so `CREATE TRIGGER` existed for
//! embedded callers only. These tests drive the surface that was missing —
//! including the case that decides whether it may exist at all, which is that
//! one tenant's trigger must not see another tenant's writes.

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

/// A server with the trigger pipeline wired in, plus its database.
async fn start_with_triggers(
    pipeline: Option<Arc<chronix::Pipeline>>,
) -> (String, Arc<Chronix>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());

    if let Some(p) = pipeline.as_ref() {
        p.spawn_cdc_listener(db.event_bus());
    }

    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
        pipeline,
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
        authz_engine: None,
        audit_logger: None,
        config: chronixd::config::ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        openapi_json: std::sync::OnceLock::new(),
    });

    let metrics_handle = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder()
        .handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base, db, tmp)
}

fn client() -> reqwest::Client {
    chronixd::tls::ensure_crypto_provider();
    reqwest::Client::new()
}

fn test_pipeline() -> Arc<chronix::Pipeline> {
    Arc::new(chronix::Pipeline::with_config(chronix::PipelineConfig {
        ..chronix::PipelineConfig::default()
    }))
}

/// Without `[triggers]`, the endpoints say so rather than 500ing or pretending.
#[tokio::test]
async fn the_endpoints_are_absent_when_triggers_are_not_configured() {
    let (base, _db, _tmp) = start_with_triggers(None).await;
    for (method, path) in [("GET", "/api/v1/triggers"), ("GET", "/api/v1/signals")] {
        let resp = match method {
            "GET" => client().get(format!("{base}{path}")).send().await.unwrap(),
            _ => unreachable!(),
        };
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "{path} must 404 when triggers are off"
        );
        let body: Value = resp.json().await.unwrap();
        assert!(
            body.to_string().contains("[triggers]"),
            "the error must say how to turn them on, got {body}"
        );
    }
}

/// Create, list and drop, over HTTP.
#[tokio::test]
async fn a_trigger_can_be_created_listed_and_dropped() {
    let (base, _db, _tmp) = start_with_triggers(Some(test_pipeline())).await;

    let resp = client()
        .post(format!("{base}/api/v1/triggers"))
        .json(&json!({ "query": "CREATE TRIGGER hot_cpu ON cpu WHEN value > 90.0 DELIVER log" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "{:?}", resp.text().await);

    let body: Value = client()
        .get(format!("{base}/api/v1/triggers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["triggers"].as_array().unwrap().len(), 1);
    assert_eq!(body["triggers"][0]["name"], "hot_cpu");
    assert_eq!(body["triggers"][0]["measurement"], "cpu");

    // One trigger, read back by name — `SHOW TRIGGERS` used to be the only
    // way, so a client that had just created one had to list everything and
    // search for it.
    let one: Value = client()
        .get(format!("{base}/api/v1/triggers/hot_cpu"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(one["name"], "hot_cpu");
    assert_eq!(one["measurement"], "cpu");
    assert_eq!(one["enabled"], true);

    // A name nobody owns is a 404, not an empty body.
    let resp = client()
        .get(format!("{base}/api/v1/triggers/no_such_trigger"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = client()
        .delete(format!("{base}/api/v1/triggers/hot_cpu"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = client()
        .get(format!("{base}/api/v1/triggers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(body["triggers"].as_array().unwrap().is_empty());
}

/// A malformed statement is a 400 with the parser's message, not a 500.
#[tokio::test]
async fn a_malformed_statement_is_a_bad_request() {
    let (base, _db, _tmp) = start_with_triggers(Some(test_pipeline())).await;
    let resp = client()
        .post(format!("{base}/api/v1/triggers"))
        .json(&json!({ "query": "CREATE TRIGGER" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// A webhook with no configured secret is refused at creation, not accepted
/// and silently never delivered.
#[tokio::test]
async fn an_unsignable_webhook_trigger_is_refused() {
    let (base, _db, _tmp) = start_with_triggers(Some(test_pipeline())).await;
    let resp = client()
        .post(format!("{base}/api/v1/triggers"))
        .json(&json!({
            "query": "CREATE TRIGGER t ON cpu WHEN value > 1.0 \
                    DELIVER webhook('https://alerts.example.com/hook')"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    assert!(
        body.to_string().contains("webhook_signing_secrets"),
        "the error must name what is missing, got {body}"
    );
}

/// A write that breaches the threshold produces a signal the API can read.
#[tokio::test]
async fn a_write_fires_a_trigger_and_the_signal_is_readable() {
    let (base, _db, _tmp) = start_with_triggers(Some(test_pipeline())).await;

    client()
        .post(format!("{base}/api/v1/triggers"))
        .json(&json!({ "query": "CREATE TRIGGER hot ON cpu WHEN value > 90.0 DELIVER log" }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    client()
        .post(format!("{base}/api/v1/write"))
        .json(&json!({
            "measurement": "cpu",
            "tags": { "host": "h1" },
            "fields": { "value": 99.0 },
            "timestamp": 1_609_459_200_000_000_000_i64
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // The CDC listener is a background task; poll to a generous deadline
    // rather than sleeping once for an interval nobody guarantees.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let signals = loop {
        let body: Value = client()
            .get(format!("{base}/api/v1/signals"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let arr = body.as_array().cloned().unwrap_or_default();
        if !arr.is_empty() || std::time::Instant::now() > deadline {
            break arr;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };

    assert_eq!(signals.len(), 1, "the write must fire the trigger");
    assert_eq!(signals[0]["trigger_name"], "hot");
    assert_eq!(signals[0]["measurement"], "cpu");
    assert!((signals[0]["value"].as_f64().unwrap() - 99.0).abs() < 1e-9);
}
