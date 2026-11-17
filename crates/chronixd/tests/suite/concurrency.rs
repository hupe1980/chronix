#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! `server.max_concurrent_queries`: SQL, PromQL and Flight SQL share one set
//! of slots; a query waits within its timeout and is then refused `503`
//! naming the setting; writes and health never wait.
//!
//! The test holds the only slot itself, so "busy" is a fact rather than a
//! race against a slow query.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_flight::FlightDescriptor;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{Any, CommandStatementQuery};
use prost::Message;
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Channel;

use chronix::Chronix;
use chronix::prelude::*;
use chronixd::config::{DatabaseConfig, ServerConfig, ServerSettings};
use chronixd::flight::ChronixFlightSqlService;
use chronixd::http::{AppState, SharedState};
use chronixd::query_slots::QuerySlots;
use chronixd::server::build_router;

struct Harness {
    http: String,
    flight: FlightServiceClient<Channel>,
    slots: QuerySlots,
    _tmp: TempDir,
}

async fn harness() -> Harness {
    chronixd::tls::ensure_crypto_provider();
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(
        Chronix::open(
            ChronixConfig::builder()
                .data_dir(tmp.path().to_path_buf())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let slots = QuerySlots::new(1);

    let server = ServerSettings {
        sql_query_timeout_secs: 1,
        prom_query_timeout_secs: 1,
        max_concurrent_queries: 1,
        ..Default::default()
    };
    let config = ServerConfig {
        server,
        database: DatabaseConfig {
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        },
        ..Default::default()
    };
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
        start_time: Instant::now(),
        connector_manager: None,
        sql_contexts: chronixd::namespace::SqlContexts::new(db.clone()),
        auth_state: None,
        namespace_registry: Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: None,
        audit_logger: None,
        config,
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        namespace_ingest_limiter: chronixd::rate_limit::NamespaceIngestLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: chronixd::http::WriteDedupCache::new(60, 1024),
        write_timeout: Duration::from_secs(5),
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
        query_slots: slots.clone(),
    });
    let handle = chronixd::server::prometheus_builder()
        .unwrap()
        .build_recorder()
        .handle();
    let app = build_router(state, "/metrics", handle, 10 * 1024 * 1024, None);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let flight = ChronixFlightSqlService::new(db)
        .with_limits(Duration::from_secs(1), 1000)
        .with_query_slots(slots.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let flight_addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(flight.into_server())
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let channel = Channel::from_shared(format!("http://{flight_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    Harness {
        http: format!("http://{http}"),
        flight: FlightServiceClient::new(channel),
        slots,
        _tmp: tmp,
    }
}

async fn sql(h: &Harness) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/api/v1/chronix/sql", h.http))
        .json(&serde_json::json!({ "query": "SELECT 1 AS one" }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn busy_slots_refuse_queries_by_name_and_never_delay_writes_or_health() {
    let mut h = harness().await;
    let held = h.slots.acquire(Duration::ZERO).await.unwrap();
    let client = reqwest::Client::new();

    // Health and writes answer while every query slot is taken, well inside
    // the one-second query timeout they would otherwise have waited out.
    let started = Instant::now();
    let health = client
        .get(format!("{}/health", h.http))
        .send()
        .await
        .unwrap();
    assert!(health.status().is_success());
    let write = client
        .post(format!("{}/write", h.http))
        .body("cpu,host=a value=1.5")
        .send()
        .await
        .unwrap();
    assert!(write.status().is_success(), "{}", write.status());
    assert!(started.elapsed() < Duration::from_millis(900));

    // SQL waits its timeout, then names the setting.
    let resp = sql(&h).await;
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers()["retry-after"], "1");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "QUERY_SLOTS_BUSY", "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("server.max_concurrent_queries"),
        "{body}"
    );

    // PromQL: the Prometheus envelope, `unavailable`, and a retry hint.
    let resp = client
        .get(format!("{}/api/v1/query?query=cpu_value", h.http))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers()["retry-after"], "1");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["errorType"], "unavailable", "{body}");

    // Flight SQL: RESOURCE_EXHAUSTED, naming the setting.
    let cmd = CommandStatementQuery {
        query: "SELECT 1".into(),
        transaction_id: None,
    };
    let mut buf = Vec::new();
    Any::pack(&cmd).unwrap().encode(&mut buf).unwrap();
    let info = h
        .flight
        .get_flight_info(FlightDescriptor::new_cmd(buf))
        .await
        .unwrap()
        .into_inner();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let status = h.flight.do_get(ticket).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted, "{status}");
    assert!(status.message().contains("server.max_concurrent_queries"));

    // A query that is waiting when the slot frees runs.
    let waiting = {
        let base = h.http.clone();
        tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("{base}/api/v1/chronix/sql"))
                .json(&serde_json::json!({ "query": "SELECT 1 AS one" }))
                .send()
                .await
                .unwrap()
                .status()
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(held);
    assert_eq!(waiting.await.unwrap(), reqwest::StatusCode::OK);
    assert_eq!(sql(&h).await.status(), reqwest::StatusCode::OK);
}
