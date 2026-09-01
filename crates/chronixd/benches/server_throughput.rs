#![allow(clippy::unwrap_used)] // benches may unwrap
//! Server throughput benchmarks for chronixd.
//!
//! Measures end-to-end write and query performance through the HTTP REST,
//! gRPC, and Flight SQL endpoints using real (ephemeral) server instances.
//!
//! Run with:
//! ```sh
//! cargo bench -p chronixd --bench server_throughput
//! ```

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::Rng;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use chronix::prelude::*;
use chronix::Chronix;

use chronixd::grpc::ChronixGrpcService;
use chronixd::http::{AppState, SharedState};
use chronixd::proto;
use chronixd::proto::chronix_service_client::ChronixServiceClient;
use chronixd::server::build_router;

// ── Helpers ────────────────────────────────────────────────────────────

/// Create a temporary Chronix database and return `(db, tmp_dir)`.
fn setup_db() -> (Arc<Chronix>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("config");
    let db = Arc::new(Chronix::open(config).expect("open db"));
    (db, tmp)
}

/// Generate a batch of JSON point bodies for the HTTP write endpoint.
fn generate_http_batch(n: usize) -> serde_json::Value {
    let mut rng = rand::rng();
    let points: Vec<serde_json::Value> = (0..n)
        .map(|i| {
            serde_json::json!({
                "measurement": "bench_http",
                "tags": { "host": format!("srv{}", i % 10) },
                "fields": { "value": rng.random::<f64>() * 100.0 },
                "timestamp": 1_609_459_200_000_000_000i64 + (i as i64 * 1_000_000)
            })
        })
        .collect();
    serde_json::Value::Array(points)
}

/// Generate a batch of protobuf points for the gRPC write endpoint.
fn generate_grpc_batch(n: usize) -> Vec<proto::Point> {
    let mut rng = rand::rng();
    (0..n)
        .map(|i| proto::Point {
            measurement: "bench_grpc".to_string(),
            tags: vec![proto::Tag {
                key: "host".to_string(),
                value: format!("srv{}", i % 10),
            }],
            fields: vec![proto::Field {
                key: "value".to_string(),
                value: Some(proto::FieldValue {
                    value: Some(proto::field_value::Value::Float64(
                        rng.random::<f64>() * 100.0,
                    )),
                }),
            }],
            timestamp: 1_609_459_200_000_000_000i64 + (i as i64 * 1_000_000),
        })
        .collect()
}

// ── HTTP benchmarks ────────────────────────────────────────────────────

fn bench_http_write(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Allow server to start
    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/write", addr.port());

    let batch_size = 1000;
    let batch = generate_http_batch(batch_size);
    let body = serde_json::to_string(&batch).unwrap();

    let mut group = c.benchmark_group("http_write");
    group.throughput(Throughput::Elements(batch_size as u64));
    group.sample_size(20);

    group.bench_function("1K_points", |b| {
        b.iter(|| {
            let resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .unwrap();
            assert!(
                resp.status().is_success(),
                "write failed: {}",
                resp.status()
            );
        });
    });

    group.finish();
}

fn bench_http_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();

    // Pre-populate with data
    let mut points = Vec::new();
    let mut rng = rand::rng();
    for i in 0..10_000 {
        let tags: BTreeMap<String, String> = [("host".to_string(), "srv1".to_string())].into();
        let fields: BTreeMap<String, FieldValue> = [(
            "value".to_string(),
            FieldValue::F64(rng.random::<f64>() * 100.0),
        )]
        .into();
        let key = SeriesKey::new("bench_query", tags.clone()).unwrap();
        let ts = 1_609_459_200_000_000_000i64 + (i * 1_000_000_000);
        points.push(Point::new(key, fields, ts).unwrap());
    }
    let _ = db.insert_batch(&points).unwrap();

    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/query", addr.port());

    // Query a 1-hour range (3600 points)
    let query_body = serde_json::json!({
        "measurement": "bench_query",
        "tags": { "host": "srv1" },
        "range": {
            "start": 1_609_459_200_000_000_000i64,
            "end":   1_609_459_200_000_000_000i64 + 3600 * 1_000_000_000i64
        }
    });
    let body = serde_json::to_string(&query_body).unwrap();

    let mut group = c.benchmark_group("http_query");
    group.sample_size(50);

    group.bench_function("1h_range_single_series", |b| {
        b.iter(|| {
            let resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .unwrap();
            assert!(resp.status().is_success());
        });
    });

    group.finish();
}

// ── gRPC benchmarks ───────────────────────────────────────────────────

fn bench_grpc_write(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();

    let start_time = std::time::Instant::now();
    let grpc_service = ChronixGrpcService::new(db, start_time);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    rt.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::chronix_service_server::ChronixServiceServer::new(
                grpc_service,
            ))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let endpoint = format!("http://127.0.0.1:{}", addr.port());
    let mut client = rt
        .block_on(ChronixServiceClient::connect(endpoint))
        .unwrap();

    let batch_size = 1000;
    let points = generate_grpc_batch(batch_size);

    let mut group = c.benchmark_group("grpc_write");
    group.throughput(Throughput::Elements(batch_size as u64));
    group.sample_size(20);

    group.bench_function("1K_points", |b| {
        b.iter(|| {
            let req = proto::WriteRequest {
                points: points.clone(),
            };
            let resp = rt.block_on(client.write(req)).unwrap().into_inner();
            assert_eq!(resp.written, batch_size as u64);
        });
    });

    group.finish();
}

fn bench_grpc_stream_write(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();

    let start_time = std::time::Instant::now();
    let grpc_service = ChronixGrpcService::new(db, start_time);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    rt.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::chronix_service_server::ChronixServiceServer::new(
                grpc_service,
            ))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let endpoint = format!("http://127.0.0.1:{}", addr.port());

    let batch_size = 1000;
    let total_messages = 10;
    let total_points = batch_size * total_messages;

    let mut group = c.benchmark_group("grpc_stream_write");
    group.throughput(Throughput::Elements(total_points as u64));
    group.sample_size(20);

    group.bench_function("10x1K_stream", |b| {
        b.iter(|| {
            let mut client = rt
                .block_on(ChronixServiceClient::connect(endpoint.clone()))
                .unwrap();

            let batches: Vec<proto::StreamWriteRequest> = (0..total_messages)
                .map(|_| proto::StreamWriteRequest {
                    points: generate_grpc_batch(batch_size),
                    dedup_key: String::new(),
                })
                .collect();

            let stream = tokio_stream::iter(batches);
            let resp = rt
                .block_on(client.stream_write(stream))
                .unwrap()
                .into_inner();
            assert_eq!(resp.total_written, total_points as u64);
        });
    });

    group.finish();
}

fn bench_grpc_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();

    // Pre-populate
    let mut points = Vec::new();
    let mut rng = rand::rng();
    for i in 0..10_000 {
        let tags: BTreeMap<String, String> = [("host".to_string(), "srv1".to_string())].into();
        let fields: BTreeMap<String, FieldValue> = [(
            "value".to_string(),
            FieldValue::F64(rng.random::<f64>() * 100.0),
        )]
        .into();
        let key = SeriesKey::new("bench_grpc_query", tags.clone()).unwrap();
        let ts = 1_609_459_200_000_000_000i64 + (i * 1_000_000_000);
        points.push(Point::new(key, fields, ts).unwrap());
    }
    let _ = db.insert_batch(&points).unwrap();

    let start_time = std::time::Instant::now();
    let grpc_service = ChronixGrpcService::new(db, start_time);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    rt.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::chronix_service_server::ChronixServiceServer::new(
                grpc_service,
            ))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let endpoint = format!("http://127.0.0.1:{}", addr.port());
    let mut client = rt
        .block_on(ChronixServiceClient::connect(endpoint))
        .unwrap();

    let mut group = c.benchmark_group("grpc_query");
    group.sample_size(50);

    group.bench_function("1h_range_streaming", |b| {
        b.iter(|| {
            let req = proto::QueryRequest {
                measurement: "bench_grpc_query".to_string(),
                range: Some(proto::TimeRange {
                    start: 1_609_459_200_000_000_000,
                    end: 1_609_459_200_000_000_000 + 3600 * 1_000_000_000,
                }),
                tag_filters: vec![],
                field_columns: vec![],
                limit: 0,
            };
            let mut stream = rt.block_on(client.query(req)).unwrap().into_inner();
            let mut rows = 0;
            while let Some(resp) = rt.block_on(stream.message()).unwrap() {
                rows += resp.rows.len();
            }
            assert!(rows > 0);
        });
    });

    group.finish();
}

// ── Concurrent benchmark ──────────────────────────────────────────────

fn bench_concurrent_writers_readers(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();

    // Pre-seed some data for readers
    let mut seed_points = Vec::new();
    let mut rng = rand::rng();
    for i in 0..1_000 {
        let tags: BTreeMap<String, String> = [("host".to_string(), "srv1".to_string())].into();
        let fields: BTreeMap<String, FieldValue> = [(
            "value".to_string(),
            FieldValue::F64(rng.random::<f64>() * 100.0),
        )]
        .into();
        let key = SeriesKey::new("bench_concurrent", tags).unwrap();
        let ts = 1_609_459_200_000_000_000i64 + (i * 1_000_000_000);
        seed_points.push(Point::new(key, fields, ts).unwrap());
    }
    let _ = db.insert_batch(&seed_points).unwrap();

    let start_time = std::time::Instant::now();
    let grpc_service = ChronixGrpcService::new(db, start_time);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    rt.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::chronix_service_server::ChronixServiceServer::new(
                grpc_service,
            ))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let endpoint = format!("http://127.0.0.1:{}", addr.port());

    let mut group = c.benchmark_group("concurrent");
    group.sample_size(10);

    group.bench_function("5_writers_5_readers", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();

                // 5 writers
                for _ in 0..5 {
                    let ep = endpoint.clone();
                    handles.push(tokio::spawn(async move {
                        let mut client = ChronixServiceClient::connect(ep).await.unwrap();
                        let points = generate_grpc_batch(100);
                        let req = proto::WriteRequest { points };
                        client.write(req).await.unwrap();
                    }));
                }

                // 5 readers
                for _ in 0..5 {
                    let ep = endpoint.clone();
                    handles.push(tokio::spawn(async move {
                        let mut client = ChronixServiceClient::connect(ep).await.unwrap();
                        let req = proto::QueryRequest {
                            measurement: "bench_concurrent".to_string(),
                            range: None,
                            tag_filters: vec![],
                            field_columns: vec![],
                            limit: 100,
                        };
                        let mut stream = client.query(req).await.unwrap().into_inner();
                        while stream.message().await.unwrap().is_some() {}
                    }));
                }

                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ── Story 9.2 — E2E Scale benchmarks ──────────────────────────────────

/// High-volume write benchmark: 10K points per batch, measuring sustained
/// write throughput (target: ≥ 50K points/sec on single node).
fn bench_scale_http_write_10k(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 100 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/write", addr.port());

    let batch_size = 10_000;
    let batch = generate_http_batch(batch_size);
    let body = serde_json::to_string(&batch).unwrap();

    let mut group = c.benchmark_group("scale_http_write");
    group.throughput(Throughput::Elements(batch_size as u64));
    group.sample_size(10);

    group.bench_function("10K_points", |b| {
        b.iter(|| {
            let resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .unwrap();
            assert!(
                resp.status().is_success(),
                "write failed: {}",
                resp.status()
            );
        });
    });

    group.finish();
}

/// Multi-measurement benchmark: write to 100 distinct measurements to
/// simulate multi-tenant workloads.
fn bench_scale_multi_measurement(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 100 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/write", addr.port());

    // Generate batches for 100 measurements, 10 points each.
    let batches: Vec<String> = (0..100)
        .map(|m| {
            let mut rng = rand::rng();
            let points: Vec<serde_json::Value> = (0..10)
                .map(|i| {
                    serde_json::json!({
                        "measurement": format!("tenant_{m}_cpu"),
                        "tags": { "host": format!("srv{}", i % 5) },
                        "fields": { "value": rng.random::<f64>() * 100.0 },
                        "timestamp": 1_609_459_200_000_000_000i64 + (i as i64 * 1_000_000)
                    })
                })
                .collect();
            serde_json::to_string(&serde_json::Value::Array(points)).unwrap()
        })
        .collect();

    let mut group = c.benchmark_group("scale_multi_measurement");
    group.throughput(Throughput::Elements(1000)); // 100 measurements × 10 points
    group.sample_size(10);

    group.bench_function("100_measurements_10pts_each", |b| {
        b.iter(|| {
            for body in &batches {
                let resp = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(body.clone())
                    .send()
                    .unwrap();
                assert!(resp.status().is_success());
            }
        });
    });

    group.finish();
}

/// Query latency benchmark on a densely populated measurement
/// (100K pre-loaded points, targeted time-range query).
fn bench_scale_targeted_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();

    // Pre-populate 100K points.
    let mut points = Vec::new();
    let mut rng = rand::rng();
    for i in 0..100_000i64 {
        let tags: BTreeMap<String, String> = [("host".to_string(), "srv1".to_string())].into();
        let fields: BTreeMap<String, FieldValue> = [(
            "value".to_string(),
            FieldValue::F64(rng.random::<f64>() * 100.0),
        )]
        .into();
        let key = SeriesKey::new("bench_scale_query", tags.clone()).unwrap();
        let ts = 1_609_459_200_000_000_000i64 + (i * 1_000_000_000);
        points.push(Point::new(key, fields, ts).unwrap());
    }
    // Insert in batches to avoid huge allocations.
    for chunk in points.chunks(10_000) {
        let _ = db.insert_batch(chunk).unwrap();
    }

    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/query", addr.port());

    // Targeted query: small window within 100K points.
    let query_body = serde_json::json!({
        "measurement": "bench_scale_query",
        "tags": { "host": "srv1" },
        "range": {
            "start": 1_609_459_200_000_000_000i64 + 50_000 * 1_000_000_000i64,
            "end":   1_609_459_200_000_000_000i64 + 51_000 * 1_000_000_000i64
        }
    });
    let body = serde_json::to_string(&query_body).unwrap();

    let mut group = c.benchmark_group("scale_targeted_query");
    group.sample_size(50);

    group.bench_function("100K_pts_1K_range", |b| {
        b.iter(|| {
            let resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .unwrap();
            assert!(resp.status().is_success());
        });
    });

    group.finish();
}

/// TSBS-style DevOps workload: CPU, memory, disk, and network metrics
/// across 100 hosts, mimicking the Time Series Benchmark Suite pattern.
fn bench_tsbs_devops_workload(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/write", addr.port());

    // Generate a TSBS DevOps-like batch: 100 hosts × 4 metrics = 400 points.
    let measurements = ["cpu_usage", "mem_free", "disk_io", "net_bytes"];
    let mut rng = rand::rng();
    let mut batch: Vec<serde_json::Value> = Vec::with_capacity(400);
    for host in 0..100u32 {
        let host_name = format!("host_{:03}", host);
        for (m, meas) in measurements.iter().enumerate() {
            batch.push(serde_json::json!({
                "measurement": *meas,
                "tags": {
                    "host": host_name,
                    "region": format!("us-east-{}", host % 3),
                    "datacenter": format!("dc{}", host % 5)
                },
                "fields": { "value": rng.random::<f64>() * 100.0 },
                "timestamp": 1_609_459_200_000_000_000i64 + (m as i64 * 1_000_000)
            }));
        }
    }
    let body = serde_json::to_string(&serde_json::Value::Array(batch)).unwrap();

    let mut group = c.benchmark_group("tsbs_devops");
    group.throughput(Throughput::Elements(400));
    group.sample_size(20);

    group.bench_function("100hosts_4metrics", |b| {
        b.iter(|| {
            let resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .unwrap();
            assert!(resp.status().is_success());
        });
    });

    group.finish();
}

/// Simulates replicated write throughput by writing the same batch
/// with a replication factor of 3 (3× sequential writes).
fn bench_replicated_write_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/write", addr.port());

    // 1000 points × replication factor 3 = 3000 effective writes.
    let batch = generate_http_batch(1000);
    let body = serde_json::to_string(&batch).unwrap();
    let replication_factor = 3;

    let mut group = c.benchmark_group("replicated_write");
    group.throughput(Throughput::Elements(1000 * replication_factor as u64));
    group.sample_size(10);

    group.bench_function("1K_pts_rf3", |b| {
        b.iter(|| {
            for _ in 0..replication_factor {
                let resp = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(body.clone())
                    .send()
                    .unwrap();
                assert!(resp.status().is_success());
            }
        });
    });

    group.finish();
}

/// Object-store tiered reads: write data to in-memory object store
/// then read back, measuring put/get throughput.
fn bench_objstore_tiered_read(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("objstore_tiered_read");
    group.sample_size(50);

    // Simulate a 1 MB segment transfer (typical cold-tiered segment size).
    let segment_data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
    let segment_bytes = bytes::Bytes::from(segment_data.clone());

    group.throughput(Throughput::Bytes(1_000_000));

    group.bench_function("1MB_segment_roundtrip", |b| {
        b.iter(|| {
            rt.block_on(async {
                use object_store::ObjectStoreExt;
                let store = object_store::memory::InMemory::new();
                let key = object_store::path::Path::from("segments/cold/bench_seg.parquet");
                let payload: object_store::PutPayload = segment_bytes.clone().into();
                store.put(&key, payload).await.unwrap();
                let result = store.get(&key).await.unwrap().bytes().await.unwrap();
                assert_eq!(result.len(), 1_000_000);
            });
        });
    });

    group.finish();
}

/// Multi-tenant write/query benchmark: creates 100 namespaces and writes
/// 10 points per namespace through the HTTP API.
fn bench_multi_tenant_100ns(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());

    let registry = Arc::new(chronix_security::tenant::NamespaceRegistry::new());
    // Create 99 extra namespaces (default already exists).
    for i in 1..100u32 {
        let id = chronix_core::NamespaceId::new(format!("tenant_{i:03}")).unwrap();
        registry
            .create_namespace(
                id,
                format!("Tenant {i}"),
                format!("owner_{i}"),
                chronix_core::NamespaceQuota {
                    max_series_count: 10_000,
                    max_ingestion_rate: 10_000,
                    max_storage_bytes: 1024 * 1024 * 1024,
                    max_measurements: 100,
                    max_request_rps: 0,
                    max_request_burst: 0,
                },
            )
            .unwrap();
    }

    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: None,
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: Some(registry.clone()),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/write", addr.port());
    let mut rng = rand::rng();

    // Pre-build per-namespace write payloads.
    let ns_names: Vec<String> = std::iter::once("default".to_string())
        .chain((1..100u32).map(|i| format!("tenant_{i:03}")))
        .collect();
    let payloads: Vec<(String, String)> = ns_names
        .iter()
        .map(|ns| {
            let pts: Vec<serde_json::Value> = (0..10)
                .map(|j| {
                    serde_json::json!({
                        "measurement": format!("metric_{ns}"),
                        "tags": { "host": "srv1" },
                        "fields": { "value": rng.random::<f64>() * 100.0 },
                        "timestamp": 1_609_459_200_000_000_000i64 + (j * 1_000_000_000i64)
                    })
                })
                .collect();
            (
                ns.clone(),
                serde_json::to_string(&serde_json::Value::Array(pts)).unwrap(),
            )
        })
        .collect();

    let mut group = c.benchmark_group("multi_tenant");
    group.throughput(Throughput::Elements(1000)); // 100 namespaces × 10 points
    group.sample_size(10);

    group.bench_function("100ns_10pts_each", |b| {
        b.iter(|| {
            for (ns, body) in &payloads {
                let resp = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .header("X-Namespace", ns.as_str())
                    .body(body.clone())
                    .send()
                    .unwrap();
                assert!(resp.status().is_success());
            }
        });
    });

    group.finish();
}

/// Large-scale targeted query: 1M pre-loaded points, then query
/// a narrow time range — tests index efficiency on dense data.
fn bench_large_scale_targeted_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (db, _tmp) = setup_db();

    // Pre-populate 1M points.
    let mut rng = rand::rng();
    for chunk_start in (0..1_000_000i64).step_by(10_000) {
        let points: Vec<Point> = (chunk_start..chunk_start + 10_000)
            .map(|i| {
                let tags: BTreeMap<String, String> =
                    [("host".to_string(), "srv_dense".to_string())].into();
                let fields: BTreeMap<String, FieldValue> = [(
                    "value".to_string(),
                    FieldValue::F64(rng.random::<f64>() * 100.0),
                )]
                .into();
                let key = SeriesKey::new("bench_dense", tags).unwrap();
                let ts = 1_609_459_200_000_000_000i64 + (i * 1_000_000_000);
                Point::new(key, fields, ts).unwrap()
            })
            .collect();
        let _ = db.insert_batch(&points).unwrap();
    }

    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
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

    let metrics_builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    rt.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));

    let client = reqwest::blocking::Client::new();
    let url = format!("http://127.0.0.1:{}/api/v1/query", addr.port());

    // Targeted query: small window within 1M points.
    let query_body = serde_json::json!({
        "measurement": "bench_dense",
        "tags": { "host": "srv_dense" },
        "range": {
            "start": 1_609_459_200_000_000_000i64 + 500_000 * 1_000_000_000i64,
            "end":   1_609_459_200_000_000_000i64 + 501_000 * 1_000_000_000i64
        }
    });
    let body = serde_json::to_string(&query_body).unwrap();

    let mut group = c.benchmark_group("large_scale_targeted");
    group.sample_size(30);

    group.bench_function("1M_pts_1K_range", |b| {
        b.iter(|| {
            let resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .unwrap();
            assert!(resp.status().is_success());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_http_write,
    bench_http_query,
    bench_grpc_write,
    bench_grpc_stream_write,
    bench_grpc_query,
    bench_concurrent_writers_readers,
    bench_scale_http_write_10k,
    bench_scale_multi_measurement,
    bench_scale_targeted_query,
    bench_tsbs_devops_workload,
    bench_replicated_write_throughput,
    bench_objstore_tiered_read,
    bench_multi_tenant_100ns,
    bench_large_scale_targeted_query,
);
criterion_main!(benches);
