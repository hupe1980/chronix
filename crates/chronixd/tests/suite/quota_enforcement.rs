//! Namespace quotas on every write surface the server mounts.
//!
//! The series quota existed, was tested, and was called by nothing: a tenant
//! limited to a thousand series grew without bound on every protocol, and its
//! usage counter never moved. The route list here is read out of the router
//! itself and filtered by the Cedar action each route maps to, so a write
//! route added later that has no case fails this file rather than slipping
//! past the quota.

use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::Chronix;
use chronix::prelude::*;
use chronixd::http::{AppState, SharedState};
use chronixd::rate_limit::{NamespaceIngestLimiter, NamespaceRateLimiter};
use chronixd::server::build_router;

use crate::route_scan;

/// Write routes that do not ingest points. Each is a `Write` under Cedar
/// because it changes state, and none can add a series, so none has a series
/// quota to reach. A route missing from both this list and `ingest` fails
/// [`every_write_route_reaches_the_series_quota`].
const WRITES_WITHOUT_POINTS: &[&str] = &[
    "/api/v1/measurements/{name}/schema/fields", // declares a field's type
    "/api/v1/measurements/{name}/restore",       // un-drops a measurement
    "/api/v1/rollups/{name}/refresh",            // recomputes derived rows
    "/api/v1/rollups",                           // declares a rollup
    "/api/v1/rollups/{name}",
    "/api/v1/detectors", // declares a detector
    "/api/v1/detectors/{name}",
    "/api/v1/triggers", // declares a trigger
    "/api/v1/triggers/{name}",
    "/api/v1/measurements/{name}", // POST is not mounted; DELETE drops
];

struct Server {
    base: String,
    db: Arc<Chronix>,
    _tmp: TempDir,
}

async fn server_over(tmp: TempDir) -> Server {
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));
    let registry = chronix_security::tenant::NamespaceRegistry::open(tmp.path().join("namespaces"))
        .expect("registry");

    // Exactly what `server::run` does before it binds: every persisted quota
    // is put in force.
    let requests = NamespaceRateLimiter::new();
    let ingest = NamespaceIngestLimiter::new();
    for ns in registry.list_all() {
        chronixd::namespace::apply_quota(
            &db,
            &requests,
            &ingest,
            ns.info.id.as_str(),
            &ns.info.quota,
        );
    }

    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts: chronixd::namespace::SqlContexts::new(db.clone()),
        auth_state: None,
        namespace_registry: Arc::new(registry),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: None,
        audit_logger: None,
        config: chronixd::config::ServerConfig {
            server: chronixd::config::ServerSettings {
                multi_tenancy: true,
                ..Default::default()
            },
            ..Default::default()
        },
        namespace_rate_limiter: requests,
        namespace_ingest_limiter: ingest,
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
        query_slots: Default::default(),
    });

    let metrics_handle = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder()
        .handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Server {
        base: format!("http://{addr}"),
        db,
        _tmp: tmp,
    }
}

async fn server() -> Server {
    server_over(TempDir::new().expect("tempdir")).await
}

fn client() -> reqwest::Client {
    chronixd::tls::ensure_crypto_provider();
    reqwest::Client::new()
}

/// Create a namespace through the API, with the quota fields given.
async fn create_namespace(base: &str, name: &str, quota: Value) {
    let mut body = json!({ "name": name });
    for (k, v) in quota.as_object().unwrap() {
        body[k] = v.clone();
    }
    let resp = client()
        .post(format!("{base}/api/v1/namespaces"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        resp.text().await.unwrap()
    );
}

async fn usage(base: &str, ns: &str) -> Value {
    client()
        .get(format!("{base}/api/v1/namespaces/{ns}/usage"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

const TS_NS: i64 = 1_700_000_000_000_000_000;
const TS_MS: i64 = 1_700_000_000_000;

fn snappy(bytes: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(bytes).unwrap()
}

fn otlp_body(host: &str) -> Value {
    json!({
        "resourceMetrics": [{
            "resource": {"attributes": [{"key": "host", "value": {"stringValue": host}}]},
            "scopeMetrics": [{"metrics": [{
                "name": "quota_probe",
                "gauge": {"dataPoints": [{"timeUnixNano": TS_NS.to_string(), "asDouble": 1.0}]}
            }]}]
        }]
    })
}

/// Write one point of a new series `host` through `route`, the way that
/// route's real clients send it. `None` for a route this harness does not
/// know — which the caller turns into a failure.
async fn write_through(base: &str, route: &str, ns: &str, host: &str) -> Option<reqwest::Response> {
    let c = client();
    let line = format!("quota_probe,host={host} value=1 {TS_NS}\n");
    let req = match route {
        "/write" => c.post(format!("{base}/write?db=x")).body(line),
        "/api/v2/write" => c
            .post(format!("{base}/api/v2/write?org=o&bucket=b"))
            .body(line),
        "/api/v1/write/influx" => c.post(format!("{base}/api/v1/write/influx")).body(line),
        "/api/v1/write" => c.post(format!("{base}/api/v1/write")).json(&json!([{
            "measurement": "quota_probe",
            "tags": {"host": host},
            "fields": {"value": 1.0},
            "timestamp": TS_NS,
        }])),
        "/api/v1/prom/write" => {
            let body = chronixd::prom_proto::WriteRequest {
                timeseries: vec![chronixd::prom_proto::TimeSeries {
                    labels: vec![
                        chronixd::prom_proto::Label {
                            name: "__name__".into(),
                            value: "quota_probe".into(),
                        },
                        chronixd::prom_proto::Label {
                            name: "host".into(),
                            value: host.into(),
                        },
                    ],
                    samples: vec![chronixd::prom_proto::Sample {
                        value: 1.0,
                        timestamp: TS_MS,
                    }],
                    ..Default::default()
                }],
            };
            c.post(format!("{base}/api/v1/prom/write"))
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(snappy(&body.encode_to_vec()))
        }
        "/v1/metrics" | "/api/v1/otlp/metrics" => c
            .post(format!("{base}{route}"))
            .header("content-type", "application/json")
            .json(&otlp_body(host)),
        _ => return None,
    };
    Some(req.header("X-Namespace", ns).send().await.unwrap())
}

/// Remote write 2.0: the same route, the other encoding.
async fn write_remote_v2(base: &str, ns: &str, host: &str) -> reqwest::Response {
    use chronixd::prom_proto_v2 as v2;
    let body = v2::Request {
        symbols: vec![
            String::new(),
            "__name__".into(),
            "quota_probe".into(),
            "host".into(),
            host.into(),
        ],
        timeseries: vec![v2::TimeSeries {
            label_refs: vec![1, 2, 3, 4],
            samples: vec![v2::Sample {
                value: 1.0,
                timestamp: TS_MS,
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    client()
        .post(format!("{base}/api/v1/prom/write"))
        .header(
            "Content-Type",
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        )
        .header("Content-Encoding", "snappy")
        .header("X-Namespace", ns)
        .body(snappy(&body.encode_to_vec()))
        .send()
        .await
        .unwrap()
}

async fn assert_refused_by_quota(resp: reqwest::Response, route: &str) {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "{route}: a series over the namespace quota must be a permanent 400; got {status}: {text}"
    );
    assert!(
        text.contains("series quota") && text.contains("max_series_count"),
        "{route}: the refusal must name the quota and the remedy; got {text}"
    );
}

/// **SC-001**: every ingesting write route is held to the series quota.
#[tokio::test]
async fn every_write_route_reaches_the_series_quota() {
    let s = server().await;
    let mut unknown = Vec::new();
    let mut checked = 0;

    for (i, route) in route_scan::mounted_paths().into_iter().enumerate() {
        let action = chronixd::namespace::action_for_route(&route, &axum::http::Method::POST);
        if action != Some(chronix_security::authz::ChronixAction::Write)
            || WRITES_WITHOUT_POINTS.contains(&route.as_str())
        {
            continue;
        }
        let ns = format!("q{i}");
        create_namespace(&s.base, &ns, json!({"max_series": 1})).await;
        let Some(first) = write_through(&s.base, &route, &ns, "first").await else {
            unknown.push(route);
            continue;
        };
        assert!(
            first.status().is_success(),
            "{route}: first write {}",
            first.status()
        );
        let second = write_through(&s.base, &route, &ns, "second").await.unwrap();
        assert_refused_by_quota(second, &route).await;
        // The refused series was never admitted.
        assert_eq!(usage(&s.base, &ns).await["series"], 1, "{route}");
        checked += 1;
    }

    // The other encoding of remote write.
    create_namespace(&s.base, "qv2", json!({"max_series": 1})).await;
    assert!(
        write_remote_v2(&s.base, "qv2", "first")
            .await
            .status()
            .is_success()
    );
    assert_refused_by_quota(
        write_remote_v2(&s.base, "qv2", "second").await,
        "remote write 2.0",
    )
    .await;
    checked += 1;

    assert!(
        unknown.is_empty(),
        "write routes with no quota case — add one to `write_through`, or to \
         WRITES_WITHOUT_POINTS if the route cannot add a series: {unknown:?}"
    );
    assert!(
        checked >= 8,
        "the route scan found only {checked} ingest routes"
    );
}

/// The measurement quota on the wire, and usage reported beside the quota.
#[tokio::test]
async fn the_measurement_quota_and_usage_report() {
    let s = server().await;
    create_namespace(&s.base, "m", json!({"max_measurements": 1})).await;
    let write = |measurement: &'static str| {
        let base = s.base.clone();
        async move {
            client()
                .post(format!("{base}/api/v1/write/influx"))
                .header("X-Namespace", "m")
                .body(format!("{measurement},host=a value=1 {TS_NS}\n"))
                .send()
                .await
                .unwrap()
        }
    };
    assert!(write("cpu").await.status().is_success());
    let refused = write("mem").await;
    assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    let body: Value = refused.json().await.unwrap();
    assert_eq!(body["code"], "NAMESPACE_QUOTA_EXCEEDED");
    assert!(body["error"].as_str().unwrap().contains("max_measurements"));

    let report = usage(&s.base, "m").await;
    assert_eq!(report["series"], 1);
    assert_eq!(report["measurements"], 1);
    assert_eq!(report["quota"]["max_measurements"], 1);
}

/// A quota changed through the API is in force for the next write, and a
/// namespace created without limits is unlimited rather than unwritable.
#[tokio::test]
async fn quota_updates_take_effect_and_zero_means_unlimited() {
    let s = server().await;
    create_namespace(&s.base, "u", json!({})).await;
    for host in ["a", "b", "c"] {
        let r = write_through(&s.base, "/api/v1/write/influx", "u", host)
            .await
            .unwrap();
        assert!(
            r.status().is_success(),
            "an unlimited namespace refused a write"
        );
    }
    let resp = client()
        .put(format!("{}/api/v1/namespaces/u/quota", s.base))
        .json(&json!({
            "max_series": 3, "max_ingestion_rate": 0, "max_measurements": 0,
            "max_request_rps": 0, "max_request_burst": 0
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let r = write_through(&s.base, "/api/v1/write/influx", "u", "d")
        .await
        .unwrap();
    assert_refused_by_quota(r, "after an update").await;
    // Existing series stay writable at the quota.
    let r = write_through(&s.base, "/api/v1/write/influx", "u", "a")
        .await
        .unwrap();
    assert!(r.status().is_success());
}

/// **SC-005**: the removed storage quota is refused by name.
#[tokio::test]
async fn the_removed_storage_quota_is_refused_by_name() {
    let s = server().await;
    let resp = client()
        .post(format!("{}/api/v1/namespaces", s.base))
        .json(&json!({"name": "s", "max_storage_bytes": 1024}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "INVALID_QUOTA");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("max_storage_bytes was removed")
    );
}

// ── Ingestion rate ─────────────────────────────────────────────────────

fn lines(n: usize, host: &str) -> String {
    (0..n)
        .map(|i| format!("rate_probe,host={host} value={i} {}\n", TS_NS + i as i64))
        .collect()
}

/// **US2**: a burst over the rate is `429` with `Retry-After`, nothing of it
/// is written, and a batch larger than one second's allowance is a `400`.
#[tokio::test]
async fn the_ingestion_rate_is_admitted_per_batch() {
    let s = server().await;
    create_namespace(&s.base, "r", json!({"max_ingestion_rate": 100})).await;
    let post = |body: String| {
        let base = s.base.clone();
        async move {
            client()
                .post(format!("{base}/api/v1/write/influx"))
                .header("X-Namespace", "r")
                .body(body)
                .send()
                .await
                .unwrap()
        }
    };

    assert!(post(lines(100, "a")).await.status().is_success());
    let over = post(lines(50, "b")).await;
    assert_eq!(over.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(over.headers().contains_key("retry-after"));
    let body: Value = over.json().await.unwrap();
    assert_eq!(body["code"], "NAMESPACE_RATE_LIMITED");
    assert_eq!(
        s.db.namespace_usage("r").series,
        1,
        "a rate-limited batch must write nothing"
    );

    let too_big = post(lines(101, "c")).await;
    assert_eq!(too_big.status(), StatusCode::BAD_REQUEST);
    let body: Value = too_big.json().await.unwrap();
    assert_eq!(body["code"], "BATCH_EXCEEDS_NAMESPACE_RATE");

    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert!(post(lines(50, "b")).await.status().is_success());
}

// ── gRPC and Flight SQL ────────────────────────────────────────────────

fn grpc_point(host: &str) -> chronixd::proto::Point {
    chronixd::proto::Point {
        measurement: "quota_probe".into(),
        tags: vec![chronixd::proto::Tag {
            key: "host".into(),
            value: host.into(),
        }],
        fields: vec![chronixd::proto::Field {
            key: "value".into(),
            value: Some(chronixd::proto::FieldValue {
                value: Some(chronixd::proto::field_value::Value::Float64(1.0)),
            }),
        }],
        timestamp: TS_NS,
    }
}

async fn serve_grpc(
    db: Arc<Chronix>,
    requests: NamespaceRateLimiter,
    ingest: NamespaceIngestLimiter,
) -> String {
    let service = chronixd::grpc::ChronixGrpcService::new(db.clone(), std::time::Instant::now())
        .with_multi_tenancy(true)
        .with_limiters(requests.clone(), ingest.clone());
    let flight = chronixd::flight::ChronixFlightSqlService::new(db)
        .with_multi_tenancy(true)
        .with_limiters(requests, ingest);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                chronixd::proto::chronix_service_server::ChronixServiceServer::new(service),
            )
            .add_service(flight.into_server())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    format!("http://{addr}")
}

fn with_namespace<T>(msg: T, ns: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut()
        .insert(chronixd::namespace::METADATA_KEY, ns.parse().unwrap());
    req
}

/// The gRPC write is held to the same series quota — `RESOURCE_EXHAUSTED`,
/// no pushback, because the refusal is permanent.
#[tokio::test]
async fn grpc_write_reaches_the_series_quota() {
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(
        Chronix::open(
            ChronixConfigBuilder::default()
                .data_dir(tmp.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    db.set_namespace_limits(
        "g",
        chronix::NamespaceLimits {
            max_series: 1,
            max_measurements: u32::MAX,
        },
    );
    let endpoint = serve_grpc(
        db,
        NamespaceRateLimiter::new(),
        NamespaceIngestLimiter::new(),
    )
    .await;
    let mut client =
        chronixd::proto::chronix_service_client::ChronixServiceClient::connect(endpoint)
            .await
            .unwrap();
    let write = |host: &str| {
        with_namespace(
            chronixd::proto::WriteRequest {
                points: vec![grpc_point(host)],
                backfill: false,
            },
            "g",
        )
    };
    client.write(write("first")).await.unwrap();
    let err = client.write(write("second")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(err.message().contains("series quota"), "{}", err.message());
    assert!(err.metadata().get("grpc-retry-pushback-ms").is_none());
}

/// Flight SQL `DoPut` is held to the same series quota.
#[tokio::test]
async fn flight_do_put_reaches_the_series_quota() {
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_flight::sql::{CommandStatementUpdate, ProstMessageExt};
    use arrow_flight::{FlightDescriptor, flight_service_client::FlightServiceClient};

    let tmp = TempDir::new().unwrap();
    let db = Arc::new(
        Chronix::open(
            ChronixConfigBuilder::default()
                .data_dir(tmp.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    db.set_namespace_limits(
        "f",
        chronix::NamespaceLimits {
            max_series: 1,
            max_measurements: u32::MAX,
        },
    );
    let endpoint = serve_grpc(
        db,
        NamespaceRateLimiter::new(),
        NamespaceIngestLimiter::new(),
    )
    .await;
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightServiceClient::new(channel);

    let put = |host: &'static str| {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix::chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![TS_NS])),
                Arc::new(StringArray::from(vec![host])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let cmd = CommandStatementUpdate {
            query: "quota_probe".to_string(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let data: Vec<_> =
            arrow_flight::utils::batches_to_flight_data(schema.as_ref(), vec![batch])
                .unwrap()
                .into_iter()
                .enumerate()
                .map(|(i, mut d)| {
                    if i == 0 {
                        d.flight_descriptor = Some(descriptor.clone());
                    }
                    d
                })
                .collect();
        with_namespace(futures::stream::iter(data), "f")
    };

    let first = client.do_put(put("first")).await.unwrap().into_inner();
    let _: Vec<_> = futures::StreamExt::collect::<Vec<_>>(first).await;
    let refused = match client.do_put(put("second")).await {
        Err(status) => status,
        Ok(stream) => {
            let results: Vec<_> = futures::StreamExt::collect::<Vec<_>>(stream.into_inner()).await;
            results
                .into_iter()
                .find_map(Result::err)
                .expect("a DoPut over the quota must be refused")
        }
    };
    assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
    assert!(
        refused.message().contains("series quota"),
        "{}",
        refused.message()
    );
}

// ── Restart (US3) ──────────────────────────────────────────────────────

/// **SC-003**: request limits persisted in the registry are in force after a
/// restart, on HTTP and gRPC alike. They used to start empty: a tenant
/// limited to one request a second was unlimited from the first request
/// after a deploy until somebody re-saved its quota.
#[tokio::test]
async fn request_rate_survives_restart() {
    let first = server().await;
    create_namespace(
        &first.base,
        "limited",
        json!({"max_request_rps": 1, "max_request_burst": 1}),
    )
    .await;

    // A second process over the persisted registry.
    let restarted = TempDir::new().unwrap();
    std::fs::create_dir_all(restarted.path().join("namespaces")).unwrap();
    std::fs::copy(
        first._tmp.path().join("namespaces/namespaces.json"),
        restarted.path().join("namespaces/namespaces.json"),
    )
    .unwrap();
    let s = server_over(restarted).await;

    let get = || {
        client()
            .get(format!("{}/api/v1/measurements", s.base))
            .header("X-Namespace", "limited")
            .send()
    };
    assert!(get().await.unwrap().status().is_success());
    assert_eq!(
        get().await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS,
        "HTTP: the persisted request limit was lost at restart"
    );

    // gRPC, with limiters built the way startup builds them.
    let registry =
        chronix_security::tenant::NamespaceRegistry::open(s._tmp.path().join("namespaces"))
            .unwrap();
    let (requests, ingest) = (NamespaceRateLimiter::new(), NamespaceIngestLimiter::new());
    for ns in registry.list_all() {
        chronixd::namespace::apply_quota(
            &s.db,
            &requests,
            &ingest,
            ns.info.id.as_str(),
            &ns.info.quota,
        );
    }
    let endpoint = serve_grpc(s.db.clone(), requests, ingest).await;
    let mut client =
        chronixd::proto::chronix_service_client::ChronixServiceClient::connect(endpoint)
            .await
            .unwrap();
    let write = || {
        with_namespace(
            chronixd::proto::WriteRequest {
                points: vec![grpc_point("a")],
                backfill: false,
            },
            "limited",
        )
    };
    client.write(write()).await.unwrap();
    let err = client.write(write()).await.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::ResourceExhausted,
        "gRPC bypassed the request limit"
    );
    assert!(err.metadata().get("grpc-retry-pushback-ms").is_some());
}

/// A namespace-scoped caller sees its own series and its own limit, and the
/// headroom it is shown is the headroom admission enforces.
#[tokio::test]
async fn status_tsdb_is_scoped_to_the_callers_namespace() {
    let s = server().await;
    create_namespace(&s.base, "acme", json!({"max_series": 5})).await;
    create_namespace(&s.base, "globex", json!({})).await;
    for (ns, line) in [
        ("acme", "cpu,host=a value=1"),
        ("acme", "cpu,host=b value=1"),
        ("globex", "secret,host=z value=1"),
    ] {
        let resp = client()
            .post(format!("{}/api/v1/write/influx", s.base))
            .header("X-Namespace", ns)
            .body(format!("{line} {TS_NS}\n"))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());
    }

    let body: Value = client()
        .get(format!("{}/api/v1/status/tsdb", s.base))
        .header("X-Namespace", "acme")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let data = &body["data"];
    assert_eq!(data["headStats"]["numSeries"], 2);
    assert_eq!(data["chronix"]["scope"], "namespace:acme");
    assert_eq!(data["chronix"]["enforced"], true);
    assert_eq!(data["chronix"]["budget"], 5);
    assert_eq!(data["chronix"]["headroom"], 3);
    assert_eq!(data["chronix"]["used"], s.db.namespace_usage("acme").series);
    let text = body.to_string();
    assert!(!text.contains("secret") && !text.contains("__namespace__"));
}

/// A misspelt quota field at creation is refused, not silently dropped —
/// dropped, it would create an unlimited namespace.
#[tokio::test]
async fn an_unknown_field_at_creation_is_refused() {
    let s = server().await;
    let resp = client()
        .post(format!("{}/api/v1/namespaces", s.base))
        .json(&json!({"name": "typo", "max_series_count": 5}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_client_error(), "{}", resp.status());
    assert!(resp.text().await.unwrap().contains("max_series_count"));
}
