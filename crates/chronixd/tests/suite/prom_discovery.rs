#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! The Prometheus **discovery** endpoints — `/series`, `/labels`,
//! `/label/{name}/values` and `/metadata`.
//!
//! These are what Grafana calls to populate a query editor. Every request here
//! goes over a **real query string** with repeated `match[]` keys, because
//! that is the shape a client sends and the one a hand-built test never
//! produces.

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

/// A server holding two measurements: `cpu{host,dc}` with two series, and
/// `mem{host}` with one.
async fn server_with_data() -> (String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let mut points = Vec::new();
    for i in 0..30i64 {
        let ts = now - (30 - i) * 60_000_000_000;
        for (host, dc) in [("a", "eu"), ("b", "us")] {
            points.push(
                Point::new(
                    SeriesKey::new("cpu", chronix::tags! { "host" => host, "dc" => dc }).unwrap(),
                    chronix::fields! { "usage" => i as f64 },
                    ts,
                )
                .unwrap(),
            );
        }
        points.push(
            Point::new(
                SeriesKey::new("mem", chronix::tags! { "host" => "a" }).unwrap(),
                chronix::fields! { "used" => i as f64 },
                ts,
            )
            .unwrap(),
        );
    }
    db.insert_batch(&points).unwrap().into_complete().unwrap();
    db.flush().unwrap();

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

/// GET a discovery endpoint over a **real query string** and return `data`.
///
/// The query is appended verbatim rather than built by a serializer: `match[]`
/// is a repeated key, and a serializer cannot produce one.
async fn data(base: &str, path_and_query: &str) -> Value {
    // Explicit rather than relying on another test in this binary having
    // installed it first: `reqwest` panics when the process-level rustls
    // provider is missing, so the suite would pass or fail on test order.
    chronixd::tls::ensure_crypto_provider();
    let resp = reqwest::get(format!("{base}{path_and_query}"))
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "{path_and_query} → {}",
        resp.status()
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "success", "{path_and_query}: {body}");
    body["data"].clone()
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .expect("an array")
        .iter()
        .map(|s| s.as_str().expect("a string").to_string())
        .collect()
}

#[tokio::test]
async fn series_applies_every_matcher_not_only_the_name() {
    let (base, _tmp) = server_with_data().await;

    let all = data(&base, "/api/v1/prom/series?match[]=cpu").await;
    assert_eq!(all.as_array().unwrap().len(), 2, "{all}");

    for (query, expected_host) in [
        (
            "/api/v1/prom/series?match[]=%7B__name__%3D%22cpu%22%2Chost%3D%22a%22%7D",
            "a",
        ),
        (
            "/api/v1/prom/series?match[]=%7B__name__%3D%22cpu%22%2Chost%3D~%22b%22%7D",
            "b",
        ),
        (
            "/api/v1/prom/series?match[]=%7B__name__%3D%22cpu%22%2Chost!%3D%22a%22%7D",
            "b",
        ),
    ] {
        let got = data(&base, query).await;
        let arr = got.as_array().unwrap();
        assert_eq!(arr.len(), 1, "{query} → {got}");
        assert_eq!(arr[0]["host"], expected_host, "{query} → {got}");
    }
}

#[tokio::test]
async fn series_unions_repeated_match_parameters() {
    let (base, _tmp) = server_with_data().await;
    let got = data(
        &base,
        "/api/v1/prom/series?match[]=%7B__name__%3D%22cpu%22%2Chost%3D%22a%22%7D&match[]=mem",
    )
    .await;
    let arr = got.as_array().unwrap();
    assert_eq!(arr.len(), 2, "{got}");
    let names: Vec<&str> = arr
        .iter()
        .map(|s| s["__name__"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"cpu") && names.contains(&"mem"), "{got}");
}

#[tokio::test]
async fn label_values_are_scoped_by_the_matcher() {
    let (base, _tmp) = server_with_data().await;

    // Without a matcher, every measurement's values.
    let hosts = strings(&data(&base, "/api/v1/prom/label/host/values").await);
    assert_eq!(hosts, vec!["a", "b"]);

    // `mem` only has host a.
    let mem_hosts = strings(&data(&base, "/api/v1/prom/label/host/values?match[]=mem").await);
    assert_eq!(mem_hosts, vec!["a"]);

    // The dc of cpu{host="a"} is eu, not both.
    let dcs = strings(
        &data(
            &base,
            "/api/v1/prom/label/dc/values?match[]=%7B__name__%3D%22cpu%22%2Chost%3D%22a%22%7D",
        )
        .await,
    );
    assert_eq!(dcs, vec!["eu"]);
}

#[tokio::test]
async fn label_names_are_scoped_by_the_matcher() {
    let (base, _tmp) = server_with_data().await;

    let all = strings(&data(&base, "/api/v1/prom/labels").await);
    assert_eq!(all, vec!["__name__", "dc", "host"]);

    // `mem` carries no `dc`.
    let mem = strings(&data(&base, "/api/v1/prom/labels?match[]=mem").await);
    assert_eq!(mem, vec!["__name__", "host"]);
}

/// The two calls Grafana's metric browser makes. Both sit on the `LIMIT 1`
/// existence probe, so both go dark if it cannot stream.
#[tokio::test]
async fn name_values_and_metadata_list_the_measurements() {
    let (base, _tmp) = server_with_data().await;

    let names = strings(&data(&base, "/api/v1/prom/label/__name__/values").await);
    assert_eq!(names, vec!["cpu", "mem"]);

    let scoped = strings(&data(&base, "/api/v1/prom/label/__name__/values?match[]=cpu").await);
    assert_eq!(scoped, vec!["cpu"]);

    let metadata = data(&base, "/api/v1/prom/metadata").await;
    let obj = metadata.as_object().expect("a metadata object");
    assert!(obj.contains_key("cpu"), "{metadata}");
    assert!(obj.contains_key("mem"), "{metadata}");
}

#[tokio::test]
async fn a_malformed_matcher_is_a_bad_request() {
    let (base, _tmp) = server_with_data().await;
    chronixd::tls::ensure_crypto_provider();
    let resp = reqwest::get(format!("{base}/api/v1/prom/series?match[]=%7Bhost%3D%22"))
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "a malformed selector returned {}",
        resp.status()
    );
}
