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
        pipeline: None,
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

    let all = data(&base, "/api/v1/prom/series?match[]=cpu_usage").await;
    assert_eq!(all.as_array().unwrap().len(), 2, "{all}");

    for (query, expected_host) in [
        (
            "/api/v1/prom/series?match[]=%7B__name__%3D%22cpu_usage%22%2Chost%3D%22a%22%7D",
            "a",
        ),
        (
            "/api/v1/prom/series?match[]=%7B__name__%3D%22cpu_usage%22%2Chost%3D~%22b%22%7D",
            "b",
        ),
        (
            "/api/v1/prom/series?match[]=%7B__name__%3D%22cpu_usage%22%2Chost!%3D%22a%22%7D",
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
        "/api/v1/prom/series?match[]=%7B__name__%3D%22cpu_usage%22%2Chost%3D%22a%22%7D&match[]=mem_used",
    )
    .await;
    let arr = got.as_array().unwrap();
    assert_eq!(arr.len(), 2, "{got}");
    let names: Vec<&str> = arr
        .iter()
        .map(|s| s["__name__"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"cpu_usage") && names.contains(&"mem_used"),
        "{got}"
    );
}

#[tokio::test]
async fn label_values_are_scoped_by_the_matcher() {
    let (base, _tmp) = server_with_data().await;

    // Without a matcher, every measurement's values.
    let hosts = strings(&data(&base, "/api/v1/prom/label/host/values").await);
    assert_eq!(hosts, vec!["a", "b"]);

    // `mem` only has host a.
    let mem_hosts = strings(&data(&base, "/api/v1/prom/label/host/values?match[]=mem_used").await);
    assert_eq!(mem_hosts, vec!["a"]);

    // The dc of cpu{host="a"} is eu, not both.
    let dcs = strings(
        &data(
            &base,
            "/api/v1/prom/label/dc/values?match[]=%7B__name__%3D%22cpu_usage%22%2Chost%3D%22a%22%7D",
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
    let mem = strings(&data(&base, "/api/v1/prom/labels?match[]=mem_used").await);
    assert_eq!(mem, vec!["__name__", "host"]);
}

/// The two calls Grafana's metric browser makes. Both sit on the `LIMIT 1`
/// existence probe, so both go dark if it cannot stream.
///
/// They list **metrics**, not measurements: the browser's job is to offer
/// names the user can then type into a query, and `cpu` is not one — the
/// series of a measurement `cpu` holding a field `usage` is `cpu_usage`.
#[tokio::test]
async fn name_values_and_metadata_list_the_metrics() {
    let (base, _tmp) = server_with_data().await;

    let names = strings(&data(&base, "/api/v1/prom/label/__name__/values").await);
    assert_eq!(names, vec!["cpu_usage", "mem_used"]);

    let scoped = strings(
        &data(
            &base,
            "/api/v1/prom/label/__name__/values?match[]=cpu_usage",
        )
        .await,
    );
    assert_eq!(scoped, vec!["cpu_usage"]);

    let metadata = data(&base, "/api/v1/prom/metadata").await;
    let obj = metadata.as_object().expect("a metadata object");
    assert!(obj.contains_key("cpu_usage"), "{metadata}");
    assert!(obj.contains_key("mem_used"), "{metadata}");
    assert!(
        !obj.contains_key("cpu"),
        "a measurement is not a metric: {metadata}"
    );
}

/// Discovery and evaluation answer the same question the same way.
///
/// This is the test the whole subsystem was missing. `/label/__name__/values`
/// listed *measurements*, `/query` returned series named
/// `measurement_field`, and `/series` reported a third thing — so a user could
/// pick a name out of Grafana's metric browser, paste it into a query and get
/// nothing, and the name the query *did* return was not selectable either.
/// Each endpoint had a passing test of its own.
#[tokio::test]
async fn every_name_discovery_offers_is_a_selector_that_answers() {
    let (base, _tmp) = server_with_data().await;

    let names = strings(&data(&base, "/api/v1/prom/label/__name__/values").await);
    assert!(!names.is_empty());

    for name in &names {
        // The metric browser's name, typed into the query field.
        let result = data(&base, &format!("/api/v1/prom/query?query={name}")).await;
        let series = result["result"].as_array().expect("a vector");
        assert!(
            !series.is_empty(),
            "/label/__name__/values offers {name}, which /query answers with nothing"
        );
        for s in series {
            assert_eq!(s["metric"]["__name__"], name.as_str(), "{s}");
        }

        // …and `/series`, which is what a dashboard variable reads.
        let listed = data(&base, &format!("/api/v1/prom/series?match[]={name}")).await;
        let listed = listed.as_array().expect("an array");
        assert_eq!(
            listed.len(),
            series.len(),
            "/series and /query disagree about {name}: {listed:?} vs {series:?}"
        );
        for s in listed {
            assert_eq!(s["__name__"], name.as_str(), "{s}");
        }
    }

    // A `__name__` regex is answered by both, and by the same resolution:
    // `/series` used to drop the selector entirely and return an empty array
    // while `/query` returned every series in the database.
    let all = data(
        &base,
        "/api/v1/prom/series?match[]=%7B__name__%3D~%22.%2B%22%7D",
    )
    .await;
    let mut listed_names: Vec<&str> = all
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["__name__"].as_str().unwrap())
        .collect();
    listed_names.sort_unstable();
    listed_names.dedup();
    assert_eq!(listed_names, names, "{all}");
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

// ─── What Grafana actually sends ──────────────────────────────────────
//
// Grafana's Prometheus datasource defaults to `httpMethod: POST` and sends
// `application/x-www-form-urlencoded`, and it derives every path by
// appending to the datasource URL — so it asks for `/api/v1/query`, not
// `/api/v1/prom/query`. Handlers that read only the query string, mounted
// only under a prefix nothing derives, answered every panel with a 400.
// These tests send the bytes rather than the specification.

/// A form-encoded POST to the path a Prometheus datasource derives.
#[tokio::test]
async fn grafana_posts_a_form_body_to_the_derived_path() {
    let (base, _tmp) = server_with_data().await;
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{base}/api/v1/query"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("query=cpu_usage&time=1725364800")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "Grafana's default POST must be accepted at the derived path"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "vector");

    // The same query as a GET, which is what `httpMethod: GET` sends.
    let body: Value = c
        .get(format!("{base}/api/v1/query"))
        .query(&[("query", "cpu_usage")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "success");

    // And a range query, with the duration spelling Grafana uses for `step`
    // when a dashboard sets an interval.
    let resp = c
        .post(format!("{base}/api/v1/query_range"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("query=cpu_usage&start=1725364800&end=1725365100&step=15s")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
}

/// `time`, `start` and `end` are "RFC 3339 **or** a Unix timestamp", and
/// `step` is "a duration **or** seconds". Both spellings must work.
#[tokio::test]
async fn timestamps_may_be_rfc3339_and_steps_may_be_durations() {
    let (base, _tmp) = server_with_data().await;
    let c = reqwest::Client::new();

    for (start, end, step) in [
        ("2024-09-03T12:00:00Z", "2024-09-03T12:05:00Z", "1m"),
        ("1725364800", "1725365100", "60"),
        ("1725364800.500", "1725365100.500", "1m30s"),
    ] {
        let resp = c
            .get(format!("{base}/api/v1/query_range"))
            .query(&[
                ("query", "cpu"),
                ("start", start),
                ("end", end),
                ("step", step),
            ])
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "start={start} end={end} step={step} was rejected"
        );
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success", "start={start} step={step}");
    }
}

/// A failing query answers with the status code Prometheus uses, because
/// clients branch on it: 400 is permanent, 503 is worth retrying. Every
/// error used to come back as HTTP 200 with an error body, which Grafana
/// reads as a successful empty result.
#[tokio::test]
async fn errors_carry_prometheus_status_codes() {
    let (base, _tmp) = server_with_data().await;
    let c = reqwest::Client::new();

    // A syntax error is bad_data → 400.
    let resp = c
        .get(format!("{base}/api/v1/query"))
        .query(&[("query", "cpu{{{")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert!(body["error"].is_string());

    // A missing required parameter is also bad_data.
    let resp = c
        .get(format!("{base}/api/v1/query_range"))
        .query(&[("query", "cpu"), ("start", "0"), ("end", "1")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // An unparseable timestamp is refused rather than silently becoming
    // epoch 0 and answering with whatever was stored in 1970.
    for bad in ["NaN", "yesterday", "inf"] {
        let resp = c
            .get(format!("{base}/api/v1/query"))
            .query(&[("query", "cpu"), ("time", bad)])
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "time={bad} must be refused"
        );
    }
}

/// The endpoints Grafana probes when a datasource is saved or tested. A 404
/// here makes the datasource report itself unhealthy even though queries
/// work, which is the first thing a new user sees.
#[tokio::test]
async fn the_capability_endpoints_grafana_probes_answer() {
    let (base, _tmp) = server_with_data().await;
    let c = reqwest::Client::new();

    let body: Value = c
        .get(format!("{base}/api/v1/status/buildinfo"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "success");
    assert!(body["data"]["version"].is_string());

    for (path, key) in [("/api/v1/rules", "groups"), ("/api/v1/alerts", "alerts")] {
        let body: Value = c
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(body["status"], "success", "{path}");
        assert!(body["data"][key].as_array().unwrap().is_empty(), "{path}");
    }

    let body: Value = c
        .get(format!("{base}/api/v1/query_exemplars"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "success");
}

/// The discovery endpoints answer on POST too — Grafana's metric browser
/// posts `match[]` in a form body.
#[tokio::test]
async fn discovery_endpoints_accept_a_posted_form() {
    let (base, _tmp) = server_with_data().await;
    let c = reqwest::Client::new();

    let body: Value = c
        .post(format!("{base}/api/v1/labels"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("match%5B%5D=cpu_usage")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "success");
    let labels: Vec<&str> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(labels.contains(&"host"), "got {labels:?}");

    let body: Value = c
        .post(format!("{base}/api/v1/series"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("match%5B%5D=cpu_usage")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "success");
    assert!(!body["data"].as_array().unwrap().is_empty());

    let body: Value = c
        .post(format!("{base}/api/v1/label/host/values"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("match%5B%5D=cpu_usage")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
}
