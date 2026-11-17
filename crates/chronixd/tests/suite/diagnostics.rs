//! Errors and warnings that say what was meant, over HTTP.
//!
//! - A SQL error body carries `hint` and `span` beside `error`, and `error`
//!   is complete on its own.
//! - A PromQL parse error is positioned the way Prometheus positions it.
//! - An empty PromQL result whose selector is one edit from something that
//!   exists says so in `warnings` — and nothing else in the body changes.
//! - `explain_empty=true` names the matcher that removed the last series.
//! - None of it ever offers a name that only another namespace holds.

use std::net::SocketAddr;
use std::sync::Arc;

use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::Chronix;
use chronix::prelude::*;
use chronixd::config::{ServerConfig, ServerSettings};
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i64
}

/// `(namespace, measurement, tags, field)`; the namespace is used only when
/// tenancy is on.
type Seed = (
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
    &'static str,
);

async fn server(multi_tenancy: bool, seeds: &[Seed]) -> (String, TempDir) {
    chronixd::tls::ensure_crypto_provider();
    let tmp = TempDir::new().expect("tempdir");
    let cfg = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(cfg).expect("open db"));

    let now = now_ns();
    let mut points = Vec::new();
    for (ns, measurement, tags, field) in seeds {
        let mut t: std::collections::BTreeMap<String, String> = tags
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        if multi_tenancy {
            t.insert(chronix_core::NAMESPACE_TAG.to_string(), (*ns).to_string());
        }
        for i in 0..3i64 {
            points.push(
                Point::new(
                    SeriesKey::new(*measurement, t.clone()).expect("series key"),
                    chronix::fields! { *field => i as f64 },
                    now - (3 - i) * 1_000_000_000,
                )
                .expect("point"),
            );
        }
    }
    db.insert_batch(&points)
        .expect("insert")
        .into_complete()
        .expect("all points accepted");

    let config = ServerConfig {
        server: ServerSettings {
            multi_tenancy,
            ..Default::default()
        },
        ..Default::default()
    };
    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
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
    (format!("http://{addr}"), tmp)
}

const REQUESTS: &[Seed] = &[
    (
        "default",
        "http_requests_total",
        &[("job", "api"), ("code", "ok")],
        "value",
    ),
    (
        "default",
        "http_requests_total",
        &[("job", "api"), ("code", "client_error")],
        "value",
    ),
    ("default", "cpu_usage", &[("host", "a")], "usage"),
];

async fn sql(base: &str, query: &str) -> (StatusCode, Value) {
    let r = reqwest::Client::new()
        .post(format!("{base}/api/v1/chronix/sql"))
        .json(&json!({ "query": query }))
        .send()
        .await
        .expect("sql");
    (r.status(), r.json().await.expect("json"))
}

async fn instant(base: &str, query: &str, extra: &[(&str, &str)]) -> (StatusCode, Value) {
    let mut params = vec![("query", query.to_string())];
    params.extend(extra.iter().map(|(k, v)| (*k, (*v).to_string())));
    let r = reqwest::Client::new()
        .get(format!("{base}/api/v1/query"))
        .query(&params)
        .send()
        .await
        .expect("query");
    (r.status(), r.json().await.expect("json"))
}

fn warnings(body: &Value) -> Vec<String> {
    body.get("warnings")
        .and_then(Value::as_array)
        .map(|w| {
            w.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sql_error_carries_a_hint_and_a_span() {
    let (base, _tmp) = server(false, REQUESTS).await;
    let query = "SELECT * FROM cpu_usge";
    let (status, body) = sql(&base, query).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["error"],
        r#"unknown table "cpu_usge" — did you mean "cpu_usage"?"#
    );
    assert_eq!(body["code"], "BAD_REQUEST");
    assert_eq!(body["hint"], r#"did you mean "cpu_usage"?"#);
    let (start, end) = (
        body["span"]["start"].as_u64().unwrap() as usize,
        body["span"]["end"].as_u64().unwrap() as usize,
    );
    assert_eq!(&query[start..end], "cpu_usge");

    let query = "SELECT usgae FROM cpu_usage";
    let (_, body) = sql(&base, query).await;
    assert_eq!(
        body["error"],
        r#"unknown column "usgae" — did you mean "usage"?"#
    );
    let start = body["span"]["start"].as_u64().unwrap() as usize;
    assert_eq!(&query[start..start + 5], "usgae");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_error_with_nothing_to_suggest_has_no_hint_field() {
    let (base, _tmp) = server(false, REQUESTS).await;
    // Not a name the database could have: no suggestion, no span to give.
    let (status, body) = sql(&base, "CREATE TABLE t (x INT)").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.get("hint").is_none(), "{body}");
    assert!(body.get("span").is_none(), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_promql_parse_error_is_positioned_like_prometheus() {
    let (base, _tmp) = server(false, REQUESTS).await;
    let (status, body) = instant(&base, "rate(x[5m]]", &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(
        body["error"],
        r#"1:11: parse error: expected ")", found "]""#
    );
    // No fields Prometheus clients do not expect.
    assert!(body.get("hint").is_none() && body.get("span").is_none());

    let (_, body) = instant(&base, "rat(http_requests_total[5m])", &[]).await;
    assert_eq!(
        body["error"],
        r#"1:1: parse error: unknown function with name "rat" — did you mean "rate"?"#
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_result_that_is_probably_a_typo_says_so() {
    let (base, _tmp) = server(false, REQUESTS).await;

    let (status, body) = instant(&base, "rate(http_requets_total[5m])", &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        warnings(&body),
        [r#"metric "http_requets_total" matches no series — did you mean "http_requests_total"?"#]
    );
    // Everything but `warnings` is what a warning-free empty answer is.
    let (_, plain) = instant(&base, "rate(zzzzqqqqxxxx[5m])", &[]).await;
    assert!(warnings(&plain).is_empty(), "{plain}");
    let mut stripped = body.clone();
    stripped.as_object_mut().unwrap().remove("warnings");
    assert_eq!(
        serde_json::to_string(&stripped).unwrap(),
        serde_json::to_string(&plain).unwrap()
    );

    let (_, body) = instant(&base, r#"http_requests_total{jbo="api"}"#, &[]).await;
    assert_eq!(
        warnings(&body),
        [r#"label "jbo" is not on any series of "http_requests_total" — did you mean "job"?"#]
    );

    // A metric that exists and is merely quiet in the window is an ordinary
    // empty answer.
    let (_, body) = instant(&base, "http_requests_total", &[("time", "1")]).await;
    assert!(warnings(&body).is_empty(), "{body}");

    // A classic histogram view is never offered as a correction.
    let (_, body) = instant(&base, "cpu_usage_usge_bucket", &[]).await;
    assert!(
        warnings(&body).iter().all(|w| !w.contains("_bucket\"?")),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explain_empty_names_the_matcher_that_emptied_it() {
    let (base, _tmp) = server(false, REQUESTS).await;
    let query = r#"http_requests_total{job="api", code=~"5.."}"#;

    // Without the flag, an empty answer with no typo has no warning.
    let (_, body) = instant(&base, query, &[]).await;
    assert!(warnings(&body).is_empty(), "{body}");

    let (status, body) = instant(&base, query, &[("explain_empty", "true")]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        warnings(&body),
        [
            r#"http_requests_total{job="api", code=~"5.."}: 2 series of "http_requests_total", 2 after job="api", 0 after code=~"5.." — code has: client_error, ok"#
        ]
    );

    // A query that answers is not touched by the flag.
    let ok = r#"http_requests_total{job="api"}"#;
    let t = (now_ns() / 1_000_000_000).to_string();
    let (_, with) = instant(&base, ok, &[("explain_empty", "true"), ("time", &t)]).await;
    let (_, without) = instant(&base, ok, &[("time", &t)]).await;
    assert!(
        !with["data"]["result"].as_array().unwrap().is_empty(),
        "{with}"
    );
    assert_eq!(
        serde_json::to_string(&with).unwrap(),
        serde_json::to_string(&without).unwrap()
    );

    let (status, body) = instant(&base, query, &[("explain_empty", "yes")]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_name_only_another_namespace_holds_is_never_suggested() {
    // `cpu_usage` exists, but only in namespace `other`; the unscoped request
    // on a tenanted server reads `default`, which holds `mem` alone.
    let seeds: &[Seed] = &[
        ("other", "cpu_usage", &[("host", "a")], "value"),
        ("default", "mem", &[("host", "a")], "free"),
    ];
    let (base, _tmp) = server(true, seeds).await;

    let (status, body) = sql(&base, "SELECT * FROM cpu_usge").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let error = body["error"].as_str().unwrap();
    assert!(!error.contains("cpu_usage"), "{error}");
    assert_eq!(error, r#"unknown table "cpu_usge"; the tables are "mem""#);
    assert!(body.get("hint").is_none(), "{body}");

    let (_, body) = instant(&base, "cpu_usge", &[("explain_empty", "true")]).await;
    let w = warnings(&body);
    assert!(w.iter().all(|w| !w.contains("cpu_usage")), "{w:?}");
    assert_eq!(w, [r#"cpu_usge: no series of "cpu_usge" in this window"#]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_openapi_document_describes_hints_and_explain_empty() {
    let (base, _tmp) = server(false, REQUESTS).await;
    let doc: Value = reqwest::get(format!("{base}/api/v1/openapi.json"))
        .await
        .expect("openapi")
        .json()
        .await
        .expect("json");
    let error = &doc["paths"]["/api/v1/chronix/sql"]["post"]["responses"]["400"]["content"]["application/json"]
        ["schema"]["properties"];
    for field in ["error", "code", "hint", "span"] {
        assert!(
            error.get(field).is_some(),
            "the SQL 400 lacks `{field}`: {error}"
        );
    }
    for path in ["/api/v1/prom/query", "/api/v1/prom/query_range"] {
        let params = doc["paths"][path]["get"]["parameters"].as_array().unwrap();
        assert!(
            params.iter().any(|p| p["name"] == "explain_empty"),
            "{path} does not document explain_empty"
        );
    }
}
