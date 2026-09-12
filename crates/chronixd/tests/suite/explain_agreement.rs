#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! `EXPLAIN` describes the query that would run.
//!
//! It described a different one. `/api/v1/chronix/query/explain` scoped its
//! plan to the `"default"` namespace whatever the deployment did, while
//! `/api/v1/chronix/query` scoped its own with the tenancy switch — `None`
//! unless `multi_tenancy` is on. So on the ordinary single-tenant server the
//! explain output carried a `__namespace__` tag filter that no stored point
//! carries, describing a plan that would have returned **nothing** for a
//! query that returned everything.
//!
//! Two independent implementations of one question, and no test that drove
//! both. This one drives both, in both deployment shapes, and compares.

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::config::{ServerConfig, ServerSettings};
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

/// A server holding one measurement, with tenancy on or off.
async fn server(multi_tenancy: bool) -> (String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(cfg).expect("open db"));

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i64;
    let mut points = Vec::new();
    for i in 0..5i64 {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert("host".to_string(), "a".to_string());
        if multi_tenancy {
            tags.insert(
                chronix_core::NAMESPACE_TAG.to_string(),
                "default".to_string(),
            );
        }
        points.push(
            Point::new(
                SeriesKey::new("cpu", tags).expect("series key"),
                chronix::fields! { "value" => i as f64 },
                now - (5 - i) * 1_000_000_000,
            )
            .expect("point"),
        );
    }
    db.insert_batch(&points)
        .expect("insert")
        .into_complete()
        .expect("all points accepted");
    db.flush().expect("flush");

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
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: std::sync::Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: None,
        audit_logger: None,
        config,
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

fn body() -> Value {
    serde_json::json!({
        "measurement": "cpu",
        "tags": {"host": "a"},
        "range": {"start": 0, "end": 4_000_000_000_000_000_000i64},
    })
}

/// The filters `EXPLAIN` lists are the filters the query applies.
async fn explain_matches_the_query(multi_tenancy: bool) {
    chronixd::tls::ensure_crypto_provider();
    let (base, _tmp) = server(multi_tenancy).await;
    let c = reqwest::Client::new();

    let explained: Value = c
        .post(format!("{base}/api/v1/chronix/query/explain"))
        .json(&body())
        .send()
        .await
        .expect("explain")
        .json()
        .await
        .expect("json");

    let rows: Value = c
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&body())
        .send()
        .await
        .expect("query")
        .json()
        .await
        .expect("json");

    let scan = &explained["plan"];
    let namespace = &scan["namespace_id"];
    let filters: Vec<String> = scan["tag_filters"]
        .as_array()
        .expect("tag_filters")
        .iter()
        .map(|f| f["key"].as_str().unwrap_or_default().to_string())
        .collect();

    // The query returns rows, so the plan `EXPLAIN` shows must be one that
    // could return them.
    let n = rows.as_array().expect("rows array").len();
    assert_eq!(n, 5, "the query itself must return the five points");

    if multi_tenancy {
        assert_eq!(namespace, "default", "a tenanted read is scoped");
        assert!(
            filters.iter().any(|k| k == chronix_core::NAMESPACE_TAG),
            "a tenanted plan carries the namespace filter: {filters:?}"
        );
    } else {
        assert!(
            namespace.is_null(),
            "with tenancy off the plan has no namespace, and EXPLAIN said {namespace}"
        );
        assert!(
            !filters.iter().any(|k| k == chronix_core::NAMESPACE_TAG),
            "with tenancy off no point carries a namespace tag, so a plan \
             filtering on one describes a query that returns nothing: {filters:?}"
        );
    }
}

#[tokio::test]
async fn explain_matches_the_query_on_a_single_tenant_server() {
    explain_matches_the_query(false).await;
}

#[tokio::test]
async fn explain_matches_the_query_under_tenancy() {
    explain_matches_the_query(true).await;
}
