#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Every gauge the engine publishes reaches a `/metrics` scrape.
//!
//! `documented_metrics` asks whether a documented metric name appears as a
//! string literal in non-test source, which is the question that catches a
//! name nothing writes. It does not catch a name written by a function
//! **nothing calls** — and that is what had happened: `Chronix::statistics()`
//! emits thirteen gauges and had no caller outside tests and examples, so a
//! running server exported none of them. `chronix_series_count`,
//! `chronix_catalog_memory_bytes`, `chronix_storage_disk_usage_bytes` — the
//! cardinality, memory and disk numbers an operator alerts on, and a panel in
//! the bundled `ingestion.json` dashboard — were all permanently absent, and
//! Prometheus answers "no data" for a metric nobody writes exactly as it does
//! for a quiet one.
//!
//! Nothing in the existing suite could see it: `suite`'s harness builds a
//! recorder handle without installing it globally (so parallel tests do not
//! fight over the one global recorder), which means `metrics::gauge!` calls
//! reach a no-op recorder and `render()` is empty whatever the server does.
//! This is its own test binary so it can install the recorder for real, and
//! it asserts on the **scrape**, not on the source.

use std::net::SocketAddr;
use std::sync::Arc;

use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

/// Every gauge `Chronix::statistics()` sets. Kept as a list here on purpose:
/// the test is the guard, so adding a term to `DatabaseStatistics` without
/// adding it here is the only way the list can go stale, and the term would
/// still have to be exported to pass.
const STATISTICS_GAUGES: &[&str] = &[
    "chronix_series_count",
    "chronix_segment_count",
    "chronix_shard_count",
    "chronix_memtable_memory_bytes",
    "chronix_interner_memory_bytes",
    "chronix_wal_buffer_bytes",
    "chronix_catalog_memory_bytes",
    "chronix_measurement_count",
    "chronix_wal_sequence",
    "chronix_tombstone_count",
    "chronix_metadata_cache_entries",
    "chronix_metadata_cache_bytes",
    "chronix_storage_disk_usage_bytes",
];

#[tokio::test(flavor = "multi_thread")]
async fn a_scrape_carries_every_gauge_the_engine_publishes() {
    // The real recorder, installed globally — this binary runs one test.
    let handle = chronixd::server::setup_prometheus().expect("install recorder");

    let tmp = TempDir::new().expect("tempdir");
    let db = Arc::new(
        Chronix::open(
            ChronixConfigBuilder::default()
                .data_dir(tmp.path().to_path_buf())
                .build()
                .expect("config"),
        )
        .expect("open"),
    );

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("timestamp");
    db.insert(
        &Point::new(
            SeriesKey::new("power", chronix::tags! { "device" => "m1" }).expect("key"),
            chronix::fields! { "w" => 230.5 },
            now,
        )
        .expect("point"),
    )
    .expect("insert");
    db.flush().expect("flush");

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

    let app = build_router(state, "/metrics", handle, 10 * 1024 * 1024, None);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // `server::run` is never called here, so nothing else installs the rustls
    // provider and `reqwest`'s builder panics rather than erroring.
    chronixd::tls::ensure_crypto_provider();
    let body = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .expect("scrape")
        .text()
        .await
        .expect("body");

    let missing: Vec<&str> = STATISTICS_GAUGES
        .iter()
        .copied()
        .filter(|name| !body.contains(name))
        .collect();
    assert!(
        missing.is_empty(),
        "a scrape of a live server is missing {} of the engine's gauges: {missing:?}\n\
         The scrape renders whatever has been recorded, so a gauge nobody sets \
         is a metric that silently does not exist.",
        missing.len(),
    );
}

/// A metric name with dots in it is not the name that reaches Prometheus.
///
/// The exposition format allows `[a-zA-Z_:][a-zA-Z0-9_:]*`, so the exporter
/// rewrites anything else — silently, and only at render time. Three metrics
/// in this tree were declared with dots while 236 used underscores, and one of
/// them was *documented* with its dotted spelling in a doc comment, so an
/// operator following the documentation would grep a scrape for a name that
/// is not in it. This test is the evidence for the naming rule that
/// `every_metric_name_is_prometheus_safe` enforces.
#[test]
fn a_dotted_metric_name_is_rewritten_before_it_reaches_a_scrape() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("chronix.example.dotted.name").increment(1);
    });
    let rendered = handle.render();
    assert!(
        !rendered.contains("chronix.example.dotted.name"),
        "the dotted name reached the scrape after all; this test's premise is gone:\n{rendered}"
    );
    assert!(
        rendered.contains("chronix_example_dotted_name"),
        "expected the dots to be rewritten to underscores, got:\n{rendered}"
    );
}
