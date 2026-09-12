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

/// Every histogram reaches a scrape as a **histogram**, never a summary.
///
/// `metrics-exporter-prometheus` renders `histogram!` as a Prometheus
/// *summary* unless buckets are configured, and a summary is not a smaller
/// histogram — it is a different metric with no `_bucket` series. So
/// `histogram_quantile()` had nothing to read, and **every latency panel in
/// `dashboards/` was permanently empty**: 24 of 45 panel targets, found by
/// pointing Grafana at a running server rather than by any test here.
///
/// The estimator was also wrong for anything that is not a latency —
/// `chronix_batch_size{quantile="0.5"}` read `0.9998` for a metric whose
/// observed values were 1 and 4320.
///
/// The assertion is on the **type line**, not on a list of metric names, so
/// it covers a histogram added tomorrow and one this deployment's features
/// do not reach.
#[tokio::test(flavor = "multi_thread")]
async fn no_histogram_reaches_a_scrape_as_a_summary() {
    let recorder = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        // One of each shape the matchers distinguish.
        metrics::histogram!("chronix_write_duration_seconds").record(0.004);
        metrics::histogram!("chronix_batch_size").record(4320.0);
        metrics::histogram!("chronix_segment_compression_ratio").record(7.5);
        // And one that matches no matcher, to prove the *default* is a
        // histogram — the arm that made all of this a summary.
        metrics::histogram!("chronix_unmatched_example").record(1.0);
    });
    let rendered = handle.render();

    let summaries: Vec<&str> = rendered
        .lines()
        .filter(|l| l.starts_with("# TYPE ") && l.ends_with(" summary"))
        .collect();
    assert!(
        summaries.is_empty(),
        "these reached the scrape as summaries, so `histogram_quantile()` \
         cannot read them and their dashboard panels are empty:\n{summaries:#?}"
    );

    for name in [
        "chronix_write_duration_seconds",
        "chronix_batch_size",
        "chronix_segment_compression_ratio",
        "chronix_unmatched_example",
    ] {
        assert!(
            rendered.contains(&format!("# TYPE {name} histogram")),
            "{name} is not a histogram in the scrape:\n{rendered}"
        );
        assert!(
            rendered.contains(&format!("{name}_bucket{{")),
            "{name} has no _bucket series, so histogram_quantile() has nothing to read"
        );
    }

    // The buckets must bracket the value, or every observation lands in +Inf
    // and the quantile is useless. A 4 ms write belongs below 5 ms.
    assert!(
        rendered.contains(r#"chronix_write_duration_seconds_bucket{le="0.005"} 1"#),
        "a 4 ms write should fall in the 5 ms bucket:\n{rendered}"
    );
}

/// Every `histogram_quantile()` in the bundled dashboards reads a metric this
/// build exports as a histogram.
///
/// The dashboards are the product surface `documented_metrics` checks the
/// *names* of; this checks that the name it reads — `X_bucket` — can exist at
/// all. A name that only ever renders as a summary passes a name check and
/// draws an empty panel.
#[test]
fn every_dashboard_quantile_reads_a_histogram_metric() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root");
    let recorder = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder();
    let handle = recorder.handle();

    // Record one observation per metric the dashboards take a quantile of,
    // then check each renders with buckets.
    let mut wanted: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(root.join("dashboards")).expect("dashboards") {
        let path = entry.expect("entry").path();
        if path.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("dashboard");
        for hit in text.split("histogram_quantile(").skip(1) {
            let Some(start) = hit.find("chronix_") else {
                continue;
            };
            let rest = &hit[start..];
            let end = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            let metric = &rest[..end];
            if let Some(base) = metric.strip_suffix("_bucket") {
                wanted.push(base.to_owned());
            }
        }
    }
    wanted.sort();
    wanted.dedup();
    assert!(
        !wanted.is_empty(),
        "no histogram_quantile targets found — this test's premise is gone"
    );

    metrics::with_local_recorder(&recorder, || {
        for name in &wanted {
            metrics::histogram!(name.clone()).record(0.01);
        }
    });
    let rendered = handle.render();

    let bad: Vec<&String> = wanted
        .iter()
        .filter(|n| !rendered.contains(&format!("{n}_bucket{{")))
        .collect();
    assert!(
        bad.is_empty(),
        "{} dashboard metrics render without `_bucket`, so their panels are \
         permanently empty: {bad:#?}",
        bad.len()
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
    let recorder = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder();
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
