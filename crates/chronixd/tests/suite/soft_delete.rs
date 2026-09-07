#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Dropping a measurement is undoable — when the operator asked for that.
//!
//! `soft_delete_ttl` and `restore_measurement` were both in the engine, both
//! tested, and reachable only from the embedded API: `chronixd` had no config
//! key for the first and no route for the second, so the server's most
//! destructive single call had no undo even though the engine implemented
//! one. That is the shape a whole audit pass was once spent on — a control
//! that is correct, tested, and never reached on the path a request takes.
//!
//! These drive the real router, because "the endpoint exists" and "the drop
//! is actually reversible" are different claims.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::config::ServerConfig;
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

/// A server holding one measurement, with the given soft-delete grace period.
async fn server(soft_delete_ttl: Option<Duration>) -> (String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let cfg = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .soft_delete_ttl(soft_delete_ttl)
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(cfg).expect("open db"));

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as i64;
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
        config: ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: Duration::ZERO,
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
    });

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (format!("http://{addr}"), tmp)
}

fn client() -> reqwest::Client {
    chronixd::tls::ensure_crypto_provider();
    reqwest::Client::new()
}

async fn measurements(base: &str) -> Vec<String> {
    let body: serde_json::Value = client()
        .get(format!("{base}/api/v1/measurements"))
        .send()
        .await
        .expect("list")
        .json()
        .await
        .expect("json");
    body.get("items")
        .and_then(|m| m.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| {
                    v.get("name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// With a grace period configured, a drop is reversible through the API.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_measurement_can_be_restored_within_the_grace_period() {
    let (base, _tmp) = server(Some(Duration::from_secs(3600))).await;
    assert!(
        measurements(&base).await.contains(&"power".to_string()),
        "premise: the measurement exists"
    );

    let dropped = client()
        .delete(format!("{base}/api/v1/measurements/power"))
        .send()
        .await
        .expect("drop");
    assert_eq!(dropped.status(), 204, "the drop must be accepted");

    let restored = client()
        .post(format!("{base}/api/v1/measurements/power/restore"))
        .send()
        .await
        .expect("restore");
    assert_eq!(
        restored.status(),
        204,
        "a drop inside the grace period must be reversible: {}",
        restored.text().await.unwrap_or_default()
    );

    // The data, not just the name: a restore that brought back an empty
    // measurement would pass a name check and still have lost everything.
    let rows: serde_json::Value = client()
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&serde_json::json!({
            "measurement": "power",
            "range": { "start": 0, "end": i64::MAX / 2 },
        }))
        .send()
        .await
        .expect("query")
        .json()
        .await
        .expect("json");
    let count = rows.as_array().map_or(0, Vec::len);
    assert_eq!(
        count, 1,
        "the restored measurement must still hold its row: {rows}"
    );
}

/// Without one, a drop is immediate — and the endpoint says so rather than
/// pretending it worked.
#[tokio::test(flavor = "multi_thread")]
async fn without_a_grace_period_there_is_nothing_to_restore() {
    let (base, _tmp) = server(None).await;
    let dropped = client()
        .delete(format!("{base}/api/v1/measurements/power"))
        .send()
        .await
        .expect("drop");
    assert_eq!(dropped.status(), 204);

    let restored = client()
        .post(format!("{base}/api/v1/measurements/power/restore"))
        .send()
        .await
        .expect("restore");
    assert_eq!(
        restored.status(),
        404,
        "an irreversible drop must not answer as though it were undone"
    );
    let body = restored.text().await.unwrap_or_default();
    assert!(
        body.contains("soft_delete_ttl_secs"),
        "the error must name the setting that would have made it reversible: {body}"
    );
}
