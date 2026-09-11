#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! TLS handshake integration tests.
//!
//! Uses `rcgen` to generate a self-signed certificate, starts an HTTPS
//! server via axum-server + rustls, and verifies the connection using
//! `reqwest` with the generated CA added as a trusted root.

use std::net::SocketAddr;
use std::sync::Arc;

use rcgen::generate_simple_self_signed;
use reqwest::{Certificate, StatusCode};
use tempfile::TempDir;
use tokio::time::Duration;

use chronix::prelude::*;
use chronix::Chronix;
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;

/// Generate a self-signed cert/key pair for `localhost` / `127.0.0.1`.
fn generate_self_signed_cert() -> (Vec<u8>, Vec<u8>) {
    // Ensure the ring crypto provider is installed for rustls
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = generate_simple_self_signed(subject_alt_names).unwrap();
    let cert_pem = cert.cert.pem().as_bytes().to_vec();
    let key_pem = cert.signing_key.serialize_pem().as_bytes().to_vec();
    (cert_pem, key_pem)
}

/// Start a TLS-enabled HTTP server on an ephemeral port.
/// Returns `(base_url, cert_pem, temp_dir)`.
async fn start_tls_server() -> (String, Vec<u8>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));

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

    let metrics_builder = chronixd::server::prometheus_builder().expect("bucket config");
    let metrics_handle = metrics_builder.build_recorder().handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);

    // Generate self-signed cert
    let (cert_pem, key_pem) = generate_self_signed_cert();

    // Write cert/key to temp files
    let cert_path = tmp.path().join("server.crt");
    let key_path = tmp.path().join("server.key");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    // Load rustls config
    let tls_config = chronixd::config::TlsConfig {
        cert: cert_path,
        key: key_path,
        client_ca: None,
        reload_interval_secs: 0,
    };
    let rustls_cfg = chronixd::tls::load_rustls_config(&tls_config).unwrap();
    let axum_tls = axum_server::tls_rustls::RustlsConfig::from_config(rustls_cfg);

    // Bind to ephemeral port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener); // Release the port so axum-server can rebind

    let base = format!("https://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        axum_server::bind_rustls(addr, axum_tls)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(200)).await;
    (base, cert_pem, tmp)
}

#[tokio::test]
async fn tls_handshake_with_self_signed_cert() {
    let (base, cert_pem, _tmp) = start_tls_server().await;

    // Create a client that trusts the self-signed cert
    let cert = Certificate::from_pem(&cert_pem).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();

    // health check via HTTPS
    let resp = client.get(format!("{base}/health")).send().await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn tls_write_and_query_round_trip() {
    let (base, cert_pem, _tmp) = start_tls_server().await;

    let cert = Certificate::from_pem(&cert_pem).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();

    // Write via HTTPS
    let write_body = serde_json::json!({
        "measurement": "cpu",
        "tags": {"host": "server01"},
        "fields": {"usage": 42.5},
        "timestamp": 1_609_459_200_000_000_000_i64
    });
    let resp = client
        .post(format!("{base}/api/v1/write"))
        .json(&write_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Query via HTTPS
    let query_body = serde_json::json!({
        "measurement": "cpu",
        "range": {"start": 1_609_459_199_000_000_000_i64, "end": 1_609_459_201_000_000_000_i64}
    });
    let resp = client
        .post(format!("{base}/api/v1/chronix/query"))
        .json(&query_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rows: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn tls_untrusted_cert_fails() {
    let (base, _cert_pem, _tmp) = start_tls_server().await;

    // Client without the self-signed cert — should fail
    let client = reqwest::Client::builder().build().unwrap();

    let result = client.get(format!("{base}/health")).send().await;
    assert!(result.is_err(), "should fail without trusted CA");
}
