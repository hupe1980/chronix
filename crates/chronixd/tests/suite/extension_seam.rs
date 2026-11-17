#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! The [`Extension`] seam: what the cluster binary plugs into.
//!
//! Two properties make it safe to hand a router to code outside this crate:
//! its routes are guarded exactly like the built-in ones, and a start that
//! fails leaves nothing running.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::routing::get;
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::Chronix;
use chronix::prelude::*;
use chronix_security::authz::{AuthzEngine, ChronixAction};
use chronixd::error::ServerError;
use chronixd::http::{AppState, SharedState};
use chronixd::server::{Extension, ExtensionContext, admin_group, build_router_with, run_with};

const KEY: &str = "extension-seam-admin-key";

fn state(db: Arc<Chronix>, auth: chronixd::auth::AuthState, engine: AuthzEngine) -> AppState {
    Arc::new(SharedState {
        sql_contexts: chronixd::namespace::SqlContexts::new(db.clone()),
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        auth_state: Some(auth),
        namespace_registry: Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: Some(Arc::new(engine)),
        audit_logger: None,
        config: chronixd::config::ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        namespace_ingest_limiter: chronixd::rate_limit::NamespaceIngestLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
        query_slots: Default::default(),
    })
}

/// A server with one extension route, `GET /api/v1/admin/test-ext`, behind
/// the `ManageNodes` capability, under `policies`.
async fn serve_with_extension(policies: &str) -> (String, TempDir) {
    let tmp = TempDir::new().unwrap();
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .unwrap();
    let db = Arc::new(Chronix::open(config).unwrap());
    let auth = chronixd::auth::AuthState::from_config(&chronixd::config::AuthConfig {
        api_keys: vec![chronixd::config::ApiKeyEntry {
            name: "admin".into(),
            key: KEY.into(),
            namespaces: Vec::new(),
            admin: true,
            roles: Vec::new(),
        }],
        jwt: None,
        exempt_paths: vec!["/health".into()],
    })
    .unwrap();
    let engine = AuthzEngine::new();
    if !policies.is_empty() {
        engine.load_policies(policies).unwrap();
    }
    let st = state(db, auth.clone(), engine);
    let ext = admin_group(
        &st,
        ChronixAction::ManageNodes,
        Router::new().route("/api/v1/admin/test-ext", get(|| async { "extension" })),
    );
    let handle = chronixd::server::prometheus_builder()
        .unwrap()
        .build_recorder()
        .handle();
    let app = build_router_with(st, "/metrics", handle, 1 << 20, Some(auth), vec![ext]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/api/v1/admin/test-ext"), tmp)
}

#[tokio::test]
async fn extension_routes_sit_behind_auth_and_cedar() {
    // `reqwest` needs a process-wide rustls provider; nothing here calls `run`.
    chronixd::tls::ensure_crypto_provider();
    let client = reqwest::Client::new();

    // No credential: authentication refuses before anything else runs.
    let (url, _tmp) = serve_with_extension("").await;
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // An admin credential under a policy set that permits nothing: Cedar refuses.
    let resp = client.get(&url).bearer_auth(KEY).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // The same credential under a policy that permits everything: the route answers.
    let (url, _tmp2) = serve_with_extension("permit(principal, action, resource);").await;
    let resp = client.get(&url).bearer_auth(KEY).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "extension");
}

/// An extension that records its lifecycle, and optionally fails to start.
struct Probe {
    name: &'static str,
    fail: bool,
    log: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Extension for Probe {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn start(&self, _ctx: ExtensionContext) -> Result<Router<AppState>, ServerError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("start {}", self.name));
        if self.fail {
            return Err(ServerError::Internal(format!("{} refused", self.name)));
        }
        Ok(Router::new())
    }
    async fn stop(&self) {
        self.log.lock().unwrap().push(format!("stop {}", self.name));
    }
}

fn server_config(tmp: &TempDir) -> chronixd::config::ServerConfig {
    let mut config = chronixd::config::ServerConfig::default();
    config.database.data_dir = tmp.path().to_path_buf();
    config
}

#[tokio::test]
async fn extension_start_failure_stops_started_ones() {
    let tmp = TempDir::new().unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let probe = |name, fail| -> Box<dyn Extension> {
        Box::new(Probe {
            name,
            fail,
            log: Arc::clone(&log),
        })
    };
    let err = run_with(
        server_config(&tmp),
        vec![probe("a", false), probe("b", false), probe("c", true)],
    )
    .await
    .expect_err("a failed start must fail the server");
    assert!(err.to_string().contains("c refused"), "{err}");
    assert_eq!(
        *log.lock().unwrap(),
        ["start a", "start b", "start c", "stop b", "stop a"],
        "the extensions that started are stopped, in reverse"
    );
}

#[tokio::test]
async fn cluster_section_is_refused_by_name() {
    let tmp = TempDir::new().unwrap();
    let mut config = server_config(&tmp);
    config.cluster = Some(chronixd::config::ClusterConfig {
        mode: chronixd::config::ClusterMode::Meta,
        node_id: 1,
        raft_bind_addr: "127.0.0.1:0".into(),
        cluster_peers: Vec::new(),
        meta_addrs: Vec::new(),
        tls: None,
    });
    config.database.data_dir = tmp.path().join("data");
    let err = chronixd::server::run(config)
        .await
        .expect_err("[cluster] must be refused");
    assert!(
        !tmp.path().join("data").exists(),
        "a refused configuration must not create the data directory"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("[cluster] is not supported by chronixd")
            && msg.contains("cargo build -p chronixd-cluster"),
        "{msg}"
    );
}
