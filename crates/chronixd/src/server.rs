//! Server lifecycle — startup, routing, and graceful shutdown.
//!
//! Wires together the HTTP (axum), gRPC (tonic), and Flight SQL servers
//! into a unified daemon. Handles signal-based shutdown, Prometheus
//! metrics export, and TLS configuration.

use std::sync::Arc;

use axum::http::{header, Method};
use axum::routing::{delete, get, post, put};
use axum::Router;
use tokio::signal;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

use chronix::Chronix;

use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::flight::ChronixFlightSqlService;
use crate::grpc::ChronixGrpcService;
use crate::http::{self, AppState, SharedState};
use crate::proto;

/// Active cluster state — either meta or data node.
#[cfg(feature = "cluster")]
enum ClusterState {
    Meta(crate::cluster::MetaNodeState),
    Data(crate::cluster::DataNodeState),
}

/// Run the chronixd server with the given configuration.
///
/// This is the main entry point — it opens the database, starts all servers,
/// and blocks until a shutdown signal is received.
///
/// # Errors
///
/// Returns an error if the database fails to open or any server fails to bind.
pub async fn run(mut config: ServerConfig) -> Result<(), ServerError> {
    // Before anything builds a TLS client or server: `reqwest` and
    // `axum-server` both resolve the *process-level* rustls provider and
    // panic when none is installed.
    crate::tls::ensure_crypto_provider();
    let start_time = std::time::Instant::now();

    // ── Open database ──────────────────────────────────────────────────
    // Database must be opened before DataNode startup (which needs it for
    // region-local storage), while MetaNode doesn't require the DB.
    let chronix_config = config.to_chronix_config()?;
    let db = Arc::new(Chronix::open(chronix_config).map_err(ServerError::Db)?);
    info!(
        data_dir = %config.database.data_dir.display(),
        "database opened"
    );

    // ── Cluster mode (optional) ────────────────────────────────────
    // `Config`, not `Internal`: a section this build cannot honour is the
    // operator's input, and every sibling refusal classifies it that way.
    #[cfg(not(feature = "cluster"))]
    if config.cluster.is_some() {
        return Err(ServerError::Config(
            crate::config::ServerConfigError::Invalid(
                "[cluster] is configured but this binary was built without the \
                 `cluster` feature: rebuild with `--features cluster`, or \
                 remove the section"
                    .into(),
            ),
        ));
    }
    #[cfg(feature = "cluster")]
    let cluster_state = if let Some(ref cluster_cfg) = config.cluster {
        use crate::config::ClusterMode;
        match cluster_cfg.mode {
            ClusterMode::Meta => {
                let state =
                    crate::cluster::start_meta_node(cluster_cfg, &config.database.data_dir).await?;
                Some(ClusterState::Meta(state))
            }
            ClusterMode::Data => {
                let grpc_addr = config.server.grpc_addr().to_string();
                let state =
                    crate::cluster::start_data_node(cluster_cfg, &grpc_addr, db.clone()).await?;
                Some(ClusterState::Data(state))
            }
        }
    } else {
        None
    };

    // ── Prometheus metrics ─────────────────────────────────────────────
    let metrics_handle = setup_prometheus()?;
    // Publish the write path's counters at zero, so a dashboard reads "0
    // errors" rather than "No data" — which is what an operator sees when a
    // metric does not exist at all.
    crate::util::register_write_metrics();
    // The security counters, for the same reason and with more of it: an
    // alert on "requests are being refused" has to be able to fire from the
    // first scrape, and a counter that does not exist until the first refusal
    // is one whose alert cannot be tested before the incident.
    metrics::counter!("chronix_auth_failures_total", "protocol" => "http").absolute(0);
    metrics::counter!("chronix_auth_failures_total", "protocol" => "grpc").absolute(0);
    metrics::counter!("chronix_namespace_denied_total").absolute(0);
    metrics::counter!("chronix_authz_denied_total").absolute(0);

    // ── Connector manager ───────────────────────────────────────────────
    let connector_manager = Arc::new(
        crate::connector::ConnectorManager::new(db.clone())
            .with_max_start_retries(config.server.connector_start_retries)
            .with_start_retry_backoff_ms(config.server.connector_start_retry_backoff_ms),
    );

    reject_unbuildable_sections(&config)?;

    // Register configured connectors
    if let Some(ref kafka_cfg) = config.kafka {
        let consumer = crate::kafka::KafkaConsumer::new_arc(
            "kafka-default",
            kafka_cfg.clone(),
            db.clone(),
            config.server.multi_tenancy,
        );
        connector_manager.register(consumer).await;
    }

    if let Some(ref mqtt_cfg) = config.mqtt {
        let subscriber = crate::mqtt::MqttSubscriber::new_arc(
            "mqtt-default",
            mqtt_cfg.clone(),
            db.clone(),
            config.server.multi_tenancy,
        );
        connector_manager.register(subscriber).await;
    }

    // Start all registered connectors
    connector_manager
        .start_all()
        .await
        .map_err(|e| ServerError::Internal(format!("failed to start connectors: {e}")))?;

    // ── Authentication (optional) ──────────────────────────────────────
    // When metrics_require_auth is false, add the metrics endpoint
    // to the auth exempt list so Prometheus can scrape without auth.
    if !config.server.metrics_require_auth {
        if let Some(ref mut auth) = config.auth {
            if !auth.exempt_paths.contains(&config.server.metrics_path) {
                auth.exempt_paths.push(config.server.metrics_path.clone());
            }
        }
    }

    // Beside the namespace registry, in the data directory: the durable
    // record of runtime key changes belongs with the data it guards, not
    // beside the config file that only *declares* keys.
    let auth_dir = db.data_dir().join("auth");
    let mut auth_state = config
        .auth
        .as_ref()
        .map(|c| crate::auth::AuthState::from_config_at(c, Some(&auth_dir)))
        .transpose()
        .map_err(|e| ServerError::Internal(format!("auth configuration error: {e}")))?;

    // Fetch the issuer's published keys before serving. A configured JWKS
    // URL that is never fetched is worse than none: the server starts, and
    // then rejects every token the issuer signs.
    if let (Some(state), Some(url)) = (
        auth_state.as_mut(),
        config
            .auth
            .as_ref()
            .and_then(|a| a.jwt.as_ref())
            .and_then(|j| j.jwks_url.as_deref()),
    ) {
        state
            .attach_jwks(url, std::time::Duration::from_secs(3600))
            .await
            .map_err(|e| ServerError::Internal(format!("JWKS error: {e}")))?;
    }
    let auth_state = auth_state;

    // ── Authorization engine (optional) ──────────────────────────────────
    let authz_engine = if let Some(ref policy_dir) = config.server.authz_policy_dir {
        let engine = chronix_security::authz::AuthzEngine::new();
        match engine.load_policies_from_dir(policy_dir) {
            Ok(count) => {
                info!(
                    policy_dir = %policy_dir.display(),
                    policy_count = count,
                    "Cedar authorization enabled"
                );
                Some(Arc::new(engine))
            }
            Err(e) => {
                return Err(ServerError::Internal(format!(
                    "failed to load Cedar policies from {}: {e}",
                    policy_dir.display()
                )));
            }
        }
    } else {
        None
    };

    // ── Audit logger ───────────────────────────────────────────────────
    let audit_logger = {
        let mut logger = chronix_security::audit::AuditLogger::new();

        // An audit file, when one is configured. Two things make it a
        // trail rather than a log: it is `fsync`ed, and the hash chain
        // **continues** across restarts by anchoring on the last event
        // already in the file. Without the anchor every restart began a
        // fresh chain in the same file, and a verifier cannot tell that
        // from a truncation — which is precisely what it exists to detect.
        if let Some(audit_cfg) = config.audit.as_ref() {
            let key = match audit_cfg.hmac_key_env.as_ref() {
                Some(var) => match std::env::var(var) {
                    Ok(v) if !v.is_empty() => Some(v.into_bytes()),
                    _ => {
                        return Err(ServerError::Internal(format!(
                            "audit.hmac_key_env names `{var}`, which is unset or empty; \
                             an unsealed chain can be recomputed by anyone who can write \
                             the log, so it proves nothing"
                        )));
                    }
                },
                None => None,
            };
            if let Some(key) = key {
                logger = logger.with_hmac_key(key);
            } else {
                warn!(
                    "audit chain is sealed with a bare SHA-256 hash — set \
                     `audit.hmac_key_env` so that tampering, not just corruption, \
                     is detectable"
                );
            }

            let anchor = chronix_security::audit::last_event(&audit_cfg.path).map_err(|e| {
                ServerError::Internal(format!(
                    "cannot read the audit log at {}: {e}",
                    audit_cfg.path.display()
                ))
            })?;
            if let Some(last) = anchor {
                info!(last_id = last.id, "continuing the existing audit chain");
                logger = logger.resume_from(last.id, last.event_hash.clone());
            }

            let sink =
                chronix_security::audit::FileSink::open(&audit_cfg.path, audit_cfg.sync_each)
                    .map_err(|e| {
                        ServerError::Internal(format!(
                            "cannot open the audit log at {}: {e}",
                            audit_cfg.path.display()
                        ))
                    })?;
            logger.add_sink(Box::new(sink));
            info!(path = %audit_cfg.path.display(), "audit log opened");
        }

        // Always add the tracing-based sink so audit events appear in
        // structured logs (forwarded to whatever tracing subscriber is
        // configured, e.g. JSON file, stdout, OTLP).
        logger.add_sink(Box::new(chronix_security::audit::TracingSink));
        Some(Arc::new(logger))
    };

    // **What the server is enforcing, sealed into the chain at startup.**
    // The policy set decides every administrative and data decision that
    // follows, and it was recorded only as an `info!` — so a trail could show
    // a refusal and nothing about the rules that produced it, and a change of
    // rules between two restarts left no evidence at all. `PolicyLoad` was a
    // category nothing constructed.
    //
    // Recorded here rather than where the policies load, because the logger
    // does not exist yet at that point.
    if let (Some(engine), Some(logger)) = (authz_engine.as_ref(), audit_logger.as_ref()) {
        let mut event = chronix_security::audit::AuditEvent::new(
            "system",
            chronix_security::audit::AuditAction::PolicyLoad,
            config
                .server
                .authz_policy_dir
                .as_ref()
                .map_or_else(String::new, |d| d.display().to_string()),
            chronix_security::audit::AuditDecision::Allow,
        )
        .with_metadata("policy_count", engine.policy_count().to_string());
        for id in engine.policy_ids() {
            event = event.with_metadata("policy", id);
        }
        logger.log(event);
    }

    // ── Shared state ───────────────────────────────────────────────────
    // The registry lives beside the data it scopes, so a restart keeps the
    // tenants. A failure to open it is fatal rather than a silent fall back
    // to an empty in-memory one, which would answer 400 for every tenant.
    let namespace_registry = {
        let dir = db.data_dir().join("namespaces");
        chronix_security::tenant::NamespaceRegistry::open(&dir).map_err(|e| {
            ServerError::Internal(format!(
                "cannot open the namespace registry at {}: {e}",
                dir.display()
            ))
        })?
    };

    let sql_contexts = crate::namespace::SqlContexts::new(db.clone());
    #[cfg(feature = "cluster")]
    let meta_client = cluster_state.as_ref().map(|cs| match cs {
        ClusterState::Meta(s) => Arc::clone(&s.meta_client),
        ClusterState::Data(s) => Arc::clone(s.manager.meta_client()),
    });
    // ── Signal triggers (optional) ─────────────────────────────────────
    let pipeline = build_trigger_pipeline(&config, &db)?;

    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
        pipeline: pipeline.clone(),
        config: config.clone(),
        start_time,
        connector_manager: Some(connector_manager.clone()),
        sql_contexts,
        auth_state: auth_state.clone(),
        #[cfg(feature = "cluster")]
        meta_client,
        // **Durable**, not in memory. A namespace that vanished at restart
        // made every non-default tenant's data unreachable — the middleware
        // answers `400 namespace not found` for a namespace the registry
        // does not know, so the rows were still on disk and nothing could
        // ask for them. `open()` existed the whole time.
        namespace_registry: Arc::new(namespace_registry),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        authz_engine: authz_engine.clone(),
        audit_logger: audit_logger.clone(),
        namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter::new(),
        // Write dedup cache for idempotency keys.
        write_dedup_cache: crate::http::WriteDedupCache::new(
            config.server.dedup_window_secs,
            config.server.max_dedup_entries,
        ),
        write_timeout: std::time::Duration::from_secs(config.server.write_timeout_secs),
        openapi_json: std::sync::OnceLock::new(),
    });

    // The pipeline is fed by the database's CDC bus, which is what makes a
    // write evaluate a trigger. Without this subscription the trigger engine
    // is a correctly implemented thing that never sees an event.
    if let Some(pipeline) = pipeline.as_ref() {
        pipeline.spawn_cdc_listener(db.event_bus());
        info!("signal triggers enabled");
    }

    spawn_cold_archiver(&config, &db)?;

    // ── TLS (optional) ─────────────────────────────────────────────────
    let tls_rustls_config = if let Some(ref tls_cfg) = config.tls {
        let cfg = crate::tls::load_rustls_config(tls_cfg)?;
        info!("TLS enabled");
        Some(cfg)
    } else {
        None
    };

    let tonic_tls = if let Some(ref tls_cfg) = config.tls {
        Some(crate::tls::load_tonic_tls_config(tls_cfg)?)
    } else {
        None
    };

    // Build reloadable TLS acceptor for gRPC/Flight SQL.
    // The resolver is shared with the TLS watcher so cert rotations
    // automatically apply to gRPC and Flight SQL connections.
    let (grpc_tls_acceptor, grpc_cert_resolver) = if let Some(ref tls_cfg) = config.tls {
        let (acceptor, resolver) = crate::tls::build_reloadable_grpc_tls(tls_cfg)?;
        (Some(acceptor), Some(resolver))
    } else {
        (None, None)
    };

    // ── HTTP server (axum) ─────────────────────────────────────────────
    // Create gRPC/Flight auth interceptors BEFORE moving auth_state into
    // the HTTP router, ensuring protocol auth parity.
    // Wire audit_logger into gRPC interceptors for auth failure logging.
    let grpc_auth_interceptor = auth_state.as_ref().map(|a| {
        if let Some(ref al) = audit_logger {
            a.grpc_interceptor_with_audit(Arc::clone(al))
        } else {
            a.grpc_interceptor()
        }
    });
    let flight_auth_interceptor = auth_state.as_ref().map(|a| {
        if let Some(ref al) = audit_logger {
            a.grpc_interceptor_with_audit(Arc::clone(al))
        } else {
            a.grpc_interceptor()
        }
    });

    let metrics_path = config.server.metrics_path.clone();
    let app = build_router(
        state,
        &metrics_path,
        metrics_handle,
        config.server.max_body_size,
        auth_state,
    );

    let http_addr = config.server.http_addr;
    let shutdown_timeout = std::time::Duration::from_secs(config.server.shutdown_timeout_secs);
    let tls_config_clone = config.tls.clone();
    // Every server drains on this one signal, and every server fires it when
    // it stops — see `Shutdown`.
    let shutdown = Shutdown::new();
    shutdown.on_os_signal();
    let http_handle = if let Some(rustls_cfg) = tls_rustls_config {
        let shutdown_guard = shutdown.guard();
        let drain_signal = shutdown.wait();
        let tls_reload_interval = tls_config_clone
            .as_ref()
            .map_or(0, |t| t.reload_interval_secs);
        let tls_config_for_watcher = tls_config_clone.clone();
        tokio::spawn(async move {
            // Held for the life of this task: dropping it — on a clean
            // return, an error or a panic — tells the other servers to drain.
            let _shutdown_guard = shutdown_guard;
            info!(%http_addr, "HTTPS server listening");
            let rustls_config = axum_server::tls_rustls::RustlsConfig::from_config(rustls_cfg);

            // Spawn TLS hot-reload watcher if configured
            if tls_reload_interval > 0 {
                if let Some(tls_cfg) = tls_config_for_watcher {
                    crate::tls_watcher::spawn_tls_watcher(
                        tls_cfg,
                        rustls_config.clone(),
                        tls_reload_interval,
                        grpc_cert_resolver,
                    );
                }
            }

            let handle = axum_server::Handle::new();
            let server_handle = handle.clone();
            let http_drain_timeout = shutdown_timeout;
            tokio::spawn(async move {
                drain_signal.await;
                server_handle.graceful_shutdown(Some(http_drain_timeout));
            });
            axum_server::bind_rustls(http_addr, rustls_config)
                .handle(handle)
                .serve(app.into_make_service())
                .await
        })
    } else {
        let shutdown_guard = shutdown.guard();
        let drain_signal = shutdown.wait();
        let drain_deadline = shutdown.wait();
        let http_drain_timeout = shutdown_timeout;
        tokio::spawn(async move {
            let _shutdown_guard = shutdown_guard;
            let listener = tokio::net::TcpListener::bind(http_addr).await?;
            info!(%http_addr, "HTTP server listening");
            // `axum::serve`'s graceful shutdown waits for every open
            // connection **without a deadline**, and `/api/v1/cdc/stream` and
            // the annotations SSE stream do not end on their own — so one
            // subscriber would hold the process open for ever. The TLS,
            // gRPC and Flight SQL paths each carry `shutdown_timeout`; this
            // one did not, and only got away with it because the old outer
            // `select!` returned as soon as *another* server drained and let
            // the process exit out from under it. Waiting for all three, as
            // the database's lifetime requires, makes the missing deadline a
            // hang.
            tokio::select! {
                res = axum::serve(listener, app).with_graceful_shutdown(drain_signal) => res,
                () = async {
                    drain_deadline.await;
                    tokio::time::sleep(http_drain_timeout).await;
                } => {
                    warn!(
                        timeout = ?http_drain_timeout,
                        "HTTP drain timeout exceeded, forcing shutdown"
                    );
                    Ok(())
                }
            }
        })
    };

    // ── gRPC server (tonic) ────────────────────────────────────────────
    let grpc_addr = config.server.grpc_addr();
    let grpc_service = ChronixGrpcService::with_streaming_config(
        db.clone(),
        start_time,
        config.server.stream_batch_size,
        config.server.stream_batch_interval_ms,
        config.server.dedup_window_secs,
    )
    .with_multi_tenancy(config.server.multi_tenancy)
    .with_authz(authz_engine.clone())
    .with_audit(audit_logger.clone())
    .with_max_dedup_entries(config.server.max_dedup_entries)
    .with_sql_limits(
        config.server.sql_query_timeout_secs,
        config.server.sql_max_rows,
    )
    .with_write_timeout(std::time::Duration::from_secs(
        config.server.write_timeout_secs,
    ));
    let tonic_tls_grpc = tonic_tls.clone();
    let grpc_tls_acceptor_grpc = grpc_tls_acceptor.clone();
    let grpc_keepalive_secs = config.server.grpc_keepalive_secs;
    let grpc_keepalive_timeout_secs = config.server.grpc_keepalive_timeout_secs;
    let grpc_reflection_enabled = config.server.grpc_reflection;
    let grpc_drain_timeout = shutdown_timeout;

    let grpc_shutdown = shutdown.clone();
    let grpc_guard = shutdown.guard();
    let grpc_handle = tokio::spawn(async move {
        let _shutdown_guard = grpc_guard;
        let shutdown = grpc_shutdown;
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<proto::chronix_service_server::ChronixServiceServer<ChronixGrpcService>>(
            )
            .await;

        // When using manual TLS (hot-reloadable), do NOT set
        // tonic's built-in TLS — we handle it at the connection level.
        let mut builder = tonic::transport::Server::builder();
        if grpc_tls_acceptor_grpc.is_none() {
            if let Some(tls) = tonic_tls_grpc {
                builder = builder.tls_config(tls).map_err(std::io::Error::other)?;
            }
        }

        // Keepalive configuration
        if grpc_keepalive_secs > 0 {
            builder = builder
                .http2_keepalive_interval(Some(std::time::Duration::from_secs(grpc_keepalive_secs)))
                .http2_keepalive_timeout(Some(std::time::Duration::from_secs(
                    grpc_keepalive_timeout_secs,
                )));
        }

        info!(%grpc_addr, "gRPC server listening");
        let mut router = builder.add_service(health_service);

        // Only expose reflection when explicitly enabled (off by default
        // to avoid leaking the service schema in production).
        if grpc_reflection_enabled {
            let reflection_service = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .build_v1()
                .map_err(std::io::Error::other)?;
            router = router.add_service(reflection_service);
        }

        // Apply auth interceptor for protocol parity with HTTP
        if let Some(interceptor) = grpc_auth_interceptor {
            router = router.add_service(
                proto::chronix_service_server::ChronixServiceServer::with_interceptor(
                    grpc_service,
                    interceptor,
                ),
            );
        } else {
            router = router.add_service(proto::chronix_service_server::ChronixServiceServer::new(
                grpc_service,
            ));
        }

        if let Some(acceptor) = grpc_tls_acceptor_grpc {
            // Manual TLS with hot-reloadable cert resolver.
            // TLS handshakes are spawned concurrently so a slow client
            // does not block accepting new connections.
            let listener = tokio::net::TcpListener::bind(grpc_addr)
                .await
                .map_err(std::io::Error::other)?;
            let (tx, rx) = tokio::sync::mpsc::channel::<
                Result<crate::tls::GrpcTlsStream, std::io::Error>,
            >(128);

            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((tcp, addr)) => {
                            let acc = acceptor.clone();
                            let tx = tx.clone();
                            tokio::spawn(async move {
                                match acc.accept(tcp).await {
                                    Ok(tls) => {
                                        let _ = tx
                                            .send(Ok(crate::tls::GrpcTlsStream::new(tls, addr)))
                                            .await;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            %addr,
                                            "gRPC TLS handshake failed"
                                        );
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "gRPC TCP accept error");
                            break;
                        }
                    }
                }
            });

            let incoming = tokio_stream::wrappers::ReceiverStream::new(rx);
            let drain_deadline = shutdown.wait();
            tokio::select! {
                res = router.serve_with_incoming_shutdown(incoming, shutdown.wait()) => {
                    res.map_err(std::io::Error::other)
                }
                _ = async {
                    drain_deadline.await;
                    tokio::time::sleep(grpc_drain_timeout).await;
                } => {
                    warn!(timeout = ?grpc_drain_timeout, "gRPC drain timeout exceeded, forcing shutdown");
                    Ok(())
                }
            }
        } else {
            let drain_deadline = shutdown.wait();
            tokio::select! {
                res = router.serve_with_shutdown(grpc_addr, shutdown.wait()) => {
                    res.map_err(std::io::Error::other)
                }
                _ = async {
                    drain_deadline.await;
                    tokio::time::sleep(grpc_drain_timeout).await;
                } => {
                    warn!(timeout = ?grpc_drain_timeout, "gRPC drain timeout exceeded, forcing shutdown");
                    Ok(())
                }
            }
        }
    });

    // ── Flight SQL server ──────────────────────────────────────────────
    let flight_addr = config.server.flight_addr();
    let flight_drain_timeout = shutdown_timeout;
    let flight_service = ChronixFlightSqlService::new(db.clone())
        .with_multi_tenancy(config.server.multi_tenancy)
        .with_authz(authz_engine.clone())
        .with_audit(audit_logger.clone())
        .with_audit(audit_logger.clone())
        .with_limits(
            std::time::Duration::from_secs(config.server.sql_query_timeout_secs),
            config.server.sql_max_rows,
        )
        .with_write_timeout(std::time::Duration::from_secs(
            config.server.write_timeout_secs,
        ));

    let flight_shutdown = shutdown.clone();
    let flight_guard = shutdown.guard();
    let flight_handle = tokio::spawn(async move {
        let _shutdown_guard = flight_guard;
        let shutdown = flight_shutdown;
        // When using manual TLS (hot-reloadable), do NOT set
        // tonic's built-in TLS — we handle it at the connection level.
        let mut builder = tonic::transport::Server::builder();
        if grpc_tls_acceptor.is_none() {
            if let Some(tls) = tonic_tls {
                builder = builder.tls_config(tls).map_err(std::io::Error::other)?;
            }
        }

        info!(%flight_addr, "Flight SQL server listening");

        // Build the tonic Router with or without auth interceptor.
        let router = if let Some(interceptor) = flight_auth_interceptor {
            let svc = tonic::service::interceptor::InterceptedService::new(
                flight_service.into_server(),
                interceptor,
            );
            builder.add_service(svc)
        } else {
            builder.add_service(flight_service.into_server())
        };

        if let Some(acceptor) = grpc_tls_acceptor {
            // Manual TLS with hot-reloadable cert resolver.
            let listener = tokio::net::TcpListener::bind(flight_addr)
                .await
                .map_err(std::io::Error::other)?;
            let (tx, rx) = tokio::sync::mpsc::channel::<
                Result<crate::tls::GrpcTlsStream, std::io::Error>,
            >(128);

            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((tcp, addr)) => {
                            let acc = acceptor.clone();
                            let tx = tx.clone();
                            tokio::spawn(async move {
                                match acc.accept(tcp).await {
                                    Ok(tls) => {
                                        let _ = tx
                                            .send(Ok(crate::tls::GrpcTlsStream::new(tls, addr)))
                                            .await;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            %addr,
                                            "Flight SQL TLS handshake failed"
                                        );
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Flight SQL TCP accept error");
                            break;
                        }
                    }
                }
            });

            let incoming = tokio_stream::wrappers::ReceiverStream::new(rx);
            let drain_deadline = shutdown.wait();
            tokio::select! {
                res = router.serve_with_incoming_shutdown(incoming, shutdown.wait()) => {
                    res.map_err(std::io::Error::other)
                }
                _ = async {
                    drain_deadline.await;
                    tokio::time::sleep(flight_drain_timeout).await;
                } => {
                    warn!(timeout = ?flight_drain_timeout, "Flight SQL drain timeout exceeded, forcing shutdown");
                    Ok(())
                }
            }
        } else {
            let drain_deadline = shutdown.wait();
            tokio::select! {
                res = router.serve_with_shutdown(flight_addr, shutdown.wait()) => {
                    res.map_err(std::io::Error::other)
                }
                _ = async {
                    drain_deadline.await;
                    tokio::time::sleep(flight_drain_timeout).await;
                } => {
                    warn!(timeout = ?flight_drain_timeout, "Flight SQL drain timeout exceeded, forcing shutdown");
                    Ok(())
                }
            }
        }
    });

    // ── Startup banner ─────────────────────────────────────────────────
    let mode_label = match &config.cluster {
        Some(c) => format!("{:?}", c.mode),
        None => "standalone".to_string(),
    };
    info!(
        version = env!("CARGO_PKG_VERSION"),
        mode = %mode_label,
        http = %config.server.http_addr,
        grpc = %config.server.grpc_addr(),
        flight_sql = %config.server.flight_addr(),
        data_dir = %config.database.data_dir.display(),
        tls = config.tls.is_some(),
        "chronixd started"
    );
    warn_if_exposed_without_auth(&config);

    // ── Wait for every server to finish ────────────────────────────────
    //
    // All three drain on the same `SIGTERM`, but they do not finish together:
    // one with no open connections returns at once while another is still
    // streaming a Flight SQL result. This used to be a `tokio::select!` over
    // the three handles, which returns on the **first** and drops the other
    // two — and dropping a `JoinHandle` does not stop its task. So the
    // database was flushed, closed and its directory lock released while gRPC
    // and Flight SQL were still serving requests against it, and a write in
    // flight at that moment met a closed database.
    //
    // `join!` waits for all three. Each already carries its own drain
    // deadline (`shutdown_timeout`), so this cannot wait for ever; what it
    // adds is that the database outlives every surface that reads it.
    let (http_res, grpc_res, flight_res) = tokio::join!(http_handle, grpc_handle, flight_handle);
    match http_res {
        Err(e) => error!(%e, "HTTP server task panicked"),
        Ok(Err(e)) => error!(%e, "HTTP server error"),
        Ok(Ok(())) => {}
    }
    match grpc_res {
        Err(e) => error!(%e, "gRPC server task panicked"),
        Ok(Err(e)) => error!(%e, "gRPC server error"),
        Ok(Ok(())) => {}
    }
    match flight_res {
        Err(e) => error!(%e, "Flight SQL server task panicked"),
        Ok(Err(e)) => error!(%e, "Flight SQL server error"),
        Ok(Ok(())) => {}
    }

    // ── Graceful shutdown ──────────────────────────────────────────────
    info!("shutting down...");

    // Stop cluster components first
    #[cfg(feature = "cluster")]
    if let Some(state) = cluster_state {
        match state {
            ClusterState::Meta(s) => crate::cluster::stop_meta_node(s).await,
            ClusterState::Data(s) => crate::cluster::stop_data_node(s).await,
        }
    }

    // Stop connectors
    connector_manager.stop_all().await;
    info!("connectors stopped");

    // Let queued signals go out. Delivery runs on each channel's own worker
    // so that a slow webhook cannot delay evaluation; the cost of that is
    // that a signal which fired a moment before `SIGTERM` may still be
    // queued, and dropping it silently is the one thing an alerting path
    // must not do. Bounded, because a channel nobody can reach must not
    // hold the process open.
    if let Some(pipeline) = pipeline.clone() {
        let flushed = tokio::task::spawn_blocking(move || {
            pipeline
                .delivery_router()
                .flush(std::time::Duration::from_secs(5))
        })
        .await
        .unwrap_or(false);
        if flushed {
            info!("signal delivery drained");
        } else {
            warn!("signal delivery did not drain within 5s — some signals were not sent");
        }
    }

    // Flush and close database
    let db_close = db.clone();
    if let Err(e) = tokio::task::spawn_blocking(move || {
        if let Err(e) = db_close.flush() {
            warn!(%e, "error flushing during shutdown");
        }
        if let Err(e) = db_close.close() {
            warn!(%e, "error closing database");
        }
    })
    .await
    {
        error!(%e, "database shutdown task panicked");
    }

    info!("chronixd stopped");
    Ok(())
}

/// One administrative route group, guarded by the capability it needs.
///
/// The routes carry their full paths rather than being `nest`ed, because
/// `route_inventory` reads the router out of this source and a nest whose
/// prefix is applied in two places is an inventory that can disagree with
/// itself.
fn admin_group(
    state: &AppState,
    action: chronix_security::authz::ChronixAction,
    routes: Router<AppState>,
) -> Router<AppState> {
    routes.layer(axum::middleware::from_fn_with_state(
        (state.clone(), action),
        crate::auth::capability_layer,
    ))
}

/// Build the axum router with all REST endpoints.
#[allow(clippy::needless_pass_by_value)] // auth_state moves into middleware closures
pub fn build_router(
    state: AppState,
    metrics_path: &str,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
    max_body_size: usize,
    auth_state: Option<crate::auth::AuthState>,
) -> Router {
    let metrics_path_owned = metrics_path.to_string();
    let state_for_metrics = state.clone();

    let mut router: Router<AppState> = Router::new()
        // Health / Readiness — un-versioned so probes work without
        // knowing the API version.
        .route("/health", get(http::health_handler))
        .route("/healthz", get(http::health_handler))  // k8s alias
        .route("/ready", get(http::ready_handler))
        .route("/readyz", get(http::ready_handler))    // k8s alias
        // Prometheus metrics endpoint.
        // Authentication is controlled by `metrics_require_auth` (default: true).
        // When `false`, the metrics path is auto-added to `auth.exempt_paths`
        // (see `run()`) so Prometheus can scrape without credentials.
        // This is a common pattern for internal monitoring endpoints that
        // are typically exposed only on an internal network or behind a
        // reverse proxy.
        .route(
            &metrics_path_owned,
            get({
                // `render()` returns whatever has been *recorded*, so the
                // engine's gauges are refreshed here. `statistics()` is what
                // sets them and it had no caller outside tests, so eleven of
                // the thirteen — series count, every memory term, disk usage —
                // were never exported by a running server, and Prometheus
                // answers "no data" for a metric nobody writes exactly as it
                // does for a quiet one.
                //
                // The cost is one pass over the catalog's segments. The one
                // expensive term is paid for elsewhere: `disk_usage_bytes()`
                // caches its directory walk for a minute.
                let stats_state = state_for_metrics.clone();
                move || {
                    let handle = metrics_handle.clone();
                    let state = stats_state.clone();
                    async move {
                        let _ = state.db.statistics();
                        // Same argument one subsystem over: a connector
                        // that cannot reach its broker had no gauge at all,
                        // and reported `Running` on the API.
                        if let Some(manager) = &state.connector_manager {
                            manager.refresh_gauges().await;
                        }
                        handle.render()
                    }
                }
            }),
        )
        // Write — chronix's JSON body, and InfluxDB Line Protocol at the
        // paths Telegraf posts to. `outputs.influxdb` appends `/write` to
        // its URL and `outputs.influxdb_v2` appends `/api/v2/write`, so a
        // server offering neither cannot be written to by Telegraf at all,
        // whatever else it implements.
        .route("/api/v1/write", post(http::write_handler))
        .route("/write", post(http::write_influx_handler))
        .route("/api/v2/write", post(http::write_influx_handler))
        .route("/api/v1/write/influx", post(http::write_influx_handler))
        // Chronix's own JSON query API.
        //
        // It used to live at `/api/v1/query`, which is where a Grafana
        // Prometheus datasource pointed at this server's base URL looks for
        // the *Prometheus* API — so the two collided and the Prometheus one
        // had to hide under `/api/v1/prom/`, which no client derives. The
        // native API moved instead: `/api/v1/query` now means what every
        // Prometheus client expects it to mean.
        .route("/api/v1/chronix/query", post(http::query_handler))
        .route("/api/v1/chronix/query/explain", post(http::query_explain_handler))
        .route("/api/v1/chronix/sql", post(http::sql_handler))
        // PromQL — at the paths a Prometheus client derives from a base URL,
        // and on both methods, because Grafana POSTs a form body by default.
        .route(
            "/api/v1/query",
            get(http::prom_instant_query_handler).post(http::prom_instant_query_handler),
        )
        .route(
            "/api/v1/query_range",
            get(http::prom_range_query_handler).post(http::prom_range_query_handler),
        )
        .route(
            "/api/v1/labels",
            get(http::prom_labels_handler).post(http::prom_labels_handler),
        )
        .route(
            "/api/v1/label/{name}/values",
            get(http::prom_label_values_handler).post(http::prom_label_values_handler),
        )
        .route(
            "/api/v1/series",
            get(http::prom_series_handler).post(http::prom_series_handler),
        )
        .route("/api/v1/metadata", get(http::prom_metadata_handler))
        .route("/api/v1/status/buildinfo", get(http::prom_buildinfo_handler))
        .route("/api/v1/rules", get(http::prom_empty_rules_handler))
        .route("/api/v1/alerts", get(http::prom_empty_alerts_handler))
        .route("/api/v1/query_exemplars", get(http::prom_empty_exemplars_handler))
        // The old prefix stays as an alias, so a deployment that configured
        // it explicitly keeps working.
        .route(
            "/api/v1/prom/query",
            get(http::prom_instant_query_handler).post(http::prom_instant_query_handler),
        )
        .route(
            "/api/v1/prom/query_range",
            get(http::prom_range_query_handler).post(http::prom_range_query_handler),
        )
        .route(
            "/api/v1/prom/labels",
            get(http::prom_labels_handler).post(http::prom_labels_handler),
        )
        .route(
            "/api/v1/prom/label/{name}/values",
            get(http::prom_label_values_handler).post(http::prom_label_values_handler),
        )
        .route(
            "/api/v1/prom/series",
            get(http::prom_series_handler).post(http::prom_series_handler),
        )
        .route("/api/v1/prom/metadata", get(http::prom_metadata_handler))
        // Signal triggers — 404 unless `[triggers]` is configured.
        .route(
            "/api/v1/triggers",
            get(http::list_triggers_handler).post(http::trigger_sql_handler),
        )
        .route(
            "/api/v1/triggers/{name}",
            get(http::get_trigger_handler).delete(http::drop_trigger_handler),
        )
        .route("/api/v1/signals", get(http::list_signals_handler))
        // Grafana annotations
        .route("/api/v1/annotations", get(http::annotations_handler))
        .route("/api/v1/annotations/stream", get(http::annotations_stream_handler))
        // CDC streaming (Server-Sent Events)
        .route("/api/v1/cdc/stream", get(http::cdc_stream_handler))
        // Dashboard export
        .route("/api/v1/dashboards/export", get(http::export_dashboards_handler))
        // Wire protocols: Prometheus remote write/read
        .route("/api/v1/prom/write", post(crate::wire::prometheus::remote_write_handler))
        .route("/api/v1/prom/read", post(crate::wire::prometheus::remote_read_handler))
        // Wire protocols: OTLP metrics
        // OTLP — at the path the Collector derives. `otlphttp` appends
        // `/v1/metrics` to its configured endpoint, so a server that only
        // listens on `/api/v1/otlp/metrics` needs the exporter's
        // `metrics_endpoint` overridden, which no default configuration does.
        .route("/v1/metrics", post(crate::wire::otlp::otlp_metrics_handler))
        .route("/api/v1/otlp/metrics", post(crate::wire::otlp::otlp_metrics_handler))
        // Management
        .route("/api/v1/measurements", get(http::list_measurements_handler))
        .route(
            "/api/v1/measurements/{name}/schema",
            get(http::get_schema_handler),
        )
        .route(
            "/api/v1/measurements/{name}/schema/fields",
            post(http::declare_field_handler),
        )
        .route(
            "/api/v1/measurements/{name}",
            delete(http::drop_measurement_handler),
        )
        .route(
            "/api/v1/measurements/{name}/restore",
            post(http::restore_measurement_handler),
        )
        .route("/api/v1/delete", post(http::delete_handler))
        // Bulk delete
        .route("/api/v1/delete_batch", post(http::delete_batch_handler))
        .route("/api/v1/rollups", get(http::list_rollups_handler).post(http::create_rollup_handler))
        .route(
            "/api/v1/rollups/{name}/refresh",
            post(http::refresh_rollup_handler),
        )
        .route("/api/v1/rollups/{name}", delete(http::delete_rollup_handler))
        .route(
            "/api/v1/export/parquet",
            post(http::export_parquet_handler),
        )
        // Connectors
        .route(
            "/api/v1/connectors",
            get(http::list_connectors_handler),
        )
        // OpenAPI specification
        .route(
            "/api/v1/openapi.json",
            get(crate::openapi::openapi_handler),
        )
        // ── Administration ──────────────────────────────────────────
        // Each group carries the capability it needs, so a credential can be
        // granted backups without also being able to mint API keys. They all
        // asked for `Admin` once, which is why nine granular capabilities
        // existed in the model and were requested by nothing.
        //
        // `capability_layer` is per group rather than one layer over
        // `/api/v1/admin`: an action derived from a path prefix is a mapping
        // that drifts from the routes, and this cannot.
        .merge(admin_group(
            &state,
            chronix_security::authz::ChronixAction::ManageModels,
            axum::Router::new()
                .route(
                    "/api/v1/admin/analytics/models",
                    get(crate::admin::list_models_handler),
                )
                .route(
                    "/api/v1/admin/analytics/models/{measurement}/{name}",
                    get(crate::admin::get_model_handler)
                        .delete(crate::admin::delete_model_handler),
                )
                .route(
                    "/api/v1/admin/analytics/retrain",
                    post(crate::admin::retrain_handler),
                ),
        ))
        .merge(admin_group(
            &state,
            chronix_security::authz::ChronixAction::ManageKeys,
            axum::Router::new()
                .route(
                    "/api/v1/admin/auth/keys",
                    post(crate::auth::create_key_handler).get(crate::auth::list_keys_handler),
                )
                .route(
                    "/api/v1/admin/auth/keys/{name}",
                    delete(crate::auth::revoke_key_handler),
                ),
        ))
        .merge(admin_group(
            &state,
            chronix_security::authz::ChronixAction::ManageConfig,
            axum::Router::new().route(
                "/api/v1/admin/log-level",
                put(http::update_log_level_handler),
            ),
        ))
        .merge(admin_group(
            &state,
            chronix_security::authz::ChronixAction::ManageBackups,
            axum::Router::new()
                .route("/api/v1/admin/backup", post(crate::admin::backup_handler))
                .route(
                    "/api/v1/admin/backup/verify",
                    post(crate::admin::verify_backup_handler),
                )
                .route("/api/v1/admin/restore", post(crate::admin::restore_handler)),
        ))
        .merge(admin_group(
            &state,
            chronix_security::authz::ChronixAction::ManageNamespaces,
            axum::Router::new()
                .route(
                    "/api/v1/namespaces",
                    post(crate::namespace::create_namespace_handler)
                        .get(crate::namespace::list_namespaces_handler),
                )
                .route(
                    "/api/v1/namespaces/{name}",
                    get(crate::namespace::get_namespace_handler)
                        .delete(crate::namespace::delete_namespace_handler),
                )
                .route(
                    "/api/v1/namespaces/{name}/usage",
                    get(crate::namespace::get_namespace_usage_handler),
                )
                .route(
                    "/api/v1/namespaces/{name}/quota",
                    put(crate::namespace::update_namespace_quota_handler),
                ),
        ));

    // Cluster administration is compiled with `--features cluster`. Three
    // capabilities rather than one: reading topology is not rebalancing.
    #[cfg(feature = "cluster")]
    {
        router = router
            .merge(admin_group(
                &state,
                chronix_security::authz::ChronixAction::ManageNodes,
                axum::Router::new()
                    .route(
                        "/api/v1/admin/nodes",
                        post(crate::admin::register_node_handler)
                            .get(crate::admin::list_nodes_handler)
                            .delete(crate::admin::deregister_node_handler),
                    )
                    .route(
                        "/api/v1/admin/heartbeat",
                        post(crate::admin::heartbeat_handler),
                    )
                    .route(
                        "/api/v1/admin/nodes/{id}/decommission",
                        post(crate::admin::decommission_node_handler),
                    ),
            ))
            .merge(admin_group(
                &state,
                chronix_security::authz::ChronixAction::ManageRegions,
                axum::Router::new()
                    .route(
                        "/api/v1/admin/regions",
                        post(crate::admin::create_region_handler),
                    )
                    .route(
                        "/api/v1/admin/regions/{id}/state",
                        put(crate::admin::update_region_state_handler),
                    ),
            ))
            .merge(admin_group(
                &state,
                chronix_security::authz::ChronixAction::ViewCluster,
                axum::Router::new()
                    .route(
                        "/api/v1/admin/routing",
                        get(crate::admin::get_routing_handler),
                    )
                    .route(
                        "/api/v1/admin/health",
                        get(crate::admin::cluster_health_handler),
                    )
                    .route(
                        "/api/v1/admin/topology",
                        get(crate::admin::cluster_topology_handler),
                    ),
            ))
            .merge(admin_group(
                &state,
                chronix_security::authz::ChronixAction::ManageCluster,
                axum::Router::new().route(
                    "/api/v1/admin/rebalance",
                    post(crate::admin::rebalance_handler),
                ),
            ));
    }

    // Namespace resolution middleware — applied BEFORE auth layer so that
    // Tower runs it AFTER auth (Tower wraps inside-out), ensuring
    // AuthContext is available for Cedar authorization checks.
    router = router.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        crate::namespace::namespace_layer,
    ));

    // Middleware layers: request_id, duration, rate-limiting
    router = router
        .layer(axum::middleware::from_fn(
            crate::request_id::request_id_layer,
        ))
        .layer(axum::middleware::from_fn(
            crate::http_metrics::request_duration_layer,
        ))
        .layer(axum::middleware::from_fn(
            crate::rate_limit::rate_limit_middleware,
        ))
        .layer({
            // Inject the global + per-user rate limiters into request extensions.
            let limiter = crate::rate_limit::build_limiter(
                state.config.server.rate_limit_rps,
                state.config.server.rate_limit_burst,
            );
            // Per-user rate limiter (None when rps == 0 → disabled).
            let user_limiter = crate::rate_limit::UserRateLimiter::new(
                state.config.server.per_user_rate_limit_rps,
                state.config.server.per_user_rate_limit_burst,
            );
            axum::middleware::from_fn(
                move |mut req: axum::extract::Request, next: axum::middleware::Next| {
                    let limiter = limiter.clone();
                    let user_limiter = user_limiter.clone();
                    async move {
                        if let Some(ref lim) = limiter {
                            req.extensions_mut().insert(lim.clone());
                        }
                        if let Some(ref ul) = user_limiter {
                            req.extensions_mut().insert(ul.clone());
                        }
                        next.run(req).await
                    }
                },
            )
        });

    // Apply auth middleware AFTER rate-limit layers so that
    // authentication runs BEFORE rate-limit tokens are consumed.
    // Previously, unauthenticated requests could exhaust rate-limit
    // buckets, enabling DoS of legitimate authenticated users.
    if auth_state.is_some() {
        info!("authentication enabled");
        router = router.layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::auth_layer,
        ));
    }

    router
        // Outermost, so it also covers a path no route matches and a method
        // no route accepts: every error this server returns is the same JSON
        // envelope, whoever produced it.
        .layer(axum::middleware::from_fn(
            crate::error::error_envelope_layer,
        ))
        .layer(TraceLayer::new_for_http())
        // CORS middleware for browser-based clients.
        // Configured via `cors_allowed_origins`:
        //   Empty (default) → restrictive same-origin policy.
        //   - ["*"]           → permissive (requires `allow_unsafe_cors: true`).
        //   - ["https://a.example.com", ...] → explicit allow-list.
        .layer({
            let cors_origins = &state.config.server.cors_allowed_origins;
            if cors_origins.is_empty() {
                // Restrictive by default: same-origin only.
                CorsLayer::new()
            } else if cors_origins.len() == 1 && cors_origins[0] == "*" {
                // Validated by `ServerConfig::validate_cors()` at startup;
                // `allow_unsafe_cors` must be true to reach this path.
                warn!("CORS configured with wildcard origin — not recommended for production");
                CorsLayer::permissive()
            } else {
                let origins: Vec<_> = cors_origins
                    .iter()
                    .filter_map(|o| o.parse().ok())
                    .collect();
                CorsLayer::new()
                    .allow_origin(AllowOrigin::list(origins))
                    .allow_methods([
                        Method::GET,
                        Method::POST,
                        Method::PUT,
                        Method::DELETE,
                        Method::OPTIONS,
                    ])
                    .allow_headers([
                        header::AUTHORIZATION,
                        header::CONTENT_TYPE,
                        header::ACCEPT,
                    ])
            }
        })
        // The body limit is applied to the *decompressed* body, so the
        // decompression layer sits inside it: a gzip bomb is caught by the
        // limit rather than by the allocator.
        .layer(RequestBodyLimitLayer::new(max_body_size))
        // The OpenTelemetry Collector's `otlphttp` exporter and Telegraf's
        // InfluxDB outputs both gzip by default. Without this every one of
        // their batches was a decode error — a 400, which both clients treat
        // as permanent, so the data was dropped rather than retried.
        // `pass_through_unaccepted` is load-bearing: Prometheus remote
        // write and read set `Content-Encoding: snappy`, which this layer
        // does not handle and would otherwise answer with 415 — so adding
        // gzip support for Telegraf and the OTel Collector would have broken
        // every Prometheus sender. Snappy is decompressed by the handler
        // that understands the payload.
        .layer(
            RequestDecompressionLayer::new()
                .gzip(true)
                .pass_through_unaccepted(true),
        )
        .with_state(state)
}

/// Latency buckets, in seconds, for every `*_duration_seconds` histogram.
///
/// Spread from 100 µs to a minute because the same suffix covers a memtable
/// write (microseconds) and a compaction (seconds). Eighteen buckets is
/// eighteen extra series per label set, which is the cost of being able to
/// aggregate a quantile across replicas at all.
const DURATION_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0, 30.0, 60.0,
];

/// Points per write batch: one point at a time through to a bulk import.
const BATCH_SIZE_BUCKETS: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 50000.0,
];

/// Compression ratio — a multiple, never below 1.
const RATIO_BUCKETS: &[f64] = &[
    1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 16.0, 24.0, 32.0, 48.0, 64.0,
];

/// The Prometheus exporter, with buckets configured.
///
/// **Without buckets `metrics-exporter-prometheus` renders every `histogram!`
/// as a Prometheus *summary*** — a different metric with no `_bucket` series,
/// so `histogram_quantile()` has nothing to read and every latency panel in
/// `dashboards/` draws nothing. Summary quantiles also cannot be aggregated
/// across replicas.
///
/// `set_buckets` sets the **default**, so a histogram added later renders as
/// a histogram even if nobody adds a matcher for it; the matchers only refine.
///
/// The test harness builds its recorder from here too, so a test can observe
/// the exposition format the server actually serves.
///
/// # Errors
///
/// [`ServerError::Internal`] if a bucket list is empty, which the constants
/// above are not.
pub fn prometheus_builder() -> Result<metrics_exporter_prometheus::PrometheusBuilder, ServerError> {
    use metrics_exporter_prometheus::Matcher;
    let bad = |e: metrics_exporter_prometheus::BuildError| {
        ServerError::Internal(format!("invalid Prometheus buckets: {e}"))
    };
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets(DURATION_BUCKETS)
        .map_err(bad)?
        .set_buckets_for_metric(
            Matcher::Suffix("_duration_seconds".to_owned()),
            DURATION_BUCKETS,
        )
        .map_err(bad)?
        .set_buckets_for_metric(
            Matcher::Full("chronix_batch_size".to_owned()),
            BATCH_SIZE_BUCKETS,
        )
        .map_err(bad)?
        .set_buckets_for_metric(
            Matcher::Full("chronix_segment_compression_ratio".to_owned()),
            RATIO_BUCKETS,
        )
        .map_err(bad)
}

/// Set up Prometheus metrics exporter and return the handle for rendering.
///
/// # Errors
///
/// [`ServerError::Internal`] if the recorder cannot be installed — which
/// happens when one already is, since the recorder is process-global.
pub fn setup_prometheus() -> Result<metrics_exporter_prometheus::PrometheusHandle, ServerError> {
    prometheus_builder()?
        .install_recorder()
        .map_err(|e| ServerError::Internal(format!("failed to install Prometheus recorder: {e}")))
}

/// The one shutdown signal, shared by every server task.
///
/// Each of the three servers used to build its own `shutdown_signal()` future
/// over the OS signal, which coordinated a `SIGTERM` correctly and nothing
/// else. Two things need the same broadcast:
///
/// - **A server that stops on its own must stop the others.** A gRPC listener
///   that cannot bind used to leave `chronixd` running as an HTTP-only server
///   with an error in the log, because the outer wait was a `select!` that
///   noticed the first exit and the process then closed the database under the
///   two survivors. Now the exit of any server — clean, failed or panicking —
///   triggers this, so the others drain and the shutdown sequence is the same
///   one a `SIGTERM` takes.
/// - **Nothing may close the database before the last server has drained.**
///   That is the caller's job (`join!` on all three handles), and it is only
///   sound because every server is reachable by this signal.
#[derive(Clone, Debug)]
pub struct Shutdown(tokio::sync::watch::Sender<bool>);

impl Shutdown {
    /// A signal nothing has fired yet.
    #[must_use]
    pub fn new() -> Self {
        Self(tokio::sync::watch::channel(false).0)
    }

    /// Fire it. Idempotent: later calls are no-ops.
    ///
    /// `send_replace`, not `send`: `watch::Sender::send` returns `Err` and
    /// **leaves the value unchanged** when no receiver is currently
    /// subscribed, so a trigger that arrives before a server task has called
    /// `wait()` would be lost and that task would never drain. The unit test
    /// below is what found it.
    pub fn trigger(&self) {
        let _ = self.0.send_replace(true);
    }

    /// A future that resolves once the signal has fired.
    ///
    /// Owned and `'static`, so it can be handed to `serve_with_shutdown`.
    pub fn wait(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut rx = self.0.subscribe();
        async move {
            // `Err` means every sender is gone, which can only happen once
            // the server is being torn down — treat it as fired.
            let _ = rx.wait_for(|fired| *fired).await;
        }
    }

    /// Fire the signal when this guard drops.
    ///
    /// Put one inside a server task and its exit — returning, erroring or
    /// panicking — brings the other servers down with it.
    #[must_use]
    pub fn guard(&self) -> ShutdownGuard {
        ShutdownGuard(self.clone())
    }

    /// Spawn the task that fires this signal on `SIGTERM` or `SIGINT`.
    pub fn on_os_signal(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            wait_for_os_signal().await;
            info!("shutdown signal received");
            this.trigger();
        });
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

/// Fires a [`Shutdown`] when dropped.
#[derive(Debug)]
pub struct ShutdownGuard(Shutdown);

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.0.trigger();
    }
}

/// Wait for SIGTERM or SIGINT (Ctrl-C).
async fn wait_for_os_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!(error = %e, "failed to install Ctrl+C handler");
            // Fall through — process will still terminate via other signals
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

/// Build the signal-trigger pipeline when `[triggers]` is configured.
///
/// Restores the persisted trigger catalog before returning, so a restart does
/// not silently stop alerting.
/// Say so, loudly, when a listener is reachable from off the machine and
/// nothing authenticates.
///
/// The three listen addresses default to `0.0.0.0` and `[auth]` defaults to
/// absent, so `chronixd` with no configuration is an unauthenticated database
/// on every interface — and it said nothing at all. It is still the default,
/// because a container that had to be told to bind `0.0.0.0` is a container
/// nobody can run; what changes is that the operator is told, once, in the
/// line right after "started", with the two ways to fix it.
///
/// Not a refusal: a `chronixd` behind a firewall or a service mesh is a
/// legitimate deployment, and a server that will not start is a server people
/// work around with `--i-know-what-i-am-doing`.
fn warn_if_exposed_without_auth(config: &crate::config::ServerConfig) {
    let exposed = exposed_listeners(config);
    if exposed.is_empty() {
        return;
    }
    tracing::warn!(
        listeners = %exposed.join(", "),
        "no [auth] section: these listeners accept reads, writes and deletes \
         from anyone who can reach them. Add [[auth.api_keys]], or bind to \
         127.0.0.1."
    );
}

/// The listeners this configuration would open to the network with nothing
/// authenticating them, named. Empty when `[auth]` is configured or every
/// listener is on loopback.
///
/// **`--check-config` and the running server ask this through one function.**
/// They asked it separately, and disagreed: `--check-config` warned whenever
/// `[auth]` was absent, whatever the addresses were, so the pre-deploy check
/// the deployment guide recommends printed a security warning for a server
/// bound entirely to loopback — every time, which is how a warning stops
/// being read.
pub fn exposed_listeners(config: &crate::config::ServerConfig) -> Vec<String> {
    if config.auth.is_some() {
        return Vec::new();
    }
    [
        ("HTTP", config.server.http_addr),
        ("gRPC", config.server.grpc_addr()),
        ("Flight SQL", config.server.flight_addr()),
    ]
    .into_iter()
    .filter(|(_, addr)| !addr.ip().is_loopback())
    .map(|(name, addr)| format!("{name} {addr}"))
    .collect()
}

fn build_trigger_pipeline(
    config: &crate::config::ServerConfig,
    db: &Arc<chronix::Chronix>,
) -> Result<Option<Arc<chronix::Pipeline>>, ServerError> {
    let Some(cfg) = config.triggers.as_ref() else {
        return Ok(None);
    };

    // Every entry resolved, not just the first: a rotation that leaves one
    // secret unresolved signs with fewer keys than the operator declared and
    // the receivers holding that key start failing — silently, one delivery
    // at a time.
    let secrets = cfg
        .webhook_signing_secrets
        .iter()
        .map(|raw| {
            crate::config::resolve_env_reference(raw).map_err(|var| {
                ServerError::Config(crate::config::ServerConfigError::Invalid(format!(
                    "triggers.webhook_signing_secrets references ${{{var}}}, which is not set"
                )))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // A relative catalog path belongs under the data directory, not under
    // whatever directory the process happened to start in.
    let catalog_path = cfg.catalog_path.as_ref().map(|p| {
        if p.is_absolute() {
            p.clone()
        } else {
            config.database.data_dir.join(p)
        }
    });

    let pipeline = Arc::new(chronix::Pipeline::with_config(chronix::PipelineConfig {
        trigger_catalog_path: catalog_path,
        signal_store_capacity: cfg.signal_store_capacity,
        webhook_signing_secrets: secrets,
        webhook_timeout: std::time::Duration::from_secs(cfg.webhook_timeout_secs),
        webhook_allow_private_targets: cfg.webhook_allow_private_targets,
        ..chronix::PipelineConfig::default()
    }));

    let _ = db;
    Ok(Some(pipeline))
}

/// Refuse a configuration section this binary cannot honour.
///
/// A section that parses and then does nothing is the same defect as a
/// documented environment variable nothing reads: no failure, and the
/// operator finds out from a graph that never fills. `[cold_archive]` has
/// always refused this way; `[kafka]` and `[mqtt]` did not, and instead
/// registered a connector that logged "enable the feature" and then reported
/// itself healthy for ever.
///
/// # Errors
///
/// Returns [`ServerError::Config`] naming the section and the feature flag
/// to rebuild with.
pub fn reject_unbuildable_sections(
    config: &crate::config::ServerConfig,
) -> Result<(), ServerError> {
    let refuse = |section: &str, feature: &str| {
        ServerError::Config(crate::config::ServerConfigError::Invalid(format!(
            "[{section}] is configured but this binary was built without the \
             `{feature}` feature: rebuild with `--features {feature}`, or \
             remove the section"
        )))
    };
    #[cfg(not(feature = "kafka"))]
    if config.kafka.is_some() {
        return Err(refuse("kafka", "kafka"));
    }
    #[cfg(not(feature = "mqtt"))]
    if config.mqtt.is_some() {
        return Err(refuse("mqtt", "mqtt"));
    }
    let _ = (&config, &refuse);
    Ok(())
}

/// Run a cold-archiving pass periodically, when `[cold_archive]` is configured.
///
/// Archiving was an embedded API call with no trigger anywhere in the server:
/// a deployment that wanted it had to write its own scheduler around
/// `Chronix::archive_cold_segments`. A reclamation nobody runs is one that
/// runs for the first time when the disk is already full.
///
/// The pass is bounded (`max_objects_per_run`) and its failures are logged
/// rather than fatal: an object store that is briefly unreachable must not
/// take the database down, and the next pass picks up where this one stopped.
#[cfg(feature = "object-store")]
fn spawn_cold_archiver(
    config: &crate::config::ServerConfig,
    db: &Arc<chronix::Chronix>,
) -> Result<(), ServerError> {
    let Some(cfg) = config.cold_archive.as_ref() else {
        return Ok(());
    };
    if cfg.remote_url.is_empty() {
        return Err(ServerError::Config(
            crate::config::ServerConfigError::Invalid(
                "cold_archive.remote_url must not be empty".into(),
            ),
        ));
    }

    let archive = chronix::cold_archive::ArchiveConfig {
        cold_after: std::time::Duration::from_secs(cfg.cold_after_secs),
        remote_url: cfg.remote_url.clone(),
        max_objects_per_run: cfg.max_objects_per_run,
    };
    let period = std::time::Duration::from_secs(cfg.interval_secs.max(1));
    let db = Arc::clone(db);

    info!(
        remote = %archive.remote_url,
        interval_secs = cfg.interval_secs,
        "cold archiving enabled"
    );

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(period);
        // The first tick fires immediately; skip it so a restart loop cannot
        // turn into an archiving loop.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match db.archive_cold_segments(&archive).await {
                Ok(outcome) if outcome.objects > 0 || outcome.failed > 0 => {
                    info!(
                        objects = outcome.objects,
                        segments = outcome.segments,
                        rows = outcome.rows,
                        bytes = outcome.bytes,
                        failed = outcome.failed,
                        more_pending = outcome.more_pending,
                        "cold archive pass complete"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    // Not fatal: the data is still hot and queryable, which is
                    // the correct failure direction for an operation whose
                    // next step is a delete.
                    tracing::warn!(error = %e, "cold archive pass failed");
                    metrics::counter!("chronix_cold_archive_pass_failures_total").increment(1);
                }
            }
        }
    });
    Ok(())
}

/// Without the `object-store` feature there is nothing to archive with, so a
/// configuration asking for it is a **startup error** rather than a section
/// the binary quietly ignores.
///
/// A configured-and-ignored setting is the defect class this tree has just
/// spent a pass removing: nothing fails, and the operator finds out when the
/// disk fills.
#[cfg(not(feature = "object-store"))]
fn spawn_cold_archiver(
    config: &crate::config::ServerConfig,
    _db: &Arc<chronix::Chronix>,
) -> Result<(), ServerError> {
    if config.cold_archive.is_some() {
        return Err(ServerError::Config(
            crate::config::ServerConfigError::Invalid(
                "[cold_archive] is configured but this binary was built without the \
                 `object-store` feature: rebuild with `--features object-store`, or \
                 remove the section"
                    .into(),
            ),
        ));
    }
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod unbuildable_section_tests {
    use super::reject_unbuildable_sections;

    /// A `[kafka]` section is accepted or refused according to the build,
    /// and the refusal names the flag.
    ///
    /// Asserted in both directions because the whole defect was a state that
    /// only existed in one of them: without the feature the connector used
    /// to register, log "enable the feature", and then report itself healthy
    /// while ingesting nothing.
    #[test]
    fn a_connector_section_needs_its_feature() {
        let mut config = crate::config::ServerConfig::default();
        assert!(reject_unbuildable_sections(&config).is_ok());

        config.kafka = Some(crate::connector::KafkaConfig {
            namespace: "default".into(),
            brokers: "localhost:9092".into(),
            group_id: "g".into(),
            topics: vec!["t".into()],
            format: crate::connector::ConnectorFormat::Json,
            auto_offset_reset: "latest".into(),
            topic_measurement_map: Default::default(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            security_protocol: None,
            ca_cert: None,
            credential_file: None,
        });

        let verdict = reject_unbuildable_sections(&config);
        if cfg!(feature = "kafka") {
            assert!(verdict.is_ok(), "a build with the feature must accept it");
        } else {
            let err = verdict.expect_err("a build without the feature must refuse");
            let msg = err.to_string();
            assert!(
                msg.contains("--features kafka"),
                "must name the flag: {msg}"
            );
        }
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::Shutdown;

    /// Every waiter resolves once the signal fires.
    #[tokio::test]
    async fn a_trigger_releases_every_waiter() {
        let shutdown = Shutdown::new();
        let a = shutdown.wait();
        let b = shutdown.wait();
        shutdown.trigger();
        tokio::join!(a, b);
    }

    /// A server that stops on its own stops the others.
    ///
    /// The guard is what turns "the gRPC listener could not bind" into a
    /// clean shutdown of the whole process instead of an HTTP-only server
    /// with an error in the log — and it fires on a panicking task too,
    /// because a panic unwinds through the guard's `Drop`.
    #[tokio::test]
    async fn a_dropped_guard_triggers_the_shutdown() {
        let shutdown = Shutdown::new();
        let waiter = shutdown.wait();
        drop(shutdown.guard());
        waiter.await;
    }

    /// A waiter created *after* the signal fired does not hang.
    ///
    /// The reason `join!` on the three server handles is safe: a task that
    /// subscribes late — a TLS acceptor spawned during startup, say — must
    /// see the signal that has already been sent, or shutdown deadlocks and
    /// the drain timeout never applies because nothing reaches it.
    #[tokio::test]
    async fn a_late_waiter_sees_a_signal_that_already_fired() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        tokio::time::timeout(std::time::Duration::from_secs(5), shutdown.wait())
            .await
            .expect("a late waiter must not hang");
    }
}
