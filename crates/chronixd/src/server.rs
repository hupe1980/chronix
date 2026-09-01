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
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

use chronix::compaction_scheduler::CompactionScheduler;
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
    #[cfg(not(feature = "cluster"))]
    if config.cluster.is_some() {
        return Err(ServerError::Internal(
            "this chronixd build has no cluster support — rebuild with `--features cluster`".into(),
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
                let grpc_addr = config.grpc_addr.to_string();
                let state =
                    crate::cluster::start_data_node(cluster_cfg, &grpc_addr, db.clone()).await?;
                Some(ClusterState::Data(state))
            }
        }
    } else {
        None
    };

    // ── Start compaction scheduler ─────────────────────────────────────
    let scheduler = CompactionScheduler::new(db.clone());
    scheduler.start();
    info!("compaction scheduler started");

    // ── Prometheus metrics ─────────────────────────────────────────────
    let metrics_handle = setup_prometheus()?;

    // ── Connector manager ───────────────────────────────────────────────
    let connector_manager = Arc::new(
        crate::connector::ConnectorManager::new(db.clone())
            .with_max_start_retries(config.connector_start_retries)
            .with_start_retry_backoff_ms(config.connector_start_retry_backoff_ms),
    );

    // Register configured connectors
    if let Some(ref kafka_cfg) = config.kafka {
        let consumer =
            crate::kafka::KafkaConsumer::new_arc("kafka-default", kafka_cfg.clone(), db.clone());
        connector_manager.register(consumer).await;
    }

    if let Some(ref mqtt_cfg) = config.mqtt {
        let subscriber =
            crate::mqtt::MqttSubscriber::new_arc("mqtt-default", mqtt_cfg.clone(), db.clone());
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
    if !config.metrics_require_auth {
        if let Some(ref mut auth) = config.auth {
            if !auth.exempt_paths.contains(&config.metrics_path) {
                auth.exempt_paths.push(config.metrics_path.clone());
            }
        }
    }

    let auth_state = config
        .auth
        .as_ref()
        .map(crate::auth::AuthState::from_config)
        .transpose()
        .map_err(|e| ServerError::Internal(format!("auth configuration error: {e}")))?;

    // ── Authorization engine (optional) ──────────────────────────────────
    let authz_engine = if let Some(ref policy_dir) = config.authz_policy_dir {
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
        let logger = chronix_security::audit::AuditLogger::new();
        // Always add the tracing-based sink so audit events appear in
        // structured logs (forwarded to whatever tracing subscriber is
        // configured, e.g. JSON file, stdout, OTLP).
        logger.add_sink(Box::new(chronix_security::audit::TracingSink));
        info!("audit logger enabled with tracing sink");
        Some(Arc::new(logger))
    };

    // ── Shared state ───────────────────────────────────────────────────
    let sql_contexts = crate::namespace::SqlContexts::new(db.clone());
    #[cfg(feature = "cluster")]
    let meta_client = cluster_state.as_ref().map(|cs| match cs {
        ClusterState::Meta(s) => Arc::clone(&s.meta_client),
        ClusterState::Data(s) => Arc::clone(s.manager.meta_client()),
    });
    let state: AppState = Arc::new(SharedState {
        db: db.clone(),
        config: config.clone(),
        start_time,
        connector_manager: Some(connector_manager.clone()),
        sql_contexts,
        auth_state: auth_state.clone(),
        #[cfg(feature = "cluster")]
        meta_client,
        namespace_registry: Some(Arc::new(chronix_security::tenant::NamespaceRegistry::new())),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        #[cfg(feature = "chaos")]
        chaos_agent: None,
        #[cfg(feature = "chaos")]
        chaos_guards: parking_lot::Mutex::new(Vec::new()),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        authz_engine,
        audit_logger: audit_logger.clone(),
        namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter::new(),
        // Write dedup cache for idempotency keys.
        write_dedup_cache: crate::http::WriteDedupCache::new(
            config.dedup_window_secs,
            config.max_dedup_entries,
        ),
        write_timeout: std::time::Duration::from_secs(config.write_timeout_secs),
        openapi_json: std::sync::OnceLock::new(),
    });

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

    let metrics_path = config.metrics_path.clone();
    let app = build_router(
        state,
        &metrics_path,
        metrics_handle,
        config.max_body_size,
        auth_state,
    );

    let http_addr = config.http_addr;
    let shutdown_timeout = std::time::Duration::from_secs(config.shutdown_timeout_secs);
    let tls_config_clone = config.tls.clone();
    let http_handle = if let Some(rustls_cfg) = tls_rustls_config {
        let shutdown = shutdown_signal();
        let tls_reload_interval = tls_config_clone
            .as_ref()
            .map_or(0, |t| t.reload_interval_secs);
        let tls_config_for_watcher = tls_config_clone.clone();
        tokio::spawn(async move {
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
                shutdown.await;
                server_handle.graceful_shutdown(Some(http_drain_timeout));
            });
            axum_server::bind_rustls(http_addr, rustls_config)
                .handle(handle)
                .serve(app.into_make_service())
                .await
        })
    } else {
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(http_addr).await?;
            info!(%http_addr, "HTTP server listening");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
        })
    };

    // ── gRPC server (tonic) ────────────────────────────────────────────
    let grpc_addr = config.grpc_addr;
    let grpc_service = ChronixGrpcService::with_streaming_config(
        db.clone(),
        start_time,
        config.stream_batch_size,
        config.stream_batch_interval_ms,
        config.dedup_window_secs,
    )
    .with_multi_tenancy(config.multi_tenancy)
    .with_max_dedup_entries(config.max_dedup_entries)
    .with_sql_limits(config.sql_query_timeout_secs, config.sql_max_rows)
    .with_write_timeout(std::time::Duration::from_secs(config.write_timeout_secs));
    let tonic_tls_grpc = tonic_tls.clone();
    let grpc_tls_acceptor_grpc = grpc_tls_acceptor.clone();
    let grpc_keepalive_secs = config.grpc_keepalive_secs;
    let grpc_keepalive_timeout_secs = config.grpc_keepalive_timeout_secs;
    let grpc_reflection_enabled = config.grpc_reflection;
    let grpc_drain_timeout = shutdown_timeout;

    let grpc_handle = tokio::spawn(async move {
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
            tokio::select! {
                res = router.serve_with_incoming_shutdown(incoming, shutdown_signal()) => {
                    res.map_err(std::io::Error::other)
                }
                _ = async {
                    shutdown_signal().await;
                    tokio::time::sleep(grpc_drain_timeout).await;
                } => {
                    warn!(timeout = ?grpc_drain_timeout, "gRPC drain timeout exceeded, forcing shutdown");
                    Ok(())
                }
            }
        } else {
            tokio::select! {
                res = router.serve_with_shutdown(grpc_addr, shutdown_signal()) => {
                    res.map_err(std::io::Error::other)
                }
                _ = async {
                    shutdown_signal().await;
                    tokio::time::sleep(grpc_drain_timeout).await;
                } => {
                    warn!(timeout = ?grpc_drain_timeout, "gRPC drain timeout exceeded, forcing shutdown");
                    Ok(())
                }
            }
        }
    });

    // ── Flight SQL server ──────────────────────────────────────────────
    let flight_addr = config.flight_addr;
    let flight_drain_timeout = shutdown_timeout;
    let flight_service = ChronixFlightSqlService::new(db.clone())
        .with_multi_tenancy(config.multi_tenancy)
        .with_limits(
            std::time::Duration::from_secs(config.sql_query_timeout_secs),
            config.sql_max_rows,
        )
        .with_write_timeout(std::time::Duration::from_secs(config.write_timeout_secs));

    let flight_handle = tokio::spawn(async move {
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
            tokio::select! {
                res = router.serve_with_incoming_shutdown(incoming, shutdown_signal()) => {
                    res.map_err(std::io::Error::other)
                }
                _ = async {
                    shutdown_signal().await;
                    tokio::time::sleep(flight_drain_timeout).await;
                } => {
                    warn!(timeout = ?flight_drain_timeout, "Flight SQL drain timeout exceeded, forcing shutdown");
                    Ok(())
                }
            }
        } else {
            tokio::select! {
                res = router.serve_with_shutdown(flight_addr, shutdown_signal()) => {
                    res.map_err(std::io::Error::other)
                }
                _ = async {
                    shutdown_signal().await;
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
        http = %config.http_addr,
        grpc = %config.grpc_addr,
        flight_sql = %config.flight_addr,
        data_dir = %config.database.data_dir.display(),
        tls = config.tls.is_some(),
        "chronixd started"
    );

    // ── Wait for any server to finish ──────────────────────────────────
    tokio::select! {
        res = http_handle => {
            match res {
                Err(e) => error!(%e, "HTTP server task panicked"),
                Ok(Err(e)) => error!(%e, "HTTP server error"),
                Ok(Ok(())) => {}
            }
        }
        res = grpc_handle => {
            match res {
                Err(e) => error!(%e, "gRPC server task panicked"),
                Ok(Err(e)) => error!(%e, "gRPC server error"),
                Ok(Ok(())) => {}
            }
        }
        res = flight_handle => {
            match res {
                Err(e) => error!(%e, "Flight SQL server task panicked"),
                Ok(Err(e)) => error!(%e, "Flight SQL server error"),
                Ok(Ok(())) => {}
            }
        }
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

    scheduler.stop();
    info!("compaction scheduler stopped");

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

    let mut router = Router::new()
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
            get(move || {
                let handle = metrics_handle.clone();
                async move { handle.render() }
            }),
        )
        // Write
        .route("/api/v1/write", post(http::write_handler))
        .route("/api/v1/write/influx", post(http::write_influx_handler))
        // Query
        .route("/api/v1/query", post(http::query_handler))
        // Query explain
        .route("/api/v1/query/explain", post(http::query_explain_handler))
        // SQL
        .route("/api/v1/sql", post(http::sql_handler))
        // PromQL
        .route("/api/v1/prom/query", get(http::prom_instant_query_handler).post(http::prom_instant_query_handler))
        .route("/api/v1/prom/query_range", get(http::prom_range_query_handler).post(http::prom_range_query_handler))
        .route("/api/v1/prom/labels", get(http::prom_labels_handler))
        .route("/api/v1/prom/label/{name}/values", get(http::prom_label_values_handler))
        .route("/api/v1/prom/series", get(http::prom_series_handler))
        .route("/api/v1/prom/metadata", get(http::prom_metadata_handler))
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
        .route("/api/v1/otlp/metrics", post(crate::wire::otlp::otlp_metrics_handler))
        // Management
        .route("/api/v1/measurements", get(http::list_measurements_handler))
        .route(
            "/api/v1/measurements/{name}/schema",
            get(http::get_schema_handler),
        )
        .route(
            "/api/v1/measurements/{name}",
            delete(http::drop_measurement_handler),
        )
        .route("/api/v1/delete", post(http::delete_handler))
        // Bulk delete
        .route("/api/v1/delete_batch", post(http::delete_batch_handler))
        .route("/api/v1/rollups", get(http::list_rollups_handler).post(http::create_rollup_handler))
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
        // Admin — cluster management (Cedar Admin authorization enforced via layer)
        .nest("/api/v1/admin", {
            let admin_routes = axum::Router::new()
                // Admin — analytics model management
                .route(
                    "/analytics/models",
                    get(crate::admin::list_models_handler),
                )
                .route(
                    "/analytics/models/{measurement}/{name}",
                    get(crate::admin::get_model_handler)
                        .delete(crate::admin::delete_model_handler),
                )
                .route(
                    "/analytics/retrain",
                    post(crate::admin::retrain_handler),
                );
            // Admin — cluster management (compiled with `--features cluster`)
            #[cfg(feature = "cluster")]
            let admin_routes = admin_routes
                .route(
                    "/nodes",
                    post(crate::admin::register_node_handler)
                        .get(crate::admin::list_nodes_handler)
                        .delete(crate::admin::deregister_node_handler),
                )
                .route(
                    "/heartbeat",
                    post(crate::admin::heartbeat_handler),
                )
                .route(
                    "/regions",
                    post(crate::admin::create_region_handler),
                )
                .route(
                    "/regions/{id}/state",
                    put(crate::admin::update_region_state_handler),
                )
                .route(
                    "/routing",
                    get(crate::admin::get_routing_handler),
                )
                .route(
                    "/health",
                    get(crate::admin::cluster_health_handler),
                )
                .route(
                    "/topology",
                    get(crate::admin::cluster_topology_handler),
                )
                .route(
                    "/rebalance",
                    post(crate::admin::rebalance_handler),
                )
                .route(
                    "/nodes/{id}/decommission",
                    post(crate::admin::decommission_node_handler),
                );
            // Admin — chaos injection (compiled with `--features chaos`)
            #[cfg(feature = "chaos")]
            let admin_routes = admin_routes
                .route(
                    "/chaos/inject",
                    post(crate::admin::chaos_inject_handler),
                )
                .route(
                    "/chaos",
                    get(crate::admin::chaos_list_handler)
                        .delete(crate::admin::chaos_clear_all_handler),
                )
                .route(
                    "/chaos/{id}",
                    delete(crate::admin::chaos_clear_handler),
                )
                ;

            admin_routes
                // Admin — API key management (requires admin authz)
                .route(
                    "/auth/keys",
                    post(crate::auth::create_key_handler).get(crate::auth::list_keys_handler),
                )
                .route(
                    "/auth/keys/{name}",
                    delete(crate::auth::revoke_key_handler),
                )
                // Admin — runtime log-level adjustment
                .route(
                    "/log-level",
                    put(http::update_log_level_handler),
                )
                // Admin — backup / restore
                .route(
                    "/backup",
                    post(crate::admin::backup_handler),
                )
                .route(
                    "/restore",
                    post(crate::admin::restore_handler),
                )
                .route(
                    "/restore/pitr",
                    post(crate::admin::pitr_restore_handler),
                )
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    crate::auth::admin_authz_layer,
                ))
                .with_state(state.clone())
        })
        // Namespaces — multi-tenancy management (Cedar Admin authorization enforced)
        .nest("/api/v1/namespaces", {

            axum::Router::new()
                .route(
                    "/",
                    post(crate::namespace::create_namespace_handler)
                        .get(crate::namespace::list_namespaces_handler),
                )
                .route(
                    "/{name}",
                    get(crate::namespace::get_namespace_handler)
                        .delete(crate::namespace::delete_namespace_handler),
                )
                .route(
                    "/{name}/usage",
                    get(crate::namespace::get_namespace_usage_handler),
                )
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    crate::auth::admin_authz_layer,
                ))
                .with_state(state.clone())
        });

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
                state.config.rate_limit_rps,
                state.config.rate_limit_burst,
            );
            // Per-user rate limiter (None when rps == 0 → disabled).
            let user_limiter = crate::rate_limit::UserRateLimiter::new(
                state.config.per_user_rate_limit_rps,
                state.config.per_user_rate_limit_burst,
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
        .layer(TraceLayer::new_for_http())
        // CORS middleware for browser-based clients.
        // Configured via `cors_allowed_origins`:
        //   Empty (default) → restrictive same-origin policy.
        //   - ["*"]           → permissive (requires `allow_unsafe_cors: true`).
        //   - ["https://a.example.com", ...] → explicit allow-list.
        .layer({
            let cors_origins = &state.config.cors_allowed_origins;
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
        .layer(RequestBodyLimitLayer::new(max_body_size))
        .with_state(state)
}

/// Set up Prometheus metrics exporter and return the handle for rendering.
pub fn setup_prometheus() -> Result<metrics_exporter_prometheus::PrometheusHandle, ServerError> {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .install_recorder()
        .map_err(|e| ServerError::Internal(format!("failed to install Prometheus recorder: {e}")))
}

/// Wait for SIGTERM or SIGINT (Ctrl-C).
///
/// # Multiple `shutdown_signal()` futures
///
/// Each server task (HTTP, gRPC, Flight SQL) creates its own independent
/// `shutdown_signal()` future. This is intentional: every `tokio::select!`
/// branch needs its own future because `Signal::recv()` is **not** `Clone`
/// and a single shared future cannot be polled from multiple tasks.
///
/// All futures resolve on the **same** OS signal, so shutdown is
/// coordinated even though the futures are independent. A
/// `tokio::sync::watch` or `CancellationToken` could consolidate this,
/// but the current approach is zero-allocation and correct.
async fn shutdown_signal() {
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
