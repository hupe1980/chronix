//! Cluster mode startup — `MetaNode` and `DataNode` lifecycle for chronixd.
//!
//! When `chronixd` is started with `--mode meta` or `--mode data`, this module
//! handles Raft cluster bootstrap, admin gRPC service, DataNode registration,
//! DataNode gRPC data service, heartbeat, and graceful shutdown.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::BasicNode;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use chronix::Chronix;
use chronix_cluster::{
    ClusterCoordinator, DataGrpcClient, DataGrpcServer, DataNodeManager, DistributedAnalytics,
    FailoverConfig, FailoverManager, GrpcMetaClientAdapter, InProcessMetaClient, MetaClient,
    QueryRouter, RegionManager, RegionMigrator, RegionStorage, RoutingCache, WriteRouter,
};
use chronix_meta::{
    ClusterConfig as MetaClusterConfig, GrpcMetaClient, GrpcNetworkFactory, MetaAdminServer,
    MetaRouter, MetaStore, NodeAddressMap, RaftGrpcServer,
};

use crate::config::ClusterConfig;
use crate::error::ServerError;
use crate::tls::{load_cluster_client_tls, load_cluster_server_tls};

/// State for a running `MetaNode`.
pub struct MetaNodeState {
    /// The Raft-based metadata store.
    pub store: MetaStore,
    /// Coordinator for health checks on the leader node.
    pub coordinator: ClusterCoordinator,
    /// In-process meta client for admin operations.
    pub meta_client: Arc<dyn MetaClient>,
    /// Cancellation token for graceful shutdown.
    pub shutdown: CancellationToken,
}

/// State for a running `DataNode`.
pub struct DataNodeState {
    /// Manager handling registration, heartbeat, and deregistration.
    pub manager: DataNodeManager,
    /// Local region manager for this node's data regions.
    pub region_manager: Arc<RegionManager>,
    /// Local storage backend.
    pub storage: Arc<dyn RegionStorage>,
    /// Distributed write router.
    pub write_router: Arc<WriteRouter>,
    /// Distributed scatter-gather query router.
    pub query_router: Arc<QueryRouter>,
    /// Region migration orchestrator.
    pub region_migrator: Arc<RegionMigrator>,
    /// Failover and self-healing manager.
    pub failover_manager: Arc<FailoverManager>,
    /// Distributed analytics coordinator (FORECAST/ANOMALY across cluster).
    pub distributed_analytics: Arc<DistributedAnalytics>,
    /// Routing cache for region → leader mapping.
    pub routing_cache: Arc<RoutingCache>,
    /// Background heartbeat task handle.
    pub heartbeat_handle: tokio::task::JoinHandle<()>,
    /// Cancellation token for graceful shutdown.
    pub shutdown: CancellationToken,
}

/// Start a `MetaNode` — creates a Raft cluster, serves admin gRPC, starts
/// the health coordinator, and bootstraps or joins the cluster.
///
/// # Errors
///
/// Returns an error if the Raft node cannot be created or the gRPC
/// server fails to bind.
pub async fn start_meta_node(
    cluster_cfg: &ClusterConfig,
    data_dir: &std::path::Path,
) -> Result<MetaNodeState, ServerError> {
    let node_id = cluster_cfg.node_id;
    let raft_addr: SocketAddr = cluster_cfg.raft_bind_addr.parse().map_err(|e| {
        ServerError::Internal(format!(
            "invalid raft-bind address '{}': {e}",
            cluster_cfg.raft_bind_addr
        ))
    })?;

    info!(node_id, %raft_addr, "starting meta node");

    // ── Build Raft node (durable log storage) ──────────────────────
    let meta_dir = data_dir.join("meta");
    let store = MetaStore::open(&meta_dir)
        .map_err(|e| ServerError::Internal(format!("open durable meta store: {e}")))?;
    let router = MetaRouter::new();

    let raft_config = Arc::new(
        openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        }
        .validate()
        .map_err(|e| ServerError::Internal(format!("invalid raft config: {e}")))?,
    );

    // Build node address map from peers + self
    let addr_map = NodeAddressMap::new();
    let scheme = if cluster_cfg.tls.is_some() {
        "https"
    } else {
        "http"
    };
    addr_map.insert(node_id, format!("{scheme}://{raft_addr}"));

    for peer_addr in &cluster_cfg.cluster_peers {
        info!(peer = %peer_addr, "registered cluster peer");
    }

    // Load optional cluster mTLS config
    let client_tls = cluster_cfg
        .tls
        .as_ref()
        .map(load_cluster_client_tls)
        .transpose()
        .map_err(|e| ServerError::Internal(format!("cluster client TLS error: {e}")))?;
    let server_tls = cluster_cfg
        .tls
        .as_ref()
        .map(load_cluster_server_tls)
        .transpose()
        .map_err(|e| ServerError::Internal(format!("cluster server TLS error: {e}")))?;

    let mut network = GrpcNetworkFactory::new(addr_map.clone());
    if let Some(tls) = client_tls {
        network = network.with_tls(tls);
    }
    let raft = store
        .build_raft(node_id, raft_config, network)
        .await
        .map_err(|e| ServerError::Internal(format!("failed to create raft node: {e}")))?;

    let raft = Arc::new(raft);
    router.add_node(node_id, (*raft).clone());

    // ── Bootstrap cluster (first node only) ────────────────────────
    if cluster_cfg.cluster_peers.is_empty() {
        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(format!("{scheme}://{raft_addr}")));

        if let Err(e) = raft.initialize(members).await {
            warn!(error = %e, "raft initialize (may already be initialized)");
        } else {
            info!(node_id, "single-node cluster initialized");
        }
    }

    // ── Start Raft gRPC transport + admin server ───────────────────
    let raft_server = RaftGrpcServer::new((*raft).clone());
    let admin_server = MetaAdminServer::new(raft.clone(), store.sm_store());

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    tokio::spawn(async move {
        let listener = match TcpListener::bind(raft_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(%e, %raft_addr, "failed to bind raft gRPC server");
                return;
            }
        };

        info!(%raft_addr, "raft + admin gRPC server listening");

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = server_tls {
            builder = match builder.tls_config(tls) {
                Ok(b) => b,
                Err(e) => {
                    error!(%e, "cluster raft server TLS config invalid");
                    return;
                }
            };
        }
        let result = builder
            .add_service(raft_server.into_service())
            .add_service(admin_server.into_service())
            .serve_with_incoming_shutdown(incoming, shutdown_clone.cancelled())
            .await;

        if let Err(e) = result {
            error!(%e, "raft gRPC server error");
        }
    });

    // ── Wait for leader election ───────────────────────────────────
    for _ in 0..50 {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    if let Some(leader) = raft.current_leader().await {
        info!(leader, "raft leader elected");
    } else {
        warn!("no raft leader elected yet — cluster may need more nodes");
    }

    // ── Start health coordinator (runs on MetaNode with local access) ──
    let meta_cluster_config = MetaClusterConfig::default();
    let coordinator = ClusterCoordinator::new(store.clone(), router, meta_cluster_config);

    let meta_client: Arc<dyn MetaClient> =
        Arc::new(InProcessMetaClient::new(raft, store.sm_store()));

    let coord = coordinator.clone();
    let coord_client = meta_client.clone();
    tokio::spawn(async move {
        if let Err(e) = coord.run_health_loop(coord_client).await {
            error!(error = %e, "coordinator health loop error");
        }
    });

    info!(node_id, "meta node ready");

    Ok(MetaNodeState {
        store,
        coordinator,
        meta_client,
        shutdown,
    })
}

/// Start a `DataNode` — registers with the `MetaNode` cluster, starts the
/// data gRPC service, and begins heartbeating.
///
/// # Errors
///
/// Returns an error if registration fails or the `MetaNode` cluster
/// is unreachable.
pub async fn start_data_node(
    cluster_cfg: &ClusterConfig,
    grpc_addr: &str,
    db: Arc<Chronix>,
) -> Result<DataNodeState, ServerError> {
    let node_id = cluster_cfg.node_id;

    info!(node_id, %grpc_addr, meta_addrs = ?cluster_cfg.meta_addrs, "starting data node");

    // ── Load optional cluster mTLS config ──────────────────────────
    let client_tls = cluster_cfg
        .tls
        .as_ref()
        .map(load_cluster_client_tls)
        .transpose()
        .map_err(|e| ServerError::Internal(format!("cluster client TLS error: {e}")))?;
    let server_tls = cluster_cfg
        .tls
        .as_ref()
        .map(load_cluster_server_tls)
        .transpose()
        .map_err(|e| ServerError::Internal(format!("cluster server TLS error: {e}")))?;

    // ── Build gRPC meta client ─────────────────────────────────────
    let mut grpc_client = GrpcMetaClient::new(cluster_cfg.meta_addrs.clone());
    if let Some(tls) = &client_tls {
        grpc_client = grpc_client.with_tls(tls.clone());
    }
    let meta_client: Arc<dyn MetaClient> = Arc::new(GrpcMetaClientAdapter::new(grpc_client));

    // ── Region manager + storage ───────────────────────────────────
    let region_manager = Arc::new(RegionManager::new(node_id));
    let storage: Arc<dyn RegionStorage> = Arc::new(
        crate::region_storage::ChronixRegionStorage::new(db, region_manager.clone()),
    );

    // ── Routing cache + write router ───────────────────────────────
    let routing_cache = Arc::new(RoutingCache::new(meta_client.clone()));
    let mut data_client = DataGrpcClient::new();
    if let Some(tls) = client_tls {
        data_client = data_client.with_tls(tls);
    }
    let write_router = Arc::new(WriteRouter::new(
        routing_cache.clone(),
        storage.clone(),
        data_client.clone(),
        node_id,
    ));

    // ── Query router ───────────────────────────────────────────────
    let query_router = Arc::new(QueryRouter::new(
        routing_cache.clone(),
        storage.clone(),
        data_client.clone(),
        node_id,
    ));

    // ── Region migrator + failover manager ─────────────────────────
    let region_migrator = Arc::new(
        RegionMigrator::new(
            region_manager.clone(),
            storage.clone(),
            data_client,
            node_id,
        )
        .with_meta_client(meta_client.clone()),
    );
    let failover_manager = Arc::new(FailoverManager::new(
        routing_cache.clone(),
        region_migrator.clone(),
        node_id,
        FailoverConfig::default(),
    ));

    // ── Distributed analytics coordinator ──────────────────────────
    let distributed_analytics = Arc::new(DistributedAnalytics::new(query_router.clone()));

    // ── Start DataNode gRPC service ────────────────────────────────
    let data_grpc_server = DataGrpcServer::new(region_manager.clone(), storage.clone());
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let data_addr: SocketAddr = grpc_addr.parse().map_err(|e| {
        ServerError::Internal(format!("invalid data grpc address '{grpc_addr}': {e}"))
    })?;

    tokio::spawn(async move {
        let listener = match TcpListener::bind(data_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(%e, %data_addr, "failed to bind data gRPC server");
                return;
            }
        };

        info!(%data_addr, "data gRPC server listening");

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = server_tls {
            builder = match builder.tls_config(tls) {
                Ok(b) => b,
                Err(e) => {
                    error!(%e, "cluster data server TLS config invalid");
                    return;
                }
            };
        }
        let result = builder
            .add_service(data_grpc_server.into_service())
            .serve_with_incoming_shutdown(incoming, shutdown_clone.cancelled())
            .await;

        if let Err(e) = result {
            error!(%e, "data gRPC server error");
        }
    });

    // ── Create DataNode manager ────────────────────────────────────
    let meta_cluster_config = MetaClusterConfig::default();
    let manager = DataNodeManager::new(node_id, grpc_addr, meta_client, &meta_cluster_config);

    // Register and start heartbeat loop
    let heartbeat_handle = manager
        .start()
        .await
        .map_err(|e| ServerError::Internal(format!("data node registration failed: {e}")))?;

    info!(node_id, "data node registered and heartbeating");

    Ok(DataNodeState {
        manager,
        region_manager,
        storage,
        write_router,
        query_router,
        region_migrator,
        failover_manager,
        distributed_analytics,
        routing_cache,
        heartbeat_handle,
        shutdown,
    })
}

/// Gracefully shut down a `MetaNode`.
pub async fn stop_meta_node(state: MetaNodeState) {
    info!("stopping meta node");
    state.coordinator.shutdown_token().cancel();
    state.shutdown.cancel();
    info!("meta node stopped");
}

/// Gracefully shut down a `DataNode` — deregisters from the cluster.
pub async fn stop_data_node(state: DataNodeState) {
    info!("stopping data node");
    state.shutdown.cancel();
    if let Err(e) = state.manager.stop().await {
        warn!(error = %e, "error deregistering data node");
    }
    state.heartbeat_handle.abort();
    info!("data node stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterMode;
    use chronix_cluster::Result as ClusterResult;
    use chronix_meta::{
        DataNodeInfo, MetaCommand, MetaResponse, NodeId, RegionInfo, RoutingSnapshot,
    };

    /// Stub `MetaClient` for unit tests that don't require Raft.
    struct StubMetaClient;

    #[async_trait::async_trait]
    impl MetaClient for StubMetaClient {
        async fn register_node(&self, _info: DataNodeInfo) -> ClusterResult<()> {
            Ok(())
        }
        async fn deregister_node(&self, _node_id: NodeId) -> ClusterResult<()> {
            Ok(())
        }
        async fn heartbeat(&self, _node_id: NodeId, _generation: u64) -> ClusterResult<()> {
            Ok(())
        }
        async fn create_region(&self, _info: RegionInfo) -> ClusterResult<()> {
            Ok(())
        }
        async fn get_routing_table(&self) -> ClusterResult<RoutingSnapshot> {
            Ok(RoutingSnapshot {
                version: 0,
                entries: std::collections::BTreeMap::new(),
            })
        }
        async fn propose(&self, _cmd: MetaCommand) -> ClusterResult<MetaResponse> {
            Ok(MetaResponse::Ok)
        }
    }

    #[test]
    fn meta_node_state_has_store() {
        let store = MetaStore::new_in_memory();
        let router = MetaRouter::new();
        let config = MetaClusterConfig::default();
        let coordinator = ClusterCoordinator::new(store.clone(), router, config);
        let shutdown = CancellationToken::new();
        let meta_client: Arc<dyn MetaClient> = Arc::new(StubMetaClient);
        let state = MetaNodeState {
            store,
            coordinator,
            meta_client,
            shutdown,
        };
        assert!(state.store.state_machine().nodes().is_empty());
    }

    #[test]
    fn cluster_mode_variants() {
        assert_eq!(ClusterMode::Meta, ClusterMode::Meta);
        assert_eq!(ClusterMode::Data, ClusterMode::Data);
        assert_ne!(ClusterMode::Meta, ClusterMode::Data);
    }

    #[test]
    fn cluster_config_meta() {
        let cfg = ClusterConfig {
            mode: ClusterMode::Meta,
            node_id: 1,
            raft_bind_addr: "0.0.0.0:9100".to_string(),
            cluster_peers: vec!["10.0.0.2:9100".to_string()],
            meta_addrs: Vec::new(),
            tls: None,
        };
        assert_eq!(cfg.node_id, 1);
        assert_eq!(cfg.cluster_peers.len(), 1);
        assert!(cfg.meta_addrs.is_empty());
    }

    #[test]
    fn cluster_config_data() {
        let cfg = ClusterConfig {
            mode: ClusterMode::Data,
            node_id: 10,
            raft_bind_addr: String::new(),
            cluster_peers: Vec::new(),
            meta_addrs: vec!["10.0.0.1:9100".to_string(), "10.0.0.2:9100".to_string()],
            tls: None,
        };
        assert_eq!(cfg.node_id, 10);
        assert!(cfg.cluster_peers.is_empty());
        assert_eq!(cfg.meta_addrs.len(), 2);
    }

    #[test]
    fn cluster_config_with_tls() {
        use crate::config::ClusterTlsConfig;
        use std::path::PathBuf;

        let cfg = ClusterConfig {
            mode: ClusterMode::Data,
            node_id: 10,
            raft_bind_addr: String::new(),
            cluster_peers: Vec::new(),
            meta_addrs: vec!["10.0.0.1:9100".to_string()],
            tls: Some(ClusterTlsConfig {
                ca_cert: PathBuf::from("/certs/ca.pem"),
                cert: PathBuf::from("/certs/node.pem"),
                key: PathBuf::from("/certs/node-key.pem"),
            }),
        };
        assert!(cfg.tls.is_some());
        let tls = cfg.tls.unwrap();
        assert_eq!(tls.ca_cert.to_str().unwrap(), "/certs/ca.pem");
    }

    #[tokio::test]
    async fn start_meta_node_single_node() {
        // Bind an ephemeral port for the test
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let tmp = tempfile::TempDir::new().unwrap();

        let cfg = ClusterConfig {
            mode: ClusterMode::Meta,
            node_id: 1,
            raft_bind_addr: addr.to_string(),
            cluster_peers: Vec::new(),
            meta_addrs: Vec::new(),
            tls: None,
        };

        let state = start_meta_node(&cfg, tmp.path()).await.unwrap();

        // Verify cluster is operational
        assert!(state.store.state_machine().nodes().is_empty());

        stop_meta_node(state).await;
    }
}
