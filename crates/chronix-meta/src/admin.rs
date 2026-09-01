//! gRPC-based MetaNode admin API — server and client.
//!
//! The [`MetaAdminServer`] runs on every MetaNode and exposes cluster
//! management operations (node registration, heartbeats, region creation,
//! routing table queries, Raft membership changes).
//!
//! The [`GrpcMetaClient`] provides a remote [`MetaClient`](crate client
//! trait) implementation that DataNodes and QueryNodes use to talk to the
//! MetaNode cluster over the network.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use tokio::sync::Mutex as TokioMutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::store::{MetaRaft, MetaSmStore};
use crate::types::{
    DataNodeInfo, MetaCommand, MetaResponse, NodeId, NodeMode, RegionInfo, RegionState,
};

/// Generated proto types for the admin service.
#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod proto {
    tonic::include_proto!("chronix.admin.v1");
}

use proto::meta_admin_service_client::MetaAdminServiceClient;
use proto::meta_admin_service_server::{MetaAdminService, MetaAdminServiceServer};

// ──────────────────────────────────────────────────────────────────────
// Server
// ──────────────────────────────────────────────────────────────────────

/// gRPC handler implementing the `MetaAdminService`.
///
/// Wraps a local `MetaRaft` handle and `MetaSmStore` to process admin
/// requests. Write operations are proposed through Raft consensus; read
/// operations (routing table, cluster info) are served from local state.
pub struct MetaAdminServer {
    raft: Arc<MetaRaft>,
    sm_store: Arc<MetaSmStore>,
    /// Serializes membership changes to prevent concurrent add/remove racing.
    membership_guard: TokioMutex<()>,
}

impl MetaAdminServer {
    /// Create a new admin server backed by the given Raft instance.
    #[must_use]
    pub fn new(raft: Arc<MetaRaft>, sm_store: Arc<MetaSmStore>) -> Self {
        Self {
            raft,
            sm_store,
            membership_guard: TokioMutex::new(()),
        }
    }

    /// Wrap this handler in a tonic service for composing with a
    /// [`tonic::transport::Server`].
    #[must_use]
    pub fn into_service(self) -> MetaAdminServiceServer<Self> {
        MetaAdminServiceServer::new(self)
    }

    /// Propose a command through Raft and return the response.
    async fn write_cmd(&self, cmd: MetaCommand) -> Result<MetaResponse, Status> {
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("raft write failed: {e}")))?;
        Ok(resp.data)
    }

    /// Convert a `MetaResponse` into a tonic result, treating `Error` as
    /// `Status::internal`.
    #[allow(clippy::result_large_err)]
    fn check(resp: MetaResponse) -> Result<(), Status> {
        match resp {
            MetaResponse::Ok | MetaResponse::Created { .. } => Ok(()),
            MetaResponse::Error { message } => Err(Status::internal(message)),
        }
    }
}

#[tonic::async_trait]
impl MetaAdminService for MetaAdminServer {
    async fn register_node(
        &self,
        request: Request<proto::NodeInfo>,
    ) -> Result<Response<proto::Ack>, Status> {
        let req = request.into_inner();
        let mode = match req.mode.as_str() {
            "data" => NodeMode::Data,
            "query" => NodeMode::Query,
            "meta" => NodeMode::Meta,
            other => return Err(Status::invalid_argument(format!("unknown mode: {other}"))),
        };

        let info = DataNodeInfo::new(req.node_id, req.grpc_addr)
            .with_mode(mode)
            .with_capacity(
                req.disk_capacity_bytes,
                req.memory_bytes,
                u64::from(req.cpu_cores),
            );

        info!(node_id = req.node_id, "admin: register node");
        let resp = self.write_cmd(MetaCommand::RegisterNode(info)).await?;
        Self::check(resp)?;
        Ok(Response::new(proto::Ack {}))
    }

    async fn deregister_node(
        &self,
        request: Request<proto::RemoveNodeRequest>,
    ) -> Result<Response<proto::Ack>, Status> {
        let node_id = request.into_inner().node_id;
        info!(node_id, "admin: deregister node");
        let resp = self
            .write_cmd(MetaCommand::DeregisterNode { node_id })
            .await?;
        Self::check(resp)?;
        Ok(Response::new(proto::Ack {}))
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::Ack>, Status> {
        let req = request.into_inner();
        debug!(
            node_id = req.node_id,
            gen = req.generation,
            "admin: heartbeat (out-of-band)"
        );

        // Update heartbeat state directly without a Raft proposal.
        // This avoids generating Raft log entries for pure liveness checks.
        let known = self
            .sm_store
            .state_machine()
            .record_heartbeat(req.node_id, req.generation);

        if known {
            Ok(Response::new(proto::Ack {}))
        } else {
            Err(Status::not_found(format!(
                "node {} not registered",
                req.node_id
            )))
        }
    }

    async fn create_region(
        &self,
        request: Request<proto::RegionSpec>,
    ) -> Result<Response<proto::Ack>, Status> {
        let req = request.into_inner();
        let info = RegionInfo {
            region_id: req.region_id,
            measurement: req.measurement,
            leader_node_id: req.leader_node_id,
            replica_node_ids: req.replica_node_ids,
            state: RegionState::Active,
            replication_factor: req.replication_factor,
            key_range: None,
        };
        info!(region_id = req.region_id, "admin: create region");
        let resp = self.write_cmd(MetaCommand::CreateRegion(info)).await?;
        Self::check(resp)?;
        Ok(Response::new(proto::Ack {}))
    }

    async fn get_routing_table(
        &self,
        _request: Request<proto::Ack>,
    ) -> Result<Response<proto::RoutingTableResponse>, Status> {
        let sm = self.sm_store.state_machine();
        let snap = sm.routing_table().snapshot();
        let groups = snap
            .entries
            .into_iter()
            .map(|(measurement, entries)| proto::RouteEntryGroup {
                measurement: measurement.clone(),
                entries: entries
                    .into_iter()
                    .map(|e| proto::RouteEntryProto {
                        region_id: e.region_id,
                        measurement: e.measurement,
                        leader_node_id: e.leader_node_id,
                        leader_addr: e.leader_addr,
                        replica_addrs: e
                            .replica_addrs
                            .into_iter()
                            .map(|(nid, addr)| proto::ReplicaAddr { node_id: nid, addr })
                            .collect(),
                    })
                    .collect(),
            })
            .collect();

        Ok(Response::new(proto::RoutingTableResponse {
            version: snap.version,
            groups,
        }))
    }

    async fn propose(
        &self,
        request: Request<proto::ProposeRequest>,
    ) -> Result<Response<proto::ProposeResponse>, Status> {
        let data = request.into_inner().command;
        let cmd: MetaCommand = serde_json::from_slice(&data)
            .map_err(|e| Status::invalid_argument(format!("invalid command JSON: {e}")))?;
        debug!(?cmd, "admin: propose");
        let resp = self.write_cmd(cmd).await?;
        let resp_bytes = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("response serialization failed: {e}")))?;
        Ok(Response::new(proto::ProposeResponse {
            response: resp_bytes,
        }))
    }

    async fn add_raft_node(
        &self,
        request: Request<proto::AddNodeRequest>,
    ) -> Result<Response<proto::Ack>, Status> {
        let _guard = self.membership_guard.lock().await;
        let req = request.into_inner();
        let node = openraft::BasicNode {
            addr: req.grpc_addr.clone(),
        };

        if req.as_learner {
            info!(node_id = req.node_id, addr = %req.grpc_addr, "admin: add learner");
            self.raft
                .add_learner(req.node_id, node, true)
                .await
                .map_err(|e| Status::internal(format!("add learner failed: {e}")))?;
        } else {
            info!(node_id = req.node_id, addr = %req.grpc_addr, "admin: add voter");
            // Add as learner first, then change membership.
            self.raft
                .add_learner(req.node_id, node, true)
                .await
                .map_err(|e| Status::internal(format!("add learner failed: {e}")))?;

            // Collect current voters + new node.
            let metrics = self.raft.metrics().borrow().clone();
            let mut voters: Vec<NodeId> =
                metrics.membership_config.membership().voter_ids().collect();
            if !voters.contains(&req.node_id) {
                voters.push(req.node_id);
            }
            let voter_set: BTreeSet<NodeId> = voters.into_iter().collect();

            // Use joint consensus (two-phase: old∪new → new)
            // to prevent split-brain during membership transitions.
            self.raft
                .change_membership(voter_set, true)
                .await
                .map_err(|e| Status::internal(format!("change membership failed: {e}")))?;
        }

        Ok(Response::new(proto::Ack {}))
    }

    async fn remove_raft_node(
        &self,
        request: Request<proto::RemoveNodeRequest>,
    ) -> Result<Response<proto::Ack>, Status> {
        let _guard = self.membership_guard.lock().await;
        let node_id = request.into_inner().node_id;
        info!(node_id, "admin: remove raft node");

        // Remove from voter set.
        let metrics = self.raft.metrics().borrow().clone();
        let mut voters: BTreeSet<NodeId> =
            metrics.membership_config.membership().voter_ids().collect();
        let old_voter_count = voters.len();
        voters.remove(&node_id);

        if voters.is_empty() {
            return Err(Status::failed_precondition(
                "cannot remove last voter from cluster",
            ));
        }

        // Quorum guard — ensure the new voter count still
        // constitutes a majority of the old voter count. Removing a node
        // from a 3-node cluster (3→2) means any further failure is fatal
        // (no quorum possible with 1 of 2). The safe threshold is:
        //   new_voters > old_voters / 2
        // This prevents going from N to N/2 where one more fault = no quorum.
        if voters.len() <= old_voter_count / 2 {
            return Err(Status::failed_precondition(format!(
                "removing node {node_id} would reduce voters from {old_voter_count} to {} — \
                 below quorum safety threshold (need > {} voters). \
                 Add a replacement node first, or use joint consensus.",
                voters.len(),
                old_voter_count / 2,
            )));
        }

        // Use joint consensus (two-phase: old∪new → new)
        // to prevent split-brain during membership transitions.
        self.raft
            .change_membership(voters, true)
            .await
            .map_err(|e| Status::internal(format!("change membership failed: {e}")))?;

        Ok(Response::new(proto::Ack {}))
    }

    async fn list_nodes(
        &self,
        _request: Request<proto::Ack>,
    ) -> Result<Response<proto::NodeListResponse>, Status> {
        let sm = self.sm_store.state_machine();
        let nodes = sm.nodes();
        let leader_id = self.raft.current_leader().await.unwrap_or(0);

        let entries: Vec<proto::NodeInfo> = nodes
            .values()
            .map(|n| proto::NodeInfo {
                node_id: n.node_id,
                grpc_addr: n.grpc_addr.clone(),
                mode: format!("{:?}", n.mode).to_lowercase(),
                disk_capacity_bytes: n.disk_bytes,
                memory_bytes: n.memory_bytes,
                cpu_cores: n.cpu_cores,
            })
            .collect();

        Ok(Response::new(proto::NodeListResponse {
            nodes: entries,
            leader_id,
        }))
    }

    async fn get_cluster_info(
        &self,
        _request: Request<proto::ClusterInfoRequest>,
    ) -> Result<Response<proto::ClusterInfoResponse>, Status> {
        let metrics = self.raft.metrics().borrow().clone();
        let sm = self.sm_store.state_machine();
        let config = sm.cluster_config();

        let leader_id = metrics.current_leader.unwrap_or(0);
        let voter_ids: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
        let learner_ids: Vec<u64> = metrics
            .membership_config
            .membership()
            .learner_ids()
            .collect();
        let committed = metrics.last_applied.map_or(0, |li| li.index);

        Ok(Response::new(proto::ClusterInfoResponse {
            leader_id,
            voter_ids,
            learner_ids,
            config: Some(proto::ClusterConfigProto {
                cluster_name: config.cluster_name,
                default_replication_factor: config.default_replication_factor,
                default_region_count: config.default_region_count,
                heartbeat_interval_secs: u32::try_from(config.heartbeat_interval_secs)
                    .unwrap_or(u32::MAX),
                suspect_threshold: config.heartbeat_suspect_threshold,
                dead_threshold: config.heartbeat_dead_threshold,
                max_concurrent_rereplications: config.max_concurrent_rereplications,
            }),
            committed_log_index: committed,
        }))
    }
}

// ──────────────────────────────────────────────────────────────────────
// Client
// ──────────────────────────────────────────────────────────────────────

/// Remote gRPC client implementing the admin interface.
///
/// Connects to `MetaNode` leader and provides methods matching the
/// [`MetaClient`] trait operations. Includes automatic leader address
/// failover — if the current endpoint returns `UNAVAILABLE`, the client
/// tries the next known `MetaNode` address.
///
/// When `tls_config` is set, all connections to `MetaNode` peers use
/// mutual TLS for encrypted and authenticated communication.
///
/// ## Connection Pooling
///
/// tonic [`Channel`] objects use HTTP/2 multiplexing, so a single channel
/// can serve many concurrent RPCs without head-of-line blocking. This
/// client caches channels per endpoint address in `channel_cache` to
/// avoid the overhead of re-establishing TCP + TLS connections on every
/// RPC call. The cache is invalidated when `update_addrs()` is called.
pub struct GrpcMetaClient {
    /// Known `MetaNode` addresses for failover.
    addrs: Arc<RwLock<Vec<String>>>,
    /// Index of the currently preferred address.
    current: Arc<std::sync::atomic::AtomicUsize>,
    /// Round-robin counter for distributing stale reads across replicas.
    read_counter: Arc<std::sync::atomic::AtomicUsize>,
    /// Connection timeout.
    timeout: Duration,
    /// Optional mTLS configuration.
    tls_config: Option<tonic::transport::ClientTlsConfig>,
    /// When true, read-only RPCs (`list_nodes`, `get_routing_table`,
    /// `get_cluster_info`) are distributed across all known MetaNode
    /// replicas instead of routing exclusively to the leader.
    /// The data may lag behind the leader by a few Raft entries.
    allow_stale_reads: bool,
    /// Cached tonic channels keyed by endpoint address.
    ///
    /// tonic `Channel` already provides HTTP/2 multiplexing and
    /// transparent reconnection, so caching them avoids redundant TCP +
    /// TLS handshakes on every RPC call.
    ///
    /// Each entry stores the creation `Instant` for TTL-based
    /// eviction (default 5 min). Stale entries from IP rotation are
    /// automatically refreshed.
    channel_cache: Mutex<HashMap<String, (Channel, std::time::Instant)>>,
    /// Maximum age for cached channels before re-establishment.
    channel_ttl: Duration,
}

impl GrpcMetaClient {
    /// Create a client with the given `MetaNode` addresses.
    ///
    /// The first address is tried initially; on failure the client
    /// rotates through the list.
    #[must_use]
    pub fn new(addrs: Vec<String>) -> Self {
        Self {
            addrs: Arc::new(RwLock::new(addrs)),
            current: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            read_counter: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            timeout: Duration::from_secs(5),
            tls_config: None,
            allow_stale_reads: false,
            channel_cache: Mutex::new(HashMap::new()),
            channel_ttl: Duration::from_secs(300),
        }
    }

    /// Enable mTLS for all connections to `MetaNode` peers.
    #[must_use]
    pub fn with_tls(mut self, tls: tonic::transport::ClientTlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    /// Set the connection timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Allow read-only RPCs to be served by any replica (follower reads).
    ///
    /// When enabled, `list_nodes`, `get_routing_table`, and
    /// `get_cluster_info` are distributed round-robin across all known
    /// MetaNode addresses instead of always going to the leader.
    /// Results may lag the leader by a small number of Raft entries.
    #[must_use]
    pub fn with_stale_reads(mut self, allow: bool) -> Self {
        self.allow_stale_reads = allow;
        self
    }

    /// Update the known `MetaNode` addresses (e.g. after discovering new nodes).
    ///
    /// Clears the connection cache so stale channels to removed
    /// nodes are not reused.
    pub fn update_addrs(&self, addrs: Vec<String>) {
        *self.addrs.write() = addrs;
        self.current.store(0, std::sync::atomic::Ordering::Release);
        // Invalidate cached channels since the address set changed.
        self.channel_cache.lock().clear();
    }

    /// Attempt to connect to the current preferred `MetaNode`.
    ///
    /// Reuses cached tonic [`Channel`] connections when available.
    /// tonic channels provide HTTP/2 multiplexing, so a single cached
    /// channel efficiently serves many concurrent RPCs.
    async fn connect(&self) -> Result<MetaAdminServiceClient<Channel>, Status> {
        let addrs = self.addrs.read().clone();
        if addrs.is_empty() {
            return Err(Status::unavailable("no meta node addresses configured"));
        }

        let start = self.current.load(std::sync::atomic::Ordering::Acquire);
        let len = addrs.len();

        // Try each address starting from current, wrapping around.
        for i in 0..len {
            let idx = (start + i) % len;
            let addr = &addrs[idx];
            let scheme = if self.tls_config.is_some() {
                "https"
            } else {
                "http"
            };
            let endpoint = format!("{scheme}://{addr}");

            // Check channel cache first.
            // Evict stale channels older than channel_ttl.
            {
                let mut cache = self.channel_cache.lock();
                if let Some((channel, created_at)) = cache.get(&endpoint) {
                    if created_at.elapsed() < self.channel_ttl {
                        self.current
                            .store(idx, std::sync::atomic::Ordering::Release);
                        return Ok(MetaAdminServiceClient::new(channel.clone()));
                    }
                    // TTL expired — remove and reconnect below.
                    cache.remove(&endpoint);
                }
            }

            let channel_builder = Channel::from_shared(endpoint.clone())
                .map_err(|e| Status::internal(format!("invalid endpoint: {e}")))?;

            // Apply TLS config if present
            let channel_builder = if let Some(tls) = &self.tls_config {
                channel_builder
                    .tls_config(tls.clone())
                    .map_err(|e| Status::internal(format!("TLS config error: {e}")))?
            } else {
                channel_builder
            };

            match tokio::time::timeout(self.timeout, channel_builder.connect()).await {
                Ok(Ok(channel)) => {
                    // Update preferred index on success.
                    self.current
                        .store(idx, std::sync::atomic::Ordering::Release);
                    // Cache the channel for future reuse.
                    self.channel_cache
                        .lock()
                        .insert(endpoint, (channel.clone(), std::time::Instant::now()));
                    return Ok(MetaAdminServiceClient::new(channel));
                }
                Ok(Err(e)) => {
                    warn!(addr, error = %e, "meta: connect failed, trying next");
                    // Remove stale cache entry if present.
                    self.channel_cache.lock().remove(&endpoint);
                }
                Err(_) => {
                    warn!(addr, "meta: connect timeout, trying next");
                    self.channel_cache.lock().remove(&endpoint);
                }
            }
        }

        Err(Status::unavailable("all meta node addresses unreachable"))
    }

    /// Connect for a read-only operation.
    ///
    /// When `allow_stale_reads` is enabled, uses round-robin distribution
    /// across all known MetaNode replicas so reads don't all hit the leader.
    /// Otherwise falls back to `connect()` (leader-preferred).
    ///
    /// Reuses cached channels when available.
    async fn connect_for_read(&self) -> Result<MetaAdminServiceClient<Channel>, Status> {
        if !self.allow_stale_reads {
            return self.connect().await;
        }

        let addrs = self.addrs.read().clone();
        if addrs.is_empty() {
            return Err(Status::unavailable("no meta node addresses configured"));
        }

        // Round-robin starting from read_counter
        let start = self
            .read_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % addrs.len();

        let len = addrs.len();
        for i in 0..len {
            let idx = (start + i) % len;
            let addr = &addrs[idx];
            let scheme = if self.tls_config.is_some() {
                "https"
            } else {
                "http"
            };
            let endpoint = format!("{scheme}://{addr}");

            // Check channel cache first.
            // Evict stale channels older than channel_ttl.
            {
                let mut cache = self.channel_cache.lock();
                if let Some((channel, created_at)) = cache.get(&endpoint) {
                    if created_at.elapsed() < self.channel_ttl {
                        return Ok(MetaAdminServiceClient::new(channel.clone()));
                    }
                    cache.remove(&endpoint);
                }
            }

            let channel_builder = Channel::from_shared(endpoint.clone())
                .map_err(|e| Status::internal(format!("invalid endpoint: {e}")))?;

            let channel_builder = if let Some(tls) = &self.tls_config {
                channel_builder
                    .tls_config(tls.clone())
                    .map_err(|e| Status::internal(format!("TLS config error: {e}")))?
            } else {
                channel_builder
            };

            match tokio::time::timeout(self.timeout, channel_builder.connect()).await {
                Ok(Ok(channel)) => {
                    // Cache the channel for future reuse.
                    self.channel_cache
                        .lock()
                        .insert(endpoint, (channel.clone(), std::time::Instant::now()));
                    return Ok(MetaAdminServiceClient::new(channel));
                }
                Ok(Err(e)) => {
                    warn!(addr, error = %e, "meta: read connect failed, trying next");
                    self.channel_cache.lock().remove(&endpoint);
                }
                Err(_) => {
                    warn!(addr, "meta: read connect timeout, trying next");
                    self.channel_cache.lock().remove(&endpoint);
                }
            }
        }

        Err(Status::unavailable("all meta node addresses unreachable"))
    }

    /// Register a data node.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft cluster rejects the registration or
    /// all `MetaNode` addresses are unreachable.
    pub async fn register_node(&self, info: DataNodeInfo) -> Result<(), Status> {
        let mut client = self.connect().await?;
        client
            .register_node(proto::NodeInfo {
                node_id: info.node_id,
                grpc_addr: info.grpc_addr,
                mode: format!("{:?}", info.mode).to_lowercase(),
                disk_capacity_bytes: info.disk_bytes,
                memory_bytes: info.memory_bytes,
                cpu_cores: info.cpu_cores,
            })
            .await?;
        Ok(())
    }

    /// Deregister a node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is unknown or unreachable.
    pub async fn deregister_node(&self, node_id: NodeId) -> Result<(), Status> {
        let mut client = self.connect().await?;
        client
            .deregister_node(proto::RemoveNodeRequest { node_id })
            .await?;
        Ok(())
    }

    /// Send a heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error if the `MetaNode` cluster is unreachable.
    pub async fn heartbeat(&self, node_id: NodeId, generation: u64) -> Result<(), Status> {
        let mut client = self.connect().await?;
        // timestamp_secs is set to 0; the receiver stamps with its own
        // clock inside record_heartbeat() to avoid clock-skew issues.
        client
            .heartbeat(proto::HeartbeatRequest {
                node_id,
                generation,
                timestamp_secs: 0,
            })
            .await?;
        Ok(())
    }

    /// Create a new region.
    ///
    /// # Errors
    ///
    /// Returns an error if the region already exists or cluster is unreachable.
    pub async fn create_region(&self, info: RegionInfo) -> Result<(), Status> {
        let mut client = self.connect().await?;
        client
            .create_region(proto::RegionSpec {
                region_id: info.region_id,
                measurement: info.measurement,
                leader_node_id: info.leader_node_id,
                replica_node_ids: info.replica_node_ids,
                replication_factor: info.replication_factor,
            })
            .await?;
        Ok(())
    }

    /// Get the current routing table.
    ///
    /// # Errors
    ///
    /// Returns an error if the `MetaNode` cluster is unreachable.
    pub async fn get_routing_table(&self) -> Result<crate::routing::RoutingSnapshot, Status> {
        let mut client = self.connect_for_read().await?;
        let resp = client.get_routing_table(proto::Ack {}).await?.into_inner();

        let entries: BTreeMap<String, Vec<crate::routing::RouteEntry>> = resp
            .groups
            .into_iter()
            .map(|g| {
                let measurement = g.measurement;
                let routes = g
                    .entries
                    .into_iter()
                    .map(|e| crate::routing::RouteEntry {
                        region_id: e.region_id,
                        measurement: e.measurement,
                        leader_node_id: e.leader_node_id,
                        leader_addr: e.leader_addr,
                        replica_addrs: e
                            .replica_addrs
                            .into_iter()
                            .map(|r| (r.node_id, r.addr))
                            .collect(),
                        key_range: None,
                        region_state: crate::types::RegionState::Active,
                    })
                    .collect();
                (measurement, routes)
            })
            .collect();

        Ok(crate::routing::RoutingSnapshot {
            version: resp.version,
            entries,
        })
    }

    /// Propose an arbitrary `MetaCommand` through the Raft cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the cluster is unreachable.
    pub async fn propose(&self, cmd: MetaCommand) -> Result<MetaResponse, Status> {
        let data = serde_json::to_vec(&cmd)
            .map_err(|e| Status::internal(format!("command serialization failed: {e}")))?;

        let mut client = self.connect().await?;
        let resp = client
            .propose(proto::ProposeRequest { command: data })
            .await?
            .into_inner();

        serde_json::from_slice(&resp.response)
            .map_err(|e| Status::internal(format!("response deserialization failed: {e}")))
    }

    /// Add a node to the Raft group.
    ///
    /// # Errors
    ///
    /// Returns an error if the membership change is rejected.
    pub async fn add_raft_node(
        &self,
        node_id: NodeId,
        grpc_addr: String,
        as_learner: bool,
    ) -> Result<(), Status> {
        let mut client = self.connect().await?;
        client
            .add_raft_node(proto::AddNodeRequest {
                node_id,
                grpc_addr,
                as_learner,
            })
            .await?;
        Ok(())
    }

    /// Remove a node from the Raft group.
    ///
    /// # Errors
    ///
    /// Returns an error if the membership change is rejected.
    pub async fn remove_raft_node(&self, node_id: NodeId) -> Result<(), Status> {
        let mut client = self.connect().await?;
        client
            .remove_raft_node(proto::RemoveNodeRequest { node_id })
            .await?;
        Ok(())
    }

    /// List registered nodes in the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the `MetaNode` cluster is unreachable.
    pub async fn list_nodes(&self) -> Result<(Vec<DataNodeInfo>, NodeId), Status> {
        let mut client = self.connect_for_read().await?;
        let resp = client.list_nodes(proto::Ack {}).await?.into_inner();

        let nodes = resp
            .nodes
            .into_iter()
            .map(|n| {
                let mode = match n.mode.as_str() {
                    "query" => NodeMode::Query,
                    "meta" => NodeMode::Meta,
                    _ => NodeMode::Data,
                };
                DataNodeInfo::new(n.node_id, n.grpc_addr)
                    .with_mode(mode)
                    .with_capacity(
                        n.disk_capacity_bytes,
                        n.memory_bytes,
                        u64::from(n.cpu_cores),
                    )
            })
            .collect();

        Ok((nodes, resp.leader_id))
    }

    /// Get cluster information.
    ///
    /// # Errors
    ///
    /// Returns an error if the `MetaNode` cluster is unreachable.
    pub async fn get_cluster_info(&self) -> Result<proto::ClusterInfoResponse, Status> {
        let mut client = self.connect_for_read().await?;
        let resp = client
            .get_cluster_info(proto::ClusterInfoRequest {})
            .await?
            .into_inner();
        Ok(resp)
    }
}

impl std::fmt::Debug for GrpcMetaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcMetaClient")
            .field("addrs", &*self.addrs.read())
            .finish_non_exhaustive()
    }
}

// ──────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_meta_client_creation() {
        let client = GrpcMetaClient::new(vec![
            "127.0.0.1:9001".to_string(),
            "127.0.0.1:9002".to_string(),
        ]);
        assert_eq!(client.addrs.read().len(), 2);
        assert_eq!(client.current.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn grpc_meta_client_update_addrs() {
        let client = GrpcMetaClient::new(vec!["127.0.0.1:9001".to_string()]);
        client
            .current
            .store(1, std::sync::atomic::Ordering::Relaxed);

        client.update_addrs(vec![
            "10.0.0.1:9001".to_string(),
            "10.0.0.2:9001".to_string(),
            "10.0.0.3:9001".to_string(),
        ]);

        assert_eq!(client.addrs.read().len(), 3);
        // Current index reset to 0 after update.
        assert_eq!(client.current.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn grpc_meta_client_with_timeout() {
        let client = GrpcMetaClient::new(vec!["127.0.0.1:9001".to_string()])
            .with_timeout(Duration::from_secs(10));
        assert_eq!(client.timeout, Duration::from_secs(10));
    }

    #[tokio::test]
    async fn grpc_meta_client_connect_no_addrs() {
        let client = GrpcMetaClient::new(vec![]);
        let result = client.connect().await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn grpc_meta_client_connect_unreachable() {
        let client = GrpcMetaClient::new(vec!["127.0.0.1:1".to_string()])
            .with_timeout(Duration::from_millis(200));
        let result = client.connect().await;
        // Either timeout or connection refused — both are Unavailable.
        assert!(result.is_err());
    }

    #[test]
    fn debug_format() {
        let client = GrpcMetaClient::new(vec!["127.0.0.1:9001".to_string()]);
        let debug = format!("{client:?}");
        assert!(debug.contains("GrpcMetaClient"));
        assert!(debug.contains("127.0.0.1:9001"));
    }

    #[test]
    fn grpc_meta_client_with_tls() {
        let tls = tonic::transport::ClientTlsConfig::new();
        let client = GrpcMetaClient::new(vec!["127.0.0.1:9001".to_string()]).with_tls(tls);
        assert!(client.tls_config.is_some());
    }

    #[test]
    fn grpc_meta_client_no_tls_by_default() {
        let client = GrpcMetaClient::new(vec!["127.0.0.1:9001".to_string()]);
        assert!(client.tls_config.is_none());
    }

    #[test]
    fn grpc_meta_client_stale_reads_default_off() {
        let client = GrpcMetaClient::new(vec!["127.0.0.1:9001".to_string()]);
        assert!(!client.allow_stale_reads);
    }

    #[test]
    fn grpc_meta_client_with_stale_reads() {
        let client = GrpcMetaClient::new(vec![
            "127.0.0.1:9001".to_string(),
            "127.0.0.1:9002".to_string(),
        ])
        .with_stale_reads(true);
        assert!(client.allow_stale_reads);
    }
}
