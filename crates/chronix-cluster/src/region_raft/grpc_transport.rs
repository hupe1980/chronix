//! gRPC-based Raft network transport for cross-node Region Raft groups.
//!
//! Mirrors the Meta Raft gRPC transport ([`chronix_meta::grpc_transport`])
//! but adds a `region_id` field to every RPC so the server can demux
//! to the correct per-region Raft group via [`RegionRaftManager`].
//!
//! All OpenRaft request/response types are serialised as **postcard**
//! inside the `RegionRaftRequest`/`RegionRaftResponse` protobuf wrappers.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory, Vote};
use parking_lot::RwLock;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use super::manager::RegionRaftManager;
use super::{NodeId, RegionId, RegionTypeConfig};

/// Generated proto types and service stubs.
#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod proto {
    tonic::include_proto!("chronix.region_raft.v1");
}

use proto::region_raft_service_client::RegionRaftServiceClient;
use proto::region_raft_service_server::RegionRaftService;
use proto::{RegionRaftRequest, RegionRaftResponse};

/// Simple newtype error for network failures.
#[derive(Debug)]
struct NetErr(String);

impl std::fmt::Display for NetErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NetErr {}

// ───────────────────────── gRPC Server ─────────────────────────────────

/// gRPC server handler for region Raft RPCs.
///
/// Wraps a [`RegionRaftManager`] and dispatches incoming RPCs to the
/// correct per-region Raft instance based on the `region_id` field.
/// Deploy one per `DataNode` process.
pub struct RegionRaftGrpcServer {
    manager: Arc<RegionRaftManager>,
}

impl RegionRaftGrpcServer {
    /// Create a new server handler backed by the given manager.
    #[must_use]
    pub fn new(manager: Arc<RegionRaftManager>) -> Self {
        Self { manager }
    }

    /// Build a tonic service for this handler.
    #[must_use]
    pub fn into_service(self) -> proto::region_raft_service_server::RegionRaftServiceServer<Self> {
        proto::region_raft_service_server::RegionRaftServiceServer::new(self)
    }

    /// Look up the Raft instance for the given region, or return NOT_FOUND.
    fn get_raft(&self, region_id: u64) -> Result<super::RegionRaft, Status> {
        self.manager
            .get_raft(region_id)
            .ok_or_else(|| Status::not_found(format!("region {region_id} not found")))
    }
}

#[tonic::async_trait]
impl RegionRaftService for RegionRaftGrpcServer {
    async fn append_entries(
        &self,
        request: Request<RegionRaftRequest>,
    ) -> Result<Response<RegionRaftResponse>, Status> {
        let inner = request.into_inner();
        let raft = self.get_raft(inner.region_id)?;

        let req: AppendEntriesRequest<RegionTypeConfig> = postcard::from_bytes(&inner.data)
            .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let resp = raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let data = postcard::to_stdvec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(RegionRaftResponse { data }))
    }

    async fn vote(
        &self,
        request: Request<RegionRaftRequest>,
    ) -> Result<Response<RegionRaftResponse>, Status> {
        let inner = request.into_inner();
        let raft = self.get_raft(inner.region_id)?;

        let req: VoteRequest<NodeId> = postcard::from_bytes(&inner.data)
            .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let resp = raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let data = postcard::to_stdvec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(RegionRaftResponse { data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RegionRaftRequest>,
    ) -> Result<Response<RegionRaftResponse>, Status> {
        let inner = request.into_inner();
        let raft = self.get_raft(inner.region_id)?;

        let req: InstallSnapshotRequest<RegionTypeConfig> = postcard::from_bytes(&inner.data)
            .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let resp = raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let data = postcard::to_stdvec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(RegionRaftResponse { data }))
    }
}

// ───────────────────────── Node Address Map ────────────────────────────

/// Maps `DataNode` IDs to gRPC endpoint addresses for Region Raft.
///
/// All regions on the same node share a single address — HTTP/2
/// multiplexing handles per-region concurrency.
#[derive(Clone, Debug, Default)]
pub struct DataNodeAddressMap {
    map: Arc<RwLock<BTreeMap<NodeId, String>>>,
}

impl DataNodeAddressMap {
    /// Create a new, empty address map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an endpoint address for a DataNode.
    pub fn insert(&self, node_id: NodeId, addr: impl Into<String>) {
        self.map.write().insert(node_id, addr.into());
    }

    /// Remove a DataNode's address.
    pub fn remove(&self, node_id: NodeId) {
        self.map.write().remove(&node_id);
    }

    /// Look up a DataNode's endpoint address.
    #[must_use]
    pub fn get(&self, node_id: NodeId) -> Option<String> {
        self.map.read().get(&node_id).cloned()
    }

    /// Number of registered addresses.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.read().len()
    }

    /// Whether the map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }
}

// ───────────────────────── gRPC Client / Network ──────────────────────

/// [`RaftNetworkFactory`] implementation that creates gRPC connections
/// to remote `DataNode` peers for a specific region Raft group.
///
/// One factory per region — the `region_id` is embedded in every RPC.
/// When `tls_config` is set, all connections use mutual TLS.
#[derive(Clone, Debug)]
pub struct RegionGrpcNetworkFactory {
    addresses: DataNodeAddressMap,
    region_id: RegionId,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
}

impl RegionGrpcNetworkFactory {
    /// Create a factory for the given region backed by the address map.
    #[must_use]
    pub fn new(addresses: DataNodeAddressMap, region_id: RegionId) -> Self {
        Self {
            addresses,
            region_id,
            tls_config: None,
        }
    }

    /// Enable mTLS for all Region Raft peer connections.
    #[must_use]
    pub fn with_tls(mut self, tls: tonic::transport::ClientTlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    /// Access the underlying address map.
    #[must_use]
    pub fn addresses(&self) -> &DataNodeAddressMap {
        &self.addresses
    }
}

impl RaftNetworkFactory<RegionTypeConfig> for RegionGrpcNetworkFactory {
    type Network = RegionGrpcNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        RegionGrpcNetwork {
            target,
            region_id: self.region_id,
            addresses: self.addresses.clone(),
            tls_config: self.tls_config.clone(),
            cached_channel: None,
        }
    }
}

/// gRPC-based Raft network connection to a single target DataNode
/// for a specific region Raft group.
///
/// Caches a lazily-connected `Channel` with HTTP/2 multiplexing.
/// All RPCs include the `region_id` for server-side demuxing.
pub struct RegionGrpcNetwork {
    target: NodeId,
    region_id: RegionId,
    addresses: DataNodeAddressMap,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
    cached_channel: Option<Channel>,
}

impl std::fmt::Debug for RegionGrpcNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionGrpcNetwork")
            .field("target", &self.target)
            .field("region_id", &self.region_id)
            .finish_non_exhaustive()
    }
}

impl RegionGrpcNetwork {
    /// Get or create a cached gRPC channel to the target DataNode.
    fn get_or_connect_channel(
        &mut self,
    ) -> Result<Channel, RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>> {
        if let Some(ref channel) = self.cached_channel {
            return Ok(channel.clone());
        }

        let addr = self.addresses.get(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(&NetErr(format!(
                "no address for DataNode {}",
                self.target
            )))))
        })?;

        debug!(
            target_node = self.target,
            region_id = self.region_id,
            addr = %addr,
            "creating lazy gRPC channel to region raft peer"
        );

        let mut endpoint = Channel::from_shared(addr.clone()).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(&NetErr(format!(
                "invalid endpoint {addr}: {e}"
            )))))
        })?;

        // HTTP/2 keepalive for fast dead-peer detection
        endpoint = endpoint
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(10))
            .keep_alive_timeout(std::time::Duration::from_secs(5))
            .connect_timeout(std::time::Duration::from_secs(5));

        if let Some(tls) = &self.tls_config {
            endpoint = endpoint.tls_config(tls.clone()).map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&NetworkError::new(&NetErr(format!(
                    "TLS config error: {e}"
                )))))
            })?;
        }

        let channel = endpoint.connect_lazy();
        self.cached_channel = Some(channel.clone());
        metrics::counter!("chronix_region_raft_channels_created_total").increment(1);
        Ok(channel)
    }

    /// Get a client stub using the cached channel.
    fn connect(
        &mut self,
    ) -> Result<
        RegionRaftServiceClient<Channel>,
        RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        let channel = self.get_or_connect_channel()?;
        Ok(RegionRaftServiceClient::new(channel))
    }
}

impl RaftNetwork<RegionTypeConfig> for RegionGrpcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RegionTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        let mut client = self.connect().map_err(|e| {
            warn!(target_node = self.target, region = self.region_id, error = %e, "region raft connect failed");
            e
        })?;

        let data = postcard::to_stdvec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&NetErr(format!("serialize: {e}"))))
        })?;

        let resp = client
            .append_entries(Request::new(RegionRaftRequest {
                region_id: self.region_id,
                data,
            }))
            .await
            .map_err(|e| {
                RPCError::Network(NetworkError::new(&NetErr(format!(
                    "append_entries rpc: {e}"
                ))))
            })?;

        postcard::from_bytes(&resp.into_inner().data)
            .map_err(|e| RPCError::Network(NetworkError::new(&NetErr(format!("deserialize: {e}")))))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>>
    {
        let mut client = self.connect().map_err(|e| {
            warn!(target_node = self.target, region = self.region_id, error = %e, "region raft connect failed");
            e
        })?;

        let data = postcard::to_stdvec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&NetErr(format!("serialize: {e}"))))
        })?;

        let resp = client
            .vote(Request::new(RegionRaftRequest {
                region_id: self.region_id,
                data,
            }))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&NetErr(format!("vote rpc: {e}")))))?;

        postcard::from_bytes(&resp.into_inner().data)
            .map_err(|e| RPCError::Network(NetworkError::new(&NetErr(format!("deserialize: {e}")))))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<RegionTypeConfig>,
        _cancel: impl futures::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<NodeId>,
        StreamingError<RegionTypeConfig, openraft::error::Fatal<NodeId>>,
    > {
        let snapshot_data = snapshot.snapshot.into_inner();
        let req: InstallSnapshotRequest<RegionTypeConfig> = InstallSnapshotRequest {
            vote,
            meta: snapshot.meta,
            offset: 0,
            data: snapshot_data,
            done: true,
        };

        let data = postcard::to_stdvec(&req).map_err(|e| {
            let err = NetErr(format!("serialize snapshot: {e}"));
            StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
        })?;

        let mut client =
            RegionRaftServiceClient::new(self.get_or_connect_channel().map_err(|e| {
                let err = NetErr(format!("connect for snapshot: {e}"));
                StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
            })?);

        let resp = client
            .install_snapshot(Request::new(RegionRaftRequest {
                region_id: self.region_id,
                data,
            }))
            .await
            .map_err(|e| {
                let err = NetErr(format!("install_snapshot rpc: {e}"));
                StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
            })?;

        let install_resp: InstallSnapshotResponse<NodeId> =
            postcard::from_bytes(&resp.into_inner().data).map_err(|e| {
                let err = NetErr(format!("deserialize snapshot resp: {e}"));
                StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
            })?;

        Ok(SnapshotResponse::new(install_resp.vote))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<RegionTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let mut client = self.connect().map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            RPCError::Network(n) => RPCError::Network(n),
            _ => RPCError::Network(NetworkError::new(&NetErr(format!(
                "install_snapshot connect: {e}"
            )))),
        })?;

        let data = postcard::to_stdvec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&NetErr(format!("serialize: {e}"))))
        })?;

        let resp = client
            .install_snapshot(Request::new(RegionRaftRequest {
                region_id: self.region_id,
                data,
            }))
            .await
            .map_err(|e| {
                RPCError::Network(NetworkError::new(&NetErr(format!(
                    "install_snapshot rpc: {e}"
                ))))
            })?;

        postcard::from_bytes(&resp.into_inner().data)
            .map_err(|e| RPCError::Network(NetworkError::new(&NetErr(format!("deserialize: {e}")))))
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_map_basic_operations() {
        let map = DataNodeAddressMap::new();
        assert!(map.is_empty());

        map.insert(1, "http://127.0.0.1:4242");
        map.insert(2, "http://127.0.0.1:4243");
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(1), Some("http://127.0.0.1:4242".to_string()));

        map.remove(1);
        assert_eq!(map.len(), 1);
        assert!(map.get(1).is_none());
    }

    #[tokio::test]
    async fn grpc_network_factory_creates_client() {
        let addresses = DataNodeAddressMap::new();
        addresses.insert(42, "http://127.0.0.1:4242");

        let mut factory = RegionGrpcNetworkFactory::new(addresses, 100);
        let network = factory
            .new_client(42, &BasicNode::new("127.0.0.1:4242"))
            .await;

        assert_eq!(network.target, 42);
        assert_eq!(network.region_id, 100);
    }

    #[tokio::test]
    async fn grpc_network_channel_caching() {
        let addresses = DataNodeAddressMap::new();
        addresses.insert(1, "http://127.0.0.1:9999");

        let mut factory = RegionGrpcNetworkFactory::new(addresses, 1);
        let mut network = factory
            .new_client(1, &BasicNode::new("127.0.0.1:9999"))
            .await;

        // First call creates the channel
        assert!(network.cached_channel.is_none());
        let _ch = network.get_or_connect_channel().unwrap();
        assert!(network.cached_channel.is_some());

        // Second call reuses it
        let _ch2 = network.get_or_connect_channel().unwrap();
        assert!(network.cached_channel.is_some());
    }

    #[tokio::test]
    async fn grpc_network_missing_address() {
        let addresses = DataNodeAddressMap::new();
        // No address registered for node 99

        let mut factory = RegionGrpcNetworkFactory::new(addresses, 1);
        let mut network = factory
            .new_client(99, &BasicNode::new("127.0.0.1:9999"))
            .await;

        let result = network.get_or_connect_channel();
        assert!(result.is_err());
    }

    #[test]
    fn server_requires_manager() {
        // Just verify construction compiles — actual RPC tests need
        // a running server which is tested at integration level.
        let router = super::super::RegionRaftRouter::new();
        let manager = Arc::new(RegionRaftManager::new_in_memory(router, 1));
        let _server = RegionRaftGrpcServer::new(manager);
    }
}
