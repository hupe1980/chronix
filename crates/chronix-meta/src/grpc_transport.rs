//! gRPC-based Raft network transport for multi-process `MetaNode` clusters.
//!
//! Provides both the server-side handler ([`RaftGrpcServer`]) and a
//! client-side [`RaftNetworkFactory`] implementation ([`GrpcNetworkFactory`])
//! that routes Raft RPCs over tonic gRPC connections.
//!
//! All OpenRaft request/response types are serialised as **postcard** inside
//! the generic `RaftRequest`/`RaftResponse` protobuf wrappers. Bincode is
//! 5–10× smaller and faster than the previous JSON encoding.

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
use tracing::debug;

use crate::store::{MetaRaft, MetaTypeConfig};

/// Default snapshot chunk size: 4 MiB.
///
/// gRPC has a default maximum message size of 4 MiB, and Chronix configures
/// a 64 MiB cap. Large Raft snapshots that approach or exceed this limit
/// risk OOM on both sender and receiver (a single `Vec<u8>` per message).
///
/// These helpers split snapshot data into fixed-size chunks for safer
/// transfer. The current `full_snapshot` implementation still sends a
/// single message (OpenRaft's `InstallSnapshotRequest` API), but callers
/// can use these utilities for custom streaming or retry logic.
pub const SNAPSHOT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Split snapshot data into fixed-size chunks for transfer.
///
/// Each chunk is at most `chunk_size` bytes. The last chunk may be
/// smaller. An empty input produces a single empty chunk to preserve
/// round-trip semantics with [`assemble_snapshot_chunks`].
///
/// # Example
///
/// ```no_run
/// use chronix_meta::grpc_transport::{chunk_snapshot, assemble_snapshot_chunks, SNAPSHOT_CHUNK_SIZE};
///
/// let data = vec![0u8; 10_000_000]; // 10 MB
/// let chunks = chunk_snapshot(&data, SNAPSHOT_CHUNK_SIZE);
/// assert_eq!(chunks.len(), 3); // 4 MiB + 4 MiB + ~1.5 MiB
///
/// let reassembled = assemble_snapshot_chunks(&chunks);
/// assert_eq!(reassembled, data);
/// ```
#[must_use]
pub fn chunk_snapshot(data: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
    if data.is_empty() {
        return vec![Vec::new()];
    }
    let chunk_size = chunk_size.max(1); // avoid division by zero
    data.chunks(chunk_size).map(|c| c.to_vec()).collect()
}

/// Reassemble chunks produced by [`chunk_snapshot`] into the original
/// snapshot data.
///
/// This is the inverse of [`chunk_snapshot`]: concatenates all chunks
/// in order.
#[must_use]
pub fn assemble_snapshot_chunks(chunks: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    let mut out = Vec::with_capacity(total);
    for chunk in chunks {
        out.extend_from_slice(chunk);
    }
    out
}

/// Generated proto types and service stubs.
#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod proto {
    tonic::include_proto!("chronix.raft.v1");
}

use proto::raft_service_client::RaftServiceClient;
use proto::raft_service_server::RaftService;
use proto::{RaftRequest, RaftResponse};

type NodeId = u64;

// ───────────────────────── gRPC Server ─────────────────────────────────

/// gRPC server handler for Raft RPCs.
///
/// Wraps a local [`MetaRaft`] instance and dispatches incoming gRPC
/// calls to the `OpenRaft` API. Deploy one per `MetaNode` process.
pub struct RaftGrpcServer {
    raft: MetaRaft,
}

impl RaftGrpcServer {
    /// Create a new server handler for the given Raft instance.
    #[must_use]
    pub fn new(raft: MetaRaft) -> Self {
        Self { raft }
    }

    /// Build a tonic [`Server`](tonic::transport::Server) service for this handler.
    ///
    /// Configures the service with a 256 MiB message-size limit to support
    /// large Raft snapshots without RESOURCE_EXHAUSTED errors.
    #[must_use]
    pub fn into_service(self) -> proto::raft_service_server::RaftServiceServer<Self> {
        proto::raft_service_server::RaftServiceServer::new(self)
            .max_decoding_message_size(256 * 1024 * 1024)
            .max_encoding_message_size(256 * 1024 * 1024)
    }
}

#[tonic::async_trait]
impl RaftService for RaftGrpcServer {
    async fn append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let req: AppendEntriesRequest<MetaTypeConfig> =
            postcard::from_bytes(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let data = postcard::to_stdvec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(RaftResponse { data }))
    }

    async fn vote(&self, request: Request<RaftRequest>) -> Result<Response<RaftResponse>, Status> {
        let req: VoteRequest<NodeId> = postcard::from_bytes(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let data = postcard::to_stdvec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(RaftResponse { data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let req: InstallSnapshotRequest<MetaTypeConfig> =
            postcard::from_bytes(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("deserialize error: {e}")))?;

        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(format!("raft error: {e}")))?;

        let data = postcard::to_stdvec(&resp)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

        Ok(Response::new(RaftResponse { data }))
    }
}

// ───────────────────────── gRPC Client / Network ───────────────────────

/// Maps node IDs to gRPC endpoint addresses.
///
/// Used by [`GrpcNetworkFactory`] to look up the target address for
/// each Raft peer.
#[derive(Clone, Debug, Default)]
pub struct NodeAddressMap {
    map: Arc<RwLock<BTreeMap<NodeId, String>>>,
}

impl NodeAddressMap {
    /// Create a new, empty address map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an endpoint address for a node.
    pub fn insert(&self, node_id: NodeId, addr: impl Into<String>) {
        self.map.write().insert(node_id, addr.into());
    }

    /// Remove a node's address.
    pub fn remove(&self, node_id: NodeId) {
        self.map.write().remove(&node_id);
    }

    /// Look up a node's endpoint address.
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

/// [`RaftNetworkFactory`] implementation that creates gRPC connections
/// to remote `MetaNode` peers.
///
/// When `tls_config` is set, all Raft peer connections use mutual TLS.
#[derive(Clone, Debug)]
pub struct GrpcNetworkFactory {
    addresses: NodeAddressMap,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
}

impl GrpcNetworkFactory {
    /// Create a factory backed by the given address map.
    #[must_use]
    pub fn new(addresses: NodeAddressMap) -> Self {
        Self {
            addresses,
            tls_config: None,
        }
    }

    /// Enable mTLS for all Raft peer connections.
    #[must_use]
    pub fn with_tls(mut self, tls: tonic::transport::ClientTlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    /// Access the underlying address map.
    #[must_use]
    pub fn addresses(&self) -> &NodeAddressMap {
        &self.addresses
    }
}

impl RaftNetworkFactory<MetaTypeConfig> for GrpcNetworkFactory {
    type Network = GrpcNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        GrpcNetwork {
            target,
            addresses: self.addresses.clone(),
            tls_config: self.tls_config.clone(),
            cached_channel: None,
        }
    }
}

/// gRPC-based Raft network connection to a single target node.
///
/// Caches a lazily-connected `Channel` that uses HTTP/2 multiplexing for
/// all RPCs to the same target. The channel reconnects automatically on
/// transport failures (connect_lazy + hyper's built-in reconnection).
pub struct GrpcNetwork {
    target: NodeId,
    addresses: NodeAddressMap,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
    /// Cached channel — created lazily on first RPC, reused thereafter.
    /// `connect_lazy()` handles automatic reconnection under the hood.
    cached_channel: Option<Channel>,
}

impl std::fmt::Debug for GrpcNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcNetwork")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl GrpcNetwork {
    /// Get or create a cached gRPC channel to the target node.
    ///
    /// Uses `connect_lazy()` which defers the actual TCP handshake until
    /// the first RPC, and automatically reconnects on transport errors
    /// (HTTP/2 GOAWAY, connection reset, etc.). This avoids creating a
    /// new TCP connection per Raft RPC.
    fn get_or_connect_channel(
        &mut self,
    ) -> Result<Channel, RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>> {
        if let Some(ref channel) = self.cached_channel {
            return Ok(channel.clone());
        }

        let addr = self.addresses.get(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(
                &crate::network::NetErr(format!("no address for node {}", self.target)),
            )))
        })?;

        debug!(target = self.target, addr = %addr, "creating lazy gRPC channel to raft peer");

        let mut endpoint = Channel::from_shared(addr.clone()).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&NetworkError::new(
                &crate::network::NetErr(format!("invalid endpoint {addr}: {e}")),
            )))
        })?;

        // Configure connection keepalive for fast failure detection
        endpoint = endpoint
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(10))
            .keep_alive_timeout(std::time::Duration::from_secs(5))
            .connect_timeout(std::time::Duration::from_secs(5));

        if let Some(tls) = &self.tls_config {
            endpoint = endpoint.tls_config(tls.clone()).map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&NetworkError::new(
                    &crate::network::NetErr(format!("TLS config error: {e}")),
                )))
            })?;
        }

        // connect_lazy() returns immediately — actual TCP handshake happens
        // on first RPC. The channel handles reconnection automatically.
        let channel = endpoint.connect_lazy();

        self.cached_channel = Some(channel.clone());
        metrics::counter!("chronix_grpc_channels_created_total").increment(1);
        Ok(channel)
    }

    /// Connect to the target node's gRPC endpoint using the cached channel.
    async fn connect(
        &mut self,
    ) -> Result<
        RaftServiceClient<Channel>,
        RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        let channel = self.get_or_connect_channel()?;
        Ok(RaftServiceClient::new(channel)
            .max_decoding_message_size(256 * 1024 * 1024)
            .max_encoding_message_size(256 * 1024 * 1024))
    }

    /// Invalidate the cached channel (e.g. after address change).
    ///
    /// Currently used only in tests; retained as a useful API for
    /// reconnect-on-address-change logic.
    #[allow(dead_code)]
    fn invalidate_channel(&mut self) {
        self.cached_channel = None;
    }
}

impl RaftNetwork<MetaTypeConfig> for GrpcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        let mut client = self.connect().await?;

        let data = postcard::to_stdvec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                "serialize: {e}"
            ))))
        })?;

        let resp = client
            .append_entries(Request::new(RaftRequest { data }))
            .await
            .map_err(|e| {
                RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                    "append_entries rpc: {e}"
                ))))
            })?;

        postcard::from_bytes(&resp.into_inner().data).map_err(|e| {
            RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                "deserialize: {e}"
            ))))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>>
    {
        let mut client = self.connect().await?;

        let data = postcard::to_stdvec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                "serialize: {e}"
            ))))
        })?;

        let resp = client
            .vote(Request::new(RaftRequest { data }))
            .await
            .map_err(|e| {
                RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                    "vote rpc: {e}"
                ))))
            })?;

        postcard::from_bytes(&resp.into_inner().data).map_err(|e| {
            RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                "deserialize: {e}"
            ))))
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<MetaTypeConfig>,
        _cancel: impl futures::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<NodeId>,
        StreamingError<MetaTypeConfig, openraft::error::Fatal<NodeId>>,
    > {
        // Snapshot transfer via single gRPC message.
        //
        // The snapshot is serialised into a single `InstallSnapshotRequest`
        // and sent as one gRPC message. This works for snapshots up to the
        // 256 MiB message-size limit configured in `into_service()`.
        //
        // For snapshots approaching this limit, the data can be pre-chunked
        // using `chunk_snapshot()` / `assemble_snapshot_chunks()` from this
        // module. A full streaming snapshot protocol (multiple gRPC messages
        // per snapshot) would require changes to the OpenRaft
        // `RaftNetwork::full_snapshot` trait and is tracked as future work.
        //
        // Current risk: A snapshot > 256 MiB will fail with a gRPC
        // RESOURCE_EXHAUSTED error. In practice, MetaNode snapshots are
        // small (schema + routing table), typically < 1 MiB.
        let snapshot_data = snapshot.snapshot.into_inner();
        let req: InstallSnapshotRequest<MetaTypeConfig> = InstallSnapshotRequest {
            vote,
            meta: snapshot.meta,
            offset: 0,
            data: snapshot_data,
            done: true,
        };

        let data = postcard::to_stdvec(&req).map_err(|e| {
            let err = crate::network::NetErr(format!("serialize snapshot: {e}"));
            StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
        })?;

        // Reuse the cached channel instead of creating a new connection
        let channel = self.get_or_connect_channel().map_err(|e| match e {
            RPCError::Unreachable(u) => StreamingError::Unreachable(u),
            RPCError::Network(n) => StreamingError::Unreachable(Unreachable::new(&n)),
            _ => {
                let err = crate::network::NetErr("unexpected error type".to_string());
                StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
            }
        })?;

        let mut client = RaftServiceClient::new(channel)
            .max_decoding_message_size(256 * 1024 * 1024)
            .max_encoding_message_size(256 * 1024 * 1024);

        let resp = client
            .install_snapshot(Request::new(RaftRequest { data }))
            .await
            .map_err(|e| {
                let err = crate::network::NetErr(format!("install_snapshot rpc: {e}"));
                StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
            })?;

        postcard::from_bytes(&resp.into_inner().data).map_err(|e| {
            let err = crate::network::NetErr(format!("deserialize snapshot resp: {e}"));
            StreamingError::Unreachable(Unreachable::new(&NetworkError::new(&err)))
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let mut client = self.connect().await.map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            RPCError::Network(n) => RPCError::Network(n),
            _ => RPCError::Network(NetworkError::new(&crate::network::NetErr(
                "unexpected error type".to_string(),
            ))),
        })?;

        let data = postcard::to_stdvec(&rpc).map_err(|e| {
            RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                "serialize: {e}"
            ))))
        })?;

        let resp: tonic::Response<RaftResponse> = client
            .install_snapshot(Request::new(RaftRequest { data }))
            .await
            .map_err(|e| {
                RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                    "install_snapshot rpc: {e}"
                ))))
            })?;

        postcard::from_bytes(&resp.into_inner().data).map_err(|e| {
            RPCError::Network(NetworkError::new(&crate::network::NetErr(format!(
                "deserialize: {e}"
            ))))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn node_address_map_operations() {
        let map = NodeAddressMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        map.insert(1, "http://127.0.0.1:9100");
        map.insert(2, "http://127.0.0.1:9200");
        assert_eq!(map.len(), 2);
        assert!(!map.is_empty());

        assert_eq!(map.get(1).unwrap(), "http://127.0.0.1:9100");
        assert_eq!(map.get(2).unwrap(), "http://127.0.0.1:9200");
        assert!(map.get(3).is_none());

        map.remove(1);
        assert_eq!(map.len(), 1);
        assert!(map.get(1).is_none());
    }

    #[test]
    fn grpc_factory_creation() {
        let addrs = NodeAddressMap::new();
        addrs.insert(1, "http://127.0.0.1:9100");

        let factory = GrpcNetworkFactory::new(addrs);
        assert_eq!(factory.addresses().len(), 1);
    }

    #[tokio::test]
    async fn grpc_network_unreachable_no_address() {
        let addrs = NodeAddressMap::new();
        let mut net = GrpcNetwork {
            target: 99,
            addresses: addrs,
            tls_config: None,
            cached_channel: None,
        };

        let req = VoteRequest::<NodeId> {
            vote: Vote::new(1, 1),
            last_log_id: None,
        };

        let result = net.vote(req, RPCOption::new(Duration::from_secs(5))).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn grpc_network_connect_refused() {
        let addrs = NodeAddressMap::new();
        // Point to a port that's not listening
        addrs.insert(1, "http://127.0.0.1:1");

        let mut net = GrpcNetwork {
            target: 1,
            addresses: addrs,
            tls_config: None,
            cached_channel: None,
        };

        let req = VoteRequest::<NodeId> {
            vote: Vote::new(1, 1),
            last_log_id: None,
        };

        let result = net.vote(req, RPCOption::new(Duration::from_secs(5))).await;
        assert!(result.is_err());
    }

    #[test]
    fn grpc_factory_with_tls() {
        let addrs = NodeAddressMap::new();
        let tls = tonic::transport::ClientTlsConfig::new();
        let factory = GrpcNetworkFactory::new(addrs).with_tls(tls);
        assert!(factory.tls_config.is_some());
    }

    #[test]
    fn grpc_factory_no_tls_by_default() {
        let addrs = NodeAddressMap::new();
        let factory = GrpcNetworkFactory::new(addrs);
        assert!(factory.tls_config.is_none());
    }

    // ── Snapshot chunking tests ─────────────────────────────

    use super::{assemble_snapshot_chunks, chunk_snapshot, SNAPSHOT_CHUNK_SIZE};

    #[test]
    fn chunk_snapshot_round_trip() {
        let data: Vec<u8> = (0..=255).cycle().take(10_000_000).collect();
        let chunks = chunk_snapshot(&data, SNAPSHOT_CHUNK_SIZE);
        // 10 MB / 4 MiB = 3 chunks (ceil)
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), SNAPSHOT_CHUNK_SIZE);
        assert_eq!(chunks[1].len(), SNAPSHOT_CHUNK_SIZE);
        assert!(chunks[2].len() < SNAPSHOT_CHUNK_SIZE);

        let reassembled = assemble_snapshot_chunks(&chunks);
        assert_eq!(reassembled, data);
    }

    #[test]
    fn chunk_snapshot_empty() {
        let chunks = chunk_snapshot(&[], SNAPSHOT_CHUNK_SIZE);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_empty());

        let reassembled = assemble_snapshot_chunks(&chunks);
        assert!(reassembled.is_empty());
    }

    #[test]
    fn chunk_snapshot_smaller_than_chunk_size() {
        let data = vec![1u8; 100];
        let chunks = chunk_snapshot(&data, SNAPSHOT_CHUNK_SIZE);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 100);

        let reassembled = assemble_snapshot_chunks(&chunks);
        assert_eq!(reassembled, data);
    }

    #[test]
    fn chunk_snapshot_exact_multiple() {
        let data = vec![42u8; 3 * SNAPSHOT_CHUNK_SIZE];
        let chunks = chunk_snapshot(&data, SNAPSHOT_CHUNK_SIZE);
        assert_eq!(chunks.len(), 3);
        for c in &chunks {
            assert_eq!(c.len(), SNAPSHOT_CHUNK_SIZE);
        }
        assert_eq!(assemble_snapshot_chunks(&chunks), data);
    }

    #[test]
    fn chunk_snapshot_custom_size() {
        let data = vec![7u8; 25];
        let chunks = chunk_snapshot(&data, 10);
        assert_eq!(chunks.len(), 3); // 10 + 10 + 5
        assert_eq!(chunks[0].len(), 10);
        assert_eq!(chunks[1].len(), 10);
        assert_eq!(chunks[2].len(), 5);
        assert_eq!(assemble_snapshot_chunks(&chunks), data);
    }

    #[test]
    fn assemble_empty_chunks_list() {
        let reassembled = assemble_snapshot_chunks(&[]);
        assert!(reassembled.is_empty());
    }

    #[tokio::test]
    async fn grpc_network_channel_caching() {
        let addrs = NodeAddressMap::new();
        addrs.insert(1, "http://127.0.0.1:9100");

        let mut net = GrpcNetwork {
            target: 1,
            addresses: addrs,
            tls_config: None,
            cached_channel: None,
        };

        // First call creates the channel
        assert!(net.cached_channel.is_none());
        let result = net.get_or_connect_channel();
        assert!(result.is_ok());
        assert!(net.cached_channel.is_some());

        // Second call reuses the cached channel
        let result2 = net.get_or_connect_channel();
        assert!(result2.is_ok());

        // Invalidate and verify
        net.invalidate_channel();
        assert!(net.cached_channel.is_none());

        // Re-creates on next call
        let result3 = net.get_or_connect_channel();
        assert!(result3.is_ok());
        assert!(net.cached_channel.is_some());
    }
}
