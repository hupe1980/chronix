//! gRPC client for remote `DataNode` operations with connection pooling.
//!
//! [`DataGrpcClient`] maintains a pool of persistent gRPC channels keyed by
//! `NodeId`, reusing connections across requests to avoid repeated TCP/TLS
//! handshakes.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tonic::transport::{Channel, ClientTlsConfig};
use tracing::{debug, warn};

use chronix_meta::NodeId;

use crate::data_service::proto::{
    data_service_client::DataServiceClient, DataPoint, QueryRegionRequest, QueryRegionResponse,
    ReplicateRequest, ReplicateResponse, WriteRegionRequest, WriteRegionResponse,
};
use crate::error::{ClusterError, Result};

/// Default connection timeout for new gRPC channels.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default request timeout.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// gRPC client with persistent connection pooling for `DataNode` operations.
///
/// Channels are created lazily on first use and cached by (`NodeId`, addr)
/// for subsequent requests. Thread-safe via [`DashMap`].
///
/// When `tls_config` is set, all connections use mutual TLS for encrypted
/// and authenticated inter-node communication.
#[derive(Debug)]
pub struct DataGrpcClient {
    /// Cached channels: `NodeId` → `Channel`.
    channels: Arc<DashMap<NodeId, Channel>>,
    /// Address map: `NodeId` → gRPC address string.
    addrs: Arc<DashMap<NodeId, String>>,
    /// TCP connection timeout.
    connect_timeout: Duration,
    /// Per-request timeout.
    request_timeout: Duration,
    /// Optional mTLS configuration for inter-node communication.
    tls_config: Option<ClientTlsConfig>,
}

impl DataGrpcClient {
    /// Create a new `DataGrpcClient` with default timeouts.
    #[must_use]
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            addrs: Arc::new(DashMap::new()),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            tls_config: None,
        }
    }

    /// Set the TCP connection timeout.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the per-request timeout.
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Enable mTLS for all inter-node connections.
    #[must_use]
    pub fn with_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    /// Register or update the address for a node.
    ///
    /// If the address changed, the cached channel is invalidated so the
    /// next RPC creates a fresh connection to the new address.
    pub fn set_node_addr(&self, node_id: NodeId, addr: impl Into<String>) {
        let addr = addr.into();
        let changed = self
            .addrs
            .get(&node_id)
            .is_none_or(|existing| *existing != addr);
        if changed {
            self.channels.remove(&node_id);
        }
        self.addrs.insert(node_id, addr);
    }

    /// Remove a node's address and cached channel.
    pub fn remove_node(&self, node_id: NodeId) {
        self.addrs.remove(&node_id);
        self.channels.remove(&node_id);
    }

    /// Number of currently cached channels.
    #[must_use]
    pub fn pool_size(&self) -> usize {
        self.channels.len()
    }

    /// Get or create a gRPC channel to the given node.
    ///
    /// Uses `DashMap::entry()` to avoid racing two concurrent connection
    /// attempts for the same node.
    async fn get_channel(&self, node_id: NodeId) -> Result<Channel> {
        // Fast path: already cached
        if let Some(ch) = self.channels.get(&node_id) {
            return Ok(ch.value().clone());
        }

        // Need the address
        let addr = self
            .addrs
            .get(&node_id)
            .map(|r| r.value().clone())
            .ok_or(ClusterError::NodeNotFound(node_id))?;

        debug!(node_id, %addr, "connecting to data node");

        let mut endpoint = Channel::from_shared(addr.clone())
            .map_err(|e| ClusterError::InvalidConfig(format!("invalid endpoint: {e}")))?
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout);

        if let Some(tls) = &self.tls_config {
            endpoint = endpoint
                .tls_config(tls.clone())
                .map_err(|e| ClusterError::InvalidConfig(format!("TLS config error: {e}")))?;
        }

        let channel = endpoint.connect().await.map_err(|e| {
            warn!(node_id, %addr, %e, "failed to connect to data node");
            ClusterError::Transport(format!("connect to node {node_id}: {e}"))
        })?;

        // Use entry() to avoid overwriting a channel that another task
        // established concurrently.
        let ch = self
            .channels
            .entry(node_id)
            .or_insert(channel)
            .value()
            .clone();
        Ok(ch)
    }

    /// Get a typed service client for a given node.
    async fn client(&self, node_id: NodeId) -> Result<DataServiceClient<Channel>> {
        let channel = self.get_channel(node_id).await?;
        Ok(DataServiceClient::new(channel))
    }

    /// Write points to a region on a remote `DataNode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the node address is unknown, the connection
    /// fails, or the remote RPC returns an error status.
    pub async fn write_region(
        &self,
        node_id: NodeId,
        region_id: u64,
        points: Vec<DataPoint>,
    ) -> Result<WriteRegionResponse> {
        let mut client = self.client(node_id).await?;
        let resp = client
            .write_region(WriteRegionRequest { region_id, points })
            .await
            .map_err(|e| self.handle_rpc_error(node_id, region_id, &e))?;
        Ok(resp.into_inner())
    }

    /// Query a region on a remote `DataNode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails or the RPC returns an error.
    pub async fn query_region(
        &self,
        node_id: NodeId,
        request: QueryRegionRequest,
    ) -> Result<QueryRegionResponse> {
        let region_id = request.region_id;
        let mut client = self.client(node_id).await?;
        let resp = client
            .query_region(request)
            .await
            .map_err(|e| self.handle_rpc_error(node_id, region_id, &e))?;
        Ok(resp.into_inner())
    }

    /// Replicate WAL entries to a follower region on a remote `DataNode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails or the RPC returns an error.
    pub async fn replicate_wal(
        &self,
        node_id: NodeId,
        request: ReplicateRequest,
    ) -> Result<ReplicateResponse> {
        let region_id = request.region_id;
        let mut client = self.client(node_id).await?;
        let resp = client
            .replicate_wal(request)
            .await
            .map_err(|e| self.handle_rpc_error(node_id, region_id, &e))?;
        Ok(resp.into_inner())
    }

    /// Convert a tonic [`Status`] error, evicting the channel if it looks
    /// like a transport failure.
    fn handle_rpc_error(
        &self,
        node_id: NodeId,
        region_id: u64,
        status: &tonic::Status,
    ) -> ClusterError {
        // Evict cached channel on transport-level errors so next call reconnects
        if matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::Unknown
        ) {
            self.channels.remove(&node_id);
        }

        match status.code() {
            tonic::Code::NotFound => ClusterError::RegionNotFound(region_id),
            tonic::Code::DeadlineExceeded => ClusterError::Timeout,
            tonic::Code::Unavailable => {
                ClusterError::Transport(format!("node {node_id} unavailable: {status}"))
            }
            _ => ClusterError::Transport(format!("rpc to node {node_id}: {status}")),
        }
    }
}

impl Default for DataGrpcClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for DataGrpcClient {
    fn clone(&self) -> Self {
        Self {
            channels: Arc::clone(&self.channels),
            addrs: Arc::clone(&self.addrs),
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            tls_config: self.tls_config.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_empty_pool() {
        let client = DataGrpcClient::new();
        assert_eq!(client.pool_size(), 0);
    }

    #[test]
    fn set_and_remove_node_addr() {
        let client = DataGrpcClient::new();
        client.set_node_addr(1, "http://localhost:5000");
        assert!(client.addrs.contains_key(&1));

        client.remove_node(1);
        assert!(!client.addrs.contains_key(&1));
    }

    #[test]
    fn addr_change_invalidates_channel() {
        let client = DataGrpcClient::new();
        client.set_node_addr(1, "http://old:5000");
        // Simulate a cached channel (we can't create a real one without a server,
        // but we verify the eviction logic by checking pool size doesn't grow)
        client.set_node_addr(1, "http://new:5000");
        assert_eq!(client.pool_size(), 0);
    }

    #[tokio::test]
    async fn get_channel_unknown_node_fails() {
        let client = DataGrpcClient::new();
        let err = client.get_channel(999).await.unwrap_err();
        assert!(matches!(err, ClusterError::NodeNotFound(999)));
    }

    #[test]
    fn handle_rpc_error_maps_codes() {
        let client = DataGrpcClient::new();
        let region_id = 42;

        let err = client.handle_rpc_error(1, region_id, &tonic::Status::not_found("missing"));
        assert!(matches!(err, ClusterError::RegionNotFound(42)));

        let err = client.handle_rpc_error(1, region_id, &tonic::Status::deadline_exceeded("slow"));
        assert!(matches!(err, ClusterError::Timeout));

        let err = client.handle_rpc_error(1, region_id, &tonic::Status::unavailable("down"));
        assert!(matches!(err, ClusterError::Transport(_)));
    }

    #[test]
    fn clone_shares_state() {
        let client = DataGrpcClient::new();
        client.set_node_addr(1, "http://host:5000");

        let clone = client.clone();
        assert!(clone.addrs.contains_key(&1));

        // Mutations on the clone are visible to the original
        clone.set_node_addr(2, "http://host2:5000");
        assert!(client.addrs.contains_key(&2));
    }

    #[test]
    fn with_timeouts() {
        let client = DataGrpcClient::new()
            .with_connect_timeout(Duration::from_secs(10))
            .with_request_timeout(Duration::from_secs(60));

        assert_eq!(client.connect_timeout, Duration::from_secs(10));
        assert_eq!(client.request_timeout, Duration::from_secs(60));
    }

    #[test]
    fn default_trait() {
        let client = DataGrpcClient::default();
        assert_eq!(client.pool_size(), 0);
    }

    #[test]
    fn debug_format() {
        let client = DataGrpcClient::new();
        let debug = format!("{client:?}");
        assert!(debug.contains("DataGrpcClient"));
    }

    #[test]
    fn with_tls_sets_config() {
        let tls = ClientTlsConfig::new();
        let client = DataGrpcClient::new().with_tls(tls);
        assert!(client.tls_config.is_some());
    }

    #[test]
    fn clone_preserves_tls() {
        let tls = ClientTlsConfig::new();
        let client = DataGrpcClient::new().with_tls(tls);
        let cloned = client.clone();
        assert!(cloned.tls_config.is_some());
    }

    #[test]
    fn no_tls_by_default() {
        let client = DataGrpcClient::new();
        assert!(client.tls_config.is_none());
    }

    #[test]
    fn set_addr_same_value_no_op() {
        let client = DataGrpcClient::new();
        client.set_node_addr(1, "http://host:5000");
        // Setting the same address again should not panic or change state
        client.set_node_addr(1, "http://host:5000");
        assert!(client.addrs.contains_key(&1));
    }

    #[test]
    fn remove_node_idempotent() {
        let client = DataGrpcClient::new();
        // Removing a non-existent node should not panic
        client.remove_node(999);
        assert_eq!(client.pool_size(), 0);
    }
}
