//! Error types for the cluster crate.

use chronix_meta::MetaError;
use thiserror::Error;

/// Errors that can occur during cluster operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClusterError {
    /// An error propagated from the metadata store.
    #[error(transparent)]
    Meta(#[from] MetaError),

    /// The requested node was not found.
    #[error("node not found: {0}")]
    NodeNotFound(u64),

    /// The requested region was not found.
    #[error("region not found: {0}")]
    RegionNotFound(u64),

    /// The region is not in Active state and cannot accept writes.
    #[error("region {0} is not writable (frozen/migrating)")]
    RegionNotWritable(u64),

    /// This node is not the Raft leader.
    #[error("not leader")]
    NotLeader,

    /// A replication operation failed.
    #[error("replication failed: {0}")]
    ReplicationFailed(String),

    /// An operation timed out.
    #[error("timeout")]
    Timeout,

    /// A gRPC / network transport error (connection failure, RPC error, etc.).
    #[error("transport error: {0}")]
    Transport(String),

    /// A Raft consensus protocol error.
    #[error("raft error: {0}")]
    Raft(String),

    /// The current node is not the Raft leader; the request should be
    /// forwarded to the specified leader node (if known).
    #[error("raft: forward to leader {leader_id:?}")]
    RaftForwardToLeader {
        /// The node ID of the current leader, if known.
        leader_id: Option<u64>,
    },

    /// An analytics / ML operation error (forecast, anomaly detection).
    #[error("analytics error: {0}")]
    Analytics(String),

    /// A data validation error (invalid series key, missing fields, etc.).
    #[error("validation error: {0}")]
    Validation(String),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),

    /// Invalid configuration.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// The service is temporarily unavailable (e.g., circuit breaker open).
    #[error("unavailable: {0}")]
    Unavailable(String),
}

/// Convenience result alias for cluster operations.
pub type Result<T> = std::result::Result<T, ClusterError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_variants() {
        assert_eq!(
            ClusterError::NodeNotFound(42).to_string(),
            "node not found: 42"
        );
        assert_eq!(
            ClusterError::RegionNotFound(7).to_string(),
            "region not found: 7"
        );
        assert_eq!(ClusterError::NotLeader.to_string(), "not leader");
        assert_eq!(
            ClusterError::ReplicationFailed("network".into()).to_string(),
            "replication failed: network"
        );
        assert_eq!(ClusterError::Timeout.to_string(), "timeout");
        assert_eq!(
            ClusterError::Transport("connect failed".into()).to_string(),
            "transport error: connect failed"
        );
        assert_eq!(
            ClusterError::Raft("leader lost".into()).to_string(),
            "raft error: leader lost"
        );
        assert_eq!(
            ClusterError::RaftForwardToLeader { leader_id: Some(3) }.to_string(),
            "raft: forward to leader Some(3)"
        );
        assert_eq!(
            ClusterError::RaftForwardToLeader { leader_id: None }.to_string(),
            "raft: forward to leader None"
        );
        assert_eq!(
            ClusterError::Analytics("fit failed".into()).to_string(),
            "analytics error: fit failed"
        );
        assert_eq!(
            ClusterError::Validation("bad key".into()).to_string(),
            "validation error: bad key"
        );
        assert_eq!(
            ClusterError::Internal("oops".into()).to_string(),
            "internal error: oops"
        );
        assert_eq!(
            ClusterError::InvalidConfig("bad".into()).to_string(),
            "invalid config: bad"
        );
    }

    #[test]
    fn from_meta_error() {
        let meta = MetaError::NotFound("node 1".into());
        let cluster: ClusterError = meta.into();
        assert!(matches!(cluster, ClusterError::Meta(_)));
        assert!(cluster.to_string().contains("node 1"));
    }

    #[test]
    fn result_alias_works() {
        fn produce_ok() -> Result<u32> {
            Ok(42)
        }
        let val = produce_ok().unwrap();
        assert_eq!(val, 42);

        let err: Result<u32> = Err(ClusterError::Timeout);
        assert!(err.is_err());
    }
}
