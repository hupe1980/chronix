//! Error types for the metadata store.

use thiserror::Error;

/// Errors from the metadata store.
#[derive(Debug, Error)]
pub enum MetaError {
    /// A Raft operation failed.
    #[error("raft error: {0}")]
    Raft(String),

    /// Serialization / deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// The resource already exists.
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// Invalid configuration or request.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),

    /// Node is not the current leader.
    #[error("not leader: leader is node {leader_id:?}")]
    NotLeader {
        /// Current leader node ID, if known.
        leader_id: Option<NodeId>,
    },
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, MetaError>;

use crate::NodeId;
