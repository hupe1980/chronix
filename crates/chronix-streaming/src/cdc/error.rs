//! Error types for the streaming CDC crate.

use thiserror::Error;

/// Errors from the CDC streaming system.
#[derive(Debug, Error)]
pub enum StreamError {
    /// Event bus has been closed.
    #[error("event bus closed")]
    BusClosed,

    /// Subscription was cancelled or the bus was dropped.
    #[error("subscription ended")]
    SubscriptionEnded,

    /// Invalid aggregation configuration.
    #[error("invalid aggregation config: {0}")]
    InvalidConfig(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
