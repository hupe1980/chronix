//! Error types for the signal system.

use thiserror::Error;

/// Errors that can occur in the signal system.
#[derive(Debug, Error)]
pub enum SignalError {
    /// Invalid trigger configuration.
    #[error("Invalid trigger config: {0}")]
    InvalidConfig(String),
    /// Trigger not found.
    #[error("Trigger not found: {0}")]
    NotFound(String),
    /// Duplicate trigger ID.
    #[error("Duplicate trigger ID: {0}")]
    Duplicate(String),
    /// JSON serialization / deserialization failed.
    #[error("Serialization error: {source}")]
    Serialization {
        /// The underlying serde_json error.
        #[source]
        source: serde_json::Error,
    },
    /// HTTP request to a webhook endpoint failed (network / TLS / timeout).
    #[error("Webhook request to {url} failed: {source}")]
    WebhookRequest {
        /// Target URL.
        url: String,
        /// The underlying reqwest error.
        #[source]
        source: reqwest::Error,
    },
    /// Webhook returned a non-success HTTP status code.
    #[error("Webhook {url} returned HTTP {status}: {body}")]
    WebhookStatus {
        /// Target URL.
        url: String,
        /// HTTP status code.
        status: u16,
        /// Response body (may be truncated).
        body: String,
    },
    /// Generic delivery failure (for custom / third-party channels).
    #[error("Delivery error: {0}")]
    Delivery(String),
    /// Internal error.
    #[error("Signal system error: {0}")]
    Internal(String),
}

/// Result alias for signal operations.
pub type Result<T> = std::result::Result<T, SignalError>;
