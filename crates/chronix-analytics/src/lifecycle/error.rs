//! Error types for the lifecycle management system.

use thiserror::Error;

/// Errors from lifecycle management operations.
#[derive(Debug, Error)]
pub enum LifecycleError {
    /// Model or version not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// Invalid configuration or parameter.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// A/B test evaluation error.
    #[error("A/B test error: {0}")]
    ABTest(String),

    /// Drift detection error.
    #[error("Drift detection error: {0}")]
    Drift(String),

    /// Serialization or persistence error.
    #[error("Persistence error: {0}")]
    Persistence(String),
}

/// Result alias for lifecycle operations.
pub type Result<T> = std::result::Result<T, LifecycleError>;
