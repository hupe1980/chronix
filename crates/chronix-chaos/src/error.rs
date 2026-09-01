//! Error types for the chaos testing framework.

use thiserror::Error;

/// Errors that can occur during chaos operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ChaosError {
    /// The requested fault injection was not found.
    #[error("injection not found: {0}")]
    NotFound(u64),

    /// A fault injection with the same ID already exists.
    #[error("duplicate injection: {0}")]
    Duplicate(u64),

    /// The fault configuration is invalid.
    #[error("invalid fault config: {0}")]
    InvalidConfig(String),

    /// Maximum concurrent faults exceeded.
    #[error("max concurrent faults ({0}) exceeded")]
    MaxFaultsExceeded(usize),
}
