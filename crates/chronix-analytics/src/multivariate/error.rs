//! Error types for multivariate analysis.

use thiserror::Error;

/// Errors produced by multivariate analysis operations.
#[derive(Debug, Error)]
pub enum MultivariateError {
    /// Too few observations to perform the analysis.
    #[error("insufficient data: need at least {min}, got {got}")]
    InsufficientData {
        /// Minimum number of data points required.
        min: usize,
        /// Number of data points actually provided.
        got: usize,
    },

    /// Series lengths or matrix dimensions do not match.
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected dimension size.
        expected: usize,
        /// Actual dimension size.
        got: usize,
    },

    /// The derived-series dependency graph contains a cycle.
    #[error("cycle detected in derived series dependency graph")]
    CycleDetected,

    /// The requested series name was not found.
    #[error("series not found: {0}")]
    SeriesNotFound(String),

    /// The covariance matrix is singular (data may be collinear).
    #[error("singular covariance matrix — data may be collinear")]
    SingularMatrix,

    /// A user-supplied parameter is invalid.
    #[error("invalid parameter: {0}")]
    InvalidParameter(String),

    /// A general computation error.
    #[error("compute error: {0}")]
    Compute(String),

    /// The model has not been fitted yet.
    #[error("not fitted — call fit() before predict/detect")]
    NotFitted,
}
