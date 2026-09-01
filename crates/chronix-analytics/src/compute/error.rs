//! Compute engine errors.

/// Errors from compute operations.
#[derive(Debug, thiserror::Error)]
pub enum ComputeError {
    /// Input slices have incompatible lengths.
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected dimension.
        expected: usize,
        /// Actual dimension received.
        got: usize,
    },

    /// Matrix is singular and cannot be inverted/solved.
    #[error("singular matrix: cannot solve linear system")]
    SingularMatrix,

    /// Insufficient data for the requested operation.
    #[error("insufficient data: need at least {min} points, got {got}")]
    InsufficientData {
        /// Minimum number of points required.
        min: usize,
        /// Actual number of points provided.
        got: usize,
    },

    /// Invalid parameter value.
    #[error("invalid parameter: {name} = {value} — {reason}")]
    InvalidParameter {
        /// Parameter name.
        name: &'static str,
        /// Supplied value.
        value: String,
        /// Why the value is invalid.
        reason: &'static str,
    },

    /// Numerical instability detected.
    #[error("numerical instability: {0}")]
    NumericalInstability(String),

    /// Internal error (e.g. thread pool, system resource failure).
    #[error("internal error: {0}")]
    Internal(String),
}
