//! Forecast errors.

/// Errors from forecast operations.
#[derive(Debug, thiserror::Error)]
pub enum ForecastError {
    /// The input time-series has fewer observations than the model requires.
    #[error("insufficient data: need at least {min} points, got {got}")]
    InsufficientData {
        /// Minimum number of data points required.
        min: usize,
        /// Number of data points actually provided.
        got: usize,
    },

    /// A user-supplied parameter or input is invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// The optimiser did not converge within the iteration limit.
    #[error("convergence failed after {iterations} iterations")]
    ConvergenceFailed {
        /// Number of iterations completed before giving up.
        iterations: usize,
    },

    /// A model hyper-parameter has an invalid value.
    #[error("invalid parameter: {name} = {value} — {reason}")]
    InvalidParams {
        /// Name of the invalid parameter.
        name: &'static str,
        /// The invalid value that was supplied.
        value: String,
        /// Human-readable explanation of why the value is rejected.
        reason: &'static str,
    },

    /// A computation produced NaN / Inf or similar numeric failure.
    #[error("numerical instability: {0}")]
    NumericalInstability(String),

    /// The model has not been fitted yet.
    #[error("model not fitted — call fit() before predict()")]
    NotFitted,

    /// An error forwarded from the compute engine.
    #[error("compute error: {0}")]
    Compute(#[from] crate::compute::ComputeError),
}
