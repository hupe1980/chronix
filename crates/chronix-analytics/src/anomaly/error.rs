//! Error types for anomaly detection.

use thiserror::Error;

/// Errors produced by anomaly detectors.
#[derive(Debug, Error)]
pub enum AnomalyError {
    /// Insufficient data for the requested operation.
    #[error("insufficient data: need at least {min}, got {got}")]
    InsufficientData {
        /// Minimum number of points required.
        min: usize,
        /// Actual number of points provided.
        got: usize,
    },

    /// Invalid input data or parameter.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Threshold value is out of range.
    #[error("invalid threshold: {0}")]
    InvalidThreshold(String),

    /// Detector has not been fitted yet.
    #[error("detector not fitted — call fit() before detect()")]
    NotFitted,

    /// Underlying forecast model error.
    #[error("forecast error: {0}")]
    Forecast(String),
}
