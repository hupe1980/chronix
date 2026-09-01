//! Error types for the analytics system.

use thiserror::Error;

/// Errors that can occur in the analytics system.
#[derive(Debug, Error)]
pub enum AnalyticsError {
    /// Configuration error.
    #[error("Analytics config error: {0}")]
    Config(String),
    /// Anomaly detection error.
    #[error("Anomaly detection error: {0}")]
    Anomaly(#[from] crate::anomaly::AnomalyError),
    /// Forecast error.
    #[error("Forecast error: {0}")]
    Forecast(#[from] crate::forecast::ForecastError),
    /// Series not found / not configured.
    #[error("Series not configured: {0}")]
    NotConfigured(String),
    /// Duplicate configuration.
    #[error("Duplicate configuration: {0}")]
    Duplicate(String),
    /// Internal error.
    #[error("Analytics internal error: {0}")]
    Internal(String),
}

/// Result alias for analytics operations.
pub type Result<T> = std::result::Result<T, AnalyticsError>;
