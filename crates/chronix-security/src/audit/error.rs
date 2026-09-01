//! Error types for the audit logging system.

use thiserror::Error;

/// Errors that can occur in the audit logging system.
#[derive(Debug, Error)]
pub enum AuditError {
    /// I/O error (file-based sink).
    #[error("Audit I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Serialization error.
    #[error("Audit serialization error: {0}")]
    Serialization(String),
    /// Configuration error.
    #[error("Audit config error: {0}")]
    Config(String),
    /// Internal error.
    #[error("Audit internal error: {0}")]
    Internal(String),
    /// Partial delivery — some sinks succeeded, others failed.
    ///
    /// Returned by strict audit logging when at least one sink fails.
    /// Contains the list of failures so the caller can decide how to
    /// handle the incomplete audit trail.
    #[error(
        "Audit partial delivery: {succeeded} of {total} sinks succeeded, failures: {failures:?}"
    )]
    PartialDelivery {
        /// Number of sinks that succeeded.
        succeeded: usize,
        /// Total number of sinks attempted.
        total: usize,
        /// Sink name and error message for each failure.
        failures: Vec<(String, String)>,
    },
    /// No durable sink configured — audit events will be lost
    /// on process restart.
    #[error("No durable audit sink configured — compliance violation")]
    NoDurableSink,
}

/// Result alias for audit operations.
pub type Result<T> = std::result::Result<T, AuditError>;
