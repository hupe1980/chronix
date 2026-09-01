//! Error types for the Chronix authorization system.

use thiserror::Error;

/// Errors that can occur during authorization operations.
#[derive(Debug, Error)]
pub enum AuthzError {
    /// A policy failed to parse.
    #[error("Policy parse error: {0}")]
    PolicyParse(String),
    /// A Cedar schema failed to parse.
    #[error("Schema parse error: {0}")]
    SchemaParse(String),
    /// Policy validation against schema failed.
    #[error("Policy validation error: {0}")]
    PolicyValidation(String),
    /// An I/O error occurred loading policies.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// An internal error.
    #[error("Internal authorization error: {0}")]
    Internal(String),
}

/// Result type alias for authorization operations.
pub type Result<T> = std::result::Result<T, AuthzError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = AuthzError::PolicyParse("bad syntax".into());
        assert!(e.to_string().contains("bad syntax"));
    }
}
