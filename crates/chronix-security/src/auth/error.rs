//! Authentication errors.

/// Errors that can occur during authentication or encryption operations.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// Invalid or missing API key.
    #[error("invalid api key: {0}")]
    InvalidApiKey(String),

    /// API key has expired.
    #[error("api key expired: {0}")]
    ExpiredApiKey(String),

    /// Invalid JWT token.
    #[error("invalid jwt: {0}")]
    InvalidJwt(String),

    /// JWT token has expired.
    #[error("jwt expired")]
    ExpiredJwt,

    /// Invalid client certificate.
    #[error("invalid client certificate: {0}")]
    InvalidCertificate(String),

    /// No authentication provided.
    #[error("no authentication provided")]
    NoAuth,

    /// Encryption error.
    #[error("encryption error: {0}")]
    Encryption(String),

    /// Decryption error.
    #[error("decryption error: {0}")]
    Decryption(String),

    /// Key not found.
    #[error("key not found: {0}")]
    KeyNotFound(String),

    /// Configuration error.
    #[error("auth config error: {0}")]
    Config(String),
}

impl AuthError {
    /// Returns a generic, client-safe error message that does not
    /// expose internal configuration details.  Use `Display` (the
    /// `#[error(...)]` messages) for server-side logging only.
    #[must_use]
    pub fn client_message(&self) -> &'static str {
        match self {
            Self::InvalidApiKey(_) | Self::InvalidJwt(_) | Self::InvalidCertificate(_) => {
                "invalid credentials"
            }
            Self::ExpiredApiKey(_) | Self::ExpiredJwt => "credentials expired",
            Self::NoAuth => "authentication required",
            Self::Encryption(_) | Self::Decryption(_) | Self::KeyNotFound(_) => "internal error",
            Self::Config(_) => "service misconfigured",
        }
    }

    /// Whether this error should be treated as HTTP 401 (vs 500).
    #[must_use]
    pub fn is_auth_failure(&self) -> bool {
        matches!(
            self,
            Self::InvalidApiKey(_)
                | Self::InvalidJwt(_)
                | Self::InvalidCertificate(_)
                | Self::ExpiredApiKey(_)
                | Self::ExpiredJwt
                | Self::NoAuth
        )
    }
}
