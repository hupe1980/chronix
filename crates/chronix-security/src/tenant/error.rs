//! Tenant-related error types.

use thiserror::Error;

/// Tenant / namespace errors.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TenantError {
    /// Namespace not found.
    #[error("namespace not found: {0}")]
    NamespaceNotFound(String),

    /// Namespace already exists.
    #[error("namespace already exists: {0}")]
    NamespaceAlreadyExists(String),

    /// A namespace quota has been exceeded.
    ///
    /// # Information Disclosure
    ///
    /// The error message deliberately includes `current` and `limit`
    /// values. This is acceptable because:
    ///
    /// The error is only returned to **authenticated, namespace-scoped**
    ///   callers who already have write access and can trivially infer
    ///   their own usage.
    /// Operators need these values for capacity-planning and debugging.
    /// The HTTP layer maps this to a 429 response with a generic
    ///   message; the detailed fields are only visible in server logs
    ///   and metrics (not forwarded to the end-user response body).
    #[error("quota exceeded for namespace '{namespace}': {resource} ({current}/{limit})")]
    QuotaExceeded {
        /// The namespace that exceeded the quota.
        namespace: String,
        /// Which resource hit the limit (e.g. `series_count`).
        resource: String,
        /// Current usage value.
        current: u64,
        /// Configured limit.
        limit: u64,
    },

    /// Invalid namespace configuration.
    #[error("invalid namespace config: {0}")]
    InvalidConfig(String),

    /// Schema validation error from chronix-core.
    #[error("schema error: {0}")]
    Schema(#[from] chronix_core::SchemaError),

    /// The registry could not be persisted.
    ///
    /// Its own variant because it is the **server's** fault and every other
    /// variant here is the caller's. They shared `InvalidConfig`, so a full
    /// disk reached an operator as `400 invalid namespace config` — a
    /// message that sends them to re-read the request body while the
    /// problem is the volume.
    #[error("cannot persist the namespace registry: {0}")]
    Persist(String),
}

/// A specialised `Result` type for tenant operations.
pub type Result<T> = std::result::Result<T, TenantError>;
