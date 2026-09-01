//! # Authentication
//!
//! Authentication and encryption library for the Chronix time-series database.
//!
//! ## Features
//!
//! - **API key authentication** — Argon2-hashed keys with optional expiry
//! - **JWT/OIDC authentication** — RS256/HS256 token validation with configurable claims
//! - **mTLS authentication** — Client certificate identity extraction
//! - **Encryption at rest** — AES-256-GCM authenticated encryption with key rotation
//! - **Unified auth context** — Common `AuthContext` across all methods
//!
//! ## Architecture
//!
//! ```text
//! Request → AuthMiddleware → [mTLS → JWT → API Key] → AuthContext
//!                                                       ├── principal
//!                                                       ├── method
//!                                                       └── claims
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod api_key;
pub mod encryption;
pub mod error;
pub mod jwt;
pub mod middleware;
pub mod mtls;
pub mod oidc;
pub mod rotation;

/// Fill `dest` from the operating system's random source.
///
/// One definition for every secret this crate generates — nonces, data keys,
/// API keys and password salts. `rand` 0.10 renamed the OS generator to
/// `SysRng` and made it fallible, and a failure here means the kernel has no
/// entropy source: there is no safe fallback, so it panics rather than
/// returning a key nobody should trust.
pub(crate) fn fill_random(dest: &mut [u8]) {
    use rand::TryRng;
    rand::rngs::SysRng
        .try_fill_bytes(dest)
        .expect("the operating system random source must be available");
}

pub use api_key::ApiKeyStore;
pub use encryption::{EncryptionService, KeyProvider, SecretKey};
pub use error::AuthError;
pub use jwt::JwtValidator;
pub use middleware::{AuthContext, AuthMethod, AuthMiddleware};
pub use mtls::MtlsValidator;
pub use oidc::{CachedKey, JwksCache};
pub use rotation::{RotatingKeyProvider, RotationEvent, RotationPolicy, RotationReason};
