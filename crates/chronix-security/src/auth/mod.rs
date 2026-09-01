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

pub use api_key::ApiKeyStore;
pub use encryption::{EncryptionService, KeyProvider, SecretKey};
pub use error::AuthError;
pub use jwt::JwtValidator;
pub use middleware::{AuthContext, AuthMethod, AuthMiddleware};
pub use mtls::MtlsValidator;
pub use oidc::{CachedKey, JwksCache};
pub use rotation::{RotatingKeyProvider, RotationEvent, RotationPolicy, RotationReason};
