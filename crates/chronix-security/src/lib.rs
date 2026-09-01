#![deny(unsafe_code)]
//! # Chronix Security
//!
//! The consolidated security layer for Chronix:
//!
//! - [`auth`] — authentication: API keys (Argon2), JWT/OIDC (JWKS discovery,
//!   `jti` replay protection), mTLS client certificates, encryption at rest
//!   (AES-256-GCM with pluggable key providers and automated rotation).
//! - [`authz`] — authorization: Cedar-based RBAC + ABAC with default-deny
//!   semantics, policy hot-reload, schema validation, and row-level policies.
//! - [`audit`] — tamper-evident audit trail: HMAC-SHA256 hash-chained records
//!   with pluggable sinks (memory, file, tracing, webhook).
//! - [`tenant`] — multi-tenancy: namespace registry and per-tenant quota
//!   enforcement.
//!
//! All submodules are independent; embedded deployments that need none of
//! this simply never construct the types.

pub mod audit;
pub mod auth;
pub mod authz;
pub mod tenant;
