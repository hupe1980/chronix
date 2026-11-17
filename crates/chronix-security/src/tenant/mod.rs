//! Multi-tenant namespace isolation and quota enforcement for Chronix.
//!
//! Provides tenant namespace management, per-namespace resource quotas,
//! and usage tracking. Each namespace encapsulates its own measurements,
//! schemas, models, and storage, preventing cross-tenant interference.
//!
//! # Where quotas are enforced
//!
//! Not in this module. The registry holds each namespace's configuration;
//! the database admits every new series against the namespace's series and
//! measurement limits (pushed to it with `Chronix::set_namespace_limits`) in
//! the same step, and from the same exact set, as its cardinality budget;
//! the server applies the ingestion and request rates on every protocol.
//!
//! # Quick Start
//!
//! ```no_run
//! use chronix_security::tenant::NamespaceRegistry;
//! use chronix_core::{NamespaceId, NamespaceQuota};
//!
//! let registry = NamespaceRegistry::new();
//! let id = NamespaceId::new("team-platform").unwrap();
//! let quota = NamespaceQuota {
//!     max_series_count: 500_000,
//!     max_ingestion_rate: 50_000,
//!     max_measurements: 200,
//!     max_request_rps: 0,
//!     max_request_burst: 0,
//! };
//! registry.create_namespace(id, "Platform team", "admin", quota).unwrap();
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod error;
pub mod quota;
pub mod registry;

pub use error::{Result, TenantError};
pub use quota::{record_usage_ratio, usage_ratio};
pub use registry::{NamespaceInfo, NamespaceRegistry, NamespaceState};
