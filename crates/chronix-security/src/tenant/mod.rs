//! Multi-tenant namespace isolation and quota enforcement for Chronix.
//!
//! Provides tenant namespace management, per-namespace resource quotas,
//! and usage tracking. Each namespace encapsulates its own measurements,
//! schemas, models, and storage, preventing cross-tenant interference.
//!
//! # Architecture
//!
//! ```text
//! ┌───────────────────────────────────────────────┐
//! │                 HTTP Layer                     │
//! │  X-Namespace header or /api/v1/ns/{ns}/...    │
//! └───────────────────┬───────────────────────────┘
//!                     │
//!          ┌──────────▼──────────┐
//!          │  QuotaEnforcer      │
//!          │  check_and_increment_write() │
//!          │  check_measurement()│
//!          └──────────┬──────────┘
//!                     │
//!          ┌──────────▼──────────┐
//!          │  NamespaceRegistry  │
//!          │  DashMap<ns, state> │
//!          │  CRUD + usage       │
//!          └─────────────────────┘
//! ```
//!
//! # Quick Start
//!
//! ```no_run
//! use chronix_security::tenant::{NamespaceRegistry, QuotaEnforcer};
//! use chronix_core::{NamespaceId, NamespaceQuota};
//!
//! let registry = NamespaceRegistry::new();
//!
//! // Create a namespace with custom quotas
//! let id = NamespaceId::new("team-platform").unwrap();
//! let quota = NamespaceQuota {
//!     max_series_count: 500_000,
//!     max_ingestion_rate: 50_000,
//!     max_storage_bytes: 50 * 1024 * 1024 * 1024,
//!     max_measurements: 200,
//!     max_request_rps: 0,
//!     max_request_burst: 0,
//! };
//! registry.create_namespace(id, "Platform team", "admin", quota).unwrap();
//!
//! // Enforce quotas on writes (atomic check + increment)
//! let enforcer = QuotaEnforcer::new(&registry);
//! enforcer.check_and_increment_write("team-platform", 10, 1000).unwrap();
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod error;
pub mod quota;
pub mod registry;

pub use error::{Result, TenantError};
pub use quota::{QuotaEnforcer, QuotaResource};
pub use registry::{NamespaceInfo, NamespaceRegistry, NamespaceState, UsageRatio};
