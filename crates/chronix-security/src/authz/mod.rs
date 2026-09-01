//! # Chronix Authz — Cedar-based Authorization
//!
//! Fine-grained authorization for the Chronix time-series database using
//! the [Cedar](https://www.cedarpolicy.com/) policy language (formally
//! verified, pure Rust).
//!
//! ## Architecture
//!
//! ```text
//! Request → AuthMiddleware → AuthContext
//!                                │
//!                                ▼
//!                         AuthzEngine (Cedar)
//!                            ├── PolicySet (hot-reloadable)
//!                            ├── Entities (principal, resource, roles)
//!                            └── Decision: Allow | Deny { reasons }
//! ```
//!
//! ## Policy Model
//!
//! - **Principals:** `Chronix::User` with role membership via `Chronix::Role`
//! - **Actions:** `Chronix::Action::{ Write, Read, Delete, Admin, ... }`
//! - **Resources:** `Chronix::Measurement` with name and optional tags
//!
//! ## Default-Deny
//!
//! If no policy explicitly permits a request, it is denied. Use `forbid`
//! policies to create hard denials that override permissive policies.
//!
//! ## Example
//!
//! ```no_run
//! use chronix_security::authz::{AuthzEngine, ChronixAction, ChronixPrincipal, ChronixResource};
//!
//! let engine = AuthzEngine::new();
//! engine.load_policies(r#"
//!     permit(
//!         principal in Chronix::Role::"admin",
//!         action,
//!         resource
//!     );
//! "#).unwrap();
//!
//! let admin = ChronixPrincipal::new("alice").with_role("admin");
//! let resource = ChronixResource::measurement("cpu");
//! let decision = engine.authorize(&admin, ChronixAction::Write, &resource);
//! assert!(decision.is_allowed());
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod engine;
pub mod error;
mod model;
#[cfg(test)]
mod perf;

pub use engine::{AuthzEngine, PolicyVersion};
pub use error::AuthzError;
pub use model::{ChronixAction, ChronixNamespace, ChronixPrincipal, ChronixResource, Decision};
