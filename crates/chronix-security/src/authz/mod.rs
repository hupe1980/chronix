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
//! - **Principals:** `Chronix::User`, with role membership via `Chronix::Role`
//! - **Resources:** `Chronix::Namespace` for data, `Chronix::System` for
//!   administration — the two things `chronixd` asks about, and nothing else
//! - **Actions:** `Read`, `Write`, `Delete` on a namespace; the capabilities
//!   in the `Admin` group on the system
//!
//! The model is compiled in as a Cedar schema ([`AuthzEngine::SCHEMA_SRC`])
//! and **every policy is validated against it**, so a rule naming something
//! this server never asks about is refused at load rather than accepted and
//! never consulted.
//!
//! ## Default-Deny
//!
//! If no policy explicitly permits a request, it is denied. Use `forbid`
//! policies to create hard denials that override permissive policies.
//!
//! ## Example
//!
//! ```
//! use chronix_security::authz::{
//!     AuthzEngine, ChronixAction, ChronixNamespace, ChronixPrincipal,
//! };
//!
//! let engine = AuthzEngine::new();
//! engine.load_policies(r#"
//!     permit(
//!         principal in Chronix::Role::"operator",
//!         action in [Chronix::Action::"Read", Chronix::Action::"Write"],
//!         resource == Chronix::Namespace::"production"
//!     );
//! "#).unwrap();
//!
//! let operator = ChronixPrincipal::new("alice").with_role("operator");
//! let production = ChronixNamespace::new("production");
//! assert!(engine
//!     .authorize_namespace(&operator, ChronixAction::Write, &production)
//!     .is_allowed());
//! assert!(engine
//!     .authorize_namespace(&operator, ChronixAction::Delete, &production)
//!     .is_denied());
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod engine;
pub mod error;
mod model;
#[cfg(test)]
mod perf;

pub use engine::{schema_action_names, AuthzEngine, SCHEMA_SRC};
pub use error::AuthzError;
pub use model::{ChronixAction, ChronixNamespace, ChronixPrincipal, ChronixSystem, Decision};
