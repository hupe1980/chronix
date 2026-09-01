//! Chaos testing and fault injection framework for Chronix.
//!
//! Provides controlled failure injection to validate cluster resilience
//! under adverse conditions. All injections are time-limited and can be
//! managed via the admin API.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐
//! │   ChaosAgent    │ ← One per DataNode
//! │  ┌────────────┐ │
//! │  │ Injections │ │ ← Active fault registry
//! │  └────────────┘ │
//! │  ┌────────────┐ │
//! │  │ FaultGuard │ │ ← RAII auto-cleanup
//! │  └────────────┘ │
//! └─────────────────┘
//! ```
//!
//! # Quick Start
//!
//! ```no_run
//! use chronix_chaos::{ChaosAgent, Fault, FaultConfig};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! let agent = Arc::new(ChaosAgent::new());
//! let guard = agent.inject(&FaultConfig {
//!     fault: Fault::LatencySpike { delay: Duration::from_millis(500) },
//!     duration: Duration::from_secs(30),
//!     description: "Test slow writes".into(),
//! });
//! assert!(agent.has_active_faults());
//! // Guard auto-clears the fault when dropped.
//! drop(guard);
//! assert!(!agent.has_active_faults());
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod agent;
mod error;
mod fault;

pub use agent::{ChaosAgent, FaultGuard, InjectionInfo};
pub use error::ChaosError;
pub use fault::{Fault, FaultConfig};
