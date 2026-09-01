//! Deterministic simulation testing framework for Chronix.
//!
//! Provides controlled, reproducible testing of distributed system properties
//! under fault injection. Inspired by FoundationDB's simulation testing and
//! Jepsen's linearizability verification.
//!
//! # Architecture
//!
//! - **`VirtualClock`** — Controlled time advancement with optional skew injection
//! - **`SimNetwork`** — Simulated network with partition, delay, and message reorder
//! - **`SimCluster`** — In-process multi-node Raft cluster with full lifecycle control
//! - **`Linearizer`** — History-based linearizability checker (Wing & Gong algorithm)
//! - **`Invariant`** — Property-based invariants verified throughout simulation
//!
//! # Usage
//!
//! ```ignore
//! let mut sim = SimCluster::new(SimConfig { nodes: 3, seed: 42 });
//! sim.start().await;
//! sim.inject_partition(&[1], &[2, 3]);
//! sim.propose_write(/* ... */).await;
//! sim.heal_partition();
//! sim.check_invariants();
//! ```

#![deny(clippy::all, missing_docs, unsafe_code)]

mod checker;
mod clock;
mod cluster;
mod invariants;
mod network;

pub use checker::{HistoryEntry, Linearizer, OpKind};
pub use clock::VirtualClock;
pub use cluster::{SimCluster, SimConfig};
pub use invariants::{Invariant, InvariantChecker, InvariantError};
pub use network::{NetworkAction, SimNetwork};
