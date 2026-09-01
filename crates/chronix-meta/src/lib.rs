//! # Chronix Meta — Raft-Replicated Cluster Metadata
//!
//! Distributed metadata store for Chronix cluster coordination, built on
//! [OpenRaft](https://docs.rs/openraft). Manages schema registry, routing
//! tables, node membership, and analytics model metadata.
//!
//! ## Architecture
//!
//! ```text
//! MetaNode cluster (3 or 5 nodes)
//!   ┌──────────┐   ┌──────────┐   ┌──────────┐
//!   │ MetaNode │◄──│ MetaNode │──►│ MetaNode │
//!   │ (leader) │   │(follower)│   │(follower)│
//!   └────┬─────┘   └──────────┘   └──────────┘
//!        │
//!        ▼
//!   MetaStateMachine
//!     ├── Schema registry
//!     ├── Routing table (region → DataNode)
//!     ├── Node membership
//!     └── Model catalog
//! ```
//!
//! Every metadata mutation is replicated via Raft consensus — the state
//! machine is deterministic so all replicas converge to the same state.

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

/// gRPC-based MetaNode admin API — server and client.
pub mod admin;
/// Durable Raft log store backed by redb — used by both Meta and Region Raft.
pub mod durable_log_store;
mod error;
mod network;
mod routing;
mod state_machine;
mod store;
mod types;

/// gRPC-based Raft transport for multi-process clusters.
pub mod grpc_transport;

pub use admin::{GrpcMetaClient, MetaAdminServer};
pub use durable_log_store::DurableLogStore;
pub use error::{MetaError, Result};
pub use grpc_transport::{
    assemble_snapshot_chunks, chunk_snapshot, GrpcNetwork, GrpcNetworkFactory, NodeAddressMap,
    RaftGrpcServer, SNAPSHOT_CHUNK_SIZE,
};
pub use network::{MetaNetwork, MetaNetworkFactory, MetaRouter};
pub use routing::{RouteEntry, RoutingSnapshot, RoutingTable};
pub use state_machine::{MetaSnapshot, MetaStateMachine};
pub use store::{MetaLogStore, MetaLogStoreKind, MetaRaft, MetaSmStore, MetaStore, MetaTypeConfig};
pub use types::{
    ClusterConfig, DataNodeInfo, FieldType, KeyRange, MeasurementSchema, MetaCommand, MetaResponse,
    NodeId, NodeMode, NodeState, RegionId, RegionInfo, RegionState,
};
