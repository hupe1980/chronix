//! # Chronix Cluster — `DataNode` Lifecycle & Region Management
//!
//! Peer-to-peer replication, distributed queries, and automatic failover
//! for Chronix data regions. Built on top of [`chronix_meta`] for
//! cluster-wide metadata coordination.
//!
//! ## Key components
//!
//! - **[`DataGrpcServer`]** — region-level Write / Query / Replicate gRPC service
//! - **[`DataGrpcClient`]** — connection-pooled gRPC client for remote `DataNode` RPCs
//! - **[`WriteRouter`]** — distributed write routing (hash → region → leader)
//! - **[`QueryRouter`]** — distributed scatter-gather query execution
//! - **[`RegionRaftManager`]** — Multi-Raft per-region quorum replication
//! - **[`RegionMigrator`]** — zero-downtime region migration between `DataNode`s
//! - **[`FailoverManager`]** — automatic failover and self-healing replication
//! - **[`RegionStorage`]** — pluggable storage backend trait (implemented by `chronixd`)
//! - **[`RoutingCache`]** — local cache of the `MetaNode` routing table

#![warn(missing_docs)]
#![deny(unsafe_code)]
#![allow(clippy::module_name_repetitions)]

/// Automatic region scaling — splits and disk-aware rebalancing.
pub mod autoscale;
/// Per-node circuit breaker for write routing.
pub mod circuit_breaker;
/// `MetaClient` trait and in-process / gRPC client implementations.
pub mod client;
/// Cluster coordinator — health checks, rebalance planning.
pub mod coordinator;
/// Connection-pooled gRPC client for remote `DataNode` operations.
pub mod data_client;
/// `DataNode` gRPC service and storage abstraction.
pub mod data_service;
/// Request-ID deduplication cache for write idempotency.
pub mod dedup;
/// Distributed analytics — FORECAST/ANOMALY across cluster.
pub mod distributed_analytics;
/// Cluster error types.
pub mod error;
/// Automatic failover and self-healing replication.
pub mod failover;
/// Prometheus-compatible cluster metrics helpers.
pub mod metrics;
/// `DataNode` registration, heartbeat, and lifecycle management.
pub mod node;
/// Topology-aware replica placement policy.
pub mod placement;
/// Distributed scatter-gather query execution.
pub mod query_router;
/// Local region bookkeeping.
pub mod region;
/// Zero-downtime region migration between `DataNode`s.
pub mod region_migration;
/// Multi-Raft per-region replication.
pub mod region_raft;
/// Routing cache — local copy of the `MetaNode` routing table.
pub mod routing_cache;
/// Distributed write routing across region leaders.
pub mod write_router;

#[cfg(test)]
pub(crate) mod test_util;

pub use autoscale::{
    build_leader_map, estimate_disk_usage, AutoScaleConfig, AutoScaler, ClusterSnapshot,
    NodeDiskUsage, PeriodicRebalancer, RebalanceResult, RegionMetrics, ScaleAssessment, SplitPhase,
    SplitPlan, SplitReason, SplitResult,
};
pub use client::{GrpcMetaClientAdapter, InProcessMetaClient, MetaClient};
pub use coordinator::{ClusterCoordinator, HealthCheckResult, RegionMigration};
pub use data_client::DataGrpcClient;
pub use data_service::{
    core_to_proto_point, proto_to_core_point, DataGrpcServer, RegionQuery, RegionStorage,
};
pub use distributed_analytics::{
    DistributedAnalytics, DistributedAnomalyRequest, DistributedAnomalyResult,
    DistributedForecastRequest, DistributedForecastResult,
};
pub use error::{ClusterError, Result};
pub use failover::{FailoverCheckResult, FailoverConfig, FailoverManager, UnderReplicatedRegion};
pub use metrics::{
    increment_circuit_breaker_open, increment_leader_changes, record_heartbeat_latency,
    record_query_latency, record_replication_lag, record_write_latency, set_nodes_total,
    set_regions_total, set_under_replicated, CIRCUIT_BREAKER_OPEN_TOTAL, HEARTBEAT_LATENCY,
    LEADER_CHANGES, NODES_TOTAL, QUERY_LATENCY, REGIONS_TOTAL, REPLICATION_LAG, UNDER_REPLICATED,
    WRITE_LATENCY,
};
pub use node::DataNodeManager;
pub use placement::PlacementPolicy;
pub use query_router::{DistributedQuery, QueryResult, QueryRouter, ReadConsistency};
pub use region::{LocalRegion, RegionManager};
pub use region_migration::{
    MigrationPhase, MigrationPlan, MigrationProgress, MigrationResult, MigrationStatus,
    RegionMigrator,
};
pub use region_raft::{
    RegionLogStore, RegionRaft, RegionRaftManager, RegionRaftNetworkFactory, RegionRaftRouter,
    RegionSmStore, RegionTypeConfig, RegionWriteCommand, RegionWriteResponse,
};
pub use routing_cache::RoutingCache;
pub use write_router::{WriteBatchResult, WriteRouter};
