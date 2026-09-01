//! Core types for the metadata store.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

/// Unique identifier for a cluster node.
pub type NodeId = u64;

/// Unique identifier for a data region (partition of a measurement).
pub type RegionId = u64;

// ── Node Info ───────────────────────────────────────────────────────

/// Runtime state of a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is healthy and sending heartbeats.
    Active,
    /// Heartbeat missed — might be transient.
    Suspect,
    /// Multiple heartbeats missed — considered dead.
    Dead,
    /// Node is shutting down gracefully.
    Decommissioning,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspect => write!(f, "suspect"),
            Self::Dead => write!(f, "dead"),
            Self::Decommissioning => write!(f, "decommissioning"),
        }
    }
}

/// Mode a node can run in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeMode {
    /// Hosts metadata Raft group.
    Meta,
    /// Hosts data regions.
    Data,
    /// Routes queries to data nodes.
    Query,
}

impl fmt::Display for NodeMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Meta => write!(f, "meta"),
            Self::Data => write!(f, "data"),
            Self::Query => write!(f, "query"),
        }
    }
}

/// Information about a registered data node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataNodeInfo {
    /// The node's unique ID.
    pub node_id: NodeId,
    /// gRPC address for inter-node communication.
    pub grpc_addr: String,
    /// Node mode (always `Data` for data nodes).
    pub mode: NodeMode,
    /// Current lifecycle state.
    pub state: NodeState,
    /// Capacity: available disk bytes.
    pub disk_bytes: u64,
    /// Capacity: available memory bytes.
    pub memory_bytes: u64,
    /// Capacity: number of CPU cores.
    pub cpu_cores: u32,
    /// Monotonic heartbeat generation.
    pub heartbeat_generation: u64,
    /// Last heartbeat timestamp (Unix epoch seconds).
    pub last_heartbeat_secs: u64,
    /// Regions hosted by this node.
    pub region_ids: Vec<RegionId>,
    /// Topology labels for geo-aware replica placement.
    ///
    /// Typical keys: `"rack"`, `"zone"`, `"datacenter"`, `"region"`.
    /// The coordinator's placement policy uses these labels to spread
    /// replicas across failure domains (e.g. never place two replicas
    /// in the same rack).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl DataNodeInfo {
    /// Create a new `DataNodeInfo`.
    #[must_use]
    pub fn new(node_id: NodeId, grpc_addr: impl Into<String>) -> Self {
        Self {
            node_id,
            grpc_addr: grpc_addr.into(),
            mode: NodeMode::Data,
            state: NodeState::Active,
            disk_bytes: 0,
            memory_bytes: 0,
            cpu_cores: 0,
            heartbeat_generation: 0,
            last_heartbeat_secs: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs(),
            region_ids: Vec::new(),
            labels: BTreeMap::new(),
        }
    }

    /// Set the node mode.
    #[must_use]
    pub fn with_mode(mut self, mode: NodeMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set capacity fields.
    #[must_use]
    pub fn with_capacity(mut self, disk_bytes: u64, memory_bytes: u64, cpu_cores: u64) -> Self {
        self.disk_bytes = disk_bytes;
        self.memory_bytes = memory_bytes;
        self.cpu_cores = u32::try_from(cpu_cores).unwrap_or(u32::MAX);
        self
    }

    /// Set topology labels for geo-aware placement.
    #[must_use]
    pub fn with_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels = labels;
        self
    }
}

// ── Region Info ─────────────────────────────────────────────────────

/// Lifecycle state of a region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionState {
    /// Region is accepting reads and writes.
    Active,
    /// Region is being migrated to another node.
    Migrating,
    /// Region is read-only (source of a migration).
    ReadOnly,
    /// Region is being re-replicated after a node failure.
    Replicating,
    /// Region is being split — still serves reads and writes
    /// while the two child regions are being prepared.
    Splitting,
}

impl RegionState {
    /// Whether the region should accept read requests in this state.
    #[must_use]
    pub fn accepts_reads(&self) -> bool {
        matches!(
            self,
            Self::Active | Self::Splitting | Self::Migrating | Self::Replicating | Self::ReadOnly
        )
    }

    /// Whether the region should accept write requests in this state.
    #[must_use]
    pub fn accepts_writes(&self) -> bool {
        matches!(self, Self::Active | Self::Splitting)
    }
}

impl fmt::Display for RegionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Migrating => write!(f, "migrating"),
            Self::ReadOnly => write!(f, "read-only"),
            Self::Replicating => write!(f, "replicating"),
            Self::Splitting => write!(f, "splitting"),
        }
    }
}

/// Contiguous hash-key range owned by a region.
///
/// The range is **start-inclusive, end-exclusive**: `[start, end)`.
/// The full key space is `[0, u64::MAX]`.  When a region is split,
/// the midpoint becomes the boundary of the two child ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRange {
    /// Inclusive lower bound of the hash range.
    pub start: u64,
    /// Exclusive upper bound of the hash range.
    pub end: u64,
}

impl KeyRange {
    /// Create a new key range.
    #[must_use]
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Full key space: `[0, u64::MAX)`.
    #[must_use]
    pub fn full() -> Self {
        Self {
            start: 0,
            end: u64::MAX,
        }
    }

    /// Whether `hash` falls within this range (`start <= hash < end`).
    #[must_use]
    pub fn contains(&self, hash: u64) -> bool {
        hash >= self.start && hash < self.end
    }

    /// Midpoint for splitting.  Returns `None` when the range contains
    /// fewer than 2 values (i.e. `end <= start + 1`) because such a
    /// range cannot be split into two non-empty children.
    #[must_use]
    pub fn midpoint(&self) -> Option<u64> {
        if self.end <= self.start.saturating_add(1) {
            return None;
        }
        Some(self.start / 2 + self.end / 2 + (self.start % 2 + self.end % 2) / 2)
    }

    /// Split into two non-empty child ranges at the midpoint.
    ///
    /// Returns `None` when the range is too small to split (fewer than
    /// 2 values).
    #[must_use]
    pub fn split(&self) -> Option<(Self, Self)> {
        let mid = self.midpoint()?;
        Some((
            Self {
                start: self.start,
                end: mid,
            },
            Self {
                start: mid,
                end: self.end,
            },
        ))
    }
}

impl fmt::Display for KeyRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {})", self.start, self.end)
    }
}

/// Information about a data region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionInfo {
    /// The region's unique ID.
    pub region_id: RegionId,
    /// Measurement this region belongs to.
    pub measurement: String,
    /// The leader node for this region's Raft group.
    pub leader_node_id: NodeId,
    /// All replica node IDs (includes leader).
    pub replica_node_ids: Vec<NodeId>,
    /// Current lifecycle state.
    pub state: RegionState,
    /// Replication factor for this region.
    pub replication_factor: u32,
    /// Hash-key range this region owns.  `None` for legacy regions
    /// that have not yet been assigned a range (modulo fallback).
    #[serde(default)]
    pub key_range: Option<KeyRange>,
}

impl RegionInfo {
    /// Create a new `RegionInfo`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        region_id: RegionId,
        measurement: impl Into<String>,
        leader_node_id: NodeId,
        replica_node_ids: Vec<NodeId>,
    ) -> Self {
        let replication_factor = replica_node_ids.len().max(1) as u32;
        Self {
            region_id,
            measurement: measurement.into(),
            leader_node_id,
            replica_node_ids,
            state: RegionState::Active,
            replication_factor,
            key_range: None,
        }
    }

    /// Set the key range for this region.
    #[must_use]
    pub fn with_key_range(mut self, range: KeyRange) -> Self {
        self.key_range = Some(range);
        self
    }
}

// ── Measurement Schema ──────────────────────────────────────────────

/// Schema definition for a measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementSchema {
    /// Measurement name.
    pub name: String,
    /// Expected tag key names (informational).
    pub tag_keys: Vec<String>,
    /// Expected field names and types.
    pub field_schemas: BTreeMap<String, FieldType>,
    /// Number of regions this measurement is partitioned into.
    pub region_count: u32,
    /// Replication factor for each region.
    pub replication_factor: u32,
}

impl MeasurementSchema {
    /// Create a new schema with defaults.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tag_keys: Vec::new(),
            field_schemas: BTreeMap::new(),
            region_count: 3,
            replication_factor: 3,
        }
    }

    /// Set the region count.
    #[must_use]
    pub fn with_region_count(mut self, count: u32) -> Self {
        self.region_count = count;
        self
    }

    /// Set the replication factor.
    #[must_use]
    pub fn with_replication_factor(mut self, factor: u32) -> Self {
        self.replication_factor = factor;
        self
    }
}

/// Field type in a measurement schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    /// 64-bit float.
    F64,
    /// 64-bit signed integer.
    I64,
    /// 64-bit unsigned integer.
    U64,
    /// Boolean.
    Bool,
    /// UTF-8 string.
    String,
}

// ── Cluster Config ──────────────────────────────────────────────────

/// Cluster-wide configuration stored in the metadata state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name.
    pub cluster_name: String,
    /// Default replication factor for new measurements.
    pub default_replication_factor: u32,
    /// Default number of regions for new measurements.
    pub default_region_count: u32,
    /// Heartbeat interval in seconds.
    pub heartbeat_interval_secs: u64,
    /// Number of missed heartbeats before suspect.
    pub heartbeat_suspect_threshold: u32,
    /// Number of missed heartbeats before dead.
    pub heartbeat_dead_threshold: u32,
    /// Maximum concurrent re-replications.
    pub max_concurrent_rereplications: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: "chronix".into(),
            default_replication_factor: 3,
            default_region_count: 3,
            heartbeat_interval_secs: 5,
            heartbeat_suspect_threshold: 2,
            heartbeat_dead_threshold: 6,
            max_concurrent_rereplications: 2,
        }
    }
}

// ── Commands & Responses ────────────────────────────────────────────

/// Commands applied to the metadata state machine via Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaCommand {
    /// Create a new measurement schema.
    CreateMeasurement(MeasurementSchema),
    /// Drop a measurement and all its regions.
    DropMeasurement {
        /// Measurement name.
        name: String,
    },
    /// Register a data node in the cluster.
    RegisterNode(DataNodeInfo),
    /// Deregister (remove) a data node.
    DeregisterNode {
        /// Node ID to remove.
        node_id: NodeId,
    },
    /// Record a heartbeat from a data node.
    Heartbeat {
        /// Node ID.
        node_id: NodeId,
        /// Heartbeat generation counter.
        generation: u64,
        /// Epoch seconds.
        timestamp_secs: u64,
    },
    /// Mark a node as suspect or dead.
    UpdateNodeState {
        /// Node ID.
        node_id: NodeId,
        /// New state.
        state: NodeState,
    },
    /// Create a new region and assign it to nodes.
    CreateRegion(RegionInfo),
    /// Update region state (migration, etc.).
    UpdateRegionState {
        /// Region ID.
        region_id: RegionId,
        /// New state.
        state: RegionState,
    },
    /// Update the region leader after Raft election.
    UpdateRegionLeader {
        /// Region ID.
        region_id: RegionId,
        /// New leader node ID.
        leader_node_id: NodeId,
    },
    /// Add a replica to an existing region.
    AddRegionReplica {
        /// Region ID.
        region_id: RegionId,
        /// Node ID to add as a replica.
        node_id: NodeId,
    },
    /// Remove a replica from a region.
    RemoveRegionReplica {
        /// Region ID.
        region_id: RegionId,
        /// Node ID to remove.
        node_id: NodeId,
    },
    /// Atomically migrate a region: add destination replica, switch leader,
    /// remove source replica — all in a single Raft log entry.
    MigrateRegion {
        /// Region ID to migrate.
        region_id: RegionId,
        /// Node currently hosting the leader.
        source_node_id: NodeId,
        /// Node that will become the new leader.
        dest_node_id: NodeId,
    },
    /// Begin a region migration — sets the region state to `Migrating`
    /// and adds the destination node as a replica (learner).
    ///
    /// Must be proposed **before** `MigrateRegion` so the data transfer
    /// happens while the region is in a tracked migration lifecycle.
    BeginMigration {
        /// Region ID to migrate.
        region_id: RegionId,
        /// Source node (must be the current leader).
        source_node_id: NodeId,
        /// Destination node to add as learner replica.
        dest_node_id: NodeId,
    },
    /// Cancel a migration that was begun but not completed.
    ///
    /// Restores the region to `Active`, removes the destination node
    /// from the replica set and the region from the destination node's
    /// region list. This is the rollback path for `BeginMigration`.
    CancelMigration {
        /// Region ID whose migration should be cancelled.
        region_id: RegionId,
        /// Destination node that was added as learner (to remove).
        dest_node_id: NodeId,
    },
    /// Update cluster-wide configuration.
    UpdateClusterConfig(ClusterConfig),
    /// Save analytics model metadata.
    SaveModel {
        /// Measurement the model belongs to.
        measurement: String,
        /// Model identifier.
        model_id: String,
        /// Serialized model metadata (opaque bytes).
        metadata: Vec<u8>,
    },
    /// Batch heartbeat replication.
    ///
    /// Periodically proposed by the leader to replicate out-of-band
    /// heartbeat timestamps through Raft. This ensures that after a
    /// leader failover the new leader's Raft-replicated
    /// `last_heartbeat_secs` is at most one heartbeat interval stale,
    /// preventing false suspect/dead cascades.
    BatchHeartbeat {
        /// `node_id → (generation, timestamp_secs)`.
        entries: BTreeMap<NodeId, (u64, u64)>,
    },
}

/// Response from applying a command to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaResponse {
    /// Command applied successfully.
    Ok,
    /// A resource was created (returns its ID).
    Created {
        /// Description of what was created.
        description: String,
    },
    /// An error occurred.
    Error {
        /// Error message.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_state_display() {
        assert_eq!(NodeState::Active.to_string(), "active");
        assert_eq!(NodeState::Suspect.to_string(), "suspect");
        assert_eq!(NodeState::Dead.to_string(), "dead");
        assert_eq!(NodeState::Decommissioning.to_string(), "decommissioning");
    }

    #[test]
    fn node_mode_display() {
        assert_eq!(NodeMode::Meta.to_string(), "meta");
        assert_eq!(NodeMode::Data.to_string(), "data");
        assert_eq!(NodeMode::Query.to_string(), "query");
    }

    #[test]
    fn region_state_display() {
        assert_eq!(RegionState::Active.to_string(), "active");
        assert_eq!(RegionState::Migrating.to_string(), "migrating");
        assert_eq!(RegionState::ReadOnly.to_string(), "read-only");
        assert_eq!(RegionState::Replicating.to_string(), "replicating");
        assert_eq!(RegionState::Splitting.to_string(), "splitting");
    }

    #[test]
    fn region_state_accepts_reads() {
        assert!(RegionState::Active.accepts_reads());
        assert!(RegionState::Splitting.accepts_reads());
        assert!(RegionState::Migrating.accepts_reads());
        assert!(RegionState::Replicating.accepts_reads());
        assert!(RegionState::ReadOnly.accepts_reads());
    }

    #[test]
    fn region_state_accepts_writes() {
        assert!(RegionState::Active.accepts_writes());
        assert!(RegionState::Splitting.accepts_writes());
        assert!(!RegionState::Migrating.accepts_writes());
        assert!(!RegionState::ReadOnly.accepts_writes());
        assert!(!RegionState::Replicating.accepts_writes());
    }

    #[test]
    fn data_node_new() {
        let node = DataNodeInfo::new(1, "127.0.0.1:9100");
        assert_eq!(node.node_id, 1);
        assert_eq!(node.grpc_addr, "127.0.0.1:9100");
        assert_eq!(node.state, NodeState::Active);
        assert_eq!(node.mode, NodeMode::Data);
        assert!(node.region_ids.is_empty());
    }

    #[test]
    fn region_info_new() {
        let region = RegionInfo::new(42, "cpu", 1, vec![1, 2, 3]);
        assert_eq!(region.region_id, 42);
        assert_eq!(region.measurement, "cpu");
        assert_eq!(region.leader_node_id, 1);
        assert_eq!(region.replica_node_ids, vec![1, 2, 3]);
        assert_eq!(region.replication_factor, 3);
        assert_eq!(region.state, RegionState::Active);
    }

    #[test]
    fn measurement_schema_builder() {
        let schema = MeasurementSchema::new("cpu")
            .with_region_count(5)
            .with_replication_factor(2);
        assert_eq!(schema.name, "cpu");
        assert_eq!(schema.region_count, 5);
        assert_eq!(schema.replication_factor, 2);
    }

    #[test]
    fn cluster_config_defaults() {
        let cfg = ClusterConfig::default();
        assert_eq!(cfg.default_replication_factor, 3);
        assert_eq!(cfg.default_region_count, 3);
        assert_eq!(cfg.heartbeat_interval_secs, 5);
        assert_eq!(cfg.heartbeat_dead_threshold, 6);
        assert_eq!(cfg.max_concurrent_rereplications, 2);
    }

    #[test]
    fn meta_command_serde_roundtrip() {
        let cmd = MetaCommand::CreateMeasurement(MeasurementSchema::new("cpu"));
        let json = serde_json::to_string(&cmd).unwrap();
        let restored: MetaCommand = serde_json::from_str(&json).unwrap();
        match restored {
            MetaCommand::CreateMeasurement(s) => assert_eq!(s.name, "cpu"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn meta_response_serde_roundtrip() {
        let resp = MetaResponse::Created {
            description: "region 1".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let restored: MetaResponse = serde_json::from_str(&json).unwrap();
        match restored {
            MetaResponse::Created { description } => assert_eq!(description, "region 1"),
            _ => panic!("wrong variant"),
        }
    }
}
