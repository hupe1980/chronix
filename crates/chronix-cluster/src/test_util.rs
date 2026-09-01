//! Shared test utilities for the cluster crate.
//!
//! Provides reusable mock implementations of [`MetaClient`] and
//! [`RegionStorage`] to avoid duplicating near-identical mocks across
//! test modules.

use parking_lot::Mutex;
use std::collections::BTreeMap;

use async_trait::async_trait;
use chronix_core::Point;
use chronix_meta::{
    DataNodeInfo, MetaCommand, MetaResponse, NodeId, RegionId, RegionInfo, RouteEntry,
    RoutingSnapshot,
};

use crate::client::MetaClient;
use crate::data_service::{RegionQuery, RegionStorage};
use crate::error::Result;

/// A [`MetaClient`] mock that returns a configurable [`RoutingSnapshot`].
///
/// All methods except `get_routing_table` are no-ops that return `Ok`.
/// Use [`MockSnapshotMetaClient::noop`] for a fully inert client that
/// returns an empty routing table.
#[derive(Debug)]
pub(crate) struct MockSnapshotMetaClient {
    /// The routing snapshot to return from `get_routing_table`.
    pub snapshot: Mutex<RoutingSnapshot>,
}

impl MockSnapshotMetaClient {
    /// Create a mock that returns the given snapshot.
    pub fn new(snapshot: RoutingSnapshot) -> Self {
        Self {
            snapshot: Mutex::new(snapshot),
        }
    }

    /// Create a fully inert mock with an empty routing table.
    #[allow(dead_code)]
    pub fn noop() -> Self {
        Self::new(RoutingSnapshot::empty())
    }
}

#[async_trait]
impl MetaClient for MockSnapshotMetaClient {
    async fn register_node(&self, _info: DataNodeInfo) -> Result<()> {
        Ok(())
    }

    async fn deregister_node(&self, _node_id: NodeId) -> Result<()> {
        Ok(())
    }

    async fn heartbeat(&self, _node_id: NodeId, _generation: u64) -> Result<()> {
        Ok(())
    }

    async fn create_region(&self, _info: RegionInfo) -> Result<()> {
        Ok(())
    }

    async fn get_routing_table(&self) -> Result<RoutingSnapshot> {
        Ok(self.snapshot.lock().clone())
    }

    async fn propose(&self, _cmd: MetaCommand) -> Result<MetaResponse> {
        Ok(MetaResponse::Ok)
    }
}

/// Build a [`RoutingSnapshot`] with each leader included as its own single
/// replica — the common pattern used in most cluster tests.
pub(crate) fn make_routing_snapshot(
    measurement: &str,
    routes: Vec<(RegionId, NodeId, &str)>,
) -> RoutingSnapshot {
    let entries: Vec<RouteEntry> = routes
        .into_iter()
        .map(|(rid, nid, addr)| RouteEntry {
            region_id: rid,
            measurement: measurement.to_string(),
            leader_node_id: nid,
            leader_addr: addr.to_string(),
            replica_addrs: vec![(nid, addr.to_string())],
            key_range: None,
            region_state: chronix_meta::RegionState::Active,
        })
        .collect();
    RoutingSnapshot {
        version: 1,
        entries: [(measurement.to_string(), entries)].into_iter().collect(),
    }
}

type RouteSpec<'a> = (RegionId, NodeId, &'a str, Vec<(NodeId, &'a str)>);

/// Build a [`RoutingSnapshot`] with explicit replica lists per route.
pub(crate) fn make_routing_snapshot_with_replicas(
    measurement: &str,
    routes: Vec<RouteSpec<'_>>,
) -> RoutingSnapshot {
    let entries: Vec<RouteEntry> = routes
        .into_iter()
        .map(|(rid, nid, addr, replicas)| RouteEntry {
            region_id: rid,
            measurement: measurement.to_string(),
            leader_node_id: nid,
            leader_addr: addr.to_string(),
            replica_addrs: replicas
                .into_iter()
                .map(|(id, a)| (id, a.to_string()))
                .collect(),
            key_range: None,
            region_state: chronix_meta::RegionState::Active,
        })
        .collect();
    RoutingSnapshot {
        version: 1,
        entries: [(measurement.to_string(), entries)].into_iter().collect(),
    }
}

// ── Shared RegionStorage mocks ─────────────────────────────────────────────

/// A write-log mock that records every [`write_points`] call and stubs
/// `query_region` / `replicate_wal` as no-ops.
///
/// Used by `write_router` and `region_raft` tests that only need to
/// verify writes were dispatched correctly.
#[derive(Debug, Default)]
pub(crate) struct MockWriteStorage {
    /// Append-only log of `(region_id, points)` tuples.
    pub writes: Mutex<Vec<(RegionId, Vec<Point>)>>,
}

impl MockWriteStorage {
    /// Return a clone of all recorded writes.
    #[allow(dead_code)]
    pub fn written_points(&self) -> Vec<(RegionId, Vec<Point>)> {
        self.writes.lock().clone()
    }

    /// Total number of points written across all regions.
    #[allow(dead_code)]
    pub fn total_points_written(&self) -> usize {
        self.writes.lock().iter().map(|(_, pts)| pts.len()).sum()
    }
}

#[async_trait]
impl RegionStorage for MockWriteStorage {
    async fn write_points(&self, region_id: RegionId, points: Vec<Point>) -> Result<u64> {
        let count = points.len() as u64;
        self.writes.lock().push((region_id, points));
        Ok(count)
    }

    async fn query_region(&self, _region_id: RegionId, _query: RegionQuery) -> Result<Vec<Point>> {
        Ok(Vec::new())
    }

    async fn replicate_wal(
        &self,
        _region_id: RegionId,
        _entries: Vec<(u64, Vec<u8>)>,
    ) -> Result<u64> {
        Ok(0)
    }

    async fn snapshot_data(&self, _region_id: RegionId) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    async fn restore_snapshot(&self, _region_id: RegionId, _data: &[u8]) -> Result<()> {
        Ok(())
    }
}

/// A per-region accumulating mock that stores written points and returns
/// them from `query_region`.
///
/// Used by `failover` and `region_migration` tests that need round-trip
/// write→query verification.
#[derive(Debug, Default)]
pub(crate) struct MockRegionStore {
    /// Per-region point storage.
    regions: Mutex<BTreeMap<RegionId, Vec<Point>>>,
}

impl MockRegionStore {
    /// Create an empty region store.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-populate a region with data (e.g. for migration source tests).
    #[allow(dead_code)]
    pub fn seed(&self, region_id: RegionId, pts: Vec<Point>) {
        self.regions.lock().insert(region_id, pts);
    }

    /// Read back points written to a specific region.
    #[allow(dead_code)]
    pub fn written_to(&self, region_id: RegionId) -> Vec<Point> {
        self.regions
            .lock()
            .get(&region_id)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl RegionStorage for MockRegionStore {
    async fn write_points(&self, region_id: RegionId, points: Vec<Point>) -> Result<u64> {
        let count = points.len() as u64;
        self.regions
            .lock()
            .entry(region_id)
            .or_default()
            .extend(points);
        Ok(count)
    }

    async fn query_region(&self, region_id: RegionId, query: RegionQuery) -> Result<Vec<Point>> {
        Ok(self
            .regions
            .lock()
            .get(&region_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|p| {
                let after_start = query.start_ns == 0 || p.timestamp() >= query.start_ns;
                let before_end = query.end_ns == 0 || p.timestamp() <= query.end_ns;
                after_start && before_end
            })
            .collect())
    }

    async fn replicate_wal(
        &self,
        _region_id: RegionId,
        _entries: Vec<(u64, Vec<u8>)>,
    ) -> Result<u64> {
        Ok(0)
    }

    async fn snapshot_data(&self, region_id: RegionId) -> Result<Vec<u8>> {
        let regions = self.regions.lock();
        let points = regions.get(&region_id).cloned().unwrap_or_default();
        serde_json::to_vec(&points)
            .map_err(|e| crate::ClusterError::Internal(format!("snapshot serialization: {e}")))
    }

    async fn restore_snapshot(&self, region_id: RegionId, data: &[u8]) -> Result<()> {
        let points: Vec<Point> = serde_json::from_slice(data)
            .map_err(|e| crate::ClusterError::Internal(format!("snapshot deserialization: {e}")))?;
        self.regions.lock().insert(region_id, points);
        Ok(())
    }
}
