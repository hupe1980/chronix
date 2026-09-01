//! Routing table — maps (measurement, region) to data node addresses.
//!
//! `QueryNodes` cache this locally to route writes and queries to the correct
//! `DataNode` without contacting the `MetaNode` on every request.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::types::{DataNodeInfo, KeyRange, NodeId, RegionId, RegionInfo, RegionState};

/// A routing entry for a single region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntry {
    /// Region ID.
    pub region_id: RegionId,
    /// Measurement this region serves.
    pub measurement: String,
    /// Leader node ID.
    pub leader_node_id: NodeId,
    /// gRPC address of the leader.
    pub leader_addr: String,
    /// All replica addresses (leader + followers).
    pub replica_addrs: Vec<(NodeId, String)>,
    /// Hash-key range this region owns (`None` for legacy modulo routing).
    #[serde(default)]
    pub key_range: Option<KeyRange>,
    /// Region lifecycle state — write path checks this to reject
    /// writes to frozen/migrating regions.
    #[serde(default = "default_region_state")]
    pub region_state: RegionState,
}

fn default_region_state() -> RegionState {
    RegionState::Active
}

/// Thread-safe routing table with version tracking.
///
/// `QueryNodes` use this to route requests. The version monotonically increases
/// on every mutation so stale-routing detection is cheap.
pub struct RoutingTable {
    /// Version counter — incremented on every mutation.
    version: AtomicU64,
    /// `measurement → Vec<RouteEntry>` (one entry per region of that measurement).
    entries: RwLock<BTreeMap<String, Vec<RouteEntry>>>,
}

impl RoutingTable {
    /// Create an empty routing table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    /// Current version (monotonically increasing).
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Rebuild the routing table from region and node information.
    pub fn rebuild(
        &self,
        regions: &BTreeMap<RegionId, RegionInfo>,
        nodes: &BTreeMap<NodeId, DataNodeInfo>,
    ) {
        let mut table: BTreeMap<String, Vec<RouteEntry>> = BTreeMap::new();

        for region in regions.values() {
            let leader_addr = nodes
                .get(&region.leader_node_id)
                .map(|n| n.grpc_addr.clone())
                .unwrap_or_default();

            let replica_addrs: Vec<(NodeId, String)> = region
                .replica_node_ids
                .iter()
                .filter_map(|nid| nodes.get(nid).map(|n| (*nid, n.grpc_addr.clone())))
                .collect();

            let entry = RouteEntry {
                region_id: region.region_id,
                measurement: region.measurement.clone(),
                leader_node_id: region.leader_node_id,
                leader_addr,
                replica_addrs,
                key_range: region.key_range,
                region_state: region.state,
            };

            table
                .entry(region.measurement.clone())
                .or_default()
                .push(entry);
        }

        // Sort routes by key-range start so binary search works.
        for routes in table.values_mut() {
            routes.sort_by_key(|r| r.key_range.map(|kr| kr.start));
        }

        *self.entries.write() = table;
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Look up routes for a measurement.
    #[must_use]
    pub fn routes_for_measurement(&self, measurement: &str) -> Vec<RouteEntry> {
        self.entries
            .read()
            .get(measurement)
            .cloned()
            .unwrap_or_default()
    }

    /// Look up the route for a specific region.
    #[must_use]
    pub fn route_for_region(&self, region_id: RegionId) -> Option<RouteEntry> {
        let entries = self.entries.read();
        for routes in entries.values() {
            for route in routes {
                if route.region_id == region_id {
                    return Some(route.clone());
                }
            }
        }
        None
    }

    /// Determine which region a series key hash should be routed to.
    ///
    /// # Routing Strategy
    ///
    /// **Range-based partitioning** (preferred): When all regions for a
    /// measurement carry a `key_range`, routes are sorted by range start
    /// and a binary search finds the owning region in O(log N).  Adding
    /// or removing regions only affects the adjacent ranges — no global
    /// reshuffle.
    ///
    /// **Modulo fallback** (legacy): When any region lacks a `key_range`,
    /// falls back to `hash % region_count`.  This reshuffles all series
    /// when region count changes.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn route_series(&self, measurement: &str, series_hash: u64) -> Option<RouteEntry> {
        let entries = self.entries.read();
        let routes = entries.get(measurement)?;
        if routes.is_empty() {
            return None;
        }

        // If all routes have key ranges, use range-based binary search.
        let all_have_ranges = routes.iter().all(|r| r.key_range.is_some());
        if all_have_ranges {
            debug_assert!(
                routes
                    .windows(2)
                    .all(|w| w[0].key_range.expect("all_have_ranges").start
                        <= w[1].key_range.expect("all_have_ranges").start),
                "routes must be sorted by key_range.start (see rebuild())"
            );
            // Routes are sorted by key_range.start (see rebuild()).
            // Binary search for the last route whose start <= series_hash.
            let idx = routes
                .partition_point(|r| r.key_range.map_or(false, |kr| kr.start <= series_hash))
                .saturating_sub(1);
            return Some(routes[idx].clone());
        }

        // Legacy modulo fallback.
        let idx = (series_hash % routes.len() as u64) as usize;
        Some(routes[idx].clone())
    }

    /// Number of measurements with routes.
    #[must_use]
    pub fn measurement_count(&self) -> usize {
        self.entries.read().len()
    }

    /// Total number of region routes.
    #[must_use]
    pub fn region_count(&self) -> usize {
        self.entries.read().values().map(Vec::len).sum()
    }

    /// Export the full routing table as a serializable snapshot.
    #[must_use]
    pub fn snapshot(&self) -> RoutingSnapshot {
        RoutingSnapshot {
            version: self.version(),
            entries: self.entries.read().clone(),
        }
    }

    /// Restore from a snapshot.
    pub fn restore(&self, snapshot: RoutingSnapshot) {
        *self.entries.write() = snapshot.entries;
        self.version.store(snapshot.version, Ordering::Release);
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RoutingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingTable")
            .field("version", &self.version())
            .field("measurements", &self.measurement_count())
            .field("regions", &self.region_count())
            .finish()
    }
}

/// Serializable routing table snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingSnapshot {
    /// Version at snapshot time.
    pub version: u64,
    /// All routing entries grouped by measurement.
    pub entries: BTreeMap<String, Vec<RouteEntry>>,
}

impl RoutingSnapshot {
    /// Create an empty snapshot at version 0.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            entries: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataNodeInfo, KeyRange, RegionInfo};

    fn make_nodes() -> BTreeMap<NodeId, DataNodeInfo> {
        let mut nodes = BTreeMap::new();
        for id in 1..=3 {
            let mut node = DataNodeInfo::new(id, format!("127.0.0.1:{}", 9100 + id));
            node.node_id = id;
            nodes.insert(id, node);
        }
        nodes
    }

    fn make_regions() -> BTreeMap<RegionId, RegionInfo> {
        let mut regions = BTreeMap::new();
        // cpu measurement: 3 regions
        for i in 0..3 {
            let region = RegionInfo::new(i + 1, "cpu", (i % 3) + 1, vec![1, 2, 3]);
            regions.insert(region.region_id, region);
        }
        // mem measurement: 2 regions
        for i in 0..2 {
            let region = RegionInfo::new(i + 4, "mem", (i % 3) + 1, vec![1, 2]);
            regions.insert(region.region_id, region);
        }
        regions
    }

    #[test]
    fn rebuild_and_query() {
        let table = RoutingTable::new();
        assert_eq!(table.version(), 0);
        assert_eq!(table.measurement_count(), 0);

        let nodes = make_nodes();
        let regions = make_regions();
        table.rebuild(&regions, &nodes);

        assert_eq!(table.version(), 1);
        assert_eq!(table.measurement_count(), 2);
        assert_eq!(table.region_count(), 5);

        let cpu_routes = table.routes_for_measurement("cpu");
        assert_eq!(cpu_routes.len(), 3);
        assert_eq!(cpu_routes[0].measurement, "cpu");

        let mem_routes = table.routes_for_measurement("mem");
        assert_eq!(mem_routes.len(), 2);

        assert!(table.routes_for_measurement("disk").is_empty());
    }

    #[test]
    fn route_for_region() {
        let table = RoutingTable::new();
        table.rebuild(&make_regions(), &make_nodes());

        let route = table.route_for_region(2).unwrap();
        assert_eq!(route.region_id, 2);
        assert_eq!(route.measurement, "cpu");

        assert!(table.route_for_region(999).is_none());
    }

    #[test]
    fn route_series_modulo_fallback() {
        // Regions without key_range use modulo fallback
        let table = RoutingTable::new();
        table.rebuild(&make_regions(), &make_nodes());

        // Different hashes should map to different regions (modulo)
        let r0 = table.route_series("cpu", 0).unwrap();
        let r1 = table.route_series("cpu", 1).unwrap();
        let r2 = table.route_series("cpu", 2).unwrap();
        assert_ne!(r0.region_id, r1.region_id);
        assert_ne!(r1.region_id, r2.region_id);

        // Same hash always routes to same region
        let ra = table.route_series("cpu", 42).unwrap();
        let rb = table.route_series("cpu", 42).unwrap();
        assert_eq!(ra.region_id, rb.region_id);
    }

    #[test]
    fn route_series_range_based() {
        let nodes = make_nodes();
        let mut regions = BTreeMap::new();

        // 3 regions covering the full u64 key space
        let third = u64::MAX / 3;
        let ranges = [
            KeyRange::new(0, third),
            KeyRange::new(third, third * 2),
            KeyRange::new(third * 2, u64::MAX),
        ];
        for (i, kr) in ranges.iter().enumerate() {
            let region = RegionInfo::new(i as u64 + 1, "cpu", (i as u64 % 3) + 1, vec![1, 2, 3])
                .with_key_range(*kr);
            regions.insert(region.region_id, region);
        }

        let table = RoutingTable::new();
        table.rebuild(&regions, &nodes);

        // Hash 0 → first range
        let r = table.route_series("cpu", 0).unwrap();
        assert_eq!(r.region_id, 1);
        assert!(r.key_range.unwrap().contains(0));

        // Hash in middle → second range
        let mid = third + 1;
        let r = table.route_series("cpu", mid).unwrap();
        assert_eq!(r.region_id, 2);

        // Hash near max → third range
        let r = table.route_series("cpu", u64::MAX - 1).unwrap();
        assert_eq!(r.region_id, 3);

        // Deterministic
        let ra = table.route_series("cpu", 12345).unwrap();
        let rb = table.route_series("cpu", 12345).unwrap();
        assert_eq!(ra.region_id, rb.region_id);
    }

    #[test]
    fn key_range_split() {
        let full = KeyRange::full();
        assert!(full.contains(0));
        assert!(full.contains(u64::MAX - 1));
        assert!(!full.contains(u64::MAX)); // end is exclusive

        let (left, right) = full.split().expect("full range must be splittable");
        assert_eq!(left.start, 0);
        assert_eq!(left.end, right.start);
        assert_eq!(right.end, u64::MAX);

        // Left and right are disjoint and cover the full range
        assert!(left.contains(0));
        assert!(!left.contains(left.end));
        assert!(right.contains(right.start));

        // Unsplittable: single-element range [5, 6)
        let tiny = KeyRange::new(5, 6);
        assert!(tiny.split().is_none());
        assert!(tiny.midpoint().is_none());

        // Empty range [5, 5)
        let empty = KeyRange::new(5, 5);
        assert!(empty.split().is_none());
    }

    #[test]
    fn snapshot_roundtrip() {
        let table = RoutingTable::new();
        table.rebuild(&make_regions(), &make_nodes());
        let snap = table.snapshot();

        let table2 = RoutingTable::new();
        table2.restore(snap.clone());
        assert_eq!(table2.version(), table.version());
        assert_eq!(table2.measurement_count(), table.measurement_count());
        assert_eq!(table2.region_count(), table.region_count());

        // Verify serialization
        let json = serde_json::to_string(&snap).unwrap();
        let restored: RoutingSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.version, snap.version);
    }

    #[test]
    fn version_increments() {
        let table = RoutingTable::new();
        assert_eq!(table.version(), 0);
        table.rebuild(&make_regions(), &make_nodes());
        assert_eq!(table.version(), 1);
        table.rebuild(&make_regions(), &make_nodes());
        assert_eq!(table.version(), 2);
    }

    #[test]
    fn debug_output() {
        let table = RoutingTable::new();
        let debug = format!("{table:?}");
        assert!(debug.contains("RoutingTable"));
        assert!(debug.contains("version"));
    }
}
