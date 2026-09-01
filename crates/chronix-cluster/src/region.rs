//! Local region management on a `DataNode`.
//!
//! Tracks which data regions are hosted on this node and their lifecycle
//! state. This is a local bookkeeping layer — cluster-wide region metadata
//! lives in the `MetaNode`'s state machine.
//!
//! # Consistency model
//!
//! `RegionManager` is **eventually consistent** with the MetaNode.
//! Region creation and removal are triggered by Raft-committed
//! MetaNode commands (via the coordinator or migration pipeline),
//! which then call into `create_region` / `remove_region` on the
//! local DataNode.  Between the MetaNode commit and the local
//! callback there is a brief window where the two views diverge.
//!
//! This is safe because:
//! - **Reads** (queries) route through the `RoutingCache`, which is
//!   refreshed from the MetaNode and always authoritative.
//! - **Writes** go through Raft proposal, so a stale local view
//!   simply results in a `RegionNotFound` error that triggers a
//!   routing refresh and retry (see `WriteRouter::write_to_region`).
//! - **Migrations** hold the `MigrationPhase` state machine, which
//!   waits for acknowledgement before advancing.
//!
//! Full synchronisation (e.g. a push-based MetaNode subscription)
//! can be added if the retry-on-miss latency becomes problematic.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use chronix_meta::{NodeId, RegionId, RegionState};

use crate::error::{ClusterError, Result};

/// A region hosted locally on this `DataNode`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalRegion {
    /// Unique region identifier.
    pub region_id: RegionId,
    /// Measurement this region belongs to.
    pub measurement: String,
    /// Current lifecycle state.
    pub state: RegionState,
    /// Creation timestamp (Unix epoch seconds).
    pub created_at: u64,
}

impl LocalRegion {
    /// Create a new local region entry.
    #[must_use]
    fn new(region_id: RegionId, measurement: impl Into<String>) -> Self {
        let created_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        Self {
            region_id,
            measurement: measurement.into(),
            state: RegionState::Active,
            created_at,
        }
    }
}

/// Manages local regions on a `DataNode`.
///
/// Thread-safe via [`parking_lot::RwLock`]. All operations are synchronous
/// because they only touch local in-memory state.
pub struct RegionManager {
    /// Local regions indexed by region ID.
    regions: RwLock<BTreeMap<RegionId, LocalRegion>>,
    /// Owning node ID.
    node_id: NodeId,
}

impl RegionManager {
    /// Create a new, empty `RegionManager` for the given node.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            regions: RwLock::new(BTreeMap::new()),
            node_id,
        }
    }

    /// Create a local region storage entry.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::Internal`] if a region with the same ID
    /// already exists.
    pub fn create_region(&self, region_id: RegionId, measurement: impl Into<String>) -> Result<()> {
        let mut regions = self.regions.write();
        if regions.contains_key(&region_id) {
            return Err(ClusterError::Internal(format!(
                "region {region_id} already exists on node {}",
                self.node_id
            )));
        }
        regions.insert(region_id, LocalRegion::new(region_id, measurement));
        Ok(())
    }

    /// Look up a local region by ID.
    #[must_use]
    pub fn get_region(&self, region_id: RegionId) -> Option<LocalRegion> {
        self.regions.read().get(&region_id).cloned()
    }

    /// Remove a local region.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::RegionNotFound`] if the region does not exist.
    pub fn remove_region(&self, region_id: RegionId) -> Result<()> {
        let mut regions = self.regions.write();
        if regions.remove(&region_id).is_none() {
            return Err(ClusterError::RegionNotFound(region_id));
        }
        Ok(())
    }

    /// List all local regions.
    #[must_use]
    pub fn regions(&self) -> Vec<LocalRegion> {
        self.regions.read().values().cloned().collect()
    }

    /// Number of local regions.
    #[must_use]
    pub fn region_count(&self) -> usize {
        self.regions.read().len()
    }

    /// Filter local regions by measurement name.
    #[must_use]
    pub fn regions_for_measurement(&self, measurement: &str) -> Vec<LocalRegion> {
        self.regions
            .read()
            .values()
            .filter(|r| r.measurement == measurement)
            .cloned()
            .collect()
    }

    /// Returns the owning node ID.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl std::fmt::Debug for RegionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionManager")
            .field("node_id", &self.node_id)
            .field("region_count", &self.region_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_get_region() {
        let mgr = RegionManager::new(1);
        mgr.create_region(10, "cpu").unwrap();

        let region = mgr.get_region(10).unwrap();
        assert_eq!(region.region_id, 10);
        assert_eq!(region.measurement, "cpu");
        assert_eq!(region.state, RegionState::Active);
        assert!(region.created_at > 0);
    }

    #[test]
    fn create_duplicate_fails() {
        let mgr = RegionManager::new(1);
        mgr.create_region(10, "cpu").unwrap();

        let err = mgr.create_region(10, "cpu").unwrap_err();
        assert!(matches!(err, ClusterError::Internal(_)));
    }

    #[test]
    fn get_missing_returns_none() {
        let mgr = RegionManager::new(1);
        assert!(mgr.get_region(999).is_none());
    }

    #[test]
    fn remove_region_succeeds() {
        let mgr = RegionManager::new(1);
        mgr.create_region(10, "cpu").unwrap();

        mgr.remove_region(10).unwrap();
        assert!(mgr.get_region(10).is_none());
        assert_eq!(mgr.region_count(), 0);
    }

    #[test]
    fn remove_missing_fails() {
        let mgr = RegionManager::new(1);
        let err = mgr.remove_region(999).unwrap_err();
        assert!(matches!(err, ClusterError::RegionNotFound(999)));
    }

    #[test]
    fn list_all_regions() {
        let mgr = RegionManager::new(1);
        mgr.create_region(1, "cpu").unwrap();
        mgr.create_region(2, "mem").unwrap();
        mgr.create_region(3, "cpu").unwrap();

        let all = mgr.regions();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn region_count() {
        let mgr = RegionManager::new(1);
        assert_eq!(mgr.region_count(), 0);

        mgr.create_region(1, "cpu").unwrap();
        mgr.create_region(2, "mem").unwrap();
        assert_eq!(mgr.region_count(), 2);
    }

    #[test]
    fn filter_by_measurement() {
        let mgr = RegionManager::new(1);
        mgr.create_region(1, "cpu").unwrap();
        mgr.create_region(2, "mem").unwrap();
        mgr.create_region(3, "cpu").unwrap();

        let cpu_regions = mgr.regions_for_measurement("cpu");
        assert_eq!(cpu_regions.len(), 2);
        assert!(cpu_regions.iter().all(|r| r.measurement == "cpu"));

        let mem_regions = mgr.regions_for_measurement("mem");
        assert_eq!(mem_regions.len(), 1);

        let empty = mgr.regions_for_measurement("disk");
        assert!(empty.is_empty());
    }

    #[test]
    fn node_id_accessor() {
        let mgr = RegionManager::new(42);
        assert_eq!(mgr.node_id(), 42);
    }

    #[test]
    fn debug_format() {
        let mgr = RegionManager::new(1);
        mgr.create_region(1, "cpu").unwrap();

        let debug = format!("{mgr:?}");
        assert!(debug.contains("RegionManager"));
        assert!(debug.contains("region_count"));
    }
}
