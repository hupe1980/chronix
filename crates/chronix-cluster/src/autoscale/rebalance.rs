//! Disk-aware rebalancing and combined scaling assessment.

use std::collections::BTreeMap;

use tracing::debug;

use chronix_meta::{DataNodeInfo, NodeId, NodeState, RegionId, RegionInfo};

use super::{AutoScaler, RegionMetrics, SplitPlan};
use crate::coordinator::RegionMigration;

// ── Disk Usage ────────────────────────────────────────────────

/// Disk usage snapshot for a node.
#[derive(Debug, Clone)]
pub struct NodeDiskUsage {
    /// Node identifier.
    pub node_id: NodeId,
    /// Bytes used by regions on this node.
    pub used_bytes: u64,
    /// Total disk capacity.
    pub capacity_bytes: u64,
    /// Region IDs hosted on this node.
    pub region_ids: Vec<RegionId>,
}

impl NodeDiskUsage {
    /// Disk usage ratio (0.0 to 1.0).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn usage_ratio(&self) -> f64 {
        if self.capacity_bytes == 0 {
            return 0.0;
        }
        self.used_bytes as f64 / self.capacity_bytes as f64
    }
}

// ── Scale Assessment ──────────────────────────────────────────

/// Combined result of a scaling assessment.
#[derive(Debug, Clone)]
pub struct ScaleAssessment {
    /// Regions that should be split.
    pub split_plans: Vec<SplitPlan>,
    /// Migrations to execute for disk rebalancing.
    pub rebalance_migrations: Vec<RegionMigration>,
    /// Whether the cluster needs rebalancing.
    pub needs_rebalancing: bool,
}

impl ScaleAssessment {
    /// Returns `true` if no scaling actions are needed.
    #[must_use]
    pub fn is_stable(&self) -> bool {
        self.split_plans.is_empty() && self.rebalance_migrations.is_empty()
    }
}

// ── Rebalancing (methods on AutoScaler) ───────────────────────

impl AutoScaler {
    /// Determine whether the cluster needs rebalancing based on disk usage deviation.
    ///
    /// Returns `true` if the maximum deviation from the mean exceeds the
    /// configured threshold.
    #[must_use]
    pub fn needs_rebalancing(&self, disk_usages: &[NodeDiskUsage]) -> bool {
        if disk_usages.len() <= 1 {
            return false;
        }

        let mean = Self::mean_usage(disk_usages);

        disk_usages
            .iter()
            .any(|n| (n.usage_ratio() - mean).abs() > self.config().disk_deviation_threshold)
    }

    /// Plan migrations to rebalance disk usage across nodes.
    ///
    /// Moves regions from nodes with above-mean disk usage to nodes with
    /// below-mean disk usage. Limited by `max_concurrent_migrations`.
    ///
    /// `leader_map` maps `RegionId` → leading `NodeId`, used to build
    /// migration entries.
    #[must_use]
    pub fn plan_disk_rebalance(
        &self,
        disk_usages: &[NodeDiskUsage],
        leader_map: &BTreeMap<RegionId, NodeId>,
    ) -> Vec<RegionMigration> {
        if disk_usages.len() <= 1 {
            return Vec::new();
        }

        let mean = Self::mean_usage(disk_usages);
        let thresh = self.config().disk_deviation_threshold;

        // Classify nodes as overloaded or underloaded.
        let mut overloaded: Vec<&NodeDiskUsage> = disk_usages
            .iter()
            .filter(|n| n.usage_ratio() - mean > thresh)
            .collect();

        let mut underloaded: Vec<&NodeDiskUsage> = disk_usages
            .iter()
            .filter(|n| mean - n.usage_ratio() > thresh)
            .collect();

        // Sort: most overloaded first, most underloaded first.
        overloaded.sort_by(|a, b| {
            b.usage_ratio()
                .partial_cmp(&a.usage_ratio())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        underloaded.sort_by(|a, b| {
            a.usage_ratio()
                .partial_cmp(&b.usage_ratio())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut migrations = Vec::new();
        let mut under_idx = 0;

        'outer: for over_node in &overloaded {
            for &region_id in &over_node.region_ids {
                if migrations.len() >= self.config().max_concurrent_migrations {
                    break 'outer;
                }

                if under_idx >= underloaded.len() {
                    break 'outer;
                }

                let from_node = leader_map
                    .get(&region_id)
                    .copied()
                    .unwrap_or(over_node.node_id);

                migrations.push(RegionMigration {
                    region_id,
                    from_node,
                    to_node: underloaded[under_idx].node_id,
                });

                // Rotate through underloaded nodes.
                under_idx = (under_idx + 1) % underloaded.len();
            }
        }

        if !migrations.is_empty() {
            debug!(
                count = migrations.len(),
                "Planned disk-aware rebalance migrations"
            );
        }

        migrations
    }

    /// Compute mean disk usage ratio across nodes.
    #[allow(clippy::cast_precision_loss)]
    fn mean_usage(usages: &[NodeDiskUsage]) -> f64 {
        if usages.is_empty() {
            return 0.0;
        }
        let sum: f64 = usages.iter().map(NodeDiskUsage::usage_ratio).sum();
        sum / usages.len() as f64
    }

    /// Produce a combined scaling assessment.
    ///
    /// Returns split plans and rebalance migrations in one pass, suitable
    /// for a periodic scan loop.
    #[must_use]
    pub fn assess(
        &self,
        regions: &BTreeMap<RegionId, RegionInfo>,
        region_metrics: &BTreeMap<RegionId, RegionMetrics>,
        disk_usages: &[NodeDiskUsage],
        leader_map: &BTreeMap<RegionId, NodeId>,
        next_id: &mut dyn FnMut() -> RegionId,
    ) -> ScaleAssessment {
        let split_plans = self.plan_splits(regions, region_metrics, next_id);
        let rebalance_migrations = self.plan_disk_rebalance(disk_usages, leader_map);
        let needs_rebalancing = self.needs_rebalancing(disk_usages);

        ScaleAssessment {
            split_plans,
            rebalance_migrations,
            needs_rebalancing,
        }
    }
}

// ── Utility functions ─────────────────────────────────────────

/// Build a `RegionId → leader NodeId` map from region metadata.
#[must_use]
pub fn build_leader_map(regions: &BTreeMap<RegionId, RegionInfo>) -> BTreeMap<RegionId, NodeId> {
    regions
        .iter()
        .map(|(&rid, info)| (rid, info.leader_node_id))
        .collect()
}

/// Build `NodeDiskUsage` entries from node metadata.
///
/// Estimates per-node disk usage from the number of regions hosted.
/// In production, this would be replaced by actual disk metrics from
/// `DataNode` heartbeats.
#[must_use]
pub fn estimate_disk_usage(
    nodes: &BTreeMap<NodeId, DataNodeInfo>,
    regions: &BTreeMap<RegionId, RegionInfo>,
    region_metrics: &BTreeMap<RegionId, RegionMetrics>,
) -> Vec<NodeDiskUsage> {
    nodes
        .values()
        .filter(|n| n.state == NodeState::Active)
        .map(|node| {
            let hosted_regions: Vec<RegionId> = regions
                .iter()
                .filter(|(_, info)| info.replica_node_ids.contains(&node.node_id))
                .map(|(&rid, _)| rid)
                .collect();

            let used_bytes: u64 = hosted_regions
                .iter()
                .filter_map(|rid| region_metrics.get(rid))
                .map(|m| m.size_bytes)
                .sum();

            NodeDiskUsage {
                node_id: node.node_id,
                used_bytes,
                capacity_bytes: node.disk_bytes,
                region_ids: hosted_regions,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::autoscale::tests_util::{make_config, make_id_alloc, make_region};

    // ── needs_rebalancing ─────────────────────────────────────

    #[test]
    fn rebalance_not_needed_single_node() {
        let scaler = AutoScaler::new(make_config());
        let usages = vec![NodeDiskUsage {
            node_id: 1,
            used_bytes: 500,
            capacity_bytes: 1000,
            region_ids: vec![1],
        }];
        assert!(!scaler.needs_rebalancing(&usages));
    }

    #[test]
    fn rebalance_not_needed_even_usage() {
        let scaler = AutoScaler::new(make_config());
        let usages = vec![
            NodeDiskUsage {
                node_id: 1,
                used_bytes: 450,
                capacity_bytes: 1000,
                region_ids: vec![1],
            },
            NodeDiskUsage {
                node_id: 2,
                used_bytes: 500,
                capacity_bytes: 1000,
                region_ids: vec![2],
            },
            NodeDiskUsage {
                node_id: 3,
                used_bytes: 480,
                capacity_bytes: 1000,
                region_ids: vec![3],
            },
        ];
        assert!(!scaler.needs_rebalancing(&usages));
    }

    #[test]
    fn rebalance_needed_uneven_usage() {
        let scaler = AutoScaler::new(make_config());
        let usages = vec![
            NodeDiskUsage {
                node_id: 1,
                used_bytes: 900,
                capacity_bytes: 1000,
                region_ids: vec![1, 2, 3],
            },
            NodeDiskUsage {
                node_id: 2,
                used_bytes: 100,
                capacity_bytes: 1000,
                region_ids: vec![4],
            },
            NodeDiskUsage {
                node_id: 3,
                used_bytes: 200,
                capacity_bytes: 1000,
                region_ids: vec![5],
            },
        ];
        assert!(scaler.needs_rebalancing(&usages));
    }

    // ── plan_disk_rebalance ───────────────────────────────────

    #[test]
    fn disk_rebalance_generates_migrations() {
        let scaler = AutoScaler::new(make_config());

        let usages = vec![
            NodeDiskUsage {
                node_id: 1,
                used_bytes: 900,
                capacity_bytes: 1000,
                region_ids: vec![1, 2, 3],
            },
            NodeDiskUsage {
                node_id: 2,
                used_bytes: 100,
                capacity_bytes: 1000,
                region_ids: vec![4],
            },
        ];

        let mut leader_map = BTreeMap::new();
        leader_map.insert(1, 1);
        leader_map.insert(2, 1);
        leader_map.insert(3, 1);
        leader_map.insert(4, 2);

        let migrations = scaler.plan_disk_rebalance(&usages, &leader_map);

        assert!(!migrations.is_empty());
        assert!(migrations.len() <= 2); // max_concurrent_migrations
        assert_eq!(migrations[0].to_node, 2); // moved to underloaded node
    }

    #[test]
    fn disk_rebalance_respects_max_concurrent() {
        let mut config = make_config();
        config.max_concurrent_migrations = 1;
        let scaler = AutoScaler::new(config);

        let usages = vec![
            NodeDiskUsage {
                node_id: 1,
                used_bytes: 900,
                capacity_bytes: 1000,
                region_ids: vec![1, 2, 3],
            },
            NodeDiskUsage {
                node_id: 2,
                used_bytes: 100,
                capacity_bytes: 1000,
                region_ids: vec![4],
            },
        ];

        let mut leader_map = BTreeMap::new();
        leader_map.insert(1, 1);
        leader_map.insert(2, 1);
        leader_map.insert(3, 1);

        let migrations = scaler.plan_disk_rebalance(&usages, &leader_map);
        assert_eq!(migrations.len(), 1);
    }

    #[test]
    fn disk_rebalance_empty_when_balanced() {
        let scaler = AutoScaler::new(make_config());

        let usages = vec![
            NodeDiskUsage {
                node_id: 1,
                used_bytes: 500,
                capacity_bytes: 1000,
                region_ids: vec![1],
            },
            NodeDiskUsage {
                node_id: 2,
                used_bytes: 500,
                capacity_bytes: 1000,
                region_ids: vec![2],
            },
        ];

        let leader_map = BTreeMap::new();
        let migrations = scaler.plan_disk_rebalance(&usages, &leader_map);
        assert!(migrations.is_empty());
    }

    // ── Combined assessment ───────────────────────────────────

    #[test]
    fn assess_combines_splits_and_rebalance() {
        let scaler = AutoScaler::new(make_config());

        let mut regions = BTreeMap::new();
        regions.insert(1, make_region(1, "cpu", 10));
        regions.insert(2, make_region(2, "mem", 20));

        let mut region_metrics = BTreeMap::new();
        region_metrics.insert(
            1,
            RegionMetrics {
                size_bytes: 2_000_000,
                series_count: 10,
                ..Default::default()
            },
        );
        region_metrics.insert(
            2,
            RegionMetrics {
                size_bytes: 100,
                series_count: 10,
                ..Default::default()
            },
        );

        let disk_usages = vec![
            NodeDiskUsage {
                node_id: 10,
                used_bytes: 900,
                capacity_bytes: 1000,
                region_ids: vec![1],
            },
            NodeDiskUsage {
                node_id: 20,
                used_bytes: 100,
                capacity_bytes: 1000,
                region_ids: vec![2],
            },
        ];

        let leader_map = build_leader_map(&regions);
        let mut id_alloc = make_id_alloc();
        let assessment = scaler.assess(
            &regions,
            &region_metrics,
            &disk_usages,
            &leader_map,
            &mut id_alloc,
        );

        assert_eq!(assessment.split_plans.len(), 1);
        assert!(assessment.needs_rebalancing);
        assert!(!assessment.rebalance_migrations.is_empty());
        assert!(!assessment.is_stable());
    }

    #[test]
    fn assess_stable_when_healthy() {
        let scaler = AutoScaler::new(make_config());

        let mut regions = BTreeMap::new();
        regions.insert(1, make_region(1, "cpu", 10));

        let mut region_metrics = BTreeMap::new();
        region_metrics.insert(
            1,
            RegionMetrics {
                size_bytes: 100,
                series_count: 10,
                ..Default::default()
            },
        );

        let disk_usages = vec![NodeDiskUsage {
            node_id: 10,
            used_bytes: 500,
            capacity_bytes: 1000,
            region_ids: vec![1],
        }];

        let leader_map = build_leader_map(&regions);
        let mut id_alloc = make_id_alloc();
        let assessment = scaler.assess(
            &regions,
            &region_metrics,
            &disk_usages,
            &leader_map,
            &mut id_alloc,
        );

        assert!(assessment.split_plans.is_empty());
        assert!(assessment.rebalance_migrations.is_empty());
        assert!(assessment.is_stable());
    }

    // ── Utility functions ─────────────────────────────────────

    #[test]
    fn build_leader_map_correct() {
        let mut regions = BTreeMap::new();
        regions.insert(1, make_region(1, "cpu", 10));
        regions.insert(2, make_region(2, "mem", 20));

        let map = build_leader_map(&regions);
        assert_eq!(map[&1], 10);
        assert_eq!(map[&2], 20);
    }

    #[test]
    fn estimate_disk_usage_sums_regions() {
        let mut nodes = BTreeMap::new();
        let node1 = DataNodeInfo::new(1, "addr1").with_capacity(0, 0, 0);
        let mut node1_mod = node1;
        node1_mod.disk_bytes = 10_000;
        nodes.insert(1, node1_mod);

        let mut regions = BTreeMap::new();
        let mut r1 = make_region(1, "cpu", 1);
        r1.replica_node_ids = vec![1];
        let mut r2 = make_region(2, "mem", 1);
        r2.replica_node_ids = vec![1];
        regions.insert(1, r1);
        regions.insert(2, r2);

        let mut metrics = BTreeMap::new();
        metrics.insert(
            1,
            RegionMetrics {
                size_bytes: 1000,
                ..Default::default()
            },
        );
        metrics.insert(
            2,
            RegionMetrics {
                size_bytes: 2000,
                ..Default::default()
            },
        );

        let usages = estimate_disk_usage(&nodes, &regions, &metrics);
        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].used_bytes, 3000);
        assert_eq!(usages[0].capacity_bytes, 10_000);
    }

    #[test]
    fn node_disk_usage_ratio() {
        let usage = NodeDiskUsage {
            node_id: 1,
            used_bytes: 250,
            capacity_bytes: 1000,
            region_ids: vec![],
        };
        assert!((usage.usage_ratio() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn node_disk_usage_ratio_zero_capacity() {
        let usage = NodeDiskUsage {
            node_id: 1,
            used_bytes: 100,
            capacity_bytes: 0,
            region_ids: vec![],
        };
        assert!((usage.usage_ratio()).abs() < f64::EPSILON);
    }
}
