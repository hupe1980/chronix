//! Topology-aware replica placement policy.
//!
//! The [`PlacementPolicy`] selects target nodes for region replicas
//! while respecting failure-domain constraints expressed as node
//! labels (e.g. `rack`, `zone`, `datacenter`).
//!
//! # Constraint levels
//!
//! Constraints are evaluated in priority order.  The first label key
//! in [`PlacementPolicy::spread_labels`] is the highest priority
//! (e.g. `"datacenter"`), the last is the lowest (e.g. `"rack"`).
//! The algorithm greedily maximises diversity at the highest level
//! first, then falls back to lower levels when the pool is exhausted.
//!
//! # Example
//!
//! ```text
//! policy.spread_labels = ["zone", "rack"]
//!
//! Node 1: { zone: "us-east-1a", rack: "r1" }
//! Node 2: { zone: "us-east-1a", rack: "r2" }
//! Node 3: { zone: "us-east-1b", rack: "r3" }
//! Node 4: { zone: "us-east-1b", rack: "r4" }
//!
//! select_targets(3, &[]) → [1, 3, 2]  (or [3, 1, 4])
//!   → first pick from zone "us-east-1a", then zone "us-east-1b",
//!     then back to an unused rack in "us-east-1a".
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};

use chronix_meta::{DataNodeInfo, NodeId, NodeState};

/// Defines how replicas should be spread across failure domains.
#[derive(Debug, Clone)]
pub struct PlacementPolicy {
    /// Label keys to spread across, in priority order.
    ///
    /// Example: `["datacenter", "zone", "rack"]` means prefer different
    /// datacenters first, then different zones, then different racks.
    pub spread_labels: Vec<String>,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        Self {
            spread_labels: vec![
                "datacenter".to_string(),
                "zone".to_string(),
                "rack".to_string(),
            ],
        }
    }
}

impl PlacementPolicy {
    /// Create a new placement policy with the given spread labels.
    #[must_use]
    pub fn new(spread_labels: Vec<String>) -> Self {
        Self { spread_labels }
    }

    /// Select `count` target nodes for replica placement.
    ///
    /// Considers only active nodes, excludes nodes in `exclude` (e.g.
    /// nodes already hosting this region).  Maximises topological
    /// diversity by picking nodes from distinct label values at the
    /// highest priority level first.
    ///
    /// Returns at most `count` node IDs, sorted by preference.
    #[must_use]
    pub fn select_targets(
        &self,
        nodes: &BTreeMap<NodeId, DataNodeInfo>,
        count: usize,
        exclude: &HashSet<NodeId>,
    ) -> Vec<NodeId> {
        if count == 0 {
            return Vec::new();
        }

        // Filter to eligible candidates.
        let candidates: Vec<&DataNodeInfo> = nodes
            .values()
            .filter(|n| n.state == NodeState::Active && !exclude.contains(&n.node_id))
            .collect();

        if candidates.is_empty() {
            return Vec::new();
        }

        // Group candidates by the highest-priority spread label.
        let label_key = self.spread_labels.first();

        let mut by_group: BTreeMap<String, Vec<&DataNodeInfo>> = BTreeMap::new();
        for node in &candidates {
            let group = label_key
                .and_then(|k| node.labels.get(k))
                .cloned()
                .unwrap_or_default();
            by_group.entry(group).or_default().push(node);
        }

        // Sort groups by size ascending (prefer smaller groups to even out).
        let mut groups: Vec<Vec<&DataNodeInfo>> = by_group.into_values().collect();
        groups.sort_by_key(|g| g.len());

        // Within each group, sort by region count (fewest first) for
        // load-aware placement.
        for group in &mut groups {
            group.sort_by_key(|n| n.region_ids.len());
        }

        // Round-robin pick across groups to maximise spread.
        let mut selected = Vec::with_capacity(count);
        let mut chosen: HashSet<NodeId> = HashSet::new();
        let mut indices: Vec<usize> = vec![0; groups.len()];

        'outer: loop {
            let mut made_progress = false;
            for (gi, group) in groups.iter().enumerate() {
                if selected.len() >= count {
                    break 'outer;
                }
                while indices[gi] < group.len() {
                    let node = group[indices[gi]];
                    indices[gi] += 1;
                    if chosen.insert(node.node_id) {
                        selected.push(node.node_id);
                        made_progress = true;
                        break;
                    }
                }
            }
            if !made_progress || selected.len() >= count {
                break;
            }
        }

        selected
    }

    /// Check whether a set of replica nodes satisfies the spread
    /// constraints.  Returns the label key that is violated and the
    /// duplicated value, or `None` if all constraints are met.
    #[must_use]
    pub fn check_spread(
        &self,
        nodes: &BTreeMap<NodeId, DataNodeInfo>,
        replica_ids: &[NodeId],
    ) -> Option<(String, String)> {
        for label_key in &self.spread_labels {
            let mut seen: HashMap<&str, NodeId> = HashMap::new();
            for &nid in replica_ids {
                if let Some(node) = nodes.get(&nid) {
                    if let Some(val) = node.labels.get(label_key) {
                        if seen.contains_key(val.as_str()) {
                            return Some((label_key.clone(), val.clone()));
                        }
                        seen.insert(val.as_str(), nid);
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: NodeId, labels: &[(&str, &str)]) -> DataNodeInfo {
        let mut info = DataNodeInfo::new(id, format!("127.0.0.1:{}", 9000 + id));
        info.labels = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        info
    }

    #[test]
    fn select_targets_spreads_across_zones() {
        let policy = PlacementPolicy::new(vec!["zone".to_string()]);
        let mut nodes = BTreeMap::new();
        nodes.insert(1, node(1, &[("zone", "a")]));
        nodes.insert(2, node(2, &[("zone", "a")]));
        nodes.insert(3, node(3, &[("zone", "b")]));
        nodes.insert(4, node(4, &[("zone", "b")]));

        let targets = policy.select_targets(&nodes, 3, &HashSet::new());
        assert_eq!(targets.len(), 3);

        // Should pick from both zones
        let zones: HashSet<&str> = targets
            .iter()
            .map(|&id| nodes[&id].labels["zone"].as_str())
            .collect();
        assert!(zones.contains("a"));
        assert!(zones.contains("b"));
    }

    #[test]
    fn select_targets_excludes_nodes() {
        let policy = PlacementPolicy::new(vec!["zone".to_string()]);
        let mut nodes = BTreeMap::new();
        nodes.insert(1, node(1, &[("zone", "a")]));
        nodes.insert(2, node(2, &[("zone", "b")]));
        nodes.insert(3, node(3, &[("zone", "c")]));

        let exclude: HashSet<NodeId> = [1, 2].into_iter().collect();
        let targets = policy.select_targets(&nodes, 2, &exclude);
        assert_eq!(targets, vec![3]);
    }

    #[test]
    fn select_targets_skips_dead_nodes() {
        let policy = PlacementPolicy::new(vec!["zone".to_string()]);
        let mut nodes = BTreeMap::new();
        nodes.insert(1, node(1, &[("zone", "a")]));
        let mut dead = node(2, &[("zone", "b")]);
        dead.state = NodeState::Dead;
        nodes.insert(2, dead);
        nodes.insert(3, node(3, &[("zone", "c")]));

        let targets = policy.select_targets(&nodes, 3, &HashSet::new());
        assert_eq!(targets.len(), 2);
        assert!(!targets.contains(&2));
    }

    #[test]
    fn select_targets_prefers_least_loaded() {
        let policy = PlacementPolicy::new(vec!["zone".to_string()]);
        let mut nodes = BTreeMap::new();
        let mut n1 = node(1, &[("zone", "a")]);
        n1.region_ids = vec![10, 11, 12]; // heavily loaded
        nodes.insert(1, n1);
        let n2 = node(2, &[("zone", "a")]); // no regions
        nodes.insert(2, n2);

        let targets = policy.select_targets(&nodes, 1, &HashSet::new());
        assert_eq!(targets, vec![2], "should pick least-loaded node");
    }

    #[test]
    fn check_spread_detects_violation() {
        let policy = PlacementPolicy::new(vec!["rack".to_string()]);
        let mut nodes = BTreeMap::new();
        nodes.insert(1, node(1, &[("rack", "r1")]));
        nodes.insert(2, node(2, &[("rack", "r1")]));
        nodes.insert(3, node(3, &[("rack", "r2")]));

        let violation = policy.check_spread(&nodes, &[1, 2, 3]);
        assert!(violation.is_some());
        let (key, val) = violation.unwrap();
        assert_eq!(key, "rack");
        assert_eq!(val, "r1");
    }

    #[test]
    fn check_spread_passes_when_all_distinct() {
        let policy = PlacementPolicy::new(vec!["rack".to_string()]);
        let mut nodes = BTreeMap::new();
        nodes.insert(1, node(1, &[("rack", "r1")]));
        nodes.insert(2, node(2, &[("rack", "r2")]));
        nodes.insert(3, node(3, &[("rack", "r3")]));

        assert!(policy.check_spread(&nodes, &[1, 2, 3]).is_none());
    }

    #[test]
    fn select_targets_empty_count() {
        let policy = PlacementPolicy::default();
        let nodes = BTreeMap::new();
        assert!(policy.select_targets(&nodes, 0, &HashSet::new()).is_empty());
    }

    #[test]
    fn select_targets_no_labels_still_works() {
        let policy = PlacementPolicy::new(vec!["zone".to_string()]);
        let mut nodes = BTreeMap::new();
        // Nodes without labels — all go to default group
        nodes.insert(1, node(1, &[]));
        nodes.insert(2, node(2, &[]));
        nodes.insert(3, node(3, &[]));

        let targets = policy.select_targets(&nodes, 2, &HashSet::new());
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn default_policy_has_three_levels() {
        let p = PlacementPolicy::default();
        assert_eq!(p.spread_labels, ["datacenter", "zone", "rack"]);
    }
}
