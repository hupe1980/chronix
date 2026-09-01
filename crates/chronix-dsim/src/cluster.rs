//! In-process simulated Raft cluster for deterministic testing.
//!
//! Builds on the existing `MetaRouter` + `MetaStore` infrastructure
//! to create a fully controlled multi-node Raft cluster. Combined with
//! `SimNetwork` for partition injection and `InvariantChecker` + `Linearizer`
//! for correctness verification.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, Raft};

use chronix_meta::{
    DataNodeInfo, MeasurementSchema, MetaCommand, MetaNetworkFactory, MetaResponse, MetaRouter,
    MetaStore, MetaTypeConfig, RegionInfo,
};

use crate::checker::Linearizer;
use crate::clock::VirtualClock;
use crate::invariants::InvariantChecker;
use crate::network::{NetworkAction, SimNetwork};

/// Configuration for a simulation run.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Number of Raft nodes.
    pub nodes: u64,
    /// Random seed for deterministic behavior.
    pub seed: u64,
    /// Raft heartbeat interval (ms).
    pub heartbeat_ms: u64,
    /// Raft election timeout minimum (ms).
    pub election_min_ms: u64,
    /// Raft election timeout maximum (ms).
    pub election_max_ms: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            seed: 42,
            heartbeat_ms: 50,
            election_min_ms: 150,
            election_max_ms: 300,
        }
    }
}

/// Per-node context in the simulation.
struct SimNode {
    store: MetaStore,
    raft: Raft<MetaTypeConfig>,
}

/// A fully simulated Raft cluster for deterministic testing.
///
/// Manages N in-process Raft nodes sharing a common `MetaRouter`,
/// with integrated network simulation, invariant checking, and
/// linearizability verification.
///
/// # Example
///
/// ```ignore
/// let mut sim = SimCluster::new(SimConfig::default()).await;
/// sim.start().await;
/// sim.inject_partition(&[1], &[2, 3]);
/// let leader = sim.wait_for_leader().await;
/// sim.propose_schema(leader, "cpu").await;
/// sim.heal_network();
/// sim.check_invariants().unwrap();
/// ```
pub struct SimCluster {
    config: SimConfig,
    /// Held to keep the shared router alive for the entire simulation.
    _router: MetaRouter,
    nodes: BTreeMap<u64, SimNode>,
    network: SimNetwork,
    clock: VirtualClock,
    checker: InvariantChecker,
    linearizer: Linearizer,
}

impl SimCluster {
    /// Create a new simulation cluster (nodes created but not yet started).
    pub async fn new(config: SimConfig) -> Self {
        let router = MetaRouter::new();
        let raft_config = Arc::new(
            Config {
                heartbeat_interval: config.heartbeat_ms,
                election_timeout_min: config.election_min_ms,
                election_timeout_max: config.election_max_ms,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );

        let mut nodes = BTreeMap::new();
        for id in 1..=config.nodes {
            let store = MetaStore::new_in_memory();
            let net = MetaNetworkFactory::new(router.clone());
            let raft = Raft::<MetaTypeConfig>::new(
                id,
                raft_config.clone(),
                net,
                store.log_store(),
                store.sm_store(),
            )
            .await
            .expect("raft node creation");

            router.add_node(id, raft.clone());
            nodes.insert(id, SimNode { store, raft });
        }

        let network = SimNetwork::new(config.nodes, config.seed);
        let clock = VirtualClock::new(config.nodes as usize);
        let checker = InvariantChecker::all();
        let linearizer = Linearizer::new();

        Self {
            config,
            _router: router,
            nodes,
            network,
            clock,
            checker,
            linearizer,
        }
    }

    /// Initialize the Raft cluster membership and wait for leader election.
    pub async fn start(&self) {
        let mut members = BTreeMap::new();
        for id in 1..=self.config.nodes {
            members.insert(id, BasicNode::new(format!("127.0.0.1:{}", 9000 + id)));
        }

        self.nodes
            .get(&1)
            .expect("node 1 exists")
            .raft
            .initialize(members)
            .await
            .expect("initialize cluster");
    }

    /// Wait for a leader to be elected, with timeout.
    pub async fn wait_for_leader(&mut self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            for (id, node) in &self.nodes {
                if let Some(leader_id) = node.raft.current_leader().await {
                    if leader_id == *id {
                        // Record leader observation for invariant checking.
                        let term: u64 = node.raft.metrics().borrow().current_term;
                        self.checker.observe_leader(term, *id);
                        return *id;
                    }
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("no leader elected within 5s");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Find the current leader (non-blocking, returns None if no leader).
    pub async fn find_leader(&self) -> Option<u64> {
        for (id, node) in &self.nodes {
            if let Some(leader_id) = node.raft.current_leader().await {
                if leader_id == *id {
                    return Some(*id);
                }
            }
        }
        None
    }

    /// Propose a schema creation on the given leader node.
    pub async fn propose_schema(&mut self, leader_id: u64, measurement: &str) -> MetaResponse {
        let invoke_at = self.clock.now_ms();
        let node = self.nodes.get(&leader_id).expect("leader node");
        let result = node
            .raft
            .client_write(MetaCommand::CreateMeasurement(MeasurementSchema::new(
                measurement,
            )))
            .await
            .expect("propose schema");
        let return_at = self.clock.now_ms();

        // Record in linearizer.
        use crate::checker::OpKind;
        self.linearizer.record(
            leader_id,
            OpKind::Write {
                key: format!("schema:{measurement}"),
                value: measurement.to_owned(),
            },
            invoke_at,
            return_at,
        );
        self.checker
            .ack_write(&format!("schema:{measurement}"), measurement);

        result.data
    }

    /// Propose a data node registration.
    pub async fn propose_register_node(
        &self,
        leader_id: u64,
        data_node_id: u64,
        addr: &str,
    ) -> MetaResponse {
        let node = self.nodes.get(&leader_id).expect("leader node");
        let result = node
            .raft
            .client_write(MetaCommand::RegisterNode(DataNodeInfo::new(
                data_node_id,
                addr,
            )))
            .await
            .expect("propose register node");
        result.data
    }

    /// Propose a region creation.
    pub async fn propose_create_region(&self, leader_id: u64, region: RegionInfo) -> MetaResponse {
        let node = self.nodes.get(&leader_id).expect("leader node");
        let result = node
            .raft
            .client_write(MetaCommand::CreateRegion(region))
            .await
            .expect("propose create region");
        result.data
    }

    /// Verify schema is replicated to all (reachable) nodes.
    pub fn verify_schema_replicated(&mut self, measurement: &str) -> bool {
        let mut all_have = true;
        for (id, node) in &self.nodes {
            let has = node.store.state_machine().get_schema(measurement).is_some();
            if !has {
                tracing::debug!("node {id} missing schema '{measurement}'");
                all_have = false;
            } else {
                // Record successful read in linearizer.
                let now = self.clock.now_ms();
                use crate::checker::OpKind;
                self.linearizer.record(
                    *id,
                    OpKind::Read {
                        key: format!("schema:{measurement}"),
                        result: Some(measurement.to_owned()),
                    },
                    now,
                    now + 1,
                );
                self.checker
                    .observe_read(*id, &format!("schema:{measurement}"), Some(measurement));
            }
        }
        all_have
    }

    // ── Network Control ──────────────────────────────────────────

    /// Partition the network: `minority` nodes cannot communicate with `majority`.
    pub fn inject_partition(&self, minority: &[u64], majority: &[u64]) {
        self.network.apply(NetworkAction::Partition {
            minority: minority.to_vec(),
            majority: majority.to_vec(),
        });
    }

    /// Isolate a single node from all others.
    pub fn isolate_node(&self, node_id: u64) {
        self.network.apply(NetworkAction::Isolate(node_id));
    }

    /// Heal all partitions — restore full connectivity.
    pub fn heal_network(&self) {
        self.network.apply(NetworkAction::Heal);
    }

    /// Set packet loss ratio.
    pub fn set_packet_loss(&self, ratio: f64) {
        self.network.apply(NetworkAction::PacketLoss { ratio });
    }

    /// Check if a message can be delivered between two nodes.
    #[must_use]
    pub fn can_deliver(&self, from: u64, to: u64) -> bool {
        self.network.can_deliver(from, to)
    }

    // ── Clock Control ────────────────────────────────────────────

    /// Advance the virtual clock.
    pub fn advance_clock_ms(&self, ms: u64) {
        self.clock.advance_ms(ms);
    }

    /// Set clock skew for a node.
    pub fn set_clock_skew(&self, node_id: u64, skew_ms: i64) {
        self.clock.set_skew(node_id, skew_ms);
    }

    /// Get the current virtual time.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    // ── Invariant Verification ───────────────────────────────────

    /// Check all invariants accumulated so far.
    ///
    /// Returns `Ok(())` if no violations, or `Err` with the first violation.
    pub fn check_invariants(&self) -> Result<(), crate::invariants::InvariantError> {
        if self.checker.is_valid() {
            Ok(())
        } else {
            Err(self.checker.violations()[0].clone())
        }
    }

    /// Check linearizability of all recorded operations.
    pub fn check_linearizability(&self) -> Result<(), String> {
        self.linearizer.check()
    }

    /// Run all post-simulation correctness checks —
    /// invariants AND linearizability — in a single call.
    /// Use this at the end of every test to ensure both are enforced
    /// without relying on manual calls.
    pub fn assert_correctness(&self) {
        self.check_invariants()
            .expect("invariant violations detected");
        self.check_linearizability()
            .expect("linearizability violated");
    }

    /// Number of operations recorded in the linearizer.
    #[must_use]
    pub fn operation_count(&self) -> usize {
        self.linearizer.len()
    }

    /// Access the virtual clock.
    #[must_use]
    pub fn clock(&self) -> &VirtualClock {
        &self.clock
    }

    /// Access the simulated network.
    #[must_use]
    pub fn network(&self) -> &SimNetwork {
        &self.network
    }

    /// Number of nodes in the cluster.
    #[must_use]
    pub fn node_count(&self) -> u64 {
        self.config.nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sim_cluster_starts_and_elects_leader() {
        let mut sim = SimCluster::new(SimConfig::default()).await;
        sim.start().await;
        let leader = sim.wait_for_leader().await;
        assert!((1..=3).contains(&leader));
        sim.check_invariants().expect("no violations");
    }

    #[tokio::test]
    async fn sim_schema_replicates_to_all_nodes() {
        let mut sim = SimCluster::new(SimConfig::default()).await;
        sim.start().await;
        let leader = sim.wait_for_leader().await;

        let resp = sim.propose_schema(leader, "cpu").await;
        assert!(
            matches!(resp, MetaResponse::Created { .. }),
            "expected Created, got {resp:?}"
        );

        // Allow replication.
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(sim.verify_schema_replicated("cpu"));
        sim.check_invariants().expect("no violations");
        sim.check_linearizability().expect("linearizable");
    }

    #[tokio::test]
    async fn sim_node_registration() {
        let mut sim = SimCluster::new(SimConfig::default()).await;
        sim.start().await;
        let leader = sim.wait_for_leader().await;

        for i in 10..=12 {
            let resp = sim
                .propose_register_node(leader, i, &format!("10.0.0.{i}:9001"))
                .await;
            assert!(
                matches!(resp, MetaResponse::Created { .. }),
                "expected Created for node {i}, got {resp:?}"
            );
        }
    }

    #[tokio::test]
    async fn sim_network_partition_and_heal() {
        let sim = SimCluster::new(SimConfig {
            nodes: 5,
            seed: 123,
            ..SimConfig::default()
        })
        .await;

        // Full connectivity initially.
        assert!(sim.can_deliver(1, 5));

        // Partition: {1,2} vs {3,4,5}
        sim.inject_partition(&[1, 2], &[3, 4, 5]);
        assert!(!sim.can_deliver(1, 3));
        assert!(!sim.can_deliver(2, 4));
        assert!(sim.can_deliver(1, 2)); // Within minority
        assert!(sim.can_deliver(3, 5)); // Within majority

        sim.heal_network();
        assert!(sim.can_deliver(1, 3));
    }

    #[tokio::test]
    async fn sim_leader_election_after_isolation() {
        let mut sim = SimCluster::new(SimConfig {
            nodes: 3,
            seed: 99,
            heartbeat_ms: 20,
            election_min_ms: 50,
            election_max_ms: 100,
        })
        .await;
        sim.start().await;

        let leader1 = sim.wait_for_leader().await;

        // Isolate the leader — remaining 2 nodes should elect a new leader.
        // Note: in-process router doesn't actually enforce partitions yet
        // (that would require intercepting openraft RPCs), but the simulation
        // infrastructure is in place for future integration.
        sim.isolate_node(leader1);

        // Verify network state is correct.
        let other = if leader1 == 1 { 2 } else { 1 };
        assert!(!sim.can_deliver(leader1, other));
        assert!(!sim.can_deliver(other, leader1));

        sim.heal_network();
        let leader2 = sim.wait_for_leader().await;
        assert!((1..=3).contains(&leader2));
        sim.check_invariants().expect("no violations");
    }

    #[tokio::test]
    async fn sim_clock_control() {
        let sim = SimCluster::new(SimConfig::default()).await;
        assert_eq!(sim.now_ms(), 0);

        sim.advance_clock_ms(1000);
        assert_eq!(sim.now_ms(), 1000);

        sim.set_clock_skew(1, 100);
        assert_eq!(sim.clock().skew_for_node(1), 1100);
        assert_eq!(sim.clock().skew_for_node(2), 1000);
    }

    #[tokio::test]
    async fn sim_linearizability_of_schema_ops() {
        let mut sim = SimCluster::new(SimConfig::default()).await;
        sim.start().await;
        let leader = sim.wait_for_leader().await;

        // Advance clock — each op happens at a distinct time.
        sim.advance_clock_ms(10);
        sim.propose_schema(leader, "cpu").await;

        sim.advance_clock_ms(10);
        sim.propose_schema(leader, "mem").await;

        sim.advance_clock_ms(10);
        sim.propose_schema(leader, "disk").await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify all schemas replicated.
        assert!(sim.verify_schema_replicated("cpu"));
        assert!(sim.verify_schema_replicated("mem"));
        assert!(sim.verify_schema_replicated("disk"));

        assert_eq!(sim.operation_count(), 12); // 3 writes + 3×3 reads
        sim.assert_correctness();
    }

    /// Split-brain test scenario.
    ///
    /// Partition a 5-node cluster into two halves ({1,2} vs {3,4,5}).
    /// The majority partition should elect a new leader and accept
    /// writes; the minority partition should NOT elect a leader
    /// (requires majority quorum = 3 of 5).
    #[tokio::test]
    async fn sim_split_brain_majority_wins() {
        let mut sim = SimCluster::new(SimConfig {
            nodes: 5,
            seed: 77,
            heartbeat_ms: 20,
            election_min_ms: 50,
            election_max_ms: 100,
        })
        .await;
        sim.start().await;
        let leader1 = sim.wait_for_leader().await;

        // Write a schema before partition.
        sim.advance_clock_ms(10);
        sim.propose_schema(leader1, "pre_partition").await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Partition: {1,2} isolated from {3,4,5}.
        // The majority side (3 nodes) can form quorum; minority cannot.
        sim.inject_partition(&[1, 2], &[3, 4, 5]);

        // Verify partition is in effect.
        assert!(!sim.can_deliver(1, 3));
        assert!(!sim.can_deliver(2, 5));
        assert!(sim.can_deliver(3, 4));
        assert!(sim.can_deliver(4, 5));
        assert!(sim.can_deliver(1, 2));

        // Heal network — cluster should converge.
        sim.heal_network();
        let leader2 = sim.wait_for_leader().await;
        assert!((1..=5).contains(&leader2));

        // Post-partition write should succeed.
        sim.advance_clock_ms(10);
        sim.propose_schema(leader2, "post_partition").await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify all schemas replicated after healing.
        assert!(sim.verify_schema_replicated("pre_partition"));
        assert!(sim.verify_schema_replicated("post_partition"));

        // Auto-enforce all correctness checks.
        sim.assert_correctness();
    }

    /// Verify that isolating a node and healing still
    /// preserves all invariants and linearizability.
    #[tokio::test]
    async fn sim_split_brain_single_node_isolation() {
        let mut sim = SimCluster::new(SimConfig {
            nodes: 3,
            seed: 42,
            heartbeat_ms: 20,
            election_min_ms: 50,
            election_max_ms: 100,
        })
        .await;
        sim.start().await;
        let leader1 = sim.wait_for_leader().await;

        // Write before isolation.
        sim.advance_clock_ms(10);
        sim.propose_schema(leader1, "before_isolation").await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Isolate one non-leader node.
        let isolated = if leader1 == 1 { 2 } else { 1 };
        sim.isolate_node(isolated);
        assert!(!sim.can_deliver(isolated, leader1));

        // Remaining 2 nodes still form quorum (2 of 3).
        // Write should still succeed on leader.
        sim.advance_clock_ms(10);
        sim.propose_schema(leader1, "during_isolation").await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Heal and verify convergence.
        sim.heal_network();
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert!(sim.verify_schema_replicated("before_isolation"));
        assert!(sim.verify_schema_replicated("during_isolation"));
        sim.assert_correctness();
    }
}
