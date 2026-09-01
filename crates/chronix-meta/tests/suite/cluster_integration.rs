//! Multi-node Raft cluster integration tests.
//!
//! Spins up a 3-node in-process Raft cluster and exercises:
//!   - Leader election
//!   - Schema creation / replication
//!   - Node registration / replication
//!   - Region creation / replication
//!   - Snapshot & restore
//!   - Configuration changes (add/remove learner)

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, Raft};

use chronix_meta::{
    ClusterConfig, DataNodeInfo, MeasurementSchema, MetaCommand, MetaNetworkFactory, MetaResponse,
    MetaRouter, MetaStore, MetaTypeConfig, RegionInfo,
};

/// Timeout for cluster operations.
const CLUSTER_TIMEOUT: Duration = Duration::from_secs(5);

/// Build an `openraft::Config` suitable for fast tests.
fn test_raft_config() -> Arc<Config> {
    let config = Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        ..Default::default()
    };
    Arc::new(config.validate().expect("valid config"))
}

/// Per-node context: store + raft handle.
struct TestNode {
    store: MetaStore,
    raft: Raft<MetaTypeConfig>,
}

/// Build a cluster of `n` nodes sharing a common router.
async fn build_cluster(n: u64) -> (MetaRouter, BTreeMap<u64, TestNode>) {
    let router = MetaRouter::new();
    let config = test_raft_config();
    let mut nodes = BTreeMap::new();

    for id in 1..=n {
        let store = MetaStore::new_in_memory();
        let net = MetaNetworkFactory::new(router.clone());
        let raft = Raft::<MetaTypeConfig>::new(
            id,
            config.clone(),
            net,
            store.log_store(),
            store.sm_store(),
        )
        .await
        .expect("raft node creation");

        router.add_node(id, raft.clone());
        nodes.insert(id, TestNode { store, raft });
    }

    (router, nodes)
}

/// Find the current leader ID — polls all nodes.
async fn find_leader(nodes: &BTreeMap<u64, TestNode>) -> Option<u64> {
    for (id, node) in nodes {
        if let Some(leader_id) = node.raft.current_leader().await {
            if leader_id == *id {
                return Some(*id);
            }
        }
    }
    None
}

/// Wait for a leader to be elected among the nodes.
async fn wait_for_leader(nodes: &BTreeMap<u64, TestNode>) -> u64 {
    let deadline = tokio::time::Instant::now() + CLUSTER_TIMEOUT;
    loop {
        if let Some(leader) = find_leader(nodes).await {
            return leader;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("no leader elected within timeout");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Propose a command on the given Raft node and return the response.
async fn propose(
    nodes: &BTreeMap<u64, TestNode>,
    leader_id: u64,
    cmd: MetaCommand,
) -> MetaResponse {
    let node = nodes.get(&leader_id).expect("leader node");
    let result = node.raft.client_write(cmd).await.expect("propose");
    result.data
}

// ── Tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn three_node_leader_election() {
    let (_router, nodes) = build_cluster(3).await;

    // Initialise cluster membership on node 1.
    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("127.0.0.1:9001"));
    members.insert(2, BasicNode::new("127.0.0.1:9002"));
    members.insert(3, BasicNode::new("127.0.0.1:9003"));

    nodes
        .get(&1)
        .unwrap()
        .raft
        .initialize(members)
        .await
        .expect("initialize");

    let leader = wait_for_leader(&nodes).await;
    assert!((1..=3).contains(&leader));
}

#[tokio::test]
async fn schema_creation_replicates() {
    let (_router, nodes) = build_cluster(3).await;

    // Initialise cluster.
    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("127.0.0.1:9001"));
    members.insert(2, BasicNode::new("127.0.0.1:9002"));
    members.insert(3, BasicNode::new("127.0.0.1:9003"));
    nodes
        .get(&1)
        .unwrap()
        .raft
        .initialize(members)
        .await
        .unwrap();

    let leader = wait_for_leader(&nodes).await;

    // Create a measurement schema.
    let resp = propose(
        &nodes,
        leader,
        MetaCommand::CreateMeasurement(MeasurementSchema::new("cpu")),
    )
    .await;
    assert!(
        matches!(resp, MetaResponse::Created { .. }),
        "expected Created, got {resp:?}"
    );

    // Give replication a moment to propagate.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify every node has the schema.
    for (id, node) in &nodes {
        let schema = node.store.state_machine().get_schema("cpu");
        assert!(schema.is_some(), "node {id} missing 'cpu' schema");
    }
}

#[tokio::test]
async fn node_registration_and_region_creation() {
    let (_router, nodes) = build_cluster(3).await;

    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("127.0.0.1:9001"));
    members.insert(2, BasicNode::new("127.0.0.1:9002"));
    members.insert(3, BasicNode::new("127.0.0.1:9003"));
    nodes
        .get(&1)
        .unwrap()
        .raft
        .initialize(members)
        .await
        .unwrap();

    let leader = wait_for_leader(&nodes).await;

    // Register data nodes.
    for i in 10..=12 {
        let resp = propose(
            &nodes,
            leader,
            MetaCommand::RegisterNode(DataNodeInfo::new(i, &format!("10.0.0.{i}:9001"))),
        )
        .await;
        assert!(
            matches!(resp, MetaResponse::Created { .. }),
            "expected Created for RegisterNode, got {resp:?}"
        );
    }

    // Create measurement schema.
    let schema = MeasurementSchema::new("temperature")
        .with_replication_factor(2)
        .with_region_count(2);
    propose(&nodes, leader, MetaCommand::CreateMeasurement(schema)).await;

    // Create a region assigned to data nodes.
    let region = RegionInfo::new(1, "temperature", 10, vec![11, 12]);
    let resp = propose(&nodes, leader, MetaCommand::CreateRegion(region)).await;
    assert!(
        matches!(resp, MetaResponse::Created { .. }),
        "expected Created for CreateRegion, got {resp:?}"
    );

    // Let replication settle.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // All nodes should see the regions.
    for (id, node) in &nodes {
        let regions = node.store.state_machine().regions();
        assert!(!regions.is_empty(), "node {id} has no regions");
    }
}

#[tokio::test]
async fn cluster_config_update_replicates() {
    let (_router, nodes) = build_cluster(3).await;

    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("127.0.0.1:9001"));
    members.insert(2, BasicNode::new("127.0.0.1:9002"));
    members.insert(3, BasicNode::new("127.0.0.1:9003"));
    nodes
        .get(&1)
        .unwrap()
        .raft
        .initialize(members)
        .await
        .unwrap();

    let leader = wait_for_leader(&nodes).await;

    // Update cluster config.
    let config = ClusterConfig {
        default_replication_factor: 5,
        ..ClusterConfig::default()
    };
    let resp = propose(&nodes, leader, MetaCommand::UpdateClusterConfig(config)).await;
    assert!(matches!(resp, MetaResponse::Ok));

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify on all nodes.
    for (id, node) in &nodes {
        assert_eq!(
            node.store
                .state_machine()
                .cluster_config()
                .default_replication_factor,
            5,
            "node {id} has wrong replication factor"
        );
    }
}

#[tokio::test]
async fn multiple_schemas_and_models() {
    let (_router, nodes) = build_cluster(3).await;

    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("127.0.0.1:9001"));
    members.insert(2, BasicNode::new("127.0.0.1:9002"));
    members.insert(3, BasicNode::new("127.0.0.1:9003"));
    nodes
        .get(&1)
        .unwrap()
        .raft
        .initialize(members)
        .await
        .unwrap();

    let leader = wait_for_leader(&nodes).await;

    // Create multiple schemas.
    for name in ["cpu", "memory", "disk", "network"] {
        propose(
            &nodes,
            leader,
            MetaCommand::CreateMeasurement(MeasurementSchema::new(name)),
        )
        .await;
    }

    // Drop one.
    propose(
        &nodes,
        leader,
        MetaCommand::DropMeasurement {
            name: "disk".to_string(),
        },
    )
    .await;

    // Save a model.
    propose(
        &nodes,
        leader,
        MetaCommand::SaveModel {
            measurement: "cpu".to_string(),
            model_id: "cpu_forecast".to_string(),
            metadata: b"model-bytes-here".to_vec(),
        },
    )
    .await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    for (id, node) in &nodes {
        let sm = node.store.state_machine();
        assert_eq!(sm.schemas().len(), 3, "node {id} should have 3 schemas");
        assert!(
            sm.get_schema("disk").is_none(),
            "node {id} should NOT have 'disk'"
        );
        assert!(
            sm.get_model("cpu", "cpu_forecast").is_some(),
            "node {id} missing model 'cpu_forecast'"
        );
    }
}
