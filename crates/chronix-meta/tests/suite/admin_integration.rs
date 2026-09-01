//! Admin gRPC service integration test.
//!
//! Starts a MetaNode with both Raft and Admin gRPC services, then uses
//! [`GrpcMetaClient`] to exercise the full admin API round-trip.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, Raft};
use tonic::transport::Server;

use chronix_meta::{
    DataNodeInfo, GrpcMetaClient, MeasurementSchema, MetaAdminServer, MetaCommand,
    MetaNetworkFactory, MetaRouter, MetaStore, MetaTypeConfig, RegionInfo,
};

/// Build a single-node Raft cluster and return the Raft handle + store.
async fn setup_raft() -> (Arc<Raft<MetaTypeConfig>>, MetaStore) {
    let router = MetaRouter::new();
    let store = MetaStore::new_in_memory();

    let config = Arc::new(
        Config {
            heartbeat_interval: 20,
            election_timeout_min: 50,
            election_timeout_max: 100,
            ..Default::default()
        }
        .validate()
        .expect("valid config"),
    );

    let net = MetaNetworkFactory::new(router.clone());
    let raft = Raft::<MetaTypeConfig>::new(1, config, net, store.log_store(), store.sm_store())
        .await
        .expect("raft init");

    router.add_node(1, raft.clone());

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::new("127.0.0.1:19001"));
    raft.initialize(members).await.expect("init");

    // Wait for leader election.
    tokio::time::sleep(Duration::from_millis(150)).await;

    (Arc::new(raft), store)
}

/// Start the admin gRPC server, returning the bound port.
async fn start_admin_server(raft: Arc<Raft<MetaTypeConfig>>, store: &MetaStore) -> u16 {
    let admin = MetaAdminServer::new(raft, store.sm_store());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let port = addr.port();

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        Server::builder()
            .add_service(admin.into_service())
            .serve_with_incoming(incoming)
            .await
            .expect("admin grpc server");
    });

    // Give the server a moment to start.
    tokio::time::sleep(Duration::from_millis(30)).await;
    port
}

#[tokio::test]
async fn admin_register_and_list_nodes() {
    let (raft, store) = setup_raft().await;
    let port = start_admin_server(raft, &store).await;

    let client = GrpcMetaClient::new(vec![format!("127.0.0.1:{port}")]);

    // Register two data nodes.
    let node1 = DataNodeInfo::new(10, "10.0.0.1:9000");
    let node2 = DataNodeInfo::new(20, "10.0.0.2:9000");

    client.register_node(node1).await.expect("register node1");
    client.register_node(node2).await.expect("register node2");

    // List nodes.
    let (nodes, leader_id) = client.list_nodes().await.expect("list nodes");
    assert!(leader_id > 0, "should have a leader");
    assert_eq!(nodes.len(), 2);

    let ids: Vec<u64> = nodes.iter().map(|n| n.node_id).collect();
    assert!(ids.contains(&10));
    assert!(ids.contains(&20));
}

#[tokio::test]
async fn admin_heartbeat() {
    let (raft, store) = setup_raft().await;
    let port = start_admin_server(raft, &store).await;

    let client = GrpcMetaClient::new(vec![format!("127.0.0.1:{port}")]);

    // Register a node first.
    client
        .register_node(DataNodeInfo::new(10, "10.0.0.1:9000"))
        .await
        .expect("register");

    // Send heartbeat.
    client.heartbeat(10, 1).await.expect("heartbeat");

    // Verify heartbeat updated in out-of-band store.
    let sm = store.state_machine();
    let hb_data = sm.heartbeat_data();
    let (gen, _ts) = hb_data.get(&10).expect("heartbeat recorded");
    assert_eq!(*gen, 1);
}

#[tokio::test]
async fn admin_create_region_and_routing_table() {
    let (raft, store) = setup_raft().await;
    let port = start_admin_server(raft, &store).await;

    let client = GrpcMetaClient::new(vec![format!("127.0.0.1:{port}")]);

    // Register a node.
    client
        .register_node(DataNodeInfo::new(10, "10.0.0.1:9000"))
        .await
        .expect("register");

    // Create a region.
    let region = RegionInfo::new(1, "cpu", 10, vec![10]);
    client.create_region(region).await.expect("create region");

    // Get routing table.
    let routing = client.get_routing_table().await.expect("routing");
    assert!(routing.version > 0);
    assert!(routing.entries.contains_key("cpu"));

    let cpu_routes = &routing.entries["cpu"];
    assert_eq!(cpu_routes.len(), 1);
    assert_eq!(cpu_routes[0].region_id, 1);
    assert_eq!(cpu_routes[0].leader_node_id, 10);
}

#[tokio::test]
async fn admin_propose_create_measurement() {
    let (raft, store) = setup_raft().await;
    let port = start_admin_server(raft, &store).await;

    let client = GrpcMetaClient::new(vec![format!("127.0.0.1:{port}")]);

    // Propose a CreateMeasurement command.
    let schema = MeasurementSchema::new("temperature");
    let resp = client
        .propose(MetaCommand::CreateMeasurement(schema))
        .await
        .expect("propose");

    assert!(
        matches!(resp, chronix_meta::MetaResponse::Created { .. }),
        "expected Created response"
    );

    // Verify schema exists in state machine.
    let sm = store.state_machine();
    assert!(sm.get_schema("temperature").is_some());
}

#[tokio::test]
async fn admin_deregister_node() {
    let (raft, store) = setup_raft().await;
    let port = start_admin_server(raft, &store).await;

    let client = GrpcMetaClient::new(vec![format!("127.0.0.1:{port}")]);

    client
        .register_node(DataNodeInfo::new(10, "10.0.0.1:9000"))
        .await
        .expect("register");
    client.deregister_node(10).await.expect("deregister");

    // Verify node removed.
    let sm = store.state_machine();
    assert!(sm.get_node(10).is_none());
}

#[tokio::test]
async fn admin_cluster_info() {
    let (raft, store) = setup_raft().await;
    let port = start_admin_server(raft, &store).await;

    let client = GrpcMetaClient::new(vec![format!("127.0.0.1:{port}")]);

    let info = client.get_cluster_info().await.expect("cluster info");
    assert_eq!(info.leader_id, 1);
    assert!(info.voter_ids.contains(&1));
    assert!(info.config.is_some());

    let config = info.config.unwrap();
    assert_eq!(config.cluster_name, "chronix");
    assert_eq!(config.default_replication_factor, 3);
}
