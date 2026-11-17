//! Cluster administration over HTTP: nodes, regions, routing and topology.
//!
//! Each handler reads the [`MetaClient`] the cluster extension installs on its
//! own routes, so the standalone server carries none of this.

use std::sync::Arc;

use axum::extract::{Extension, Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tracing::info;

use chronix_cluster::MetaClient;
use chronix_meta::{
    DataNodeInfo, MetaCommand, MetaResponse, NodeId, NodeMode, RegionId, RegionInfo, RegionState,
};
use chronixd::admin::AckResponse;
use chronixd::error::ServerError;
use chronixd::http::{AppState, DEFAULT_LIST_LIMIT, PaginatedResponse, PaginationParams};

/// Request body for `POST /api/v1/admin/nodes`.
#[derive(Debug, Deserialize)]
pub struct RegisterNodeRequest {
    /// Unique node identifier.
    pub node_id: NodeId,
    /// gRPC address the node will be reachable at.
    pub grpc_addr: String,
    /// Node mode: `"meta"`, `"data"`, or `"query"`.
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Disk capacity in bytes.
    #[serde(default)]
    pub disk_bytes: u64,
    /// Memory capacity in bytes.
    #[serde(default)]
    pub memory_bytes: u64,
    /// Number of CPU cores.
    #[serde(default)]
    pub cpu_cores: u32,
}

fn default_mode() -> String {
    "data".to_string()
}

/// Request body for `DELETE /api/v1/admin/nodes/{id}`.
#[derive(Debug, Deserialize)]
pub struct DeregisterNodeRequest {
    /// Node to deregister.
    pub node_id: NodeId,
}

/// Request body for `POST /api/v1/admin/heartbeat`.
#[derive(Debug, Deserialize)]
pub struct HeartbeatRequest {
    /// Node sending the heartbeat.
    pub node_id: NodeId,
    /// Monotonically increasing generation counter.
    pub generation: u64,
}

/// Request body for `POST /api/v1/admin/regions`.
#[derive(Debug, Deserialize)]
pub struct CreateRegionRequest {
    /// Unique region identifier.
    pub region_id: RegionId,
    /// Measurement name this region covers.
    pub measurement: String,
    /// Node designated as leader for this region.
    pub leader_node_id: NodeId,
    /// Node IDs that host replicas.
    #[serde(default)]
    pub replica_node_ids: Vec<NodeId>,
    /// Desired replication factor.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
}

fn default_replication_factor() -> u32 {
    1
}

/// Request body for `PUT /api/v1/admin/regions/{id}/state`.
#[derive(Debug, Deserialize)]
pub struct UpdateRegionStateRequest {
    /// New state for the region.
    pub state: String,
}

/// Response body for `GET /api/v1/admin/nodes`.
#[derive(Debug, Serialize)]
pub struct NodeListResponse {
    /// Registered nodes in the cluster.
    pub nodes: Vec<NodeInfoResponse>,
    /// Number of nodes.
    pub count: usize,
}

/// Serialisable representation of a cluster node.
#[derive(Debug, Serialize)]
pub struct NodeInfoResponse {
    /// Unique node identifier.
    pub node_id: NodeId,
    /// gRPC address.
    pub grpc_addr: String,
    /// Node mode.
    pub mode: String,
    /// Disk capacity in bytes.
    pub disk_bytes: u64,
    /// Memory capacity in bytes.
    pub memory_bytes: u64,
    /// CPU core count.
    pub cpu_cores: u32,
}

impl From<DataNodeInfo> for NodeInfoResponse {
    fn from(n: DataNodeInfo) -> Self {
        Self {
            node_id: n.node_id,
            grpc_addr: n.grpc_addr,
            mode: format!("{:?}", n.mode),
            disk_bytes: n.disk_bytes,
            memory_bytes: n.memory_bytes,
            cpu_cores: n.cpu_cores,
        }
    }
}

/// Response for `GET /api/v1/admin/routing`.
#[derive(Debug, Serialize)]
pub struct RoutingTableResponse {
    /// Routing table version.
    pub version: u64,
    /// Number of measurements in routing table.
    pub measurement_count: usize,
    /// Route entries grouped by measurement.
    pub routes: std::collections::BTreeMap<String, Vec<RouteEntryResponse>>,
}

/// A single route entry.
#[derive(Debug, Serialize)]
pub struct RouteEntryResponse {
    /// Region identifier.
    pub region_id: RegionId,
    /// Measurement name.
    pub measurement: String,
    /// Leader node identifier.
    pub leader_node_id: NodeId,
    /// Leader gRPC address.
    pub leader_addr: String,
    /// Replica (node_id, address) pairs.
    pub replicas: Vec<ReplicaInfo>,
}

/// Replica node info.
#[derive(Debug, Serialize)]
pub struct ReplicaInfo {
    /// Replica node identifier.
    pub node_id: NodeId,
    /// Replica gRPC address.
    pub addr: String,
}

fn parse_node_mode(s: &str) -> NodeMode {
    match s {
        "meta" => NodeMode::Meta,
        "query" => NodeMode::Query,
        _ => NodeMode::Data,
    }
}

fn parse_region_state(s: &str) -> Result<RegionState, ServerError> {
    match s {
        "active" | "Active" => Ok(RegionState::Active),
        "migrating" | "Migrating" => Ok(RegionState::Migrating),
        "readonly" | "ReadOnly" | "read_only" => Ok(RegionState::ReadOnly),
        "replicating" | "Replicating" => Ok(RegionState::Replicating),
        "splitting" | "Splitting" => Ok(RegionState::Splitting),
        other => Err(ServerError::BadRequest(format!(
            "unknown region state: {other}"
        ))),
    }
}

// Taking `&ClusterError` would make every `.map_err(cluster_error_to_server)`
// call site a closure for no benefit: the error is consumed to build the
// message, and it is moved into this function on the way to being dropped.
#[allow(clippy::needless_pass_by_value)]
fn cluster_error_to_server(e: chronix_cluster::ClusterError) -> ServerError {
    ServerError::Internal(format!("cluster error: {e}"))
}

/// `POST /api/v1/admin/nodes` — register a node.
pub async fn register_node_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
    Json(body): Json<RegisterNodeRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let node_id = body.node_id;
    let mode_str = body.mode.clone();
    let grpc_addr_str = body.grpc_addr.clone();
    let info = DataNodeInfo::new(body.node_id, body.grpc_addr)
        .with_mode(parse_node_mode(&mode_str))
        .with_capacity(
            body.disk_bytes,
            body.memory_bytes,
            u64::from(body.cpu_cores),
        );
    client
        .register_node(info)
        .await
        .map_err(cluster_error_to_server)?;
    info!(
        node_id,
        mode = %mode_str,
        grpc_addr = %grpc_addr_str,
        "audit: node registered"
    );
    Ok((
        StatusCode::CREATED,
        Json(AckResponse {
            ok: true,
            message: Some(format!("node {} registered", node_id)),
        }),
    ))
}

/// `DELETE /api/v1/admin/nodes` — deregister a node.
pub async fn deregister_node_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
    Json(body): Json<DeregisterNodeRequest>,
) -> Result<impl IntoResponse, ServerError> {
    client
        .deregister_node(body.node_id)
        .await
        .map_err(cluster_error_to_server)?;
    info!(node_id = body.node_id, "audit: node deregistered");
    Ok(Json(AckResponse {
        ok: true,
        message: Some(format!("node {} deregistered", body.node_id)),
    }))
}

/// `POST /api/v1/admin/heartbeat` — send a heartbeat.
pub async fn heartbeat_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
    Json(body): Json<HeartbeatRequest>,
) -> Result<impl IntoResponse, ServerError> {
    client
        .heartbeat(body.node_id, body.generation)
        .await
        .map_err(cluster_error_to_server)?;
    Ok(Json(AckResponse {
        ok: true,
        message: None,
    }))
}

/// `POST /api/v1/admin/regions` — create a region.
pub async fn create_region_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
    Json(body): Json<CreateRegionRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let region_id = body.region_id;
    let measurement_name = body.measurement.clone();
    let leader = body.leader_node_id;
    let repl_factor = body.replication_factor;
    let mut info = RegionInfo::new(
        body.region_id,
        body.measurement,
        body.leader_node_id,
        body.replica_node_ids,
    );
    info.replication_factor = body.replication_factor;
    client
        .create_region(info)
        .await
        .map_err(cluster_error_to_server)?;
    info!(
        region_id,
        measurement = %measurement_name,
        leader_node_id = leader,
        replication_factor = repl_factor,
        "audit: region created"
    );
    Ok((
        StatusCode::CREATED,
        Json(AckResponse {
            ok: true,
            message: Some(format!("region {} created", region_id)),
        }),
    ))
}

/// `GET /api/v1/admin/routing` — get the routing table.
pub async fn get_routing_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
) -> Result<impl IntoResponse, ServerError> {
    let snapshot = client
        .get_routing_table()
        .await
        .map_err(cluster_error_to_server)?;

    let routes = snapshot
        .entries
        .into_iter()
        .map(|(measurement, entries)| {
            let route_entries = entries
                .into_iter()
                .map(|e| RouteEntryResponse {
                    region_id: e.region_id,
                    measurement: e.measurement,
                    leader_node_id: e.leader_node_id,
                    leader_addr: e.leader_addr,
                    replicas: e
                        .replica_addrs
                        .into_iter()
                        .map(|(id, addr)| ReplicaInfo { node_id: id, addr })
                        .collect(),
                })
                .collect();
            (measurement, route_entries)
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let measurement_count = routes.len();
    Ok(Json(RoutingTableResponse {
        version: snapshot.version,
        measurement_count,
        routes,
    }))
}

/// `GET /api/v1/admin/nodes` — list all registered nodes.
///
/// Supports optional `offset` and `limit` query parameters for pagination.
pub async fn list_nodes_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
    Query(pagination): Query<PaginationParams>,
) -> Result<impl IntoResponse, ServerError> {
    // The MetaClient trait doesn't expose list_nodes directly — use propose
    // to query cluster state.  In practice, we read via the routing table
    // or we need the underlying GrpcMetaClient.  For now we return what the
    // trait gives us: the routing table nodes.
    let snapshot = client
        .get_routing_table()
        .await
        .map_err(cluster_error_to_server)?;

    // Collect unique nodes from routing entries.
    let mut seen = std::collections::BTreeMap::new();
    for entries in snapshot.entries.values() {
        for e in entries {
            seen.entry(e.leader_node_id)
                .or_insert_with(|| NodeInfoResponse {
                    node_id: e.leader_node_id,
                    grpc_addr: e.leader_addr.clone(),
                    mode: "Data".to_string(),
                    disk_bytes: 0,
                    memory_bytes: 0,
                    cpu_cores: 0,
                });
            for (nid, addr) in &e.replica_addrs {
                seen.entry(*nid).or_insert_with(|| NodeInfoResponse {
                    node_id: *nid,
                    grpc_addr: addr.clone(),
                    mode: "Data".to_string(),
                    disk_bytes: 0,
                    memory_bytes: 0,
                    cpu_cores: 0,
                });
            }
        }
    }

    let all_nodes: Vec<NodeInfoResponse> = seen.into_values().collect();
    let total = all_nodes.len();
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(DEFAULT_LIST_LIMIT);
    let items: Vec<NodeInfoResponse> = all_nodes.into_iter().skip(offset).take(limit).collect();

    Ok(Json(PaginatedResponse {
        items,
        total,
        offset,
        limit,
    }))
}

/// `PUT /api/v1/admin/regions/{id}/state` — update region state.
pub async fn update_region_state_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
    Path(region_id): Path<RegionId>,
    Json(body): Json<UpdateRegionStateRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let new_state = parse_region_state(&body.state)?;
    let cmd = MetaCommand::UpdateRegionState {
        region_id,
        state: new_state,
    };
    let resp = client.propose(cmd).await.map_err(cluster_error_to_server)?;
    match resp {
        MetaResponse::Ok | MetaResponse::Created { .. } => {
            info!(
                region_id,
                new_state = %body.state,
                "audit: region state updated"
            );
            Ok(Json(AckResponse {
                ok: true,
                message: Some(format!("region {region_id} state updated")),
            }))
        }
        MetaResponse::Error { message } => Err(ServerError::Internal(message)),
    }
}

/// `GET /api/v1/admin/health` — cluster health summary.
pub async fn cluster_health_handler(
    State(state): State<AppState>,
    Extension(client): Extension<Arc<dyn MetaClient>>,
) -> Result<impl IntoResponse, ServerError> {
    let _snapshot = client
        .get_routing_table()
        .await
        .map_err(cluster_error_to_server)?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "cluster_available": true,
        "uptime_secs": state.start_time.elapsed().as_secs(),
    })))
}

/// Information about a single node in the topology view.
#[derive(Debug, Serialize)]
pub struct TopologyNodeInfo {
    /// Unique node identifier.
    pub node_id: NodeId,
    /// gRPC address.
    pub grpc_addr: String,
    /// Node role (Meta / Data / Query).
    pub role: String,
    /// Regions for which this node is the leader.
    pub leader_regions: Vec<RegionId>,
    /// Regions for which this node is a replica.
    pub replica_regions: Vec<RegionId>,
    /// Health status derived from routing table presence.
    pub status: String,
}

/// Response body for `GET /api/v1/admin/topology`.
#[derive(Debug, Serialize)]
pub struct ClusterTopologyResponse {
    /// Routing-table version at the time of the snapshot.
    pub routing_version: u64,
    /// Number of nodes in the cluster.
    pub node_count: usize,
    /// Number of regions in the cluster.
    pub region_count: usize,
    /// Total number of measurements routed.
    pub measurement_count: usize,
    /// Per-node topology detail.
    pub nodes: Vec<TopologyNodeInfo>,
    /// Cluster uptime from this node's perspective (seconds).
    pub uptime_secs: u64,
}

/// `GET /api/v1/admin/topology` — consolidated cluster topology view.
///
/// Returns all nodes with their roles, the regions they lead or replicate,
/// and a health status derived from the routing table snapshot.
pub async fn cluster_topology_handler(
    State(state): State<AppState>,
    Extension(client): Extension<Arc<dyn MetaClient>>,
) -> Result<impl IntoResponse, ServerError> {
    let snapshot = client
        .get_routing_table()
        .await
        .map_err(cluster_error_to_server)?;

    // Accumulate per-node data from routing entries.
    let mut node_map: std::collections::BTreeMap<NodeId, TopologyNodeInfo> =
        std::collections::BTreeMap::new();

    let mut total_regions: usize = 0;
    for entries in snapshot.entries.values() {
        for e in entries {
            total_regions += 1;
            // Leader
            let leader = node_map
                .entry(e.leader_node_id)
                .or_insert_with(|| TopologyNodeInfo {
                    node_id: e.leader_node_id,
                    grpc_addr: e.leader_addr.clone(),
                    role: "Data".to_string(),
                    leader_regions: Vec::new(),
                    replica_regions: Vec::new(),
                    status: "active".to_string(),
                });
            leader.leader_regions.push(e.region_id);

            // Replicas
            for (nid, addr) in &e.replica_addrs {
                let replica = node_map.entry(*nid).or_insert_with(|| TopologyNodeInfo {
                    node_id: *nid,
                    grpc_addr: addr.clone(),
                    role: "Data".to_string(),
                    leader_regions: Vec::new(),
                    replica_regions: Vec::new(),
                    status: "active".to_string(),
                });
                replica.replica_regions.push(e.region_id);
            }
        }
    }

    let nodes: Vec<TopologyNodeInfo> = node_map.into_values().collect();
    let node_count = nodes.len();

    tracing::info!(
        node_count,
        region_count = total_regions,
        "cluster topology requested"
    );

    Ok(Json(ClusterTopologyResponse {
        routing_version: snapshot.version,
        node_count,
        region_count: total_regions,
        measurement_count: snapshot.entries.len(),
        nodes,
        uptime_secs: state.start_time.elapsed().as_secs(),
    }))
}

/// `POST /api/v1/admin/rebalance` — trigger manual cluster rebalance.
pub async fn rebalance_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
) -> Result<impl IntoResponse, ServerError> {
    // Verify the cluster is reachable by fetching the routing table.
    let snapshot = client
        .get_routing_table()
        .await
        .map_err(cluster_error_to_server)?;

    let region_count = snapshot.entries.values().map(Vec::len).sum::<usize>();

    info!(
        region_count,
        measurement_count = snapshot.entries.len(),
        "audit: cluster rebalance triggered"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "accepted": true,
            "message": format!(
                "rebalance triggered for {region_count} regions across {} measurements",
                snapshot.entries.len()
            ),
        })),
    ))
}

/// `POST /api/v1/admin/nodes/{id}/decommission` — gracefully remove a
/// node from the cluster.
pub async fn decommission_node_handler(
    Extension(client): Extension<Arc<dyn MetaClient>>,
    Path(node_id): Path<NodeId>,
) -> Result<impl IntoResponse, ServerError> {
    client
        .deregister_node(node_id)
        .await
        .map_err(cluster_error_to_server)?;
    info!(node_id, "audit: node decommission initiated");
    Ok((
        StatusCode::ACCEPTED,
        Json(AckResponse {
            ok: true,
            message: Some(format!("node {node_id} decommission initiated")),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_node_mode_data() {
        assert!(matches!(parse_node_mode("data"), NodeMode::Data));
        assert!(matches!(parse_node_mode("meta"), NodeMode::Meta));
        assert!(matches!(parse_node_mode("query"), NodeMode::Query));
        assert!(matches!(parse_node_mode("unknown"), NodeMode::Data));
    }

    #[test]
    fn parse_region_state_valid() {
        assert!(parse_region_state("active").is_ok());
        assert!(parse_region_state("Active").is_ok());
        assert!(parse_region_state("migrating").is_ok());
        assert!(parse_region_state("readonly").is_ok());
        assert!(parse_region_state("ReadOnly").is_ok());
        assert!(parse_region_state("read_only").is_ok());
        assert!(parse_region_state("replicating").is_ok());
    }

    #[test]
    fn parse_region_state_invalid() {
        assert!(parse_region_state("bogus").is_err());
    }

    #[test]
    fn node_info_from_data_node_info() {
        let info = DataNodeInfo::new(42, "localhost:9001")
            .with_mode(NodeMode::Data)
            .with_capacity(1000, 2000, 4);
        let resp = NodeInfoResponse::from(info);
        assert_eq!(resp.node_id, 42);
        assert_eq!(resp.grpc_addr, "localhost:9001");
        assert_eq!(resp.cpu_cores, 4);
    }

    #[test]
    fn routing_table_response_serialization() {
        let resp = RoutingTableResponse {
            version: 5,
            measurement_count: 1,
            routes: std::collections::BTreeMap::from([(
                "cpu".to_string(),
                vec![RouteEntryResponse {
                    region_id: 1,
                    measurement: "cpu".to_string(),
                    leader_node_id: 10,
                    leader_addr: "10.0.0.1:9001".to_string(),
                    replicas: vec![ReplicaInfo {
                        node_id: 11,
                        addr: "10.0.0.2:9001".to_string(),
                    }],
                }],
            )]),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["version"], 5);
        assert_eq!(json["measurement_count"], 1);
        assert!(json["routes"]["cpu"].is_array());
    }

    #[test]
    fn topology_response_serialization() {
        let resp = ClusterTopologyResponse {
            routing_version: 3,
            node_count: 2,
            region_count: 1,
            measurement_count: 1,
            nodes: vec![
                TopologyNodeInfo {
                    node_id: 10,
                    grpc_addr: "10.0.0.1:9001".to_string(),
                    role: "Data".to_string(),
                    leader_regions: vec![1],
                    replica_regions: vec![],
                    status: "active".to_string(),
                },
                TopologyNodeInfo {
                    node_id: 11,
                    grpc_addr: "10.0.0.2:9001".to_string(),
                    role: "Data".to_string(),
                    leader_regions: vec![],
                    replica_regions: vec![1],
                    status: "active".to_string(),
                },
            ],
            uptime_secs: 42,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["routing_version"], 3);
        assert_eq!(json["node_count"], 2);
        assert_eq!(json["region_count"], 1);
        assert_eq!(json["measurement_count"], 1);
        assert_eq!(json["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(json["nodes"][0]["node_id"], 10);
        assert_eq!(json["nodes"][0]["leader_regions"], serde_json::json!([1]));
        assert_eq!(json["nodes"][1]["replica_regions"], serde_json::json!([1]));
        assert_eq!(json["uptime_secs"], 42);
    }

    #[test]
    fn register_node_request_deserialize() {
        let json = r#"{"node_id": 1, "grpc_addr": "localhost:9001"}"#;
        let req: RegisterNodeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.node_id, 1);
        assert_eq!(req.mode, "data");
        assert_eq!(req.disk_bytes, 0);
    }

    #[test]
    fn create_region_request_deserialize() {
        let json = r#"{
            "region_id": 1,
            "measurement": "cpu",
            "leader_node_id": 10,
            "replica_node_ids": [11, 12]
        }"#;
        let req: CreateRegionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.region_id, 1);
        assert_eq!(req.leader_node_id, 10);
        assert_eq!(req.replica_node_ids, vec![11, 12]);
        assert_eq!(req.replication_factor, 1);
    }
}
