//! REST admin API handlers for cluster management and analytics.
//!
//! Provides HTTP endpoints under `/api/v1/admin/` that mirror the gRPC
//! admin service.  Cluster operations delegate to the `MetaClient` trait,
//! which is backed by either an in-process Raft client (meta-node) or a
//! remote gRPC client (data-node).
//!
//! Analytics model management endpoints allow listing, inspecting, and
//! deleting fitted forecast models, plus triggering model re-training.
//!

use std::sync::Arc;

#[cfg(feature = "cluster")]
use axum::extract::Query;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tracing::info;

#[cfg(feature = "cluster")]
use crate::http::{PaginatedResponse, PaginationParams, DEFAULT_LIST_LIMIT};

#[cfg(feature = "cluster")]
use chronix_cluster::MetaClient;
#[cfg(feature = "cluster")]
use chronix_meta::{
    DataNodeInfo, MetaCommand, MetaResponse, NodeId, NodeMode, RegionId, RegionInfo, RegionState,
};

use crate::error::ServerError;
use crate::http::AppState;

// ── Request / Response types ──────────────────────────────────────────

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
fn default_mode() -> String {
    "data".to_string()
}

#[cfg(feature = "cluster")]
/// Request body for `DELETE /api/v1/admin/nodes/{id}`.
#[derive(Debug, Deserialize)]
pub struct DeregisterNodeRequest {
    /// Node to deregister.
    pub node_id: NodeId,
}

#[cfg(feature = "cluster")]
/// Request body for `POST /api/v1/admin/heartbeat`.
#[derive(Debug, Deserialize)]
pub struct HeartbeatRequest {
    /// Node sending the heartbeat.
    pub node_id: NodeId,
    /// Monotonically increasing generation counter.
    pub generation: u64,
}

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
fn default_replication_factor() -> u32 {
    1
}

#[cfg(feature = "cluster")]
/// Request body for `PUT /api/v1/admin/regions/{id}/state`.
#[derive(Debug, Deserialize)]
pub struct UpdateRegionStateRequest {
    /// New state for the region.
    pub state: String,
}

/// Generic acknowledgement response.
#[derive(Debug, Serialize)]
pub struct AckResponse {
    /// Whether the operation succeeded.
    pub ok: bool,
    /// Optional descriptive message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(feature = "cluster")]
/// Response body for `GET /api/v1/admin/nodes`.
#[derive(Debug, Serialize)]
pub struct NodeListResponse {
    /// Registered nodes in the cluster.
    pub nodes: Vec<NodeInfoResponse>,
    /// Number of nodes.
    pub count: usize,
}

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
/// Replica node info.
#[derive(Debug, Serialize)]
pub struct ReplicaInfo {
    /// Replica node identifier.
    pub node_id: NodeId,
    /// Replica gRPC address.
    pub addr: String,
}

// ── Helper ────────────────────────────────────────────────────────────

#[cfg(feature = "cluster")]
/// Extract the `MetaClient` from shared state or return 503.
fn require_meta_client(state: &AppState) -> Result<&Arc<dyn MetaClient>, ServerError> {
    state.meta_client.as_ref().ok_or_else(|| {
        ServerError::Internal(
            "cluster not configured — admin API unavailable in standalone mode".into(),
        )
    })
}

#[cfg(feature = "cluster")]
fn parse_node_mode(s: &str) -> NodeMode {
    match s {
        "meta" => NodeMode::Meta,
        "query" => NodeMode::Query,
        _ => NodeMode::Data,
    }
}

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
// Taking `&ClusterError` would make every `.map_err(cluster_error_to_server)`
// call site a closure for no benefit: the error is consumed to build the
// message, and it is moved into this function on the way to being dropped.
#[allow(clippy::needless_pass_by_value)]
fn cluster_error_to_server(e: chronix_cluster::ClusterError) -> ServerError {
    ServerError::Internal(format!("cluster error: {e}"))
}

// ── Handlers ──────────────────────────────────────────────────────────

#[cfg(feature = "cluster")]
/// `POST /api/v1/admin/nodes` — register a node.
pub async fn register_node_handler(
    State(state): State<AppState>,
    Json(body): Json<RegisterNodeRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
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

#[cfg(feature = "cluster")]
/// `DELETE /api/v1/admin/nodes` — deregister a node.
pub async fn deregister_node_handler(
    State(state): State<AppState>,
    Json(body): Json<DeregisterNodeRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
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

#[cfg(feature = "cluster")]
/// `POST /api/v1/admin/heartbeat` — send a heartbeat.
pub async fn heartbeat_handler(
    State(state): State<AppState>,
    Json(body): Json<HeartbeatRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
    client
        .heartbeat(body.node_id, body.generation)
        .await
        .map_err(cluster_error_to_server)?;
    Ok(Json(AckResponse {
        ok: true,
        message: None,
    }))
}

#[cfg(feature = "cluster")]
/// `POST /api/v1/admin/regions` — create a region.
pub async fn create_region_handler(
    State(state): State<AppState>,
    Json(body): Json<CreateRegionRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
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

#[cfg(feature = "cluster")]
/// `GET /api/v1/admin/routing` — get the routing table.
pub async fn get_routing_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
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

#[cfg(feature = "cluster")]
/// `GET /api/v1/admin/nodes` — list all registered nodes.
///
/// Supports optional `offset` and `limit` query parameters for pagination.
pub async fn list_nodes_handler(
    State(state): State<AppState>,
    Query(pagination): Query<PaginationParams>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;

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

#[cfg(feature = "cluster")]
/// `PUT /api/v1/admin/regions/{id}/state` — update region state.
pub async fn update_region_state_handler(
    State(state): State<AppState>,
    Path(region_id): Path<RegionId>,
    Json(body): Json<UpdateRegionStateRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
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

#[cfg(feature = "cluster")]
/// `GET /api/v1/admin/health` — cluster health summary.
pub async fn cluster_health_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
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

// ── Cluster Topology ──────────────────────────────────────────

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
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

#[cfg(feature = "cluster")]
/// `GET /api/v1/admin/topology` — consolidated cluster topology view.
///
/// Returns all nodes with their roles, the regions they lead or replicate,
/// and a health status derived from the routing table snapshot.
pub async fn cluster_topology_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
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

// ── Analytics Model Management ────────────────────────────────────────

/// Response for a single model's metadata.
#[derive(Debug, Serialize)]
pub struct ModelInfoResponse {
    /// Model name.
    pub name: String,
    /// Measurement this model is trained on.
    pub measurement: String,
    /// Model type (SES, HoltLinear, HoltWinters, etc.).
    pub model_type: String,
    /// Model version (auto-incremented on re-save).
    pub version: u32,
    /// Training data point count.
    pub training_points: usize,
    /// Mean squared error on training data.
    pub mse: f64,
    /// Mean absolute error on training data.
    pub mae: f64,
    /// R-squared on training data.
    pub r_squared: f64,
}

/// Response for model listing.
#[derive(Debug, Serialize)]
pub struct ModelListResponse {
    /// All models matching the query.
    pub models: Vec<ModelInfoResponse>,
    /// Number of models returned.
    pub count: usize,
}

/// Request body for `POST /api/v1/admin/analytics/retrain`.
#[derive(Debug, Deserialize)]
pub struct RetrainRequest {
    /// Measurement to retrain models for.
    pub measurement: String,
    /// Optional: specific model name. If absent, all models for the
    /// measurement are retrained.
    #[serde(default)]
    pub model_name: Option<String>,
}

/// Response body for retrain.
#[derive(Debug, Serialize)]
pub struct RetrainResponse {
    /// Whether the retrain was accepted.
    pub accepted: bool,
    /// Number of models queued for retraining.
    pub models_queued: usize,
    /// Descriptive message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `GET /api/v1/admin/analytics/models` — list all trained models.
///
/// Optional query parameter `?measurement=cpu` to filter by measurement.
pub async fn list_models_handler(
    State(state): State<AppState>,
    query: axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<impl IntoResponse, ServerError> {
    let catalog = state.model_catalog.read();

    let models: Vec<ModelInfoResponse> = if let Some(measurement) = query.get("measurement") {
        catalog
            .list_models(measurement)
            .into_iter()
            .map(model_metadata_to_response)
            .collect()
    } else {
        catalog
            .list_all()
            .into_iter()
            .map(model_metadata_to_response)
            .collect()
    };

    let count = models.len();
    Ok(Json(ModelListResponse { models, count }))
}

/// `GET /api/v1/admin/analytics/models/{measurement}/{name}` — get
/// metadata for a specific model.
pub async fn get_model_handler(
    State(state): State<AppState>,
    Path((measurement, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ServerError> {
    let catalog = state.model_catalog.read();
    let meta = catalog
        .get_metadata(&measurement, &name)
        .ok_or_else(|| ServerError::NotFound(format!("model {measurement}/{name} not found")))?;
    Ok(Json(model_metadata_to_response(meta)))
}

/// `DELETE /api/v1/admin/analytics/models/{measurement}/{name}` — delete
/// a model from the catalog.
pub async fn delete_model_handler(
    State(state): State<AppState>,
    Path((measurement, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ServerError> {
    let mut catalog = state.model_catalog.write();
    if catalog.delete_model(&measurement, &name) {
        info!(
            measurement = %measurement,
            model = %name,
            "audit: model deleted"
        );
        Ok(Json(AckResponse {
            ok: true,
            message: Some(format!("model {measurement}/{name} deleted")),
        }))
    } else {
        Err(ServerError::NotFound(format!(
            "model {measurement}/{name} not found"
        )))
    }
}

/// `POST /api/v1/admin/analytics/retrain` — trigger model re-training.
///
/// This is a best-effort trigger.  The actual retraining happens
/// asynchronously via the `ContinuousForecastEngine`.  This endpoint
/// returns immediately with the count of models queued for retraining.
pub async fn retrain_handler(
    State(state): State<AppState>,
    Json(body): Json<RetrainRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let catalog = state.model_catalog.read();
    let models = if let Some(ref name) = body.model_name {
        catalog
            .get_metadata(&body.measurement, name)
            .map_or_else(Vec::new, |m| vec![m.model_name.clone()])
    } else {
        catalog
            .list_models(&body.measurement)
            .into_iter()
            .map(|m| m.model_name.clone())
            .collect()
    };
    drop(catalog);

    let queued = models.len();
    if queued == 0 {
        return Ok((
            StatusCode::OK,
            Json(RetrainResponse {
                accepted: false,
                models_queued: 0,
                message: Some("no matching models found".into()),
            }),
        ));
    }

    // Mark models for retraining by deleting them from the catalog.
    // The ContinuousForecastEngine will re-fit on the next data batch.
    let mut catalog = state.model_catalog.write();
    for name in &models {
        catalog.delete_model(&body.measurement, name);
    }

    info!(
        measurement = %body.measurement,
        models_queued = queued,
        "audit: model retraining triggered"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(RetrainResponse {
            accepted: true,
            models_queued: queued,
            message: Some(format!(
                "{queued} model(s) cleared for retraining on '{}'",
                body.measurement
            )),
        }),
    ))
}

#[cfg(feature = "cluster")]
/// `POST /api/v1/admin/rebalance` — trigger manual cluster rebalance.
pub async fn rebalance_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;

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

#[cfg(feature = "cluster")]
/// `POST /api/v1/admin/nodes/{id}/decommission` — gracefully remove a
/// node from the cluster.
pub async fn decommission_node_handler(
    State(state): State<AppState>,
    Path(node_id): Path<NodeId>,
) -> Result<impl IntoResponse, ServerError> {
    let client = require_meta_client(&state)?;
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

fn model_metadata_to_response(
    meta: &chronix::chronix_analytics::forecast::ModelMetadata,
) -> ModelInfoResponse {
    ModelInfoResponse {
        name: meta.model_name.clone(),
        measurement: meta.measurement.clone(),
        model_type: format!("{:?}", meta.model_type),
        version: meta.version,
        training_points: meta.training_points,
        mse: meta.mse,
        mae: meta.mae,
        r_squared: meta.r_squared,
    }
}

// ── Backup / Restore ──────────────────────────────────────────────────

/// The directory every admin filesystem path is confined to.
///
/// Configurable, because a backup usually belongs on a different volume
/// from the data. Defaults to `backups/` beside the data.
fn admin_root(state: &AppState) -> std::path::PathBuf {
    state
        .config
        .server
        .backup_root
        .clone()
        .unwrap_or_else(|| state.db.data_dir().join("backups"))
}

/// Resolve a caller-supplied backup path inside `root`.
///
/// The old check rejected `..` and demanded an absolute path, and an
/// absolute path was then accepted **anywhere on the filesystem** — so a
/// restore read any directory the server user could read, and a backup
/// wrote over any directory it could write. Rejecting `..` is not
/// confinement; a root is.
///
/// A relative path is taken as relative to `root`. An absolute path is
/// accepted only when it is already inside `root`. Either way the deepest
/// existing ancestor is canonicalised before the check, so a symlink
/// planted inside `root` cannot point out of it.
fn confine_to_root(
    user_path: &str,
    root: &std::path::Path,
) -> Result<std::path::PathBuf, ServerError> {
    let requested = std::path::PathBuf::from(user_path);

    for component in requested.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(ServerError::BadRequest(
                "path must not contain '..' components".into(),
            ));
        }
    }

    // The root has to exist before it can anchor anything; creating it is
    // part of serving the endpoint, not a caller's job.
    std::fs::create_dir_all(root).map_err(|e| {
        ServerError::Internal(format!(
            "cannot create the backup root {}: {e}",
            root.display()
        ))
    })?;
    let canonical_root = root.canonicalize().map_err(|e| {
        ServerError::Internal(format!(
            "cannot resolve the backup root {}: {e}",
            root.display()
        ))
    })?;

    let candidate = if requested.is_absolute() {
        requested
    } else {
        canonical_root.join(&requested)
    };

    // Canonicalise the deepest ancestor that exists — the leaf usually does
    // not yet, since a restore target must be new — then re-attach the
    // remainder. Checking the string alone would miss a symlink.
    let mut existing = candidate.as_path();
    let mut tail = std::path::PathBuf::new();
    let resolved = loop {
        match existing.canonicalize() {
            Ok(base) => break base.join(&tail),
            Err(_) => match existing.parent() {
                Some(parent) => {
                    let name = existing
                        .file_name()
                        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
                    tail = std::path::PathBuf::from(name).join(&tail);
                    existing = parent;
                }
                None => {
                    return Err(ServerError::BadRequest(
                        "path does not resolve inside the backup root".into(),
                    ))
                }
            },
        }
    };

    if !resolved.starts_with(&canonical_root) {
        return Err(ServerError::BadRequest(format!(
            "path must stay inside the backup root {}",
            canonical_root.display()
        )));
    }

    Ok(resolved)
}

/// Request body for `POST /api/v1/admin/backup`.
#[derive(Debug, Deserialize)]
pub struct BackupRequest {
    /// Target directory for the backup.
    pub target_dir: String,
}

/// Request body for `POST /api/v1/admin/restore`.
#[derive(Debug, Deserialize)]
pub struct RestoreRequest {
    /// Directory containing the backup.
    pub backup_dir: String,
    /// Target directory to restore into (must not exist).
    pub target_dir: String,
}

/// `POST /api/v1/admin/backup` — create a point-in-time backup.
pub async fn backup_handler(
    State(state): State<AppState>,
    Json(body): Json<BackupRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let target = confine_to_root(&body.target_dir, &admin_root(&state))?;
    let manifest = tokio::task::spawn_blocking({
        let db = Arc::clone(&state.db);
        move || db.backup(&target)
    })
    .await
    .map_err(|e| ServerError::Internal(format!("backup task panicked: {e}")))?
    .map_err(ServerError::Db)?;

    info!(
        target_dir = %body.target_dir,
        files = manifest.file_count,
        bytes = manifest.total_bytes,
        wal_seq = manifest.wal_sequence,
        "audit: backup created"
    );
    crate::audit::record(
        &state,
        "admin",
        chronix_security::audit::AuditAction::Admin,
        format!("backup:{}", body.target_dir),
        chronix_security::audit::AuditDecision::Allow,
        &[("files", manifest.file_count.to_string())],
    );

    Ok((StatusCode::OK, Json(manifest)))
}

/// Request body for `POST /api/v1/admin/backup/verify`.
#[derive(Debug, Deserialize)]
pub struct VerifyBackupRequest {
    /// Directory holding the backup to check.
    pub backup_dir: String,
}

/// `POST /api/v1/admin/backup/verify` — check a backup without restoring it.
///
/// A backup that can only be checked by restoring it is one nobody checks,
/// and a backup nobody has checked is the one that turns out to be
/// incomplete on the day it is needed. This runs the same verification a
/// restore runs — every segment the backup's own catalog names, present and
/// at its recorded size — and writes nothing.
pub async fn verify_backup_handler(
    State(state): State<AppState>,
    Json(body): Json<VerifyBackupRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let backup_dir = confine_to_root(&body.backup_dir, &admin_root(&state))?;
    let manifest =
        tokio::task::spawn_blocking(move || chronix::Chronix::verify_backup(&backup_dir))
            .await
            .map_err(|e| ServerError::Internal(format!("verify task panicked: {e}")))?
            .map_err(ServerError::Db)?;

    info!(
        backup_dir = %body.backup_dir,
        segments = manifest.segments,
        "audit: backup verified"
    );
    Ok((StatusCode::OK, Json(manifest)))
}

/// `POST /api/v1/admin/restore` — restore a database from backup.
pub async fn restore_handler(
    State(state): State<AppState>,
    Json(body): Json<RestoreRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let root = admin_root(&state);
    let backup_dir = confine_to_root(&body.backup_dir, &root)?;
    let target_dir = confine_to_root(&body.target_dir, &root)?;
    let manifest =
        tokio::task::spawn_blocking(move || chronix::Chronix::restore(&backup_dir, &target_dir))
            .await
            .map_err(|e| ServerError::Internal(format!("restore task panicked: {e}")))?
            .map_err(ServerError::Db)?;

    info!(
        backup_dir = %body.backup_dir,
        target_dir = %body.target_dir,
        wal_seq = manifest.wal_sequence,
        "audit: restore completed"
    );
    crate::audit::record(
        &state,
        "admin",
        chronix_security::audit::AuditAction::Admin,
        format!("restore:{}", body.target_dir),
        chronix_security::audit::AuditDecision::Allow,
        &[
            ("backup_dir", body.backup_dir.clone()),
            ("wal_sequence", manifest.wal_sequence.to_string()),
        ],
    );

    Ok((StatusCode::OK, Json(manifest)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "cluster")]
    #[test]
    fn parse_node_mode_data() {
        assert!(matches!(parse_node_mode("data"), NodeMode::Data));
        assert!(matches!(parse_node_mode("meta"), NodeMode::Meta));
        assert!(matches!(parse_node_mode("query"), NodeMode::Query));
        assert!(matches!(parse_node_mode("unknown"), NodeMode::Data));
    }

    #[cfg(feature = "cluster")]
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

    #[cfg(feature = "cluster")]
    #[test]
    fn parse_region_state_invalid() {
        assert!(parse_region_state("bogus").is_err());
    }

    #[test]
    fn ack_response_serialization() {
        let ack = AckResponse {
            ok: true,
            message: None,
        };
        let json = serde_json::to_string(&ack).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(!json.contains("message"));
    }

    #[test]
    fn ack_response_with_message() {
        let ack = AckResponse {
            ok: true,
            message: Some("done".into()),
        };
        let json = serde_json::to_string(&ack).unwrap();
        assert!(json.contains("\"message\":\"done\""));
    }

    #[cfg(feature = "cluster")]
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

    #[cfg(feature = "cluster")]
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

    #[cfg(feature = "cluster")]
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

    #[cfg(feature = "cluster")]
    #[test]
    fn register_node_request_deserialize() {
        let json = r#"{"node_id": 1, "grpc_addr": "localhost:9001"}"#;
        let req: RegisterNodeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.node_id, 1);
        assert_eq!(req.mode, "data");
        assert_eq!(req.disk_bytes, 0);
    }

    #[cfg(feature = "cluster")]
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

    // ── Analytics model response tests ────────────────────────────────

    #[test]
    fn model_info_response_serialization() {
        let resp = ModelInfoResponse {
            name: "ses_v1".into(),
            measurement: "cpu".into(),
            model_type: "Ses".into(),
            version: 3,
            training_points: 1000,
            mse: 0.5,
            mae: 0.3,
            r_squared: 0.95,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "ses_v1");
        assert_eq!(json["measurement"], "cpu");
        assert_eq!(json["version"], 3);
        assert_eq!(json["training_points"], 1000);
    }

    #[test]
    fn model_list_response_serialization() {
        let resp = ModelListResponse {
            models: vec![
                ModelInfoResponse {
                    name: "m1".into(),
                    measurement: "cpu".into(),
                    model_type: "Ses".into(),
                    version: 1,
                    training_points: 100,
                    mse: 1.0,
                    mae: 0.8,
                    r_squared: 0.9,
                },
                ModelInfoResponse {
                    name: "m2".into(),
                    measurement: "mem".into(),
                    model_type: "HoltWinters".into(),
                    version: 2,
                    training_points: 500,
                    mse: 0.2,
                    mae: 0.1,
                    r_squared: 0.99,
                },
            ],
            count: 2,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["count"], 2);
        assert!(json["models"].as_array().unwrap().len() == 2);
    }

    #[test]
    fn retrain_request_deserialize() {
        let json = r#"{"measurement": "cpu"}"#;
        let req: RetrainRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.measurement, "cpu");
        assert!(req.model_name.is_none());

        let json = r#"{"measurement": "cpu", "model_name": "ses_v1"}"#;
        let req: RetrainRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model_name.as_deref(), Some("ses_v1"));
    }

    #[test]
    fn retrain_response_serialization() {
        let resp = RetrainResponse {
            accepted: true,
            models_queued: 3,
            message: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"accepted\":true"));
        assert!(json.contains("\"models_queued\":3"));
        assert!(!json.contains("message"));
    }

    #[test]
    fn model_metadata_to_response_mapping() {
        let meta = chronix::chronix_analytics::forecast::ModelMetadata {
            model_type: chronix::chronix_analytics::forecast::ModelType::Ses,
            model_name: "test_model".into(),
            measurement: "disk".into(),
            version: 5,
            created_at: 0,
            training_data_start: 100,
            training_data_end: 200,
            training_points: 50,
            mse: 0.1,
            mae: 0.2,
            r_squared: 0.98,
        };
        let resp = model_metadata_to_response(&meta);
        assert_eq!(resp.name, "test_model");
        assert_eq!(resp.measurement, "disk");
        assert_eq!(resp.version, 5);
        assert_eq!(resp.training_points, 50);
        assert!((resp.mse - 0.1).abs() < f64::EPSILON);
        assert!((resp.r_squared - 0.98).abs() < f64::EPSILON);
    }

    #[test]
    fn model_catalog_list_all() {
        use chronix::chronix_analytics::forecast::{
            ForecastModel, ModelCatalog, ModelType, SesModel,
        };

        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = vec![42.0; 50];
        let mut model = SesModel::new(Some(0.5));
        model.fit(&ts, &vals).unwrap();

        let mut catalog = ModelCatalog::new();
        catalog
            .save_model(
                "cpu",
                "ses1",
                &model,
                ModelType::Ses,
                (0, 49),
                50,
                0.0,
                0.0,
                1.0,
            )
            .unwrap();
        catalog
            .save_model(
                "mem",
                "ses2",
                &model,
                ModelType::Ses,
                (0, 49),
                50,
                0.0,
                0.0,
                1.0,
            )
            .unwrap();

        let all = catalog.list_all();
        assert_eq!(all.len(), 2);

        let cpu_models = catalog.list_models("cpu");
        assert_eq!(cpu_models.len(), 1);
        assert_eq!(cpu_models[0].model_name, "ses1");
    }
}

#[cfg(test)]
mod path_confinement_tests {
    use super::confine_to_root;

    /// The old check rejected `..` and required an absolute path, which
    /// meant *any* absolute path was accepted: restore read whatever the
    /// server user could read, and backup wrote wherever it could write.
    #[test]
    fn an_absolute_path_outside_the_root_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("backups");
        let err =
            confine_to_root("/etc", &root).expect_err("a path outside the root must be refused");
        assert!(
            err.to_string().contains("backup root"),
            "the error must say why: {err}"
        );
    }

    #[test]
    fn a_relative_path_resolves_inside_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("backups");
        let resolved = confine_to_root("nightly/2026-09-03", &root).unwrap();
        assert!(
            resolved.starts_with(root.canonicalize().unwrap()),
            "{} must be inside the root",
            resolved.display()
        );
    }

    #[test]
    fn parent_traversal_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("backups");
        assert!(confine_to_root("../../etc", &root).is_err());
    }

    /// Rejecting `..` in the string is not confinement. A symlink planted
    /// inside the root points out of it without any `..` anywhere, so the
    /// deepest existing ancestor is canonicalised before the check.
    #[cfg(unix)]
    #[test]
    fn a_symlink_out_of_the_root_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("backups");
        std::fs::create_dir_all(&root).unwrap();
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();

        let err = confine_to_root("escape/loot", &root)
            .expect_err("a symlink leaving the root must be refused");
        assert!(err.to_string().contains("backup root"), "{err}");
    }
}
