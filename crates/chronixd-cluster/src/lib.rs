//! `chronixd` in cluster mode — the frozen Multi-Raft tier.
//!
//! The standalone server is `chronixd`; this crate adds Meta and Data node
//! roles to it through [`chronixd::server::Extension`]. It is built from the
//! repository (`cargo build -p chronixd-cluster`) and never published.

pub mod admin;
pub mod cluster;
pub mod region_storage;

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post, put};
use chronix_cluster::MetaClient;
use chronix_security::authz::ChronixAction;
use chronixd::config::ClusterMode;
use chronixd::error::ServerError;
use chronixd::http::AppState;
use chronixd::server::{Extension, ExtensionContext, admin_group};
use tokio::sync::Mutex;

/// A running node: one of the two cluster roles.
enum Node {
    Meta(cluster::MetaNodeState),
    Data(cluster::DataNodeState),
}

/// The cluster extension: starts the node the `[cluster]` section names and
/// serves the cluster administration routes.
#[derive(Default)]
pub struct ClusterExtension {
    node: Mutex<Option<Node>>,
}

#[async_trait::async_trait]
impl Extension for ClusterExtension {
    fn name(&self) -> &'static str {
        "cluster"
    }

    async fn start(&self, ctx: ExtensionContext) -> Result<Router<AppState>, ServerError> {
        let cfg = ctx.config.cluster.as_ref().ok_or_else(|| {
            ServerError::Config(chronixd::config::ServerConfigError::Invalid(
                "chronixd-cluster needs a [cluster] section or --mode meta|data".into(),
            ))
        })?;
        let (node, client): (Node, Arc<dyn MetaClient>) = match cfg.mode {
            ClusterMode::Meta => {
                let s = cluster::start_meta_node(cfg, &ctx.config.database.data_dir).await?;
                let client = Arc::clone(&s.meta_client);
                (Node::Meta(s), client)
            }
            ClusterMode::Data => {
                let grpc_addr = ctx.config.server.grpc_addr().to_string();
                let s = cluster::start_data_node(cfg, &grpc_addr, Arc::clone(&ctx.db)).await?;
                let client = Arc::clone(s.manager.meta_client());
                (Node::Data(s), client)
            }
        };
        *self.node.lock().await = Some(node);
        Ok(routes(&ctx.state).layer(axum::Extension(client)))
    }

    async fn stop(&self) {
        match self.node.lock().await.take() {
            Some(Node::Meta(s)) => cluster::stop_meta_node(s).await,
            Some(Node::Data(s)) => cluster::stop_data_node(s).await,
            None => {}
        }
    }
}

/// The cluster administration routes, one capability per group: reading
/// topology is not rebalancing.
fn routes(state: &AppState) -> Router<AppState> {
    Router::new()
        .merge(admin_group(
            state,
            ChronixAction::ManageNodes,
            Router::new()
                .route(
                    "/api/v1/admin/nodes",
                    post(admin::register_node_handler)
                        .get(admin::list_nodes_handler)
                        .delete(admin::deregister_node_handler),
                )
                .route("/api/v1/admin/heartbeat", post(admin::heartbeat_handler))
                .route(
                    "/api/v1/admin/nodes/{id}/decommission",
                    post(admin::decommission_node_handler),
                ),
        ))
        .merge(admin_group(
            state,
            ChronixAction::ManageRegions,
            Router::new()
                .route("/api/v1/admin/regions", post(admin::create_region_handler))
                .route(
                    "/api/v1/admin/regions/{id}/state",
                    put(admin::update_region_state_handler),
                ),
        ))
        .merge(admin_group(
            state,
            ChronixAction::ViewCluster,
            Router::new()
                .route("/api/v1/admin/routing", get(admin::get_routing_handler))
                .route("/api/v1/admin/health", get(admin::cluster_health_handler))
                .route(
                    "/api/v1/admin/topology",
                    get(admin::cluster_topology_handler),
                ),
        ))
        .merge(admin_group(
            state,
            ChronixAction::ManageCluster,
            Router::new().route("/api/v1/admin/rebalance", post(admin::rebalance_handler)),
        ))
}
