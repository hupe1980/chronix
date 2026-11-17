//! `chronixd-cluster` — `chronixd` with the frozen Meta/Data node roles.
//!
//! ```text
//! chronixd-cluster --mode meta --node-id 1 --raft-bind 0.0.0.0:9100 --cluster-peers 10.0.0.2:9100,10.0.0.3:9100
//! chronixd-cluster --mode data --node-id 10 --meta-addrs 10.0.0.1:9100,10.0.0.2:9100,10.0.0.3:9100
//! ```
//!
//! Every `chronixd` flag is accepted too.

use clap::{Parser, ValueEnum};

use chronixd::cli::ServerArgs;
use chronixd::config::{ClusterConfig, ClusterMode};
use chronixd_cluster::ClusterExtension;

/// Node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// Use the `[cluster]` section of the config file.
    Config,
    /// MetaNode: the Raft group that holds cluster metadata.
    Meta,
    /// DataNode: hosts regions and registers with the MetaNodes.
    Data,
}

/// Chronix server in cluster mode (frozen tier).
#[derive(Parser, Debug)]
#[command(name = "chronixd-cluster", version)]
struct Cli {
    /// Node role.
    #[arg(long, env = "CHRONIXD_MODE", value_enum, default_value = "config")]
    mode: Mode,

    /// Unique node identifier (required for `--mode meta|data`).
    #[arg(long, env = "CHRONIXD_NODE_ID")]
    node_id: Option<u64>,

    /// Raft and admin gRPC bind address (meta).
    #[arg(long, env = "CHRONIXD_RAFT_BIND")]
    raft_bind: Option<String>,

    /// MetaNode peers for bootstrap, comma-separated (meta).
    #[arg(long, env = "CHRONIXD_CLUSTER_PEERS", value_delimiter = ',')]
    cluster_peers: Vec<String>,

    /// MetaNode addresses to register with, comma-separated (data).
    #[arg(long, env = "CHRONIXD_META_ADDRS", value_delimiter = ',')]
    meta_addrs: Vec<String>,

    #[command(flatten)]
    server: ServerArgs,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("chronixd-cluster: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let mut config = cli.server.load()?;
    match cli.mode {
        Mode::Meta => {
            config.cluster = Some(ClusterConfig {
                mode: ClusterMode::Meta,
                node_id: cli.node_id.ok_or("--node-id is required for meta mode")?,
                raft_bind_addr: cli
                    .raft_bind
                    .ok_or("--raft-bind is required for meta mode")?,
                cluster_peers: cli.cluster_peers,
                meta_addrs: Vec::new(),
                tls: None,
            });
        }
        Mode::Data => {
            if cli.meta_addrs.is_empty() {
                return Err("--meta-addrs is required for data mode".into());
            }
            config.cluster = Some(ClusterConfig {
                mode: ClusterMode::Data,
                node_id: cli.node_id.ok_or("--node-id is required for data mode")?,
                raft_bind_addr: String::new(),
                cluster_peers: Vec::new(),
                meta_addrs: cli.meta_addrs,
                tls: None,
            });
        }
        Mode::Config => {}
    }
    chronixd::cli::serve(
        &cli.server,
        config,
        vec![Box::new(ClusterExtension::default())],
    )
    .await
}
