//! `chronixd` — the Chronix time-series database server.
//!
//! ```text
//! chronixd --data-dir /var/lib/chronix --bind 0.0.0.0:8086
//! chronixd --mode meta --node-id 1 --raft-bind 0.0.0.0:9100 --cluster-peers 10.0.0.2:9100,10.0.0.3:9100
//! chronixd --mode data --node-id 10 --meta-addrs 10.0.0.1:9100,10.0.0.2:9100,10.0.0.3:9100
//! ```

use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use tracing::info;

use chronixd::config::ServerConfig;

/// Node operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum NodeMode {
    /// Standalone single-node server (default — no cluster).
    Standalone,
    /// MetaNode — Raft consensus cluster for metadata management.
    Meta,
    /// DataNode — hosts region data, registers with MetaNodes.
    Data,
}

/// Chronix time-series database server.
#[derive(Parser, Debug)]
#[command(
    name = "chronixd",
    version,
    about = "Chronix time-series database server"
)]
struct Cli {
    /// Path to TOML configuration file.
    #[arg(long, short = 'c', env = "CHRONIXD_CONFIG")]
    config: Option<PathBuf>,

    /// Node operating mode.
    #[arg(long, env = "CHRONIXD_MODE", value_enum, default_value = "standalone")]
    mode: NodeMode,

    /// Unique node identifier (required for meta/data modes).
    #[arg(long, env = "CHRONIXD_NODE_ID")]
    node_id: Option<u64>,

    /// Raft/admin gRPC bind address (meta mode).
    #[arg(long, env = "CHRONIXD_RAFT_BIND")]
    raft_bind: Option<String>,

    /// Comma-separated MetaNode peer addresses for cluster bootstrap (meta mode).
    #[arg(long, env = "CHRONIXD_CLUSTER_PEERS", value_delimiter = ',')]
    cluster_peers: Vec<String>,

    /// Comma-separated MetaNode addresses for registration (data mode).
    #[arg(long, env = "CHRONIXD_META_ADDRS", value_delimiter = ',')]
    meta_addrs: Vec<String>,

    /// Data directory (overrides config file).
    #[arg(long, env = "CHRONIXD_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// HTTP bind address (overrides config file).
    #[arg(long, env = "CHRONIXD_BIND")]
    bind: Option<String>,

    /// gRPC bind address (overrides config file).
    #[arg(long, env = "CHRONIXD_GRPC_BIND")]
    grpc_bind: Option<String>,

    /// Flight SQL bind address (overrides config file).
    #[arg(long, env = "CHRONIXD_FLIGHT_BIND")]
    flight_bind: Option<String>,

    /// TLS certificate file.
    #[arg(long, env = "CHRONIXD_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    /// TLS private key file.
    #[arg(long, env = "CHRONIXD_TLS_KEY")]
    tls_key: Option<PathBuf>,

    /// TLS client CA certificate for mutual TLS (optional).
    /// When set, clients must present a certificate signed by this CA.
    #[arg(long, env = "CHRONIXD_TLS_CLIENT_CA")]
    tls_client_ca: Option<PathBuf>,

    /// TLS certificate/key hot-reload interval in seconds (0 = disabled).
    /// When set, the server watches cert/key files and atomically reloads
    /// the TLS configuration on change for zero-downtime certificate rotation.
    #[arg(long, env = "CHRONIXD_TLS_RELOAD_INTERVAL", default_value = "0")]
    tls_reload_interval_secs: u64,

    /// Log format. `--log-format jsom` is a usage error, not plain text.
    ///
    /// `Option`, with no `default_value`: a default makes the flag always
    /// present, so assigning it would overwrite the file and the environment
    /// with a value nobody typed, inverting the documented precedence.
    #[arg(long, env = "CHRONIXD_LOG_FORMAT", value_enum)]
    log_format: Option<chronixd::config::LogFormat>,

    /// Log level filter.
    #[arg(long, env = "CHRONIXD_LOG_LEVEL")]
    log_level: Option<String>,

    /// Export bundled Grafana dashboards to the given directory and exit.
    #[arg(long)]
    export_dashboards: Option<PathBuf>,

    /// Load and validate the configuration, print what it resolves to, and
    /// exit without opening the database or binding a port.
    ///
    /// An unknown key is a startup error, but "start the server to find out"
    /// is not a check anybody runs before a deploy.
    #[arg(long)]
    check_config: bool,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            // `Display`, not `Debug`: returning `Result` from `main` prints
            // the `Debug` form, which renders a config parse error with its
            // newlines escaped inside a tuple struct — legible to nobody.
            eprintln!("chronixd: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Load config from file or use defaults
    let mut config = if let Some(ref path) = cli.config {
        ServerConfig::from_toml(path)?
    } else {
        ServerConfig::default()
    };

    // Environment overrides sit between the file and the CLI: a container
    // image carries the file, the deployment overrides what differs, and an
    // operator debugging by hand still wins with a flag.
    config.apply_env_overrides()?;

    // CLI overrides
    if let Some(ref data_dir) = cli.data_dir {
        config.database.data_dir = data_dir.clone();
    }
    if let Some(ref bind) = cli.bind {
        config.server.http_addr = bind
            .parse()
            .map_err(|e| format!("invalid HTTP bind address '{bind}': {e}"))?;
    }
    if let Some(ref grpc_bind) = cli.grpc_bind {
        config.server.grpc_addr = Some(
            grpc_bind
                .parse()
                .map_err(|e| format!("invalid gRPC bind address '{grpc_bind}': {e}"))?,
        );
    }
    if let Some(ref flight_bind) = cli.flight_bind {
        config.server.flight_addr = Some(
            flight_bind
                .parse()
                .map_err(|e| format!("invalid Flight SQL bind address '{flight_bind}': {e}"))?,
        );
    }
    if let Some(ref cert) = cli.tls_cert {
        let key = cli
            .tls_key
            .clone()
            .ok_or("TLS certificate provided but no key (--tls-key)")?;
        config.tls = Some(chronixd::config::TlsConfig {
            cert: cert.clone(),
            key,
            client_ca: cli.tls_client_ca.clone(),
            reload_interval_secs: cli.tls_reload_interval_secs,
        });
    }
    if let Some(format) = cli.log_format {
        config.server.log_format = format;
    }
    if let Some(ref level) = cli.log_level {
        config.server.log_level.clone_from(level);
    }

    // Cluster mode configuration
    match cli.mode {
        NodeMode::Meta => {
            let node_id = cli.node_id.ok_or("--node-id is required for meta mode")?;
            let raft_bind = cli
                .raft_bind
                .ok_or("--raft-bind is required for meta mode")?;
            config.cluster = Some(chronixd::config::ClusterConfig {
                mode: chronixd::config::ClusterMode::Meta,
                node_id,
                raft_bind_addr: raft_bind,
                cluster_peers: cli.cluster_peers,
                meta_addrs: Vec::new(),
                tls: None,
            });
        }
        NodeMode::Data => {
            let node_id = cli.node_id.ok_or("--node-id is required for data mode")?;
            if cli.meta_addrs.is_empty() {
                return Err("--meta-addrs is required for data mode".into());
            }
            config.cluster = Some(chronixd::config::ClusterConfig {
                mode: chronixd::config::ClusterMode::Data,
                node_id,
                raft_bind_addr: String::new(),
                cluster_peers: Vec::new(),
                meta_addrs: cli.meta_addrs,
                tls: None,
            });
        }
        NodeMode::Standalone => {}
    }

    // Validate port configuration
    config.validate_ports()?;
    // Validate CORS configuration
    config.validate_cors()?;
    // Refuse to serve multiple tenants with keys that are confined to none
    config.validate_tenancy()?;
    // Refuse to start with an [auth] section that authenticates nobody
    config.validate_auth()?;
    config.validate_authz()?;
    // Refuse a [tracing] section this build cannot honour
    config.validate_tracing()?;

    if cli.check_config {
        // Print what the file *resolved to*, not what it said: the point of
        // the check is the difference between the two.
        println!("configuration is valid");
        println!("  http_addr        {}", config.server.http_addr);
        println!("  grpc_addr        {}", config.server.grpc_addr());
        println!("  flight_addr      {}", config.server.flight_addr());
        println!("  data_dir         {}", config.database.data_dir.display());
        println!("  log_format       {:?}", config.server.log_format);
        println!("  log_level        {}", config.server.log_level);
        println!("  sql_max_rows     {}", config.server.sql_max_rows);
        println!("  max_body_size    {}", config.server.max_body_size);
        println!("  multi_tenancy    {}", config.server.multi_tenancy);
        let sections = [
            ("tls", config.tls.is_some()),
            ("auth", config.auth.is_some()),
            ("audit", config.audit.is_some()),
            ("triggers", config.triggers.is_some()),
            ("cold_archive", config.cold_archive.is_some()),
            ("kafka", config.kafka.is_some()),
            ("mqtt", config.mqtt.is_some()),
            ("cluster", config.cluster.is_some()),
            ("tracing", config.tracing.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, present)| present.then_some(name))
        .collect::<Vec<_>>();
        println!(
            "  sections         {}",
            if sections.is_empty() {
                "none besides [server] and [database]".to_string()
            } else {
                sections.join(", ")
            }
        );
        let exposed = chronixd::server::exposed_listeners(&config);
        if !exposed.is_empty() {
            println!(
                "  warning          no [auth] section: {} accept reads, writes \
                 and deletes from anyone who can reach them",
                exposed.join(", ")
            );
        }
        return Ok(());
    }

    // `init_tracing` is the only initialiser: it registers the reload handle
    // `PUT /api/v1/admin/log-level` swaps, and the OTLP exporter. A second,
    // private one would leave both dead. The guard flushes pending OTLP
    // batches on drop, so it is held for the life of the process.
    let _tracing_guard = chronixd::otel::init_tracing(&config.tracing_config())?;

    // Handle --export-dashboards early exit
    if let Some(ref out_dir) = cli.export_dashboards {
        export_dashboards(out_dir)?;
        return Ok(());
    }

    info!(
        version = env!("CARGO_PKG_VERSION"),
        mode = ?cli.mode,
        "starting chronixd"
    );

    // Run the server
    chronixd::server::run(config).await?;

    Ok(())
}

/// Export bundled Grafana dashboard JSON files from the `dashboards/` directory.
fn export_dashboards(out_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let src_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("dashboards"))
        .ok_or("cannot resolve dashboards directory")?;

    if !src_dir.exists() {
        return Err(format!("dashboards directory not found at {}", src_dir.display()).into());
    }

    std::fs::create_dir_all(out_dir)?;

    let mut count = 0u32;
    for entry in std::fs::read_dir(&src_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            let dest = out_dir.join(entry.file_name());
            std::fs::copy(&path, &dest)?;
            println!("exported: {}", dest.display());
            count += 1;
        }
    }

    println!("{count} dashboard(s) exported to {}", out_dir.display());
    Ok(())
}
