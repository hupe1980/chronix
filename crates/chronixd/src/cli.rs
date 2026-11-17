//! The server's command line, shared by every binary that runs it.
//!
//! [`ServerArgs`] is a `clap::Args` group: `chronixd` uses it as its whole
//! command line, and `chronixd-cluster` flattens it beside its own flags, so
//! the two binaries cannot drift apart.

use std::path::{Path, PathBuf};

use clap::Args;
use tracing::info;

use crate::config::{LogFormat, ServerConfig, TlsConfig};
use crate::server::Extension;

/// The flags every server binary accepts.
///
/// Precedence: flag, then environment variable, then config file, then the
/// built-in default.
#[derive(Args, Debug)]
pub struct ServerArgs {
    /// Path to TOML configuration file.
    #[arg(long, short = 'c', env = "CHRONIXD_CONFIG")]
    pub config: Option<PathBuf>,

    /// Data directory (overrides config file).
    #[arg(long, env = "CHRONIXD_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// HTTP bind address (overrides config file).
    #[arg(long, env = "CHRONIXD_BIND")]
    pub bind: Option<String>,

    /// gRPC bind address (overrides config file).
    #[arg(long, env = "CHRONIXD_GRPC_BIND")]
    pub grpc_bind: Option<String>,

    /// Flight SQL bind address (overrides config file).
    #[arg(long, env = "CHRONIXD_FLIGHT_BIND")]
    pub flight_bind: Option<String>,

    /// TLS certificate file.
    #[arg(long, env = "CHRONIXD_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// TLS private key file.
    #[arg(long, env = "CHRONIXD_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// CA certificate for mutual TLS: clients must present a certificate it signed.
    #[arg(long, env = "CHRONIXD_TLS_CLIENT_CA")]
    pub tls_client_ca: Option<PathBuf>,

    /// Reload the TLS certificate and key every N seconds (0 = never).
    ///
    /// No default here: unset, the config file's `[tls] reload_interval_secs`
    /// applies, or 60 s.
    #[arg(long, env = "CHRONIXD_TLS_RELOAD_INTERVAL")]
    pub tls_reload_interval_secs: Option<u64>,

    /// Log format.
    ///
    /// No default here: one would always be present and override the file and
    /// the environment with a value nobody typed.
    #[arg(long, env = "CHRONIXD_LOG_FORMAT", value_enum)]
    pub log_format: Option<LogFormat>,

    /// Log level filter.
    #[arg(long, env = "CHRONIXD_LOG_LEVEL")]
    pub log_level: Option<String>,

    /// Write the bundled Grafana dashboards to this directory and exit.
    #[arg(long)]
    pub export_dashboards: Option<PathBuf>,

    /// Validate the configuration, print what it resolves to, and exit
    /// without opening the database or binding a port.
    #[arg(long)]
    pub check_config: bool,
}

impl ServerArgs {
    /// The configuration these flags resolve to: the file (or the defaults),
    /// then the environment, then the flags.
    ///
    /// # Errors
    ///
    /// An unreadable or invalid file, an invalid environment override, or an
    /// unparsable flag.
    pub fn load(&self) -> Result<ServerConfig, Box<dyn std::error::Error>> {
        let mut config = match &self.config {
            Some(path) => ServerConfig::from_toml(path)?,
            None => ServerConfig::default(),
        };
        config.apply_env_overrides()?;

        if let Some(dir) = &self.data_dir {
            config.database.data_dir.clone_from(dir);
        }
        if let Some(bind) = &self.bind {
            config.server.http_addr = bind
                .parse()
                .map_err(|e| format!("invalid HTTP bind address '{bind}': {e}"))?;
        }
        if let Some(bind) = &self.grpc_bind {
            config.server.grpc_addr = Some(
                bind.parse()
                    .map_err(|e| format!("invalid gRPC bind address '{bind}': {e}"))?,
            );
        }
        if let Some(bind) = &self.flight_bind {
            config.server.flight_addr = Some(
                bind.parse()
                    .map_err(|e| format!("invalid Flight SQL bind address '{bind}': {e}"))?,
            );
        }
        if let Some(cert) = &self.tls_cert {
            let key = self
                .tls_key
                .clone()
                .ok_or("TLS certificate provided but no key (--tls-key)")?;
            let reload_interval_secs = self
                .tls_reload_interval_secs
                .or_else(|| config.tls.as_ref().map(|t| t.reload_interval_secs))
                .unwrap_or_else(crate::config::default_tls_reload_interval_secs);
            config.tls = Some(TlsConfig {
                cert: cert.clone(),
                key,
                client_ca: self.tls_client_ca.clone(),
                reload_interval_secs,
            });
        } else if let (Some(secs), Some(tls)) = (self.tls_reload_interval_secs, config.tls.as_mut())
        {
            tls.reload_interval_secs = secs;
        }
        if let Some(format) = self.log_format {
            config.server.log_format = format;
        }
        if let Some(level) = &self.log_level {
            config.server.log_level.clone_from(level);
        }
        Ok(config)
    }
}

/// Validate `config`, then do what `args` asks: check it, export the
/// dashboards, or run the server with `extensions`.
///
/// # Errors
///
/// Any validation failure, or the server's own error.
pub async fn serve(
    args: &ServerArgs,
    config: ServerConfig,
    extensions: Vec<Box<dyn Extension>>,
) -> Result<(), Box<dyn std::error::Error>> {
    config.validate_ports()?;
    config.validate_cors()?;
    config.validate_tenancy()?;
    config.validate_auth()?;
    config.validate_authz()?;
    config.validate_tracing()?;
    config.validate_mcp()?;

    if args.check_config {
        print_resolved(&config);
        return Ok(());
    }

    // The only tracing initialiser: it owns the reload handle the log-level
    // endpoint swaps and the OTLP exporter. The guard flushes on drop.
    let _tracing_guard = crate::otel::init_tracing(&config.tracing_config())?;

    if let Some(dir) = &args.export_dashboards {
        export_dashboards(dir)?;
        return Ok(());
    }

    info!(version = env!("CARGO_PKG_VERSION"), "starting chronixd");
    crate::server::run_with(config, extensions).await?;
    Ok(())
}

/// Print what the configuration resolved to.
fn print_resolved(config: &ServerConfig) {
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
    let exposed = crate::server::exposed_listeners(config);
    if !exposed.is_empty() {
        println!(
            "  warning          no [auth] section: {} accept reads, writes \
             and deletes from anyone who can reach them",
            exposed.join(", ")
        );
    }
}

/// The bundled Grafana dashboards, compiled in so an installed binary has them.
pub const DASHBOARDS: &[(&str, &str)] = &[
    (
        "analytics.json",
        include_str!("../dashboards/analytics.json"),
    ),
    (
        "cluster-overview.json",
        include_str!("../dashboards/cluster-overview.json"),
    ),
    (
        "ingestion.json",
        include_str!("../dashboards/ingestion.json"),
    ),
    (
        "query-performance.json",
        include_str!("../dashboards/query-performance.json"),
    ),
    ("signals.json", include_str!("../dashboards/signals.json")),
    ("storage.json", include_str!("../dashboards/storage.json")),
];

/// Write [`DASHBOARDS`] into `out_dir`.
///
/// # Errors
///
/// The directory cannot be created or a file cannot be written.
pub fn export_dashboards(out_dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(out_dir)?;
    for (name, json) in DASHBOARDS {
        let dest = out_dir.join(name);
        std::fs::write(&dest, json)?;
        println!("exported: {}", dest.display());
    }
    println!(
        "{} dashboard(s) exported to {}",
        DASHBOARDS.len(),
        out_dir.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(clap::Parser)]
    struct Probe {
        #[command(flatten)]
        args: ServerArgs,
    }

    fn tls_interval(argv: &[&str]) -> u64 {
        use clap::Parser as _;
        let mut full = vec!["chronixd", "--tls-cert", "c.pem", "--tls-key", "k.pem"];
        full.extend_from_slice(argv);
        let config = Probe::parse_from(full).args.load().unwrap();
        config.tls.unwrap().reload_interval_secs
    }

    /// A certificate given on the command line reloads on the same default as
    /// one given in the config file; only an explicit flag changes it.
    #[test]
    fn tls_reload_interval_defaults_like_the_config_file() {
        assert_eq!(
            tls_interval(&[]),
            crate::config::default_tls_reload_interval_secs()
        );
        assert_eq!(tls_interval(&["--tls-reload-interval-secs", "0"]), 0);
        assert_eq!(tls_interval(&["--tls-reload-interval-secs", "300"]), 300);
    }

    /// Every dashboard in the repository is compiled in, and nothing else.
    #[test]
    fn every_dashboard_is_bundled() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("dashboards");
        let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| {
                let name = e.unwrap().file_name().into_string().unwrap();
                name.ends_with(".json").then_some(name)
            })
            .collect();
        on_disk.sort();
        let mut bundled: Vec<String> = DASHBOARDS.iter().map(|(n, _)| (*n).to_string()).collect();
        bundled.sort();
        assert_eq!(bundled, on_disk);
        for (name, json) in DASHBOARDS {
            serde_json::from_str::<serde_json::Value>(json)
                .unwrap_or_else(|e| panic!("{name} is not valid JSON: {e}"));
        }
    }
}
