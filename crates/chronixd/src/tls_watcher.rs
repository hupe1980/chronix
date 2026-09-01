//! TLS certificate hot-reload watcher.
//!
//! Monitors cert/key file modification times and atomically reloads the
//! TLS configuration when changes are detected — enabling zero-downtime
//! certificate rotation.
//!
//! Uses a simple polling approach (tokio interval + filesystem mtime) rather
//! than `inotify`/`kqueue`/`FSEvents` for maximum portability and simplicity.
//! The polling interval is configurable via `TlsConfig::reload_interval_secs`.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tokio::time::{interval, Duration};
use tracing::{debug, error, info};

/// Metadata snapshot of a watched file (path + last-modified time).
#[derive(Debug)]
struct FileState {
    path: PathBuf,
    mtime: Option<SystemTime>,
}

impl FileState {
    fn new(path: PathBuf) -> Self {
        let mtime = Self::read_mtime(&path);
        Self { path, mtime }
    }

    fn read_mtime(path: &Path) -> Option<SystemTime> {
        std::fs::metadata(path).and_then(|m| m.modified()).ok()
    }

    /// Returns `true` if the file's modification time has changed since last check.
    fn has_changed(&mut self) -> bool {
        let current = Self::read_mtime(&self.path);
        if current != self.mtime {
            self.mtime = current;
            true
        } else {
            false
        }
    }
}

/// Spawns a background task that watches cert/key files and reloads TLS
/// configuration when changes are detected.
///
/// The `rustls_config` handle from `axum_server::tls_rustls::RustlsConfig`
/// supports atomic reload via `reload_from_config()`. This function creates
/// a polling loop that:
///
/// 1. Checks cert and key file modification times every `interval_secs` seconds
/// 2. On change, re-parses the PEM files via `load_rustls_config()`
/// 3. Atomically swaps the live TLS configuration (zero downtime)
///
/// # Error tracking
///
/// Reload errors are tracked via the `chronix_tls_reloads_total{status="error"}`
/// counter. If the watcher **task itself** panics (e.g. due to a bug in
/// filesystem I/O), the tokio `JoinHandle` is currently dropped without
/// notification. An `is_finished()` health check or `panic = "abort"`
/// policy can guard against silent watcher death.
///
/// # Arguments
///
/// * `tls_cfg` - The TLS configuration with cert/key paths
/// * `rustls_config` - The live `RustlsConfig` handle to reload
/// * `interval_secs` - Polling interval in seconds
///
/// Spawns the TLS watcher and returns a `JoinHandle` so the caller can
/// optionally monitor it (e.g. via `is_finished()` in a health check).
///
/// # Supervision
///
/// A supervisor task awaits the inner watcher `JoinHandle`.  If the
/// watcher panics, the supervisor:
///
/// 1. Increments `chronix_tls_watcher_panics_total`
/// 2. Logs the panic via `tracing::error!`
///
/// This ensures silent watcher death is always observable through
/// metrics and logs.
pub fn spawn_tls_watcher(
    tls_cfg: crate::config::TlsConfig,
    rustls_config: axum_server::tls_rustls::RustlsConfig,
    interval_secs: u64,
    grpc_cert_resolver: Option<std::sync::Arc<crate::tls::ReloadableCertResolver>>,
) {
    if interval_secs == 0 {
        return;
    }

    // Clone for the supervisor restart loop.
    let tls_cfg_restart = tls_cfg.clone();
    let rustls_config_restart = rustls_config.clone();
    let grpc_resolver_restart = grpc_cert_resolver.clone();

    // Inner task — runs the actual polling loop.
    let watcher_handle = tokio::spawn(async move {
        let mut cert_state = FileState::new(tls_cfg.cert.clone());
        let mut key_state = FileState::new(tls_cfg.key.clone());
        let mut ca_state = tls_cfg
            .client_ca
            .as_ref()
            .map(|p| FileState::new(p.clone()));

        let mut ticker = interval(Duration::from_secs(interval_secs));
        // Skip the immediate first tick (files were just loaded at startup)
        ticker.tick().await;

        info!(
            interval_secs,
            cert = %tls_cfg.cert.display(),
            key = %tls_cfg.key.display(),
            "TLS hot-reload watcher started"
        );

        loop {
            ticker.tick().await;

            let cert_changed = cert_state.has_changed();
            let key_changed = key_state.has_changed();
            let ca_changed = ca_state.as_mut().is_some_and(FileState::has_changed);

            if !cert_changed && !key_changed && !ca_changed {
                continue;
            }

            debug!(
                cert_changed,
                key_changed, ca_changed, "TLS file change detected, reloading"
            );

            match crate::tls::load_rustls_config(&tls_cfg) {
                Ok(new_config) => {
                    rustls_config.reload_from_config(new_config);

                    // Also update gRPC/Flight cert resolver.
                    if let Some(ref resolver) = grpc_cert_resolver {
                        match crate::tls::load_certified_key(&tls_cfg) {
                            Ok(key) => resolver.update(key),
                            Err(e) => {
                                error!(error = %e, "gRPC cert reload failed — HTTP updated, gRPC keeps previous cert");
                            }
                        }
                    }

                    info!("TLS configuration reloaded successfully");
                    metrics::counter!("chronix_tls_reloads_total", "status" => "success")
                        .increment(1);
                }
                Err(e) => {
                    error!(error = %e, "TLS reload failed — keeping previous configuration");
                    metrics::counter!("chronix_tls_reloads_total", "status" => "error")
                        .increment(1);
                    // Reset mtimes so we retry on next tick
                    cert_state.mtime = FileState::read_mtime(&cert_state.path);
                    key_state.mtime = FileState::read_mtime(&key_state.path);
                    if let Some(ref mut s) = ca_state {
                        s.mtime = FileState::read_mtime(&s.path);
                    }
                }
            }
        }
    });

    // Supervisor task — monitors the watcher for panics and
    // restarts with exponential backoff.
    tokio::spawn(async move {
        let mut restart_count = 0u32;

        // First wait for the initial watcher.
        let mut handle = watcher_handle;

        loop {
            match handle.await {
                Ok(()) => {
                    tracing::warn!("TLS hot-reload watcher exited unexpectedly, restarting...");
                }
                Err(join_err) => {
                    let reason = if join_err.is_panic() {
                        let panic_val = join_err.into_panic();
                        if let Some(s) = panic_val.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(s) = panic_val.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic payload".to_string()
                        }
                    } else {
                        format!("task cancelled: {join_err}")
                    };

                    error!(reason = %reason, "TLS hot-reload watcher crashed");
                    metrics::counter!("chronix_tls_watcher_panics_total").increment(1);
                }
            }

            restart_count += 1;
            metrics::counter!("chronix_tls_watcher_restarts_total").increment(1);

            // Cap exponential backoff at 32 seconds.
            let backoff_secs = 2u64.pow(restart_count.min(5));
            tracing::info!(
                restart_count,
                backoff_secs,
                "restarting TLS watcher after backoff"
            );
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;

            // Respawn the inner watcher.
            let tls_cfg2 = tls_cfg_restart.clone();
            let rustls_config2 = rustls_config_restart.clone();
            let grpc_resolver2 = grpc_resolver_restart.clone();
            handle = tokio::spawn(async move {
                let mut cert_state = FileState::new(tls_cfg2.cert.clone());
                let mut key_state = FileState::new(tls_cfg2.key.clone());
                let mut ca_state = tls_cfg2
                    .client_ca
                    .as_ref()
                    .map(|p| FileState::new(p.clone()));
                let mut ticker = interval(Duration::from_secs(interval_secs));
                ticker.tick().await;

                info!(interval_secs, "TLS hot-reload watcher restarted");

                loop {
                    ticker.tick().await;
                    let cert_changed = cert_state.has_changed();
                    let key_changed = key_state.has_changed();
                    let ca_changed = ca_state.as_mut().is_some_and(FileState::has_changed);

                    if !cert_changed && !key_changed && !ca_changed {
                        continue;
                    }

                    match crate::tls::load_rustls_config(&tls_cfg2) {
                        Ok(new_config) => {
                            rustls_config2.reload_from_config(new_config);

                            // Also update gRPC/Flight cert resolver.
                            if let Some(ref resolver) = grpc_resolver2 {
                                match crate::tls::load_certified_key(&tls_cfg2) {
                                    Ok(key) => resolver.update(key),
                                    Err(e) => {
                                        error!(error = %e, "gRPC cert reload failed — HTTP updated, gRPC keeps previous cert");
                                    }
                                }
                            }

                            info!("TLS configuration reloaded successfully");
                            metrics::counter!("chronix_tls_reloads_total", "status" => "success")
                                .increment(1);
                        }
                        Err(e) => {
                            error!(error = %e, "TLS reload failed — keeping previous configuration");
                            metrics::counter!("chronix_tls_reloads_total", "status" => "error")
                                .increment(1);
                            cert_state.mtime = FileState::read_mtime(&cert_state.path);
                            key_state.mtime = FileState::read_mtime(&key_state.path);
                            if let Some(ref mut s) = ca_state {
                                s.mtime = FileState::read_mtime(&s.path);
                            }
                        }
                    }
                }
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_state_detects_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cert.pem");
        std::fs::write(&path, b"initial").unwrap();

        let mut state = FileState::new(path.clone());
        assert!(!state.has_changed(), "no change on first re-check");

        // Ensure time resolution picks up the change (some filesystems
        // have 1-second mtime granularity).
        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::write(&path, b"updated").unwrap();
        assert!(state.has_changed(), "should detect file change");
        assert!(!state.has_changed(), "second check — no new change");
    }

    #[test]
    fn file_state_missing_file() {
        let mut state = FileState::new(PathBuf::from("/nonexistent/cert.pem"));
        assert!(state.mtime.is_none());
        assert!(!state.has_changed(), "missing file stays unchanged");
    }
}
