//! Namespace registry — CRUD for tenant namespaces.
//!
//! Manages the lifecycle of namespaces: creation, deletion, listing,
//! and configuration updates. Each namespace encapsulates its own
//! quota limits and usage counters.
//!
//! Optionally persists namespace state to disk via JSON snapshots.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::SystemTime;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use chronix_core::{NamespaceId, NamespaceQuota, NamespaceUsage};

use crate::tenant::error::{Result, TenantError};

/// Metadata for a registered namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceInfo {
    /// Unique namespace identifier.
    pub id: NamespaceId,
    /// Human-readable description.
    pub description: String,
    /// Owner principal (user ID that created the namespace).
    pub owner: String,
    /// Resource quota limits.
    pub quota: NamespaceQuota,
    /// When the namespace was created (ms since epoch).
    pub created_at_ms: u64,
}

/// Current state of a namespace including live usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceState {
    /// Namespace metadata.
    pub info: NamespaceInfo,
    /// Real-time resource usage.
    pub usage: NamespaceUsage,
}

/// Thread-safe namespace registry.
///
/// Provides fast, concurrent access to namespace metadata and usage
/// counters using [`DashMap`] for lock-free reads.
///
/// # Default Namespace
///
/// The `default` namespace is always present and cannot be deleted.
/// It is used when multi-tenancy is disabled or no namespace header
/// is provided.
#[derive(Debug)]
pub struct NamespaceRegistry {
    /// Namespace states keyed by namespace name.
    namespaces: DashMap<String, NamespaceState>,
    /// Optional directory for persisting namespace state.
    persist_dir: Option<PathBuf>,
    /// Handle for the most recent background snapshot thread.
    pending_snapshot: Mutex<Option<JoinHandle<()>>>,
}

/// File name for the namespace snapshot inside the persist directory.
const SNAPSHOT_FILE: &str = "namespaces.json";

/// Atomic write-to-tmp + fsync + rename for namespace state.
fn write_snapshot(dir: &Path, states: &[NamespaceState]) -> Result<()> {
    let json = serde_json::to_string_pretty(states)
        .map_err(|e| TenantError::InvalidConfig(format!("cannot serialize namespaces: {e}")))?;

    let tmp_path = dir.join("namespaces.json.tmp");
    let final_path = dir.join(SNAPSHOT_FILE);
    std::fs::write(&tmp_path, &json)
        .map_err(|e| TenantError::InvalidConfig(format!("cannot write namespace snapshot: {e}")))?;

    // Clean up tmp file on fsync or rename failure to avoid
    // stale tmp files leaking on disk.
    let result = (|| -> Result<()> {
        let f = std::fs::File::open(&tmp_path).map_err(|e| {
            TenantError::InvalidConfig(format!("cannot open tmp snapshot for fsync: {e}"))
        })?;
        f.sync_all().map_err(|e| {
            TenantError::InvalidConfig(format!("cannot fsync namespace snapshot: {e}"))
        })?;

        std::fs::rename(&tmp_path, &final_path).map_err(|e| {
            TenantError::InvalidConfig(format!("cannot rename namespace snapshot: {e}"))
        })?;
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }

    result?;

    debug!("namespace snapshot saved");
    Ok(())
}
impl NamespaceRegistry {
    /// Create a new in-memory registry with only the `default` namespace.
    ///
    /// State is not persisted. Use `open` for durable registries.
    #[must_use]
    pub fn new() -> Self {
        let registry = Self {
            namespaces: DashMap::new(),
            persist_dir: None,
            pending_snapshot: Mutex::new(None),
        };

        registry.ensure_default();
        registry
    }

    /// Open a durable namespace registry backed by a directory.
    ///
    /// If a previous snapshot exists, it is loaded. Otherwise the directory
    /// is created and an initial snapshot with only the `default` namespace
    /// is written.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or the snapshot
    /// file is corrupt.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)
            .map_err(|e| TenantError::InvalidConfig(format!("cannot create namespace dir: {e}")))?;

        let snapshot_path = dir.join(SNAPSHOT_FILE);
        let registry = if snapshot_path.exists() {
            let data = std::fs::read_to_string(&snapshot_path).map_err(|e| {
                TenantError::InvalidConfig(format!("cannot read namespace snapshot: {e}"))
            })?;
            let states: Vec<NamespaceState> = serde_json::from_str(&data).map_err(|e| {
                TenantError::InvalidConfig(format!("corrupt namespace snapshot: {e}"))
            })?;
            let namespaces = DashMap::new();
            for state in states {
                namespaces.insert(state.info.id.as_str().to_string(), state);
            }
            info!(count = namespaces.len(), "loaded namespace snapshot");
            Self {
                namespaces,
                persist_dir: Some(dir),
                pending_snapshot: Mutex::new(None),
            }
        } else {
            Self {
                namespaces: DashMap::new(),
                persist_dir: Some(dir),
                pending_snapshot: Mutex::new(None),
            }
        };

        registry.ensure_default();

        // Write initial snapshot if it didn't exist
        if !snapshot_path.exists() {
            registry.save_snapshot_inner()?;
        }

        Ok(registry)
    }

    /// Ensure the default namespace exists.
    fn ensure_default(&self) {
        if !self.namespaces.contains_key(NamespaceId::DEFAULT) {
            let default_info = NamespaceInfo {
                id: NamespaceId::default_namespace(),
                description: "Default namespace".to_string(),
                owner: "system".to_string(),
                quota: NamespaceQuota::default(),
                created_at_ms: now_ms(),
            };
            self.namespaces.insert(
                NamespaceId::DEFAULT.to_string(),
                NamespaceState {
                    info: default_info,
                    usage: NamespaceUsage::default(),
                },
            );
        }
    }

    /// Persist current namespace state to disk (atomic write-to-tmp + rename).
    ///
    /// No-op if the registry has no `persist_dir`.
    fn save_snapshot_inner(&self) -> Result<()> {
        let Some(dir) = &self.persist_dir else {
            return Ok(());
        };

        let states: Vec<NamespaceState> =
            self.namespaces.iter().map(|r| r.value().clone()).collect();

        write_snapshot(dir, &states)
    }

    /// Save a best-effort snapshot on a background thread,
    /// avoiding blocking the caller with fsync I/O.
    fn save_snapshot_best_effort(&self) {
        let dir = match &self.persist_dir {
            Some(d) => d.clone(),
            None => return,
        };

        // Collect state snapshot from DashMap on the caller thread
        // (fast — only clones the current namespace states).
        let states: Vec<NamespaceState> =
            self.namespaces.iter().map(|r| r.value().clone()).collect();

        // Serialize snapshot writers: join the previous in-flight snapshot
        // BEFORE spawning the new one. Spawning first would let an older
        // snapshot race a newer one for the final rename — last-rename-wins
        // could then persist stale state.
        let Ok(mut guard) = self.pending_snapshot.lock() else {
            // Poisoned lock: fall back to a synchronous write so the update
            // is never silently lost.
            if let Err(e) = write_snapshot(&dir, &states) {
                warn!("failed to persist namespace snapshot: {e}");
            }
            return;
        };
        if let Some(prev) = guard.take() {
            let _ = prev.join();
        }

        // Offload serialization + file I/O to a background thread.
        match std::thread::Builder::new()
            .name("chronix-ns-snapshot".into())
            .spawn(move || {
                if let Err(e) = write_snapshot(&dir, &states) {
                    warn!("failed to persist namespace snapshot: {e}");
                }
            }) {
            Ok(handle) => {
                // Store the handle so Drop (or the next snapshot) joins it.
                *guard = Some(handle);
            }
            Err(e) => {
                // Thread spawn failed (e.g. resource exhaustion): write
                // synchronously rather than dropping the snapshot.
                warn!("failed to spawn namespace snapshot thread, writing synchronously: {e}");
                drop(guard);
                if let Err(e) = self.save_snapshot_inner() {
                    warn!("failed to persist namespace snapshot: {e}");
                }
            }
        }
    }

    /// Create a new namespace.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceAlreadyExists` if a namespace with the same name
    /// already exists, or `Schema` if the name is invalid.
    pub fn create_namespace(
        &self,
        id: NamespaceId,
        description: impl Into<String>,
        owner: impl Into<String>,
        quota: NamespaceQuota,
    ) -> Result<NamespaceInfo> {
        let name = id.as_str().to_string();

        // Use DashMap::entry() to atomically check-and-insert, avoiding the
        // TOCTOU race between contains_key() and insert().
        use dashmap::mapref::entry::Entry;
        match self.namespaces.entry(name.clone()) {
            Entry::Occupied(_) => Err(TenantError::NamespaceAlreadyExists(name)),
            Entry::Vacant(vacant) => {
                let info = NamespaceInfo {
                    id,
                    description: description.into(),
                    owner: owner.into(),
                    quota,
                    created_at_ms: now_ms(),
                };

                let state = NamespaceState {
                    info: info.clone(),
                    usage: NamespaceUsage::default(),
                };

                vacant.insert(state);

                info!(namespace = %name, "namespace created");
                #[allow(clippy::cast_precision_loss)]
                metrics::gauge!("chronix_namespace_count").set(self.namespaces.len() as f64);

                self.save_snapshot_best_effort();
                Ok(info)
            }
        }
    }

    /// Delete a namespace.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    /// The `default` namespace cannot be deleted.
    pub fn delete_namespace(&self, name: &str) -> Result<NamespaceInfo> {
        if name == NamespaceId::DEFAULT {
            return Err(TenantError::InvalidConfig(
                "cannot delete the default namespace".to_string(),
            ));
        }

        let (_, state) = self
            .namespaces
            .remove(name)
            .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))?;

        info!(namespace = %name, "namespace deleted");
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("chronix_namespace_count").set(self.namespaces.len() as f64);

        // Join any in-flight background snapshot to prevent it from
        // overwriting our synchronous save with stale state.
        if let Ok(mut guard) = self.pending_snapshot.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }

        // Persist synchronously — deletes are destructive and must be
        // durable before returning to the caller.
        self.save_snapshot_inner()?;
        Ok(state.info)
    }

    /// Get namespace info by name.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    pub fn get(&self, name: &str) -> Result<NamespaceState> {
        self.namespaces
            .get(name)
            .map(|r| r.value().clone())
            .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))
    }

    /// Check if a namespace exists.
    #[must_use]
    pub fn exists(&self, name: &str) -> bool {
        self.namespaces.contains_key(name)
    }

    /// List all namespace names.
    #[must_use]
    pub fn list_names(&self) -> Vec<String> {
        self.namespaces.iter().map(|r| r.key().clone()).collect()
    }

    /// List all namespace states.
    #[must_use]
    pub fn list_all(&self) -> Vec<NamespaceState> {
        self.namespaces.iter().map(|r| r.value().clone()).collect()
    }

    /// Get the number of registered namespaces.
    #[must_use]
    pub fn count(&self) -> usize {
        self.namespaces.len()
    }

    /// Update the quota for a namespace.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    pub fn update_quota(&self, name: &str, quota: NamespaceQuota) -> Result<()> {
        // Validate quota upper bounds before applying.
        quota.validate().map_err(TenantError::InvalidConfig)?;
        {
            let mut entry = self
                .namespaces
                .get_mut(name)
                .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))?;
            entry.value_mut().info.quota = quota;
        }
        debug!(namespace = %name, "quota updated");
        self.save_snapshot_best_effort();
        Ok(())
    }

    /// Get a reference to the usage counters for a namespace.
    ///
    /// Returns `None` if the namespace does not exist.
    #[must_use]
    pub fn get_usage(&self, name: &str) -> Option<NamespaceUsage> {
        self.namespaces.get(name).map(|r| r.value().usage.clone())
    }

    /// Update usage counters for a namespace.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    pub fn update_usage(&self, name: &str, usage: NamespaceUsage) -> Result<()> {
        let mut entry = self
            .namespaces
            .get_mut(name)
            .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))?;

        entry.value_mut().usage = usage;
        Ok(())
    }

    /// Increment the series count for a namespace.
    ///
    /// Uses saturating addition to prevent `u64` overflow.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    pub fn increment_series(&self, name: &str, count: u64) -> Result<()> {
        let mut entry = self
            .namespaces
            .get_mut(name)
            .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))?;

        entry.value_mut().usage.series_count =
            entry.value().usage.series_count.saturating_add(count);
        Ok(())
    }

    /// Increment the storage bytes for a namespace.
    ///
    /// Uses saturating addition to prevent `u64` overflow.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    pub fn increment_storage(&self, name: &str, bytes: u64) -> Result<()> {
        let mut entry = self
            .namespaces
            .get_mut(name)
            .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))?;

        entry.value_mut().usage.storage_bytes =
            entry.value().usage.storage_bytes.saturating_add(bytes);
        Ok(())
    }

    /// Increment the measurement count for a namespace.
    ///
    /// Uses saturating addition to prevent `u32` overflow.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    pub fn increment_measurements(&self, name: &str, count: u32) -> Result<()> {
        let mut entry = self
            .namespaces
            .get_mut(name)
            .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))?;

        entry.value_mut().usage.measurements =
            entry.value().usage.measurements.saturating_add(count);
        Ok(())
    }

    /// Atomically check write quota and increment usage counters.
    ///
    /// Holds the DashMap shard lock for the namespace across both the
    /// quota check and the usage update, eliminating the TOCTOU race
    /// between a separate `check_write()` and `increment_series()`.
    ///
    /// The ingestion rate is tracked as a **windowed counter** that resets
    /// once per second. This prevents the rate from accumulating
    /// indefinitely and permanently blocking writes.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    /// Returns `QuotaExceeded` if any resource limit would be breached.
    pub fn check_and_increment_write(
        &self,
        name: &str,
        new_series: u64,
        new_points: u64,
    ) -> Result<()> {
        let mut entry = self
            .namespaces
            .get_mut(name)
            .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))?;

        // Read quota limits and current usage into local copies to avoid
        // overlapping borrows between value() and value_mut().
        let max_series = entry.value().info.quota.max_series_count;
        let max_rate = entry.value().info.quota.max_ingestion_rate;
        let max_storage = entry.value().info.quota.max_storage_bytes;
        let current_series = entry.value().usage.series_count;
        let current_storage = entry.value().usage.storage_bytes;

        // 1. Series count
        let projected_series = current_series.saturating_add(new_series);
        if projected_series > max_series {
            return Err(TenantError::QuotaExceeded {
                namespace: name.to_string(),
                resource: "series_count".to_string(),
                current: current_series,
                limit: max_series,
            });
        }

        // 2. Ingestion rate — windowed per second.
        //    Reset the counter if we've moved into a new 1-second window.
        let now_s = now_ms() / 1000;
        let last_window = entry.value().usage.ingestion_rate_window_s;
        let current_rate = if last_window == now_s {
            entry.value().usage.ingestion_rate
        } else {
            // New window — reset rate.
            0.0
        };

        #[allow(clippy::cast_precision_loss)]
        let projected_rate = current_rate + new_points as f64;
        #[allow(clippy::cast_precision_loss)]
        let rate_limit = max_rate as f64;
        if projected_rate > rate_limit {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let current = current_rate as u64;
            return Err(TenantError::QuotaExceeded {
                namespace: name.to_string(),
                resource: "ingestion_rate".to_string(),
                current,
                limit: max_rate,
            });
        }

        // 3. Storage (soft check — bytes tracked asynchronously)
        if current_storage > max_storage {
            return Err(TenantError::QuotaExceeded {
                namespace: name.to_string(),
                resource: "storage_bytes".to_string(),
                current: current_storage,
                limit: max_storage,
            });
        }

        // All checks passed — atomically update usage while still holding
        // the shard lock.
        let state_mut = entry.value_mut();
        state_mut.usage.series_count = projected_series;
        state_mut.usage.ingestion_rate = projected_rate;
        state_mut.usage.ingestion_rate_window_s = now_s;

        Ok(())
    }

    /// Atomically check measurement quota and increment count.
    ///
    /// Like [`check_and_increment_write`](Self::check_and_increment_write),
    /// this holds the shard lock across check and update.
    ///
    /// # Errors
    ///
    /// Returns `NamespaceNotFound` if the namespace does not exist.
    /// Returns `QuotaExceeded` if the measurement limit would be breached.
    pub fn check_and_increment_measurement(&self, name: &str) -> Result<()> {
        let mut entry = self
            .namespaces
            .get_mut(name)
            .ok_or_else(|| TenantError::NamespaceNotFound(name.to_string()))?;

        let state = entry.value();
        let projected = u64::from(state.usage.measurements) + 1;
        let limit = u64::from(state.info.quota.max_measurements);

        if projected > limit {
            return Err(TenantError::QuotaExceeded {
                namespace: name.to_string(),
                resource: "measurements".to_string(),
                current: u64::from(state.usage.measurements),
                limit,
            });
        }

        entry.value_mut().usage.measurements = state.usage.measurements.saturating_add(1);
        Ok(())
    }

    /// Compute usage ratios for all namespaces.
    ///
    /// Returns a list of `(namespace, resource, ratio)` tuples suitable for
    /// metric emission or admin API responses. The ratio is `current / limit`
    /// (0.0 when the limit is 0).
    #[must_use]
    pub fn usage_ratios(&self) -> Vec<UsageRatio> {
        let mut ratios = Vec::new();

        for entry in &self.namespaces {
            let ns = entry.key();
            let state = entry.value();
            let q = &state.info.quota;
            let u = &state.usage;

            ratios.push(UsageRatio {
                namespace: ns.clone(),
                resource: "series_count".to_string(),
                current: u.series_count,
                limit: q.max_series_count,
                ratio: ratio(u.series_count, q.max_series_count),
            });
            ratios.push(UsageRatio {
                namespace: ns.clone(),
                resource: "storage_bytes".to_string(),
                current: u.storage_bytes,
                limit: q.max_storage_bytes,
                ratio: ratio(u.storage_bytes, q.max_storage_bytes),
            });
            ratios.push(UsageRatio {
                namespace: ns.clone(),
                resource: "measurements".to_string(),
                current: u64::from(u.measurements),
                limit: u64::from(q.max_measurements),
                ratio: ratio(u64::from(u.measurements), u64::from(q.max_measurements)),
            });
        }

        ratios
    }

    /// Emit `chronix_namespace_usage_ratio` gauges for all namespaces.
    ///
    /// Intended to be called periodically (e.g. every 10 s) from a
    /// background task so dashboards always have fresh data, not just
    /// on write paths.
    pub fn emit_usage_metrics(&self) {
        for r in self.usage_ratios() {
            metrics::gauge!(
                "chronix_namespace_usage_ratio",
                "namespace" => r.namespace,
                "resource" => r.resource,
            )
            .set(r.ratio);
        }
    }
}

/// Per-resource usage ratio for a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageRatio {
    /// Namespace identifier.
    pub namespace: String,
    /// Resource type (e.g. `"series_count"`, `"storage_bytes"`).
    pub resource: String,
    /// Current usage value.
    pub current: u64,
    /// Configured limit.
    pub limit: u64,
    /// Usage ratio (0.0–1.0+). Values > 1.0 indicate over-quota.
    pub ratio: f64,
}

/// Compute a safe ratio avoiding division by zero.
///
/// When the limit is zero and usage is non-zero, returns [`f64::MAX`]
/// to indicate over-quota. This avoids returning `f64::INFINITY` which
/// is not valid JSON (RFC 7159) and breaks `serde_json` serialization.
#[allow(clippy::cast_precision_loss)]
fn ratio(current: u64, limit: u64) -> f64 {
    if limit == 0 {
        return if current > 0 { f64::MAX } else { 0.0 };
    }
    current as f64 / limit as f64
}

impl Default for NamespaceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for NamespaceRegistry {
    fn drop(&mut self) {
        // Wait for any in-flight background snapshot to complete so the
        // data is durable before the registry goes away.
        if let Ok(mut guard) = self.pending_snapshot.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
    }
}

fn now_ms() -> u64 {
    #[allow(clippy::cast_possible_truncation)]
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Create a shared, reference-counted registry.
#[must_use]
pub fn shared_registry() -> Arc<NamespaceRegistry> {
    Arc::new(NamespaceRegistry::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_namespace_exists_on_creation() {
        let registry = NamespaceRegistry::new();
        assert!(registry.exists(NamespaceId::DEFAULT));
        assert_eq!(registry.count(), 1);
    }

    #[test]
    fn create_and_get_namespace() {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("team-alpha").unwrap();
        let info = registry
            .create_namespace(id, "Team Alpha", "admin", NamespaceQuota::default())
            .unwrap();

        assert_eq!(info.id.as_str(), "team-alpha");
        assert_eq!(info.owner, "admin");
        assert_eq!(registry.count(), 2);

        let state = registry.get("team-alpha").unwrap();
        assert_eq!(state.info.description, "Team Alpha");
    }

    #[test]
    fn create_duplicate_fails() {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("my-ns").unwrap();
        registry
            .create_namespace(id, "first", "admin", NamespaceQuota::default())
            .unwrap();

        let id2 = NamespaceId::new("my-ns").unwrap();
        let err = registry
            .create_namespace(id2, "second", "admin", NamespaceQuota::default())
            .unwrap_err();

        assert!(matches!(err, TenantError::NamespaceAlreadyExists(_)));
    }

    #[test]
    fn delete_namespace() {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("temp-ns").unwrap();
        registry
            .create_namespace(id, "temp", "admin", NamespaceQuota::default())
            .unwrap();

        assert_eq!(registry.count(), 2);
        registry.delete_namespace("temp-ns").unwrap();
        assert_eq!(registry.count(), 1);
        assert!(!registry.exists("temp-ns"));
    }

    #[test]
    fn cannot_delete_default_namespace() {
        let registry = NamespaceRegistry::new();
        let err = registry.delete_namespace("default").unwrap_err();
        assert!(matches!(err, TenantError::InvalidConfig(_)));
    }

    #[test]
    fn delete_nonexistent_fails() {
        let registry = NamespaceRegistry::new();
        let err = registry.delete_namespace("nope").unwrap_err();
        assert!(matches!(err, TenantError::NamespaceNotFound(_)));
    }

    #[test]
    fn list_namespaces() {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("ns-a").unwrap();
        registry
            .create_namespace(id, "A", "admin", NamespaceQuota::default())
            .unwrap();
        let id = NamespaceId::new("ns-b").unwrap();
        registry
            .create_namespace(id, "B", "admin", NamespaceQuota::default())
            .unwrap();

        let mut names = registry.list_names();
        names.sort();
        assert_eq!(names, vec!["default", "ns-a", "ns-b"]);
    }

    #[test]
    fn update_quota() {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("quota-ns").unwrap();
        registry
            .create_namespace(id, "quota test", "admin", NamespaceQuota::default())
            .unwrap();

        let new_quota = NamespaceQuota {
            max_series_count: 500,
            max_ingestion_rate: 1000,
            max_storage_bytes: 1024,
            max_measurements: 10,
            max_request_rps: 0,
            max_request_burst: 0,
        };

        registry
            .update_quota("quota-ns", new_quota.clone())
            .unwrap();
        let state = registry.get("quota-ns").unwrap();
        assert_eq!(state.info.quota, new_quota);
    }

    #[test]
    fn usage_tracking() {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("usage-ns").unwrap();
        registry
            .create_namespace(id, "usage test", "admin", NamespaceQuota::default())
            .unwrap();

        registry.increment_series("usage-ns", 100).unwrap();
        registry.increment_storage("usage-ns", 4096).unwrap();
        registry.increment_measurements("usage-ns", 5).unwrap();

        let usage = registry.get_usage("usage-ns").unwrap();
        assert_eq!(usage.series_count, 100);
        assert_eq!(usage.storage_bytes, 4096);
        assert_eq!(usage.measurements, 5);
    }

    #[test]
    fn shared_registry_is_arc() {
        let reg = shared_registry();
        let reg2 = reg.clone();
        let id = NamespaceId::new("shared-test").unwrap();
        reg.create_namespace(id, "test", "admin", NamespaceQuota::default())
            .unwrap();
        assert!(reg2.exists("shared-test"));
    }

    #[test]
    fn namespace_id_validation() {
        assert!(NamespaceId::new("valid-name").is_ok());
        assert!(NamespaceId::new("a123").is_ok());
        assert!(NamespaceId::new("").is_err()); // empty
        assert!(NamespaceId::new("Invalid").is_err()); // uppercase
        assert!(NamespaceId::new("has space").is_err()); // space
        assert!(NamespaceId::new("-leading").is_err()); // leading hyphen
        assert!(NamespaceId::new("trailing-").is_err()); // trailing hyphen
        assert!(NamespaceId::new("a".repeat(64)).is_err()); // too long
        assert!(NamespaceId::new("a".repeat(63)).is_ok()); // max length ok
    }

    // ── Usage ratios ──────────────────────────────────────────

    #[test]
    fn usage_ratios_empty_registry() {
        let registry = NamespaceRegistry::new();
        let ratios = registry.usage_ratios();
        // Only "default" namespace → 3 resources
        assert_eq!(ratios.len(), 3);
        for r in &ratios {
            assert_eq!(r.namespace, "default");
            assert!((r.ratio - 0.0).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn usage_ratios_with_usage() {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("metrics-ns").unwrap();
        let quota = NamespaceQuota {
            max_series_count: 100,
            max_ingestion_rate: 1000,
            max_storage_bytes: 10_000,
            max_measurements: 20,
            max_request_rps: 0,
            max_request_burst: 0,
        };
        registry
            .create_namespace(id, "test", "admin", quota)
            .unwrap();

        registry.increment_series("metrics-ns", 50).unwrap();
        registry.increment_storage("metrics-ns", 7500).unwrap();
        registry.increment_measurements("metrics-ns", 10).unwrap();

        let ratios = registry.usage_ratios();
        let ns_ratios: Vec<_> = ratios
            .iter()
            .filter(|r| r.namespace == "metrics-ns")
            .collect();
        assert_eq!(ns_ratios.len(), 3);

        let series = ns_ratios
            .iter()
            .find(|r| r.resource == "series_count")
            .unwrap();
        assert!((series.ratio - 0.5).abs() < f64::EPSILON);
        assert_eq!(series.current, 50);
        assert_eq!(series.limit, 100);

        let storage = ns_ratios
            .iter()
            .find(|r| r.resource == "storage_bytes")
            .unwrap();
        assert!((storage.ratio - 0.75).abs() < f64::EPSILON);

        let measurements = ns_ratios
            .iter()
            .find(|r| r.resource == "measurements")
            .unwrap();
        assert!((measurements.ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn emit_usage_metrics_does_not_panic() {
        let registry = NamespaceRegistry::new();
        let id = NamespaceId::new("emit-ns").unwrap();
        registry
            .create_namespace(id, "test", "admin", NamespaceQuota::default())
            .unwrap();
        registry.increment_series("emit-ns", 42).unwrap();

        // Should not panic (metrics crate handles missing recorder gracefully).
        registry.emit_usage_metrics();
    }

    #[test]
    fn usage_ratio_zero_limit() {
        // current > 0, limit == 0 → over-quota → f64::MAX (JSON-safe)
        let r = ratio(100, 0);
        assert_eq!(r, f64::MAX);

        // current == 0, limit == 0 → 0.0
        let r = ratio(0, 0);
        assert!((r - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn usage_ratio_over_quota() {
        let r = ratio(200, 100);
        assert!((r - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn open_creates_and_loads_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("namespaces");

        // Open creates directory and initial snapshot
        {
            let reg = NamespaceRegistry::open(&dir).unwrap();
            let id = NamespaceId::new("test-ns").unwrap();
            reg.create_namespace(id, "Test", "admin", NamespaceQuota::default())
                .unwrap();
            assert_eq!(reg.count(), 2);
        }

        // Reopen loads the snapshot
        {
            let reg = NamespaceRegistry::open(&dir).unwrap();
            assert_eq!(reg.count(), 2);
            assert!(reg.exists("test-ns"));
            assert!(reg.exists(NamespaceId::DEFAULT));
        }
    }

    #[test]
    fn open_delete_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("namespaces");

        {
            let reg = NamespaceRegistry::open(&dir).unwrap();
            let id = NamespaceId::new("ephemeral").unwrap();
            reg.create_namespace(id, "Ephemeral", "admin", NamespaceQuota::default())
                .unwrap();
            reg.delete_namespace("ephemeral").unwrap();
        }

        {
            let reg = NamespaceRegistry::open(&dir).unwrap();
            assert_eq!(reg.count(), 1);
            assert!(!reg.exists("ephemeral"));
        }
    }

    #[test]
    fn open_quota_update_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("namespaces");

        {
            let reg = NamespaceRegistry::open(&dir).unwrap();
            let id = NamespaceId::new("quotans").unwrap();
            reg.create_namespace(id, "Quota Test", "admin", NamespaceQuota::default())
                .unwrap();
            reg.update_quota(
                "quotans",
                NamespaceQuota {
                    max_series_count: 42,
                    ..NamespaceQuota::default()
                },
            )
            .unwrap();
        }

        {
            let reg = NamespaceRegistry::open(&dir).unwrap();
            let state = reg.get("quotans").unwrap();
            assert_eq!(state.info.quota.max_series_count, 42);
        }
    }
}
