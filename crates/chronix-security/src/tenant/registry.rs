//! Namespace registry — CRUD for tenant namespaces.
//!
//! Manages the lifecycle of namespaces: creation, deletion, listing,
//! and configuration updates. Each namespace carries its quota limits;
//! usage is the database's to report, not this registry's.
//!
//! Optionally persists namespace state to disk via JSON snapshots.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use chronix_core::{NamespaceId, NamespaceQuota};

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

/// A registered namespace.
///
/// It carries configuration only. Usage is not kept here: a counter beside
/// the data is one nothing decrements on retention or delete, which is how
/// this registry once reported a namespace's series count without any write
/// ever moving it. Usage is read from the database, which admits every
/// series against the same exact set it counts
/// (`Chronix::namespace_usage`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceState {
    /// Namespace metadata.
    pub info: NamespaceInfo,
}

/// Thread-safe namespace registry.
///
/// Provides fast, concurrent access to namespace metadata using
/// [`DashMap`] for lock-free reads.
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
        .map_err(|e| TenantError::Persist(format!("cannot write namespace snapshot: {e}")))?;

    // Clean up tmp file on fsync or rename failure to avoid
    // stale tmp files leaking on disk.
    let result = (|| -> Result<()> {
        let f = std::fs::File::open(&tmp_path).map_err(|e| {
            TenantError::Persist(format!("cannot open tmp snapshot for fsync: {e}"))
        })?;
        f.sync_all()
            .map_err(|e| TenantError::Persist(format!("cannot fsync namespace snapshot: {e}")))?;

        std::fs::rename(&tmp_path, &final_path)
            .map_err(|e| TenantError::Persist(format!("cannot rename namespace snapshot: {e}")))?;
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
            refuse_removed_fields(&snapshot_path, &data)?;
            let states: Vec<NamespaceState> = serde_json::from_str(&data).map_err(|e| {
                TenantError::InvalidConfig(format!(
                    "corrupt namespace snapshot {}: {e}",
                    snapshot_path.display()
                ))
            })?;
            let namespaces = DashMap::new();
            for state in states {
                namespaces.insert(state.info.id.as_str().to_string(), state);
            }
            info!(count = namespaces.len(), "loaded namespace snapshot");
            Self {
                namespaces,
                persist_dir: Some(dir),
            }
        } else {
            Self {
                namespaces: DashMap::new(),
                persist_dir: Some(dir),
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
                NamespaceState { info: default_info },
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

                let state = NamespaceState { info: info.clone() };

                vacant.insert(state);

                info!(namespace = %name, "namespace created");
                #[allow(clippy::cast_precision_loss)]
                metrics::gauge!("chronix_namespace_count").set(self.namespaces.len() as f64);

                // Synchronous, like the delete below. A create that is
                // acknowledged and then lost is the delete case mirrored:
                // the operator provisions a tenant, hands out a credential
                // bound to it, and after the next restart every request from
                // that tenant answers `400 namespace not found`. The
                // in-memory entry is rolled back so the running process does
                // not disagree with its own restart.
                if let Err(e) = self.save_snapshot_inner() {
                    self.namespaces.remove(&name);
                    #[allow(clippy::cast_precision_loss)]
                    metrics::gauge!("chronix_namespace_count").set(self.namespaces.len() as f64);
                    return Err(e);
                }
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
        // Synchronous for the same reason: a quota the API reports as
        // applied and the next restart reverts is a setting that is accepted
        // and ignored.
        self.save_snapshot_inner()?;
        Ok(())
    }
}

/// Refuse a snapshot that still carries a quota field this release removed,
/// naming the file, the field and the remedy.
///
/// `NamespaceQuota` denies unknown fields, so the snapshot would be refused
/// anyway — but as "unknown field", which reads like a corrupt file. The
/// operator who upgraded needs to hear that the field was removed on
/// purpose and that deleting it is the whole fix.
fn refuse_removed_fields(path: &Path, data: &str) -> Result<()> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
        return Ok(()); // the typed parse reports a malformed file
    };
    let carries = value.as_array().is_some_and(|states| {
        states
            .iter()
            .any(|s| s.pointer("/info/quota/max_storage_bytes").is_some())
    });
    if carries {
        return Err(TenantError::InvalidConfig(format!(
            "{} sets max_storage_bytes, which was removed: namespaces share segments, so \
             per-namespace bytes cannot be measured. Delete the field from every namespace's \
             quota and restart; use retention to bound disk",
            path.display()
        )));
    }
    Ok(())
}

impl Default for NamespaceRegistry {
    fn default() -> Self {
        Self::new()
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

    /// **Creating a tenant must be durable before it is acknowledged.**
    ///
    /// A delete was — with a comment saying destructive operations must be —
    /// and a create was not: it snapshotted *best effort* on a background
    /// thread and returned `Ok` regardless. So an operator could provision a
    /// namespace, be told it exists, hand out a credential bound to it, and
    /// find after the next restart that every request from that tenant
    /// answers `400 namespace not found`. The mirror of the delete case, and
    /// the asymmetry is the tell that only one of the two was thought about.
    #[test]
    fn creating_a_namespace_is_durable_or_it_fails() {
        let dir = tempfile::tempdir().unwrap();
        let registry = NamespaceRegistry::open(dir.path()).unwrap();

        // A directory where the snapshot wants to write its temp file.
        std::fs::create_dir_all(dir.path().join("namespaces.json.tmp")).unwrap();

        let err = registry.create_namespace(
            NamespaceId::new("tenant-a").unwrap(),
            "a tenant",
            "ops",
            NamespaceQuota::default(),
        );
        assert!(
            err.is_err(),
            "a namespace that cannot be persisted must not be acknowledged"
        );
    }

    /// The same for a quota change, which `PUT /namespaces/{name}/quota`
    /// returns as though it had taken effect.
    #[test]
    fn changing_a_quota_is_durable_or_it_fails() {
        let dir = tempfile::tempdir().unwrap();
        let registry = NamespaceRegistry::open(dir.path()).unwrap();
        registry
            .create_namespace(
                NamespaceId::new("tenant-a").unwrap(),
                "a tenant",
                "ops",
                NamespaceQuota::default(),
            )
            .unwrap();

        std::fs::create_dir_all(dir.path().join("namespaces.json.tmp")).unwrap();

        let quota = NamespaceQuota {
            max_series_count: 42,
            ..NamespaceQuota::default()
        };
        assert!(
            registry.update_quota("tenant-a", quota).is_err(),
            "a quota that cannot be persisted reverts at the next restart, so \
             reporting it as applied is a setting that is accepted and ignored"
        );
    }
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

    /// **A snapshot written before the storage quota was removed is refused
    /// by name, not parsed as corrupt and not silently accepted.** Silently
    /// accepting it would keep a limit the operator believes is enforced;
    /// "corrupt" would send them looking for a failing disk.
    #[test]
    fn a_snapshot_with_the_removed_storage_quota_is_refused_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("namespaces");
        std::fs::create_dir_all(&dir).unwrap();
        let stale = serde_json::json!([{
            "info": {
                "id": "team-a",
                "description": "",
                "owner": "admin",
                "created_at_ms": 0,
                "quota": {
                    "max_series_count": 10,
                    "max_ingestion_rate": 10,
                    "max_storage_bytes": 1024,
                    "max_measurements": 1
                }
            },
            "usage": {"series_count": 0}
        }]);
        std::fs::write(dir.join(SNAPSHOT_FILE), stale.to_string()).unwrap();

        let err = NamespaceRegistry::open(&dir).unwrap_err().to_string();
        assert!(err.contains("max_storage_bytes"), "{err}");
        assert!(err.contains("removed"), "{err}");
        assert!(err.contains("namespaces.json"), "{err}");
        assert!(!err.contains("corrupt"), "{err}");
    }

    /// The `usage` block an older snapshot carries is ignored: usage is the
    /// database's to report, and a stale counter is not a fact.
    #[test]
    fn a_snapshot_with_an_old_usage_block_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("namespaces");
        std::fs::create_dir_all(&dir).unwrap();
        let old = serde_json::json!([{
            "info": {
                "id": "team-a",
                "description": "",
                "owner": "admin",
                "created_at_ms": 0,
                "quota": {
                    "max_series_count": 10,
                    "max_ingestion_rate": 10,
                    "max_measurements": 1
                }
            },
            "usage": {"series_count": 99, "storage_bytes": 7}
        }]);
        std::fs::write(dir.join(SNAPSHOT_FILE), old.to_string()).unwrap();
        let reg = NamespaceRegistry::open(&dir).unwrap();
        assert_eq!(reg.get("team-a").unwrap().info.quota.max_series_count, 10);
    }
}
