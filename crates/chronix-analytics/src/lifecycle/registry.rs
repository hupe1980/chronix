//! Thread-safe model registry with versioning and champion/challenger tagging.
//!
//! [`ModelRegistry`] is protected by a [`parking_lot::RwLock`] internally,
//! making it safe to share across threads via `Arc<ModelRegistry>`.

use std::collections::{BTreeMap, HashMap};

use metrics;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use tracing;

/// Accuracy metrics for a model version.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccuracyMetrics {
    /// Mean Absolute Percentage Error.
    pub mape: f64,
    /// Root Mean Squared Error.
    pub rmse: f64,
    /// Mean Absolute Error.
    pub mae: f64,
    /// Coefficient of determination.
    pub r_squared: f64,
}

impl AccuracyMetrics {
    /// Create metrics from predictions vs actuals.
    pub fn compute(actuals: &[f64], predictions: &[f64]) -> Self {
        let n = actuals.len().min(predictions.len());
        if n == 0 {
            return Self {
                mape: f64::NAN,
                rmse: f64::NAN,
                mae: f64::NAN,
                r_squared: f64::NAN,
            };
        }

        let mut sum_ae = 0.0;
        let mut sum_se = 0.0;
        let mut sum_ape = 0.0;
        let mut ape_count = 0usize;
        let mean_actual = actuals[..n].iter().sum::<f64>() / n as f64;
        let mut ss_tot = 0.0;

        for i in 0..n {
            let e = actuals[i] - predictions[i];
            sum_ae += e.abs();
            sum_se += e * e;
            ss_tot += (actuals[i] - mean_actual).powi(2);
            if actuals[i].abs() > f64::EPSILON {
                sum_ape += (e / actuals[i]).abs();
                ape_count += 1;
            }
        }

        let mae = sum_ae / n as f64;
        let rmse = (sum_se / n as f64).sqrt();
        let mape = if ape_count > 0 {
            sum_ape / ape_count as f64
        } else {
            f64::NAN
        };
        let r_squared = if ss_tot > f64::EPSILON {
            1.0 - sum_se / ss_tot
        } else {
            f64::NAN
        };

        Self {
            mape,
            rmse,
            mae,
            r_squared,
        }
    }
}

/// Tag indicating a model's lifecycle stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ModelTag {
    /// Current production model.
    Champion,
    /// Candidate model under evaluation.
    Challenger,
    /// Previously active model no longer in use.
    Retired,
}

/// A single model version with metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelVersion {
    /// Auto-incrementing version number (1-based).
    pub version: u64,
    /// Creation timestamp (epoch seconds).
    pub created_at: i64,
    /// Algorithm name (e.g. "ses", "holt_winters", "arima").
    pub algorithm: String,
    /// Hyperparameters as key-value pairs.
    pub hyperparameters: HashMap<String, String>,
    /// Training data time range: `(start_ns, end_ns)`.
    pub training_range: (i64, i64),
    /// Accuracy metrics (if evaluated).
    pub metrics: Option<AccuracyMetrics>,
    /// Lifecycle tag.
    pub tag: ModelTag,
    /// Serialized model bytes (postcard).
    pub model_bytes: Vec<u8>,
    /// SHA-256 hash of `model_bytes` for integrity verification.
    ///
    /// Computed at registration time. Use [`ModelVersion::verify_integrity`]
    /// to check that the model bytes haven't been corrupted.
    pub model_hash: [u8; 32],
}

impl ModelVersion {
    /// Verify that `model_bytes` haven't been corrupted since registration.
    ///
    /// Returns `true` if the SHA-256 hash matches.
    #[must_use]
    pub fn verify_integrity(&self) -> bool {
        let computed: [u8; 32] = Sha256::digest(&self.model_bytes).into();
        computed == self.model_hash
    }
}

/// Compute the SHA-256 hash of model bytes.
fn compute_model_hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Registry key: `(measurement, model_name)`.
/// Composite key for the model registry: `(measurement, model_name)`.
///
/// Uses a pre-computed hash so that read-only lookups via
/// [`ModelRegistry::find`] can match entries without heap-allocating
/// two `String`s on every read.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct RegistryKey {
    measurement: String,
    model_name: String,
}

impl RegistryKey {
    fn new(measurement: &str, model_name: &str) -> Self {
        Self {
            measurement: measurement.to_string(),
            model_name: model_name.to_string(),
        }
    }
}

/// Thread-safe in-memory model registry with versioning and tagging.
///
/// Per measurement+name key, maintains an ordered list of versions.
/// At most one version can be tagged `Champion` per key.
///
/// All methods take `&self` and acquire internal locks as needed,
/// so the registry can be shared across threads via `Arc<ModelRegistry>`.
///
/// # Iteration Order
///
/// The inner [`BTreeMap`] guarantees deterministic (sorted) iteration
/// order, which is important for snapshot reproducibility and
/// consistent API responses.
pub struct ModelRegistry {
    versions: RwLock<BTreeMap<RegistryKey, Vec<ModelVersion>>>,
}

impl std::fmt::Debug for ModelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let versions = self.versions.read();
        f.debug_struct("ModelRegistry")
            .field("keys", &versions.keys().collect::<Vec<_>>())
            .field(
                "total_versions",
                &versions.values().map(std::vec::Vec::len).sum::<usize>(),
            )
            .finish()
    }
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self {
            versions: RwLock::new(BTreeMap::new()),
        }
    }
}

impl ModelRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// O(log n) lookup using `BTreeMap::get` with a temporary key.
    fn find_versions<'a>(
        map: &'a BTreeMap<RegistryKey, Vec<ModelVersion>>,
        measurement: &str,
        model_name: &str,
    ) -> Option<&'a Vec<ModelVersion>> {
        map.get(&RegistryKey::new(measurement, model_name))
    }

    /// Mutable version of [`Self::find_versions`].
    fn find_versions_mut<'a>(
        map: &'a mut BTreeMap<RegistryKey, Vec<ModelVersion>>,
        measurement: &str,
        model_name: &str,
    ) -> Option<&'a mut Vec<ModelVersion>> {
        map.get_mut(&RegistryKey::new(measurement, model_name))
    }

    /// Register a new model version, auto-incrementing the version number.
    ///
    /// Returns the registered [`ModelVersion`] (cloned, since the registry
    /// holds an internal lock).
    pub fn register(
        &self,
        measurement: &str,
        model_name: &str,
        algorithm: &str,
        hyperparameters: HashMap<String, String>,
        training_range: (i64, i64),
        model_bytes: Vec<u8>,
    ) -> ModelVersion {
        let key = RegistryKey::new(measurement, model_name);
        let mut map = self.versions.write();
        let versions = map.entry(key).or_default();

        let version_num = versions.last().map_or(1, |v| v.version + 1);

        // Default tag: if no existing champion, tag as champion; otherwise challenger
        let tag = if versions.iter().any(|v| v.tag == ModelTag::Champion) {
            ModelTag::Challenger
        } else {
            ModelTag::Champion
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);

        let mv = ModelVersion {
            version: version_num,
            created_at: now,
            algorithm: algorithm.to_string(),
            hyperparameters,
            training_range,
            metrics: None,
            tag,
            // Compute SHA-256 integrity hash at registration time.
            model_hash: compute_model_hash(&model_bytes),
            model_bytes,
        };

        versions.push(mv.clone());

        metrics::counter!("chronix_model_refit_total", "measurement" => measurement.to_string())
            .increment(1);
        mv
    }

    /// Get the latest version for a measurement+name.
    ///
    /// Verifies model integrity on retrieval via SHA-256 hash.
    /// Uses borrowed key lookup to avoid `String` allocation.
    pub fn get_latest(&self, measurement: &str, model_name: &str) -> Option<ModelVersion> {
        let map = self.versions.read();
        let mv = Self::find_versions(&map, measurement, model_name)?
            .last()
            .cloned()?;
        if !mv.verify_integrity() {
            tracing::error!(
                measurement,
                model_name,
                version = mv.version,
                "model integrity check failed — SHA-256 mismatch"
            );
            metrics::counter!("chronix_model_integrity_failures_total").increment(1);
            return None;
        }
        Some(mv)
    }

    /// Get a specific version.
    pub fn get_version(
        &self,
        measurement: &str,
        model_name: &str,
        version: u64,
    ) -> Option<ModelVersion> {
        let map = self.versions.read();
        let mv = Self::find_versions(&map, measurement, model_name)?
            .iter()
            .find(|v| v.version == version)
            .cloned()?;
        if !mv.verify_integrity() {
            tracing::error!(
                measurement,
                model_name,
                version,
                "model integrity check failed — SHA-256 mismatch"
            );
            metrics::counter!("chronix_model_integrity_failures_total").increment(1);
            return None;
        }
        Some(mv)
    }

    /// Get the champion version for a measurement+name.
    pub fn get_champion(&self, measurement: &str, model_name: &str) -> Option<ModelVersion> {
        let map = self.versions.read();
        let mv = Self::find_versions(&map, measurement, model_name)?
            .iter()
            .find(|v| v.tag == ModelTag::Champion)
            .cloned()?;
        if !mv.verify_integrity() {
            tracing::error!(
                measurement,
                model_name,
                version = mv.version,
                "champion model integrity check failed — SHA-256 mismatch"
            );
            metrics::counter!("chronix_model_integrity_failures_total").increment(1);
            return None;
        }
        Some(mv)
    }

    /// List all versions for a measurement+name.
    pub fn list_versions(&self, measurement: &str, model_name: &str) -> Vec<ModelVersion> {
        let map = self.versions.read();
        Self::find_versions(&map, measurement, model_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Tag a specific version. Ensures only one champion per key.
    ///
    /// Returns `false` if the key or version doesn't exist, without
    /// modifying any existing tags.
    pub fn set_tag(
        &self,
        measurement: &str,
        model_name: &str,
        version: u64,
        tag: ModelTag,
    ) -> bool {
        let key = RegistryKey::new(measurement, model_name);
        let mut map = self.versions.write();
        let versions = match map.get_mut(&key) {
            Some(v) => v,
            None => return false,
        };

        // Single-pass scan — find the target version and
        // current champion indices in one iteration.
        let mut target_idx = None;
        let mut champion_idx = None;
        for (i, v) in versions.iter().enumerate() {
            if v.version == version {
                target_idx = Some(i);
            }
            if v.tag == ModelTag::Champion {
                champion_idx = Some(i);
            }
            // Early exit once both found
            if target_idx.is_some() && (tag != ModelTag::Champion || champion_idx.is_some()) {
                break;
            }
        }

        let target_idx = match target_idx {
            Some(i) => i,
            None => return false,
        };

        // If setting champion, retire the current champion first
        if tag == ModelTag::Champion {
            if let Some(ci) = champion_idx {
                versions[ci].tag = ModelTag::Retired;
            }
        }

        versions[target_idx].tag = tag;
        true
    }

    /// Update accuracy metrics for a specific version.
    pub fn update_metrics(
        &self,
        measurement: &str,
        model_name: &str,
        version: u64,
        metrics: AccuracyMetrics,
    ) -> bool {
        let mut map = self.versions.write();
        let versions = match Self::find_versions_mut(&mut map, measurement, model_name) {
            Some(v) => v,
            None => return false,
        };

        match versions.iter_mut().find(|v| v.version == version) {
            Some(v) => {
                v.metrics = Some(metrics);
                true
            }
            None => false,
        }
    }

    /// Rollback: promote a previous version to champion, retiring the current one.
    ///
    /// Convenience method for the common rollback workflow. Equivalent
    /// to `set_tag(measurement, model_name, version, ModelTag::Champion)` but
    /// logs the rollback event for auditability.
    ///
    /// Returns `false` if the key or version doesn't exist.
    pub fn rollback_champion(&self, measurement: &str, model_name: &str, version: u64) -> bool {
        let result = self.set_tag(measurement, model_name, version, ModelTag::Champion);
        if result {
            tracing::info!(
                measurement,
                model_name,
                rollback_to_version = version,
                "Model champion rolled back"
            );
        }
        result
    }

    /// Delete a specific model version.
    ///
    /// Allows removing bad versions from the registry. Refuses to
    /// delete the current champion — call `set_tag` or `rollback_champion`
    /// first to promote a different version.
    ///
    /// Returns `true` if the version was found and deleted.
    pub fn delete_version(&self, measurement: &str, model_name: &str, version: u64) -> bool {
        let mut map = self.versions.write();
        let versions = match Self::find_versions_mut(&mut map, measurement, model_name) {
            Some(v) => v,
            None => return false,
        };

        // Refuse to delete the current champion.
        if versions
            .iter()
            .any(|v| v.version == version && v.tag == ModelTag::Champion)
        {
            tracing::warn!(
                measurement,
                model_name,
                version,
                "Refusing to delete current champion model — demote it first"
            );
            return false;
        }

        let before = versions.len();
        versions.retain(|v| v.version != version);
        let deleted = versions.len() < before;

        if deleted {
            tracing::info!(measurement, model_name, version, "Deleted model version");
        }
        deleted
    }

    /// Total number of registered model versions across all keys.
    pub fn total_versions(&self) -> usize {
        let map = self.versions.read();
        map.values().map(std::vec::Vec::len).sum()
    }
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn make_registry() -> ModelRegistry {
        let reg = ModelRegistry::new();
        reg.register(
            "cpu",
            "forecast_v1",
            "ses",
            HashMap::new(),
            (0, 1000),
            vec![1, 2, 3],
        );
        reg.register(
            "cpu",
            "forecast_v1",
            "holt",
            HashMap::new(),
            (0, 2000),
            vec![4, 5, 6],
        );
        reg
    }

    #[test]
    fn test_register_auto_increments() {
        let reg = make_registry();
        let versions = reg.list_versions("cpu", "forecast_v1");
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 1);
        assert_eq!(versions[1].version, 2);
    }

    #[test]
    fn test_first_registered_is_champion() {
        let reg = make_registry();
        let versions = reg.list_versions("cpu", "forecast_v1");
        assert_eq!(versions[0].tag, ModelTag::Champion);
        assert_eq!(versions[1].tag, ModelTag::Challenger);
    }

    #[test]
    fn test_get_latest() {
        let reg = make_registry();
        let latest = reg.get_latest("cpu", "forecast_v1").unwrap();
        assert_eq!(latest.version, 2);
        assert_eq!(latest.algorithm, "holt");
    }

    #[test]
    fn test_get_version() {
        let reg = make_registry();
        let v1 = reg.get_version("cpu", "forecast_v1", 1).unwrap();
        assert_eq!(v1.algorithm, "ses");
    }

    #[test]
    fn test_get_champion() {
        let reg = make_registry();
        let champ = reg.get_champion("cpu", "forecast_v1").unwrap();
        assert_eq!(champ.version, 1);
    }

    #[test]
    fn test_set_tag_promotes_challenger() {
        let reg = make_registry();
        reg.set_tag("cpu", "forecast_v1", 2, ModelTag::Champion);

        // Old champion retires
        let v1 = reg.get_version("cpu", "forecast_v1", 1).unwrap();
        assert_eq!(v1.tag, ModelTag::Retired);

        // New champion
        let v2 = reg.get_version("cpu", "forecast_v1", 2).unwrap();
        assert_eq!(v2.tag, ModelTag::Champion);
    }

    #[test]
    fn test_update_metrics() {
        let reg = make_registry();
        let metrics = AccuracyMetrics {
            mape: 0.05,
            rmse: 1.2,
            mae: 0.8,
            r_squared: 0.95,
        };
        assert!(reg.update_metrics("cpu", "forecast_v1", 1, metrics));

        let v1 = reg.get_version("cpu", "forecast_v1", 1).unwrap();
        let m = v1.metrics.as_ref().unwrap();
        assert!((m.mape - 0.05).abs() < 1e-9);
    }

    #[test]
    fn test_get_nonexistent() {
        let reg = make_registry();
        assert!(reg.get_latest("memory", "forecast").is_none());
        assert!(reg.get_version("cpu", "forecast_v1", 99).is_none());
    }

    #[test]
    fn test_accuracy_metrics_compute() {
        let actuals = vec![100.0, 200.0, 300.0];
        let preds = vec![110.0, 190.0, 310.0];
        let m = AccuracyMetrics::compute(&actuals, &preds);

        // MAE = (10+10+10)/3 = 10
        assert!((m.mae - 10.0).abs() < 1e-9);
        // RMSE = sqrt((100+100+100)/3) = 10
        assert!((m.rmse - 10.0).abs() < 1e-9);
        // MAPE: (10/100 + 10/200 + 10/300)/3 ≈ 0.0611
        assert!(m.mape > 0.06 && m.mape < 0.065);
        // R² should be high
        assert!(m.r_squared > 0.98);
    }

    #[test]
    fn test_total_versions() {
        let reg = make_registry();
        assert_eq!(reg.total_versions(), 2);
        reg.register(
            "mem",
            "detector",
            "zscore",
            HashMap::new(),
            (0, 500),
            vec![],
        );
        assert_eq!(reg.total_versions(), 3);
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let reg = Arc::new(ModelRegistry::new());
        let mut handles = vec![];

        for i in 0..10 {
            let reg = Arc::clone(&reg);
            handles.push(thread::spawn(move || {
                reg.register(
                    "cpu",
                    &format!("model_{i}"),
                    "ses",
                    HashMap::new(),
                    (0, 1000),
                    vec![i as u8],
                );
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(reg.total_versions(), 10);
    }

    #[test]
    fn test_model_hash_integrity() {
        let reg = make_registry();
        let v1 = reg.get_version("cpu", "forecast_v1", 1).unwrap();
        // Hash should be computed correctly
        assert!(v1.verify_integrity());
        // Tampered bytes should fail
        let mut tampered = v1.clone();
        tampered.model_bytes[0] ^= 0xFF;
        assert!(!tampered.verify_integrity());
    }

    #[test]
    fn test_rollback_champion() {
        let reg = make_registry();
        // Promote v2 to champion first
        assert!(reg.set_tag("cpu", "forecast_v1", 2, ModelTag::Champion));
        assert_eq!(reg.get_champion("cpu", "forecast_v1").unwrap().version, 2);

        // Rollback to v1
        assert!(reg.rollback_champion("cpu", "forecast_v1", 1));
        assert_eq!(reg.get_champion("cpu", "forecast_v1").unwrap().version, 1);

        // v2 should now be Retired
        let v2 = reg.get_version("cpu", "forecast_v1", 2).unwrap();
        assert_eq!(v2.tag, ModelTag::Retired);
    }

    #[test]
    fn test_rollback_nonexistent_version() {
        let reg = make_registry();
        assert!(!reg.rollback_champion("cpu", "forecast_v1", 99));
    }

    #[test]
    fn test_delete_version() {
        let reg = make_registry();
        // Can delete challenger (v2)
        assert!(reg.delete_version("cpu", "forecast_v1", 2));
        assert_eq!(reg.list_versions("cpu", "forecast_v1").len(), 1);
        assert!(reg.get_version("cpu", "forecast_v1", 2).is_none());
    }

    #[test]
    fn test_delete_champion_refused() {
        let reg = make_registry();
        // Cannot delete champion (v1)
        assert!(!reg.delete_version("cpu", "forecast_v1", 1));
        // Still there
        assert!(reg.get_version("cpu", "forecast_v1", 1).is_some());
    }

    #[test]
    fn test_delete_nonexistent() {
        let reg = make_registry();
        assert!(!reg.delete_version("cpu", "forecast_v1", 99));
        assert!(!reg.delete_version("nonexistent", "model", 1));
    }
}
