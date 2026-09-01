//! Thread-safe plugin registry for custom forecast models and anomaly detectors.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::anomaly::AnomalyDetector;
use crate::forecast::ForecastModel;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use metrics::counter;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Metadata describing a registered plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PluginInfo {
    /// Unique plugin name.
    pub name: String,
    /// Human-readable description.
    pub description: String,
    /// Semantic version string.
    pub version: String,
    /// Optional SHA-256 hash of the plugin source (hex-encoded).
    ///
    /// When set, the registry can verify this hash against an allowlist
    /// to ensure only trusted plugins are loaded.
    pub sha256_hash: Option<String>,
}

impl PluginInfo {
    /// Create new plugin info with name only (empty description and version).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            version: String::new(),
            sha256_hash: None,
        }
    }

    /// Set description.
    #[must_use]
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    /// Set version.
    #[must_use]
    pub fn with_version(mut self, ver: impl Into<String>) -> Self {
        self.version = ver.into();
        self
    }

    /// Set the SHA-256 hash of the plugin source.
    #[must_use]
    pub fn with_sha256(mut self, hash: impl Into<String>) -> Self {
        self.sha256_hash = Some(hash.into());
        self
    }
}

/// Factory function that creates a forecast model from JSON configuration.
pub type ModelFactory =
    Arc<dyn Fn(&serde_json::Value) -> Result<Box<dyn ForecastModel>, PluginError> + Send + Sync>;

/// Factory function that creates an anomaly detector from JSON configuration.
pub type DetectorFactory =
    Arc<dyn Fn(&serde_json::Value) -> Result<Box<dyn AnomalyDetector>, PluginError> + Send + Sync>;

/// Built-in model names that cannot be used for custom plugins.
const RESERVED_MODEL_NAMES: &[&str] = &[
    "ses",
    "holt",
    "holt_linear",
    "holt_winters",
    "arima",
    "sarima",
    "linear_regression",
];

/// Built-in detector names that cannot be used for custom plugins.
const RESERVED_DETECTOR_NAMES: &[&str] = &[
    "zscore",
    "modified_zscore",
    "iqr",
    "forecast_residual",
    "moving_average",
    "dynamic_threshold",
    "cusum",
];

/// Maximum allowed length for plugin names.
const MAX_NAME_LEN: usize = 64;

/// Validate a plugin name.
///
/// Rules:
/// Non-empty and no leading/trailing whitespace.
/// Max 64 characters.
/// Must start with an ASCII letter.
/// Only ASCII letters, digits, underscores, and hyphens allowed.
/// Must not conflict with a built-in name (case-insensitive).
fn validate_name(name: &str, reserved: &[&str]) -> Result<(), PluginError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(PluginError::InvalidName(
            "plugin name must not be empty or whitespace-only".into(),
        ));
    }
    if trimmed != name {
        return Err(PluginError::InvalidName(format!(
            "plugin name {name:?} contains leading/trailing whitespace"
        )));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(PluginError::InvalidName(format!(
            "plugin name exceeds maximum length of {MAX_NAME_LEN} characters"
        )));
    }
    let mut chars = name.chars();
    if let Some(first) = chars.next() {
        if !first.is_ascii_alphabetic() {
            return Err(PluginError::InvalidName(format!(
                "plugin name must start with an ASCII letter, got {first:?}"
            )));
        }
    }
    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '-' {
            return Err(PluginError::InvalidName(format!(
                "plugin name contains invalid character {ch:?}; \
                 only ASCII letters, digits, underscores, and hyphens are allowed"
            )));
        }
    }
    let lower = name.to_ascii_lowercase();
    if reserved.contains(&lower.as_str()) {
        return Err(PluginError::InvalidName(format!(
            "{name:?} conflicts with built-in name"
        )));
    }
    Ok(())
}

// ─── Generic factory map ────────────────────────────────────────────
//
// Eliminates the model/detector code duplication by extracting the
// shared DashMap + validation + metrics logic into a reusable type
// parameterised on the factory's output trait object.

/// Type-erased factory closure stored as `Arc<dyn Fn(…) -> Result<T, PluginError>>`.
type Factory<T> = Arc<dyn Fn(&serde_json::Value) -> Result<T, PluginError> + Send + Sync>;

/// A named collection of plugin factories sharing the same output type.
///
/// All operations are lock-free (DashMap-backed). Registration uses
/// atomic entry-based insertion to prevent TOCTOU races. Factory
/// execution is performed *after* releasing the DashMap shard guard.
struct FactoryMap<T: ?Sized> {
    map: DashMap<String, (PluginInfo, Factory<Box<T>>)>,
    reserved: &'static [&'static str],
    kind: &'static str,
    metric_registered: &'static str,
    metric_created: &'static str,
    /// Optional allowlist of SHA-256 hashes (hex-encoded, lowercase).
    /// When non-empty, only plugins whose `sha256_hash` matches are accepted.
    hash_allowlist: parking_lot::RwLock<HashSet<String>>,
}

impl<T: ?Sized> FactoryMap<T> {
    fn new(
        reserved: &'static [&'static str],
        kind: &'static str,
        metric_registered: &'static str,
        metric_created: &'static str,
    ) -> Self {
        Self {
            map: DashMap::new(),
            reserved,
            kind,
            metric_registered,
            metric_created,
            hash_allowlist: parking_lot::RwLock::new(HashSet::new()),
        }
    }

    /// Verify plugin hash against the allowlist.
    /// If the allowlist is empty, all plugins are accepted.
    fn verify_hash(&self, info: &PluginInfo) -> Result<(), PluginError> {
        let allowlist = self.hash_allowlist.read();
        if allowlist.is_empty() {
            return Ok(());
        }
        match &info.sha256_hash {
            Some(hash) => {
                let normalized = hash.to_ascii_lowercase();
                if allowlist.contains(&normalized) {
                    Ok(())
                } else {
                    Err(PluginError::UntrustedPlugin(format!(
                        "plugin {:?} hash {} not in allowlist",
                        info.name, normalized
                    )))
                }
            }
            None => Err(PluginError::UntrustedPlugin(format!(
                "plugin {:?} has no sha256_hash but allowlist is active",
                info.name
            ))),
        }
    }

    /// Register a factory with explicit metadata. Atomic entry insertion.
    fn register(&self, info: PluginInfo, factory: Factory<Box<T>>) -> Result<(), PluginError> {
        validate_name(&info.name, self.reserved)?;
        self.verify_hash(&info)?;
        match self.map.entry(info.name.clone()) {
            Entry::Occupied(_) => Err(PluginError::AlreadyRegistered(info.name)),
            Entry::Vacant(entry) => {
                info!(plugin = %info.name, version = %info.version, kind = self.kind, "registered custom plugin");
                counter!(self.metric_registered).increment(1);
                entry.insert((info, factory));
                Ok(())
            }
        }
    }

    /// Replace (or insert) a factory with metadata. Returns previous info.
    #[allow(clippy::needless_pass_by_value)] // factory ownership is the intended contract
    fn replace(
        &self,
        info: PluginInfo,
        factory: Factory<Box<T>>,
    ) -> Result<Option<PluginInfo>, PluginError> {
        validate_name(&info.name, self.reserved)?;
        self.verify_hash(&info)?;
        let prev = self
            .map
            .insert(info.name.clone(), (info.clone(), factory))
            .map(|(pi, _)| pi);
        // Emit audit event on plugin replacement so operators can
        // detect unexpected overwrites (e.g. supply-chain attacks).
        if let Some(ref old_info) = prev {
            tracing::warn!(
                plugin = %info.name,
                old_version = %old_info.version,
                new_version = %info.version,
                old_hash = ?old_info.sha256_hash,
                new_hash = ?info.sha256_hash,
                kind = self.kind,
                "plugin replaced — verify this was intentional",
            );
            counter!("chronix_plugin_replaced_total").increment(1);
        } else {
            info!(plugin = %info.name, version = %info.version, kind = self.kind, "registered new plugin");
        }
        counter!(self.metric_registered).increment(1);
        Ok(prev)
    }

    /// Create an instance. Clones the factory `Arc` and drops the guard
    /// before invocation so the shard lock is not held during execution.
    fn create(&self, name: &str, config: &serde_json::Value) -> Result<Box<T>, PluginError> {
        let factory = {
            let entry = self
                .map
                .get(name)
                .ok_or_else(|| PluginError::NotFound(name.to_string()))?;
            Arc::clone(&entry.value().1)
        };
        let result = factory(config);
        match &result {
            Ok(_) => counter!(self.metric_created, "status" => "ok").increment(1),
            Err(_) => counter!(self.metric_created, "status" => "error").increment(1),
        }
        result
    }

    /// Try to create — returns `None` when no plugin is registered.
    fn try_create(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Option<Result<Box<T>, PluginError>> {
        let factory = {
            let entry = self.map.get(name)?;
            Arc::clone(&entry.value().1)
        };
        let result = factory(config);
        match &result {
            Ok(_) => counter!(self.metric_created, "status" => "ok").increment(1),
            Err(_) => counter!(self.metric_created, "status" => "error").increment(1),
        }
        Some(result)
    }

    fn has(&self, name: &str) -> bool {
        self.map.contains_key(name)
    }

    fn info(&self, name: &str) -> Option<PluginInfo> {
        self.map.get(name).map(|e| e.value().0.clone())
    }

    fn list_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.map.iter().map(|e| e.key().clone()).collect();
        names.sort();
        names
    }

    fn list_infos(&self) -> Vec<PluginInfo> {
        let mut infos: Vec<_> = self.map.iter().map(|e| e.value().0.clone()).collect();
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        infos
    }

    fn unregister(&self, name: &str) -> Option<PluginInfo> {
        self.map.remove(name).map(|(_, (info, _))| info)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn clear(&self) {
        self.map.clear();
    }
}

// ─── PluginRegistry ─────────────────────────────────────────────────

/// Thread-safe registry for custom forecast models and anomaly detectors.
///
/// Plugins register factory functions keyed by name. The factories are called
/// at runtime to create model/detector instances with user-supplied config.
///
/// All registration and lookup operations are lock-free (DashMap-backed) and
/// safe under concurrent access. Registration uses atomic entry-based insertion
/// to prevent TOCTOU races.
pub struct PluginRegistry {
    models: FactoryMap<dyn ForecastModel>,
    detectors: FactoryMap<dyn AnomalyDetector>,
}

impl PluginRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            models: FactoryMap::new(
                RESERVED_MODEL_NAMES,
                "model",
                "chronix_plugin_models_registered_total",
                "chronix_plugin_model_creations_total",
            ),
            detectors: FactoryMap::new(
                RESERVED_DETECTOR_NAMES,
                "detector",
                "chronix_plugin_detectors_registered_total",
                "chronix_plugin_detector_creations_total",
            ),
        }
    }

    // ── Model factories ───────────────────────────────────────────

    /// Register a custom forecast model factory.
    ///
    /// Returns an error if the name is invalid or a model with the same name
    /// is already registered. Uses atomic entry insertion to prevent TOCTOU races.
    pub fn register_model(
        &self,
        name: impl Into<String>,
        factory: ModelFactory,
    ) -> Result<(), PluginError> {
        let name = name.into();
        let info = PluginInfo::new(&name);
        self.register_model_with_info(info, factory)
    }

    /// Register a custom forecast model factory with explicit metadata.
    pub fn register_model_with_info(
        &self,
        info: PluginInfo,
        factory: ModelFactory,
    ) -> Result<(), PluginError> {
        self.models.register(info, factory)
    }

    /// Atomically replace a model factory. Inserts if not present.
    ///
    /// Returns the previous `PluginInfo` if there was an existing registration.
    pub fn replace_model(
        &self,
        name: impl Into<String>,
        factory: ModelFactory,
    ) -> Result<Option<PluginInfo>, PluginError> {
        let name = name.into();
        let info = PluginInfo::new(&name);
        self.replace_model_with_info(info, factory)
    }

    /// Atomically replace a model factory with explicit metadata.
    pub fn replace_model_with_info(
        &self,
        info: PluginInfo,
        factory: ModelFactory,
    ) -> Result<Option<PluginInfo>, PluginError> {
        self.models.replace(info, factory)
    }

    /// Create a forecast model instance by plugin name.
    pub fn create_model(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Result<Box<dyn ForecastModel>, PluginError> {
        self.models.create(name, config)
    }

    /// Create a forecast model with default (null) configuration.
    pub fn create_model_default(&self, name: &str) -> Result<Box<dyn ForecastModel>, PluginError> {
        self.models.create(name, &serde_json::Value::Null)
    }

    /// Try to create a forecast model if the plugin is registered.
    ///
    /// Returns `None` if no plugin with the given name exists.
    /// Returns `Some(Err(...))` if the plugin exists but the factory fails.
    /// Single DashMap lookup — no TOCTOU.
    pub fn try_create_model(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Option<Result<Box<dyn ForecastModel>, PluginError>> {
        self.models.try_create(name, config)
    }

    /// Try to create a forecast model with default config (single lookup).
    pub fn try_create_model_default(
        &self,
        name: &str,
    ) -> Option<Result<Box<dyn ForecastModel>, PluginError>> {
        self.models.try_create(name, &serde_json::Value::Null)
    }

    /// Check if a model plugin is registered.
    pub fn has_model(&self, name: &str) -> bool {
        self.models.has(name)
    }

    /// Get metadata for a registered model plugin.
    pub fn model_info(&self, name: &str) -> Option<PluginInfo> {
        self.models.info(name)
    }

    /// List all registered model plugin names (sorted).
    pub fn list_models(&self) -> Vec<String> {
        self.models.list_names()
    }

    /// List all registered model plugins with their metadata (sorted by name).
    pub fn list_model_infos(&self) -> Vec<PluginInfo> {
        self.models.list_infos()
    }

    /// Unregister a model plugin. Returns the `PluginInfo` if it existed.
    pub fn unregister_model(&self, name: &str) -> Option<PluginInfo> {
        self.models.unregister(name)
    }

    /// Number of registered model plugins.
    pub fn model_count(&self) -> usize {
        self.models.len()
    }

    // ── Detector factories ────────────────────────────────────────

    /// Register a custom anomaly detector factory.
    ///
    /// Returns an error if the name is invalid or a detector with the same name
    /// is already registered. Uses atomic entry insertion to prevent TOCTOU races.
    pub fn register_detector(
        &self,
        name: impl Into<String>,
        factory: DetectorFactory,
    ) -> Result<(), PluginError> {
        let name = name.into();
        let info = PluginInfo::new(&name);
        self.register_detector_with_info(info, factory)
    }

    /// Register a custom anomaly detector factory with explicit metadata.
    pub fn register_detector_with_info(
        &self,
        info: PluginInfo,
        factory: DetectorFactory,
    ) -> Result<(), PluginError> {
        self.detectors.register(info, factory)
    }

    /// Atomically replace a detector factory. Inserts if not present.
    ///
    /// Returns the previous `PluginInfo` if there was an existing registration.
    pub fn replace_detector(
        &self,
        name: impl Into<String>,
        factory: DetectorFactory,
    ) -> Result<Option<PluginInfo>, PluginError> {
        let name = name.into();
        let info = PluginInfo::new(&name);
        self.replace_detector_with_info(info, factory)
    }

    /// Atomically replace a detector factory with explicit metadata.
    pub fn replace_detector_with_info(
        &self,
        info: PluginInfo,
        factory: DetectorFactory,
    ) -> Result<Option<PluginInfo>, PluginError> {
        self.detectors.replace(info, factory)
    }

    /// Create an anomaly detector instance by plugin name.
    pub fn create_detector(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Result<Box<dyn AnomalyDetector>, PluginError> {
        self.detectors.create(name, config)
    }

    /// Create an anomaly detector with default (null) configuration.
    pub fn create_detector_default(
        &self,
        name: &str,
    ) -> Result<Box<dyn AnomalyDetector>, PluginError> {
        self.detectors.create(name, &serde_json::Value::Null)
    }

    /// Try to create an anomaly detector if the plugin is registered.
    ///
    /// Returns `None` if no plugin with the given name exists.
    /// Returns `Some(Err(...))` if the plugin exists but the factory fails.
    /// Single DashMap lookup — no TOCTOU.
    pub fn try_create_detector(
        &self,
        name: &str,
        config: &serde_json::Value,
    ) -> Option<Result<Box<dyn AnomalyDetector>, PluginError>> {
        self.detectors.try_create(name, config)
    }

    /// Try to create an anomaly detector with default config (single lookup).
    pub fn try_create_detector_default(
        &self,
        name: &str,
    ) -> Option<Result<Box<dyn AnomalyDetector>, PluginError>> {
        self.detectors.try_create(name, &serde_json::Value::Null)
    }

    /// Check if a detector plugin is registered.
    pub fn has_detector(&self, name: &str) -> bool {
        self.detectors.has(name)
    }

    /// Get metadata for a registered detector plugin.
    pub fn detector_info(&self, name: &str) -> Option<PluginInfo> {
        self.detectors.info(name)
    }

    /// List all registered detector plugin names (sorted).
    pub fn list_detectors(&self) -> Vec<String> {
        self.detectors.list_names()
    }

    /// List all registered detector plugins with their metadata (sorted by name).
    pub fn list_detector_infos(&self) -> Vec<PluginInfo> {
        self.detectors.list_infos()
    }

    /// Unregister a detector plugin. Returns the `PluginInfo` if it existed.
    pub fn unregister_detector(&self, name: &str) -> Option<PluginInfo> {
        self.detectors.unregister(name)
    }

    /// Number of registered detector plugins.
    pub fn detector_count(&self) -> usize {
        self.detectors.len()
    }

    // ── Combined queries ──────────────────────────────────────────

    /// Total number of registered plugins (models + detectors).
    pub fn total_plugins(&self) -> usize {
        self.models.len() + self.detectors.len()
    }

    /// Remove all registered models and detectors.
    pub fn clear(&self) {
        self.models.clear();
        self.detectors.clear();
    }

    // ── Plugin allowlist ──────────────────────────────────

    /// Set the SHA-256 hash allowlist for model plugins.
    ///
    /// When the allowlist is non-empty, only plugins whose
    /// `PluginInfo::sha256_hash` is present in the list are accepted.
    /// Hashes should be lowercase hex-encoded.
    pub fn set_model_allowlist(&self, hashes: impl IntoIterator<Item = String>) {
        *self.models.hash_allowlist.write() =
            hashes.into_iter().map(|h| h.to_ascii_lowercase()).collect();
    }

    /// Set the SHA-256 hash allowlist for detector plugins.
    pub fn set_detector_allowlist(&self, hashes: impl IntoIterator<Item = String>) {
        *self.detectors.hash_allowlist.write() =
            hashes.into_iter().map(|h| h.to_ascii_lowercase()).collect();
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PluginRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginRegistry")
            .field("models", &self.list_models())
            .field("detectors", &self.list_detectors())
            .finish()
    }
}

// ─── Global singleton ───────────────────────────────────────────────

static GLOBAL_REGISTRY: OnceLock<Arc<PluginRegistry>> = OnceLock::new();

/// Returns the global plugin registry.
///
/// The registry is lazily initialised on first access.
pub fn global_registry() -> &'static Arc<PluginRegistry> {
    GLOBAL_REGISTRY.get_or_init(|| Arc::new(PluginRegistry::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anomaly::{AnomalyScore, DetectorType};
    use crate::forecast::{ForecastResult, ModelParams, ModelType};

    // ── helpers ──────────────────────────────────────────────────

    /// Trivial "constant" forecast model for testing.
    struct ConstantModel {
        value: f64,
        params: ModelParams,
    }

    impl ConstantModel {
        fn new(value: f64) -> Self {
            Self {
                value,
                params: ModelParams::Custom {
                    name: "constant".into(),
                    data: Vec::new(),
                },
            }
        }
    }

    impl ForecastModel for ConstantModel {
        fn fit(
            &mut self,
            _ts: &[i64],
            _vals: &[f64],
        ) -> Result<(), crate::forecast::ForecastError> {
            Ok(())
        }

        fn predict(
            &self,
            horizon: usize,
        ) -> Result<ForecastResult, crate::forecast::ForecastError> {
            Ok(ForecastResult {
                values: vec![self.value; horizon],
                timestamps: (1..=horizon as i64).collect(),
                confidence_lower: vec![self.value; horizon],
                confidence_upper: vec![self.value; horizon],
                confidence_level: 1.0,
            })
        }

        fn update(&mut self, _ts: i64, _val: f64) -> Result<(), crate::forecast::ForecastError> {
            Ok(())
        }

        fn model_type(&self) -> ModelType {
            ModelType::Custom("constant".into())
        }

        fn params(&self) -> &ModelParams {
            &self.params
        }
    }

    /// Trivial "always normal" anomaly detector for testing.
    struct AlwaysNormalDetector;

    impl crate::anomaly::AnomalyDetector for AlwaysNormalDetector {
        fn fit(&mut self, _ts: &[i64], _vals: &[f64]) -> Result<(), crate::anomaly::AnomalyError> {
            Ok(())
        }

        fn detect(
            &mut self,
            timestamps: &[i64],
            values: &[f64],
        ) -> Result<Vec<AnomalyScore>, crate::anomaly::AnomalyError> {
            Ok(timestamps
                .iter()
                .zip(values)
                .map(|(&t, &v)| AnomalyScore {
                    timestamp: t,
                    value: v,
                    score: 0.0,
                    is_anomaly: false,
                    method: DetectorType::Custom("always_normal".into()),
                    threshold: 0.0,
                    details: String::new(),
                })
                .collect())
        }

        fn detect_point(
            &mut self,
            timestamp: i64,
            value: f64,
        ) -> Result<AnomalyScore, crate::anomaly::AnomalyError> {
            Ok(AnomalyScore {
                timestamp,
                value,
                score: 0.0,
                is_anomaly: false,
                method: DetectorType::Custom("always_normal".into()),
                threshold: 0.0,
                details: String::new(),
            })
        }

        fn detector_type(&self) -> DetectorType {
            DetectorType::Custom("always_normal".into())
        }
    }

    fn constant_factory(value: f64) -> ModelFactory {
        Arc::new(move |_config| Ok(Box::new(ConstantModel::new(value))))
    }

    fn normal_detector_factory() -> DetectorFactory {
        Arc::new(|_config| Ok(Box::new(AlwaysNormalDetector)))
    }

    // ── model tests ──────────────────────────────────────────────

    #[test]
    fn register_and_create_model() {
        let reg = PluginRegistry::new();
        reg.register_model("constant", constant_factory(42.0))
            .unwrap();

        assert!(reg.has_model("constant"));
        assert!(!reg.has_model("nonexistent"));
        assert_eq!(reg.list_models(), vec!["constant"]);

        let mut model = reg.create_model_default("constant").unwrap();
        model.fit(&[1, 2, 3], &[1.0, 2.0, 3.0]).unwrap();
        let result = model.predict(3).unwrap();
        assert_eq!(result.values, vec![42.0, 42.0, 42.0]);
        assert_eq!(model.model_type(), ModelType::Custom("constant".into()));
    }

    #[test]
    fn register_model_with_metadata() {
        let reg = PluginRegistry::new();
        let info = PluginInfo::new("fancy_model")
            .with_description("A fancy forecasting model")
            .with_version("1.2.3");
        reg.register_model_with_info(info, constant_factory(1.0))
            .unwrap();

        let retrieved = reg.model_info("fancy_model").unwrap();
        assert_eq!(retrieved.name, "fancy_model");
        assert_eq!(retrieved.description, "A fancy forecasting model");
        assert_eq!(retrieved.version, "1.2.3");
    }

    #[test]
    fn duplicate_model_registration_fails() {
        let reg = PluginRegistry::new();
        reg.register_model("dup", constant_factory(1.0)).unwrap();
        let err = reg.register_model("dup", constant_factory(2.0));
        assert!(matches!(err, Err(PluginError::AlreadyRegistered(_))));
    }

    #[test]
    fn create_missing_model_fails() {
        let reg = PluginRegistry::new();
        let err = reg.create_model_default("nope");
        assert!(matches!(err, Err(PluginError::NotFound(_))));
    }

    #[test]
    fn unregister_model() {
        let reg = PluginRegistry::new();
        reg.register_model("tmp", constant_factory(1.0)).unwrap();
        assert!(reg.has_model("tmp"));

        let info = reg.unregister_model("tmp");
        assert!(info.is_some());
        assert_eq!(info.unwrap().name, "tmp");
        assert!(!reg.has_model("tmp"));
        assert!(reg.unregister_model("tmp").is_none());
    }

    #[test]
    fn replace_model_inserts_and_replaces() {
        let reg = PluginRegistry::new();

        // Insert (no previous)
        let prev = reg.replace_model("rp", constant_factory(1.0)).unwrap();
        assert!(prev.is_none());

        // Verify old factory produces value 1.0
        let model = reg.create_model_default("rp").unwrap();
        assert_eq!(model.predict(1).unwrap().values, vec![1.0]);

        // Replace with new factory
        let prev = reg.replace_model("rp", constant_factory(99.0)).unwrap();
        assert!(prev.is_some());

        // Verify new factory produces value 99.0
        let model = reg.create_model_default("rp").unwrap();
        assert_eq!(model.predict(1).unwrap().values, vec![99.0]);
    }

    #[test]
    fn try_create_model_single_lookup() {
        let reg = PluginRegistry::new();

        // Missing → None
        assert!(reg.try_create_model_default("nope").is_none());

        // Present → Some(Ok(...))
        reg.register_model("tc", constant_factory(7.0)).unwrap();
        let result = reg.try_create_model_default("tc");
        assert!(result.is_some());
        let model = result.unwrap().unwrap();
        assert_eq!(model.predict(1).unwrap().values, vec![7.0]);
    }

    // ── detector tests ───────────────────────────────────────────

    #[test]
    fn register_and_create_detector() {
        let reg = PluginRegistry::new();
        reg.register_detector("always_normal", normal_detector_factory())
            .unwrap();

        assert!(reg.has_detector("always_normal"));
        assert_eq!(reg.list_detectors(), vec!["always_normal"]);

        let mut det = reg.create_detector_default("always_normal").unwrap();
        det.fit(&[1, 2, 3], &[1.0, 2.0, 3.0]).unwrap();
        let scores = det.detect(&[4], &[100.0]).unwrap();
        assert_eq!(scores.len(), 1);
        assert!(!scores[0].is_anomaly);
        assert_eq!(
            det.detector_type(),
            DetectorType::Custom("always_normal".into())
        );
    }

    #[test]
    fn duplicate_detector_registration_fails() {
        let reg = PluginRegistry::new();
        reg.register_detector("dup", normal_detector_factory())
            .unwrap();
        let err = reg.register_detector("dup", normal_detector_factory());
        assert!(matches!(err, Err(PluginError::AlreadyRegistered(_))));
    }

    #[test]
    fn create_missing_detector_fails() {
        let reg = PluginRegistry::new();
        let err = reg.create_detector_default("nope");
        assert!(matches!(err, Err(PluginError::NotFound(_))));
    }

    #[test]
    fn unregister_detector() {
        let reg = PluginRegistry::new();
        reg.register_detector("tmp", normal_detector_factory())
            .unwrap();
        assert!(reg.has_detector("tmp"));

        let info = reg.unregister_detector("tmp");
        assert!(info.is_some());
        assert!(!reg.has_detector("tmp"));
        assert!(reg.unregister_detector("tmp").is_none());
    }

    #[test]
    fn try_create_detector_single_lookup() {
        let reg = PluginRegistry::new();

        assert!(reg.try_create_detector_default("nope").is_none());

        reg.register_detector("td", normal_detector_factory())
            .unwrap();
        let result = reg.try_create_detector_default("td");
        assert!(result.is_some());
        let mut det = result.unwrap().unwrap();
        let scores = det.detect(&[1], &[1.0]).unwrap();
        assert!(!scores[0].is_anomaly);
    }

    // ── name validation tests ────────────────────────────────────

    #[test]
    fn reject_empty_name() {
        let reg = PluginRegistry::new();
        let err = reg.register_model("", constant_factory(1.0));
        assert!(matches!(err, Err(PluginError::InvalidName(_))));

        let err = reg.register_model("   ", constant_factory(1.0));
        assert!(matches!(err, Err(PluginError::InvalidName(_))));
    }

    #[test]
    fn reject_whitespace_padded_name() {
        let reg = PluginRegistry::new();
        let err = reg.register_model(" leading", constant_factory(1.0));
        assert!(matches!(err, Err(PluginError::InvalidName(_))));

        let err = reg.register_detector("trailing ", normal_detector_factory());
        assert!(matches!(err, Err(PluginError::InvalidName(_))));
    }

    #[test]
    fn reject_reserved_model_names() {
        let reg = PluginRegistry::new();
        for name in ["ses", "SES", "holt", "Arima", "SARIMA", "linear_regression"] {
            let err = reg.register_model(name, constant_factory(1.0));
            assert!(
                matches!(err, Err(PluginError::InvalidName(_))),
                "should reject reserved model name: {name}"
            );
        }
    }

    #[test]
    fn reject_reserved_detector_names() {
        let reg = PluginRegistry::new();
        for name in ["zscore", "ZScore", "iqr", "cusum", "CUSUM"] {
            let err = reg.register_detector(name, normal_detector_factory());
            assert!(
                matches!(err, Err(PluginError::InvalidName(_))),
                "should reject reserved detector name: {name}"
            );
        }
    }

    // ── combined & misc tests ────────────────────────────────────

    #[test]
    fn total_plugins_and_clear() {
        let reg = PluginRegistry::new();
        assert_eq!(reg.total_plugins(), 0);

        reg.register_model("m1", constant_factory(1.0)).unwrap();
        reg.register_model("m2", constant_factory(2.0)).unwrap();
        reg.register_detector("d1", normal_detector_factory())
            .unwrap();
        assert_eq!(reg.total_plugins(), 3);

        reg.clear();
        assert_eq!(reg.total_plugins(), 0);
        assert!(reg.list_models().is_empty());
        assert!(reg.list_detectors().is_empty());
    }

    #[test]
    fn factory_error_propagated() {
        let reg = PluginRegistry::new();
        let factory: ModelFactory =
            Arc::new(|_| Err(PluginError::FactoryFailed("out of memory".into())));
        reg.register_model("broken", factory).unwrap();

        let err = reg.create_model_default("broken");
        assert!(matches!(err, Err(PluginError::FactoryFailed(_))));
    }

    #[test]
    fn model_factory_with_config() {
        let reg = PluginRegistry::new();
        let factory: ModelFactory = Arc::new(|config| {
            let value = config
                .get("value")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            Ok(Box::new(ConstantModel::new(value)))
        });
        reg.register_model("configurable", factory).unwrap();

        let config = serde_json::json!({"value": 99.0});
        let model = reg.create_model("configurable", &config).unwrap();
        let result = model.predict(1).unwrap();
        assert_eq!(result.values, vec![99.0]);
    }

    #[test]
    fn list_model_infos_sorted() {
        let reg = PluginRegistry::new();
        reg.register_model("zeta", constant_factory(1.0)).unwrap();
        reg.register_model("alpha", constant_factory(2.0)).unwrap();
        let infos = reg.list_model_infos();
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].name, "alpha");
        assert_eq!(infos[1].name, "zeta");
    }

    #[test]
    fn debug_impl() {
        let reg = PluginRegistry::new();
        reg.register_model("m1", constant_factory(1.0)).unwrap();
        let dbg = format!("{:?}", reg);
        assert!(dbg.contains("PluginRegistry"));
        assert!(dbg.contains("m1"));
    }

    #[test]
    fn global_registry_is_accessible() {
        let reg = global_registry();
        // Verify it's a valid registry that can be queried
        let models = reg.list_models();
        let detectors = reg.list_detectors();
        assert_eq!(reg.total_plugins(), models.len() + detectors.len());
    }

    #[test]
    fn concurrent_registration_is_safe() {
        use std::thread;

        let reg = Arc::new(PluginRegistry::new());
        let mut handles = Vec::new();

        for i in 0..8 {
            let reg = Arc::clone(&reg);
            handles.push(thread::spawn(move || {
                let name = format!("concurrent_model_{i}");
                reg.register_model(&name, constant_factory(i as f64))
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All 8 should succeed since names are unique
        assert!(results.iter().all(std::result::Result::is_ok));
        assert_eq!(reg.total_plugins(), 8);
    }

    #[test]
    fn concurrent_duplicate_registration_one_wins() {
        use std::sync::Barrier;
        use std::thread;

        let reg = Arc::new(PluginRegistry::new());
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let reg = Arc::clone(&reg);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                reg.register_model("race_target", constant_factory(1.0))
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let failures = results.iter().filter(|r| r.is_err()).count();

        // Exactly one thread wins the entry; the rest get AlreadyRegistered
        assert_eq!(successes, 1);
        assert_eq!(failures, 3);
        assert!(reg.has_model("race_target"));
    }

    // ── name charset & length tests ──────────────────────────────

    #[test]
    fn reject_name_starting_with_digit() {
        let reg = PluginRegistry::new();
        let err = reg.register_model("9lives", constant_factory(1.0));
        assert!(matches!(err, Err(PluginError::InvalidName(_))));
    }

    #[test]
    fn reject_name_with_special_chars() {
        let reg = PluginRegistry::new();
        for bad in ["foo/bar", "foo bar", "hello!", "a@b", "a.b", "a+b"] {
            let err = reg.register_model(bad, constant_factory(1.0));
            assert!(
                matches!(err, Err(PluginError::InvalidName(_))),
                "should reject name with special chars: {bad:?}"
            );
        }
    }

    #[test]
    fn accept_valid_name_chars() {
        let reg = PluginRegistry::new();
        for good in ["alpha", "my_model", "my-model", "Model123", "a"] {
            reg.register_model(good, constant_factory(1.0)).unwrap();
        }
    }

    #[test]
    fn reject_name_exceeding_max_length() {
        let reg = PluginRegistry::new();
        let long_name = format!("a{}", "b".repeat(MAX_NAME_LEN));
        assert!(long_name.len() > MAX_NAME_LEN);
        let err = reg.register_model(&long_name, constant_factory(1.0));
        assert!(matches!(err, Err(PluginError::InvalidName(_))));

        // Exactly MAX_NAME_LEN should be fine
        let exact = format!("a{}", "b".repeat(MAX_NAME_LEN - 1));
        assert_eq!(exact.len(), MAX_NAME_LEN);
        reg.register_model(&exact, constant_factory(1.0)).unwrap();
    }

    // ── replace_*_with_info tests ────────────────────────────────

    #[test]
    fn replace_model_with_info_preserves_metadata() {
        let reg = PluginRegistry::new();

        let info = PluginInfo::new("rp_info")
            .with_description("v1 model")
            .with_version("1.0.0");
        reg.register_model_with_info(info, constant_factory(1.0))
            .unwrap();

        let new_info = PluginInfo::new("rp_info")
            .with_description("v2 model — improved")
            .with_version("2.0.0");
        let prev = reg
            .replace_model_with_info(new_info, constant_factory(99.0))
            .unwrap();

        assert_eq!(prev.unwrap().version, "1.0.0");
        let current = reg.model_info("rp_info").unwrap();
        assert_eq!(current.description, "v2 model — improved");
        assert_eq!(current.version, "2.0.0");

        // Verify the new factory is active
        let model = reg.create_model_default("rp_info").unwrap();
        assert_eq!(model.predict(1).unwrap().values, vec![99.0]);
    }

    #[test]
    fn replace_detector_with_info_preserves_metadata() {
        let reg = PluginRegistry::new();

        let info = PluginInfo::new("rd_info")
            .with_description("v1 detector")
            .with_version("1.0.0");
        reg.register_detector_with_info(info, normal_detector_factory())
            .unwrap();

        let new_info = PluginInfo::new("rd_info")
            .with_description("v2 detector")
            .with_version("2.0.0");
        let prev = reg
            .replace_detector_with_info(new_info, normal_detector_factory())
            .unwrap();

        assert_eq!(prev.unwrap().version, "1.0.0");
        let current = reg.detector_info("rd_info").unwrap();
        assert_eq!(current.description, "v2 detector");
        assert_eq!(current.version, "2.0.0");
    }

    // ── count tests ─────────────────────────────────────────────

    #[test]
    fn model_and_detector_count() {
        let reg = PluginRegistry::new();
        assert_eq!(reg.model_count(), 0);
        assert_eq!(reg.detector_count(), 0);

        reg.register_model("m1", constant_factory(1.0)).unwrap();
        reg.register_model("m2", constant_factory(2.0)).unwrap();
        reg.register_detector("d1", normal_detector_factory())
            .unwrap();

        assert_eq!(reg.model_count(), 2);
        assert_eq!(reg.detector_count(), 1);
        assert_eq!(reg.total_plugins(), 3);

        reg.unregister_model("m1");
        assert_eq!(reg.model_count(), 1);
    }

    // ── error clone test ─────────────────────────────────────────

    #[test]
    fn plugin_error_is_cloneable() {
        let err = PluginError::NotFound("missing".into());
        let cloned = err.clone();
        assert_eq!(format!("{err}"), format!("{cloned}"));

        let err2 = PluginError::InvalidName("bad name".into());
        let cloned2 = err2.clone();
        assert_eq!(format!("{err2}"), format!("{cloned2}"));
    }

    // ── PluginInfo equality test ─────────────────────────────────

    #[test]
    fn plugin_info_equality() {
        let a = PluginInfo::new("test")
            .with_description("desc")
            .with_version("1.0.0");
        let b = PluginInfo::new("test")
            .with_description("desc")
            .with_version("1.0.0");
        let c = PluginInfo::new("test")
            .with_description("different")
            .with_version("1.0.0");

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ── PluginInfo serialization test ────────────────────────────

    #[test]
    fn plugin_info_serde_roundtrip() {
        let info = PluginInfo::new("serde_test")
            .with_description("A test plugin")
            .with_version("0.1.0");
        let json = serde_json::to_string(&info).unwrap();
        let deserialized: PluginInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, deserialized);
    }

    // ── guard-release test ───────────────────────────────────────

    #[test]
    fn factory_does_not_hold_shard_lock() {
        use std::thread;

        // Verify that factory execution doesn't block concurrent writes.
        let reg = Arc::new(PluginRegistry::new());
        let factory: ModelFactory = Arc::new(|_| {
            // Simulate a non-trivial factory
            std::thread::yield_now();
            Ok(Box::new(ConstantModel::new(1.0)))
        });
        reg.register_model("slow", factory).unwrap();

        // Spawn a thread that creates a model (invokes the factory)
        let reg2 = Arc::clone(&reg);
        let create_handle = thread::spawn(move || {
            reg2.create_model_default("slow").unwrap();
        });

        // Meanwhile, register another model concurrently
        // (would deadlock or block if the guard were held during factory exec)
        reg.register_model("other", constant_factory(2.0)).unwrap();

        create_handle.join().unwrap();
        assert!(reg.has_model("slow"));
        assert!(reg.has_model("other"));
    }

    // ── Plugin source verification ──────────────────────

    #[test]
    fn allowlist_empty_permits_all_plugins() {
        let reg = PluginRegistry::new();
        // No allowlist set → any plugin accepted
        reg.register_model("open_model", constant_factory(1.0))
            .unwrap();
        assert!(reg.has_model("open_model"));
    }

    #[test]
    fn allowlist_rejects_plugin_without_hash() {
        let reg = PluginRegistry::new();
        reg.set_model_allowlist(vec!["abcd1234".to_string()]);

        // PluginInfo has no sha256_hash → rejected
        let info = PluginInfo::new("unhashed");
        let result = reg.register_model_with_info(info, constant_factory(1.0));
        assert!(matches!(result, Err(PluginError::UntrustedPlugin(_))));
    }

    #[test]
    fn allowlist_rejects_plugin_with_wrong_hash() {
        let reg = PluginRegistry::new();
        reg.set_model_allowlist(vec!["aaaa".to_string()]);

        let info = PluginInfo::new("wronghash").with_sha256("bbbb");
        let result = reg.register_model_with_info(info, constant_factory(1.0));
        assert!(
            matches!(result, Err(PluginError::UntrustedPlugin(ref msg)) if msg.contains("not in allowlist"))
        );
    }

    #[test]
    fn allowlist_accepts_plugin_with_matching_hash() {
        let reg = PluginRegistry::new();
        let hash = "a1b2c3d4e5f6".to_string();
        reg.set_model_allowlist(vec![hash.clone()]);

        let info = PluginInfo::new("trusted").with_sha256(&hash);
        reg.register_model_with_info(info, constant_factory(1.0))
            .unwrap();
        assert!(reg.has_model("trusted"));
    }

    #[test]
    fn allowlist_case_insensitive() {
        let reg = PluginRegistry::new();
        reg.set_model_allowlist(vec!["ABCDEF".to_string()]);

        let info = PluginInfo::new("casetest").with_sha256("abcdef");
        reg.register_model_with_info(info, constant_factory(1.0))
            .unwrap();
        assert!(reg.has_model("casetest"));
    }
}

// ── Error type (moved from chronix-plugin) ──────────────────────────

/// Errors from the custom model/detector registry.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum PluginError {
    /// A plugin with the given name is already registered.
    #[error("plugin already registered: {0}")]
    AlreadyRegistered(String),

    /// No plugin with the given name.
    #[error("plugin not found: {0}")]
    NotFound(String),

    /// The factory function returned an error.
    #[error("plugin factory failed: {0}")]
    FactoryFailed(String),

    /// Invalid plugin configuration.
    #[error("invalid plugin config: {0}")]
    InvalidConfig(String),

    /// Invalid plugin name (empty, whitespace-only, or conflicts with built-in).
    #[error("invalid plugin name: {0}")]
    InvalidName(String),

    /// Plugin source hash is missing or not in the allowlist.
    #[error("untrusted plugin: {0}")]
    UntrustedPlugin(String),
}
