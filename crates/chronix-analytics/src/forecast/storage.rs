//! Model storage: serialization, metadata, and in-memory catalog.
//!
//! `ModelStore` enables round-trip serialization of fitted models via
//! `serde` + `postcard`. `ModelCatalog` provides an in-memory registry
//! keyed by `(measurement, model_name)`.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::forecast::result::ModelType;

// ─── ModelStore trait ───────────────────────────────────────────────

/// Trait for serializing and deserializing fitted models.
///
/// Implementors must be `Serialize + Deserialize`. The default
/// implementation uses `postcard` for compact binary encoding.
pub trait ModelStore: Serialize + for<'de> Deserialize<'de> {
    /// Serializes this model to a byte vector.
    fn to_bytes(&self) -> Result<Vec<u8>, StorageError> {
        postcard::to_stdvec(self).map_err(|e| StorageError::Serialize(e.to_string()))
    }

    /// Deserializes a model from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, StorageError>
    where
        Self: Sized,
    {
        postcard::from_bytes(bytes).map_err(|e| StorageError::Deserialize(e.to_string()))
    }
}

// ─── StorageError ───────────────────────────────────────────────────

/// Errors from model storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The model could not be serialized to bytes.
    #[error("serialization failed: {0}")]
    Serialize(String),
    /// The byte payload could not be deserialized into a model.
    #[error("deserialization failed: {0}")]
    Deserialize(String),
    /// No model with the given key exists in the store.
    #[error("model not found: {0}")]
    NotFound(String),
}

// ─── ModelMetadata ──────────────────────────────────────────────────

/// Metadata describing a stored model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    /// Type of the forecast model (SES, Holt, ARIMA, …).
    pub model_type: ModelType,
    /// User-supplied name for this model instance.
    pub model_name: String,
    /// Measurement (metric) on which the model was trained.
    pub measurement: String,
    /// Monotonically increasing version counter.
    pub version: u32,
    /// UNIX timestamp (seconds) when the model was stored.
    pub created_at: i64,
    /// Start of the training data range (nanoseconds).
    pub training_data_start: i64,
    /// End of the training data range (nanoseconds).
    pub training_data_end: i64,
    /// Number of data points used for training.
    pub training_points: usize,
    /// Mean Squared Error on training data.
    pub mse: f64,
    /// Mean Absolute Error on training data.
    pub mae: f64,
    /// Coefficient of determination R² on training data.
    pub r_squared: f64,
}

// ─── CatalogEntry ───────────────────────────────────────────────────

/// A single entry in the catalog: metadata + serialized model bytes.
#[derive(Debug, Clone)]
struct CatalogEntry {
    metadata: ModelMetadata,
    bytes: Vec<u8>,
}

// ─── ModelCatalog ───────────────────────────────────────────────────

/// In-memory model catalog keyed by `(measurement, model_name)`.
///
/// # Thread Safety
///
/// `ModelCatalog` is not internally synchronised. The server wraps it
/// in `Arc<RwLock<ModelCatalog>>` (see `AppState::model_catalog`),
/// which provides correct concurrent access. This is intentional:
/// embedding a lock would impose overhead on single-threaded forecast
/// pipelines and prevents read-side parallelism that `RwLock` provides.
#[derive(Debug, Default)]
pub struct ModelCatalog {
    /// `(measurement, model_name)` → `CatalogEntry`
    entries: HashMap<(String, String), CatalogEntry>,
}

impl ModelCatalog {
    /// Creates an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Saves a model to the catalog.
    ///
    /// If a model with the same `(measurement, name)` already exists
    /// the version is auto-incremented.
    #[allow(clippy::too_many_arguments)]
    pub fn save_model<M: ModelStore>(
        &mut self,
        measurement: &str,
        name: &str,
        model: &M,
        model_type: ModelType,
        training_range: (i64, i64),
        training_points: usize,
        mse: f64,
        mae: f64,
        r_squared: f64,
    ) -> Result<ModelMetadata, StorageError> {
        let bytes = model.to_bytes()?;
        let key = (measurement.to_string(), name.to_string());

        let version = self.entries.get(&key).map_or(1, |e| e.metadata.version + 1);

        let now_ns = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);

        let metadata = ModelMetadata {
            model_type,
            model_name: name.to_string(),
            measurement: measurement.to_string(),
            version,
            created_at: now_ns,
            training_data_start: training_range.0,
            training_data_end: training_range.1,
            training_points,
            mse,
            mae,
            r_squared,
        };

        self.entries.insert(
            key,
            CatalogEntry {
                metadata: metadata.clone(),
                bytes,
            },
        );

        Ok(metadata)
    }

    /// Loads a model from the catalog, deserializing it.
    pub fn load_model<M: ModelStore>(
        &self,
        measurement: &str,
        name: &str,
    ) -> Result<M, StorageError> {
        let key = (measurement.to_string(), name.to_string());
        let entry = self
            .entries
            .get(&key)
            .ok_or_else(|| StorageError::NotFound(format!("{measurement}/{name}")))?;
        M::from_bytes(&entry.bytes)
    }

    /// Returns metadata for a stored model (if it exists).
    pub fn get_metadata(&self, measurement: &str, name: &str) -> Option<&ModelMetadata> {
        let key = (measurement.to_string(), name.to_string());
        self.entries.get(&key).map(|e| &e.metadata)
    }

    /// Lists metadata for all models under a given measurement.
    pub fn list_models(&self, measurement: &str) -> Vec<&ModelMetadata> {
        self.entries
            .values()
            .filter(|e| e.metadata.measurement == measurement)
            .map(|e| &e.metadata)
            .collect()
    }

    /// Lists metadata for all models in the catalog.
    pub fn list_all(&self) -> Vec<&ModelMetadata> {
        self.entries.values().map(|e| &e.metadata).collect()
    }

    /// Deletes a model from the catalog.
    pub fn delete_model(&mut self, measurement: &str, name: &str) -> bool {
        let key = (measurement.to_string(), name.to_string());
        self.entries.remove(&key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forecast::{ForecastModel, SesModel};

    #[test]
    fn ses_round_trip() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| 50.0 + i as f64 * 0.1).collect();
        let mut model = SesModel::new(Some(0.3));
        model.fit(&ts, &vals).unwrap();

        let orig_pred = model.predict(10).unwrap();
        let bytes = model.to_bytes().unwrap();
        let restored = SesModel::from_bytes(&bytes).unwrap();
        let rest_pred = restored.predict(10).unwrap();

        assert_eq!(orig_pred.values.len(), rest_pred.values.len());
        for (a, b) in orig_pred.values.iter().zip(rest_pred.values.iter()) {
            assert!((a - b).abs() < 1e-10, "values diverged: {a} vs {b}");
        }
    }

    #[test]
    fn catalog_save_load_delete() {
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = vec![42.0; 50];
        let mut model = SesModel::new(Some(0.5));
        model.fit(&ts, &vals).unwrap();

        let mut catalog = ModelCatalog::new();
        let meta = catalog
            .save_model(
                "cpu",
                "ses_v1",
                &model,
                ModelType::Ses,
                (ts[0], *ts.last().unwrap()),
                vals.len(),
                0.0,
                0.0,
                1.0,
            )
            .unwrap();
        assert_eq!(meta.version, 1);

        // List
        let listed = catalog.list_models("cpu");
        assert_eq!(listed.len(), 1);

        // Load
        let loaded: SesModel = catalog.load_model("cpu", "ses_v1").unwrap();
        let pred = loaded.predict(5).unwrap();
        assert_eq!(pred.values.len(), 5);

        // Version bump
        let meta2 = catalog
            .save_model(
                "cpu",
                "ses_v1",
                &model,
                ModelType::Ses,
                (0, 0),
                0,
                0.0,
                0.0,
                0.0,
            )
            .unwrap();
        assert_eq!(meta2.version, 2);

        // Delete
        assert!(catalog.delete_model("cpu", "ses_v1"));
        assert!(catalog.load_model::<SesModel>("cpu", "ses_v1").is_err());
    }

    #[test]
    fn not_found() {
        let catalog = ModelCatalog::new();
        let result = catalog.load_model::<SesModel>("cpu", "missing");
        assert!(result.is_err());
    }

    #[test]
    fn list_all_models() {
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = vec![42.0; 50];
        let mut model = SesModel::new(Some(0.5));
        model.fit(&ts, &vals).unwrap();

        let mut catalog = ModelCatalog::new();
        catalog
            .save_model(
                "cpu",
                "m1",
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
                "m2",
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
                "cpu",
                "m3",
                &model,
                ModelType::Ses,
                (0, 49),
                50,
                0.0,
                0.0,
                1.0,
            )
            .unwrap();

        // list_all returns entries from all measurements.
        let all = catalog.list_all();
        assert_eq!(all.len(), 3);

        // list_models filters by measurement.
        assert_eq!(catalog.list_models("cpu").len(), 2);
        assert_eq!(catalog.list_models("mem").len(), 1);
        assert_eq!(catalog.list_models("disk").len(), 0);
    }
}
