//! Anomaly detector serialization support.
//!
//! Re-uses the same `postcard`-based approach as the forecast `ModelStore`.

use serde::{Deserialize, Serialize};

/// Trait for serializing and deserializing fitted detectors.
pub trait DetectorStore: Serialize + for<'de> Deserialize<'de> {
    /// Serializes this detector to a byte vector.
    fn to_bytes(&self) -> Result<Vec<u8>, DetectorStorageError> {
        postcard::to_stdvec(self).map_err(|e| DetectorStorageError::Serialize(e.to_string()))
    }

    /// Deserializes a detector from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, DetectorStorageError>
    where
        Self: Sized,
    {
        postcard::from_bytes(bytes).map_err(|e| DetectorStorageError::Deserialize(e.to_string()))
    }
}

/// Errors from detector storage operations.
#[derive(Debug, thiserror::Error)]
pub enum DetectorStorageError {
    #[error("serialization failed: {0}")]
    Serialize(String),
    #[error("deserialization failed: {0}")]
    Deserialize(String),
}

// ─── ModelStore impls ───────────────────────────────────────────────

impl DetectorStore for crate::anomaly::ZScoreDetector {}
impl DetectorStore for crate::anomaly::ModifiedZScoreDetector {}
impl DetectorStore for crate::anomaly::IqrDetector {}
impl DetectorStore for crate::anomaly::MovingAverageResidualDetector {}
impl DetectorStore for crate::anomaly::DynamicThresholdDetector {}
// ForecastResidualDetector: serialize stats only, call refit() after deserialize
impl DetectorStore for crate::anomaly::ForecastResidualDetector {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anomaly::{AnomalyDetector, ZScoreDetector};

    #[test]
    fn zscore_round_trip() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| 50.0 + (i as f64 * 0.1).sin()).collect();

        let mut det = ZScoreDetector::new(Some(3.0));
        det.fit(&ts, &vals).unwrap();

        let bytes = det.to_bytes().unwrap();
        let mut restored = ZScoreDetector::from_bytes(&bytes).unwrap();

        // Verify same detection results
        let orig = det.detect_point(999, 500.0).unwrap();
        let rest = restored.detect_point(999, 500.0).unwrap();
        assert_eq!(orig.is_anomaly, rest.is_anomaly);
        assert!((orig.score - rest.score).abs() < 1e-10);
    }

    #[test]
    fn iqr_round_trip() {
        let ts: Vec<i64> = (0..100).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();

        let mut det = crate::anomaly::IqrDetector::new(Some(1.5));
        det.fit(&ts, &vals).unwrap();

        let bytes = det.to_bytes().unwrap();
        let mut restored = crate::anomaly::IqrDetector::from_bytes(&bytes).unwrap();

        let orig = det.detect_point(999, 500.0).unwrap();
        let rest = restored.detect_point(999, 500.0).unwrap();
        assert_eq!(orig.is_anomaly, rest.is_anomaly);
    }
}
