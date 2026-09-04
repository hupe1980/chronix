//! Parallel batch anomaly detection.
//!
//! Provides batch-fit and batch-detect operations across many series using
//! rayon per-series parallelism, with SIMD-accelerated inner statistics via
//! the [`CpuEngine`].
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────┐
//! │   batch_fit / batch_detect / batch_*   │
//! │   rayon per-series parallelism (outer) │
//! ├────────────────────────────────────────┤
//! │   CpuEngine (inner math)               │
//! │   batch_z_score for Z-Score detection  │
//! └────────────────────────────────────────┘
//! ```
//!
//! [`CpuEngine`]: crate::compute::CpuEngine

use std::sync::Arc;

use rayon::prelude::*;

use crate::compute::CpuEngine;

use crate::anomaly::error::AnomalyError;
use crate::anomaly::traits::{AnomalyDetector, AnomalyScore, DetectorType};

/// Fit multiple detectors on multiple series in parallel.
///
/// Each detector is fitted on its corresponding series. The number of
/// detectors must equal the number of series.
///
/// Returns one `Result` per series in the same order.
///
/// # Errors
///
/// Returns a single-element vec with `AnomalyError` if slice lengths differ.
pub fn batch_fit<D: AnomalyDetector + Send>(
    detectors: &mut [D],
    data: &[(&[i64], &[f64])],
) -> Vec<Result<(), AnomalyError>> {
    if detectors.len() != data.len() {
        return vec![Err(AnomalyError::InvalidInput(format!(
            "detectors.len() ({}) != data.len() ({})",
            detectors.len(),
            data.len()
        )))];
    }
    let _start = std::time::Instant::now();
    let results: Vec<_> = detectors
        .par_iter_mut()
        .zip(data.par_iter())
        .map(|(det, (ts, vals))| det.fit(ts, vals))
        .collect();
    metrics::histogram!("chronix_anomaly_batch_fit_duration_seconds")
        .record(_start.elapsed().as_secs_f64());
    results
}

/// Detect anomalies across multiple series in parallel using fitted detectors.
///
/// Each detector must already be fitted. Returns one `Vec<AnomalyScore>` per
/// series in the same order.
///
/// # Errors
///
/// Returns a single-element vec with `AnomalyError` if slice lengths differ.
pub fn batch_detect<D: AnomalyDetector + Send>(
    detectors: &mut [D],
    data: &[(&[i64], &[f64])],
) -> Vec<Result<Vec<AnomalyScore>, AnomalyError>> {
    if detectors.len() != data.len() {
        return vec![Err(AnomalyError::InvalidInput(format!(
            "detectors.len() ({}) != data.len() ({})",
            detectors.len(),
            data.len()
        )))];
    }
    let _start = std::time::Instant::now();
    let results: Vec<_> = detectors
        .par_iter_mut()
        .zip(data.par_iter())
        .map(|(det, (ts, vals))| det.detect(ts, vals))
        .collect();
    metrics::histogram!("chronix_anomaly_batch_detect_duration_seconds")
        .record(_start.elapsed().as_secs_f64());
    results
}

/// Fit detectors and detect anomalies in one pass across multiple series.
///
/// Creates one detector per series via `factory`, fits it, then detects.
/// Returns `AnomalyScore` vectors per series.
pub fn batch_fit_detect<D, F>(
    data: &[(&[i64], &[f64])],
    factory: F,
) -> Vec<Result<Vec<AnomalyScore>, AnomalyError>>
where
    D: AnomalyDetector + Send,
    F: Fn() -> D + Sync,
{
    let _start = std::time::Instant::now();
    let results: Vec<_> = data
        .par_iter()
        .map(|(ts, vals)| {
            let mut det = factory();
            det.fit(ts, vals)?;
            det.detect(ts, vals)
        })
        .collect();
    metrics::histogram!("chronix_anomaly_batch_fit_detect_duration_seconds")
        .record(_start.elapsed().as_secs_f64());
    results
}

/// Parallel batch Z-Score anomaly detection.
///
/// Uses the [`CpuEngine`]'s `batch_z_score` operation which dispatches to
/// GPU for large batches. This computes raw z-scores on the engine, then maps
/// them to `AnomalyScore` with sigmoid normalization.
///
/// Each series is processed independently via rayon parallelism, with the
/// z-score computation itself GPU-accelerated for large series.
///
/// # GPU Fallback Behaviour
///
/// This function **always uses Z-Score** regardless of the caller's
/// preferred detector type. Other detectors (Modified Z-Score, IQR,
/// Moving Average Residual, Dynamic Threshold) go through the generic
/// per-detector path, so this function is a Z-Score-only fast path. For other detector types, use [`batch_detect`] (CPU/rayon)
/// or [`batch_detect_iqr`] (parallel IQR).
///
/// # Arguments
///
/// * `engine` — shared CPU compute engine
/// * `data` — slice of `(timestamps, values)` per series
/// * `threshold` — z-score threshold for anomaly flagging (default: 3.0 if `None`)
///
/// # Example
///
/// ```rust,ignore
/// use chronix_analytics::compute::CpuEngine;
/// use chronix_analytics::anomaly::batch::batch_detect_zscore;
/// use std::sync::Arc;
///
/// let engine = Arc::new(CpuEngine::new(Default::default()).unwrap());
/// let data = vec![(&ts[..], &vals[..])];
/// let results = batch_detect_zscore(&engine, &data, None);
/// ```
pub fn batch_detect_zscore(
    engine: &Arc<CpuEngine>,
    data: &[(&[i64], &[f64])],
    threshold: Option<f64>,
) -> Vec<Result<Vec<AnomalyScore>, AnomalyError>> {
    let threshold = threshold.unwrap_or(3.0);
    let _start = std::time::Instant::now();

    let results: Vec<_> = data
        .par_iter()
        .map(|(ts, vals)| detect_zscore_engine(engine, ts, vals, threshold))
        .collect();

    metrics::histogram!("chronix_anomaly_batch_detect_zscore_duration_seconds")
        .record(_start.elapsed().as_secs_f64());
    metrics::counter!("chronix_anomaly_batch_detect_zscore_series_total")
        .increment(data.len() as u64);
    results
}

/// Parallel batch IQR anomaly detection.
///
/// Uses rayon parallelism with sorted-based IQR computation per series;
/// the parallel outer loop provides significant speedup for many-series
/// workloads.
///
/// # Arguments
///
/// * `data` — slice of `(timestamps, values)` per series
/// * `k` — IQR multiplier for fence computation (default: 1.5 if `None`)
pub fn batch_detect_iqr(
    data: &[(&[i64], &[f64])],
    k: Option<f64>,
) -> Vec<Result<Vec<AnomalyScore>, AnomalyError>> {
    let k = k.unwrap_or(1.5);
    let _start = std::time::Instant::now();

    let results: Vec<_> = data
        .par_iter()
        .map(|(ts, vals)| detect_iqr_batch(ts, vals, k))
        .collect();

    metrics::histogram!("chronix_anomaly_batch_detect_iqr_duration_seconds")
        .record(_start.elapsed().as_secs_f64());
    results
}

/// Detect anomalies across many series using their **fitted** detectors.
///
/// # Why there is no fast path
///
/// [`batch_detect_zscore`] and [`batch_detect_iqr`] are not faster
/// implementations of this — they compute a *different* statistic. Both
/// re-derive the baseline from the series they are scoring and take the
/// threshold from an argument, so a detector fitted on a healthy window and
/// pointed at a series that has since shifted by 1000σ flags **nothing**:
/// relative to itself, the shifted series is ordinary. Dispatching to them
/// on [`AnomalyDetector::detector_type`] silently discards both the fitted
/// state and the configured threshold.
///
/// Detecting against a stored baseline and detecting against the batch's own
/// distribution are two different jobs. This function does the first (and is
/// exactly [`batch_detect`]); [`batch_detect_zscore`] and
/// [`batch_detect_iqr`] do the second and say so.
pub fn batch_detect_auto<D: AnomalyDetector + Send>(
    detectors: &mut [D],
    data: &[(&[i64], &[f64])],
) -> Vec<Result<Vec<AnomalyScore>, AnomalyError>> {
    batch_detect(detectors, data)
}

/// Compute z-score anomaly detection for a single series using the compute engine.
fn detect_zscore_engine(
    engine: &Arc<CpuEngine>,
    timestamps: &[i64],
    values: &[f64],
    threshold: f64,
) -> Result<Vec<AnomalyScore>, AnomalyError> {
    if values.len() < 2 {
        return Err(AnomalyError::InsufficientData {
            min: 2,
            got: values.len(),
        });
    }

    // Dispatch z-score computation to the SIMD compute engine.
    let z_scores = engine
        .batch_z_score(values)
        .map_err(|e| AnomalyError::InvalidInput(format!("compute engine error: {e}")))?;

    let mut scores = Vec::with_capacity(values.len());
    for (i, (&z, &v)) in z_scores.iter().zip(values.iter()).enumerate() {
        let abs_z = z.abs();
        let is_anomaly = abs_z > threshold;
        scores.push(AnomalyScore {
            timestamp: timestamps[i],
            value: v,
            score: normalize_sigmoid(abs_z, threshold),
            is_anomaly,
            method: DetectorType::ZScore,
            threshold,
            details: format!("z={abs_z:.4}"),
        });
    }

    Ok(scores)
}

/// Compute IQR anomaly detection for a single series, self-calibrated.
///
/// Delegates to [`IqrDetector`] rather than reimplementing the fences: the
/// second copy scored constant data by absolute distance from the median
/// (`inf` for anything off-median) while the detector scored it against a
/// floored IQR, so the same input got two different answers from two
/// functions documented as doing the same thing.
fn detect_iqr_batch(
    timestamps: &[i64],
    values: &[f64],
    k: f64,
) -> Result<Vec<AnomalyScore>, AnomalyError> {
    let mut det = crate::anomaly::iqr::IqrDetector::new(Some(k));
    det.fit(timestamps, values)?;
    det.detect(timestamps, values)
}

/// Sigmoid normalization: maps `threshold → ~0.5`, well-above → ~1.0.
#[inline]
fn normalize_sigmoid(raw: f64, threshold: f64) -> f64 {
    let x = (raw - threshold) * 2.0;
    1.0 / (1.0 + (-x).exp())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anomaly::iqr::IqrDetector;
    use crate::anomaly::zscore::ZScoreDetector;
    use crate::compute::ComputeConfig;

    fn make_engine() -> Arc<CpuEngine> {
        Arc::new(CpuEngine::new(ComputeConfig::default()).unwrap())
    }

    fn make_normal_series(n: usize) -> (Vec<i64>, Vec<f64>) {
        let ts: Vec<i64> = (0..n as i64).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..n)
            .map(|i| ((i as f64 * 0.1).sin() * 5.0) + 50.0)
            .collect();
        (ts, vals)
    }

    fn make_series_with_outliers(
        n: usize,
        outlier_indices: &[usize],
        outlier_val: f64,
    ) -> (Vec<i64>, Vec<f64>) {
        let mut s = make_normal_series(n);
        for &idx in outlier_indices {
            if idx < n {
                s.1[idx] = outlier_val;
            }
        }
        s
    }

    // ── batch_fit ────────────────────────────────────────────

    #[test]
    fn batch_fit_all_succeed() {
        let s1 = make_normal_series(100);
        let s2 = make_normal_series(100);
        let data: Vec<(&[i64], &[f64])> = vec![(&s1.0, &s1.1), (&s2.0, &s2.1)];
        let mut dets = vec![ZScoreDetector::new(None), ZScoreDetector::new(None)];
        let results = batch_fit(&mut dets, &data);
        assert!(results.iter().all(Result::is_ok));
    }

    #[test]
    fn batch_fit_partial_failure() {
        let good = make_normal_series(100);
        let bad_ts: Vec<i64> = vec![0, 1];
        let bad_vals: Vec<f64> = vec![1.0, 2.0];
        let data: Vec<(&[i64], &[f64])> = vec![(&good.0, &good.1), (&bad_ts, &bad_vals)];
        let mut dets = vec![ZScoreDetector::new(None), ZScoreDetector::new(None)];
        let results = batch_fit(&mut dets, &data);
        assert!(results[0].is_ok());
        assert!(results[1].is_err()); // needs 3 points
    }

    // ── batch_detect ─────────────────────────────────────────

    #[test]
    fn batch_detect_multiple_series() {
        let s1 = make_series_with_outliers(200, &[50], 500.0);
        let s2 = make_series_with_outliers(200, &[150], 500.0);
        let data: Vec<(&[i64], &[f64])> = vec![(&s1.0, &s1.1), (&s2.0, &s2.1)];
        let mut dets = vec![
            ZScoreDetector::new(Some(3.0)),
            ZScoreDetector::new(Some(3.0)),
        ];
        batch_fit(&mut dets, &data);
        let results = batch_detect(&mut dets, &data);
        assert_eq!(results.len(), 2);
        assert!(results[0].as_ref().unwrap()[50].is_anomaly);
        assert!(results[1].as_ref().unwrap()[150].is_anomaly);
    }

    // ── batch_fit_detect ─────────────────────────────────────

    #[test]
    fn batch_fit_detect_convenience() {
        let s1 = make_series_with_outliers(200, &[50], 500.0);
        let s2 = make_normal_series(200);
        let data: Vec<(&[i64], &[f64])> = vec![(&s1.0, &s1.1), (&s2.0, &s2.1)];
        let results = batch_fit_detect(&data, || ZScoreDetector::new(None));
        assert_eq!(results.len(), 2);
        assert!(results[0].as_ref().unwrap()[50].is_anomaly);
        // Normal series should have minimal anomalies
        let anomaly_count = results[1]
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| s.is_anomaly)
            .count();
        assert!(anomaly_count < 5);
    }

    // ── batch_detect_zscore ─────────────────────────────────────

    #[test]
    fn batch_detect_zscore_basic() {
        let engine = make_engine();
        let s = make_series_with_outliers(200, &[50, 150], 500.0);
        let data: Vec<(&[i64], &[f64])> = vec![(&s.0, &s.1)];
        let results = batch_detect_zscore(&engine, &data, Some(3.0));
        assert_eq!(results.len(), 1);
        let scores = results[0].as_ref().unwrap();
        assert!(scores[50].is_anomaly);
        assert!(scores[150].is_anomaly);
    }

    #[test]
    fn batch_detect_zscore_multiple_series() {
        let engine = make_engine();
        let s1 = make_series_with_outliers(100, &[10], 1000.0);
        let s2 = make_normal_series(100);
        let s3 = make_series_with_outliers(100, &[90], -1000.0);
        let data: Vec<(&[i64], &[f64])> = vec![(&s1.0, &s1.1), (&s2.0, &s2.1), (&s3.0, &s3.1)];
        let results = batch_detect_zscore(&engine, &data, None);
        assert_eq!(results.len(), 3);
        assert!(results[0].as_ref().unwrap()[10].is_anomaly);
        // Normal series
        let normal_anomalies = results[1]
            .as_ref()
            .unwrap()
            .iter()
            .filter(|s| s.is_anomaly)
            .count();
        assert!(normal_anomalies < 5);
        assert!(results[2].as_ref().unwrap()[90].is_anomaly);
    }

    #[test]
    fn batch_detect_zscore_matches_cpu_zscore() {
        let engine = make_engine();
        let s = make_series_with_outliers(200, &[50], 500.0);
        let threshold = 3.0;

        // GPU path
        let gpu_results = batch_detect_zscore(&engine, &[(&s.0, &s.1)], Some(threshold));
        let gpu_scores = gpu_results[0].as_ref().unwrap();

        // CPU path (via ZScoreDetector)
        let mut det = ZScoreDetector::new(Some(threshold));
        det.fit(&s.0, &s.1).unwrap();
        let cpu_scores = det.detect(&s.0, &s.1).unwrap();

        // Anomaly flags should match
        for (i, (g, c)) in gpu_scores.iter().zip(cpu_scores.iter()).enumerate() {
            assert_eq!(
                g.is_anomaly, c.is_anomaly,
                "anomaly flag mismatch at index {i}: gpu={} cpu={}",
                g.is_anomaly, c.is_anomaly
            );
        }
    }

    #[test]
    fn batch_detect_zscore_insufficient_data() {
        let engine = make_engine();
        let data: Vec<(&[i64], &[f64])> = vec![(&[0], &[1.0])];
        let results = batch_detect_zscore(&engine, &data, None);
        assert!(results[0].is_err());
    }

    #[test]
    fn batch_detect_zscore_constant_series() {
        let engine = make_engine();
        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals = vec![42.0; 50];
        let data: Vec<(&[i64], &[f64])> = vec![(&ts, &vals)];
        let results = batch_detect_zscore(&engine, &data, None);
        let scores = results[0].as_ref().unwrap();
        // All z-scores should be 0 for constant series → no anomalies
        for s in scores {
            assert!(!s.is_anomaly);
        }
    }

    // ── batch_detect_iqr ─────────────────────────────────────

    #[test]
    fn batch_detect_iqr_basic() {
        let s = make_series_with_outliers(100, &[10], 5000.0);
        let data: Vec<(&[i64], &[f64])> = vec![(&s.0, &s.1)];
        let results = batch_detect_iqr(&data, None);
        assert_eq!(results.len(), 1);
        assert!(results[0].as_ref().unwrap()[10].is_anomaly);
    }

    #[test]
    fn batch_detect_iqr_multiple() {
        let s1 = make_series_with_outliers(100, &[50], 5000.0);
        let s2 = make_normal_series(100);
        let data: Vec<(&[i64], &[f64])> = vec![(&s1.0, &s1.1), (&s2.0, &s2.1)];
        let results = batch_detect_iqr(&data, Some(1.5));
        assert!(results[0].as_ref().unwrap()[50].is_anomaly);
    }

    #[test]
    fn batch_detect_iqr_matches_detector() {
        let s = make_series_with_outliers(200, &[100], 5000.0);
        let k = 1.5;

        // Batch path
        let batch_results = batch_detect_iqr(&[(&s.0, &s.1)], Some(k));
        let batch_scores = batch_results[0].as_ref().unwrap();

        // IqrDetector path
        let mut det = IqrDetector::new(Some(k));
        det.fit(&s.0, &s.1).unwrap();
        let det_scores = det.detect(&s.0, &s.1).unwrap();

        for (i, (b, d)) in batch_scores.iter().zip(det_scores.iter()).enumerate() {
            assert_eq!(
                b.is_anomaly, d.is_anomaly,
                "IQR anomaly flag mismatch at index {i}"
            );
        }
    }

    #[test]
    fn batch_detect_iqr_insufficient_data() {
        let data: Vec<(&[i64], &[f64])> = vec![(&[0, 1, 2], &[1.0, 2.0, 3.0])];
        let results = batch_detect_iqr(&data, None);
        assert!(results[0].is_err());
    }

    // ── Metrics emitted ──────────────────────────────────────

    #[test]
    fn metrics_no_panic() {
        let engine = make_engine();
        let s = make_normal_series(50);
        let data: Vec<(&[i64], &[f64])> = vec![(&s.0, &s.1)];
        let _ = batch_detect_zscore(&engine, &data, None);
        let _ = batch_detect_iqr(&data, None);
        let _ = batch_fit_detect(&data, || ZScoreDetector::new(None));
    }
}
