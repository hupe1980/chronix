//! Time-series cross-validation for forecast model evaluation.
//!
//! Standard k-fold cross-validation is inappropriate for time-series
//! data because it breaks temporal ordering.  This module implements
//! **expanding-window** (anchored) and **sliding-window** walk-forward
//! cross-validation, which respect the arrow of time.
//!
//! # Example
//!
//! ```no_run
//! use chronix_analytics::forecast::cross_validation::{CrossValidator, CrossValidationMode};
//! use chronix_analytics::forecast::{SesModel, ForecastModel};
//!
//! let timestamps: Vec<i64> = (0..120).map(|i| i * 1_000_000_000).collect();
//! let values: Vec<f64> = (0..120).map(|i| 10.0 + i as f64 * 0.5).collect();
//!
//! let cv = CrossValidator::new(
//!     CrossValidationMode::ExpandingWindow,
//!     30,   // minimum training size
//!     10,   // prediction horizon per fold
//!     5,    // number of folds
//! );
//!
//! let results = cv.evaluate(
//!     &timestamps,
//!     &values,
//!     || Box::new(SesModel::new(None)),
//! ).unwrap();
//!
//! assert_eq!(results.folds.len(), 5);
//! assert!(results.mean_rmse > 0.0);
//! ```

use crate::forecast::diagnostics;
use crate::forecast::error::ForecastError;
use crate::forecast::traits::ForecastModel;

// ── Configuration ──────────────────────────────────────────────────

/// Walk-forward cross-validation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossValidationMode {
    /// Expanding window — each fold's training set starts at index 0
    /// and grows by `step` observations.
    ExpandingWindow,
    /// Sliding window — each fold's training set is a fixed-size
    /// window that slides forward by `step` observations.
    SlidingWindow,
}

/// Cross-validation configuration.
#[derive(Debug, Clone)]
pub struct CrossValidator {
    /// Mode: expanding or sliding window.
    pub mode: CrossValidationMode,
    /// Minimum number of training observations.
    pub min_train_size: usize,
    /// Number of steps to predict (test window).
    pub horizon: usize,
    /// Number of folds.
    pub n_folds: usize,
}

impl CrossValidator {
    /// Create a new cross-validator.
    #[must_use]
    pub fn new(
        mode: CrossValidationMode,
        min_train_size: usize,
        horizon: usize,
        n_folds: usize,
    ) -> Self {
        Self {
            mode,
            min_train_size,
            horizon,
            n_folds,
        }
    }

    /// Evaluate a forecast model using walk-forward cross-validation.
    ///
    /// `model_factory` is called once per fold to create a fresh model
    /// instance — this avoids carry-over state between folds.
    ///
    /// # Errors
    ///
    /// Returns an error if the data is too short for the requested
    /// configuration or if any fold's fit/predict fails.
    pub fn evaluate<F>(
        &self,
        timestamps: &[i64],
        values: &[f64],
        model_factory: F,
    ) -> Result<CrossValidationResult, ForecastError>
    where
        F: Fn() -> Box<dyn ForecastModel>,
    {
        let n = values.len();
        if n != timestamps.len() {
            return Err(ForecastError::InvalidInput(
                "timestamps and values must have the same length".into(),
            ));
        }

        let required = self.min_train_size + self.horizon;
        if n < required {
            return Err(ForecastError::InvalidInput(format!(
                "need at least {} observations (min_train={} + horizon={}), got {}",
                required, self.min_train_size, self.horizon, n
            )));
        }

        // Calculate step size so that folds are evenly spaced.
        let available = n - self.min_train_size - self.horizon;
        let step = if self.n_folds <= 1 {
            0
        } else {
            available / (self.n_folds - 1).max(1)
        };

        let mut folds = Vec::with_capacity(self.n_folds);

        for fold_idx in 0..self.n_folds {
            let train_end = self.min_train_size + fold_idx * step;
            if train_end + self.horizon > n {
                break;
            }

            let train_start = match self.mode {
                CrossValidationMode::ExpandingWindow => 0,
                CrossValidationMode::SlidingWindow => train_end.saturating_sub(self.min_train_size),
            };

            let test_start = train_end;
            let test_end = (train_end + self.horizon).min(n);

            let train_ts = &timestamps[train_start..train_end];
            let train_vals = &values[train_start..train_end];
            let test_vals = &values[test_start..test_end];

            let mut model = model_factory();
            model.fit(train_ts, train_vals)?;

            let forecast = model.predict(test_end - test_start)?;
            let predicted = &forecast.values[..test_vals.len().min(forecast.values.len())];
            let actual = &test_vals[..predicted.len()];

            let rmse = diagnostics::rmse(actual, predicted).unwrap_or(f64::NAN);
            let mae = diagnostics::mae(actual, predicted).unwrap_or(f64::NAN);
            let mape = diagnostics::mape(actual, predicted);
            let smape = diagnostics::smape(actual, predicted);

            folds.push(FoldResult {
                fold_index: fold_idx,
                train_size: train_end - train_start,
                test_size: actual.len(),
                rmse,
                mae,
                mape,
                smape,
            });
        }

        if folds.is_empty() {
            return Err(ForecastError::InvalidInput(
                "no valid folds could be constructed".into(),
            ));
        }

        let mean_rmse = folds.iter().map(|f| f.rmse).sum::<f64>() / folds.len() as f64;
        let mean_mae = folds.iter().map(|f| f.mae).sum::<f64>() / folds.len() as f64;
        let mean_mape = {
            let vals: Vec<f64> = folds.iter().filter_map(|f| f.mape).collect();
            if vals.is_empty() {
                None
            } else {
                Some(vals.iter().sum::<f64>() / vals.len() as f64)
            }
        };
        let mean_smape = {
            let vals: Vec<f64> = folds.iter().filter_map(|f| f.smape).collect();
            if vals.is_empty() {
                None
            } else {
                Some(vals.iter().sum::<f64>() / vals.len() as f64)
            }
        };

        Ok(CrossValidationResult {
            folds,
            mean_rmse,
            mean_mae,
            mean_mape,
            mean_smape,
        })
    }
}

// ── Results ────────────────────────────────────────────────────────

/// Metrics from a single cross-validation fold.
#[derive(Debug, Clone)]
pub struct FoldResult {
    /// Zero-based fold index.
    pub fold_index: usize,
    /// Number of training observations for this fold.
    pub train_size: usize,
    /// Number of test observations for this fold.
    pub test_size: usize,
    /// RMSE for this fold.
    pub rmse: f64,
    /// MAE for this fold.
    pub mae: f64,
    /// MAPE for this fold (if computable).
    pub mape: Option<f64>,
    /// sMAPE for this fold (if computable).
    pub smape: Option<f64>,
}

/// Aggregate cross-validation results across all folds.
#[derive(Debug, Clone)]
pub struct CrossValidationResult {
    /// Per-fold results.
    pub folds: Vec<FoldResult>,
    /// Mean RMSE across folds.
    pub mean_rmse: f64,
    /// Mean MAE across folds.
    pub mean_mae: f64,
    /// Mean MAPE across folds (if any fold computed it).
    pub mean_mape: Option<f64>,
    /// Mean sMAPE across folds (if any fold computed it).
    pub mean_smape: Option<f64>,
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forecast::SesModel;

    fn make_linear_data(n: usize) -> (Vec<i64>, Vec<f64>) {
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..n).map(|i| 10.0 + i as f64 * 0.5).collect();
        (ts, vals)
    }

    #[test]
    fn expanding_window_produces_folds() {
        let (ts, vals) = make_linear_data(100);
        let cv = CrossValidator::new(CrossValidationMode::ExpandingWindow, 30, 10, 5);
        let result = cv
            .evaluate(&ts, &vals, || Box::new(SesModel::new(None)))
            .unwrap();

        assert_eq!(result.folds.len(), 5);
        assert!(result.mean_rmse > 0.0);
        assert!(result.mean_mae > 0.0);

        // Training sizes should be non-decreasing (expanding).
        for i in 1..result.folds.len() {
            assert!(result.folds[i].train_size >= result.folds[i - 1].train_size);
        }
    }

    #[test]
    fn sliding_window_constant_train_size() {
        let (ts, vals) = make_linear_data(100);
        let cv = CrossValidator::new(CrossValidationMode::SlidingWindow, 30, 10, 5);
        let result = cv
            .evaluate(&ts, &vals, || Box::new(SesModel::new(None)))
            .unwrap();

        assert_eq!(result.folds.len(), 5);
        for fold in &result.folds {
            assert_eq!(
                fold.train_size, 30,
                "sliding window should keep constant train size"
            );
        }
    }

    #[test]
    fn too_short_data_returns_error() {
        let (ts, vals) = make_linear_data(10);
        let cv = CrossValidator::new(CrossValidationMode::ExpandingWindow, 30, 10, 5);
        let err = cv.evaluate(&ts, &vals, || Box::new(SesModel::new(None)));
        assert!(err.is_err());
    }

    #[test]
    fn single_fold() {
        let (ts, vals) = make_linear_data(50);
        let cv = CrossValidator::new(CrossValidationMode::ExpandingWindow, 30, 10, 1);
        let result = cv
            .evaluate(&ts, &vals, || Box::new(SesModel::new(None)))
            .unwrap();
        assert_eq!(result.folds.len(), 1);
        assert_eq!(result.folds[0].train_size, 30);
    }

    #[test]
    fn metrics_are_finite() {
        let (ts, vals) = make_linear_data(120);
        let cv = CrossValidator::new(CrossValidationMode::ExpandingWindow, 30, 10, 5);
        let result = cv
            .evaluate(&ts, &vals, || Box::new(SesModel::new(None)))
            .unwrap();

        assert!(result.mean_rmse.is_finite());
        assert!(result.mean_mae.is_finite());
        if let Some(mape) = result.mean_mape {
            assert!(mape.is_finite());
        }
        if let Some(smape) = result.mean_smape {
            assert!(smape.is_finite());
        }
    }
}
