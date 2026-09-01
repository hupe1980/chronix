//! # chronix-anomaly
//!
//! Anomaly detection for the Chronix time-series database.
//!
//! ## Detectors
//!
//! | Detector | Type | Assumptions |
//! |----------|------|-------------|
//! | [`ZScoreDetector`] | Statistical | Approximate normality |
//! | [`ModifiedZScoreDetector`] | Robust statistical | Symmetric distribution |
//! | [`IqrDetector`] | Non-parametric | None |
//! | [`ForecastResidualDetector`] | Model-based | Adequate forecast model |
//! | [`MovingAverageResidualDetector`] | Model-based | Local stationarity |
//! | [`DynamicThresholdDetector`] | Adaptive | Evolving baseline |
//!
//! All detectors implement [`AnomalyDetector`] and produce [`AnomalyScore`]
//! values in `[0, 1]` for uniform downstream consumption.
//!
//! ## Batch Operations
//!
//! The [`batch`] module provides parallel batch detection:
//!
//! - [`batch::batch_detect_zscore`] — SIMD-accelerated Z-Score across many series
//! - [`batch::batch_detect_iqr`] — parallel IQR detection
//! - [`batch::batch_fit_detect`] — fit-and-detect in one pass
//!
//! ## Persistence
//!
//! [`DetectorStore`] serialises fitted detector state for checkpointing
//! and recovery.

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod batch;
mod cusum;
mod dynamic_threshold;
mod error;
mod forecast_residual;
mod iqr;
mod modified_zscore;
mod moving_average;
mod storage;
mod traits;
mod zscore;

pub use batch::{batch_detect, batch_detect_iqr, batch_detect_zscore, batch_fit, batch_fit_detect};
pub use cusum::CusumDetector;
pub use dynamic_threshold::DynamicThresholdDetector;
pub use error::AnomalyError;
pub use forecast_residual::ForecastResidualDetector;
pub use iqr::IqrDetector;
pub use modified_zscore::ModifiedZScoreDetector;
pub use moving_average::MovingAverageResidualDetector;
pub use storage::DetectorStore;
pub use traits::{AnomalyDetector, AnomalyScore, DetectorType};
pub use zscore::ZScoreDetector;
