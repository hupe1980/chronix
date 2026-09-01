//! # Chronix Analytics — Real-Time Streaming Analytics
//!
//! Streaming anomaly detection, continuous forecasting, materialized
//! forecast views, alerting, feedback, and forecast accuracy tracking.
//!
//! ## Architecture
//!
//! ```text
//! CDC PointWritten events
//!       │
//!       ├──→ StreamingAnomalyEngine (per-series anomaly detection)
//!       │          └──→ AlertEngine (configurable alert actions)
//!       │
//!       ├──→ ContinuousForecastEngine (incremental model updates)
//!       │          └──→ ForecastCache (materialized forecast views)
//!       │
//!       └──→ AnomalyPrecisionTracker (user feedback)
//!            ForecastAccuracyTracker (retroactive accuracy metrics)
//! ```
//!
//! ## Example
//!
//! ```no_run
//! use chronix_analytics::{
//!     StreamingAnomalyEngine, StreamingAnomalyConfig,
//!     ContinuousForecastEngine, ContinuousForecastConfig,
//! };
//! use chronix_analytics::anomaly::DetectorType;
//! use chronix_analytics::forecast::ModelType;
//!
//! // Set up streaming anomaly detection
//! let anomaly_engine = StreamingAnomalyEngine::new();
//! let config = StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 3.0);
//! anomaly_engine.enable(config).unwrap();
//!
//! // Set up continuous forecasting
//! let forecast_engine = ContinuousForecastEngine::new();
//! let config = ContinuousForecastConfig::new("cpu", ModelType::Ses);
//! forecast_engine.enable(config).unwrap();
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]
// Index-coupled numeric loops (matrix pivots, ACF lags, ring buffers) read
// clearer with explicit indices than iterator chains.
#![allow(clippy::needless_range_loop, clippy::explicit_counter_loop)]

mod accuracy;
mod alerting;
pub mod anomaly;
pub mod compute;
mod continuous_forecast;
pub mod forecast;
pub mod lifecycle;
pub mod multivariate;
pub mod preprocess;

pub mod error;
mod feedback;
mod materialized;
#[cfg(test)]
mod perf;
pub mod registry;
mod streaming_anomaly;
mod util;

pub use accuracy::{
    compute_accuracy, compute_accuracy_with_mase, mase, AccuracyMetrics, ForecastAccuracyTracker,
};
pub use alerting::{AlertAction, AlertConfig, AlertEngine, FiredAlert};
pub use continuous_forecast::{ContinuousForecastConfig, ContinuousForecastEngine, ForecastUpdate};
pub use error::AnalyticsError;
pub use feedback::{AnomalyFeedback, AnomalyPrecisionTracker, FeedbackLabel, PrecisionStats};
pub use materialized::{ForecastCache, MaterializedForecast};
pub use registry::{
    global_registry, DetectorFactory, ModelFactory, PluginError, PluginInfo, PluginRegistry,
};
pub use streaming_anomaly::{ScoredAnomaly, StreamingAnomalyConfig, StreamingAnomalyEngine};
