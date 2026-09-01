//! # Chronix Preprocessing Pipeline
//!
//! Gap detection, interpolation, smoothing, resampling, and clock drift
//! correction for time-series data — all operating on Arrow-compatible
//! `&[f64]` / `&[i64]` arrays with zero-copy where possible.
//!
//! ## Pipeline Order
//!
//! ```text
//! Raw data → Clock drift correction → Gap detection → Interpolation → Smoothing → Resampling
//! ```
//!
//! ## Interpolation Methods
//!
//! - `Linear` — linearly interpolated values between known points
//! - `Nearest` — nearest neighbor fill
//! - `CatmullRom` — Catmull-Rom spline (C1 local interpolation, requires ≥ 4 surrounding points)
//! - `Zero` — fill with 0.0
//! - `Forward` — last known value carried forward
//! - `Nocb` — next observation carried backward
//!
//! ## Smoothing Filters
//!
//! - `ExponentialSmoother` — single-pass EMA with configurable alpha
//! - `MovingAverageSmoother` — sliding window mean
//! - `WeightedMovingAverage` — custom weight vector

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod clock_drift;
mod gap;
mod interpolation;
mod pipeline;
mod resampling;
mod smoothing;

pub mod arrow_adapters;
pub mod auto_features;
pub mod decomposition;
pub mod features;

pub use arrow_adapters::{
    arrow_interpolate, arrow_preprocess, arrow_smooth, ArrowPreprocessResult,
};
pub use auto_features::{AutoFeatureConfig, FeatureMatrix};
pub use clock_drift::{ClockDriftDetector, ClockDriftStrategy, DriftReport};
pub use decomposition::{
    detect_period, detect_period_with_threshold, stl_decompose, Decomposition, DecompositionError,
    SeasonalDecomposer, StlConfig, StlDecomposer,
};
pub use features::{diff, ewm, lag, pct_change, rolling_corr, rolling_mean, rolling_std, zscore};
pub use gap::GapDetector;
pub use interpolation::{InterpolationError, Interpolator};
pub use pipeline::{PreprocessConfig, PreprocessError, PreprocessPipeline, PreprocessResult};
pub use resampling::{AggregationFn, ResampleConfig, Resampler};
pub use smoothing::{ExponentialSmoother, MovingAverageSmoother, Smoother, WeightedMovingAverage};
