//! # chronix-forecast
//!
//! Statistical time-series forecasting for the Chronix database.
//!
//! ## Models
//!
//! | Model | Struct | Trend | Seasonality | Complexity |
//! |-------|--------|-------|-------------|------------|
//! | Simple Exponential Smoothing | [`SesModel`] | — | — | O(n) |
//! | Holt's Linear Trend | [`HoltLinearModel`] | ✓ | — | O(n) |
//! | Holt-Winters | [`HoltWintersModel`] | ✓ | ✓ | O(n) |
//! | ARIMA | [`ArimaModel`] | ✓ | — | O(n·p²) |
//! | SARIMA | [`SarimaModel`] | ✓ | ✓ | O(n·(p+Ps)²) |
//! | Linear Regression | [`LinearRegressionModel`] | ✓ | — | O(n) |
//!
//! All models implement [`ForecastModel`] for uniform dispatch and produce
//! [`ForecastResult`] with point estimates and confidence intervals.
//!
//! ## Model Selection
//!
//! [`auto_arima`] searches over candidate (p,d,q) orders and selects the
//! model minimising AICc.
//!
//! ## Parallel & GPU
//!
//! - [`parallel_fit`] / [`parallel_predict`] — multi-threaded batch operations
//!
//! ## Persistence
//!
//! [`ModelStore`] serialises trained models to disk; [`ModelCatalog`]
//! provides metadata lookup and versioning.

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod arima;
pub mod cross_validation;
pub mod diagnostics;
mod error;
mod holt;
mod holt_winters;
mod linear_regression;
pub mod optimizer;
mod parallel;
pub mod quantile;
mod result;
mod selection;
mod ses;
mod storage;
mod traits;
pub mod util;

pub use arima::{
    ArimaModel, AutoArimaOptions, AutoArimaResult, KPSS_CRITICAL_5PCT, SEASONAL_STRENGTH_THRESHOLD,
    SarimaConfig, SarimaModel, SearchStrategy, auto_arima, kpss_statistic,
    select_differencing_order, select_seasonal_differencing_order,
};
pub use cross_validation::{
    CrossValidationMode, CrossValidationResult, CrossValidator, FoldResult,
};
pub use diagnostics::{
    LjungBoxResult, ModelDiagnostics, aic, aicc, bic, compute_diagnostics, ljung_box, mae, mape,
    residual_acf, rmse, smape,
};
pub use error::ForecastError;
pub use holt::HoltLinearModel;
pub use holt_winters::HoltWintersModel;
pub use linear_regression::LinearRegressionModel;
pub use parallel::{parallel_fit, parallel_fit_predict, parallel_predict};
pub use quantile::{CalibrationStrategy, QuantileConfig, QuantileForecast, QuantileForecaster};
pub use result::{ForecastResult, ModelParams, ModelType};
pub use selection::{
    AutoForecast, AutoForecastOptions, CandidateScore, ModelSelection, SelectionMetric,
    auto_forecast, select_model,
};
pub use ses::SesModel;
pub use storage::{ModelCatalog, ModelMetadata, ModelStore, StorageError};
pub use traits::ForecastModel;
pub use util::{normal_quantile, z_for_confidence};
