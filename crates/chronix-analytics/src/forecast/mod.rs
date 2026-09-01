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
    auto_arima, kpss_statistic, select_differencing_order, ArimaModel, AutoArimaOptions,
    AutoArimaResult, SarimaConfig, SarimaModel, SearchStrategy, KPSS_CRITICAL_5PCT,
};
pub use cross_validation::{
    CrossValidationMode, CrossValidationResult, CrossValidator, FoldResult,
};
pub use diagnostics::{
    aic, aicc, bic, compute_diagnostics, ljung_box, mae, mape, residual_acf, rmse, smape,
    LjungBoxResult, ModelDiagnostics,
};
pub use error::ForecastError;
pub use holt::HoltLinearModel;
pub use holt_winters::HoltWintersModel;
pub use linear_regression::LinearRegressionModel;
pub use parallel::{parallel_fit, parallel_fit_predict, parallel_predict};
pub use quantile::{CalibrationStrategy, QuantileConfig, QuantileForecast, QuantileForecaster};
pub use result::{ForecastResult, ModelParams, ModelType};
pub use selection::{
    auto_forecast, select_model, AutoForecast, AutoForecastOptions, CandidateScore, ModelSelection,
    SelectionMetric,
};
pub use ses::SesModel;
pub use storage::{ModelCatalog, ModelMetadata, ModelStore, StorageError};
pub use traits::ForecastModel;
pub use util::{normal_quantile, z_for_confidence};
