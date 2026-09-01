//! ForecastModel trait definition.

use crate::forecast::error::ForecastError;
use crate::forecast::result::{ForecastResult, ModelParams, ModelType};

/// Core forecasting abstraction.
///
/// All forecast models implement this trait for uniform dispatch, persistence,
/// and online updating.
pub trait ForecastModel: Send + Sync {
    /// Fit the model on historical data.
    fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError>;

    /// Predict `horizon` steps ahead.
    fn predict(&self, horizon: usize) -> Result<ForecastResult, ForecastError>;

    /// Online update with a single new observation.
    fn update(&mut self, timestamp: i64, value: f64) -> Result<(), ForecastError>;

    /// Returns the model type.
    fn model_type(&self) -> ModelType;

    /// Returns the fitted parameters.
    fn params(&self) -> &ModelParams;
}
