//! # chronix-multivariate
//!
//! Multi-series analysis for the Chronix time-series database.
//!
//! ## Capabilities
//!
//! | Feature | Key Types |
//! |---------|-----------|
//! | Series alignment | [`MultiSeriesContext`], [`ColumnarMatrix`] |
//! | Correlation | [`PearsonCorrelation`], [`SpearmanCorrelation`], [`KendallTau`] |
//! | Rolling / lag correlation | [`RollingCorrelation`], [`LagCorrelation`] |
//! | Correlation matrix | [`CrossCorrelationMatrix`] |
//! | Anomaly detection | [`MahalanobisDetector`], [`IsolationForestDetector`], [`PcaAnomalyDetector`] |
//! | Forecasting | [`MultiLinearRegression`], [`VarModel`] |
//! | Derived series | [`ArithmeticExpr`], [`DerivedSeriesEngine`] |
//! | Composite signals | [`CompositeSignalEngine`], [`CompositeSignalRule`] |
//!
//! ## Overview
//!
//! [`MultiSeriesContext`] aligns multiple time-series to a common time grid
//! (via interpolation or forward-fill), producing a [`ColumnarMatrix`] that
//! feeds into correlation, anomaly detection, and forecasting APIs.
//!
//! All multivariate anomaly detectors implement [`MultivariateAnomalyDetector`]
//! and produce [`MultivariateAnomalyScore`] for uniform consumption.
//!
//! [`CompositeSignalEngine`] evaluates boolean rules over multiple series
//! and emits signals via an async channel when conditions are met.

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod composite;
mod context;
mod correlation;
mod derived;
mod error;
/// Standalone Granger causality utilities.
mod mv_anomaly;
mod mv_forecast;

pub use composite::{
    AnalyticsResults, CompositeSignal, CompositeSignalEngine, CompositeSignalRule,
    DEFAULT_SIGNAL_CHANNEL_CAPACITY, SignalReceiver, SignalSender, signal_channel,
};
pub use context::{ColumnarMatrix, MultiSeriesContext};
pub use correlation::{
    CorrelationMethod, CrossCorrelationMatrix, KendallTau, LagCorrelation, PearsonCorrelation,
    RollingCorrelation, SpearmanCorrelation,
};
pub use derived::{
    ArithOp, ArithmeticExpr, DerivedSeriesDefinition, DerivedSeriesEngine, DerivedSeriesExpr,
    LazyDerivedSeries, LogReturnExpr, RollingStat, RollingStatExpr,
};
pub use error::MultivariateError;
pub use mv_anomaly::{
    IsolationForestDetector, MahalanobisDetector, MultivariateAnomalyDetector,
    MultivariateAnomalyScore, PcaAnomalyDetector,
};
pub use mv_forecast::{
    GrangerCausalityResult, MultiLinearRegression, MultivariateForecastModel,
    MultivariateForecastResult, VarModel,
};
