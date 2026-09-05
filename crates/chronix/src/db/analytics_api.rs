//! Analytics methods: forecast, anomaly detection, and preprocessing.

use arrow::array;

use super::Chronix;
use crate::analytics::{AnomalyConfig, ForecastConfig};
use crate::error::{DbError, Result};

impl Chronix {
    /// Forecast future values for a series.
    ///
    /// Fetches historical data for the given measurement and field,
    /// fits a forecast model, and returns predicted values.
    pub fn forecast(
        &self,
        measurement: &str,
        field: &str,
        tags: &[(&str, &str)],
        start_ns: i64,
        end_ns: i64,
        horizon: usize,
        config: Option<ForecastConfig>,
    ) -> Result<chronix_analytics::forecast::ForecastResult> {
        use chronix_analytics::forecast::{
            ArimaModel, ForecastModel, HoltLinearModel, HoltWintersModel, LinearRegressionModel,
            SarimaModel, SesModel,
        };

        let cfg = config.unwrap_or_default();
        let (timestamps, values) =
            self.fetch_series_data(measurement, field, tags, start_ns, end_ns)?;

        if values.len() < 2 {
            return Err(DbError::Internal(
                "forecast: need at least 2 data points".into(),
            ));
        }

        let mut model: Box<dyn ForecastModel> = match cfg.model.as_deref() {
            Some("ses") | None => Box::new(SesModel::new(None)),
            Some("holt") => Box::new(HoltLinearModel::new(None, None, 0.98)),
            Some("holt_winters") => {
                Box::new(HoltWintersModel::new(None, None, None, cfg.period, false))
            }
            Some("arima") => {
                let (p, d, q) = cfg.arima_order.unwrap_or((1, 1, 1));
                Box::new(ArimaModel::new(p, d, q))
            }
            Some("sarima") => {
                let (p, d, q) = cfg.arima_order.unwrap_or((1, 1, 1));
                let (sp, sd, sq, m) =
                    cfg.sarima_order
                        .unwrap_or((1, 1, 1, cfg.period.unwrap_or(24)));
                Box::new(SarimaModel::new(p, d, q, sp, sd, sq, m))
            }
            Some("linear_regression") => Box::new(LinearRegressionModel::new()),
            // A name that is neither built in nor registered is an error, not
            // a reason to quietly fit something else: a typo in `"holtwinters"`
            // used to return a simple exponential smoothing forecast under the
            // caller's belief that it was seasonal.
            Some(name) => match chronix_analytics::registry::global_registry()
                .try_create_model_default(name)
            {
                Some(Ok(model)) => model,
                Some(Err(e)) => return Err(DbError::Internal(e.to_string())),
                None => {
                    return Err(DbError::Internal(format!(
                        "forecast: unknown model {name:?}; expected one of \"ses\", \"holt\", \
                         \"holt_winters\", \"arima\", \"sarima\", \"linear_regression\", \
                         or a name registered with the model registry"
                    )))
                }
            },
        };

        model
            .fit(&timestamps, &values)
            .map_err(|e| DbError::Internal(e.to_string()))?;

        let result = model
            .predict(horizon)
            .map_err(|e| DbError::Internal(e.to_string()))?;

        // Every model builds a 95 % interval; `confidence` used to be read by
        // nothing at all, so a caller asking for 99 % got 95 % and no warning.
        result
            .with_confidence(cfg.confidence)
            .map_err(|e| DbError::Internal(e.to_string()))
    }

    /// Forecast a series with the model chosen automatically.
    ///
    /// Fetches the window, then hands it to
    /// [`select_model`](chronix_analytics::forecast::select_model), which
    /// detects the seasonal period, chooses a differencing order by a KPSS
    /// unit-root test, searches an ARIMA order, and scores every eligible
    /// candidate by rolling-origin cross-validation **at `horizon`** — the
    /// only comparison that transfers across model families. The winner is
    /// refitted on the whole window.
    ///
    /// The returned [`AutoForecast`](chronix_analytics::forecast::AutoForecast)
    /// carries the forecast *and* the reasoning: which model won, what every
    /// candidate scored, and why anything that was not scored was left out.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] if the window cannot be read, if it is too short to
    /// hold a training window plus a `horizon`-long test window, or if no
    /// candidate could be cross-validated.
    pub fn auto_forecast(
        &self,
        measurement: &str,
        field: &str,
        tags: &[(&str, &str)],
        start_ns: i64,
        end_ns: i64,
        horizon: usize,
        options: Option<chronix_analytics::forecast::AutoForecastOptions>,
    ) -> Result<chronix_analytics::forecast::AutoForecast> {
        let (timestamps, values) =
            self.fetch_series_data(measurement, field, tags, start_ns, end_ns)?;
        chronix_analytics::forecast::auto_forecast(
            &timestamps,
            &values,
            horizon,
            &options.unwrap_or_default(),
        )
        .map_err(|e| DbError::Internal(e.to_string()))
    }

    /// Detect anomalies in a series.
    ///
    /// Fetches historical data, fits a detector, and returns scores.
    pub fn detect_anomalies(
        &self,
        measurement: &str,
        field: &str,
        tags: &[(&str, &str)],
        start_ns: i64,
        end_ns: i64,
        config: Option<AnomalyConfig>,
    ) -> Result<Vec<chronix_analytics::anomaly::AnomalyScore>> {
        use chronix_analytics::anomaly::{
            AnomalyDetector, DynamicThresholdDetector, ForecastResidualDetector, IqrDetector,
            ModifiedZScoreDetector, MovingAverageResidualDetector, ZScoreDetector,
        };

        let cfg = config.unwrap_or_default();
        let (timestamps, values) =
            self.fetch_series_data(measurement, field, tags, start_ns, end_ns)?;

        if values.len() < 3 {
            return Err(DbError::Internal(
                "detect_anomalies: need at least 3 data points".into(),
            ));
        }

        let mut detector: Box<dyn AnomalyDetector> = match cfg.method.as_deref() {
            Some("iqr") => Box::new(IqrDetector::new(Some(cfg.threshold))),
            Some("modified_zscore") => Box::new(ModifiedZScoreDetector::new(Some(cfg.threshold))),
            Some("dynamic_threshold") => Box::new(DynamicThresholdDetector::new(
                cfg.window_size,
                Some(cfg.threshold),
            )),
            Some("forecast_residual") => {
                Box::new(ForecastResidualDetector::new(Some(cfg.threshold), None))
            }
            Some("moving_average") => Box::new(MovingAverageResidualDetector::new(
                cfg.window_size,
                Some(cfg.threshold),
            )),
            Some(name) => match chronix_analytics::registry::global_registry()
                .try_create_detector_default(name)
            {
                Some(Ok(det)) => det,
                Some(Err(e)) => return Err(DbError::Internal(e.to_string())),
                None => Box::new(ZScoreDetector::new(Some(cfg.threshold))),
            },
            None => Box::new(ZScoreDetector::new(Some(cfg.threshold))),
        };

        detector
            .fit(&timestamps, &values)
            .map_err(|e| DbError::Internal(e.to_string()))?;

        detector
            .detect(&timestamps, &values)
            .map_err(|e| DbError::Internal(e.to_string()))
    }

    /// Preprocess a series (interpolate, smooth, resample).
    #[allow(clippy::needless_pass_by_value)] // by-value config mirrors the sibling forecast/anomaly APIs
    pub fn preprocess(
        &self,
        measurement: &str,
        field: &str,
        tags: &[(&str, &str)],
        start_ns: i64,
        end_ns: i64,
        config: chronix_analytics::preprocess::PreprocessConfig,
    ) -> Result<chronix_analytics::preprocess::PreprocessResult> {
        let (timestamps, values) =
            self.fetch_series_data(measurement, field, tags, start_ns, end_ns)?;

        chronix_analytics::preprocess::PreprocessPipeline::run(&config, &timestamps, &values)
            .map_err(|e| DbError::Internal(e.to_string()))
    }

    /// Fetch raw (timestamps, values) for a single field+tags combination.
    ///
    /// Uses memtable scan for in-memory data. For persisted data, build
    /// a query via [`Chronix::query()`] and [`Chronix::execute()`].
    pub fn fetch_series_data(
        &self,
        measurement: &str,
        field: &str,
        tags: &[(&str, &str)],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<i64>, Vec<f64>)> {
        // Use the full query engine so both memtable AND on-disk segments
        // are scanned — not just the memtable.
        let mut builder = self.query().measurement(measurement);
        for &(k, v) in tags {
            builder = builder.tag(k, v);
        }
        builder = builder.range(start_ns, end_ns);
        let plan = builder.build()?;
        let batches = self.execute_stream(&plan)?;

        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        for batch in &batches {
            let schema = batch.schema();

            // Find timestamp column
            let ts_idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == chronix_core::TIME_COLUMN);
            let field_idx = schema.index_of(field).ok();

            if let (Some(ts_idx), Some(field_idx)) = (ts_idx, field_idx) {
                let ts_arr = batch.column(ts_idx);
                let val_arr = batch.column(field_idx);

                if let Some(ts_i64) = ts_arr.as_any().downcast_ref::<array::Int64Array>() {
                    for row in 0..batch.num_rows() {
                        if val_arr.is_null(row) {
                            continue;
                        }
                        let v = if let Some(a) =
                            val_arr.as_any().downcast_ref::<array::Float64Array>()
                        {
                            a.value(row)
                        } else if let Some(a) = val_arr.as_any().downcast_ref::<array::Int64Array>()
                        {
                            a.value(row) as f64
                        } else if let Some(a) =
                            val_arr.as_any().downcast_ref::<array::UInt64Array>()
                        {
                            a.value(row) as f64
                        } else {
                            continue;
                        };
                        timestamps.push(ts_i64.value(row));
                        values.push(v);
                    }
                }
            }
        }

        Ok((timestamps, values))
    }
}
