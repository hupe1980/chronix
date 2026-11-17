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
        let (timestamps, values) = match cfg.step_ns {
            Some(step) => bucket_mean(&timestamps, &values, start_ns, end_ns, step)?,
            None => (timestamps, values),
        };

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
            // A name that is neither built in nor registered is an error, never a
            // reason to quietly fit a different model.
            Some(name) => match chronix_analytics::registry::global_registry()
                .try_create_model_default(name)
            {
                Some(Ok(model)) => model,
                Some(Err(e)) => return Err(DbError::Internal(e.to_string())),
                None => {
                    return Err(DbError::InvalidRequest(format!(
                        "forecast: unknown model {name:?}; expected one of \"ses\", \"holt\", \
                         \"holt_winters\", \"arima\", \"sarima\", \"linear_regression\", \
                         or a name registered with the model registry"
                    )));
                }
            },
        };

        let result = self
            .executor
            .run(|| {
                model.fit(&timestamps, &values)?;
                model.predict(horizon)
            })
            .map_err(|e| DbError::Internal(e.to_string()))?;

        // Every model builds a 95 % interval; rescale it to the requested
        // `confidence`.
        let mut result = result
            .with_confidence(cfg.confidence)
            .map_err(|e| DbError::Internal(e.to_string()))?;
        if let Some(min) = cfg.lower_bound {
            result.clamp_below(min);
        }
        Ok(result)
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
        let options = options.unwrap_or_default();
        self.executor
            .run(|| {
                chronix_analytics::forecast::auto_forecast(&timestamps, &values, horizon, &options)
            })
            .map_err(|e| DbError::Internal(e.to_string()))
    }

    /// [`auto_forecast`](Self::auto_forecast) on the series averaged into
    /// `step_ns`-wide buckets.
    ///
    /// Fitting at a chart's resolution rather than on raw samples is both
    /// faster and better: a week of 10 s samples is 60 480 points and too
    /// long to cross-validate interactively, while the same week at 15 min
    /// is 672 points in which the daily season is visible to period
    /// detection. `horizon` and the result are in steps.
    ///
    /// Buckets are aligned to `start_ns` and hold the mean of their samples,
    /// stamped at the bucket's start. An empty bucket between two non-empty
    /// ones is filled by linear interpolation, so the spacing stays regular,
    /// which every seasonal model assumes; empty buckets at either end are
    /// dropped.
    ///
    /// # Errors
    ///
    /// As [`auto_forecast`](Self::auto_forecast), and
    /// [`DbError::InvalidRequest`] if `step_ns` is not positive.
    #[allow(clippy::too_many_arguments)]
    pub fn auto_forecast_at_step(
        &self,
        measurement: &str,
        field: &str,
        tags: &[(&str, &str)],
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        horizon: usize,
        options: Option<chronix_analytics::forecast::AutoForecastOptions>,
    ) -> Result<chronix_analytics::forecast::AutoForecast> {
        let (timestamps, values) =
            self.fetch_series_data(measurement, field, tags, start_ns, end_ns)?;
        let (timestamps, values) = bucket_mean(&timestamps, &values, start_ns, end_ns, step_ns)?;
        let options = options.unwrap_or_default();
        self.executor
            .run(|| {
                chronix_analytics::forecast::auto_forecast(&timestamps, &values, horizon, &options)
            })
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

        // An unknown method name is refused, as `forecast` refuses an unknown
        // model: a typo must not silently run a different detector. `"zscore"` has
        // its own branch for the same reason.
        let mut detector: Box<dyn AnomalyDetector> = match cfg.method.as_deref() {
            Some("zscore") | None => Box::new(ZScoreDetector::new(Some(cfg.threshold))),
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
                None => {
                    return Err(DbError::InvalidRequest(format!(
                        "detect_anomalies: unknown detector {name:?}; expected one of \
                         \"zscore\", \"modified_zscore\", \"iqr\", \"dynamic_threshold\", \
                         \"forecast_residual\", \"moving_average\", or a name registered \
                         with the detector registry"
                    )));
                }
            },
        };

        self.executor
            .run(|| {
                detector.fit(&timestamps, &values)?;
                detector.detect(&timestamps, &values)
            })
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

        self.executor
            .run(|| {
                chronix_analytics::preprocess::PreprocessPipeline::run(
                    &config,
                    &timestamps,
                    &values,
                )
            })
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

/// Average `(timestamps, values)` into `step`-wide buckets aligned to
/// `start`; interior empty buckets are interpolated, edge ones dropped.
fn bucket_mean(
    timestamps: &[i64],
    values: &[f64],
    start: i64,
    end: i64,
    step: i64,
) -> Result<(Vec<i64>, Vec<f64>)> {
    if step <= 0 {
        return Err(DbError::InvalidRequest(format!(
            "step must be a positive number of nanoseconds, got {step}"
        )));
    }
    // One accumulator per bucket; a window of more than a million steps is
    // a step chosen too small, not a series worth fitting.
    const MAX_BUCKETS: i64 = 1_000_000;
    let buckets = end.saturating_sub(start).max(0) / step + 1;
    if buckets > MAX_BUCKETS {
        return Err(DbError::InvalidRequest(format!(
            "step {step} ns splits the window into {buckets} buckets; at most {MAX_BUCKETS}"
        )));
    }
    let n = usize::try_from(buckets).unwrap_or(0);
    let mut sum = vec![0.0_f64; n];
    let mut count = vec![0_u32; n];
    for (&t, &v) in timestamps.iter().zip(values) {
        if t < start || !v.is_finite() {
            continue;
        }
        let Ok(i) = usize::try_from((t - start) / step) else {
            continue;
        };
        if let (Some(s), Some(c)) = (sum.get_mut(i), count.get_mut(i)) {
            *s += v;
            *c += 1;
        }
    }
    let filled: Vec<usize> = (0..n)
        .filter(|&i| count.get(i).is_some_and(|&c| c > 0))
        .collect();
    let (Some(&first), Some(&last)) = (filled.first(), filled.last()) else {
        return Ok((Vec::new(), Vec::new()));
    };
    let mut out_t = Vec::with_capacity(last - first + 1);
    let mut out_v = Vec::with_capacity(last - first + 1);
    let mean = |i: usize| {
        sum.get(i).copied().unwrap_or(0.0) / f64::from(count.get(i).copied().unwrap_or(1))
    };
    for pair in filled.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let (va, vb) = (mean(a), mean(b));
        for i in a..b {
            let frac = (i - a) as f64 / (b - a) as f64;
            out_t.push(start + i as i64 * step);
            out_v.push(va + (vb - va) * frac);
        }
    }
    out_t.push(start + last as i64 * step);
    out_v.push(mean(last));
    Ok((out_t, out_v))
}

#[cfg(test)]
mod bucket_tests {
    use super::bucket_mean;

    #[test]
    fn averages_and_interpolates_interior_gaps() {
        // Buckets of 10: [0,10) holds 1 and 3, [10,20) is empty, [20,30) holds 6.
        let (t, v) = bucket_mean(&[0, 5, 25], &[1.0, 3.0, 6.0], 0, 29, 10).unwrap();
        assert_eq!(t, vec![0, 10, 20]);
        assert_eq!(v, vec![2.0, 4.0, 6.0]);
    }

    #[test]
    fn drops_empty_edges_and_refuses_a_bad_step() {
        let (t, v) = bucket_mean(&[15], &[7.0], 0, 40, 10).unwrap();
        assert_eq!((t, v), (vec![10], vec![7.0]));
        assert!(bucket_mean(&[1], &[1.0], 0, 10, 0).is_err());
    }
}
