//! Quantile forecasting via empirical residual quantiles.
//!
//! The built-in models produce *Gaussian* prediction intervals from a single
//! in-sample `residual_std`: `ŷ ± z·σ·f(h)`. That makes two assumptions that
//! do not hold for the workloads Chronix targets:
//!
//! 1. **Symmetric, normal errors.** Energy series (PV generation, household
//!    load, battery state of charge) are heavily skewed and heteroscedastic.
//!    A symmetric interval under-covers the long tail while wasting width on
//!    the short one.
//! 2. **A single error scale for every horizon.** Forecast error grows with
//!    the horizon, and how it grows depends on the model and the data, not on
//!    a closed-form factor.
//!
//! Note that empirical residual quantiles are *additive* offsets, so they do
//! not by themselves respect a physical domain: a series bounded at zero can
//! still receive a negative lower bound where the point forecast is noisy.
//! Set [`QuantileConfig::lower_bound`] / [`upper_bound`] for that — learning
//! the residual shape and declaring the domain are separate jobs.
//!
//! [`upper_bound`]: QuantileConfig::upper_bound
//!
//! [`QuantileForecaster`] drops both assumptions. It calibrates on the
//! **empirical distribution of walk-forward residuals, bucketed per horizon
//! step**, so the interval shape is learned from the data: asymmetric where
//! the data is asymmetric, and widening at whatever rate the data actually
//! widens.
//!
//! ## Calibration is strictly out-of-sample
//!
//! Residuals come from rolling-origin evaluation (Hyndman & Athanasopoulos,
//! *FPP3* §5.10): the model is refit — or replayed — on data up to each
//! origin, then asked for `horizon` steps, and only genuinely unseen points
//! contribute residuals. In-sample residuals would be optimistically small
//! and the intervals correspondingly too narrow.
//!
//! ## Optional conformal calibration
//!
//! With [`QuantileConfig::conformal`] enabled the residual quantile is taken
//! at the finite-sample-corrected rank `⌈(n+1)(1-α)⌉/n` (Vovk et al.;
//! Gibbs & Candès, *Adaptive Conformal Inference*, NeurIPS 2021). Under
//! exchangeability that yields marginal coverage of at least `1-α` rather
//! than merely asymptotic coverage — the difference matters at the small
//! calibration sizes an embedded gateway actually has.

use crate::forecast::error::ForecastError;
use crate::forecast::result::ForecastResult;
use crate::forecast::traits::ForecastModel;

/// How to build the residual sample used for calibration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalibrationStrategy {
    /// Refit the model from scratch at every origin.
    ///
    /// The most faithful simulation of production behaviour and the most
    /// expensive. Use for offline calibration.
    Refit,
    /// Fit once on the initial window, then advance the origin using the
    /// model's online [`update`](ForecastModel::update).
    ///
    /// Matches how a long-running forecaster actually behaves and costs
    /// roughly one fit plus `n` updates, which is what makes calibration
    /// affordable on a gateway.
    OnlineUpdate,
}

/// Configuration for [`QuantileForecaster`].
#[derive(Debug, Clone)]
pub struct QuantileConfig {
    /// Quantile levels to produce, each in `(0, 1)`.
    pub levels: Vec<f64>,
    /// Maximum horizon to calibrate. Residuals are bucketed per step
    /// `1..=horizon`.
    pub horizon: usize,
    /// Number of observations in the initial training window.
    pub initial_window: usize,
    /// Observations to advance between rolling origins.
    pub step: usize,
    /// How to advance the model between origins.
    pub strategy: CalibrationStrategy,
    /// Apply the finite-sample conformal rank correction.
    pub conformal: bool,
    /// Optional hard lower bound on predicted quantiles.
    ///
    /// Residual quantiles are *additive* offsets on the point forecast, so
    /// they can cross a physical limit — a PV series bounded at zero can
    /// still get a negative lower bound when the point forecast is noisy.
    /// Learning the residual *shape* from data and stating the *domain*
    /// explicitly are different jobs; this is the second one.
    pub lower_bound: Option<f64>,
    /// Optional hard upper bound on predicted quantiles (e.g. inverter
    /// nameplate capacity, 100% state of charge).
    pub upper_bound: Option<f64>,
}

impl Default for QuantileConfig {
    fn default() -> Self {
        Self {
            levels: vec![0.1, 0.5, 0.9],
            horizon: 24,
            initial_window: 0, // 0 = derive from the data (see `resolve`)
            step: 1,
            strategy: CalibrationStrategy::OnlineUpdate,
            conformal: true,
            lower_bound: None,
            upper_bound: None,
        }
    }
}

impl QuantileConfig {
    /// Constrain predicted quantiles to `[lo, hi]`.
    #[must_use]
    pub fn with_bounds(mut self, lo: Option<f64>, hi: Option<f64>) -> Self {
        self.lower_bound = lo;
        self.upper_bound = hi;
        self
    }

    /// Clamp a value to the configured domain.
    fn clamp(&self, v: f64) -> f64 {
        let v = self.lower_bound.map_or(v, |lo| v.max(lo));
        self.upper_bound.map_or(v, |hi| v.min(hi))
    }

    /// Config producing a symmetric central interval at `level` coverage
    /// (e.g. `0.9` → the 5% and 95% quantiles plus the median).
    #[must_use]
    pub fn central(level: f64, horizon: usize) -> Self {
        let tail = (1.0 - level) / 2.0;
        Self {
            levels: vec![tail, 0.5, 1.0 - tail],
            horizon,
            ..Self::default()
        }
    }

    fn validate(&self) -> Result<(), ForecastError> {
        if self.levels.is_empty() {
            return Err(ForecastError::InvalidInput(
                "at least one quantile level is required".into(),
            ));
        }
        for &q in &self.levels {
            if !(q > 0.0 && q < 1.0) {
                return Err(ForecastError::InvalidInput(format!(
                    "quantile level must be in (0, 1), got {q}"
                )));
            }
        }
        if self.horizon == 0 {
            return Err(ForecastError::InvalidInput(
                "horizon must be at least 1".into(),
            ));
        }
        if self.step == 0 {
            return Err(ForecastError::InvalidInput(
                "step must be at least 1".into(),
            ));
        }
        if let (Some(lo), Some(hi)) = (self.lower_bound, self.upper_bound) {
            if lo > hi {
                return Err(ForecastError::InvalidInput(format!(
                    "lower_bound {lo} exceeds upper_bound {hi}"
                )));
            }
        }
        Ok(())
    }

    /// Choose an initial window when the caller left it at 0.
    ///
    /// Half the series, clamped so that at least one rolling origin exists.
    fn resolve_initial_window(&self, n: usize) -> usize {
        if self.initial_window > 0 {
            return self.initial_window;
        }
        (n / 2).max(1)
    }
}

/// A per-horizon set of predicted quantiles.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QuantileForecast {
    /// Timestamps of the forecast points (nanoseconds).
    pub timestamps: Vec<i64>,
    /// Point forecast from the underlying model, one per horizon step.
    pub point: Vec<f64>,
    /// Requested quantile levels, ascending.
    pub levels: Vec<f64>,
    /// `quantiles[i][h]` is the `levels[i]` quantile at horizon step `h`.
    pub quantiles: Vec<Vec<f64>>,
    /// Number of residuals backing each horizon step's calibration.
    ///
    /// Small counts mean wide sampling error in the quantile estimate; the
    /// caller can use this to decide whether to trust a given horizon.
    pub calibration_counts: Vec<usize>,
}

impl QuantileForecast {
    /// Values for `level`, if it was among the requested levels.
    ///
    /// Matched with a small tolerance rather than exactly: levels are
    /// routinely derived arithmetically (`(1.0 - 0.9) / 2.0` is
    /// `0.049999999999999996`, not `0.05`), so exact `f64` equality would
    /// fail for the very levels [`QuantileConfig::central`] produces.
    #[must_use]
    pub fn level(&self, level: f64) -> Option<&[f64]> {
        const TOL: f64 = 1e-9;
        self.levels
            .iter()
            .position(|l| (*l - level).abs() < TOL)
            .map(|i| self.quantiles[i].as_slice())
    }

    /// Number of horizon steps.
    #[must_use]
    pub fn horizon(&self) -> usize {
        self.point.len()
    }
}

/// Wraps a [`ForecastModel`] with empirical, horizon-aware quantile intervals.
///
/// # Example
///
/// ```no_run
/// use chronix_analytics::forecast::{
///     HoltWintersModel, QuantileConfig, QuantileForecaster,
/// };
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let (timestamps, values): (Vec<i64>, Vec<f64>) = (vec![], vec![]);
/// let config = QuantileConfig::central(0.9, 24);
///
/// let mut qf = QuantileForecaster::new(
///     || Box::new(HoltWintersModel::new(Some(0.3), Some(0.1), Some(0.1), Some(24), false)),
///     config,
/// );
/// qf.fit(&timestamps, &values)?;
///
/// let forecast = qf.predict(24)?;
/// let p05 = forecast.level(0.05).expect("requested level");
/// # Ok(())
/// # }
/// ```
pub struct QuantileForecaster {
    /// Builds a fresh, unfitted model. A factory (rather than a single model
    /// instance) is what lets `Refit` start each rolling origin from a clean
    /// state without needing to reconstruct a model from its parameters.
    factory: Box<dyn Fn() -> Box<dyn ForecastModel> + Send + Sync>,
    model: Box<dyn ForecastModel>,
    config: QuantileConfig,
    /// `residuals[h - 1]` holds the out-of-sample errors observed at horizon
    /// step `h`.
    residuals: Vec<Vec<f64>>,
    fitted: bool,
}

impl QuantileForecaster {
    /// Build a forecaster from a model factory.
    ///
    /// `factory` must return a fresh, unfitted model on every call.
    pub fn new<F>(factory: F, config: QuantileConfig) -> Self
    where
        F: Fn() -> Box<dyn ForecastModel> + Send + Sync + 'static,
    {
        let model = factory();
        Self {
            factory: Box::new(factory),
            model,
            config,
            residuals: Vec::new(),
            fitted: false,
        }
    }

    /// Fit the underlying model and calibrate the residual distribution.
    ///
    /// # Errors
    ///
    /// Returns [`ForecastError::InvalidInput`] if the configuration is
    /// invalid or the series is too short to produce a single rolling origin,
    /// or propagates the underlying model's fit errors.
    pub fn fit(&mut self, timestamps: &[i64], values: &[f64]) -> Result<(), ForecastError> {
        self.config.validate()?;
        if timestamps.len() != values.len() {
            return Err(ForecastError::InvalidInput(
                "timestamps and values must have the same length".into(),
            ));
        }

        let n = values.len();
        let window = self.config.resolve_initial_window(n);
        let horizon = self.config.horizon;

        if n < window + 1 {
            return Err(ForecastError::InvalidInput(format!(
                "need more than {window} observations to calibrate, got {n}"
            )));
        }

        self.residuals = vec![Vec::new(); horizon];
        self.collect_residuals(timestamps, values, window)?;

        // Final model is fit on everything, so predictions use all the data.
        self.model.fit(timestamps, values)?;
        self.fitted = true;
        Ok(())
    }

    /// Walk the rolling origins, recording out-of-sample residuals per
    /// horizon step.
    fn collect_residuals(
        &mut self,
        timestamps: &[i64],
        values: &[f64],
        window: usize,
    ) -> Result<(), ForecastError> {
        let n = values.len();
        let horizon = self.config.horizon;

        match self.config.strategy {
            CalibrationStrategy::Refit => {
                let mut origin = window;
                while origin < n {
                    let mut model = (self.factory)();
                    model.fit(&timestamps[..origin], &values[..origin])?;
                    let avail = (n - origin).min(horizon);
                    if avail > 0 {
                        let f = model.predict(avail)?;
                        record(&mut self.residuals, &f.values, &values[origin..], avail);
                    }
                    origin += self.config.step;
                }
            }
            CalibrationStrategy::OnlineUpdate => {
                // Fit once, then advance with online updates — the cheap path
                // and the one that mirrors a live forecaster.
                self.model.fit(&timestamps[..window], &values[..window])?;
                let mut origin = window;
                while origin < n {
                    let avail = (n - origin).min(horizon);
                    if avail > 0 {
                        let f = self.model.predict(avail)?;
                        record(&mut self.residuals, &f.values, &values[origin..], avail);
                    }
                    // Advance the origin by feeding the observations we just
                    // scored against.
                    let next = (origin + self.config.step).min(n);
                    for i in origin..next {
                        self.model.update(timestamps[i], values[i])?;
                    }
                    if next == origin {
                        break;
                    }
                    origin = next;
                }
            }
        }
        Ok(())
    }

    /// Produce point forecasts plus calibrated quantiles for `horizon` steps.
    ///
    /// Horizons beyond the calibrated range reuse the last calibrated step's
    /// residual distribution, which is the most conservative available
    /// estimate rather than a silent extrapolation.
    ///
    /// # Errors
    ///
    /// Returns [`ForecastError::NotFitted`] if [`fit`](Self::fit) has not
    /// been called, or propagates the underlying model's prediction errors.
    pub fn predict(&self, horizon: usize) -> Result<QuantileForecast, ForecastError> {
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }
        let base = self.model.predict(horizon)?;

        let mut levels = self.config.levels.clone();
        levels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mut quantiles = vec![Vec::with_capacity(horizon); levels.len()];
        let mut counts = Vec::with_capacity(horizon);

        for h in 0..horizon {
            // Fall back to the widest calibrated horizon rather than
            // extrapolating a distribution we never observed.
            let bucket = h.min(self.residuals.len().saturating_sub(1));
            let sample = self.residuals.get(bucket).map_or(&[][..], Vec::as_slice);
            counts.push(sample.len());

            let mut sorted: Vec<f64> = sample.iter().copied().filter(|v| v.is_finite()).collect();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            for (i, &q) in levels.iter().enumerate() {
                let offset = empirical_quantile(&sorted, q, self.config.conformal);
                quantiles[i].push(self.config.clamp(base.values[h] + offset));
            }
        }

        // Quantiles must not cross: a finite sample can otherwise produce a
        // 90% bound below the 50% bound at a given horizon.
        enforce_monotonicity(&mut quantiles);

        // The bounds are a physical fact about the quantity, so they hold
        // for the point forecast too: a PV forecast of −66 W at night with
        // an interval of [0, 450] is a contradiction the reader has to
        // resolve by hand.
        let point = base.values.iter().map(|&v| self.config.clamp(v)).collect();
        Ok(QuantileForecast {
            timestamps: base.timestamps,
            point,
            levels,
            quantiles,
            calibration_counts: counts,
        })
    }

    /// Rewrite a [`ForecastResult`]'s bounds using the calibrated empirical
    /// quantiles, keeping the existing API shape.
    ///
    /// # Errors
    ///
    /// As [`predict`](Self::predict).
    pub fn predict_interval(
        &self,
        horizon: usize,
        level: f64,
    ) -> Result<ForecastResult, ForecastError> {
        if !self.fitted {
            return Err(ForecastError::NotFitted);
        }
        let tail = (1.0 - level) / 2.0;
        let base = self.model.predict(horizon)?;
        let (lo, hi) = self.quantiles_only(&base, horizon, tail, 1.0 - tail);

        // The point forecast is clamped for the same reason the bounds are:
        // `predict` already does it, and an unclamped point inside a clamped
        // interval is a contradiction the reader has to resolve by hand.
        let values = base.values.iter().map(|&v| self.config.clamp(v)).collect();

        Ok(ForecastResult {
            values,
            timestamps: base.timestamps,
            confidence_lower: lo,
            confidence_upper: hi,
            confidence_level: level,
        })
    }

    fn quantiles_only(
        &self,
        base: &ForecastResult,
        horizon: usize,
        lo_level: f64,
        hi_level: f64,
    ) -> (Vec<f64>, Vec<f64>) {
        let mut lo = Vec::with_capacity(horizon);
        let mut hi = Vec::with_capacity(horizon);
        for h in 0..horizon {
            let bucket = h.min(self.residuals.len().saturating_sub(1));
            let sample = self.residuals.get(bucket).map_or(&[][..], Vec::as_slice);
            let mut sorted: Vec<f64> = sample.iter().copied().filter(|v| v.is_finite()).collect();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            lo.push(self.config.clamp(
                base.values[h] + empirical_quantile(&sorted, lo_level, self.config.conformal),
            ));
            hi.push(self.config.clamp(
                base.values[h] + empirical_quantile(&sorted, hi_level, self.config.conformal),
            ));
        }
        (lo, hi)
    }

    /// Number of calibration residuals collected for horizon step `h`
    /// (1-based). Returns 0 for uncalibrated horizons.
    #[must_use]
    pub fn calibration_size(&self, h: usize) -> usize {
        h.checked_sub(1)
            .and_then(|i| self.residuals.get(i))
            .map_or(0, Vec::len)
    }

    /// The wrapped model.
    #[must_use]
    pub fn model(&self) -> &dyn ForecastModel {
        &*self.model
    }
}

/// Record residuals for one rolling origin into the per-horizon buckets.
fn record(residuals: &mut [Vec<f64>], predicted: &[f64], actual: &[f64], avail: usize) {
    for h in 0..avail.min(predicted.len()).min(actual.len()) {
        let e = actual[h] - predicted[h];
        if e.is_finite() {
            residuals[h].push(e);
        }
    }
}

/// Empirical quantile of a sorted sample using linear interpolation
/// (Hyndman & Fan type 7, the R/NumPy default).
///
/// With `conformal`, the rank is moved **outward** by the split-conformal
/// finite-sample correction: `⌈(n+1)q⌉/n` for an upper quantile, and its
/// mirror image `1 − ⌈(n+1)(1−q)⌉/n` for a lower one. Returns 0.0 for an
/// empty sample, which degrades the interval to the point forecast rather
/// than inventing a spread.
///
/// # The mirror is the whole point
///
/// `⌈(n+1)q⌉/n` is derived for the upper tail, and applying it to both tails
/// moves the *lower* bound up rather than down — it made the correction
/// **narrow** the interval on the low side. At `n = 40` and `q = 0.05` the
/// rank went from 0.05 to 0.075, so a nominal 90 % interval lost coverage
/// below exactly where the conformal argument was supposed to add it. The
/// only test on this asserted the upper tail (`conformal_correction_widens_
/// the_upper_tail`), and the coverage test allowed 0.70 against a nominal
/// 0.90, so neither could see it — a numeric result checked only for a loose
/// bound is uncovered (QUALITY rule 2).
///
/// The consequence was not confined to statistics: forecast-deviation
/// triggers fire on an observation falling outside the interval, so a
/// silently narrow lower bound raises the alert rate below the forecast above
/// its nominal level, for every trigger built on one.
fn empirical_quantile(sorted: &[f64], q: f64, conformal: bool) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }

    let q = if conformal {
        let nf = (n + 1) as f64;
        // The correction moves a *tail* rank outward. The median is not a
        // tail: `q >= 0.5` pushed q = 0.5 to ⌈41·0.5⌉/40 = 0.525 at n = 40,
        // so the level advertised as the median was the 52.5 % quantile and
        // the "median" forecast was biased high by construction.
        let adjusted = if q > 0.5 {
            (nf * q).ceil() / n as f64
        } else if q < 0.5 {
            1.0 - (nf * (1.0 - q)).ceil() / n as f64
        } else {
            0.5
        };
        adjusted.clamp(0.0, 1.0)
    } else {
        q
    };

    let pos = q * (n - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        sorted[lo.min(n - 1)]
    } else {
        let frac = pos - lo as f64;
        let a = sorted[lo.min(n - 1)];
        let b = sorted[hi.min(n - 1)];
        a + (b - a) * frac
    }
}

/// Enforce non-crossing quantiles across levels at each horizon step.
fn enforce_monotonicity(quantiles: &mut [Vec<f64>]) {
    if quantiles.len() < 2 {
        return;
    }
    let horizon = quantiles[0].len();
    for h in 0..horizon {
        for i in 1..quantiles.len() {
            let prev = quantiles[i - 1][h];
            if quantiles[i][h] < prev {
                quantiles[i][h] = prev;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forecast::{HoltWintersModel, SesModel};

    fn seasonal_series(n: usize, period: usize) -> (Vec<i64>, Vec<f64>) {
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 1_000_000_000).collect();
        let vals: Vec<f64> = (0..n)
            .map(|i| {
                let phase = (i % period) as f64 / period as f64 * std::f64::consts::TAU;
                10.0 + 5.0 * phase.sin() + ((i * 37) % 11) as f64 * 0.1
            })
            .collect();
        (ts, vals)
    }

    #[test]
    fn empirical_quantile_matches_type7() {
        let s = [1.0, 2.0, 3.0, 4.0];
        // Type-7 median of 4 points is the midpoint of the middle pair.
        assert!((empirical_quantile(&s, 0.5, false) - 2.5).abs() < 1e-9);
        assert!((empirical_quantile(&s, 0.0, false) - 1.0).abs() < 1e-9);
        assert!((empirical_quantile(&s, 1.0, false) - 4.0).abs() < 1e-9);
    }

    /// The conformal correction is a *tail* correction; the median must be
    /// the plain empirical median. With `sorted[i] = i + 1`, n = 40, the
    /// type-7 median is `(sorted[19] + sorted[20]) / 2 = 20.5`. The old
    /// `q >= 0.5` branch produced rank ⌈41·0.5⌉/40 = 0.525 → 21.475.
    #[test]
    fn conformal_leaves_the_median_alone() {
        let s: Vec<f64> = (1..=40).map(f64::from).collect();
        assert!((empirical_quantile(&s, 0.5, false) - 20.5).abs() < 1e-12);
        assert!(
            (empirical_quantile(&s, 0.5, true) - 20.5).abs() < 1e-12,
            "conformal median = {}, want 20.5",
            empirical_quantile(&s, 0.5, true)
        );
    }

    /// `predict_interval` must clamp the point forecast to the configured
    /// domain, exactly as `predict` does — otherwise a PV forecast prints a
    /// negative point inside a `[0, 5000]` interval.
    #[test]
    fn predict_interval_clamps_the_point_forecast() {
        let ts: Vec<i64> = (0..60).map(|i| i as i64 * 1_000_000_000).collect();
        // A series that trends hard negative so the point forecast leaves
        // the declared domain.
        let vals: Vec<f64> = (0..60).map(|i| 100.0 - 4.0 * i as f64).collect();
        let cfg = QuantileConfig {
            levels: vec![0.1, 0.5, 0.9],
            horizon: 3,
            initial_window: 30,
            step: 1,
            strategy: CalibrationStrategy::OnlineUpdate,
            conformal: true,
            lower_bound: Some(0.0),
            upper_bound: Some(5000.0),
        };
        let mut f = QuantileForecaster::new(
            || Box::new(SesModel::new(Some(0.3))) as Box<dyn ForecastModel>,
            cfg,
        );
        f.fit(&ts, &vals).unwrap();
        let r = f.predict_interval(3, 0.9).unwrap();
        for (i, &v) in r.values.iter().enumerate() {
            assert!(
                (0.0..=5000.0).contains(&v),
                "point forecast {v} at h={} outside the configured [0, 5000]",
                i + 1
            );
        }
    }

    /// The correction must widen **both** tails.
    ///
    /// Asserting only the upper one is how the mirrored form went missing: the
    /// same expression applied to a lower quantile moves it up, and an
    /// interval that is wider above and narrower below still passes an
    /// upper-tail-only test while under-covering.
    #[test]
    fn conformal_correction_widens_both_tails() {
        let s: Vec<f64> = (1..=10).map(f64::from).collect();
        for &q in &[0.9, 0.95, 0.99] {
            let (plain, conf) = (
                empirical_quantile(&s, q, false),
                empirical_quantile(&s, q, true),
            );
            assert!(
                conf >= plain,
                "upper tail q={q} must not tighten: {conf} < {plain}"
            );
        }
        for &q in &[0.1, 0.05, 0.01] {
            let (plain, conf) = (
                empirical_quantile(&s, q, false),
                empirical_quantile(&s, q, true),
            );
            assert!(
                conf <= plain,
                "lower tail q={q} must not tighten: {conf} > {plain}"
            );
        }
        // …and it must stay an interval: the corrected bounds cannot cross.
        assert!(empirical_quantile(&s, 0.05, true) <= empirical_quantile(&s, 0.95, true));
    }

    /// A larger calibration sample needs a smaller correction: the finite-
    /// sample term vanishes as `n` grows, which is what makes it a correction
    /// rather than a fudge factor.
    #[test]
    fn the_conformal_correction_shrinks_as_the_sample_grows() {
        let widen = |n: usize| {
            let s: Vec<f64> = (1..=n).map(|i| i as f64).collect();
            let lo = empirical_quantile(&s, 0.05, false) - empirical_quantile(&s, 0.05, true);
            lo / n as f64
        };
        assert!(
            widen(400) < widen(40),
            "the correction must shrink relative to the sample as n grows"
        );
    }

    #[test]
    fn empty_sample_degrades_to_point_forecast() {
        assert_eq!(empirical_quantile(&[], 0.9, true), 0.0);
    }

    #[test]
    fn calibrates_and_orders_quantiles() {
        let (ts, vals) = seasonal_series(200, 24);
        let cfg = QuantileConfig {
            levels: vec![0.1, 0.5, 0.9],
            horizon: 12,
            initial_window: 100,
            step: 4,
            strategy: CalibrationStrategy::OnlineUpdate,
            conformal: true,
            lower_bound: None,
            upper_bound: None,
        };
        let mut qf = QuantileForecaster::new(
            || {
                Box::new(HoltWintersModel::new(
                    Some(0.3),
                    Some(0.1),
                    Some(0.1),
                    Some(24),
                    false,
                ))
            },
            cfg,
        );
        qf.fit(&ts, &vals).expect("fit");

        assert!(qf.calibration_size(1) > 0, "horizon 1 must be calibrated");

        let f = qf.predict(12).expect("predict");
        assert_eq!(f.horizon(), 12);
        assert_eq!(f.levels.len(), 3);
        for h in 0..12 {
            assert!(
                f.quantiles[0][h] <= f.quantiles[1][h] && f.quantiles[1][h] <= f.quantiles[2][h],
                "quantiles crossed at horizon {h}"
            );
        }
        assert!(f.level(0.9).is_some());
        assert!(f.level(0.42).is_none());

        // Levels derived arithmetically must still be findable.
        let central = QuantileConfig::central(0.9, 4);
        let mut qf2 = QuantileForecaster::new(
            || {
                Box::new(HoltWintersModel::new(
                    Some(0.3),
                    Some(0.1),
                    Some(0.1),
                    Some(24),
                    false,
                ))
            },
            central,
        );
        qf2.fit(&ts, &vals).expect("fit");
        let f2 = qf2.predict(4).expect("predict");
        assert!(
            f2.level(0.05).is_some(),
            "central(0.9) level 0.05 must match"
        );
        assert!(
            f2.level(0.95).is_some(),
            "central(0.9) level 0.95 must match"
        );
    }

    /// The whole point of per-horizon buckets: uncertainty must be allowed to
    /// grow with the horizon instead of being a fixed multiple of one sigma.
    #[test]
    fn interval_width_grows_with_horizon() {
        let ts: Vec<i64> = (0..300).map(|i| i as i64 * 1_000_000_000).collect();
        // Random walk: h-step error variance grows linearly in h.
        let mut v = 0.0f64;
        let mut seed = 12345u64;
        let vals: Vec<f64> = (0..300)
            .map(|_| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let u = ((seed >> 33) as f64 / f64::from(u32::MAX)) - 0.5;
                v += u;
                v
            })
            .collect();

        let cfg = QuantileConfig {
            levels: vec![0.1, 0.9],
            horizon: 20,
            initial_window: 150,
            step: 1,
            strategy: CalibrationStrategy::OnlineUpdate,
            conformal: false,
            lower_bound: None,
            upper_bound: None,
        };
        let mut qf = QuantileForecaster::new(|| Box::new(SesModel::new(Some(0.5))), cfg);
        qf.fit(&ts, &vals).expect("fit");
        let f = qf.predict(20).expect("predict");

        let width = |h: usize| f.quantiles[1][h] - f.quantiles[0][h];
        assert!(
            width(19) > width(0),
            "20-step interval ({}) should exceed 1-step ({})",
            width(19),
            width(0)
        );
    }

    #[test]
    fn rejects_invalid_config() {
        let (ts, vals) = seasonal_series(50, 12);
        let bad = QuantileConfig {
            levels: vec![1.5],
            ..QuantileConfig::default()
        };
        let mut qf = QuantileForecaster::new(|| Box::new(SesModel::new(Some(0.5))), bad);
        assert!(qf.fit(&ts, &vals).is_err());

        let short = QuantileConfig {
            initial_window: 1000,
            ..QuantileConfig::default()
        };
        let mut qf = QuantileForecaster::new(|| Box::new(SesModel::new(Some(0.5))), short);
        assert!(qf.fit(&ts, &vals).is_err());
    }

    #[test]
    fn bounds_are_respected() {
        let (ts, vals) = seasonal_series(200, 24);
        let cfg = QuantileConfig {
            levels: vec![0.05, 0.95],
            horizon: 6,
            initial_window: 100,
            step: 4,
            strategy: CalibrationStrategy::OnlineUpdate,
            conformal: true,
            lower_bound: Some(0.0),
            upper_bound: Some(20.0),
        };
        let mut qf = QuantileForecaster::new(|| Box::new(SesModel::new(Some(0.5))), cfg);
        qf.fit(&ts, &vals).expect("fit");
        let f = qf.predict(6).expect("predict");
        for level in &f.quantiles {
            for &v in level {
                assert!((0.0..=20.0).contains(&v), "bound violated: {v}");
            }
        }

        // predict_interval must clamp too.
        let ci = qf.predict_interval(6, 0.9).expect("interval");
        assert!(ci.confidence_lower.iter().all(|v| *v >= 0.0));
        assert!(ci.confidence_upper.iter().all(|v| *v <= 20.0));
    }

    #[test]
    fn rejects_inverted_bounds() {
        let (ts, vals) = seasonal_series(60, 12);
        let cfg = QuantileConfig::default().with_bounds(Some(10.0), Some(1.0));
        let mut qf = QuantileForecaster::new(|| Box::new(SesModel::new(Some(0.5))), cfg);
        assert!(qf.fit(&ts, &vals).is_err());
    }

    #[test]
    fn predict_before_fit_errors() {
        let qf = QuantileForecaster::new(
            || Box::new(SesModel::new(Some(0.5))),
            QuantileConfig::default(),
        );
        assert!(matches!(qf.predict(4), Err(ForecastError::NotFitted)));
    }

    #[test]
    fn central_config_is_symmetric() {
        let cfg = QuantileConfig::central(0.9, 12);
        assert_eq!(cfg.levels.len(), 3);
        for (got, want) in cfg.levels.iter().zip([0.05, 0.5, 0.95]) {
            assert!((got - want).abs() < 1e-9, "got {got}, want {want}");
        }
        assert_eq!(cfg.horizon, 12);
    }

    /// Empirical intervals must actually cover at roughly the nominal rate on
    /// held-out data — the property Gaussian intervals lose on skewed series.
    #[test]
    fn achieves_approximately_nominal_coverage() {
        let (ts, vals) = seasonal_series(400, 24);
        let split = 300;

        let cfg = QuantileConfig {
            levels: vec![0.05, 0.95],
            horizon: 1,
            initial_window: 150,
            step: 1,
            strategy: CalibrationStrategy::OnlineUpdate,
            conformal: true,
            lower_bound: None,
            upper_bound: None,
        };
        let mut qf = QuantileForecaster::new(
            || {
                Box::new(HoltWintersModel::new(
                    Some(0.3),
                    Some(0.1),
                    Some(0.1),
                    Some(24),
                    false,
                ))
            },
            cfg,
        );
        qf.fit(&ts[..split], &vals[..split]).expect("fit");

        let mut covered = 0;
        let mut total = 0;
        for i in split..vals.len() {
            let f = qf.predict(1).expect("predict");
            let (lo, hi) = (f.quantiles[0][0], f.quantiles[1][0]);
            if vals[i] >= lo && vals[i] <= hi {
                covered += 1;
            }
            total += 1;
            // Not re-calibrating here; just advancing the point forecast.
            qf.model.update(ts[i], vals[i]).expect("update");
        }
        let coverage = f64::from(covered) / f64::from(total);
        // A 90 % interval is asserted near 90 %, not merely above 70 %. The
        // looser bar was satisfied by an interval whose lower bound the
        // conformal correction was actively tightening, which is what let
        // that defect sit under a passing coverage test.
        assert!(
            coverage >= 0.85,
            "90% interval covered only {coverage:.2} of held-out points"
        );
    }
}
