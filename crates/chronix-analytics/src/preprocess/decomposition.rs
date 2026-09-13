//! Seasonal-Trend decomposition using Loess (STL).
//!
//! Splits a time series into **trend**, **seasonal**, and **residual**
//! components:  `y = trend + seasonal + residual`.
//!
//! The implementation follows the Cleveland et al. (1990) STL algorithm:
//!
//! 1. Start with `seasonal = 0`.
//! 2. Detrend: `detrended = y - trend`.
//! 3. Subseries smoothing: extract each season's slice, apply degree-1 Loess
//!    (locally weighted polynomial regression with tricube kernel).
//! 4. Trend update: `y - seasonal`, then Loess with `trend_window`.
//! 5. Repeat for `n_iter` iterations.

use thiserror::Error;

/// Decomposition result: `y[i] = trend[i] + seasonal[i] + residual[i]`.
#[derive(Debug, Clone)]
pub struct Decomposition {
    /// Trend component.
    pub trend: Vec<f64>,
    /// Seasonal component.
    pub seasonal: Vec<f64>,
    /// Residual (remainder) component.
    pub residual: Vec<f64>,
}

/// Trait for seasonal decomposition strategies.
pub trait SeasonalDecomposer: Send + Sync {
    /// Decompose a time series into trend, seasonal, and residual components.
    fn decompose(
        &self,
        timestamps: &[i64],
        values: &[f64],
        period: usize,
    ) -> Result<Decomposition, DecompositionError>;
}

/// STL decomposer implementing [`SeasonalDecomposer`].
pub struct StlDecomposer {
    /// Number of outer-loop iterations.
    pub n_iter: usize,
    /// Trend smoother window length (None = auto).
    pub trend_window: Option<usize>,
}

impl Default for StlDecomposer {
    fn default() -> Self {
        Self {
            n_iter: 2,
            trend_window: None,
        }
    }
}

impl SeasonalDecomposer for StlDecomposer {
    #[tracing::instrument(skip_all, level = "debug")]
    fn decompose(
        &self,
        _timestamps: &[i64],
        values: &[f64],
        period: usize,
    ) -> Result<Decomposition, DecompositionError> {
        let config = StlConfig {
            period,
            n_iter: self.n_iter,
            trend_window: self.trend_window,
            seasonal_window: None,
            low_pass_window: None,
            outer_iter: 0,
        };
        stl_decompose(values, &config)
    }
}

/// Errors that can occur during decomposition.
#[derive(Debug, Error)]
pub enum DecompositionError {
    /// The series is too short for the requested seasonal period.
    #[error("series length ({len}) must be at least 2× the period ({period})")]
    TooShort {
        /// Actual series length.
        len: usize,
        /// Requested seasonal period.
        period: usize,
    },

    /// The seasonal period is invalid (must be ≥ 2).
    #[error("period must be ≥ 2, got {0}")]
    InvalidPeriod(usize),
}

/// Configuration for the STL decomposer.
#[derive(Debug, Clone)]
pub struct StlConfig {
    /// Seasonal period (e.g., 24 for hourly data with daily seasonality).
    pub period: usize,
    /// Number of outer-loop iterations (default: 2).
    pub n_iter: usize,
    /// Trend smoother window length — must be odd, ≥ 3.
    /// Default: `period + 1` (rounded up to next odd).
    pub trend_window: Option<usize>,
    /// Seasonal subseries smoother window length, in **cycles** — must be
    /// odd, ≥ 7. Default: 7, as in Cleveland et al. (1990), R's `stl` and
    /// statsmodels' `STL`.
    pub seasonal_window: Option<usize>,
    /// Low-pass smoother window length — must be odd, ≥ 3.
    /// Default: the smallest odd integer ≥ `period`.
    pub low_pass_window: Option<usize>,
    /// Number of **outer** iterations, which is what makes the fit robust to
    /// outliers. `0` (the default) is the plain inner loop.
    ///
    /// Each outer pass reweights every point by Cleveland's bisquare of its
    /// residual against six times the median absolute residual, so a point
    /// far from the fit stops pulling on the next one. In a metrics database
    /// the case for it is ordinary rather than exotic: a restart, a scrape
    /// that timed out and was backfilled as a spike, a sensor returning its
    /// error sentinel. Without it one such point is spread across the whole
    /// seasonal component by the cycle-subseries smoother — it lands in the
    /// same season of every cycle.
    ///
    /// Cleveland et al. (1990) §4.3 suggests 5–10 when robustness is wanted
    /// and 0 when it is not; statsmodels' `STL(robust=True)` uses 15 with an
    /// early exit, which is the same bargain. Default 0, so the cost is paid
    /// only where it is asked for.
    pub outer_iter: usize,
}

impl StlConfig {
    /// Creates a new STL configuration with the given seasonal period.
    pub fn new(period: usize) -> Self {
        Self {
            period,
            n_iter: 2,
            trend_window: None,
            seasonal_window: None,
            low_pass_window: None,
            outer_iter: 0,
        }
    }

    /// Sets the number of STL iterations.
    pub fn with_iterations(mut self, n: usize) -> Self {
        self.n_iter = n.max(1);
        self
    }

    /// Sets the trend smoother window length.
    pub fn with_trend_window(mut self, w: usize) -> Self {
        self.trend_window = Some(w);
        self
    }

    /// Sets the seasonal subseries smoother window length, in **cycles**.
    pub fn with_seasonal_window(mut self, w: usize) -> Self {
        self.seasonal_window = Some(w);
        self
    }

    /// Sets the low-pass smoother window length.
    pub fn with_low_pass_window(mut self, w: usize) -> Self {
        self.low_pass_window = Some(w);
        self
    }

    /// Makes the fit robust to outliers, with Cleveland's suggested ten
    /// outer iterations.
    pub fn robust(mut self) -> Self {
        self.outer_iter = 10;
        self
    }

    /// Sets the number of outer (robustness) iterations explicitly.
    pub fn with_outer_iterations(mut self, n: usize) -> Self {
        self.outer_iter = n;
        self
    }

    /// Cleveland et al. (1990) §3.4: `n_t` is the smallest odd integer at
    /// least `1.5 * period / (1 - 1.5 / n_s)`. The ratio to `n_s` is the
    /// point — a trend smoother chosen independently of the seasonal one
    /// lets the two compete for the same variation, and the paper's rule is
    /// what keeps the trend from absorbing the seasonal cycle.
    fn effective_trend_window(&self) -> usize {
        let w = self.trend_window.unwrap_or_else(|| {
            let n_s = self.effective_seasonal_window() as f64;
            let raw = 1.5 * self.period as f64 / (1.0 - 1.5 / n_s);
            raw.ceil() as usize
        });
        next_odd_at_least(w, 3)
    }

    /// Cleveland et al. (1990) §3.3: `n_s` is in units of **cycles**, not of
    /// samples — the seasonal smoother runs along a cycle-subseries, which
    /// holds one point per cycle, so a series of `n` points offers it only
    /// `n / period` of them. The paper's fixed default of 7 is what R's
    /// `stl(s.window = "periodic")` and statsmodels' `STL(seasonal = 7)`
    /// both use.
    ///
    /// This used to default to `max(7, period)`, which reads the window in
    /// the *series'* units: for hourly data with a daily cycle it asked for
    /// a 25-point smoother over a subseries holding one point per day, so
    /// every deployment with fewer than `period` cycles of history got a
    /// smoother wider than its own data.
    fn effective_seasonal_window(&self) -> usize {
        next_odd_at_least(self.seasonal_window.unwrap_or(7), 7)
    }

    /// Cleveland et al. (1990) §3.5: `n_l` is the smallest odd integer at
    /// least `period`.
    fn effective_low_pass_window(&self) -> usize {
        next_odd_at_least(self.low_pass_window.unwrap_or(self.period), 3)
    }
}

/// The smallest odd integer that is at least `w` and at least `floor`.
fn next_odd_at_least(w: usize, floor: usize) -> usize {
    let w = w.max(floor);
    if w.is_multiple_of(2) {
        w + 1
    } else {
        w
    }
}

/// Perform STL decomposition.
pub fn stl_decompose(
    values: &[f64],
    config: &StlConfig,
) -> Result<Decomposition, DecompositionError> {
    let n = values.len();
    let period = config.period;

    if period < 2 {
        return Err(DecompositionError::InvalidPeriod(period));
    }
    if n < 2 * period {
        return Err(DecompositionError::TooShort { len: n, period });
    }

    let trend_w = config.effective_trend_window();
    let seasonal_w = config.effective_seasonal_window();
    let low_pass_w = config.effective_low_pass_window();

    let mut seasonal = vec![0.0; n];
    let mut trend = vec![0.0; n];
    // Robustness weights, all 1.0 until an outer pass computes them. `None`
    // is the plain inner loop and costs nothing.
    let mut weights: Option<Vec<f64>> = None;

    // The inner loop of Cleveland et al. (1990), steps 1–6, wrapped in the
    // outer loop of §4.3 when `outer_iter > 0`.
    //
    // Step 2 is the one that carries the algorithm's shape: each
    // cycle-subseries is smoothed *and extended one cycle beyond either
    // end*, so the low-pass filter in step 3 can run in valid mode and
    // still produce a value for every input point. The extension is the
    // loess fit evaluated at −1 and at `len`, which continues the local
    // trend. Padding by repeating the first and last cycle instead — which
    // is what stood here — is correct only for a series that is already
    // periodic: on a ramp the copy meets the original at a discontinuity
    // the moving averages then smear across the first and last period. A
    // straight line, whose seasonal component is exactly zero, came out of
    // it with a seasonal swing of 278 on values running 1_000 to 11_619,
    // and `seasonal_strength` read that as a seasonal series.
    for outer in 0..=config.outer_iter {
        for _ in 0..config.n_iter {
            // Step 1: detrend.
            let detrended: Vec<f64> = values
                .iter()
                .zip(trend.iter())
                .map(|(y, t)| y - t)
                .collect();

            // Step 2: cycle-subseries smoothing, extended by one cycle at
            // each end. `cycle_ext[period + i]` is the smoothed value for
            // `i`. A robustness weight travels with its point into the
            // subseries, which is the whole mechanism: an outlier lands in
            // the same season of one cycle, so down-weighting it there is
            // what stops it being spread across every cycle.
            let mut cycle_ext = vec![0.0; n + 2 * period];
            for s in 0..period {
                let idx: Vec<usize> = (s..n).step_by(period).collect();
                let sub: Vec<f64> = idx.iter().map(|&i| detrended[i]).collect();
                let sub_w = weights
                    .as_ref()
                    .map(|w| idx.iter().map(|&i| w[i]).collect::<Vec<f64>>());
                // `extended[0]` is the fit at −1, `extended[k + 1]` at `k`.
                let extended = loess_extended(&sub, seasonal_w, sub_w.as_deref());
                for (k, &v) in extended.iter().enumerate() {
                    // Subseries position `k - 1` sits at series index
                    // `s + (k - 1) * period`, i.e. `s + k * period` once the
                    // frame is shifted by one cycle.
                    let at = s + k * period;
                    if at < cycle_ext.len() {
                        cycle_ext[at] = v;
                    }
                }
            }

            // Step 3: low-pass filter — MA(period), MA(period), MA(3), then
            // a loess of width `n_l`. Each moving average runs in valid
            // mode, so the three together consume exactly the `2 * period`
            // points the extension added and the result is `n` long by
            // construction.
            let low_pass = low_pass_filter(&cycle_ext, period, low_pass_w);
            debug_assert_eq!(low_pass.len(), n);

            // Step 4: the seasonal is what the low-pass filter removed.
            for i in 0..n {
                seasonal[i] = cycle_ext[period + i] - low_pass[i];
            }

            // Steps 5–6: deseasonalise and smooth the trend.
            let deseasoned: Vec<f64> = values
                .iter()
                .zip(seasonal.iter())
                .map(|(y, s)| y - s)
                .collect();
            trend = loess_smooth_weighted(&deseasoned, trend_w, weights.as_deref());
        }

        // The outer loop of §4.3: reweight by how far each point fell from
        // the fit, then run the inner loop again. Skipped after the last
        // pass, whose weights nothing would use.
        if outer < config.outer_iter {
            weights = Some(robustness_weights(values, &trend, &seasonal));
        }
    }

    // Residual
    let residual: Vec<f64> = values
        .iter()
        .zip(trend.iter())
        .zip(seasonal.iter())
        .map(|((y, t), s)| y - t - s)
        .collect();

    Ok(Decomposition {
        trend,
        seasonal,
        residual,
    })
}

/// Detect the dominant period using autocorrelation peak finding.
///
/// Returns `None` if no clear period is found or the series is too short.
///
/// # ACF Threshold
///
/// The series is **detrended** first, then the autocorrelation is searched
/// for **local maxima** — the two together are what make this a period
/// detector rather than a smoothness detector.
///
/// The minimum ACF peak value is hardcoded to **0.3** here, which suits the
/// pronounced cycles of typical TSDB workloads. Use
/// [`detect_period_with_threshold`] for a noisy signal (a lower threshold
/// trades precision for recall) or a very clean one (a higher threshold
/// rejects incidental peaks).
pub fn detect_period(values: &[f64], max_period: usize) -> Option<usize> {
    detect_period_with_threshold(values, max_period, 0.3)
}

/// Detect the dominant period using autocorrelation peak finding with
/// a caller-specified minimum ACF threshold.
///
/// `min_acf_threshold` controls how strong the autocorrelation peak
/// must be to accept a period:
/// Lower values (e.g. 0.1) improve recall for weak/noisy signals
///   at the cost of more false positives.
/// Higher values (e.g. 0.5) require a strong seasonal pattern,
///   avoiding spurious detections.
///
/// Returns `None` if no clear period is found or the series is too short.
pub fn detect_period_with_threshold(
    values: &[f64],
    max_period: usize,
    min_acf_threshold: f64,
) -> Option<usize> {
    let n = values.len();
    if n < 4 {
        return None;
    }

    // Detrend before correlating. A trended series' autocorrelation is
    // dominated by the trend and decays from lag 1, so the largest value sits
    // at the smallest lag and the seasonal peak is invisible underneath it.
    // Removing the least-squares line leaves the periodic component.
    let residual = detrend(values);

    let mean = residual.iter().sum::<f64>() / n as f64;
    let var: f64 = residual.iter().map(|v| (v - mean).powi(2)).sum();
    if var < f64::EPSILON {
        return None;
    }

    let max_lag = max_period.min(n / 2);
    if max_lag < 3 {
        return None;
    }

    // Biased ACF estimator (divide by the total variance rather than by
    // `n - lag`): it tapers naturally at long lags, which is what stops a
    // harmonic multiple 2P from outscoring the fundamental P.
    let acf: Vec<f64> = (0..=max_lag)
        .map(|lag| {
            let s: f64 = (0..(n - lag))
                .map(|i| (residual[i] - mean) * (residual[i + lag] - mean))
                .sum();
            s / var
        })
        .collect();

    // A period is a **local maximum** of the autocorrelation, not its largest
    // value: a smooth series is most correlated with its immediate neighbour,
    // which is a statement about smoothness rather than seasonality.
    let mut best: Option<(usize, f64)> = None;
    for lag in 2..max_lag {
        let (prev, here, next) = (acf[lag - 1], acf[lag], acf[lag + 1]);
        if here > prev && here >= next && here > min_acf_threshold {
            // Ties go to the shorter lag: the fundamental, not its harmonic.
            if best.is_none_or(|(_, v)| here > v) {
                best = Some((lag, here));
            }
        }
    }

    best.map(|(lag, _)| lag)
}

/// Remove the least-squares linear trend from a series.
fn detrend(values: &[f64]) -> Vec<f64> {
    let n = values.len();
    if n < 2 {
        return values.to_vec();
    }
    let n_f = n as f64;
    let mean_x = (n_f - 1.0) / 2.0;
    let mean_y = values.iter().sum::<f64>() / n_f;
    let mut sxx = 0.0;
    let mut sxy = 0.0;
    for (i, &y) in values.iter().enumerate() {
        let dx = i as f64 - mean_x;
        sxx += dx * dx;
        sxy += dx * (y - mean_y);
    }
    if sxx <= 0.0 {
        return values.iter().map(|v| v - mean_y).collect();
    }
    let slope = sxy / sxx;
    values
        .iter()
        .enumerate()
        .map(|(i, &y)| y - (mean_y + slope * (i as f64 - mean_x)))
        .collect()
}

/// Locally weighted regression (Loess) smoother with tricube
/// kernel (Cleveland 1990).
///
/// Fits a degree-1 (linear) polynomial at each point using weighted
/// least squares.  The tricube kernel `w(u) = (1 - |u|³)³` for `|u| < 1`
/// downweights points far from the center, producing better edge
/// behaviour and outlier resistance than a uniform moving average.
/// The low-pass filter of STL step 3: pad by one cycle at each end with the
/// adjacent cycle, apply a `period`-point centred moving average (a 2×MA for
/// The low-pass filter of STL step 3: `MA(period)`, `MA(period)`, `MA(3)`,
/// then a degree-1 loess of width `n_l`.
///
/// Every moving average runs in **valid** mode — an output point only where
/// the whole window lies inside the input — so the three of them consume
/// exactly `(period - 1) + (period - 1) + 2 = 2 * period` points. That is
/// precisely what the cycle-subseries extension added, which is why the
/// result is `n` long without any edge special-casing. A partial window at
/// the ends, which is what `centered_moving_average` used to supply, is an
/// average of fewer points presented as if it were an average of the whole
/// window.
fn low_pass_filter(cycle_ext: &[f64], period: usize, low_pass_window: usize) -> Vec<f64> {
    let a = moving_average_valid(cycle_ext, period);
    let b = moving_average_valid(&a, period);
    let c = moving_average_valid(&b, 3);
    loess_smooth(&c, low_pass_window)
}

/// A moving average of `w` points, valid mode: the output is
/// `x.len() - w + 1` long and each point averages a full window.
fn moving_average_valid(x: &[f64], w: usize) -> Vec<f64> {
    if w <= 1 {
        return x.to_vec();
    }
    if x.len() < w {
        return Vec::new();
    }
    let wf = w as f64;
    let mut out = Vec::with_capacity(x.len() - w + 1);
    let mut sum: f64 = x[..w].iter().sum();
    out.push(sum / wf);
    for i in w..x.len() {
        sum += x[i] - x[i - w];
        out.push(sum / wf);
    }
    out
}

/// Degree-1 loess evaluated at one point `x`, which may lie outside
/// `0..values.len()`.
///
/// The neighbourhood is the `q = min(window, n)` nearest points and the
/// bandwidth is the distance to the farthest of them, scaled by `window / n`
/// when the window is wider than the data — Cleveland's rule for `q > n`,
/// which is what lets a smoother wider than its series still express a
/// slope instead of collapsing to the mean.
fn loess_at(values: &[f64], window: usize, x: f64, robust: Option<&[f64]>) -> f64 {
    let n = values.len();
    if n == 0 {
        return f64::NAN;
    }
    if n == 1 {
        return values[0];
    }
    let q = window.max(2).min(n);
    // On a regular grid the `q` nearest points are contiguous.
    let lo = (x - (q as f64 - 1.0) / 2.0)
        .round()
        .clamp(0.0, (n - q) as f64) as usize;
    let hi = lo + q - 1;

    let mut lambda = ((lo as f64 - x).abs()).max((hi as f64 - x).abs());
    if window > n {
        lambda *= window as f64 / n as f64;
    }

    let (mut sw, mut swx, mut swy, mut swxx, mut swxy) = (0.0, 0.0, 0.0, 0.0, 0.0);
    for j in lo..=hi {
        let d = (j as f64 - x).abs();
        let mut w = if lambda <= 0.0 {
            f64::from(u8::from(d == 0.0))
        } else {
            let u = (d / lambda).min(1.0);
            let t = 1.0 - u * u * u;
            t * t * t
        };
        // The robustness weight multiplies the neighbourhood weight, which
        // is exactly how Cleveland et al. (1990) §4.3 fold the outer loop
        // into the same smoother rather than adding a second one.
        if let Some(rw) = robust {
            w *= rw[j];
        }
        if w <= 0.0 {
            continue;
        }
        let (xj, yj) = (j as f64, values[j]);
        sw += w;
        swx += w * xj;
        swy += w * yj;
        swxx += w * xj * xj;
        swxy += w * xj * yj;
    }
    if sw <= 0.0 {
        // Every weight in the neighbourhood vanished. With robustness
        // weights that is a real situation — a run of points next to a spike
        // can all be down-weighted at once — and the answer must not be the
        // raw observation: at the outlier itself that returns the outlier,
        // which is the least robust value available and made the fit
        // oscillate between absorbing the spike and rejecting it. The
        // unweighted fit is the best estimate that remains.
        if robust.is_some() {
            return loess_at(values, window, x, None);
        }
        return values[(x.round().clamp(0.0, (n - 1) as f64)) as usize];
    }
    let det = sw * swxx - swx * swx;
    if det.abs() < 1e-12 * sw * sw.max(1.0) {
        // A single distinct abscissa carries no slope; the weighted mean is
        // the degree-0 fit, which is the right answer only here.
        return swy / sw;
    }
    let a = (swxx * swy - swx * swxy) / det;
    let b = (sw * swxy - swx * swy) / det;
    a + b * x
}

/// Degree-1 loess at every point of `values`.
fn loess_smooth(values: &[f64], window: usize) -> Vec<f64> {
    loess_smooth_weighted(values, window, None)
}

/// [`loess_smooth`] with an additional per-point robustness weight.
fn loess_smooth_weighted(values: &[f64], window: usize, robust: Option<&[f64]>) -> Vec<f64> {
    if window == 0 {
        return values.to_vec();
    }
    (0..values.len())
        .map(|i| loess_at(values, window, i as f64, robust))
        .collect()
}

/// Cleveland's bisquare weights: `(1 - (r / 6m)^2)^2` clamped to `[0, 1]`,
/// where `r` is a point's absolute residual and `m` their median.
///
/// Six times the median absolute residual is the paper's cut-off, and it is
/// the reason a single spike cannot survive an outer pass: its weight goes to
/// zero while an ordinary point's stays near one.
///
/// The cut-off is **floored at a thousandth of the series' range**, which the
/// paper does not need to say and an implementation does. `6m` describes the
/// spread of the residuals and means nothing when they have none: a series
/// the inner loop fits almost exactly leaves rounding error, so `m` is
/// rounding error too and everything above it counts as an outlier — on a
/// clean sine with one spike, a median of 0.0047 against a 99th percentile of
/// 47 gave 101 of 240 points weight zero.
///
/// The floor asks what the median cannot: is this residual large *relative to
/// the series*? It sits far below any real noise level, so `6m` stays in
/// charge wherever the residuals carry noise — measured at noise 0.5, 2 and 5
/// on a series of range 260, where it never binds.
fn robustness_weights(values: &[f64], trend: &[f64], seasonal: &[f64]) -> Vec<f64> {
    let resid: Vec<f64> = values
        .iter()
        .zip(trend)
        .zip(seasonal)
        .map(|((y, t), s)| (y - t - s).abs())
        .collect();

    let mut sorted = resid.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    let median = if sorted.is_empty() {
        0.0
    } else if sorted.len().is_multiple_of(2) {
        f64::midpoint(sorted[mid - 1], sorted[mid])
    } else {
        sorted[mid]
    };

    let (lo, hi) = values
        .iter()
        .fold((f64::MAX, f64::MIN), |(l, h), &v| (l.min(v), h.max(v)));
    let scale_floor = if hi > lo { (hi - lo) * 1e-3 } else { 0.0 };
    let cutoff = (6.0 * median).max(scale_floor);
    if cutoff <= 0.0 {
        return vec![1.0; resid.len()];
    }
    resid
        .iter()
        .map(|r| {
            let u = (r / cutoff).min(1.0);
            let t = 1.0 - u * u;
            t * t
        })
        .collect()
}

/// Degree-1 loess at `-1 ..= values.len()`, so the result is `len + 2` long.
///
/// STL step 2 needs the smoother to say what the cycle before the first and
/// the cycle after the last would have been; a loess can answer that because
/// it is a local *fit*, and evaluating it one step outside its data is the
/// extension Cleveland et al. (1990) specify.
fn loess_extended(values: &[f64], window: usize, robust: Option<&[f64]>) -> Vec<f64> {
    let n = values.len();
    (0..n + 2)
        .map(|k| loess_at(values, window, k as f64 - 1.0, robust))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() < eps
    }

    /// `y = 0.5·i + 10·sin(2πi/12)`: the seasonal component must *be* the
    /// sine and the trend must be the line. The previous version of this
    /// test asserted only that the three components summed to the input,
    /// which the residual guarantees by definition — and the seasonal
    /// component it accepted was identically zero.
    #[test]
    fn stl_recovers_the_trend_and_the_seasonal_pattern() {
        let n = 120;
        let period = 12;
        let seasonal_of = |i: usize| 10.0 * (2.0 * std::f64::consts::PI * i as f64 / 12.0).sin();
        let values: Vec<f64> = (0..n).map(|i| 0.5 * i as f64 + seasonal_of(i)).collect();

        let dec = stl_decompose(&values, &StlConfig::new(period).with_iterations(5)).unwrap();

        let amplitude = dec.seasonal.iter().copied().fold(f64::MIN, f64::max)
            - dec.seasonal.iter().copied().fold(f64::MAX, f64::min);
        assert!(
            amplitude > 18.0,
            "seasonal amplitude collapsed: {amplitude}"
        );

        // Away from the ends the recovery is tight; the ends see a shorter
        // smoother window and are allowed a little more.
        for i in period..n - period {
            assert!(
                approx(dec.seasonal[i], seasonal_of(i), 0.6),
                "seasonal at {i}: {} vs {}",
                dec.seasonal[i],
                seasonal_of(i)
            );
            assert!(
                approx(dec.trend[i], 0.5 * i as f64, 0.6),
                "trend at {i}: {} vs {}",
                dec.trend[i],
                0.5 * i as f64
            );
            assert!(
                dec.residual[i].abs() < 0.6,
                "residual at {i}: {}",
                dec.residual[i]
            );
        }
        for i in 0..n {
            let recon = dec.trend[i] + dec.seasonal[i] + dec.residual[i];
            assert!(approx(recon, values[i], 1e-9));
        }
    }

    /// An even period is centred with the classic 2×m moving average.
    #[test]
    fn stl_handles_an_even_period_without_bias() {
        let n = 96;
        let period = 24;
        let seasonal_of = |i: usize| 5.0 * (2.0 * std::f64::consts::PI * i as f64 / 24.0).sin();
        let values: Vec<f64> = (0..n).map(|i| 100.0 + seasonal_of(i)).collect();

        let dec = stl_decompose(&values, &StlConfig::new(period).with_iterations(3)).unwrap();
        for i in period..n - period {
            assert!(
                approx(dec.seasonal[i], seasonal_of(i), 0.4),
                "seasonal at {i}"
            );
            assert!(
                approx(dec.trend[i], 100.0, 0.4),
                "trend at {i}: {}",
                dec.trend[i]
            );
        }
        let max_residual = dec.residual.iter().map(|r| r.abs()).fold(0.0_f64, f64::max);
        assert!(max_residual < 0.8, "residual too large: {max_residual}");
    }

    /// A straight line has no seasonal component, and STL has to say so.
    ///
    /// This is the cheapest test in the file and the one that would have
    /// caught the padding defect: the old low-pass filter padded by copying
    /// the first and last cycle, which on a ramp butts the copy against the
    /// original at a discontinuity, and the moving averages smeared it into
    /// the first and last period. The seasonal component of `1000 + 37 i`
    /// swung by 278.
    ///
    /// A monotonic counter is the commonest series in a metrics database, so
    /// this is not a corner: `seasonal_strength` read that swing as 0.81
    /// against a 0.64 threshold and auto-ARIMA seasonally differenced every
    /// counter it was shown.
    #[test]
    fn a_straight_line_has_no_seasonal_component() {
        for period in [4usize, 7, 12, 24] {
            for cycles in [2usize, 3, 7, 12, 30] {
                let n = period * cycles;
                let y: Vec<f64> = (0..n).map(|i| 1000.0 + 37.0 * i as f64).collect();
                let d = stl_decompose(&y, &StlConfig::new(period)).unwrap();
                let swing = d.seasonal.iter().fold(0.0f64, |a, b| a.max(b.abs()));
                assert!(
                    swing < 1e-6,
                    "period {period}, {cycles} cycles: seasonal swing {swing} on a straight line"
                );
                let resid = d.residual.iter().fold(0.0f64, |a, b| a.max(b.abs()));
                assert!(
                    resid < 1e-6,
                    "period {period}, {cycles} cycles: residual {resid}"
                );
            }
        }
    }

    /// The seasonal amplitude that comes out is the one that went in.
    ///
    /// Checked across the number of *cycles*, which is the axis that broke:
    /// the seasonal smoother runs along a cycle-subseries holding one point
    /// per cycle, and its window used to default to `max(7, period)` — read
    /// in the series' units rather than in cycles. Seven days of hourly data
    /// with a daily cycle therefore asked for a 25-point smoother over a
    /// 7-point subseries, whereupon `loess_smooth` returned the subseries
    /// mean and 22 % of the amplitude went into the trend.
    ///
    /// The values match statsmodels' `STL` to three decimals on every row of
    /// this table; where a genuine edge effect remains — `period = 7` over
    /// 10 cycles — statsmodels reports the same 11.699.
    #[test]
    fn stl_recovers_the_seasonal_amplitude_from_few_cycles() {
        use std::f64::consts::TAU;
        for &(period, n, expected) in &[
            (12usize, 96usize, 12.000f64),
            (12, 240, 12.000),
            (24, 168, 12.000),
            (24, 720, 12.000),
            (7, 70, 11.699),
        ] {
            let y: Vec<f64> = (0..n)
                .map(|i| 100.0 + 0.7 * i as f64 + 12.0 * (i as f64 * TAU / period as f64).sin())
                .collect();
            let d = stl_decompose(&y, &StlConfig::new(period)).unwrap();
            let mid = &d.seasonal[n / 4..3 * n / 4];
            let amp = (mid.iter().copied().fold(f64::MIN, f64::max)
                - mid.iter().copied().fold(f64::MAX, f64::min))
                / 2.0;
            assert!(
                (amp - expected).abs() < 0.01,
                "period {period}, n {n}: amplitude {amp:.4}, expected {expected}"
            );
        }
    }

    /// A seasonal pattern whose amplitude grows is the reason to run STL
    /// rather than subtract per-season means, so it is asserted rather than
    /// assumed.
    #[test]
    fn stl_follows_a_seasonal_pattern_that_changes() {
        use std::f64::consts::TAU;
        let (period, n) = (12usize, 240usize);
        let y: Vec<f64> = (0..n)
            .map(|i| {
                let amp = 5.0 + 15.0 * i as f64 / n as f64;
                100.0 + amp * (i as f64 * TAU / period as f64).sin()
            })
            .collect();
        let d = stl_decompose(&y, &StlConfig::new(period)).unwrap();
        let amp_of = |s: &[f64]| {
            (s.iter().copied().fold(f64::MIN, f64::max)
                - s.iter().copied().fold(f64::MAX, f64::min))
                / 2.0
        };
        let early = amp_of(&d.seasonal[12..36]);
        let late = amp_of(&d.seasonal[n - 36..n - 12]);
        assert!(
            (early - 6.5).abs() < 0.6,
            "early amplitude {early}, expected ~6.5"
        );
        assert!(
            (late - 18.5).abs() < 0.6,
            "late amplitude {late}, expected ~18.5"
        );
        assert!(
            late > early * 2.0,
            "the growth was not followed: {early} → {late}"
        );
    }

    /// An outlier is left in the residual instead of being spread across the
    /// seasonal component.
    ///
    /// This is the case for having the outer loop at all, and it is ordinary
    /// rather than exotic in a metrics database: a restart, a scrape that
    /// timed out and was backfilled as one enormous sample, a sensor
    /// returning its error sentinel. Without robustness weights the
    /// cycle-subseries smoother spreads that one point across **every** cycle
    /// — it lands in the same season of each — and the seasonal amplitude of
    /// a 10-unit sine came back as 33.7.
    ///
    /// Checked at three noise levels and at none, because the two regimes
    /// exercise different halves of the weighting: with noise the median
    /// absolute residual is a real scale and Cleveland's `6m` governs; with
    /// none it is rounding error and the floor in `robustness_weights` does.
    #[test]
    fn a_robust_fit_leaves_a_spike_in_the_residual() {
        use std::f64::consts::TAU;
        let (period, n) = (12usize, 240usize);
        let mut seed = 4u64;
        let mut gauss = || {
            let mut u = || {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                ((seed >> 11) as f64 / (1u64 << 53) as f64).max(1e-12)
            };
            let (a, b) = (u(), u());
            (-2.0 * a.ln()).sqrt() * (TAU * b).cos()
        };

        for noise in [0.0f64, 0.5, 2.0, 5.0] {
            let clean: Vec<f64> = (0..n)
                .map(|i| {
                    100.0
                        + 0.5 * i as f64
                        + 10.0 * (i as f64 * TAU / period as f64).sin()
                        + gauss() * noise
                })
                .collect();
            let base = stl_decompose(&clean, &StlConfig::new(period)).unwrap();

            let mut spiked = clean.clone();
            spiked[120] += 200.0;
            let plain = stl_decompose(&spiked, &StlConfig::new(period)).unwrap();
            let robust = stl_decompose(&spiked, &StlConfig::new(period).robust()).unwrap();

            let deviation = |d: &Decomposition| {
                (0..n)
                    .filter(|&i| i != 120)
                    .map(|i| (d.seasonal[i] - base.seasonal[i]).abs())
                    .fold(0.0f64, f64::max)
            };
            let (p, r) = (deviation(&plain), deviation(&robust));
            assert!(
                r < p / 4.0,
                "noise {noise}: robust deviation {r:.3} is not much better than plain {p:.3}"
            );
            // The spike belongs to the residual, which is where an
            // unexplained observation goes.
            assert!(
                robust.residual[120] > 150.0,
                "noise {noise}: the spike was absorbed — residual {:.3} of 200",
                robust.residual[120]
            );
        }
    }

    /// With no outliers to down-weight, the robust fit is the plain fit.
    ///
    /// Not a formality: the first version of the weighting zeroed 101 of 240
    /// points on a series it fitted exactly, because the median absolute
    /// residual was rounding error and everything above it counted as an
    /// outlier, and the seasonal component moved by 3.7 on an amplitude of
    /// 10.
    ///
    /// The tolerance is not zero because the outer loop runs the inner loop
    /// eleven times rather than once, so an all-ones weighting still
    /// converges a little further; the difference is at 1e-13 on values near
    /// 200, which is the arithmetic and not the weights.
    #[test]
    fn robustness_changes_nothing_without_outliers() {
        use std::f64::consts::TAU;
        let (period, n) = (12usize, 240usize);
        let y: Vec<f64> = (0..n)
            .map(|i| 100.0 + 0.5 * i as f64 + 10.0 * (i as f64 * TAU / period as f64).sin())
            .collect();
        let plain = stl_decompose(&y, &StlConfig::new(period)).unwrap();
        let robust = stl_decompose(&y, &StlConfig::new(period).robust()).unwrap();
        let worst = (0..n)
            .map(|i| (plain.seasonal[i] - robust.seasonal[i]).abs())
            .fold(0.0f64, f64::max);
        assert!(
            worst < 1e-9,
            "robustness moved the seasonal component by {worst:e} with nothing to down-weight"
        );
    }

    #[test]
    fn test_stl_sum_equals_original() {
        let n = 200;
        let period = 10;
        let values: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.1).sin() + 0.01 * i as f64)
            .collect();

        let dec = stl_decompose(&values, &StlConfig::new(period)).unwrap();
        for i in 0..n {
            let recon = dec.trend[i] + dec.seasonal[i] + dec.residual[i];
            assert!(approx(recon, values[i], 1e-12));
        }
    }

    #[test]
    fn test_stl_too_short() {
        let values = vec![1.0; 10];
        let err = stl_decompose(&values, &StlConfig::new(8)).unwrap_err();
        assert!(matches!(err, DecompositionError::TooShort { .. }));
    }

    #[test]
    fn test_stl_invalid_period() {
        let values = vec![1.0; 100];
        let err = stl_decompose(&values, &StlConfig::new(1)).unwrap_err();
        assert!(matches!(err, DecompositionError::InvalidPeriod(1)));
    }

    #[test]
    fn test_detect_period_sinusoidal() {
        let n = 240;
        let true_period = 24;
        let values: Vec<f64> = (0..n)
            .map(|i| (2.0 * std::f64::consts::PI * i as f64 / true_period as f64).sin())
            .collect();

        let detected = detect_period(&values, 48);
        assert_eq!(detected, Some(true_period));
    }

    #[test]
    fn test_detect_period_constant_returns_none() {
        let values = vec![42.0; 100];
        assert_eq!(detect_period(&values, 50), None);
    }

    #[test]
    fn test_loess_smooth_identity() {
        // Single-point bandwidth: each point is its own neighborhood → output ≈ input
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let smoothed = loess_smooth(&v, 1);
        for (a, b) in smoothed.iter().zip(v.iter()) {
            assert!(approx(*a, *b, 1e-9));
        }
    }

    #[test]
    fn test_loess_smooth_smoothing() {
        // With a wider bandwidth the output should be smoother
        let v = vec![1.0, 3.0, 5.0, 7.0, 9.0];
        let smoothed = loess_smooth(&v, 3);
        // For a perfectly linear sequence, Loess (degree 1) should recover
        // the exact values regardless of bandwidth.
        for (a, b) in smoothed.iter().zip(v.iter()) {
            assert!(approx(*a, *b, 1e-6));
        }
    }

    #[test]
    fn test_seasonal_decomposer_trait() {
        let decomposer = StlDecomposer::default();
        let n = 200;
        let period = 10;
        let timestamps: Vec<i64> = (0..n as i64).collect();
        let values: Vec<f64> = (0..n)
            .map(|i| (i as f64 * 0.1).sin() + 0.01 * i as f64)
            .collect();

        let dec = decomposer.decompose(&timestamps, &values, period).unwrap();
        assert_eq!(dec.trend.len(), n);
        assert_eq!(dec.seasonal.len(), n);
        assert_eq!(dec.residual.len(), n);
        // Components should sum to original
        for i in 0..n {
            let recon = dec.trend[i] + dec.seasonal[i] + dec.residual[i];
            assert!(approx(recon, values[i], 1e-12));
        }
    }

    #[test]
    fn test_seasonal_decomposer_trait_too_short() {
        let decomposer = StlDecomposer::default();
        let err = decomposer.decompose(&[0, 1], &[1.0, 2.0], 5).unwrap_err();
        assert!(matches!(err, DecompositionError::TooShort { .. }));
    }

    #[test]
    fn test_detect_period_with_threshold_controls_sensitivity() {
        // Build a noisy signal with a buried periodic component.
        // Signal amplitude tuned so ACF peak falls between the two
        // thresholds (0.1 and 0.95).
        let n = 120;
        let true_period = 24;
        let values: Vec<f64> = (0..n)
            .map(|i: usize| {
                let t = i as f64;
                let signal = (2.0 * std::f64::consts::PI * t / true_period as f64).sin();
                // splitmix64-style deterministic hash → uniform in [-1, 1]
                let mut h = i as u64;
                h = h.wrapping_mul(0x9E3779B97F4A7C15);
                h = (h ^ (h >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
                h = (h ^ (h >> 27)).wrapping_mul(0x94D049BB133111EB);
                h = h ^ (h >> 31);
                let noise = (h % 10000) as f64 / 5000.0 - 1.0;
                signal + noise * 3.0
            })
            .collect();

        // With a low threshold the weak period should be detected.
        let detected_low = detect_period_with_threshold(&values, 48, 0.1);
        assert!(
            detected_low.is_some(),
            "low threshold (0.1) should detect the weak periodicity",
        );

        // With a high threshold the same signal should be rejected.
        let detected_high = detect_period_with_threshold(&values, 48, 0.95);
        assert_eq!(
            detected_high, None,
            "high threshold (0.95) should reject the weak periodicity",
        );
    }

    /// Why the series is detrended first: the autocorrelation of
    /// `200 + 0.5·t + 40·sin(2πt/24)` is dominated by the trend and decays
    /// from lag 1, so its largest value is 2 — smoothness, not seasonality.
    #[test]
    fn detect_period_sees_through_a_trend() {
        let n = 300;
        let period = 24;
        let values: Vec<f64> = (0..n)
            .map(|i| {
                let t = i as f64;
                200.0 + t * 0.5 + 40.0 * (2.0 * std::f64::consts::PI * t / period as f64).sin()
            })
            .collect();
        assert_eq!(detect_period(&values, 48), Some(period));
    }

    /// A smooth series with no cycle must report no period rather than the
    /// shortest lag it can find.
    #[test]
    fn detect_period_rejects_a_pure_trend() {
        let values: Vec<f64> = (0..200).map(|i| 10.0 + i as f64 * 0.3).collect();
        assert_eq!(detect_period(&values, 48), None);
    }

    /// The fundamental wins over its harmonics: a period-12 signal must not
    /// report 24, 36 or 48.
    #[test]
    fn detect_period_prefers_the_fundamental_over_a_harmonic() {
        let n = 480;
        let values: Vec<f64> = (0..n)
            .map(|i| {
                let t = i as f64;
                (2.0 * std::f64::consts::PI * t / 12.0).sin()
                    + 0.4 * (2.0 * std::f64::consts::PI * t / 6.0).sin()
            })
            .collect();
        assert_eq!(detect_period(&values, 60), Some(12));
    }

    #[test]
    fn test_detect_period_delegates_to_with_threshold() {
        // Verify that detect_period(v, m) == detect_period_with_threshold(v, m, 0.3).
        let n = 240;
        let values: Vec<f64> = (0..n)
            .map(|i| (2.0 * std::f64::consts::PI * i as f64 / 24.0).sin())
            .collect();

        assert_eq!(
            detect_period(&values, 48),
            detect_period_with_threshold(&values, 48, 0.3),
        );
    }
}
