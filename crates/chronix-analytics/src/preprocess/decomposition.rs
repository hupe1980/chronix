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
    /// Seasonal subseries smoother window length — must be odd, ≥ 3.
    /// Default: `max(7, period)` (rounded up to next odd).
    /// R's STL defaults to at least 7; the previous hardcoded value of 3
    /// was too small and allowed noise into the seasonal component.
    pub seasonal_window: Option<usize>,
}

impl StlConfig {
    /// Creates a new STL configuration with the given seasonal period.
    pub fn new(period: usize) -> Self {
        Self {
            period,
            n_iter: 2,
            trend_window: None,
            seasonal_window: None,
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

    /// Sets the seasonal subseries smoother window length.
    pub fn with_seasonal_window(mut self, w: usize) -> Self {
        self.seasonal_window = Some(w);
        self
    }

    fn effective_trend_window(&self) -> usize {
        let w = self.trend_window.unwrap_or(self.period + 1);
        // must be odd and ≥ 3
        let w = w.max(3);
        if w.is_multiple_of(2) {
            w + 1
        } else {
            w
        }
    }

    fn effective_seasonal_window(&self) -> usize {
        let w = self.seasonal_window.unwrap_or(self.period.max(7));
        let w = w.max(3);
        if w.is_multiple_of(2) {
            w + 1
        } else {
            w
        }
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

    let mut seasonal = vec![0.0; n];
    let mut trend = vec![0.0; n];

    // The inner loop of Cleveland et al. (1990), steps 1–6, without the
    // robustness weights. The step that matters is the low-pass filter: the
    // cycle-subseries smoother produces a nearly periodic series `C`, and
    // the seasonal component is `C` minus the *low-frequency* part of `C`,
    // so that any drift the subseries smoother absorbed goes back to the
    // trend. What stood here before centred each subseries on its own mean
    // across cycles — which is zero for exactly the series STL exists for,
    // a stable seasonal pattern — and printed a seasonal component of 1e-16
    // for a clean 24-period sine.
    for _ in 0..config.n_iter {
        // Step 1: detrend.
        let detrended: Vec<f64> = values
            .iter()
            .zip(trend.iter())
            .map(|(y, t)| y - t)
            .collect();

        // Step 2: cycle-subseries smoothing. Each season offset is smoothed
        // across its cycles, and the result is scattered back in place.
        let mut cycle = vec![0.0; n];
        for s in 0..period {
            let indices: Vec<usize> = (s..n).step_by(period).collect();
            let sub: Vec<f64> = indices.iter().map(|&i| detrended[i]).collect();
            let smoothed = loess_smooth(&sub, seasonal_w);
            for (&i, &v) in indices.iter().zip(smoothed.iter()) {
                cycle[i] = v;
            }
        }

        // Step 3: low-pass filter of the cycle-subseries — a centred moving
        // average of one period (two, for an even period, to stay centred)
        // and one of three. The full algorithm extends each subseries by a
        // cycle at either end before filtering; padding with the adjacent
        // cycle is the same idea for a series that is nearly periodic.
        let low_pass = low_pass_filter(&cycle, period);

        // Step 4: the seasonal is what the low-pass filter removed.
        for i in 0..n {
            seasonal[i] = cycle[i] - low_pass[i];
        }

        // Steps 5–6: deseasonalise and smooth the trend.
        let deseasoned: Vec<f64> = values
            .iter()
            .zip(seasonal.iter())
            .map(|(y, s)| y - s)
            .collect();
        trend = loess_smooth(&deseasoned, trend_w);
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
/// an even period), then a 3-point one, and cut the padding off again.
fn low_pass_filter(cycle: &[f64], period: usize) -> Vec<f64> {
    let n = cycle.len();
    let mut padded = Vec::with_capacity(n + 2 * period);
    padded.extend_from_slice(&cycle[..period]);
    padded.extend_from_slice(cycle);
    padded.extend_from_slice(&cycle[n - period..]);

    let mut filtered = centered_moving_average(&padded, period);
    if period.is_multiple_of(2) {
        filtered = centered_moving_average(&filtered, 2);
    }
    let filtered = centered_moving_average(&filtered, 3);
    filtered[period..period + n].to_vec()
}

/// A centred moving average of `w` points. For an even `w` the window is
/// `[i - w/2, i + w/2 - 1]`, so two passes (`w`, then `2`) give the classic
/// `2×m`-MA; the window shrinks symmetrically at the ends.
fn centered_moving_average(x: &[f64], w: usize) -> Vec<f64> {
    let n = x.len();
    if w <= 1 || n == 0 {
        return x.to_vec();
    }
    let before = w / 2;
    let after = if w.is_multiple_of(2) {
        w / 2 - 1
    } else {
        w / 2
    };
    (0..n)
        .map(|i| {
            let lo = i.saturating_sub(before);
            let hi = (i + after).min(n - 1);
            let slice = &x[lo..=hi];
            slice.iter().sum::<f64>() / slice.len() as f64
        })
        .collect()
}

fn loess_smooth(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    if window == 0 {
        return values.to_vec();
    }
    if window >= n {
        let mean = values.iter().sum::<f64>() / n as f64;
        return vec![mean; n];
    }
    let half = window / 2;
    let mut out = Vec::with_capacity(n);

    for i in 0..n {
        let lo = i.saturating_sub(half);
        let hi = (i + half).min(n - 1);
        let max_dist = (half.max(1)) as f64;

        let (mut sw, mut swx, mut swy, mut swxx, mut swxy) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for j in lo..=hi {
            let u = ((j as f64 - i as f64) / max_dist).abs().min(1.0);
            let u3 = u * u * u;
            let w = (1.0 - u3) * (1.0 - u3) * (1.0 - u3); // tricube
            let x = j as f64;
            let y = values[j];
            sw += w;
            swx += w * x;
            swy += w * y;
            swxx += w * x * x;
            swxy += w * x * y;
        }
        let det = sw * swxx - swx * swx;
        let fitted = if det.abs() < 1e-15 {
            if sw > 0.0 {
                swy / sw
            } else {
                values[i]
            }
        } else {
            let a = (swxx * swy - swx * swxy) / det;
            let b = (sw * swxy - swx * swy) / det;
            a + b * i as f64
        };
        out.push(fitted);
    }
    out
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------
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
