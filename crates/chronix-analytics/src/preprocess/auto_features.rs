//! Automatic feature generation for time-series data.
//!
//! Produces a [`FeatureMatrix`] containing named feature columns derived from
//! raw timestamps and values:
//!
//! - **Temporal:** hour_of_day, day_of_week, day_of_month, month_of_year, is_weekend
//! - **Statistical:** rolling_mean, rolling_std, rolling_min, rolling_max at
//!   configurable windows (default: 60, 360, 1440, 10080 — representing
//!   1 h, 6 h, 24 h, 7 d when the interval is 1 minute).
//! - **Lag-based:** lag_1, lag_2, lag_period (requires known period)
//!
//! Optional variance-threshold pruning removes near-constant features.

use crate::preprocess::features;

/// Named feature matrix: one row per observation, one column per feature.
#[derive(Debug, Clone)]
pub struct FeatureMatrix {
    /// Feature names (column headers).
    pub names: Vec<String>,
    /// Row-major matrix: `data[row * n_features + col]`.
    pub data: Vec<f64>,
    /// Number of rows.
    pub n_rows: usize,
}

impl FeatureMatrix {
    /// Number of features (columns).
    #[inline]
    pub fn n_features(&self) -> usize {
        self.names.len()
    }

    /// Get a feature column by index (returns a `Vec<f64>` copy).
    pub fn column(&self, idx: usize) -> Vec<f64> {
        let nf = self.n_features();
        (0..self.n_rows).map(|r| self.data[r * nf + idx]).collect()
    }

    /// Get a feature column by name.
    pub fn column_by_name(&self, name: &str) -> Option<Vec<f64>> {
        let idx = self.names.iter().position(|n| n == name)?;
        Some(self.column(idx))
    }
}

/// Configuration for auto-feature generation.
#[derive(Debug, Clone)]
pub struct AutoFeatureConfig {
    /// Known seasonal period (in number of observations).
    pub period: Option<usize>,
    /// Rolling window sizes (number of observations).  Default: `[60, 360, 1440, 10080]`.
    pub rolling_windows: Vec<usize>,
    /// Variance threshold for pruning.  `None` → keep all features.
    pub variance_threshold: Option<f64>,
    /// Whether to generate temporal features from nanosecond timestamps.
    pub temporal_features: bool,
}

impl Default for AutoFeatureConfig {
    fn default() -> Self {
        Self {
            period: None,
            rolling_windows: vec![60, 360, 1440, 10080],
            variance_threshold: None,
            temporal_features: true,
        }
    }
}

impl AutoFeatureConfig {
    /// Sets the seasonal period (in number of observations).
    pub fn with_period(mut self, period: usize) -> Self {
        self.period = Some(period);
        self
    }

    /// Overrides the rolling window sizes.
    pub fn with_windows(mut self, windows: Vec<usize>) -> Self {
        self.rolling_windows = windows;
        self
    }

    /// Sets the variance threshold for pruning near-constant features.
    pub fn with_variance_threshold(mut self, threshold: f64) -> Self {
        self.variance_threshold = Some(threshold);
        self
    }
}

/// Generate features from timestamps (nanosecond epoch) and values.
pub fn auto_features(
    timestamps_ns: &[i64],
    values: &[f64],
    config: &AutoFeatureConfig,
) -> FeatureMatrix {
    let n = timestamps_ns.len().min(values.len());
    let mut names: Vec<String> = Vec::new();
    let mut columns: Vec<Vec<f64>> = Vec::new();

    // --- Temporal features ---
    if config.temporal_features {
        let mut hour = Vec::with_capacity(n);
        let mut dow = Vec::with_capacity(n);
        let mut dom = Vec::with_capacity(n);
        let mut month = Vec::with_capacity(n);
        let mut weekend = Vec::with_capacity(n);

        for &ts in timestamps_ns.iter().take(n) {
            let secs = ts / 1_000_000_000;
            let (h, d_of_w, d_of_m, m_of_y) = decompose_epoch_secs(secs);
            hour.push(h as f64);
            dow.push(d_of_w as f64);
            dom.push(d_of_m as f64);
            month.push(m_of_y as f64);
            weekend.push(if d_of_w >= 5 { 1.0 } else { 0.0 });
        }

        names.extend([
            "hour_of_day".into(),
            "day_of_week".into(),
            "day_of_month".into(),
            "month_of_year".into(),
            "is_weekend".into(),
        ]);
        columns.extend([hour, dow, dom, month, weekend]);
    }

    // --- Statistical features: rolling windows ---
    let vals = &values[..n];
    for &w in &config.rolling_windows {
        if w == 0 || w > n {
            continue;
        }
        // rolling mean
        let rmean = rolling_mean(vals, w);
        names.push(format!("rolling_mean_{w}"));
        columns.push(rmean);

        // rolling std
        let rstd = features::rolling_std(vals, w);
        names.push(format!("rolling_std_{w}"));
        columns.push(rstd);

        // rolling min
        let rmin = rolling_min(vals, w);
        names.push(format!("rolling_min_{w}"));
        columns.push(rmin);

        // rolling max
        let rmax = rolling_max(vals, w);
        names.push(format!("rolling_max_{w}"));
        columns.push(rmax);
    }

    // --- Lag features ---
    let lag1 = features::lag(vals, 1);
    names.push("lag_1".into());
    columns.push(lag1);

    let lag2 = features::lag(vals, 2);
    names.push("lag_2".into());
    columns.push(lag2);

    if let Some(period) = config.period {
        if period > 0 && period < n {
            let lag_p = features::lag(vals, period);
            names.push(format!("lag_{period}"));
            columns.push(lag_p);
        }
    }

    // --- Build row-major matrix ---
    let n_features = names.len();
    let mut data = vec![0.0; n * n_features];
    for (col_idx, col) in columns.iter().enumerate() {
        for row in 0..n {
            data[row * n_features + col_idx] = col[row];
        }
    }

    let mut matrix = FeatureMatrix {
        names,
        data,
        n_rows: n,
    };

    // --- Variance-threshold pruning ---
    if let Some(threshold) = config.variance_threshold {
        prune_low_variance(&mut matrix, threshold);
    }

    matrix
}

/// Remove features whose variance is below `threshold`.
fn prune_low_variance(matrix: &mut FeatureMatrix, threshold: f64) {
    let nf = matrix.n_features();
    let n = matrix.n_rows;
    if nf == 0 || n < 2 {
        return;
    }

    let mut keep: Vec<bool> = Vec::with_capacity(nf);
    for col in 0..nf {
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut count = 0usize;
        for row in 0..n {
            let v = matrix.data[row * nf + col];
            if !v.is_nan() {
                sum += v;
                sum_sq += v * v;
                count += 1;
            }
        }
        let var = if count > 1 {
            (sum_sq - sum * sum / count as f64) / (count - 1) as f64
        } else {
            0.0
        };
        keep.push(var >= threshold);
    }

    let new_nf = keep.iter().filter(|&&k| k).count();
    if new_nf == nf {
        return; // nothing to prune
    }

    let new_names: Vec<String> = matrix
        .names
        .iter()
        .zip(keep.iter())
        .filter(|(_, &k)| k)
        .map(|(n, _)| n.clone())
        .collect();

    let mut new_data = Vec::with_capacity(n * new_nf);
    for row in 0..n {
        for (col, &k) in keep.iter().enumerate() {
            if k {
                new_data.push(matrix.data[row * nf + col]);
            }
        }
    }

    matrix.names = new_names;
    matrix.data = new_data;
}

/// Rolling mean (trailing window).  NAN for first `window - 1` rows.
fn rolling_mean(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    let mut out = vec![f64::NAN; n];
    if window == 0 || window > n {
        return out;
    }
    let mut sum: f64 = values[..window].iter().sum();
    out[window - 1] = sum / window as f64;

    // Periodic recomputation every 1024 steps to bound FP drift.
    const RECOMPUTE_INTERVAL: usize = 1024;
    for i in window..n {
        if (i - window).is_multiple_of(RECOMPUTE_INTERVAL) {
            sum = values[i + 1 - window..=i].iter().sum();
        } else {
            sum += values[i] - values[i - window];
        }
        out[i] = sum / window as f64;
    }
    out
}

/// Rolling min (trailing window) using monotonic deque — O(n) total.
/// NAN for first `window - 1` rows.
fn rolling_min(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    let mut out = vec![f64::NAN; n];
    if window == 0 || window > n {
        return out;
    }
    // Deque stores indices; front is always the index of the min in the current window
    let mut deque = std::collections::VecDeque::with_capacity(window);
    for i in 0..n {
        // Remove elements outside the window
        while let Some(&front) = deque.front() {
            if front + window <= i {
                deque.pop_front();
            } else {
                break;
            }
        }
        // Maintain monotonically increasing invariant
        while let Some(&back) = deque.back() {
            if values[back] >= values[i] {
                deque.pop_back();
            } else {
                break;
            }
        }
        deque.push_back(i);
        if i >= window - 1 {
            out[i] = values[deque[0]];
        }
    }
    out
}

/// Rolling max (trailing window) using monotonic deque — O(n) total.
/// NAN for first `window - 1` rows.
fn rolling_max(values: &[f64], window: usize) -> Vec<f64> {
    let n = values.len();
    let mut out = vec![f64::NAN; n];
    if window == 0 || window > n {
        return out;
    }
    let mut deque = std::collections::VecDeque::with_capacity(window);
    for i in 0..n {
        while let Some(&front) = deque.front() {
            if front + window <= i {
                deque.pop_front();
            } else {
                break;
            }
        }
        while let Some(&back) = deque.back() {
            if values[back] <= values[i] {
                deque.pop_back();
            } else {
                break;
            }
        }
        deque.push_back(i);
        if i >= window - 1 {
            out[i] = values[deque[0]];
        }
    }
    out
}

/// Decompose a Unix epoch second into (hour_of_day, day_of_week, day_of_month, month_of_year).
///
/// Uses a simplified civil-date algorithm (no leap-second handling).
fn decompose_epoch_secs(epoch_secs: i64) -> (u32, u32, u32, u32) {
    // seconds in a day
    const SECS_PER_DAY: i64 = 86400;

    let day_secs = epoch_secs.rem_euclid(SECS_PER_DAY);
    let hour = (day_secs / 3600) as u32;

    // days since epoch (1970-01-01 was Thursday = day 4)
    let days = epoch_secs.div_euclid(SECS_PER_DAY);
    // 0=Mon … 6=Sun.  1970-01-01 = Thursday = 3.
    let dow = ((days + 3) % 7) as u32;

    // Civil date from days since epoch — Howard Hinnant's algorithm
    let (y, m, d) = civil_from_days(days);
    let _ = y; // not needed
    let dom = d as u32;
    let moy = m as u32;

    (hour, dow, dom, moy)
}

/// Convert days since 1970-01-01 to (year, month 1–12, day 1–31).
/// Adapted from Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_features_basic() {
        let n = 100;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 60_000_000_000).collect(); // 1-min interval
        let vals: Vec<f64> = (0..n).map(|i| (i as f64 * 0.1).sin()).collect();

        let config = AutoFeatureConfig::default().with_windows(vec![5, 10]);
        let fm = auto_features(&ts, &vals, &config);

        assert_eq!(fm.n_rows, n);
        // 5 temporal + 4*2 rolling + 2 lag = 15
        assert_eq!(fm.n_features(), 15);
        assert!(fm.names.contains(&"hour_of_day".to_string()));
        assert!(fm.names.contains(&"rolling_mean_5".to_string()));
        assert!(fm.names.contains(&"lag_1".to_string()));
    }

    #[test]
    fn test_auto_features_with_period() {
        let n = 50;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 3_600_000_000_000).collect();
        let vals: Vec<f64> = (0..n).map(|i| i as f64).collect();

        let config = AutoFeatureConfig::default()
            .with_windows(vec![5])
            .with_period(24);
        let fm = auto_features(&ts, &vals, &config);

        // 5 temporal + 4 rolling + 3 lag (lag_1, lag_2, lag_24) = 12
        assert_eq!(fm.n_features(), 12);
        assert!(fm.names.contains(&"lag_24".to_string()));
    }

    #[test]
    fn test_variance_threshold_pruning() {
        let n = 50;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 60_000_000_000).collect();
        // Constant signal → many features will have near-zero variance
        let vals = vec![42.0; n];

        let config = AutoFeatureConfig::default()
            .with_windows(vec![5])
            .with_variance_threshold(0.01);
        let fm = auto_features(&ts, &vals, &config);

        // Constant values → rolling_std=NAN, lag features are 42 (constant) → pruned
        // Most features should be pruned since the signal is constant
        assert!(fm.n_features() < 12);
    }

    #[test]
    fn test_feature_matrix_column_by_name() {
        let n = 20;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 60_000_000_000).collect();
        let vals: Vec<f64> = (0..n).map(|i| i as f64).collect();

        let config = AutoFeatureConfig {
            temporal_features: false,
            rolling_windows: vec![],
            period: None,
            variance_threshold: None,
        };
        let fm = auto_features(&ts, &vals, &config);

        // Only lag_1, lag_2
        assert_eq!(fm.n_features(), 2);
        let lag1 = fm.column_by_name("lag_1").unwrap();
        assert!(lag1[0].is_nan());
        assert_eq!(lag1[1], 0.0);
        assert_eq!(lag1[2], 1.0);
    }

    #[test]
    fn test_rolling_mean() {
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let rm = rolling_mean(&v, 3);
        assert!(rm[0].is_nan());
        assert!(rm[1].is_nan());
        assert!((rm[2] - 2.0).abs() < 1e-9);
        assert!((rm[3] - 3.0).abs() < 1e-9);
        assert!((rm[4] - 4.0).abs() < 1e-9);
    }

    #[test]
    fn test_rolling_min_max() {
        let v = vec![3.0, 1.0, 4.0, 1.0, 5.0];
        let rmin = rolling_min(&v, 3);
        let rmax = rolling_max(&v, 3);
        assert!(rmin[0].is_nan());
        assert!(rmin[1].is_nan());
        assert_eq!(rmin[2], 1.0);
        assert_eq!(rmin[3], 1.0);
        assert_eq!(rmin[4], 1.0);
        assert_eq!(rmax[2], 4.0);
        assert_eq!(rmax[3], 4.0);
        assert_eq!(rmax[4], 5.0);
    }

    #[test]
    fn test_decompose_epoch_known_date() {
        // 2024-01-15 10:30:00 UTC = Monday
        // epoch = 1705314600
        let (hour, dow, dom, moy) = decompose_epoch_secs(1_705_314_600);
        assert_eq!(hour, 10);
        assert_eq!(dow, 0); // Monday = 0
        assert_eq!(dom, 15);
        assert_eq!(moy, 1);
    }

    #[test]
    fn test_decompose_epoch_weekend() {
        // 2024-01-13 15:00:00 UTC = Saturday
        // epoch = 1705158000
        let (_, dow, _, _) = decompose_epoch_secs(1_705_158_000);
        assert_eq!(dow, 5); // Saturday = 5
    }

    #[test]
    fn test_no_temporal_features() {
        let n = 10;
        let ts: Vec<i64> = (0..n).map(|i| i as i64 * 60_000_000_000).collect();
        let vals: Vec<f64> = (0..n).map(|i| i as f64).collect();

        let config = AutoFeatureConfig {
            temporal_features: false,
            rolling_windows: vec![3],
            period: None,
            variance_threshold: None,
        };
        let fm = auto_features(&ts, &vals, &config);

        // 4 rolling (mean, std, min, max @3) + 2 lag = 6
        assert_eq!(fm.n_features(), 6);
        assert!(!fm.names.contains(&"hour_of_day".to_string()));
    }
}
