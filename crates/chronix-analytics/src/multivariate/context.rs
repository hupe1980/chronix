//! Multi-series context: time-aligned columnar matrix of multiple series.

use crate::multivariate::error::MultivariateError;

/// Column-major matrix for aligned multi-series data.
///
/// Layout: `data[series_idx][time_idx]` — each inner Vec is one series.
#[derive(Debug, Clone)]
pub struct ColumnarMatrix {
    /// One row per series, contiguous for SIMD-friendly access.
    pub data: Vec<Vec<f64>>,
    /// Common aligned timestamps.
    pub timestamps: Vec<i64>,
    /// Series identifiers (measurement + tags).
    pub series_ids: Vec<String>,
}

impl ColumnarMatrix {
    /// Number of series (columns in the conceptual layout).
    pub fn n_series(&self) -> usize {
        self.data.len()
    }

    /// Number of time points.
    pub fn n_timestamps(&self) -> usize {
        self.timestamps.len()
    }

    /// Get a single series by index.
    pub fn series(&self, idx: usize) -> Option<&[f64]> {
        self.data.get(idx).map(std::vec::Vec::as_slice)
    }

    /// Get a single series by name.
    pub fn series_by_name(&self, name: &str) -> Option<&[f64]> {
        self.series_ids
            .iter()
            .position(|s| s == name)
            .and_then(|idx| self.series(idx))
    }
}

/// Time-aligned context for multi-series analysis.
///
/// Constructed by providing raw per-series data possibly at different sampling
/// rates; the context resamples and aligns them to a common timeline.
#[derive(Debug, Clone)]
pub struct MultiSeriesContext {
    /// Aligned columnar data matrix.
    pub matrix: ColumnarMatrix,
}

impl MultiSeriesContext {
    /// Builds a context from individual series data.
    ///
    /// Each entry in `series` is (id, timestamps, values).
    /// All series will be resampled to the finest common interval via linear
    /// interpolation.
    ///
    /// `max_points` overrides the default resampling cap (1 000 000). Pass
    /// `None` to use the default.
    pub fn build(
        series: Vec<(String, Vec<i64>, Vec<f64>)>,
        max_series: Option<usize>,
    ) -> Result<Self, MultivariateError> {
        Self::build_with_max_points(series, max_series, None)
    }

    /// Like [`build`](Self::build) but with a configurable resampling cap.
    #[allow(clippy::needless_pass_by_value)] // series vector is consumed conceptually (aligned copies)
    pub fn build_with_max_points(
        series: Vec<(String, Vec<i64>, Vec<f64>)>,
        max_series: Option<usize>,
        max_points: Option<usize>,
    ) -> Result<Self, MultivariateError> {
        if series.is_empty() {
            return Err(MultivariateError::InsufficientData { min: 1, got: 0 });
        }
        let max = max_series.unwrap_or(100);
        if series.len() > max {
            return Err(MultivariateError::InvalidParameter(format!(
                "too many series: {} > max {}",
                series.len(),
                max
            )));
        }

        // Find global time range and finest interval
        let mut global_min = i64::MAX;
        let mut global_max = i64::MIN;
        let mut finest_interval = i64::MAX;

        for (_, ts, _) in &series {
            if ts.is_empty() {
                continue;
            }
            global_min = global_min.min(ts[0]);
            if let Some(&last) = ts.last() {
                global_max = global_max.max(last);
            }
            for w in ts.windows(2) {
                let delta = w[1] - w[0];
                if delta > 0 {
                    finest_interval = finest_interval.min(delta);
                }
            }
        }

        // If we never found a valid range (all series have 0 or 1 points), use single-point fallback
        if global_min > global_max {
            // Gather single-point timestamps
            let mut only_ts = i64::MIN;
            for (_, ts, _) in &series {
                if let Some(&t) = ts.first() {
                    only_ts = only_ts.max(t);
                }
            }
            if only_ts == i64::MIN {
                return Err(MultivariateError::InsufficientData { min: 1, got: 0 });
            }
            global_min = only_ts;
            global_max = only_ts;
        }

        if finest_interval == i64::MAX || finest_interval <= 0 {
            finest_interval = 1_000_000_000; // fallback 1s
        }

        // Build common timeline
        let n_points = ((global_max - global_min) / finest_interval + 1) as usize;
        // Reject instead of silently truncating at the safety cap.
        // The cap is configurable; defaults to 1M if not specified.
        const DEFAULT_MAX_POINTS: usize = 1_000_000;
        let cap = max_points.unwrap_or(DEFAULT_MAX_POINTS);
        if n_points > cap {
            return Err(MultivariateError::InvalidParameter(format!(
                "resampled timeline would produce {n_points} points (max {cap}); \
                 reduce the time range or increase the sampling interval"
            )));
        }
        let timestamps: Vec<i64> = (0..n_points)
            .map(|i| global_min + i as i64 * finest_interval)
            .collect();

        // Resample each series onto the common timeline via linear interpolation
        let mut data = Vec::with_capacity(series.len());
        let mut ids = Vec::with_capacity(series.len());

        for (id, ts, vals) in &series {
            ids.push(id.clone());
            let aligned = linear_interpolate(ts, vals, &timestamps);
            data.push(aligned);
        }

        Ok(Self {
            matrix: ColumnarMatrix {
                data,
                timestamps,
                series_ids: ids,
            },
        })
    }
}

/// Linear interpolation of (ts, vals) onto `target_ts`.
///
/// Returns `NaN` for target timestamps outside the input range
/// to prevent false correlations from constant extrapolation.
fn linear_interpolate(ts: &[i64], vals: &[f64], target_ts: &[i64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(target_ts.len());
    if ts.is_empty() || vals.is_empty() {
        result.resize(target_ts.len(), f64::NAN);
        return result;
    }
    let mut j = 0usize;
    for &t in target_ts {
        // Advance j until ts[j] <= t < ts[j+1]
        while j + 1 < ts.len() && ts[j + 1] <= t {
            j += 1;
        }
        if t < ts[0] {
            // Before first timestamp — return NaN.
            result.push(f64::NAN);
        } else if j + 1 >= ts.len() {
            // At or past the last timestamp.
            if t == ts[ts.len() - 1] {
                // Exact match at the last point — return its value.
                result.push(vals[ts.len() - 1]);
            } else {
                // Beyond range — return NaN instead of extrapolating.
                result.push(f64::NAN);
            }
        } else {
            let dt = (ts[j + 1] - ts[j]) as f64;
            let frac = if dt > 0.0 {
                (t - ts[j]) as f64 / dt
            } else {
                0.0
            };
            result.push(vals[j] + frac * (vals[j + 1] - vals[j]));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_aligned_context() {
        let s1 = (
            "cpu".to_string(),
            vec![0, 1_000_000_000, 2_000_000_000],
            vec![10.0, 20.0, 30.0],
        );
        let s2 = (
            "mem".to_string(),
            vec![0, 2_000_000_000],
            vec![100.0, 200.0],
        );
        let ctx = MultiSeriesContext::build(vec![s1, s2], None).unwrap();
        assert_eq!(ctx.matrix.n_series(), 2);
        assert!(ctx.matrix.n_timestamps() >= 2);
    }

    #[test]
    fn series_by_name() {
        let s1 = ("cpu".to_string(), vec![0, 1_000_000_000], vec![10.0, 20.0]);
        let ctx = MultiSeriesContext::build(vec![s1], None).unwrap();
        assert!(ctx.matrix.series_by_name("cpu").is_some());
        assert!(ctx.matrix.series_by_name("missing").is_none());
    }

    #[test]
    fn too_many_series() {
        let series: Vec<_> = (0..200)
            .map(|i| (format!("s{i}"), vec![0i64, 1], vec![0.0, 1.0]))
            .collect();
        assert!(MultiSeriesContext::build(series, Some(100)).is_err());
    }

    #[test]
    fn interpolation_correctness() {
        let ts = vec![0i64, 10];
        let vals = vec![0.0, 100.0];
        let target = vec![0, 5, 10];
        let result = linear_interpolate(&ts, &vals, &target);
        assert!((result[0] - 0.0).abs() < 1e-6);
        assert!((result[1] - 50.0).abs() < 1e-6);
        assert!((result[2] - 100.0).abs() < 1e-6);
    }
}
