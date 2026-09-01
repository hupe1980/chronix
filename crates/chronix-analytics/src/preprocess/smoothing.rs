//! Smoothing filters for noise reduction.

use crate::compute::simd_sum;

/// Smoothing method enum for configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Smoother {
    /// Exponential smoothing with alpha parameter.
    Exponential {
        /// Smoothing factor in `(0.0, 1.0]`.
        alpha: f64,
    },
    /// Simple moving average with window size.
    MovingAverage {
        /// Number of points in the sliding window.
        window: usize,
    },
    /// Weighted moving average with custom weights.
    WeightedMovingAverage {
        /// Weights applied to the sliding window.
        weights: Vec<f64>,
    },
}

impl Smoother {
    /// Apply the smoother to the given values.
    ///
    /// Invalid parameters are clamped to safe defaults to prevent panics
    /// from deserialized or user-supplied configuration:
    /// - `alpha` is clamped to `(0.0, 1.0]`
    /// - `window` is clamped to `max(1, window)`
    /// - empty `weights` produces a pass-through (identity)
    pub fn smooth(&self, values: &[f64]) -> Vec<f64> {
        match self {
            Smoother::Exponential { alpha } => {
                let safe_alpha = alpha.clamp(f64::MIN_POSITIVE, 1.0);
                ExponentialSmoother::new(safe_alpha).smooth(values)
            }
            Smoother::MovingAverage { window } => {
                let safe_window = (*window).max(1);
                MovingAverageSmoother::new(safe_window).smooth(values)
            }
            Smoother::WeightedMovingAverage { weights } => {
                if weights.is_empty() {
                    return values.to_vec();
                }
                WeightedMovingAverage::new(weights.clone()).smooth(values)
            }
        }
    }
}

/// Exponential smoothing with configurable alpha (0..1).
///
/// Single-pass O(n) — each output is `alpha * x[i] + (1 - alpha) * s[i-1]`.
pub struct ExponentialSmoother {
    alpha: f64,
}

impl ExponentialSmoother {
    /// Creates a new exponential smoother.
    ///
    /// # Panics
    /// Panics if alpha is not in (0, 1].
    pub fn new(alpha: f64) -> Self {
        assert!(
            alpha > 0.0 && alpha <= 1.0,
            "alpha must be in (0, 1], got {alpha}"
        );
        Self { alpha }
    }

    /// Smooths the input values.
    pub fn smooth(&self, values: &[f64]) -> Vec<f64> {
        if values.is_empty() {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(values.len());
        result.push(values[0]);
        let one_minus = 1.0 - self.alpha;
        for i in 1..values.len() {
            result.push(self.alpha * values[i] + one_minus * result[i - 1]);
        }
        result
    }
}

/// Simple moving average with configurable window size.
///
/// Uses a running sum for O(n) computation. Handles series shorter than window
/// by reducing the effective window.
pub struct MovingAverageSmoother {
    window: usize,
}

impl MovingAverageSmoother {
    /// Creates a new moving average smoother.
    ///
    /// # Panics
    /// Panics if window is 0.
    pub fn new(window: usize) -> Self {
        assert!(window > 0, "window must be > 0");
        Self { window }
    }

    /// Smooths the input values using a centered moving average.
    ///
    /// For edge elements where the full window is not available, uses the
    /// available range (reduced window). Uses a running sum for O(n)
    /// computation in the interior of the series.
    pub fn smooth(&self, values: &[f64]) -> Vec<f64> {
        if values.is_empty() {
            return Vec::new();
        }

        let n = values.len();
        let w = self.window.min(n);
        // For even windows use asymmetric halves so the effective window
        // matches the requested size exactly.
        let half_left = if w.is_multiple_of(2) {
            w / 2 - 1
        } else {
            w / 2
        };
        let half_right = w / 2;
        let mut result = Vec::with_capacity(n);

        // Prefix sum for O(n) windowed average computation.
        // prefix[i] = values[0] + values[1] + … + values[i-1]
        let mut prefix = Vec::with_capacity(n + 1);
        prefix.push(0.0);
        let mut acc = 0.0;
        for &v in values {
            acc += v;
            prefix.push(acc);
        }

        for i in 0..n {
            let lo = i.saturating_sub(half_left);
            let hi = (i + half_right + 1).min(n);
            let count = (hi - lo) as f64;
            let sum = prefix[hi] - prefix[lo];
            result.push(sum / count);
        }

        result
    }
}

/// Weighted moving average with custom weight vector.
pub struct WeightedMovingAverage {
    weights: Vec<f64>,
    weight_sum: f64,
}

impl WeightedMovingAverage {
    /// Creates a new weighted moving average smoother.
    ///
    /// # Panics
    /// Panics if weights is empty.
    pub fn new(weights: Vec<f64>) -> Self {
        assert!(!weights.is_empty(), "weights must not be empty");
        let weight_sum = simd_sum(&weights);
        Self {
            weights,
            weight_sum,
        }
    }

    /// Smooths the input values.
    ///
    /// Weight vector is applied as a trailing window — `weights[0]` is the
    /// oldest value in the window.
    pub fn smooth(&self, values: &[f64]) -> Vec<f64> {
        if values.is_empty() {
            return Vec::new();
        }

        let n = values.len();
        let w = self.weights.len();
        let mut result = Vec::with_capacity(n);

        for i in 0..n {
            if i + 1 < w {
                // Not enough history — use available weights with renormalization
                let available = i + 1;
                let offset = w - available;
                let mut sum = 0.0;
                let mut wsum = 0.0;
                for (j, &val) in values[..available].iter().enumerate() {
                    sum += val * self.weights[offset + j];
                    wsum += self.weights[offset + j];
                }
                result.push(if wsum.abs() > 1e-15 {
                    sum / wsum
                } else {
                    values[i]
                });
            } else {
                let mut sum = 0.0;
                let start = i + 1 - w;
                for j in 0..w {
                    sum += values[start + j] * self.weights[j];
                }
                result.push(if self.weight_sum.abs() > 1e-15 {
                    sum / self.weight_sum
                } else {
                    values[i]
                });
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::simd_mean;

    #[test]
    fn exponential_smoother_alpha_1() {
        let s = ExponentialSmoother::new(1.0);
        let vals = vec![1.0, 5.0, 3.0, 7.0];
        let result = s.smooth(&vals);
        assert_eq!(result, vals); // alpha=1 → passthrough
    }

    #[test]
    fn exponential_smoother_smooth() {
        let s = ExponentialSmoother::new(0.5);
        let vals = vec![10.0, 20.0, 30.0, 40.0];
        let result = s.smooth(&vals);
        assert!((result[0] - 10.0).abs() < 1e-10);
        assert!((result[1] - 15.0).abs() < 1e-10); // 0.5*20 + 0.5*10
        assert!((result[2] - 22.5).abs() < 1e-10); // 0.5*30 + 0.5*15
        assert!((result[3] - 31.25).abs() < 1e-10); // 0.5*40 + 0.5*22.5
    }

    #[test]
    fn moving_average_basic() {
        let s = MovingAverageSmoother::new(3);
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = s.smooth(&vals);
        assert_eq!(result.len(), 5);
        // Center at index 1: mean(1,2,3) = 2.0
        assert!((result[1] - 2.0).abs() < 1e-10);
        // Center at index 2: mean(2,3,4) = 3.0
        assert!((result[2] - 3.0).abs() < 1e-10);
    }

    #[test]
    fn moving_average_window_larger_than_data() {
        let s = MovingAverageSmoother::new(100);
        let vals = vec![1.0, 2.0, 3.0];
        let result = s.smooth(&vals);
        assert_eq!(result.len(), 3);
        // All values should be the mean of the available window
        let mean = simd_mean(&vals);
        // Middle value: mean of all 3
        assert!((result[1] - mean).abs() < 1e-10);
    }

    #[test]
    fn weighted_moving_average() {
        // Weights: [1, 2, 3] — most recent gets highest weight
        let s = WeightedMovingAverage::new(vec![1.0, 2.0, 3.0]);
        let vals = vec![10.0, 20.0, 30.0, 40.0];
        let result = s.smooth(&vals);
        assert_eq!(result.len(), 4);
        // Index 2: (1*10 + 2*20 + 3*30) / 6 = (10+40+90)/6 = 140/6 ≈ 23.33
        assert!((result[2] - 140.0 / 6.0).abs() < 1e-10);
    }

    #[test]
    fn empty_data() {
        assert!(ExponentialSmoother::new(0.5).smooth(&[]).is_empty());
        assert!(MovingAverageSmoother::new(3).smooth(&[]).is_empty());
        assert!(WeightedMovingAverage::new(vec![1.0]).smooth(&[]).is_empty());
    }

    #[test]
    fn smoother_enum_dispatch() {
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let s = Smoother::Exponential { alpha: 0.5 };
        let r = s.smooth(&vals);
        assert_eq!(r.len(), 5);
    }
}
