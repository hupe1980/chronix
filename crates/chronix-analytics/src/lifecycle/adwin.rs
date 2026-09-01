//! ADWIN (ADaptive WINdowing) streaming change detection.
//!
//! Implements the algorithm from Bifet & Gavaldà (2007) with
//! exponential histogram compression, providing:
//!
//! - O(log W) amortised cost per observation
//! - O(log W) memory
//! - Automatic window shrinking when concept drift is detected
//!
//! # Example
//!
//! ```no_run
//! use chronix_analytics::lifecycle::adwin::Adwin;
//!
//! let mut adwin = Adwin::new(0.002);
//!
//! // Stable phase — no drift expected.
//! for _ in 0..200 {
//!     assert!(!adwin.add(5.0));
//! }
//!
//! // Sudden shift — drift should be detected.
//! let mut detected = false;
//! for _ in 0..50 {
//!     if adwin.add(15.0) {
//!         detected = true;
//!         break;
//!     }
//! }
//! assert!(detected, "ADWIN should detect mean shift from 5 to 15");
//! ```

/// Maximum number of buckets per row in the exponential histogram.
/// More buckets per row → finer granularity but more memory.
const MAX_BUCKETS_PER_ROW: usize = 5;

/// A single bucket in the exponential histogram.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    /// Sum of the values in this bucket.
    total: f64,
    /// Sum of squared values (for variance estimation).
    variance: f64,
    /// Number of observations in this bucket.
    count: usize,
}

/// ADWIN streaming change detector.
///
/// Maintains a variable-length window of recent observations using an
/// exponential histogram.  After each [`add`](Self::add) call the
/// algorithm checks whether any sub-window split shows a statistically
/// significant mean difference (Hoeffding bound at confidence `1 - δ`).
/// If so it shrinks the window to the more recent portion and reports
/// drift.
///
/// # Parameters
///
/// - `delta` — confidence parameter (smaller = fewer false positives,
///   slower reaction).  Typical value: 0.002.
#[derive(Debug, Clone)]
pub struct Adwin {
    /// Confidence parameter δ.
    delta: f64,
    /// Exponential histogram: `rows[i]` contains buckets of size `2^i`.
    rows: Vec<Vec<Bucket>>,
    /// Total number of observations in the window.
    total_count: usize,
    /// Running sum of all values in the window.
    total_sum: f64,
    /// Running sum of squared values.
    total_variance: f64,
    /// Width (number of observations) stored.
    width: usize,
}

impl Adwin {
    /// Create a new ADWIN detector with the given confidence parameter.
    ///
    /// Lower `delta` values make the detector less sensitive (fewer false
    /// positives).  `0.002` is a common choice.
    #[must_use]
    pub fn new(delta: f64) -> Self {
        Self {
            delta: delta.max(1e-15),
            rows: Vec::new(),
            total_count: 0,
            total_sum: 0.0,
            total_variance: 0.0,
            width: 0,
        }
    }

    /// Add a new observation and check for drift.
    ///
    /// Returns `true` if drift is detected (the window was shrunk).
    ///
    /// Non-finite values (`NaN`, `±Inf`) are rejected and the method returns
    /// `false` without modifying the window, because they would silently
    /// poison `total_sum` and `total_variance`.
    pub fn add(&mut self, value: f64) -> bool {
        if !value.is_finite() {
            return false;
        }

        // Insert a new size-1 bucket at position 0.
        self.insert_bucket(value);
        // Compress: merge if a row has too many buckets.
        self.compress();
        // Check for drift from the tail (oldest) end.
        self.detect_drift()
    }

    /// Current window mean.
    #[must_use]
    pub fn mean(&self) -> f64 {
        if self.width == 0 {
            return 0.0;
        }
        self.total_sum / self.width as f64
    }

    /// Current window size (number of observations).
    #[must_use]
    pub fn window_size(&self) -> usize {
        self.width
    }

    /// Current estimated standard deviation of the window.
    #[must_use]
    pub fn std_dev(&self) -> f64 {
        if self.width < 2 {
            return 0.0;
        }
        let var = (self.total_variance / self.width as f64)
            - (self.total_sum / self.width as f64).powi(2);
        var.max(0.0).sqrt()
    }

    /// Reset the detector to its initial state.
    pub fn reset(&mut self) {
        self.rows.clear();
        self.total_count = 0;
        self.total_sum = 0.0;
        self.total_variance = 0.0;
        self.width = 0;
    }

    // ── Internals ──────────────────────────────────────────────────

    fn insert_bucket(&mut self, value: f64) {
        if self.rows.is_empty() {
            self.rows.push(Vec::new());
        }
        self.rows[0].push(Bucket {
            total: value,
            variance: value * value,
            count: 1,
        });
        self.total_count += 1;
        self.total_sum += value;
        self.total_variance += value * value;
        self.width += 1;
    }

    fn compress(&mut self) {
        for i in 0..self.rows.len() {
            if self.rows[i].len() > MAX_BUCKETS_PER_ROW + 1 {
                // Merge the two oldest (last) buckets and push to
                // the next row.
                let b2 = self.rows[i].remove(0);
                let b1 = self.rows[i].remove(0);
                let merged = Bucket {
                    total: b1.total + b2.total,
                    variance: b1.variance + b2.variance,
                    count: b1.count + b2.count,
                };
                if i + 1 >= self.rows.len() {
                    self.rows.push(Vec::new());
                }
                self.rows[i + 1].push(merged);
            }
        }
    }

    fn detect_drift(&mut self) -> bool {
        let mut drift = false;
        // Walk from the oldest (largest) buckets to the newest.
        // Try removing the oldest bucket and check if the remaining
        // window has a significantly different mean.
        let mut n0: usize = 0;
        let mut sum0: f64 = 0.0;

        // Iterate from largest row to smallest (oldest to newest).
        let n_rows = self.rows.len();
        for i in (0..n_rows).rev() {
            let row = &self.rows[i];
            for j in 0..row.len() {
                let bucket = &row[j];
                n0 += bucket.count;
                sum0 += bucket.total;

                let n1 = self.width - n0;
                if n0 < 5 || n1 < 5 {
                    continue;
                }
                let sum1 = self.total_sum - sum0;

                let mean0 = sum0 / n0 as f64;
                let mean1 = sum1 / n1 as f64;

                let m = 1.0 / (1.0 / n0 as f64 + 1.0 / n1 as f64);
                let epsilon = ((2.0 / m) * (4.0 * self.width as f64 / self.delta).ln()).sqrt();

                if (mean0 - mean1).abs() >= epsilon {
                    // Drift detected — shrink the window by removing
                    // the oldest observations (up to and including this bucket).
                    self.remove_oldest(i, j);
                    drift = true;
                    break;
                }
            }
            if drift {
                break;
            }
        }
        drift
    }

    fn remove_oldest(&mut self, row_idx: usize, bucket_idx: usize) {
        // Remove all buckets from row_idx down (older) and all buckets
        // at indices ≤ bucket_idx in row_idx.

        // First: remove rows larger than row_idx entirely.
        let n_rows = self.rows.len();
        for i in (row_idx + 1..n_rows).rev() {
            for b in &self.rows[i] {
                self.total_sum -= b.total;
                self.total_variance -= b.variance;
                self.width -= b.count;
            }
            self.rows[i].clear();
        }

        // Then: remove buckets 0..=bucket_idx in row_idx.
        let to_remove = bucket_idx + 1;
        for _ in 0..to_remove {
            if self.rows[row_idx].is_empty() {
                break;
            }
            let b = self.rows[row_idx].remove(0);
            self.total_sum -= b.total;
            self.total_variance -= b.variance;
            self.width -= b.count;
        }

        // Clean up trailing empty rows.
        while self.rows.last().is_some_and(std::vec::Vec::is_empty) {
            self.rows.pop();
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_drift_on_stable_stream() {
        let mut adwin = Adwin::new(0.002);
        for i in 0..500 {
            let v = 10.0 + 0.1 * ((i * 7 + 3) % 11) as f64;
            assert!(!adwin.add(v), "unexpected drift at step {i}");
        }
        assert!(adwin.window_size() > 400);
    }

    #[test]
    fn detects_mean_shift() {
        let mut adwin = Adwin::new(0.002);
        for _ in 0..300 {
            adwin.add(5.0);
        }
        let mut detected = false;
        for _ in 0..100 {
            if adwin.add(50.0) {
                detected = true;
                break;
            }
        }
        assert!(detected, "should detect shift from 5 to 50");
    }

    #[test]
    fn window_shrinks_after_drift() {
        let mut adwin = Adwin::new(0.002);
        for _ in 0..300 {
            adwin.add(0.0);
        }
        let size_before = adwin.window_size();

        // Introduce drift.
        for _ in 0..200 {
            adwin.add(100.0);
        }
        // The old data should have been dropped.
        assert!(
            adwin.window_size() < size_before,
            "window should shrink: was {size_before}, now {}",
            adwin.window_size()
        );
    }

    #[test]
    fn mean_tracks_current_distribution() {
        let mut adwin = Adwin::new(0.002);
        for _ in 0..500 {
            adwin.add(10.0);
        }
        assert!((adwin.mean() - 10.0).abs() < 0.01);

        // Shift distribution.
        for _ in 0..500 {
            adwin.add(20.0);
        }
        // Mean should be close to 20 since old data was dropped.
        assert!(
            adwin.mean() > 15.0,
            "mean after shift should be near 20, got {}",
            adwin.mean()
        );
    }

    #[test]
    fn reset_clears_state() {
        let mut adwin = Adwin::new(0.002);
        for _ in 0..100 {
            adwin.add(5.0);
        }
        adwin.reset();
        assert_eq!(adwin.window_size(), 0);
        assert_eq!(adwin.mean(), 0.0);
    }

    #[test]
    fn lower_delta_is_less_sensitive() {
        // More conservative (lower delta) should need more evidence.
        let mut adwin_sensitive = Adwin::new(0.5);
        let mut adwin_conservative = Adwin::new(0.0001);

        let mut sensitive_detected_at = None;
        let mut conservative_detected_at = None;

        for _ in 0..200 {
            adwin_sensitive.add(1.0);
            adwin_conservative.add(1.0);
        }

        for i in 0..200 {
            if sensitive_detected_at.is_none() && adwin_sensitive.add(5.0) {
                sensitive_detected_at = Some(i);
            } else if sensitive_detected_at.is_some() {
                adwin_sensitive.add(5.0);
            }
            if conservative_detected_at.is_none() && adwin_conservative.add(5.0) {
                conservative_detected_at = Some(i);
            } else if conservative_detected_at.is_some() {
                adwin_conservative.add(5.0);
            }
        }

        if let (Some(s), Some(c)) = (sensitive_detected_at, conservative_detected_at) {
            assert!(
                s <= c,
                "sensitive (δ=0.5) detected at {s}, conservative (δ=0.0001) at {c}"
            );
        }
    }

    #[test]
    fn std_dev_reasonable() {
        let mut adwin = Adwin::new(0.002);
        for i in 0..1000 {
            adwin.add((i % 10) as f64);
        }
        let sd = adwin.std_dev();
        assert!(sd > 0.0 && sd < 5.0, "std_dev = {sd}");
    }

    #[test]
    fn gradual_drift_eventually_detected() {
        let mut adwin = Adwin::new(0.01);
        // Start with baseline.
        for _ in 0..200 {
            adwin.add(0.0);
        }
        // Gradual increase.
        let mut detected = false;
        for i in 0..2000 {
            let v = i as f64 * 0.1;
            if adwin.add(v) {
                detected = true;
                break;
            }
        }
        assert!(detected, "should detect gradual drift");
    }

    #[test]
    fn nan_rejected_without_poisoning() {
        let mut adwin = Adwin::new(0.002);
        for _ in 0..50 {
            adwin.add(1.0);
        }
        let mean_before = adwin.mean();

        // NaN must be rejected.
        assert!(!adwin.add(f64::NAN));
        assert_eq!(adwin.mean(), mean_before);
        assert_eq!(adwin.window_size(), 50);
    }

    #[test]
    fn infinity_rejected() {
        let mut adwin = Adwin::new(0.002);
        assert!(!adwin.add(f64::INFINITY));
        assert!(!adwin.add(f64::NEG_INFINITY));
        assert_eq!(adwin.window_size(), 0);
    }
}
