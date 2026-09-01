//! Gap detection in time-series data.

/// Detects gaps in a sorted timestamp column where the interval exceeds
/// `expected_interval × tolerance`.
pub struct GapDetector {
    /// Expected interval between consecutive timestamps (nanoseconds).
    pub expected_interval_ns: i64,
    /// Tolerance multiplier — gap is detected when interval > expected × tolerance.
    pub tolerance: f64,
}

/// A detected gap in the time series.
#[derive(Debug, Clone, PartialEq)]
pub struct Gap {
    /// Index of the point before the gap.
    pub before_idx: usize,
    /// Index of the point after the gap.
    pub after_idx: usize,
    /// Timestamp of the point before the gap.
    pub before_ts: i64,
    /// Timestamp of the point after the gap.
    pub after_ts: i64,
    /// Number of expected points missing in this gap.
    pub missing_count: usize,
}

impl GapDetector {
    /// Creates a new gap detector.
    ///
    /// # Arguments
    /// - `expected_interval_ns` — expected interval between consecutive points (nanoseconds, must be > 0)
    /// - `tolerance` — multiplier for gap threshold (e.g. 1.5 means gap if interval > 1.5× expected, must be > 0)
    ///
    /// # Errors
    ///
    /// Returns `None` if `expected_interval_ns <= 0` or `tolerance <= 0.0`.
    pub fn new(expected_interval_ns: i64, tolerance: f64) -> Option<Self> {
        if expected_interval_ns <= 0 || tolerance <= 0.0 {
            return None;
        }
        Some(Self {
            expected_interval_ns,
            tolerance,
        })
    }

    /// Detects all gaps in the given sorted timestamp array.
    pub fn detect(&self, timestamps: &[i64]) -> Vec<Gap> {
        if timestamps.len() < 2 {
            return Vec::new();
        }

        let threshold = (self.expected_interval_ns as f64 * self.tolerance) as i64;
        let mut gaps = Vec::new();

        for i in 1..timestamps.len() {
            let delta = timestamps[i] - timestamps[i - 1];
            if delta > threshold {
                let missing = ((delta as f64 / self.expected_interval_ns as f64).round() as usize)
                    .saturating_sub(1);
                if missing > 0 {
                    gaps.push(Gap {
                        before_idx: i - 1,
                        after_idx: i,
                        before_ts: timestamps[i - 1],
                        after_ts: timestamps[i],
                        missing_count: missing,
                    });
                }
            }
        }
        gaps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_gaps() {
        let ts: Vec<i64> = (0..10).map(|i| i * 1_000_000_000).collect(); // 1s intervals
        let detector = GapDetector::new(1_000_000_000, 1.5).unwrap();
        assert!(detector.detect(&ts).is_empty());
    }

    #[test]
    fn single_gap() {
        // 0, 1, 2, 5, 6 (gap of 3s between index 2 and 3)
        let ts = vec![
            0,
            1_000_000_000,
            2_000_000_000,
            5_000_000_000,
            6_000_000_000,
        ];
        let detector = GapDetector::new(1_000_000_000, 1.5).unwrap();
        let gaps = detector.detect(&ts);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].before_idx, 2);
        assert_eq!(gaps[0].after_idx, 3);
        assert_eq!(gaps[0].missing_count, 2);
    }

    #[test]
    fn multiple_gaps() {
        let ts = vec![
            0,
            1_000_000_000,
            5_000_000_000,
            6_000_000_000,
            10_000_000_000,
        ];
        let detector = GapDetector::new(1_000_000_000, 1.5).unwrap();
        let gaps = detector.detect(&ts);
        assert_eq!(gaps.len(), 2);
    }

    #[test]
    fn empty_and_single() {
        let detector = GapDetector::new(1_000_000_000, 1.5).unwrap();
        assert!(detector.detect(&[]).is_empty());
        assert!(detector.detect(&[42]).is_empty());
    }

    #[test]
    fn invalid_params_return_none() {
        assert!(GapDetector::new(0, 1.5).is_none());
        assert!(GapDetector::new(-1, 1.5).is_none());
        assert!(GapDetector::new(1_000, 0.0).is_none());
        assert!(GapDetector::new(1_000, -1.0).is_none());
    }
}
