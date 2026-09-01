//! Clock drift detection and correction for irregularly-sampled data.

/// Strategy for clock drift correction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClockDriftStrategy {
    /// No correction.
    #[default]
    None,
    /// Enforce monotonically increasing timestamps — duplicate/backward stamps
    /// are snapped to `last_ts + 1ns`.
    Monotonic,
    /// Detect gradual clock drift via regression on timestamp deltas and apply
    /// linear correction to align with the expected interval.
    ///
    /// # Limitation
    ///
    /// The smooth correction fits a **linear** regression on timestamp
    /// deltas (`delta[i] = a + b*i`) and blends toward the ideal grid
    /// proportionally to drift magnitude.  This assumes clock drift is
    /// approximately constant-rate (e.g. quartz oscillator frequency
    /// offset).  **Non-linear** drift patterns — such as thermal-induced
    /// frequency ramps or GPS-disciplined clock step corrections — are
    /// not well modelled and may leave residual jitter.  For those cases
    /// consider pre-processing with piecewise-linear or spline-based
    /// correction before ingestion.
    Smooth,
}

/// Report of clock drift detection and correction.
#[derive(Debug, Clone)]
pub struct DriftReport {
    /// Maximum observed drift in nanoseconds.
    pub max_drift_ns: i64,
    /// Ranges of affected indices (start, end).
    pub affected_ranges: Vec<(usize, usize)>,
    /// Number of timestamps that were corrected.
    pub corrections_applied: usize,
}

/// Detects and corrects clock drift in timestamp arrays.
pub struct ClockDriftDetector;

impl ClockDriftDetector {
    /// Detect and correct clock drift according to the given strategy.
    ///
    /// Returns the corrected timestamps and a drift report.
    pub fn correct(
        timestamps: &[i64],
        expected_interval_ns: i64,
        strategy: ClockDriftStrategy,
    ) -> (Vec<i64>, DriftReport) {
        let result = match strategy {
            ClockDriftStrategy::None => (
                timestamps.to_vec(),
                DriftReport {
                    max_drift_ns: 0,
                    affected_ranges: Vec::new(),
                    corrections_applied: 0,
                },
            ),
            ClockDriftStrategy::Monotonic => Self::monotonic(timestamps),
            ClockDriftStrategy::Smooth => Self::smooth(timestamps, expected_interval_ns),
        };
        metrics::counter!("chronix_clock_drift_corrections_total")
            .increment(result.1.corrections_applied as u64);
        result
    }

    /// Monotonic enforcement: snap backward/duplicate timestamps forward.
    fn monotonic(timestamps: &[i64]) -> (Vec<i64>, DriftReport) {
        if timestamps.is_empty() {
            return (
                Vec::new(),
                DriftReport {
                    max_drift_ns: 0,
                    affected_ranges: Vec::new(),
                    corrections_applied: 0,
                },
            );
        }

        let mut corrected = Vec::with_capacity(timestamps.len());
        corrected.push(timestamps[0]);

        let mut max_drift: i64 = 0;
        let mut corrections = 0usize;
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        let mut in_range = false;
        let mut range_start = 0usize;

        for i in 1..timestamps.len() {
            if timestamps[i] <= corrected[i - 1] {
                let drift = corrected[i - 1] - timestamps[i] + 1;
                max_drift = max_drift.max(drift);
                corrected.push(corrected[i - 1] + 1);
                corrections += 1;

                if !in_range {
                    range_start = i;
                    in_range = true;
                }
            } else {
                corrected.push(timestamps[i]);
                if in_range {
                    ranges.push((range_start, i - 1));
                    in_range = false;
                }
            }
        }

        if in_range {
            ranges.push((range_start, timestamps.len() - 1));
        }

        (
            corrected,
            DriftReport {
                max_drift_ns: max_drift,
                affected_ranges: ranges,
                corrections_applied: corrections,
            },
        )
    }

    /// Smooth correction: detect gradual drift via regression on deltas,
    /// apply linear correction.
    fn smooth(timestamps: &[i64], expected_interval_ns: i64) -> (Vec<i64>, DriftReport) {
        if timestamps.len() < 3 {
            return (
                timestamps.to_vec(),
                DriftReport {
                    max_drift_ns: 0,
                    affected_ranges: Vec::new(),
                    corrections_applied: 0,
                },
            );
        }

        let n = timestamps.len();
        let deltas: Vec<f64> = timestamps
            .windows(2)
            .map(|w| (w[1] - w[0]) as f64)
            .collect();

        // Linear regression on deltas: delta[i] = a + b*i
        // If b is significant, there's gradual drift
        let n_d = deltas.len() as f64;
        let sum_x: f64 = (0..deltas.len()).map(|i| i as f64).sum();
        let sum_y: f64 = deltas.iter().sum();
        let sum_xy: f64 = deltas.iter().enumerate().map(|(i, &d)| i as f64 * d).sum();
        let sum_xx: f64 = (0..deltas.len()).map(|i| (i as f64).powi(2)).sum();

        let denom = n_d * sum_xx - sum_x * sum_x;
        if denom.abs() < 1e-10 {
            return (
                timestamps.to_vec(),
                DriftReport {
                    max_drift_ns: 0,
                    affected_ranges: Vec::new(),
                    corrections_applied: 0,
                },
            );
        }

        let slope = (n_d * sum_xy - sum_x * sum_y) / denom;
        let expected = expected_interval_ns as f64;

        // Only correct if drift rate is significant (> 0.1% per point)
        if slope.abs() < expected * 0.001 {
            return (
                timestamps.to_vec(),
                DriftReport {
                    max_drift_ns: 0,
                    affected_ranges: Vec::new(),
                    corrections_applied: 0,
                },
            );
        }

        // Blend original and ideal timestamps proportionally to local drift magnitude.
        // Timestamps with near-zero drift are preserved; those with large drift are
        // pulled toward the ideal position.  The threshold for full correction is set
        // to `expected_interval_ns` — drift ≥ 1 full interval → fully corrected.
        let threshold = expected;
        let mut corrected = Vec::with_capacity(n);
        corrected.push(timestamps[0]);
        let mut max_drift: i64 = 0;
        for i in 1..n {
            let ideal =
                timestamps[0].saturating_add((i as i64).saturating_mul(expected_interval_ns));
            let local_drift = (timestamps[i] - ideal).abs();
            max_drift = max_drift.max(local_drift);
            let alpha = if threshold > 0.0 {
                (local_drift as f64 / threshold).clamp(0.0, 1.0)
            } else {
                0.0
            };
            // alpha=0 → keep original, alpha=1 → fully corrected
            let blended =
                ((1.0 - alpha) * timestamps[i] as f64 + alpha * ideal as f64).round() as i64;
            corrected.push(blended);
        }

        // Count how many timestamps actually changed
        let corrections = (0..n).filter(|&i| timestamps[i] != corrected[i]).count();

        (
            corrected,
            DriftReport {
                max_drift_ns: max_drift,
                affected_ranges: if max_drift > 0 {
                    vec![(0, n - 1)]
                } else {
                    Vec::new()
                },
                corrections_applied: corrections,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_correction() {
        let ts = vec![0, 100, 200, 300];
        let (corrected, report) = ClockDriftDetector::correct(&ts, 100, ClockDriftStrategy::None);
        assert_eq!(corrected, ts);
        assert_eq!(report.corrections_applied, 0);
    }

    #[test]
    fn monotonic_no_issue() {
        let ts = vec![0, 100, 200, 300];
        let (corrected, report) =
            ClockDriftDetector::correct(&ts, 100, ClockDriftStrategy::Monotonic);
        assert_eq!(corrected, ts);
        assert_eq!(report.corrections_applied, 0);
    }

    #[test]
    fn monotonic_backward_timestamps() {
        let ts = vec![0, 100, 50, 200]; // 50 < 100 → snap forward
        let (corrected, report) =
            ClockDriftDetector::correct(&ts, 100, ClockDriftStrategy::Monotonic);
        assert!(corrected[2] > corrected[1]); // monotonically increasing
        assert_eq!(corrected[2], 101); // snapped to 100+1
        assert_eq!(report.corrections_applied, 1);
        assert_eq!(report.affected_ranges.len(), 1);
    }

    #[test]
    fn monotonic_duplicate_timestamps() {
        let ts = vec![0, 100, 100, 100, 200];
        let (corrected, report) =
            ClockDriftDetector::correct(&ts, 100, ClockDriftStrategy::Monotonic);
        // Each duplicate gets snapped forward by 1
        assert_eq!(corrected[2], 101);
        assert_eq!(corrected[3], 102);
        assert_eq!(report.corrections_applied, 2);
    }

    #[test]
    fn smooth_gradual_drift() {
        // Simulate gradual drift: expected interval 100ns, actual drifts by +10 each step
        let expected = 100;
        let ts: Vec<i64> = (0..20)
            .map(|i| i * expected + i * i * 5) // quadratic drift
            .collect();
        let (corrected, report) =
            ClockDriftDetector::correct(&ts, expected, ClockDriftStrategy::Smooth);
        // Corrected timestamps should be closer to expected interval than originals
        let original_var: f64 = ts
            .windows(2)
            .map(|w| ((w[1] - w[0]) as f64 - expected as f64).powi(2))
            .sum();
        let corrected_var: f64 = corrected
            .windows(2)
            .map(|w| ((w[1] - w[0]) as f64 - expected as f64).powi(2))
            .sum();
        assert!(
            corrected_var < original_var,
            "corrected intervals should have less variance"
        );
        assert!(report.corrections_applied > 0);
    }

    #[test]
    fn smooth_no_drift() {
        let ts: Vec<i64> = (0..10).map(|i| i * 1000).collect();
        let (corrected, report) =
            ClockDriftDetector::correct(&ts, 1000, ClockDriftStrategy::Smooth);
        assert_eq!(corrected, ts);
        assert_eq!(report.corrections_applied, 0);
    }

    #[test]
    fn empty_timestamps() {
        let (corrected, report) =
            ClockDriftDetector::correct(&[], 100, ClockDriftStrategy::Monotonic);
        assert!(corrected.is_empty());
        assert_eq!(report.corrections_applied, 0);
    }
}
