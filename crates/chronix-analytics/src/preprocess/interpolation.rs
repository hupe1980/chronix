//! Interpolation methods for filling gaps in time-series data.

use thiserror::Error;

/// Errors that can occur during interpolation.
#[derive(Debug, Error)]
pub enum InterpolationError {
    /// Catmull-Rom interpolation requires strictly monotonically increasing timestamps.
    #[error(
        "timestamps are not strictly monotonically increasing at index {index}: \
         prev={prev}, curr={curr}"
    )]
    NonMonotonicTimestamps {
        /// Index of the offending timestamp.
        index: usize,
        /// Previous timestamp value.
        prev: i64,
        /// Current timestamp value.
        curr: i64,
    },
}

/// Interpolation strategy for gap filling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Interpolator {
    /// Linearly interpolated values between known points.
    Linear,
    /// Nearest neighbor fill.
    Nearest,
    /// Catmull-Rom spline (C1 local interpolation using 4 surrounding points, falls back to Linear).
    ///
    /// # Monotonic timestamps requirement
    ///
    /// The Catmull-Rom interpolation assumes that input timestamps are
    /// **strictly monotonically increasing**.  If timestamps are
    /// out-of-order or contain duplicates, the interpolated values will
    /// be incorrect (the parametric `t` fraction becomes negative or
    /// > 1).  Callers should sort / deduplicate timestamps before
    /// > interpolation, or apply `ClockDriftStrategy::Monotonic`
    /// > correction first.
    CatmullRom,
    /// Fill with 0.0.
    Zero,
    /// Last known value carried forward.
    Forward,
    /// Next observation carried backward (Nocb).
    Nocb,
}

impl Interpolator {
    /// Fills gaps in the data using this interpolation method.
    ///
    /// `timestamps` and `values` are the original (sparse) data.
    /// `expected_interval_ns` is the expected interval between points.
    ///
    /// Returns `(filled_timestamps, filled_values, gaps_filled_count, gaps_skipped_count)`.
    ///
    /// `gaps_skipped_count` reports how many gap segments exceeded
    /// `MAX_GAP_FILL` and were left unfilled, so callers can detect incomplete
    /// interpolation without parsing logs.
    pub fn fill(
        &self,
        timestamps: &[i64],
        values: &[f64],
        expected_interval_ns: i64,
    ) -> Result<(Vec<i64>, Vec<f64>, usize, usize), InterpolationError> {
        if timestamps.len() < 2 || expected_interval_ns <= 0 {
            return Ok((timestamps.to_vec(), values.to_vec(), 0, 0));
        }

        // Catmull-Rom requires strictly monotonically increasing timestamps.
        if *self == Interpolator::CatmullRom {
            for i in 1..timestamps.len() {
                if timestamps[i] <= timestamps[i - 1] {
                    return Err(InterpolationError::NonMonotonicTimestamps {
                        index: i,
                        prev: timestamps[i - 1],
                        curr: timestamps[i],
                    });
                }
            }
        }

        // Estimate output size from actual timestamp span vs expected interval.
        // For data with no gaps this equals timestamps.len(); for gappy data
        // it's the number of expected intervals. Avoids blind 2x over-allocation.
        let span = timestamps[timestamps.len() - 1] - timestamps[0];
        let estimated = if expected_interval_ns > 0 && span > 0 {
            let est = (span / expected_interval_ns) as usize + 1;
            est.max(timestamps.len())
        } else {
            timestamps.len()
        };
        let mut out_ts = Vec::with_capacity(estimated);
        let mut out_vals = Vec::with_capacity(estimated);
        let mut gaps_filled = 0usize;
        let mut gaps_skipped = 0usize;

        out_ts.push(timestamps[0]);
        out_vals.push(values[0]);

        for i in 1..timestamps.len() {
            let delta = timestamps[i] - timestamps[i - 1];
            let steps = (delta as f64 / expected_interval_ns as f64).round() as i64;

            // Cap gap fill to prevent OOM on huge gaps (e.g. 1-day gap at 1-ns resolution).
            const MAX_GAP_FILL: i64 = 100_000;
            if steps > MAX_GAP_FILL {
                tracing::warn!(
                    gap_steps = steps,
                    max = MAX_GAP_FILL,
                    idx = i,
                    "interpolation gap exceeds MAX_GAP_FILL; skipping fill",
                );
                gaps_skipped += 1;
            }
            if steps > 1 && steps <= MAX_GAP_FILL {
                // Fill gap
                for step in 1..steps {
                    let t =
                        timestamps[i - 1].saturating_add(step.saturating_mul(expected_interval_ns));
                    let v = self.interpolate_value(
                        timestamps,
                        values,
                        i - 1,
                        i,
                        step as f64 / steps as f64,
                    );
                    out_ts.push(t);
                    out_vals.push(v);
                    gaps_filled += 1;
                }
            }

            out_ts.push(timestamps[i]);
            out_vals.push(values[i]);
        }

        Ok((out_ts, out_vals, gaps_filled, gaps_skipped))
    }

    /// Compute the interpolated value at fraction `frac` between points `before_idx` and `after_idx`.
    #[allow(clippy::trivially_copy_pass_by_ref)] // &self method signature symmetry
    fn interpolate_value(
        &self,
        timestamps: &[i64],
        values: &[f64],
        before_idx: usize,
        after_idx: usize,
        frac: f64,
    ) -> f64 {
        let v0 = values[before_idx];
        let v1 = values[after_idx];

        match self {
            Interpolator::Linear => v0 + (v1 - v0) * frac,
            Interpolator::Nearest => {
                if frac < 0.5 {
                    v0
                } else {
                    v1
                }
            }
            Interpolator::CatmullRom => {
                // Non-uniform Catmull-Rom spline using actual timestamp spacing.
                let n = values.len();
                if before_idx > 0 && after_idx + 1 < n {
                    let p0 = values[before_idx - 1];
                    let p1 = v0;
                    let p2 = v1;
                    let p3 = values[after_idx + 1];
                    let t0 = timestamps[before_idx - 1] as f64;
                    let t1 = timestamps[before_idx] as f64;
                    let t2 = timestamps[after_idx] as f64;
                    let t3 = timestamps[after_idx + 1] as f64;
                    // Map frac to actual time position between t1 and t2.
                    let t = t1 + (t2 - t1) * frac;
                    catmull_rom_nonuniform(p0, p1, p2, p3, t0, t1, t2, t3, t)
                } else {
                    // Fallback to linear if not enough surrounding points
                    v0 + (v1 - v0) * frac
                }
            }
            Interpolator::Zero => 0.0,
            Interpolator::Forward => v0,
            Interpolator::Nocb => v1,
        }
    }
}

/// Non-uniform Catmull-Rom (Barry & Goldman) using actual knot times.
///
/// Gracefully reduces to standard uniform Catmull-Rom when knots are
/// equally spaced.
#[allow(clippy::too_many_arguments)] // 4 control points + 4 knots + t: the textbook signature
fn catmull_rom_nonuniform(
    p0: f64,
    p1: f64,
    p2: f64,
    p3: f64,
    t0: f64,
    t1: f64,
    t2: f64,
    t3: f64,
    t: f64,
) -> f64 {
    let dt10 = t1 - t0;
    let dt21 = t2 - t1;
    let dt32 = t3 - t2;
    // Guard against zero-length intervals.
    if dt21.abs() < 1e-12 {
        return p1;
    }
    let safe = |d: f64, fallback: f64| if d.abs() < 1e-12 { fallback } else { d };
    let dt10 = safe(dt10, dt21);
    let dt32 = safe(dt32, dt21);

    // Tangents at p1 and p2 (centripetal-like formulation).
    let m1 = (p2 - p1) / dt21 + (p1 - p0) / dt10 - (p2 - p0) / (dt10 + dt21);
    let m2 = (p2 - p1) / dt21 + (p3 - p2) / dt32 - (p3 - p1) / (dt21 + dt32);
    // Scale tangents by the interval length.
    let mut m1 = m1 * dt21;
    let mut m2 = m2 * dt21;

    // Fritsch-Carlson monotonicity clamping.
    // Prevents overshoot/undershoot that violates data monotonicity.
    let delta = p2 - p1;
    if delta.abs() < 1e-30 {
        // Flat section — force zero tangents.
        m1 = 0.0;
        m2 = 0.0;
    } else {
        // If tangent sign opposes the secant, clamp to zero.
        if m1 * delta < 0.0 {
            m1 = 0.0;
        }
        if m2 * delta < 0.0 {
            m2 = 0.0;
        }
        // Bound tangent magnitudes: alpha^2 + beta^2 <= 9
        let alpha = m1 / delta;
        let beta = m2 / delta;
        let r2 = alpha * alpha + beta * beta;
        if r2 > 9.0 {
            let phi = 3.0 / r2.sqrt();
            m1 = phi * alpha * delta;
            m2 = phi * beta * delta;
        }
    }

    let u = (t - t1) / dt21;
    let u2 = u * u;
    let u3 = u2 * u;
    // Hermite basis
    (2.0 * u3 - 3.0 * u2 + 1.0) * p1
        + (u3 - 2.0 * u2 + u) * m1
        + (-2.0 * u3 + 3.0 * u2) * p2
        + (u3 - u2) * m2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ts(secs: &[i64]) -> Vec<i64> {
        secs.iter().map(|&s| s * 1_000_000_000).collect()
    }

    #[test]
    fn linear_interpolation() {
        let ts = make_ts(&[0, 1, 4, 5]); // gap between index 1 and 2
        let vals = vec![10.0, 20.0, 50.0, 60.0];
        let (out_ts, out_vals, filled, _) = Interpolator::Linear
            .fill(&ts, &vals, 1_000_000_000)
            .unwrap();
        assert_eq!(filled, 2); // 2 points filled
        assert_eq!(out_ts.len(), 6);
        // Interpolated at t=2s: 20 + (50-20)*1/3 = 30
        assert!((out_vals[2] - 30.0).abs() < 1e-6);
        // Interpolated at t=3s: 20 + (50-20)*2/3 = 40
        assert!((out_vals[3] - 40.0).abs() < 1e-6);
    }

    #[test]
    fn nearest_interpolation() {
        let ts = make_ts(&[0, 1, 4, 5]);
        let vals = vec![10.0, 20.0, 50.0, 60.0];
        let (_, out_vals, filled, _) = Interpolator::Nearest
            .fill(&ts, &vals, 1_000_000_000)
            .unwrap();
        assert_eq!(filled, 2);
        assert!((out_vals[2] - 20.0).abs() < 1e-6); // frac=1/3 < 0.5 → before
        assert!((out_vals[3] - 50.0).abs() < 1e-6); // frac=2/3 >= 0.5 → after
    }

    #[test]
    fn zero_fill() {
        let ts = make_ts(&[0, 3]);
        let vals = vec![10.0, 40.0];
        let (_, out_vals, filled, _) = Interpolator::Zero.fill(&ts, &vals, 1_000_000_000).unwrap();
        assert_eq!(filled, 2);
        assert!((out_vals[1]).abs() < 1e-10);
        assert!((out_vals[2]).abs() < 1e-10);
    }

    #[test]
    fn forward_fill() {
        let ts = make_ts(&[0, 3]);
        let vals = vec![10.0, 40.0];
        let (_, out_vals, filled, _) = Interpolator::Forward
            .fill(&ts, &vals, 1_000_000_000)
            .unwrap();
        assert_eq!(filled, 2);
        assert!((out_vals[1] - 10.0).abs() < 1e-10);
        assert!((out_vals[2] - 10.0).abs() < 1e-10);
    }

    #[test]
    fn nocb_fill() {
        let ts = make_ts(&[0, 3]);
        let vals = vec![10.0, 40.0];
        let (_, out_vals, filled, _) = Interpolator::Nocb.fill(&ts, &vals, 1_000_000_000).unwrap();
        assert_eq!(filled, 2);
        assert!((out_vals[1] - 40.0).abs() < 1e-10);
        assert!((out_vals[2] - 40.0).abs() < 1e-10);
    }

    #[test]
    fn spline_with_enough_points() {
        let ts = make_ts(&[0, 1, 2, 5, 6, 7]);
        let vals = vec![1.0, 2.0, 4.0, 10.0, 12.0, 15.0];
        let (_, out_vals, filled, _) = Interpolator::CatmullRom
            .fill(&ts, &vals, 1_000_000_000)
            .unwrap();
        assert_eq!(filled, 2); // 2 points fill between index 2 and 3
                               // Spline values should be between surrounding points
        assert!(out_vals[3] > 4.0 && out_vals[3] < 10.0);
        assert!(out_vals[4] > 4.0 && out_vals[4] < 10.0);
    }

    #[test]
    fn no_gaps_passthrough() {
        let ts = make_ts(&[0, 1, 2, 3]);
        let vals = vec![1.0, 2.0, 3.0, 4.0];
        let (out_ts, out_vals, filled, _) = Interpolator::Linear
            .fill(&ts, &vals, 1_000_000_000)
            .unwrap();
        assert_eq!(filled, 0);
        assert_eq!(out_ts, ts);
        assert_eq!(out_vals, vals);
    }

    #[test]
    fn empty_data() {
        let (_, _, filled, _) = Interpolator::Linear.fill(&[], &[], 1_000_000_000).unwrap();
        assert_eq!(filled, 0);
    }

    #[test]
    fn catmull_rom_rejects_non_monotonic_timestamps() {
        // Duplicate timestamp at index 2
        let ts = make_ts(&[0, 1, 1, 3, 4, 5]);
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let err = Interpolator::CatmullRom
            .fill(&ts, &vals, 1_000_000_000)
            .unwrap_err();
        assert!(
            matches!(
                err,
                InterpolationError::NonMonotonicTimestamps { index: 2, .. }
            ),
            "expected NonMonotonicTimestamps at index 2, got {err:?}"
        );

        // Out-of-order timestamp at index 3
        let ts2 = make_ts(&[0, 1, 5, 3, 6, 7]);
        let vals2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let err2 = Interpolator::CatmullRom
            .fill(&ts2, &vals2, 1_000_000_000)
            .unwrap_err();
        assert!(
            matches!(
                err2,
                InterpolationError::NonMonotonicTimestamps { index: 3, .. }
            ),
            "expected NonMonotonicTimestamps at index 3, got {err2:?}"
        );
    }

    #[test]
    fn catmull_rom_accepts_monotonic_timestamps() {
        let ts = make_ts(&[0, 1, 2, 5, 6, 7]);
        let vals = vec![1.0, 2.0, 4.0, 10.0, 12.0, 15.0];
        let (out_ts, out_vals, filled, _) = Interpolator::CatmullRom
            .fill(&ts, &vals, 1_000_000_000)
            .unwrap();
        assert_eq!(filled, 2);
        assert!(out_ts.len() > ts.len());
        // Spline values should be between surrounding points
        assert!(out_vals[3] > 4.0 && out_vals[3] < 10.0);
        assert!(out_vals[4] > 4.0 && out_vals[4] < 10.0);
    }

    #[test]
    fn cq02_catmull_rom_monotonicity_preserved() {
        // Monotonically increasing data — interpolated values must also be monotonic.
        let ts = make_ts(&[0, 1, 2, 5, 6, 7]);
        let vals = vec![1.0, 3.0, 5.0, 20.0, 22.0, 24.0];
        let (_, out_vals, filled, _) = Interpolator::CatmullRom
            .fill(&ts, &vals, 1_000_000_000)
            .unwrap();
        assert_eq!(filled, 2);
        // All adjacent values must be non-decreasing.
        for w in out_vals.windows(2) {
            assert!(
                w[1] >= w[0],
                "monotonicity violated: {} followed by {}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn cq03_gap_skipped_count_reported() {
        // Create a gap that will exceed MAX_GAP_FILL (100_000).
        // interval = 1ns, gap = 200_001 ns → 200_001 steps > 100_000.
        let ts = vec![0i64, 200_001];
        let vals = vec![1.0, 2.0];
        let (_, _, filled, skipped) = Interpolator::Linear.fill(&ts, &vals, 1).unwrap();
        assert_eq!(filled, 0, "should not fill a gap exceeding MAX_GAP_FILL");
        assert_eq!(skipped, 1, "should report 1 skipped gap");
    }
}
