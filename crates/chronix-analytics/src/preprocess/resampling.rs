//! Resampling — change the sampling rate of time-series data.

use crate::compute::simd_sum;
use crate::preprocess::interpolation::Interpolator;

/// Aggregation function for downsampling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AggregationFn {
    /// Arithmetic mean of values in the bucket.
    Mean,
    /// Minimum value in the bucket.
    Min,
    /// Maximum value in the bucket.
    Max,
    /// Sum of values in the bucket.
    Sum,
    /// Last (most recent) value in the bucket.
    Last,
    /// First (earliest) value in the bucket.
    First,
}

/// Configuration for resampling.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResampleConfig {
    /// Target interval in nanoseconds.
    pub target_interval_ns: i64,
    /// Aggregation function for downsampling.
    pub aggregation: AggregationFn,
    /// Interpolation method for upsampling.
    pub interpolation: Interpolator,
}

/// Resampler converts time-series data from one interval to another.
pub struct Resampler;

impl Resampler {
    /// Resample the given data to the target interval.
    ///
    /// - **Downscale** (reduce rate): groups by time boundaries, applies aggregation.
    /// - **Upscale** (increase rate): interpolates between points.
    pub fn resample(
        timestamps: &[i64],
        values: &[f64],
        config: &ResampleConfig,
    ) -> (Vec<i64>, Vec<f64>) {
        if timestamps.is_empty() {
            return (Vec::new(), Vec::new());
        }
        if timestamps.len() == 1 {
            return (timestamps.to_vec(), values.to_vec());
        }

        // Determine source interval (median delta)
        let source_interval = Self::estimate_interval(timestamps);

        if config.target_interval_ns > source_interval {
            Self::downscale(timestamps, values, config)
        } else if config.target_interval_ns < source_interval {
            Self::upscale(timestamps, values, config)
        } else {
            (timestamps.to_vec(), values.to_vec())
        }
    }

    /// Estimate the source interval from the timestamp data.
    fn estimate_interval(timestamps: &[i64]) -> i64 {
        if timestamps.len() < 2 {
            return 0;
        }
        let mut deltas: Vec<i64> = timestamps.windows(2).map(|w| w[1] - w[0]).collect();
        deltas.sort_unstable();
        deltas[deltas.len() / 2] // median
    }

    /// Downscale: group into buckets and aggregate using O(n) two-pointer scan.
    fn downscale(
        timestamps: &[i64],
        values: &[f64],
        config: &ResampleConfig,
    ) -> (Vec<i64>, Vec<f64>) {
        let interval = config.target_interval_ns;
        if interval <= 0 {
            return (timestamps.to_vec(), values.to_vec());
        }
        let start = timestamps[0];
        let end = timestamps[timestamps.len() - 1];

        // Use saturating arithmetic to prevent overflow when the time span
        // is huge relative to the interval (e.g. years of nanosecond data).
        let n_buckets = ((end - start) / interval).saturating_add(1) as usize;
        // Cap pre-allocation at input length to prevent OOM when
        // target_interval_ns is very small relative to the time span.
        let cap = n_buckets.min(timestamps.len());
        let mut out_ts = Vec::with_capacity(cap);
        let mut out_vals = Vec::with_capacity(cap);

        // Single-pass two-pointer: timestamps are sorted, so we advance a cursor.
        // When a bucket is empty (sparse data), skip directly to the bucket
        // containing the next timestamp instead of iterating empty buckets.
        let mut cursor = 0usize;
        let mut bucket_start = start;
        while bucket_start <= end && cursor < timestamps.len() {
            let bucket_end = bucket_start.saturating_add(interval);

            // Collect values in this bucket via advancing cursor
            let bucket_begin = cursor;
            while cursor < timestamps.len()
                && timestamps[cursor] >= bucket_start
                && timestamps[cursor] < bucket_end
            {
                cursor += 1;
            }
            let bucket_vals = &values[bucket_begin..cursor];

            if !bucket_vals.is_empty() {
                let agg = match config.aggregation {
                    AggregationFn::Mean => simd_sum(bucket_vals) / bucket_vals.len() as f64,
                    AggregationFn::Min => bucket_vals.iter().copied().fold(f64::INFINITY, f64::min),
                    AggregationFn::Max => bucket_vals
                        .iter()
                        .copied()
                        .fold(f64::NEG_INFINITY, f64::max),
                    AggregationFn::Sum => simd_sum(bucket_vals),
                    AggregationFn::Last => bucket_vals[bucket_vals.len() - 1],
                    AggregationFn::First => bucket_vals[0],
                };
                out_ts.push(bucket_start);
                out_vals.push(agg);
                bucket_start = bucket_end;
            } else if cursor < timestamps.len() {
                // Empty bucket with more data ahead — skip directly to the
                // bucket containing the next timestamp (O(1) instead of
                // iterating through potentially millions of empty buckets).
                // Use overflow-safe arithmetic: offset - offset.rem_euclid(interval)
                let offset = timestamps[cursor] - start;
                bucket_start = start.saturating_add(offset - offset.rem_euclid(interval));
            } else {
                // No more data points — done.
                break;
            }
        }

        (out_ts, out_vals)
    }

    /// Upscale: interpolate to fill in higher-frequency points.
    fn upscale(
        timestamps: &[i64],
        values: &[f64],
        config: &ResampleConfig,
    ) -> (Vec<i64>, Vec<f64>) {
        // Linear interpolation never returns an error, but fall back to
        // the original data if it somehow does rather than panicking.
        match config
            .interpolation
            .fill(timestamps, values, config.target_interval_ns)
        {
            Ok((ts, vals, _, _)) => (ts, vals),
            Err(_) => (timestamps.to_vec(), values.to_vec()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ts(secs: &[i64]) -> Vec<i64> {
        secs.iter().map(|&s| s * 1_000_000_000).collect()
    }

    #[test]
    fn downscale_mean() {
        // 1s data → 3s downscale (mean)
        let ts = make_ts(&[0, 1, 2, 3, 4, 5, 6, 7, 8]);
        let vals: Vec<f64> = (0..9).map(|i| i as f64 * 10.0).collect();
        let config = ResampleConfig {
            target_interval_ns: 3_000_000_000,
            aggregation: AggregationFn::Mean,
            interpolation: Interpolator::Linear,
        };
        let (out_ts, out_vals) = Resampler::resample(&ts, &vals, &config);
        assert_eq!(out_ts.len(), 3);
        // Bucket [0,3): values 0,10,20 → mean=10
        assert!((out_vals[0] - 10.0).abs() < 1e-10);
        // Bucket [3,6): values 30,40,50 → mean=40
        assert!((out_vals[1] - 40.0).abs() < 1e-10);
        // Bucket [6,9): values 60,70,80 → mean=70
        assert!((out_vals[2] - 70.0).abs() < 1e-10);
    }

    #[test]
    fn downscale_min_max() {
        let ts = make_ts(&[0, 1, 2, 3, 4, 5]);
        let vals = vec![5.0, 1.0, 8.0, 3.0, 9.0, 2.0];
        let min_config = ResampleConfig {
            target_interval_ns: 3_000_000_000,
            aggregation: AggregationFn::Min,
            interpolation: Interpolator::Linear,
        };
        let (_, min_vals) = Resampler::resample(&ts, &vals, &min_config);
        assert!((min_vals[0] - 1.0).abs() < 1e-10);

        let max_config = ResampleConfig {
            target_interval_ns: 3_000_000_000,
            aggregation: AggregationFn::Max,
            interpolation: Interpolator::Linear,
        };
        let (_, max_vals) = Resampler::resample(&ts, &vals, &max_config);
        assert!((max_vals[0] - 8.0).abs() < 1e-10);
    }

    #[test]
    fn empty_data() {
        let config = ResampleConfig {
            target_interval_ns: 1_000_000_000,
            aggregation: AggregationFn::Mean,
            interpolation: Interpolator::Linear,
        };
        let (ts, vals) = Resampler::resample(&[], &[], &config);
        assert!(ts.is_empty());
        assert!(vals.is_empty());
    }

    #[test]
    fn same_interval_passthrough() {
        let ts = make_ts(&[0, 1, 2, 3]);
        let vals = vec![1.0, 2.0, 3.0, 4.0];
        let config = ResampleConfig {
            target_interval_ns: 1_000_000_000,
            aggregation: AggregationFn::Mean,
            interpolation: Interpolator::Linear,
        };
        let (out_ts, out_vals) = Resampler::resample(&ts, &vals, &config);
        assert_eq!(out_ts, ts);
        assert_eq!(out_vals, vals);
    }

    #[test]
    fn sparse_downscale_skips_empty_buckets() {
        // Two data points separated by a huge gap (1 000 000 seconds apart)
        // with 1s downscale interval. Without skip-ahead, this would iterate
        // ~1 000 000 empty buckets. With the fix, it completes in O(n).
        let ts = make_ts(&[0, 1_000_000]);
        let vals = vec![10.0, 20.0];
        let config = ResampleConfig {
            target_interval_ns: 1_000_000_000, // 1s buckets
            aggregation: AggregationFn::Mean,
            interpolation: Interpolator::Linear,
        };
        let start = std::time::Instant::now();
        let (out_ts, out_vals) = Resampler::resample(&ts, &vals, &config);
        let elapsed = start.elapsed();

        // Should produce exactly 2 output buckets
        assert_eq!(out_ts.len(), 2);
        assert!((out_vals[0] - 10.0).abs() < 1e-10);
        assert!((out_vals[1] - 20.0).abs() < 1e-10);

        // Must complete in well under 1 second (linear scan would take ~1M iterations)
        assert!(
            elapsed.as_millis() < 100,
            "Sparse downscale took {}ms — skip-ahead not working",
            elapsed.as_millis()
        );
    }

    #[test]
    fn sparse_downscale_correctness() {
        // Three clusters of data with gaps: [0,1,2], [100,101], [500]
        let ts = make_ts(&[0, 1, 2, 100, 101, 500]);
        let vals = vec![1.0, 2.0, 3.0, 10.0, 11.0, 50.0];
        let config = ResampleConfig {
            target_interval_ns: 3_000_000_000, // 3s buckets
            aggregation: AggregationFn::Mean,
            interpolation: Interpolator::Linear,
        };
        let (out_ts, out_vals) = Resampler::resample(&ts, &vals, &config);

        // Bucket [0,3): {1,2,3} → mean=2.0
        // Bucket [99,102): {10,11} → mean=10.5
        // Bucket [498,501): {50} → mean=50.0
        assert_eq!(out_ts.len(), 3);
        assert!((out_vals[0] - 2.0).abs() < 1e-10);
        assert!((out_vals[1] - 10.5).abs() < 1e-10);
        assert!((out_vals[2] - 50.0).abs() < 1e-10);
    }
}
