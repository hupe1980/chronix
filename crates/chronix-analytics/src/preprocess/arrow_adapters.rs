//! Arrow-native wrappers for preprocessing operations.
//!
//! These adapters provide zero-copy interop between Arrow `Float64Array` /
//! `Int64Array` and the slice-based preprocessing functions.  Input arrays
//! are accessed via `.values()` (O(1), no copy), and output arrays are
//! constructed from the resulting `Vec`s.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};

use crate::preprocess::{Interpolator, PreprocessConfig, PreprocessPipeline, Smoother};

/// Result of an Arrow-native preprocessing operation.
#[derive(Debug)]
pub struct ArrowPreprocessResult {
    /// Processed values as Arrow Float64Array.
    pub values: Float64Array,
    /// Aligned timestamps as Arrow Int64Array.
    pub timestamps: Int64Array,
    /// Number of gaps filled during interpolation.
    pub gaps_filled: usize,
}

impl ArrowPreprocessResult {
    /// Returns the values column as an `ArrayRef` (for DataFusion interop).
    pub fn values_array_ref(&self) -> ArrayRef {
        Arc::new(self.values.clone())
    }

    /// Returns the timestamps column as an `ArrayRef`.
    pub fn timestamps_array_ref(&self) -> ArrayRef {
        Arc::new(self.timestamps.clone())
    }
}

/// Interpolate gap-fills on Arrow arrays — zero-copy input, new array output.
pub fn arrow_interpolate(
    interpolator: &Interpolator,
    timestamps: &Int64Array,
    values: &Float64Array,
    expected_interval_ns: i64,
) -> Result<ArrowPreprocessResult, crate::preprocess::interpolation::InterpolationError> {
    // Zero-copy slice access
    let ts_slice = timestamps.values().as_ref();
    let val_slice = values.values().as_ref();

    let (out_ts, out_vals, gaps_filled, _gaps_skipped) =
        interpolator.fill(ts_slice, val_slice, expected_interval_ns)?;

    Ok(ArrowPreprocessResult {
        values: Float64Array::from(out_vals),
        timestamps: Int64Array::from(out_ts),
        gaps_filled,
    })
}

/// Smooth an Arrow Float64Array — zero-copy input.
pub fn arrow_smooth(smoother: &Smoother, values: &Float64Array) -> Float64Array {
    let slice = values.values().as_ref();
    Float64Array::from(smoother.smooth(slice))
}

/// Run the full preprocessing pipeline on Arrow arrays.
pub fn arrow_preprocess(
    config: &PreprocessConfig,
    timestamps: &Int64Array,
    values: &Float64Array,
) -> Result<ArrowPreprocessResult, crate::preprocess::PreprocessError> {
    let ts_slice = timestamps.values().as_ref();
    let val_slice = values.values().as_ref();

    let result = PreprocessPipeline::run(config, ts_slice, val_slice)?;

    Ok(ArrowPreprocessResult {
        values: Float64Array::from(result.values),
        timestamps: Int64Array::from(result.timestamps),
        gaps_filled: result.gaps_filled,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_interpolate_zero_copy() {
        let ts = Int64Array::from(vec![0, 1_000_000_000, 3_000_000_000, 4_000_000_000]);
        let vals = Float64Array::from(vec![1.0, 2.0, 4.0, 5.0]);

        let result = arrow_interpolate(&Interpolator::Linear, &ts, &vals, 1_000_000_000).unwrap();

        // Gap at t=2s should be filled
        assert!(result.gaps_filled > 0);
        assert!(result.values.len() >= 4);
        assert!(result.timestamps.len() == result.values.len());
    }

    #[test]
    fn arrow_smooth_produces_float64array() {
        let vals = Float64Array::from(vec![1.0, 3.0, 5.0, 7.0, 9.0, 11.0]);
        let smoother = Smoother::MovingAverage { window: 3 };
        let smoothed = arrow_smooth(&smoother, &vals);
        assert_eq!(smoothed.len(), vals.len());
        // Centered moving average at index 2: mean of values[1..4] = (3+5+7)/3 = 5.0
        assert!((smoothed.value(2) - 5.0).abs() < 1e-10);
    }

    #[test]
    fn arrow_preprocess_pipeline() {
        let ts = Int64Array::from(vec![0i64, 1_000_000_000, 2_000_000_000, 3_000_000_000]);
        let vals = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);

        let config = PreprocessConfig {
            smoothing: Some(Smoother::Exponential { alpha: 0.3 }),
            ..PreprocessConfig::default()
        };

        let result = arrow_preprocess(&config, &ts, &vals).unwrap();
        assert_eq!(result.values.len(), 4);
    }

    #[test]
    fn arrow_result_array_refs() {
        let result = ArrowPreprocessResult {
            values: Float64Array::from(vec![1.0, 2.0]),
            timestamps: Int64Array::from(vec![100, 200]),
            gaps_filled: 0,
        };
        let val_ref = result.values_array_ref();
        assert_eq!(val_ref.len(), 2);
        let ts_ref = result.timestamps_array_ref();
        assert_eq!(ts_ref.len(), 2);
    }
}
