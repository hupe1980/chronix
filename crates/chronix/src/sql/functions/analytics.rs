//! Analytics window functions: anomaly scoring, correlation and multivariate
//! detection.
//!
//! Like the feature-engineering functions these read a whole partition — see
//! [`window`](super::window). Forecasting lives in
//! [`aggregates`](super::aggregates) instead, because a forecast produces
//! *new* rows rather than a value for each existing one.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DFResult};

use super::window::{column_f64, constant_f64, exec_err, f64_array, on_dense, PartitionKernel};

/// Synthetic uniform timestamps for the routines that want one per sample.
///
/// The partition is already in the order the `OVER` clause asked for, and
/// these routines use timestamps only to establish that order, so the index is
/// the honest encoding of what is known. Feeding them the real column would
/// imply the model handles irregular spacing, which it does not.
fn positional_timestamps(n: usize) -> Vec<i64> {
    (0..n as i64)
        .map(|i| i.saturating_mul(1_000_000_000))
        .collect()
}

macro_rules! kernel {
    (
        $(#[$meta:meta])*
        $name:ident, $sql:literal, [$($ty:expr),* $(,)?]
        , |$args:ident, $rows:ident| $body:block
    ) => {
        $(#[$meta])*
        #[derive(Debug)]
        pub(super) struct $name;

        impl PartitionKernel for $name {
            const NAME: &'static str = $sql;
            fn arg_types() -> Vec<DataType> {
                vec![$($ty),*]
            }
            fn evaluate($args: &[ArrayRef], $rows: usize) -> DFResult<ArrayRef> $body
        }
    };
}

kernel! {
    /// `anomaly_score(values, threshold)` — Z-score anomaly score per row,
    /// against the partition's own mean and standard deviation.
    AnomalyScoreKernel, "anomaly_score", [DataType::Float64, DataType::Float64],
    |args, _rows| {
        use chronix_analytics::anomaly::{AnomalyDetector, ZScoreDetector};

        let values = column_f64(args, 0)?;
        let threshold = constant_f64(args, 1, "anomaly_score threshold")?;
        let out = on_dense(&values, |dense| {
            if dense.len() < 3 {
                return Err(DataFusionError::Plan(
                    "anomaly_score: need at least 3 non-null rows in the partition".into(),
                ));
            }
            let ts = positional_timestamps(dense.len());
            let mut detector = ZScoreDetector::new(Some(threshold));
            detector.fit(&ts, dense).map_err(exec_err)?;
            Ok(detector
                .detect(&ts, dense)
                .map_err(exec_err)?
                .iter()
                .map(|s| s.score)
                .collect())
        })?;
        Ok(f64_array(out))
    }
}

kernel! {
    /// `correlation(a, b)` — Pearson correlation over the partition, repeated
    /// on every row of it.
    ///
    /// A window function rather than an aggregate so it can sit beside the
    /// rows it summarises without a join.
    CorrelationKernel, "correlation", [DataType::Float64, DataType::Float64],
    |args, rows| {
        use chronix_analytics::multivariate::PearsonCorrelation;

        let a = column_f64(args, 0)?;
        let b = column_f64(args, 1)?;
        let r = PearsonCorrelation::compute(&a, &b).map_err(exec_err)?;
        Ok(f64_array(vec![r; rows]))
    }
}

kernel! {
    /// `cross_correlation(a, b, lag)` — correlation of `a` with `b` shifted by
    /// `lag` rows, repeated on every row of the partition.
    ///
    /// One lag, the one asked for: computing a range and returning one of it
    /// makes the argument change the cost and not the answer.
    CrossCorrelationKernel, "cross_correlation",
    [DataType::Float64, DataType::Float64, DataType::Int64],
    |args, rows| {
        use chronix_analytics::multivariate::LagCorrelation;

        let a = column_f64(args, 0)?;
        let b = column_f64(args, 1)?;
        let arr = args.get(2).ok_or_else(|| {
            DataFusionError::Internal("cross_correlation: missing lag".into())
        })?;
        let lag_col = arr
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| DataFusionError::Plan("cross_correlation: lag must be an integer".into()))?;
        if lag_col.is_empty() || lag_col.is_null(0) {
            return Err(DataFusionError::Plan(
                "cross_correlation: lag must not be NULL".into(),
            ));
        }
        let lag = i32::try_from(lag_col.value(0)).map_err(|_| {
            DataFusionError::Plan(format!(
                "cross_correlation: lag {} is out of range",
                lag_col.value(0)
            ))
        })?;

        let results = LagCorrelation::compute(&a, &b, &[lag]).map_err(exec_err)?;
        let r = results.first().map_or(f64::NAN, |(_, v)| *v);
        Ok(f64_array(vec![r; rows]))
    }
}

kernel! {
    /// `multivariate_anomaly(a, b, threshold)` — Mahalanobis distance of each
    /// row's `(a, b)` pair from the partition's joint distribution.
    MultivariateAnomalyKernel, "multivariate_anomaly",
    [DataType::Float64, DataType::Float64, DataType::Float64],
    |args, rows| {
        use chronix_analytics::multivariate::{
            ColumnarMatrix, MahalanobisDetector, MultiSeriesContext, MultivariateAnomalyDetector,
        };

        let a = column_f64(args, 0)?;
        let b = column_f64(args, 1)?;
        let threshold = constant_f64(args, 2, "multivariate_anomaly threshold")?;
        if rows < 3 {
            return Err(DataFusionError::Plan(
                "multivariate_anomaly: need at least 3 rows in the partition".into(),
            ));
        }

        let ctx = MultiSeriesContext {
            matrix: ColumnarMatrix {
                data: vec![a, b],
                timestamps: positional_timestamps(rows),
                series_ids: vec!["a".into(), "b".into()],
            },
        };
        let mut detector = MahalanobisDetector::new(Some(threshold), None);
        detector.fit(&ctx).map_err(exec_err)?;
        let scores = detector.detect(&ctx).map_err(exec_err)?;
        Ok(f64_array(scores.iter().map(|s| s.score).collect()))
    }
}
