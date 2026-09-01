//! Feature-engineering window functions.
//!
//! Every function here reads its whole partition — see
//! [`window`](super::window) for why they are window functions and not scalar
//! ones. Each is called as
//!
//! ```sql
//! diff(usage, 1) OVER (PARTITION BY host ORDER BY _time)
//! ```

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use datafusion::common::Result as DFResult;

use super::window::{
    column_f64, constant_f64, constant_usize, exec_err, f64_array, on_dense, PartitionKernel,
};

macro_rules! kernel {
    (
        $(#[$meta:meta])*
        $name:ident, $sql:literal, [$($ty:expr),* $(,)?]
        $(, returns: $ret:expr)?
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

            $(fn return_type() -> DataType { $ret })?

            fn evaluate($args: &[ArrayRef], $rows: usize) -> DFResult<ArrayRef> $body
        }
    };
}

kernel! {
    /// `diff(values, order)` — n-th order differencing.
    ///
    /// The first `order` rows of the partition are NULL: there is nothing
    /// before them to subtract.
    DiffKernel, "diff", [DataType::Float64, DataType::Int64],
    |args, _rows| {
        let values = column_f64(args, 0)?;
        let order = constant_usize(args, 1, "diff order")?;
        Ok(f64_array(chronix_analytics::preprocess::diff(&values, order)))
    }
}

kernel! {
    /// `pct_change(values)` — fractional change from the previous row.
    ///
    /// NULL for the first row and wherever the previous value is zero.
    PctChangeKernel, "pct_change", [DataType::Float64],
    |args, _rows| {
        let values = column_f64(args, 0)?;
        Ok(f64_array(chronix_analytics::preprocess::pct_change(&values)))
    }
}

kernel! {
    /// `rolling_mean(values, window)` — trailing arithmetic mean.
    ///
    /// NULL until the window is full, so the first value is at row
    /// `window - 1`.
    RollingMeanKernel, "rolling_mean", [DataType::Float64, DataType::Int64],
    |args, _rows| {
        let values = column_f64(args, 0)?;
        let window = constant_usize(args, 1, "rolling_mean window")?;
        Ok(f64_array(chronix_analytics::preprocess::rolling_mean(&values, window)))
    }
}

kernel! {
    /// `rolling_std(values, window)` — trailing sample standard deviation,
    /// by Welford's algorithm.
    RollingStdKernel, "rolling_std", [DataType::Float64, DataType::Int64],
    |args, _rows| {
        let values = column_f64(args, 0)?;
        let window = constant_usize(args, 1, "rolling_std window")?;
        Ok(f64_array(chronix_analytics::preprocess::rolling_std(&values, window)))
    }
}

kernel! {
    /// `rolling_corr(a, b, window)` — trailing Pearson correlation.
    RollingCorrKernel, "rolling_corr",
    [DataType::Float64, DataType::Float64, DataType::Int64],
    |args, _rows| {
        let a = column_f64(args, 0)?;
        let b = column_f64(args, 1)?;
        let window = constant_usize(args, 2, "rolling_corr window")?;
        Ok(f64_array(chronix_analytics::preprocess::rolling_corr(&a, &b, window)))
    }
}

kernel! {
    /// `zscore(values)` — standardised against the partition's own mean and
    /// standard deviation.
    ///
    /// The partition is the whole population here, which is what makes the
    /// `PARTITION BY` clause part of the definition rather than a hint.
    ZscoreKernel, "zscore", [DataType::Float64],
    |args, _rows| {
        let values = column_f64(args, 0)?;
        Ok(f64_array(chronix_analytics::preprocess::zscore(&values)))
    }
}

kernel! {
    /// `ewm(values, alpha)` — exponentially weighted mean with smoothing
    /// factor `alpha` in `(0, 1]`.
    EwmKernel, "ewm", [DataType::Float64, DataType::Float64],
    |args, _rows| {
        let values = column_f64(args, 0)?;
        let alpha = constant_f64(args, 1, "ewm alpha")?;
        if !(alpha > 0.0 && alpha <= 1.0) {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "ewm alpha must be in (0, 1], got {alpha}"
            )));
        }
        Ok(f64_array(chronix_analytics::preprocess::ewm(&values, alpha)))
    }
}

// ── STL ─────────────────────────────────────────────────────────────────

/// Which component of an STL decomposition a kernel returns.
#[derive(Debug, Clone, Copy)]
enum StlPart {
    Trend,
    Seasonal,
    Residual,
}

/// Decompose a partition and pull out one component.
fn stl_component(args: &[ArrayRef], part: StlPart) -> DFResult<ArrayRef> {
    use chronix_analytics::preprocess::{stl_decompose, StlConfig};

    let values = column_f64(args, 0)?;
    let period = constant_usize(args, 1, "stl period")?;
    let out = on_dense(&values, |dense| {
        let d = stl_decompose(dense, &StlConfig::new(period)).map_err(exec_err)?;
        Ok(match part {
            StlPart::Trend => d.trend,
            StlPart::Seasonal => d.seasonal,
            StlPart::Residual => d.residual,
        })
    })?;
    Ok(f64_array(out))
}

kernel! {
    /// `stl_trend(values, period)` — the trend component of an STL
    /// decomposition of the partition.
    StlTrendKernel, "stl_trend", [DataType::Float64, DataType::Int64],
    |args, _rows| { stl_component(args, StlPart::Trend) }
}

kernel! {
    /// `stl_seasonal(values, period)` — the seasonal component.
    StlSeasonalKernel, "stl_seasonal", [DataType::Float64, DataType::Int64],
    |args, _rows| { stl_component(args, StlPart::Seasonal) }
}

kernel! {
    /// `stl_residual(values, period)` — what the trend and season leave over.
    StlResidualKernel, "stl_residual", [DataType::Float64, DataType::Int64],
    |args, _rows| { stl_component(args, StlPart::Residual) }
}

/// The struct type `stl_decompose` returns.
fn stl_struct_fields() -> arrow::datatypes::Fields {
    vec![
        Field::new("trend", DataType::Float64, true),
        Field::new("seasonal", DataType::Float64, true),
        Field::new("residual", DataType::Float64, true),
    ]
    .into()
}

kernel! {
    /// `stl_decompose(values, period)` — all three components at once, as a
    /// `STRUCT{trend, seasonal, residual}`.
    ///
    /// One decomposition instead of three, for the common case of wanting the
    /// whole split: `stl_decompose(v, 24) OVER (…)` then `.trend`, `.seasonal`
    /// and `.residual`.
    StlDecomposeKernel, "stl_decompose", [DataType::Float64, DataType::Int64],
    returns: DataType::Struct(stl_struct_fields()),
    |args, _rows| {
        use std::sync::Arc;

        use arrow::array::StructArray;
        use chronix_analytics::preprocess::{stl_decompose, StlConfig};

        let values = column_f64(args, 0)?;
        let period = constant_usize(args, 1, "stl period")?;
        if period < 2 {
            return Err(datafusion::common::DataFusionError::Plan(
                "stl_decompose: period must be at least 2".into(),
            ));
        }

        // One decomposition, three projections — computing it three times is
        // what the separate component functions cost, and this is the reason
        // to prefer this one.
        let dense: Vec<f64> = values.iter().copied().filter(|v| v.is_finite()).collect();
        if dense.len() < period * 2 {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "stl_decompose: need at least 2*period ({}) non-null rows, got {}",
                period * 2,
                dense.len()
            )));
        }
        let d = stl_decompose(&dense, &StlConfig::new(period)).map_err(exec_err)?;

        let spread = |src: &[f64]| {
            let mut out = vec![f64::NAN; values.len()];
            let mut k = 0;
            for (i, v) in values.iter().enumerate() {
                if v.is_finite() {
                    if let Some(c) = src.get(k) {
                        out[i] = *c;
                    }
                    k += 1;
                }
            }
            f64_array(out)
        };

        let arr = StructArray::try_new(
            stl_struct_fields(),
            vec![spread(&d.trend), spread(&d.seasonal), spread(&d.residual)],
            None,
        )
        .map_err(exec_err)?;
        Ok(Arc::new(arr))
    }
}
