//! Chronix-specific `DataFusion` UDFs, UDAFs and window functions.
//!
//! # The three shapes
//!
//! | Shape | When | Example |
//! |---|---|---|
//! | **Scalar** | the answer for a row depends only on that row | `time_bucket('1h', _time)` |
//! | **Window** | the answer depends on the neighbouring rows, in order | `diff(v, 1) OVER (PARTITION BY host ORDER BY _time)` |
//! | **Aggregate** | the answer is one value (or list) for a whole group | `last(v, _time)`, `forecast(v, _time, 12)` |
//!
//! The distinction is not stylistic. A scalar UDF is handed one `RecordBatch`
//! at a time with no ordering, no partitioning and no guarantee about how the
//! plan split the rows, so a function that depends on its neighbours cannot be
//! one without its answer depending on batch size, partition count and segment
//! layout.
//!
//! Window functions get a partition and an order; aggregates get a group. Both
//! are stated by the caller in SQL, and the `OVER` clause is mandatory, so a
//! query that forgets to partition fails to plan instead of answering wrongly.
//!
//! # Catalogue
//!
//! ## Scalar
//!
//! | Function | Description |
//! |---|---|
//! | `time_bucket(interval, timestamp)` | Floor a timestamp to an interval |
//!
//! ## Window — call with `OVER (PARTITION BY … ORDER BY …)`
//!
//! | Function | Description |
//! |---|---|
//! | `diff(value, order)` | n-th order differencing |
//! | `pct_change(value)` | fractional change from the previous row |
//! | `rolling_mean(value, window)` | trailing mean |
//! | `rolling_std(value, window)` | trailing sample standard deviation |
//! | `rolling_corr(a, b, window)` | trailing Pearson correlation |
//! | `zscore(value)` | standardised against the partition |
//! | `ewm(value, alpha)` | exponentially weighted mean |
//! | `anomaly_score(value, threshold)` | Z-score anomaly score |
//! | `stl_trend/stl_seasonal/stl_residual(value, period)` | one STL component |
//! | `stl_decompose(value, period)` | all three, as a struct |
//! | `correlation(a, b)` | Pearson correlation over the partition |
//! | `cross_correlation(a, b, lag)` | correlation at one lag |
//! | `multivariate_anomaly(a, b, threshold)` | Mahalanobis distance |
//!
//! ## Aggregate
//!
//! | Function | Description |
//! |---|---|
//! | `first(value, timestamp)` / `last(value, timestamp)` | value at the earliest / latest timestamp |
//! | `rate(value, timestamp)` / `irate(value, timestamp)` | per-second rate of a counter |
//! | `forecast(value, timestamp, horizon)` | `LIST(DOUBLE)` of predicted values |
//! | `multivariate_forecast(target, predictor, timestamp, horizon)` | `LIST(DOUBLE)` from a regression |

mod aggregates;
mod analytics;
mod feature_engineering;
mod helpers;
mod time_bucket;
mod window;

use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::prelude::SessionContext;

use window::PartitionWindowUdf;

/// Register all Chronix custom SQL functions on a [`SessionContext`].
pub fn register_udfs(ctx: &SessionContext) {
    // ── Scalar ──────────────────────────────────────────────────────
    ctx.register_udf(ScalarUDF::new_from_impl(time_bucket::TimeBucketUdf::new()));

    // ── Window ──────────────────────────────────────────────────────
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::DiffKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::PctChangeKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::RollingMeanKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::RollingStdKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::RollingCorrKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::ZscoreKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::EwmKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::StlTrendKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::StlSeasonalKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::StlResidualKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<feature_engineering::StlDecomposeKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<analytics::AnomalyScoreKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<analytics::CorrelationKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<analytics::CrossCorrelationKernel>::udf());
    ctx.register_udwf(PartitionWindowUdf::<analytics::MultivariateAnomalyKernel>::udf());

    // ── Aggregate ───────────────────────────────────────────────────
    ctx.register_udaf(AggregateUDF::new_from_impl(aggregates::FirstUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(aggregates::LastUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(aggregates::RateUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(aggregates::IRateUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(aggregates::ForecastUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(
        aggregates::MultivariateForecastUdaf::new(),
    ));
}
