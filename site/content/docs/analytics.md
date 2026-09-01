+++
title = "Analytics"
description = "Forecasting (SES, Holt, Holt-Winters, ARIMA, SARIMA) and anomaly detection (Z-score, MAD, IQR, CUSUM, residual) inside the database, callable from SQL."
weight = 30
+++

Chronix provides built-in time-series analytics: forecasting (6 models),
anomaly detection (6 detectors), and multivariate analysis — all accelerated
on a SIMD/rayon CPU compute engine.

## Forecast Models

### Available Models

| Model | Crate | Complexity | Best For |
|-------|-------|------------|----------|
| **SES** (Simple Exponential Smoothing) | `chronix-analytics::forecast` | Low | Stationary series, no trend |
| **Holt** (Double Exponential) | `chronix-analytics::forecast` | Low | Linear trend, no seasonality |
| **Holt-Winters** (Triple Exponential) | `chronix-analytics::forecast` | Medium | Trend + seasonality |
| **ARIMA** | `chronix-analytics::forecast` | High | Complex dependencies, differencing |
| **SARIMA** | `chronix-analytics::forecast` | High | Seasonal ARIMA with periodic patterns |
| **Linear Regression** | `chronix-analytics::forecast` | Low | Simple trend estimation |

### SQL Interface

`forecast` is an **aggregate** function returning a `LIST(DOUBLE)` of
predicted values. A forecast is not a property of any existing row — it is
`horizon` new values that come *after* the last one — so it produces one list
per group, and `unnest` expands it:

```sql
-- 24 points ahead, per host
SELECT host, unnest(forecast(usage, _time, 24)) AS predicted
FROM cpu_usage
WHERE _time > now() - INTERVAL '7 days'
GROUP BY host;
```

The `_time` argument is not decoration: an aggregate sees rows in whatever order
the plan delivers them, so the accumulator sorts by the timestamp column it is
given. Passing a constant, or a column that is not time, forecasts a shuffled
series.

The SQL model is simple exponential smoothing — the right default for a metric
with no declared seasonality. Choosing a model needs orders, a period and a
seasonal flag, which belong in typed arguments rather than a JSON string, so it
is a Rust-API job (below).

`multivariate_forecast(target, predictor, _time, horizon)` has the same shape
and returns a regression forecast of `target`.

### Rust API

```rust
use chronix_analytics::forecast::{SesModel, ForecastModel};

let mut model = SesModel::new(Some(0.3));
model.fit(&timestamps, &values)?;
let forecast = model.predict(24)?;
```

#### Letting Chronix choose the model

`auto_forecast` detects the seasonal period, picks a differencing order with a
KPSS unit-root test, searches an ARIMA order, and scores every eligible
candidate by rolling-origin cross-validation **at the horizon asked for** — the
only comparison that transfers across model families. The winner is refitted on
the whole window.

```rust
use chronix_analytics::forecast::{auto_forecast, AutoForecastOptions};

let chosen = auto_forecast(&timestamps, &values, 24, &AutoForecastOptions::default())?;
println!("{} — {} {:.3}", chosen.selection.label,
         chosen.selection.metric, chosen.selection.score);
for c in &chosen.selection.candidates {
    println!("  {:<32} {:.3}", c.label, c.score);
}
for (label, reason) in &chosen.selection.rejected {
    println!("  {label:<32} not considered: {reason}");
}
```

The same thing through the database, over a stored window:

```rust
let chosen = db.auto_forecast(
    "energy_usage", "kwh", &[("building", "HQ")],
    start, end, 24, None,
)?;
```

The report lists what was *not* scored as well as what lost: "SARIMA was not
considered" and "SARIMA lost" are different answers to "why is my forecast not
seasonal?". See [Model Selection](@/internals/forecasting-selection.md) for why
the winner is chosen by out-of-sample error rather than by an information
criterion.

#### Prediction interval levels

Every parametric model computes a 95 % interval, and
`ForecastResult::with_confidence(level)` rescales it — exact, for a symmetric
normal interval. `ForecastConfig::confidence` routes through it.

```rust
let wide = model.predict(24)?.with_confidence(0.99)?;
```

For an asymmetric or bounded interval, learned from the residuals rather than
assumed Gaussian, use `QuantileForecaster`.

#### SARIMA Configuration

Complex models like SARIMA use a dedicated config struct to avoid
positional parameter ambiguity:

```rust
use chronix_analytics::forecast::{SarimaModel, SarimaConfig, ForecastModel};

let model = SarimaModel::from_config(SarimaConfig {
    p: 1, d: 1, q: 1,   // Non-seasonal ARIMA orders
    sp: 1, sd: 1, sq: 0, // Seasonal orders
    m: 12,                // Seasonal period
});
```

### ARIMA Effective AR Order

When the Burg estimator terminates early (e.g. due to numerical instability
or insufficient data), the model records the **effective AR order** — the
number of AR coefficients actually computed — in `ModelParams::Arima`:

- `effective_ar_order: Option<usize>` — `Some(k)` when `k < p` (early
  termination), `None` when the full requested `p` was used
- A `WARN`-level log is emitted on early termination
- The `chronix_forecast_arima_effective_order` gauge metric is emitted on
  every `fit()` call

This metadata lets callers detect degraded fits and decide whether to retry
with a lower order or flag the result for review.

### ARIMA Prediction Intervals (ψ-weights)

ARIMA and SARIMA models compute prediction intervals using proper
**ψ-weight accumulation** rather than naive residual scaling. The ψ-weights
capture the cumulative impulse response of the ARIMA process, so prediction
intervals widen correctly at longer horizons:

$$\text{Var}(e_t(h)) = \sigma^2 \sum_{j=0}^{h-1} \psi_j^2$$

The `predict_with_intervals(horizon, confidence)` method returns point
forecasts along with lower/upper bounds:

```rust
use chronix_analytics::forecast::{ArimaModel, ForecastModel};

let mut model = ArimaModel::new(1, 1, 1);
model.fit(&timestamps, &values)?;
let (forecast, lower, upper) = model.predict_with_intervals(24, 0.95)?;
```

### Quantile Forecasting (empirical residual quantiles)

The interval methods above are **parametric**: they assume errors are Gaussian
and derive the interval width from a variance formula. That is a good fit for
ARIMA on well-behaved data and a poor one for the workloads Chronix is aimed
at — energy series are heavily skewed and heteroscedastic, and their error
scale grows with the horizon at a rate the closed form does not know.

`QuantileForecaster` wraps any `ForecastModel` and derives intervals from the
**empirical distribution of walk-forward residuals, bucketed per horizon
step**. Interval shape and growth are learned from the data rather than
assumed.

```rust
use chronix_analytics::forecast::{
    CalibrationStrategy, HoltWintersModel, QuantileConfig, QuantileForecaster,
};

let config = QuantileConfig {
    levels: vec![0.1, 0.5, 0.9],
    horizon: 24,
    initial_window: 240,
    step: 6,
    strategy: CalibrationStrategy::OnlineUpdate,
    conformal: true,
    lower_bound: Some(0.0),      // PV generation cannot be negative
    upper_bound: Some(5_000.0),  // inverter nameplate
};

let mut qf = QuantileForecaster::new(
    || Box::new(HoltWintersModel::new(Some(0.3), Some(0.05), Some(0.4), Some(24), false)),
    config,
);
qf.fit(&timestamps, &values)?;

let forecast = qf.predict(24)?;
let p10 = forecast.level(0.1).expect("configured level");
let p90 = forecast.level(0.9).expect("configured level");
```

**Calibration is strictly out-of-sample.** Residuals come from rolling-origin
evaluation (Hyndman & Athanasopoulos, *FPP3* §5.10): the model is refit — or
replayed via online `update()` — up to each origin, then asked for `horizon`
steps, and only genuinely unseen points contribute residuals. In-sample
residuals would be optimistically small and the intervals correspondingly too
narrow.

| `CalibrationStrategy` | Cost | Use when |
|---|---|---|
| `Refit` | one full fit per origin | offline calibration, most faithful |
| `OnlineUpdate` | one fit + *n* updates | on-device; mirrors a live forecaster |

**Conformal correction.** With `conformal: true` the residual quantile is taken
at the finite-sample-corrected rank `⌈(n+1)q⌉/n` (Vovk et al.; Gibbs & Candès,
*Adaptive Conformal Inference*, NeurIPS 2021). Under exchangeability that gives
marginal coverage of at least `1-α` rather than merely asymptotic coverage —
which matters at the small calibration sizes an embedded gateway has.

**Bounds are separate from shape.** Residual quantiles are *additive* offsets
on the point forecast, so they do not by themselves respect a physical domain:
a series bounded at zero can still receive a negative lower bound where the
point forecast is noisy. `lower_bound` / `upper_bound` express that domain
explicitly. Learning the residual shape and declaring the domain are different
jobs.

`QuantileForecast::calibration_counts` reports how many residuals back each
horizon step, so callers can tell when a long horizon rests on too few
samples to trust. `predict_interval(horizon, level)` returns the familiar
`ForecastResult` shape with calibrated bounds, for code already written
against the parametric API.

See `crates/chronix/examples/quantile_forecast.rs` for a runnable end-to-end demonstration.

### Stepwise ARIMA Search (`auto_arima`)

The `auto_arima` function supports a `SearchStrategy::Stepwise` option that
implements the Hyndman-Khandakar algorithm for efficient ARIMA order selection.
Instead of exhaustive grid search over all $(p, d, q)$ combinations, stepwise
search evaluates a small set of seed models, then explores neighbours by
incrementing/decrementing individual orders:

```rust
use chronix_analytics::forecast::{auto_arima, SearchStrategy};

// Stepwise search — O(P+D+Q) evaluations instead of O(P×D×Q)
let model = auto_arima(&timestamps, &values, SearchStrategy::Stepwise)?;

// Exhaustive grid search (previous default)
let model = auto_arima(&timestamps, &values, SearchStrategy::Grid)?;
```

**Algorithm:**
1. Evaluate seed models: `(0,d,0)`, `(1,d,0)`, `(0,d,1)`, `(2,d,2)`
2. Select the best seed by AICc
3. Explore neighbours: increment/decrement `p`, `q` by 1
4. Stop when no neighbour improves AICc

Stepwise search typically evaluates 10–30 models instead of hundreds, making
it practical for real-time `auto_arima` on ingestion pipelines.

### ACF Configurable Threshold

The autocorrelation-based period detection now accepts a custom significance
threshold via `detect_period_with_threshold()`:

```rust
use chronix_analytics::forecast::acf::detect_period_with_threshold;

// Default threshold (0.3)
let period = detect_period(&values, max_lag)?;

// Custom threshold for noisy data
let period = detect_period_with_threshold(&values, max_lag, 0.2)?;
```

Lower thresholds are more sensitive to weak seasonal signals; higher
thresholds reduce false-positive period detection.

### Distributed Forecasting

In a cluster, forecasting uses scatter-gather:

1. **Scatter**: Fetch training data from all regions via `QueryRouter`
2. **Fit**: Model fitted locally on the coordinator node
3. **Predict**: Forecast computed on fitted model

```rust
use chronix_cluster::DistributedAnalytics;

let result = analytics.forecast(&req, &mut model).await?;
println!("Forecast {} points from {} regions", 
    result.forecast.len(), result.regions_queried);
```

## Anomaly Detection

### Available Detectors

| Detector | Method | Best For |
|----------|--------|----------|
| **Z-Score** | Statistical deviation | Gaussian-distributed metrics |
| **Modified Z-Score** | MAD-based robust z-score | Metrics with outliers |
| **IQR** (Interquartile Range) | Quartile-based fencing | Skewed distributions (handles zero-IQR gracefully) |
| **DBSCAN** | Density-based clustering | Multimodal, non-linear patterns |
| **Isolation Forest** | Random partitioning | High-dimensional data |
| **Dynamic Threshold** | Rolling window adaptive | Non-stationary baselines |
| **Forecast Residual** | Walk-forward forecast residuals | Trend/seasonal series with forecastable patterns |
| **MA Residual** | Moving-average residuals | Stationary series with short-term deviations |
| **CUSUM** | Cumulative sum control chart | Detecting mean shifts in process data |

### CUSUM Configuration

The CUSUM detector supports two reset modes via `reset_after_alarm`:

- **`true`** (default) — Western Electric convention: the cumulative sum resets
  to zero after an alarm, tracking fresh deviations from that point.
- **`false`** — Page (1954) convention: the cumulative sum continues accumulating
  after an alarm, reporting sustained shifts.

Floor values are enforced: decision threshold `h ≥ 1e-3`, slack parameter `k ≥ 1e-4`.

### Walk-Forward Residuals (Forecast Residual Detector)

The Forecast Residual detector uses strict **walk-forward one-step-ahead
residuals** for both `fit()` and `detect()`, eliminating future data leakage:

1. **`fit()`**: Computes walk-forward residuals over the training data,
   producing the baseline `residual_mean` and `residual_std`.
2. **`detect()`**: Recomputes walk-forward residuals on the detection window
   using the same algorithm. Each point is predicted using only prior
   observations (SES alpha smoothing, Holt Linear level+trend, or fallback
   lag-1 residuals depending on the fitted model type).
3. Z-scores are computed as `|residual - mean| / std` against the training
   baseline, and points exceeding the configured threshold are flagged.

This design ensures consistent behavior between training and inference —
the detector never uses information from future time steps.

### SQL Interface

`anomaly_score` is a **window** function: the score of a point is its Z-score
against the distribution of the *partition*, so the partition has to be stated.

```sql
SELECT _time, host, usage,
       anomaly_score(usage, 3.0) OVER (PARTITION BY host ORDER BY _time) AS z
FROM cpu_usage
WHERE _time > now() - INTERVAL '1 hour';
```

`PARTITION BY host` keeps one host's spike out of another host's baseline, and
`ORDER BY _time` is what makes "previous point" mean anything. Both are
required: a window function without an `OVER` clause fails to plan.

The other detectors (MAD, IQR, CUSUM, forecast-residual, dynamic threshold)
are Rust-API types, because each carries its own configuration.

### Sub-batching

When input exceeds `max_batch_size` (default: 65,536), operations are
automatically split into sub-batches:

- **dot_product**: Chunks summed independently, partial results accumulated
- **difference**: Overlapping chunks (last element shared between adjacent chunks)
- **z_score**: Independent chunks (mean/std computed globally on CPU)


### Rolling Correlation Conventions

`multivariate::RollingCorrelation` and `preprocess::rolling_corr` compute the
same statistic and follow the same conventions, which match pandas
`rolling(window).corr()`:

| Situation | Result |
|---|---|
| Fewer than `window` observations so far | `NaN` |
| Either series has zero variance in the window | `NaN` |
| Otherwise | Pearson *r* over the trailing `window` |

`NaN` — not `0.0` — is the correct value for an undefined correlation: `0.0`
asserts "these series are uncorrelated" when the truth is "there is not enough
information to say". `RollingCorrelation` previously reported values computed
from *partial* windows and `0.0` for zero variance, so the two functions gave
different answers for the same input. A test pins them to the same output.

## Multivariate Analysis

### Correlation

```rust
use chronix_analytics::multivariate::MultivariateAnalyzer;

let mut analyzer = MultivariateAnalyzer::new();
analyzer.add_series("cpu", &cpu_values);
analyzer.add_series("memory", &mem_values);

let matrix = analyzer.correlation_matrix()?;
let granger = analyzer.granger_causality("cpu", "memory", 5)?;
```

### VAR (Vector Autoregression)

```rust
let var_model = analyzer.fit_var(lag_order)?;
let forecast = var_model.predict(horizon)?;
```

## Compute Engine Architecture

All batch operations run on the `CpuEngine` — a dedicated rayon thread pool
with runtime-dispatched SIMD (AVX-512F → AVX2+FMA → SSE2 on x86_64, NEON on
aarch64) and Kahan compensated summation:

| Operation | Implementation |
|-----------|----------------|
| `batch_dot_product` | rayon + SIMD FMA |
| `batch_exponential_smooth` | Sequential (inherently recursive) |
| `batch_difference` | rayon |
| `batch_autocorrelation` | rayon per-lag |
| `batch_z_score` | Vectorized, sample variance (Bessel) |
| `batch_matrix_solve` | LU decomposition with partial pivoting (pure Rust) |
| `batch_least_squares` | QR decomposition |

## Tracing Integration

All analytics operations emit `tracing` spans:

- `forecast_fit` — model fitting phase
- `anomaly_detect` — anomaly detection phase

These integrate with the OpenTelemetry pipeline for end-to-end distributed
tracing when `chronixd::otel` is configured with OTLP export.

### Continuous Forecast Re-fit Threshold

A continuous forecaster keeps a rolling window of absolute 1-step-ahead errors
and triggers a **full model re-fit** when
`current_mae > baseline_mae × refit_mae_multiplier`.

`ContinuousForecastConfig::min_mae_samples` (default 10) is the minimum number
of residuals before that MAE is treated as meaningful. Below it the MAE is
`None` and no comparison is made. A MAE from one or two residuals is noise
driving an expensive decision: it either causes a spurious re-fit — wasted CPU
on a constrained gateway — or, when the unlucky residual lands in the
*baseline*, inflates the threshold so genuine drift never triggers one.

| Setting | Default | Meaning |
|---|---|---|
| `min_mae_samples` | `10` | Residuals required before the MAE is used |
| `refit_mae_multiplier` | `2.0` | Re-fit when current MAE exceeds baseline × this |
| `update_interval_points` | `1000` | How often the re-fit check runs |

## Model Catalog & Admin API

Trained models are stored in the `ModelCatalog` and managed via the admin
REST API:

```bash
# List all trained models
curl http://localhost:8086/api/v1/admin/analytics/models

# List models for a specific measurement
curl http://localhost:8086/api/v1/admin/analytics/models?measurement=cpu

# Get metadata for a specific model
curl http://localhost:8086/api/v1/admin/analytics/models/cpu/ses_v1

# Delete a model
curl -X DELETE http://localhost:8086/api/v1/admin/analytics/models/cpu/ses_v1

# Trigger re-training for all models on a measurement
curl -X POST http://localhost:8086/api/v1/admin/analytics/retrain \
  -H 'Content-Type: application/json' \
  -d '{"measurement": "cpu"}'

# Re-train a specific model
curl -X POST http://localhost:8086/api/v1/admin/analytics/retrain \
  -H 'Content-Type: application/json' \
  -d '{"measurement": "cpu", "model_name": "ses_v1"}'
```

Re-training clears models from the catalog. The `ContinuousForecastEngine`
automatically re-fits them when the next data batch arrives.

## Drift Detection & Statistical Testing

### PSI Quantile-Based Drift Detection

Population Stability Index (PSI) drift detection uses **quantile-based bins**
rather than fixed-width bins, providing more robust detection across
non-uniform distributions:

```rust
use chronix_analytics::drift::PsiDetector;

let detector = PsiDetector::new(10); // 10 quantile bins
let psi = detector.compute(&baseline_values, &current_values)?;
// PSI < 0.1: no drift, 0.1–0.25: moderate, > 0.25: significant
```

Quantile bins ensure each bin receives approximately equal representation
from the baseline distribution, preventing empty or dominant bins from
skewing the PSI score.

### AB Test Tie Handling

The AB test comparator handles ties (equal metric values) by preserving the
current winning streak rather than resetting it. This prevents spurious
streak breaks when metric values are identical between variants:

```rust
use chronix_analytics::ab_test::AbTestComparator;

let mut comparator = AbTestComparator::new();
comparator.record("A", 0.85);
comparator.record("B", 0.85); // Tie — streak preserved
comparator.record("A", 0.90); // Streak continues
```

---

## See Also

- [Performance Tuning](@/docs/performance.md) — compute backend selection and tuning
- [Architecture Reference](@/reference/_index.md) — analytics engine internals
- [Operations Guide](@/docs/operations.md) — deployment and configuration
- [Guide](@/docs/_index.md)
