+++
title = "Forecast and detect anomalies"
description = "Forecast a series and find anomalies inside Chronix from Rust or SQL: choosing a model, intervals, detectors, multivariate checks and drift."
weight = 50
aliases = ["/docs/analytics/"]
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
KPSS unit-root test and a *seasonal* differencing order from STL seasonal
strength (the same measure and 0.64 threshold R's `nsdiffs` uses by default),
searches an ARIMA order, and scores every eligible candidate by rolling-origin
cross-validation **at the horizon asked for** — the only comparison that
transfers across model families. The winner is refitted on the whole window.

`stl_trend`, `stl_seasonal`, `stl_residual` and `stl_decompose` take an
optional `robust` flag, and `StlConfig::robust()` is the same thing in Rust:
it runs Cleveland's outer loop so that an outlier is left in the residual
rather than spread across every cycle.

The decomposition under that measure is Cleveland et al. (1990) STL with the
paper's defaults: a seasonal smoother of 7 **cycles**, a trend smoother of
`1.5·period / (1 − 1.5/7)` and a low-pass of `period`, each rounded up to the
next odd number. Components match statsmodels' `STL` to three decimals.

The candidate set is deliberately small: a wider search lets a candidate win
the folds and lose out of sample, which measures worse. See
[Model Selection](@/explanation/forecast-intervals-and-selection.md) for the numbers.

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
seasonal?". See [Model Selection](@/explanation/forecast-intervals-and-selection.md) for why
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

`anomaly_score(value, threshold)` is a **window** function. It takes each
point's Z-score against the mean and standard deviation of its *partition* and
maps it into (0, 1): **0.5 at the threshold**, close to 1 well above it. A point
beyond the threshold is one with a score above 0.5.

```sql
SELECT * FROM (
  SELECT _time, host, usage,
         anomaly_score(usage, 3.0) OVER (PARTITION BY host ORDER BY _time) AS score
  FROM cpu_usage
  WHERE _time > now() - INTERVAL '1 hour'
) WHERE score > 0.5
ORDER BY score DESC;
```

`PARTITION BY host` keeps one host's spike out of another host's baseline, and
`ORDER BY _time` is what makes "previous point" mean anything. Both are
required: a window function without an `OVER` clause fails to plan.

The other detectors (MAD, IQR, CUSUM, forecast-residual, dynamic threshold)
are Rust-API types, because each carries its own configuration.

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
