+++
title = "Forecasting"
description = "Time-series forecasting predicts future values from observed history. In an operational context — infrastructure monitoring, capacity planning, SLA management — forecasts drive automated…."
weight = 140
+++

Time-series forecasting predicts future values from observed history. In an
operational context — infrastructure monitoring, capacity planning, SLA
management — forecasts drive **automated decisions**: scale-out triggers,
anomaly baselines, budget projections.

## Taxonomy of Methods

Chronix implements classical statistical forecasters. The table below places
each method in the broader forecasting landscape:

| Family | Method | Trend | Seasonality | Complexity |
|--------|--------|-------|-------------|------------|
| Exponential Smoothing | Simple (SES) | ✗ | ✗ | O(n) |
| | Holt | ✓ | ✗ | O(n) |
| | Holt-Winters | ✓ | ✓ | O(n) |
| Autoregressive | ARIMA | ✓ | ✗ | O(n·p²) |
| | SARIMA | ✓ | ✓ | O(n·(p+P·s)²) |

### When to Use What

```text
                     Seasonality?
                     ╱          ╲
                  Yes            No
                  ╱                ╲
            Strong trend?      Strong trend?
            ╱       ╲          ╱       ╲
          Yes       No       Yes       No
           │         │        │         │
       Holt-     Seasonal   Holt      SES
       Winters   ARIMA      or
       or                   ARIMA
       SARIMA
```

**Rules of thumb:**

1. **SES** — flat series, short horizons, no seasonal pattern
2. **Holt** — trended series without seasonality
3. **Holt-Winters** — trended series with clear periodic seasonality
4. **ARIMA** — auto-correlated residuals, non-seasonal
5. **SARIMA** — auto-correlated residuals with seasonal component

Or skip the chart: `auto_forecast` fits every eligible candidate and picks by
out-of-sample error at the horizon you asked for, and tells you what it tried
(see [Model Selection](@/internals/forecasting-selection.md)).

```rust
use chronix_analytics::forecast::{auto_forecast, AutoForecastOptions};

let chosen = auto_forecast(&timestamps, &values, 24, &AutoForecastOptions::default())?;
println!("{} — {} {:.3}", chosen.selection.label, chosen.selection.metric, chosen.selection.score);
```

## Forecast Horizon

The **forecast horizon** *h* is the number of future time steps to predict.
All Chronix forecasters produce *h* point estimates plus optional prediction
intervals (see [Prediction Intervals](@/internals/forecasting-intervals.md)).

Forecast accuracy degrades with increasing *h*. A common heuristic is to
keep *h ≤ n/3* where *n* is the training history length.

## Stationarity

Many methods assume **stationarity** — the statistical properties (mean,
variance, autocorrelation) do not change over time. Non-stationary series
are made stationary through **differencing**:

$$
y'_t = y_t - y_{t-1}
$$

Second-order differencing removes quadratic trends:

$$
y''_t = y'_t - y'_{t-1} = y_t - 2y_{t-1} + y_{t-2}
$$

ARIMA automates this through its *d* parameter. Exponential smoothing methods
handle non-stationarity implicitly through level and trend components.

## Integration with Chronix

Forecasters are registered through the **Model Lifecycle** system, which
handles versioning, training, and retraining. The query engine exposes
forecasting through the `FORECAST` SQL extension:

```sql
SELECT FORECAST(value, 24) FROM metrics
WHERE metric_name = 'cpu'
AND time > now() - INTERVAL '7 days';
```

Deep dives into each method family follow in the sub-sections.
