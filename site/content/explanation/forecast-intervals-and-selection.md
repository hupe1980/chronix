+++
title = "Intervals and model selection"
description = "How Chronix builds prediction intervals and picks a forecasting model by rolling-origin cross-validation, and what coverage to expect."
weight = 110
aliases = ["/internals/forecasting-intervals/", "/internals/forecasting-selection/"]
+++

A forecast is a point estimate and an interval, and the interval is only as
useful as its calibration. Chronix gives every model a parametric interval, and
offers an empirical one learned from out-of-sample errors when the data is
skewed or the horizon long. `auto_forecast` picks the model by the same
measure: walk forward through the history, forecast at the horizon you asked
for, and keep the model that was least wrong.

## Prediction intervals

### Parametric

Each model's interval comes from its own variance formula, so it widens with
the horizon at that model's rate:

| Model | σ at horizon *h* |
|---|---|
| SES | σ̂ √(1 + (*h*−1)α²) |
| Holt | σ̂ √(1 + Σⱼ<ₕ (α + jαβ)²) |
| ARIMA, SARIMA | σ̂ √(Σⱼ<ₕ ψⱼ²), the ψ-weights of the fitted process |

σ̂ divides the residual sum of squares by the residual count **minus the
fitted parameters** — *n*−2 for SES, *n*−4 for Holt, *n*−(*p*+*q*+1) for ARIMA,
*n*−(period+3) for seasonal Holt-Winters. Using the raw count narrows every
interval, and a forecast-deviation trigger fires outside the interval, so that
would be a higher alert rate.

### Empirical (`QuantileForecaster`)

Energy and latency series are skewed, and their error grows with the horizon at
a rate no closed form knows. `QuantileForecaster` wraps any model and learns
the interval instead:

1. Fit on an initial window.
2. Walk the origin forward; at each origin forecast *h* steps and record the
   error for each step *j* in bucket *j*.
3. The interval at step *j* is the forecast plus that bucket's quantiles.

The errors are strictly out of sample, so the interval measures forecast error,
not fit. It can be asymmetric. With `conformal: true` the quantile rank is
moved outward by the split-conformal correction — ⌈(*n*+1)*q*⌉/*n* for the
upper tail, mirrored for the lower — which turns asymptotic coverage into a
finite-sample guarantee: what matters when the calibration set is forty points.

```rust
use chronix_analytics::forecast::{quantile::{QuantileConfig, QuantileForecaster}, SesModel};

let cfg = QuantileConfig::central(0.90, 24).with_bounds(Some(0.0), None);
let mut qf = QuantileForecaster::new(|| Box::new(SesModel::new(Some(0.3))), cfg);
qf.fit(&timestamps, &values)?;
let f = qf.predict(24)?;
// f.point[h]; f.quantiles[level][h]; f.calibration_counts[h] — how many errors backed step h
```

`with_bounds` states a physical domain (power ≥ 0): learning the error shape
and declaring physical limits are different jobs.

## Model selection

**Every eligible model is fitted and scored by rolling-origin cross-validation
at the requested horizon; the lowest error wins.** An information criterion
cannot rank across families — the smoothers have no likelihood, and an ARIMA
with *d* = 1 is fitted to a different series — so AICc is used only inside
ARIMA, for *(p, q)* at a fixed *d*.

| Step | How |
|---|---|
| 1. Season | detrend, then the first local ACF maximum above 0.3 |
| 2. Differencing *d* | KPSS: difference while stationarity is rejected (statistic > 0.463) |
| 3. Seasonal *D* | STL seasonal strength above 0.64 (R's `nsdiffs` default) |
| 4. Candidates | pruned by series length (below) |
| 5. ARIMA order | `auto_arima` on the first fold's training window only |
| 6. Score | expanding-window cross-validation, 3 origins by default; RMSE (or MAE, sMAPE) |
| 7. Refit | the winner, on the whole series |

| Training window | Candidates added |
|---|---|
| any | SES |
| ≥ 10 points | Holt, damped Holt (φ = 0.9), linear regression |
| ≥ 30 points in the first fold | ARIMA |
| a period *m* and two seasons per fold | Holt-Winters additive; multiplicative if every value > 0; SARIMA (1,1,0)ₘ and (0,1,1)ₘ |

### The report

`ModelSelection` names the winner, every scored candidate best first, and every
candidate that was **not** scored with the reason — "SARIMA was not considered"
and "SARIMA lost" answer "why is my forecast not seasonal?" differently.

```text
Winner: SARIMA(2,1,0)(0,1,1)[24]
Chosen by: RMSE over 3 rolling origins, 264 training points in the first fold
Detected period: 24   KPSS differencing order: 1
Candidates:
  SARIMA(2,1,0)(0,1,1)[24]           RMSE      0.000
  Holt-Winters additive[24]          RMSE      0.305
  ARIMA(2,1,0)                       RMSE      1.027
  Holt damped (φ=0.9)                RMSE     16.215
  SES                                RMSE     39.045
```

(`cargo run -p chronix --example forecast`.)

### Why the search is small

Each candidate is another chance to win the folds by luck. Measured: offering
both seasonal differencing orders, a wider seasonal grid, or 12 folds instead of
3 changed mean MASE by less than 0.01 — and sometimes for the worse. The cost is
*candidates × folds* fits, so a gateway can afford it.

The seasonal-strength rule does not detect a **stochastic** season — one whose
shape drifts from cycle to cycle. Holt-Winters is in the candidate set and its
seasonal smoothing tracks such a series, so the case is covered without a
seasonal unit-root test.

### Cross-validation on its own

```rust
use chronix_analytics::forecast::{cross_validation::{CrossValidationMode, CrossValidator}, SesModel};

let cv = CrossValidator::new(CrossValidationMode::ExpandingWindow, 30, 10, 5);
let r = cv.evaluate(&timestamps, &values, || Box::new(SesModel::new(None)))?;
println!("{:.3}", r.mean_rmse);
```

Every fold fits a fresh model. Results carry MAE, RMSE (the default), MAPE
(`None` where the actual is zero) and sMAPE.

## Related

- [Forecasting models](@/explanation/forecasting.md)
- [Anomaly detection](@/explanation/anomaly-detection.md) — forecast-residual detection
- [Forecast and detect](@/guides/forecast-and-detect.md)
