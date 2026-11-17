+++
title = "Forecasting models"
description = "The forecasting models inside Chronix — exponential smoothing, Holt-Winters, ARIMA and SARIMA — how each is fitted and when it wins."
weight = 100
aliases = ["/internals/forecasting/", "/internals/forecasting-exponential/", "/internals/forecasting-arima/"]
+++

Chronix forecasts with classical statistical models — exponential smoothing,
Holt-Winters, ARIMA and SARIMA, plus linear regression — fitted in the engine on
the series you name. They need seconds of CPU and kilobytes of memory, run on a
gateway, and every estimate is checked against `statsmodels`. In practice you
rarely pick one: `auto_forecast` fits every eligible model and keeps the one
with the lowest out-of-sample error at your horizon
([Intervals and model selection](@/explanation/forecast-intervals-and-selection.md)).

```rust
let f = db.auto_forecast("pv", "power_w", &[("device", "pv")], start, end, 96, None)?;
println!("{} — {:?}", f.selection.label, &f.result.values[..4]);
```

From SQL, `forecast(value, _time, h)` and `auto_forecast(value, _time, h)` are
aggregates returning a list of `h` values per group; over HTTP and MCP the
`forecast` call also returns the interval.

## The models

| Model | Captures | Fitted by | Use when |
|---|---|---|---|
| SES | level | squared one-step error | flat series, short horizons |
| Holt (linear or damped) | level and trend | squared one-step error | a trend, no season |
| Holt-Winters (additive or multiplicative) | level, trend, season | squared one-step error | a clear, stable season |
| ARIMA(p, d, q) | autocorrelation, after *d* differences | conditional sum of squares | correlated noise, no season |
| SARIMA(p,d,q)(P,D,Q)ₘ | the above plus seasonal terms at lag *m* | conditional sum of squares | correlated noise with a season |
| Linear regression | a straight line in time | least squares | a baseline to beat |

All implement the `ForecastModel` trait — `fit`, `predict`, `update` for one new
point — and serialise with `serde`, so a fitted model can be stored and reloaded.

### Exponential smoothing

Each smoother keeps exponentially weighted estimates of the level (α), the
trend (β, with an optional damping φ that flattens long-range forecasts) and
one seasonal factor per position in the period (γ). Parameters are chosen by
minimising the one-step error on the training data. Multiplicative seasonality
scales with the level and needs strictly positive data; additive seasonality
does not. Holt-Winters needs at least two full seasons.

### ARIMA and SARIMA

ARIMA differences the series *d* times to remove trend, then models what is
left as autoregressive (*p*) and moving-average (*q*) terms; SARIMA adds the
same terms at the seasonal lag. How Chronix fits them:

- **Order selection.** `auto_arima` follows Hyndman–Khandakar: *d* from the
  KPSS stationarity test (a statistic above 0.463 means *not* stationary — the
  opposite direction from ADF), then *(p, q)* at that fixed *d* by AICc, over a
  grid or a stepwise walk. *d* is never chosen by AIC: differencing changes the
  data the likelihood is computed on, so AIC across *d* compares nothing and
  over-differences.
- **One recursion.** The seasonal and non-seasonal polynomials are multiplied
  out and estimated jointly, so SARIMA and ARIMA are the same code.
- **Valid by construction.** The optimiser searches unconstrained values that
  map to stationary, invertible polynomials (Jones' reparameterisation), so no
  fit can produce an explosive forecast.
- **A constant** when *d + D ≤ 1*: the level the series reverts to, or the
  drift per step after one difference.
- **Conditional** sum of squares: the first innovations, which depend on
  history the model does not have, are excluded from the fit and from the
  residual variance.

Intervals widen with the ψ-weights of the fitted process:
Var(*e*(*h*)) = σ² Σⱼ<ₕ ψⱼ².

## What the forecaster needs from the data

| Requirement | Why |
|---|---|
| Regular spacing | Models count steps, not seconds. Resample first, or forecast at a step (`auto_forecast_at_step`, or `step` on the MCP tool) |
| Finite values | NaN and infinity are refused at `fit`, not silently propagated |
| History | SES works on anything; Holt needs 10 points, ARIMA 30, seasonal models two full periods |
| A horizon within reason | Error grows with *h*; *h* beyond a third of the history is guesswork. `[analytics] max_forecast_horizon` caps it |

`[analytics] max_training_points` caps the input, keeping the newest points.

## Bounded series

Power from a solar panel cannot be negative, but a model with symmetric errors
does not know that. `lower_bound` on `ForecastConfig` (or `min` on the MCP
`forecast` tool) clamps the forecast and its interval, and the result reports
the bound it applied — it is never inferred.

## Related

- [Intervals and model selection](@/explanation/forecast-intervals-and-selection.md)
- [Preprocessing and model lifecycle](@/explanation/preprocessing-and-models.md)
- [Forecast and detect](@/guides/forecast-and-detect.md)
