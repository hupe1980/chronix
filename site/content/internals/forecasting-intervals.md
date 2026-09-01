+++
title = "Prediction Intervals"
description = "A point forecast is incomplete without an estimate of its uncertainty. Prediction intervals quantify the range within which future observations are expected to fall with a given probability."
weight = 180
+++

A point forecast $\hat{y}_{t+h}$ is incomplete without an estimate of
its uncertainty. **Prediction intervals** quantify the range within which
future observations are expected to fall with a given probability.

## Formal Definition

A $(1 - \alpha)$ prediction interval for $y_{t+h}$ is:

$$
\hat{y}_{t+h} \pm z_{\alpha/2} \cdot \sigma_h
$$

where $z_{\alpha/2}$ is the standard normal quantile (e.g. $z_{0.025} = 1.96$
for a 95% interval) and $\sigma_h$ is the **forecast standard deviation**
at horizon *h*.

Chronix computes every parametric interval at 95 %, and
`ForecastResult::with_confidence(level)` rescales it. That is exact rather than
approximate: for a symmetric normal interval, changing the level *is* changing
$z$. `normal_quantile` is Acklam's rational approximation to $\Phi^{-1}$, whose
relative error is below $1.15 \times 10^{-9}$ — several orders of magnitude
finer than any interval it is used to build.

The rescaling is valid only because the interval is symmetric and normal. It is
not valid for the empirical residual quantiles below, which is the point of
having them.

## Interval Width Growth

Prediction intervals **widen** with the forecast horizon because uncertainty
accumulates. The growth pattern depends on the model:

### SES

$$
\sigma_h = \hat{\sigma} \sqrt{1 + (h-1)\alpha^2}
$$

For large *h*, the width grows as $\hat{\sigma}\alpha\sqrt{h}$.

### Holt

$$
\sigma_h = \hat{\sigma} \sqrt{1 + \sum_{j=1}^{h-1}\left(\alpha + j\alpha\beta\right)^2}
$$

The trend component causes intervals to widen faster than SES.

### Holt-Winters

The formula extends further with seasonal variance components. Where the
closed form becomes unwieldy — and wherever the Gaussian assumption behind it
is doubtful — the empirical route below applies to any model uniformly.

### ARIMA

For ARIMA(p, d, q), the forecast variance is:

$$
\sigma_h^2 = \hat{\sigma}^2 \sum_{j=0}^{h-1} \psi_j^2
$$

where $\psi_j$ are the coefficients of the **MA(∞) representation**
obtained by inverting the AR polynomial:

$$
\Psi(B) = \frac{\Theta(B)}{\Phi(B)(1-B)^d}
$$

## Degrees of Freedom

$\hat{\sigma}$ above is $\sqrt{\text{RSS} / \nu}$, and $\nu$ is the residual
count *minus the number of fitted parameters* — not the raw count. Chronix
uses $n-2$ for SES, $n-4$ for Holt linear, $n-(p+q+1)$ for ARIMA, $n-3$ for
non-seasonal Holt-Winters and $n-(\text{period}+3)$ for seasonal
Holt-Winters.

Using the raw count under-estimates residual spread, which narrows every
interval derived from it — and because a forecast-deviation trigger fires on
an observation outside the interval, that is an alert rate rather than a
statistics detail. `prediction_intervals_use_consistent_degrees_of_freedom`
pins the convention across all of them.

## Visualization

```text
         ┌──────────────────── 95% interval
         │   ┌──────────────── 80% interval
         │   │
  ───────┼───┼──────────
 ●●●●●●●●│●●●│●────────── point forecast
  ───────┼───┼──────────
         │   │
         │   └──────────────── 80% interval
         └──────────────────── 95% interval
         │
      forecast origin
```

## Empirical Intervals

The closed forms above assume a distribution *and* a growth rate. Chronix's
`QuantileForecaster` assumes neither: it calibrates on the empirical
distribution of **walk-forward residuals, bucketed per horizon step**, so both
the shape and the widening of the interval are learned from the data.

1. Fit on an initial window of $w$ observations.
2. Walk the origin forward. At each origin $t$, forecast $h$ steps and record
   $e_{t,j} = y_{t+j} - \hat{y}_{t+j}$ into the bucket for step $j$. With
   `CalibrationStrategy::OnlineUpdate` the model advances by
   `update()` rather than refitting, which costs one fit plus $n$ updates.
3. The interval at horizon $j$ is the point forecast plus the empirical
   quantiles of bucket $j$.

Residuals are **strictly out-of-sample** (rolling-origin evaluation, Hyndman &
Athanasopoulos, *FPP3* §5.10). An in-sample residual measures fit rather than
forecast error and produces intervals that are confidently too narrow.

### Split-conformal correction

Optionally the rank is moved **outward** by the split-conformal finite-sample
correction, which turns asymptotic coverage into a marginal guarantee under
exchangeability — the asymptotic argument is not available when the
calibration sample is forty points, which is what an embedded gateway
actually has.

$$
q^{\text{upper}} = \frac{\lceil (n+1)q \rceil}{n}
\qquad
q^{\text{lower}} = 1 - \frac{\lceil (n+1)(1-q) \rceil}{n}
$$

The two expressions are mirror images, and that matters more than it looks:
$\lceil (n+1)q \rceil / n$ is derived for the **upper** tail, and applying it
to a lower quantile moves that bound *up*. At $n = 40$ and $q = 0.05$ the rank
goes from $0.05$ to $0.075$ — so a correction meant to add coverage removes it
below the forecast. Both tails are asserted separately by
`conformal_correction_widens_both_tails`.

### Physical bounds

Residual quantiles are *additive* offsets, so they do not respect a domain on
their own: a PV series bounded at zero can still receive a negative lower
bound where the point forecast is noisy. `QuantileConfig::lower_bound` /
`upper_bound` state the domain explicitly. Learning the residual shape and
declaring the physical limits are different jobs.

Empirical intervals are **distribution-free** and naturally asymmetric.

## Practical Considerations

| Concern | Recommendation |
|---------|---------------|
| Interval too wide | Shorten horizon; use more training data |
| Interval too narrow | Check for structural breaks; refit model |
| Negative lower bound | Clamp at zero for non-negative metrics |
| Multi-step intervals | Always use $\sigma_h$, not $\sigma_1$ scaled by $\sqrt{h}$ |

## Chronix API

Calibrated intervals come from `QuantileForecaster` in `chronix-analytics`:

```rust
use chronix_analytics::forecast::{
    quantile::{QuantileConfig, QuantileForecaster},
    SesModel,
};

// A 90% central interval over a 24-step horizon.
let cfg = QuantileConfig::central(0.90, 24).with_bounds(Some(0.0), None);
let mut qf = QuantileForecaster::new(|| Box::new(SesModel::new(Some(0.3))), cfg);
qf.fit(&timestamps, &values)?;

let f = qf.predict(24)?;
// f.point[h], and f.quantiles[level][h] for each level in f.levels.
// f.calibration_counts[h] says how many residuals backed step h — an
// interval calibrated on two residuals is reported as such rather than
// presented with the same confidence as one calibrated on two hundred.
# Ok::<(), chronix_analytics::forecast::ForecastError>(())
```

`predict_interval(horizon, level)` returns the same bounds in the
`ForecastResult` shape used by the rest of the forecasting API.

Narrower intervals (e.g. 0.80) are useful when the cost of false alarms
outweighs the cost of misses — and note that a forecast-deviation trigger
fires on an observation outside the interval, so interval width is an alert
rate.
