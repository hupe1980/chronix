+++
title = "ARIMA & SARIMA"
description = "ARIMA models capture autocorrelation — the dependency of a value on its own past values — through a combination of three components."
weight = 160
+++

## Autoregressive Integrated Moving Average (ARIMA)

ARIMA models capture **autocorrelation** — the dependency of a value on its
own past values — through a combination of three components.

### The ARIMA(p, d, q) Model

An ARIMA(p, d, q) model applies *d* differences to make the series
stationary, then fits an ARMA(p, q) model:

$$
\Phi(B)\, (1 - B)^d\, y_t = \Theta(B)\, \varepsilon_t
$$

where $B$ is the **backshift operator** ($B\,y_t = y_{t-1}$).

#### AR(p) — Autoregressive Component

$$
\Phi(B) = 1 - \phi_1 B - \phi_2 B^2 - \cdots - \phi_p B^p
$$

Each $\phi_i$ captures how much weight the *i*-th lag carries. An AR(1)
model $y_t = \phi_1 y_{t-1} + \varepsilon_t$ is a first-order Markov
process — the present depends only on the immediate past.

#### I(d) — Integration (Differencing)

Differencing of order *d* removes polynomial trends:
- *d* = 0: series is already stationary
- *d* = 1: removes linear trends
- *d* = 2: removes quadratic trends

In practice *d* ∈ {0, 1, 2} covers virtually all real-world time-series.

#### MA(q) — Moving Average Component

$$
\Theta(B) = 1 + \theta_1 B + \theta_2 B^2 + \cdots + \theta_q B^q
$$

MA terms model the **residual autocorrelation** — structure in the noise
that AR terms alone cannot capture.

### Parameter Selection

`auto_arima` follows **Hyndman–Khandakar**, which splits the decision in two:

```text
1. Differencing order d
   └── KPSS level-stationarity test, repeated until the series stops
       rejecting stationarity, capped at max_d

2. Non-seasonal orders (p, q), at that fixed d
   ├── Exhaustive grid, evaluated in parallel, or
   └── Stepwise: seed with (2,2), (0,0), (1,0), (0,1), then walk to
       improving neighbours
   └── scored by AICc

3. Estimation
   └── Burg for the AR seed, then conditional sum of squares
```

#### Why d is not chosen by AIC

Because it cannot be. Differencing changes the series the likelihood is
computed over — one fewer observation, and a different scale — so AIC values at
different \(d\) are not measuring the same quantity and do not rank anything.
Comparing them anyway **systematically over-differences**: the differenced
residual sum of squares is smaller, so on stationary noise every \(d = 1\)
candidate wins and the selected model forecasts a random walk.

The KPSS statistic tests the null hypothesis that the series is stationary
around a constant, so a value **above** the critical value (0.463 at 5 %) is
evidence that it is not. That direction is the opposite of an augmented
Dickey–Fuller test.

#### Information Criteria

Once \(d\) is fixed, `(p, q)` are ranked by **AICc**:

$$
\text{AICc} = n \ln\!\left(\frac{\text{RSS}}{n}\right) + 2k
             + \frac{2k(k+1)}{n - k - 1}
$$

where \(n\) counts only the innovations the *conditional* likelihood is
defined over — the conditioning warm-up is excluded — and \(k\) is
\(p + q + 1\), plus one more if the model carries a constant. AICc rather than
AIC because the correction matters at exactly the series lengths a gateway has,
and it converges to AIC as \(n\) grows.

A perfect fit (RSS exactly zero) makes the conditional log-likelihood
unbounded. The variance is floored at the smallest positive double rather than
the criterion returning "undefined", so every perfect fit scores the same
enormous negative number and the parameter penalty picks the simplest of them —
otherwise a deterministic series produces *no* scorable model at all.

### The constant

A constant is included when \(d + D \le 1\), and it is the mean of the fully
differenced series — so with no differencing it is the **level** the series
reverts to, and with one difference it is the **drift** per step. Above one
difference a constant implies a polynomial trend that keeps accelerating, which
is essentially never wanted from a metric.

Without it, ARIMA(0,1,0) on a line forecasts a flat line, and ARIMA(1,0,0) on a
series sitting at 100 has to explain that level with a near-unit root.

### The conditioning warm-up

The first \(\max(\deg \phi^*, \deg \theta^*)\) innovations are computed
from pre-sample history the model does not have, and are **excluded** from the
residual variance, from the RSS, and from the CSS objective. That is what the
word *conditional* in "conditional sum of squares" means.

Skipping it is a unit error rather than an imprecision: for ARIMA(p,0,q) the
first innovation is the raw first observation, so the residual variance would
measure the *level* of the series rather than its noise, and every prediction
interval with it.

### Stationarity and Invertibility

For the model to be valid:
- **Stationarity**: all roots of $\Phi(B) = 0$ must lie outside the unit circle
- **Invertibility**: all roots of $\Theta(B) = 0$ must lie outside the unit circle

The Burg algorithm guarantees a stable AR polynomial by construction, which is
why it is used to seed the search.

Invertibility is enforced by the **objective**, not the bounds. The ±0.99 box
on each MA coefficient is sufficient only for \(q = 1\) — \(\theta = (0.99,
-0.99)\) is inside it and \(1 + 0.99z - 0.99z^2\) has a root at \(|z| \approx
0.62\) — but the CSS error recursion diverges outside the invertible region,
so the optimiser does not go there. Measured across 612 fits at \(q \ge 2\):
none non-invertible, closest \(|z| = 1.069\), unchanged when the bounds are
widened to ±5.0. Checked by `a_fitted_ma_polynomial_is_invertible`.

---

## Seasonal ARIMA — SARIMA(p, d, q)(P, D, Q)_s

### Extension to Seasonality

SARIMA adds seasonal AR, differencing, and MA terms at lag *s*:

$$
\Phi(B)\,\Phi_s(B^s)\,(1-B)^d\,(1-B^s)^D\, y_t = \Theta(B)\,\Theta_s(B^s)\,\varepsilon_t
$$

where:
- $\Phi_s(B^s) = 1 - \Phi_1 B^s - \cdots - \Phi_P B^{Ps}$ (seasonal AR)
- $\Theta_s(B^s) = 1 + \Theta_1 B^s + \cdots + \Theta_Q B^{Qs}$ (seasonal MA)
- $(1 - B^s)^D$ is the seasonal differencing operator

### Example: SARIMA(1,1,1)(1,1,1)₂₄ for hourly data

This is a common model for hourly infrastructure metrics with daily
seasonality (*s* = 24):

| Component | Order | Interpretation |
|-----------|-------|----------------|
| AR(1) | $\phi_1$ | Short-range autocorrelation |
| I(1) | $d=1$ | Remove linear trend |
| MA(1) | $\theta_1$ | Smooth noise |
| SAR(1) | $\Phi_1$ at lag 24 | Daily autocorrelation |
| SI(1) | $D=1$ | Remove daily seasonal trend |
| SMA(1) | $\Theta_1$ at lag 24 | Smooth seasonal noise |

### One recursion, not two

The multiplicative form is **expanded** before anything is fitted: the AR
factors \(\Phi(B)\Phi_s(B^s)\) are convolved into a single polynomial of
degree \(p + P s\), and the MA factors into one of degree \(q + Q s\). The
ordinary ARMA recursion then runs over those, and `ArimaModel` is the
\(P = D = Q = 0\) case of the same code.

Expanding rather than special-casing is what makes the seasonal orders actually
reach the forecast. All four coefficient blocks (\(\phi, \Phi, \theta,
\Theta\)) are estimated **jointly** by conditional sum of squares, seeded from
a Burg fit of the non-seasonal AR part — the two AR factors multiply, so
estimating one while pretending the other is absent biases both.

### Computational Complexity

ARIMA estimation is **O(n · p²)** where *p* is the AR order. SARIMA
extends this to **O(n · (p + P·s)²)**, which can become expensive for
large seasonal periods.

Chronix mitigates this through:
1. **CSS instead of exact likelihood** — no Kalman filter, and the objective is
   a single pass over the differenced series per optimiser evaluation
2. **A split search** — `d` by unit-root test rather than by fitting, and a
   stepwise `(p, q)` walk that costs O(P+Q) fits instead of O(P×Q)
3. **Caching** — trained models are cached in the Model Lifecycle store
   and only retrained when the data distribution shifts significantly

### Limitations

| Limitation | Mitigation |
|------------|------------|
| Assumes linear relationships | Sufficient for most infrastructure metrics |
| Sensitive to outliers | Pre-process with anomaly detection |
| Expensive for large *s* | Limit seasonal period; use Fourier terms |
| Requires ≥ 2 full seasons | Fall back to Holt or SES for short series |
| CSS is less efficient than exact likelihood on short series | Accepted; a Kalman-filter likelihood is post-1.0 |
