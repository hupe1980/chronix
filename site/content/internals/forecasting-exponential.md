+++
title = "Exponential Smoothing"
description = "Exponential smoothing methods produce forecasts as weighted averages of past observations, where the weights decay exponentially. They are the most widely used forecasting family for operational…."
weight = 150
+++

Exponential smoothing methods produce forecasts as **weighted averages** of
past observations, where the weights decay exponentially. They are the
most widely used forecasting family for operational time-series.

## Simple Exponential Smoothing (SES)

### Model

SES maintains a single **level** component:

$$
\hat{y}_{t+1} = \ell_t = \alpha\, y_t + (1 - \alpha)\, \ell_{t-1}
$$

where $\alpha \in (0, 1]$ is the smoothing parameter.

Expanding recursively:

$$
\ell_t = \alpha \sum_{j=0}^{t-1} (1-\alpha)^j\, y_{t-j} + (1-\alpha)^t\, \ell_0
$$

The weight on observation $y_{t-j}$ is $\alpha(1-\alpha)^j$, which decays
geometrically — hence "exponential" smoothing.

### Choosing α

| α value | Behavior | Use case |
|---------|----------|----------|
| → 0 | Heavy smoothing, slow response | Stable, noisy series |
| ≈ 0.2 | Moderate decay | General purpose |
| → 1 | No smoothing, last value | Rapidly changing series |

Chronix selects α by minimizing **SSE** (sum of squared one-step-ahead
errors) over the training window using Brent's method.

### Flat Forecast

SES produces a **flat forecast**: $\hat{y}_{t+h} = \ell_t$ for all
horizons *h*. This makes it unsuitable for trended series.

---

## Holt's Linear Trend Method

### Model

Holt (1957) adds a **trend** component $b_t$:

$$
\ell_t = \alpha\, y_t + (1 - \alpha)(\ell_{t-1} + b_{t-1})
$$

$$
b_t = \beta\, (\ell_t - \ell_{t-1}) + (1 - \beta)\, b_{t-1}
$$

The *h*-step forecast extrapolates the trend linearly:

$$
\hat{y}_{t+h} = \ell_t + h\, b_t
$$

### Parameters

- $\alpha$ — level smoothing (same role as SES)
- $\beta$ — trend smoothing
  - $\beta \to 0$: trend changes slowly (stiff)
  - $\beta \to 1$: trend responds immediately

### Damped Trend

Gardner & McKenzie (1985) introduced a **damping parameter** $\phi \in (0, 1]$:

$$
\hat{y}_{t+h} = \ell_t + (\phi + \phi^2 + \cdots + \phi^h)\, b_t
$$

As $h \to \infty$, the forecast converges to $\ell_t + \frac{\phi}{1-\phi} b_t$
instead of diverging. Damped trends are the default in Chronix because
linear extrapolation is rarely realistic for infrastructure metrics.

---

## Holt-Winters (Triple Exponential Smoothing)

### Model

Holt-Winters (Winters 1960) adds a **seasonal** component $s_t$ with
period *m* (e.g. *m* = 24 for hourly data with daily seasonality):

**Additive seasonality:**

$$
\ell_t = \alpha\, (y_t - s_{t-m}) + (1 - \alpha)(\ell_{t-1} + b_{t-1})
$$

$$
b_t = \beta\, (\ell_t - \ell_{t-1}) + (1 - \beta)\, b_{t-1}
$$

$$
s_t = \gamma\, (y_t - \ell_t) + (1 - \gamma)\, s_{t-m}
$$

$$
\hat{y}_{t+h} = \ell_t + h\, b_t + s_{t-m+h_m^+}
$$

where $h_m^+ = ((h-1) \mod m) + 1$.

**Multiplicative seasonality:**

$$
\ell_t = \alpha\, \frac{y_t}{s_{t-m}} + (1 - \alpha)(\ell_{t-1} + b_{t-1})
$$

$$
s_t = \gamma\, \frac{y_t}{\ell_t} + (1 - \gamma)\, s_{t-m}
$$

$$
\hat{y}_{t+h} = (\ell_t + h\, b_t) \cdot s_{t-m+h_m^+}
$$

### Additive vs Multiplicative

| Aspect | Additive | Multiplicative |
|--------|----------|----------------|
| Seasonal amplitude | Constant | Proportional to level |
| Formula | $y = \ell + s$ | $y = \ell \times s$ |
| Use when | Amplitude ≈ constant | Amplitude grows with level |
| Example | Temperature | Retail sales |

### Parameters

| Parameter | Controls | Typical range |
|-----------|----------|---------------|
| α | Level responsiveness | 0.05 – 0.3 |
| β | Trend responsiveness | 0.01 – 0.2 |
| γ | Seasonal responsiveness | 0.01 – 0.3 |
| m | Seasonal period | Auto-detected or user-specified |

### Initialization

Good initial values for $\ell_0$, $b_0$, and $s_{1..m}$ are critical.
Chronix uses the **OLS decomposition** method:

1. Compute initial level: average of first complete season
2. Compute initial trend: slope of a linear regression over 2–3 seasons
3. Compute initial seasonal indices: ratios (multiplicative) or differences
   (additive) of the first complete season from the initial level

### Computational Complexity

All exponential smoothing variants are **O(n)** in the training set size
and **O(m)** in memory (storing *m* seasonal indices). This makes them
extremely efficient for real-time retraining on sliding windows.
