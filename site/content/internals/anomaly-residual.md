+++
title = "Residual-Based Anomaly Detection"
description = "Instead of applying statistical tests to raw values, the residual method first removes expected structure (trend, seasonality) using a forecast model, then applies anomaly detection to the residuals."
weight = 220
+++

## The Idea

Instead of applying statistical tests to raw values, the **residual method**
first removes expected structure (trend, seasonality) using a forecast model,
then applies anomaly detection to the **residuals**:

$$
e_t = y_t - \hat{y}_t
$$

If the forecast model captures the time-series dynamics well, the residuals
$e_t$ should be approximately **i.i.d.** (independent and identically
distributed) with zero mean. Any large residual indicates a deviation from
expected behavior.

## Pipeline

```text
Time-Series
     │
     ▼
┌───────────────┐
│ Forecast Model │  ← SES, Holt-Winters, ARIMA, …
│ (§ Forecasting)│
└───────┬───────┘
        │ ŷₜ
        ▼
┌───────────────┐
│  Residuals    │  eₜ = yₜ - ŷₜ
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ Statistical   │  ← Z-Score, MAD, or IQR on residuals
│ Detector      │
└───────┬───────┘
        │
        ▼
   Anomaly Label
```

## Why Residuals Are Better Than Raw Values

Consider a metric with daily seasonality — CPU usage that peaks at 80%
every afternoon:

| Approach | 80% at 2 PM | 80% at 3 AM |
|----------|-------------|-------------|
| Raw Z-score | Normal | Normal (same value!) |
| Residual Z-score | Normal (expected peak) | **Anomalous** (unexpected) |

The residual method inherits **contextual awareness** from the forecast
model. This eliminates a major class of false positives and false negatives.

## Residual Standardization

Raw residuals may have non-constant variance (heteroscedasticity). Chronix
offers two standardization options:

### Simple standardization

$$
z_t = \frac{e_t}{\hat{\sigma}_e}
$$

where $\hat{\sigma}_e$ is the residual standard deviation from the
training period.

### Rolling standardization

$$
z_t = \frac{e_t}{\hat{\sigma}_{e,w}}
$$

where $\hat{\sigma}_{e,w}$ is computed over a sliding window of width *w*.
This adapts to changing noise levels.

## Forecast Model Requirements

The quality of residual-based detection depends entirely on the forecast
model. A poor model leaves structure in the residuals, causing:

- **Systematic false positives** if the model under-predicts
- **Missed anomalies** if the model over-fits noise

### Diagnostic Checks

Chronix validates residuals automatically:

| Check | Method | Action on failure |
|-------|--------|-------------------|
| Autocorrelation | Ljung-Box test | Increase model order |
| Normality | Shapiro-Wilk test | Switch to MAD or IQR |
| Constant variance | Levene's test | Use rolling standardization |
| Zero mean | t-test | Add bias correction |

## One-Step vs Multi-Step Residuals

**One-step residuals** ($e_{t} = y_t - \hat{y}_{t|t-1}$) are preferred for
anomaly detection because they use the most recent information. Multi-step
residuals are noisier and less sensitive.

## Computational Cost

The cost is dominated by the forecast model:

$$
\text{Cost} = \text{Cost}_{\text{forecast}} + O(n)
$$

The residual computation and Z-score are both O(n), making the total cost
approximately equal to the forecast cost. For SES, this means the full
pipeline is O(n). For SARIMA, it is O(n · (p + P·s)²).
