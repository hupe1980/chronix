+++
title = "Preprocessing"
description = "Time-series data arriving from real-world systems is rarely clean. Sensors fail, networks drop packets, clocks drift, and sampling intervals vary. Preprocessing transforms raw data into a form…."
weight = 310
+++

Time-series data arriving from real-world systems is rarely clean. Sensors
fail, networks drop packets, clocks drift, and sampling intervals vary.
Preprocessing transforms raw data into a form suitable for analysis.

## Pipeline

```text
Raw ingested data
      │
      ▼
┌──────────────┐
│  Validation  │  ← Type checks, range checks, NaN detection
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Imputation  │  ← Fill missing values
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Resampling  │  ← Align to uniform time grid
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Smoothing   │  ← Remove high-frequency noise
└──────┬───────┘
       │
       ▼
  Clean time-series
```

## Missing Value Imputation

| Method | Formula | When to Use |
|--------|---------|-------------|
| Forward fill (LOCF) | $\hat{y}_t = y_{t^-}$ | Step-like metrics (states) |
| Linear interpolation | $\hat{y}_t = y_a + \frac{t - a}{b - a}(y_b - y_a)$ | Smooth metrics |
| Mean fill | $\hat{y}_t = \bar{y}$ | Random missing, no trend |
| Seasonal fill | $\hat{y}_t = y_{t-m}$ | Seasonal metrics with gaps |

**LOCF** (Last Observation Carried Forward) is the default in Chronix
because most infrastructure metrics represent instantaneous state —
"the value is whatever it was most recently."

### Gap Detection

Chronix distinguishes between **sporadic missing values** (imputable) and
**extended gaps** (data outage). A gap exceeding a configurable threshold
(default: 10× the expected interval) is marked as a true data gap rather
than imputed.

## Resampling

Real data arrives at irregular intervals. Resampling aligns observations to
a uniform time grid, required by most analytical methods.

### Downsampling

Reduce resolution (e.g. 1-second → 1-minute):

| Aggregation | SQL | Use Case |
|-------------|-----|----------|
| Mean | `AVG(value)` | Smooth metrics |
| Max | `MAX(value)` | Peak detection |
| Min | `MIN(value)` | Floor detection |
| Last | `LAST(value)` | Current state |
| Count | `COUNT(*)` | Event rates |

### Upsampling

Increase resolution (e.g. 1-minute → 1-second). Requires interpolation
to fill the new time slots. Linear interpolation is the standard choice.

## Smoothing

### Moving Average

The simple moving average over a window of width $w$:

$$
\bar{y}_t = \frac{1}{w} \sum_{i=0}^{w-1} y_{t-i}
$$

Removes high-frequency noise but introduces lag.

### Exponential Moving Average (EMA)

$$
\text{EMA}_t = \alpha\, y_t + (1-\alpha)\, \text{EMA}_{t-1}
$$

Less lag than SMA, controlled by $\alpha$.

### Savitzky-Golay Filter

Fits a local polynomial of degree $d$ to a window of $2m+1$ points by
least squares, then uses the central fitted value. Preserves peaks and
inflection points better than moving averages.

## Normalization

### Min-Max Scaling

$$
\hat{y}_t = \frac{y_t - y_{\min}}{y_{\max} - y_{\min}}
$$

Maps values to $[0, 1]$. Sensitive to outliers.

### Z-Score Standardization

$$
\hat{y}_t = \frac{y_t - \mu}{\sigma}
$$

Centers at zero, unit variance. Required for multivariate methods.

## Differencing

Removes trend for stationarity:

$$
y'_t = y_t - y_{t-1}
$$

Seasonal differencing removes periodic patterns:

$$
y'_t = y_t - y_{t-m}
$$

## Chronix Query Integration

Preprocessing is available as SQL functions:

```sql
-- Resampling is a time bucket.
SELECT time_bucket('1m', _time) AS bucket,
       avg(value)               AS resampled
FROM metrics
WHERE metric_name = 'cpu'
  AND _time > now() - INTERVAL '1 hour'
GROUP BY bucket;

-- Smoothing is `ewm`, a window function: it needs the ordering to smooth
-- along, so it is always used with OVER.
SELECT _time,
       ewm(value, 0.3) OVER (ORDER BY _time) AS smoothed
FROM metrics
WHERE metric_name = 'cpu';

-- Gap filling has no SQL form. It is a preprocessing step on the Rust API,
-- because "what value belongs in a gap" is a modelling choice a query
-- cannot make on your behalf.
```
