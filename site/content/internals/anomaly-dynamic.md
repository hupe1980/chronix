+++
title = "Dynamic Thresholds"
description = "Traditional monitoring uses static thresholds: 'alert if CPU > 90%'. This fails for metrics with."
weight = 230
+++

## The Problem with Static Thresholds

Traditional monitoring uses static thresholds: "alert if CPU > 90%".
This fails for metrics with:

- **Seasonal patterns**: A threshold tuned for peak hours triggers false
  alarms during off-peak, or misses anomalies during peaks.
- **Organic growth**: The baseline shifts upward over weeks, requiring
  manual threshold updates.
- **Heterogeneous sources**: Different servers have different normals — a
  single threshold cannot fit all.

## Dynamic Threshold Approach

A dynamic threshold **adapts** to the observed data distribution in a
sliding window. The threshold at time *t* is a function of recent history:

$$
\text{Threshold}_t = f(y_{t-w}, \ldots, y_{t-1})
$$

### EWMA-Based Thresholds

Chronix uses **Exponentially Weighted Moving Average** (EWMA) to track
the running mean and variance:

$$
\mu_t = \lambda\, y_t + (1 - \lambda)\, \mu_{t-1}
$$

$$
\sigma_t^2 = \lambda\, (y_t - \mu_t)^2 + (1 - \lambda)\, \sigma_{t-1}^2
$$

The dynamic bounds are:

$$
\text{Upper}_t = \mu_t + k \cdot \sigma_t
$$

$$
\text{Lower}_t = \mu_t - k \cdot \sigma_t
$$

where $\lambda \in (0, 1]$ controls the adaptation speed and $k$ is the
sensitivity multiplier (typically 3.0).

### Parameter Effects

| Parameter | Low value | High value |
|-----------|-----------|------------|
| λ (decay) | Smooth, slow adaptation | Responsive, noisy bounds |
| k (sensitivity) | More alerts, higher recall | Fewer alerts, lower recall |

## Seasonal Dynamic Thresholds

For metrics with strong seasonality, the dynamic threshold can incorporate
**seasonal baselines**:

$$
\mu_t^{(s)} = \text{EWMA of } y_t \text{ at the same seasonal offset}
$$

For hourly data with daily seasonality (*m* = 24), this maintains 24
separate EWMA trackers — one for each hour of the day:

```text
Hour 0:  μ₀, σ₀  →  bounds for midnight observations
Hour 1:  μ₁, σ₁  →  bounds for 1 AM observations
  ⋮
Hour 23: μ₂₃, σ₂₃ → bounds for 11 PM observations
```

Each tracker sees only the values at its corresponding hour, so the
bounds naturally reflect the diurnal pattern.

## Warm-Up Period

Dynamic thresholds require a **warm-up period** before producing
reliable bounds. During warm-up:

- Insufficient data: bounds are either disabled or set very wide
- Chronix requires at least *2w* observations before activating alerts
- For seasonal thresholds: at least 2 full seasonal cycles

## Comparison with Static Thresholds

| Aspect | Static | Dynamic |
|--------|--------|---------|
| Setup effort | Manual tuning per metric | Automatic |
| Seasonality | ✗ | ✓ (with seasonal variant) |
| Trend adaptation | ✗ | ✓ |
| Alert fatigue | High (poorly tuned) | Low |
| Warm-up needed | No | Yes |
| Interpretability | "CPU > 90%" | "3σ above 24h average" |

## EWMA Control Charts

Dynamic thresholds are closely related to **EWMA control charts** from
statistical process control (SPC), introduced by Roberts (1959). The key
difference is that SPC assumes a stationary in-control process, while
Chronix tracks a non-stationary evolving baseline.

### Shewhart vs EWMA vs CUSUM

| Method | Detects | Sensitivity |
|--------|---------|-------------|
| Shewhart | Large, sudden shifts | 3σ rule |
| EWMA | Small, gradual shifts | Weighted history |
| CUSUM | Persistent small shifts | Cumulative sum |

Chronix implements EWMA by default. CUSUM may be used for detecting
subtle performance degradations that are individually within normal range
but collectively significant.

## Implementation Notes

The dynamic threshold detector maintains state of size O(1) per metric
(or O(m) for seasonal variant). Updates are O(1) per observation,
making it suitable for high-throughput streaming ingestion.
