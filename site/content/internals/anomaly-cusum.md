+++
title = "CUSUM Change-Point Detection"
description = "Cumulative Sum (CUSUM) control charts detect persistent shifts in the mean of a process. Unlike point anomaly detectors (Z-Score, IQR) that flag individual outliers, CUSUM accumulates small…."
weight = 240
+++

Cumulative Sum (CUSUM) control charts detect **persistent shifts** in the
mean of a process. Unlike point anomaly detectors (Z-Score, IQR) that
flag individual outliers, CUSUM accumulates small deviations over time
and triggers when the cumulative evidence exceeds a threshold.

## Background

CUSUM was introduced by E.S. Page in 1954 for industrial quality control.
It remains one of the most widely used sequential change-point detection
methods due to its simplicity, efficiency, and theoretical optimality
(Lorden 1971 showed it minimizes worst-case detection delay for a given
false alarm rate).

## Algorithm

Chronix implements the **two-sided tabular CUSUM** which tracks both
upward and downward shifts simultaneously.

### Parameters

| Parameter | Symbol | Default | Description |
|-----------|--------|---------|-------------|
| Slack (allowance) | $k$ | $0.5\sigma$ | Minimum shift to detect |
| Decision threshold | $h$ | $4\sigma$ | Cumulative sum trigger level |
| Reference mean | $\mu_0$ | Training mean | Expected process center |
| Reference std | $\sigma$ | Training std | Process variability |

### Recurrence

At each observation $x_i$:

$$S_i^+ = \max(0, \; S_{i-1}^+ + (x_i - \mu_0) - k)$$
$$S_i^- = \max(0, \; S_{i-1}^- - (x_i - \mu_0) - k)$$

An **upward shift** is signaled when $S_i^+ > h$ and a **downward shift**
when $S_i^- > h$. After each alarm the corresponding accumulator resets
to zero (Western Electric convention).

### Anomaly Score

Raw CUSUM values are normalized to $[0, 1]$ via a sigmoid:

$$\text{score} = \frac{1}{1 + e^{-4(S/h - 1)}}$$

This maps the decision boundary ($S = h$) to score $\approx 0.5$, with
values well below threshold near 0 and large exceedances near 1.

## Average Run Length

The **Average Run Length** (ARL) is the expected number of observations
between false alarms under no-change conditions ($\text{ARL}_0$), or the
expected delay to detection after a real shift ($\text{ARL}_1$).

| $h / \sigma$ | $\text{ARL}_0$ (approx.) | Detects 1σ shift in |
|---------------|--------------------------|---------------------|
| 4 | ~170 | ~6 points |
| 5 | ~470 | ~8 points |
| 8 | ~4,000+ | ~12 points |

Higher $h$ reduces false alarms at the cost of slower detection.

## When to Use CUSUM

| Scenario | Recommended? |
|----------|-------------|
| Detecting gradual drift (sensor calibration) | **Yes** — CUSUM excels |
| Finding sudden large spikes | No — use Z-Score or IQR |
| Monitoring SLO compliance shifts | **Yes** |
| Streaming/online detection | **Yes** — O(1) per point |
| Seasonal data | Only after deseasonalization |

## Usage in Chronix

### Batch Detection

```rust
use chronix_analytics::anomaly::CusumDetector;

let mut detector = CusumDetector::new(None, None); // auto-tune h, k
detector.fit(&timestamps, &values)?;
let scores = detector.detect(&timestamps, &values)?;
```

### Streaming Detection

```rust
let mut detector = CusumDetector::new(Some(5.0), Some(1.0));
detector.fit(&training_ts, &training_vals)?;

for (ts, val) in live_stream {
    let score = detector.detect_point(ts, val)?;
    if score.is_anomaly {
        alert(score.details); // "upward shift" or "downward shift"
    }
}
```

### Via Streaming Analytics

CUSUM is available as `DetectorType::Cusum` in the streaming anomaly
detection pipeline, automatically tracked per series.

## References

- Page, E.S. (1954). "Continuous inspection schemes." *Biometrika* 41(1/2):100–115.
- Lorden, G. (1971). "Procedures for reacting to a change in distribution." *Annals of Mathematical Statistics* 42(6):1897–1908.
- Montgomery, D.C. (2009). *Statistical Quality Control.* 6th ed. Wiley. Chapter 9.
