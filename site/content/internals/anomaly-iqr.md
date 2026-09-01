+++
title = "IQR (Interquartile Range) Detector"
description = "The interquartile range method is a non-parametric anomaly detector that makes no assumptions about the data distribution. It is based on the box-plot methodology introduced by Tukey (1977)."
weight = 210
+++

## Theory

The interquartile range method is a **non-parametric** anomaly detector that
makes no assumptions about the data distribution. It is based on the
box-plot methodology introduced by Tukey (1977).

### Quartiles

Given a sorted dataset, the quartiles divide it into four equal parts:

$$
Q_1 = \text{25th percentile}, \quad Q_3 = \text{75th percentile}
$$

The interquartile range is:

$$
\text{IQR} = Q_3 - Q_1
$$

### Fences

Tukey's fences define the boundary beyond which a point is considered
anomalous:

$$
\text{Lower fence} = Q_1 - k \cdot \text{IQR}
$$

$$
\text{Upper fence} = Q_3 + k \cdot \text{IQR}
$$

| Multiplier *k* | Classification |
|----------------|---------------|
| 1.5 | Mild outlier (Tukey's "outer fence") |
| 3.0 | Extreme outlier |

Any observation $y_t$ outside $[\text{Lower fence}, \text{Upper fence}]$
is flagged as anomalous.

### Geometric Interpretation

For a **normal distribution**, $\text{IQR} \approx 1.35\sigma$, so the
1.5× IQR fence corresponds to approximately $Q_1 - 2.0\sigma$ and
$Q_3 + 2.0\sigma$, or about ±2.7σ from the mean. This flags roughly
0.7% of normally distributed data — comparable to a Z-score threshold
of 2.7.

### Why Non-Parametric Matters

Real infrastructure metrics are often **skewed** (e.g. latency
distributions with a long right tail) or **multi-modal** (e.g. bimodal
CPU usage under different workload regimes). The IQR method handles
these naturally:

```text
Normal distribution:        Skewed distribution:
  ┌────────────────┐          ┌──────────────────────┐
  │   ┌──┬──┐     │          │  ┌┬─┐                │
  │   │  │  │     │          │  ││ │                 │
  │───┘  │  └───  │          │──┘│ └─────────────    │
  │      │        │          │   │                   │
  └──────┴────────┘          └───┴───────────────────┘
  Symmetric fences           Asymmetric data, same formula works
```

## Sliding Window IQR

For streaming anomaly detection, Chronix computes IQR over a sliding
window. Efficient computation requires maintaining sorted order:

| Approach | Insert | Remove | Quartile Query |
|----------|--------|--------|---------------|
| Sorted array | O(w) | O(w) | O(1) |
| Order-statistic tree | O(log w) | O(log w) | O(log w) |
| Two-heap (P2 approx) | O(1) | O(1) | O(1) approximate |

Chronix uses the **P² algorithm** (Jain & Chlamtac, 1985) for approximate
quantile estimation in O(1) per observation when the window size is very
large, and exact computation for smaller windows.

## Advantages and Limitations

| Aspect | Assessment |
|--------|-----------|
| Distribution-free | ✓ Works on any shape |
| Robust to outliers | ✓ Quartiles have 25% breakdown point |
| Interpretable | ✓ Maps directly to box-plot |
| Sensitivity tuning | ✓ Adjust *k* multiplier |
| Seasonal awareness | ✗ Requires seasonal decomposition first |
| Trend awareness | ✗ Requires detrending first |

For seasonal or trended data, pass the series through the preprocessor
(differencing or STL decomposition) before applying IQR detection.
