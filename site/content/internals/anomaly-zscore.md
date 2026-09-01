+++
title = "Z-Score & MAD Detectors"
description = "The Z-score measures how many standard deviations a point lies from the mean."
weight = 200
+++

## Z-Score Detector

### Theory

The Z-score measures how many standard deviations a point lies from the mean:

$$
z_t = \frac{y_t - \mu}{\sigma}
$$

Under a **normal distribution**, the probability of observing $|z| > k$ is:

| Threshold *k* | Probability | Interpretation |
|---------------|-------------|----------------|
| 2.0 | 4.55% | Mild anomaly |
| 3.0 | 0.27% | Strong anomaly |
| 3.5 | 0.047% | Very strong |
| 4.0 | 0.006% | Extreme |

A point is flagged anomalous if $|z_t| > k$ where *k* is the configured
threshold (default: 3.0).

### Sliding Window

For non-stationary time-series, global $\mu$ and $\sigma$ are meaningless.
Chronix computes them over a **sliding window** of the most recent *w*
observations:

$$
\mu_w = \frac{1}{w} \sum_{i=t-w+1}^{t} y_i
\qquad
\sigma_w = \sqrt{\frac{1}{w-1} \sum_{i=t-w+1}^{t} (y_i - \mu_w)^2}
$$

The window can be updated incrementally in **O(1)** per new observation
using Welford's online algorithm:

$$
M_{2,n} = M_{2,n-1} + (y_n - \mu_{n-1})(y_n - \mu_n)
$$

$$
\sigma_n = \sqrt{M_{2,n} / (n-1)}
$$

### Limitations

- **Assumes normality**: Skewed or heavy-tailed distributions produce
  either too many or too few alerts
- **Sensitive to outliers**: A single extreme value inflates $\sigma$,
  masking subsequent anomalies (the **masking effect**)
- **No seasonality awareness**: A value normal at noon may be anomalous
  at midnight

---

## Median Absolute Deviation (MAD)

### Theory

MAD is a **robust** estimator of scale — it is not affected by outliers:

$$
\text{MAD} = \text{median}\left(|y_t - \tilde{y}|\right)
$$

where $\tilde{y} = \text{median}(y_1, \ldots, y_n)$.

The modified Z-score uses MAD instead of $\sigma$:

$$
z_t^{\text{MAD}} = \frac{0.6745 \cdot (y_t - \tilde{y})}{\text{MAD}}
$$

The constant **0.6745** is the 75th percentile of the standard normal
distribution, which makes the modified Z-score comparable to the standard
Z-score under normality:

$$
\text{For } Y \sim N(\mu, \sigma^2): \quad E[\text{MAD}] = 0.6745 \cdot \sigma
$$

### Robustness

The **breakdown point** of an estimator is the fraction of contaminated
data it can tolerate before giving arbitrary results:

| Estimator | Breakdown Point |
|-----------|----------------|
| Mean | 0% (1 outlier can shift it arbitrarily) |
| Standard deviation | 0% |
| Median | 50% |
| MAD | 50% |

MAD can tolerate up to 50% of the data being outliers and still produce
a valid scale estimate. This makes it ideal for noisy operational data.

### Sliding Window MAD

For streaming computation, Chronix maintains a sorted data structure
(order-statistic tree) over the window, enabling **O(log w)** median
updates as observations enter and leave the window.

### When to Use MAD vs Z-Score

| Scenario | Recommended |
|----------|-------------|
| Clean, normally distributed data | Z-Score (faster, well-understood) |
| Noisy data with occasional outliers | MAD (robust) |
| Unknown distribution | MAD (safer default) |
| Sub-millisecond latency required | Z-Score (O(1) update) |
