+++
title = "Anomaly detection"
description = "The anomaly detectors in Chronix — z-score, MAD, IQR, CUSUM, forecast residual and seasonal thresholds — how each works and when to use it."
weight = 120
aliases = ["/internals/anomaly-detection/", "/internals/anomaly-zscore/", "/internals/anomaly-iqr/", "/internals/anomaly-cusum/", "/internals/anomaly-residual/", "/internals/anomaly-dynamic/"]
+++

Chronix ships seven single-series detectors. Each is fitted on a training
window, then scores points — in a batch or one at a time as they arrive. Every
result carries a score in [0, 1], an `is_anomaly` flag and a short reason, so
the detectors are interchangeable behind one trait. Pick by the shape of the data: a spike in
clean data, a spike in noisy or skewed data, a slow shift, or a value that is
only wrong at this time of day.

| Detector | Flags | Default | Assumes | Use for |
|---|---|---|---|---|
| `ZScoreDetector` | \|z\| above *k* | *k* = 3 | roughly normal | spikes in clean data |
| `ModifiedZScoreDetector` | median/MAD z-score above *k* | *k* = 3.5 | symmetric | spikes in noisy data |
| `IqrDetector` | outside Tukey's fences | 1.5 × IQR | nothing | skewed data such as latency |
| `MovingAverageResidualDetector` | distance from a moving average | window 24, *k* = 3 | slow trend | spikes on a drifting level |
| `DynamicThresholdDetector` | outside a rolling or per-season band | lookback 100, *k* = 3 | recent past is normal | baselines that move; daily patterns |
| `ForecastResidualDetector` | large forecast error | *k* = 3 | the model fits | contextual anomalies |
| `CusumDetector` | a persistent shift in the mean | *h* = 4σ, slack 0.5σ | independent points | drift, slow degradation |

## Spikes: z-score, MAD, IQR

**Z-score** measures distance from the mean in standard deviations. At
*k* = 3, about 0.27 % of normally distributed points are flagged. One extreme
value inflates σ and can hide the next one, and nothing here knows the time of
day.

**Modified z-score** replaces the mean with the median and σ with the median
absolute deviation (scaled by 0.6745 to be comparable). Median and MAD tolerate
up to half the data being outliers, so it is the safer default when the
distribution is unknown.

**IQR** flags points beyond *Q₁ − k·IQR* or *Q₃ + k·IQR*. It assumes no
distribution and handles the long right tail of latency naturally. For seasonal
or trending data, remove the season or trend first — STL or differencing in
[preprocessing](@/explanation/preprocessing-and-models.md).

## Context: forecast residuals and seasonal thresholds

A CPU at 80 % is normal at 2 PM and wrong at 3 AM. Two detectors know the
difference:

- **`ForecastResidualDetector`** wraps any forecasting model, predicts each
  point and z-scores the error. It is as good as the model: a model that misses
  the season leaves it in the residuals and flags every peak.
- **`DynamicThresholdDetector`** keeps a rolling mean and deviation over the
  last `lookback` points. Given a `period`, it keeps statistics **per position in
  the season** — one band per hour of the day for hourly data — and compares each
  point with its own slot.

## Shifts: CUSUM

CUSUM accumulates small deviations from the training mean in both directions
and signals when the running sum crosses *h*:

$$
S_i^+ = \max(0,\ S_{i-1}^+ + (x_i - \mu_0) - k) \qquad
S_i^- = \max(0,\ S_{i-1}^- - (x_i - \mu_0) - k)
$$

The slack *k* is the smallest shift worth detecting; *h* trades false alarms
against delay. With the defaults (*h* = 4σ, *k* = 0.5σ, both fitted from the
training window) a false alarm comes about every 170 points, and a one-σ shift
is caught in about six. An accumulator resets after it signals; the result says
whether the shift was up or down.

```rust
use chronix_analytics::anomaly::CusumDetector;

let mut detector = CusumDetector::new(None, None); // h and k from the data
detector.fit(&train_ts, &train_values)?;
for (ts, value) in live {
    let point = detector.detect_point(ts, value)?;
    if point.is_anomaly {
        println!("{}", point.details); // "upward shift" / "downward shift"
    }
}
```

## Detectors the store runs for you

A detector object belongs to the caller: its state, its schedule and its
persistence. A **declared detector** belongs to the database — declared once on
a measurement, advanced by the maintenance pass, and resumed after a restart
without relearning. It combines a seasonal baseline (one slot per position in a
calendar period, correct across daylight-saving days) with z-score and CUSUM
verdicts written to a measurement of their own. See
[Forecast and detect](@/guides/forecast-and-detect.md).

## From SQL

```sql
SELECT * FROM (
  SELECT _time, host, usage,
         anomaly_score(usage, 3.0) OVER (PARTITION BY host ORDER BY _time) AS score
  FROM cpu
) WHERE score > 0.5;
```

`anomaly_score(value, threshold)` is the z-score against the partition, mapped
to (0, 1) with 0.5 at the threshold. It is a window function: the `OVER` clause
is required, so a query that forgets to partition by series fails to plan
rather than mixing series. `detect_anomalies` on the Rust facade, the HTTP API
and the MCP server runs any detector by name.

## Choosing a threshold

Precision matters more than recall in operations: an alert that is usually
wrong gets ignored. Start at the default, look at what it flags on a week of
history, and raise *k* or *h* until the flagged points are ones you would act
on. Detectors judge single series; for patterns across series see
[Multivariate analysis](@/explanation/multivariate.md).

## Related

- [Forecasting](@/explanation/forecasting.md)
- [Streaming and triggers](@/explanation/streaming-and-triggers.md) — alerting on scores
- [SQL reference](@/reference/sql.md)
