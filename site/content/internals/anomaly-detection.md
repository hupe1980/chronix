+++
title = "Anomaly Detection"
description = "Anomaly detection identifies observations that deviate significantly from expected behavior. In time-series monitoring, anomalies signal potential incidents — hardware failures, misconfigurations,…."
weight = 190
+++

Anomaly detection identifies observations that deviate significantly from
expected behavior. In time-series monitoring, anomalies signal potential
incidents — hardware failures, misconfigurations, traffic spikes,
security breaches.

## Taxonomy

Anomaly detection methods fall into several families:

| Family | Method | Assumptions | Complexity |
|--------|--------|-------------|------------|
| Statistical | Z-Score | Normal distribution | O(n) |
| | MAD | Symmetric distribution | O(n) |
| | IQR | None (non-parametric) | O(n log n) |
| Residual-based | Forecast residuals | Adequate forecast model | O(forecast) |
| Adaptive | Dynamic thresholds | Evolving baseline | O(n) |
| Change-point | CUSUM | Independent observations | O(n) |

### Point vs Contextual vs Collective

- **Point anomalies**: A single value is anomalous regardless of context
  (e.g. CPU at 100% when baseline is 30%)
- **Contextual anomalies**: A value is anomalous only in a specific context
  (e.g. high traffic at 3 AM, normal at noon)
- **Collective anomalies**: A sequence of values is anomalous as a group
  (e.g. a sustained 20% drop over 2 hours)

Chronix focuses on **point** and **contextual** anomalies. Contextual
anomalies are handled by the forecast-residual approach, which accounts
for seasonality and trend.

## Detection Pipeline

```text
Raw time-series
      │
      ▼
┌─────────────┐
│ Preprocessing│  ← Imputation, resampling, smoothing
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Detector   │  ← Z-Score, MAD, IQR, Residual, Dynamic
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Scorer     │  ← Anomaly score per point
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Thresholding│  ← Binary decision: anomaly or not
└──────┬──────┘
       │
       ▼
  Alert / Label
```

## Evaluation Metrics

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| Precision | $\frac{TP}{TP + FP}$ | Fraction of alerts that are real |
| Recall | $\frac{TP}{TP + FN}$ | Fraction of real anomalies caught |
| F1 | $\frac{2 \cdot P \cdot R}{P + R}$ | Harmonic mean |

In operations, **precision** matters most — too many false alerts cause
**alert fatigue** and operators start ignoring alarms.

## Integration with Chronix

Anomaly detection is available through the query engine:

```sql
SELECT _time, value,
       anomaly_score(value, 3.0) OVER (ORDER BY _time) AS score
FROM metrics
WHERE metric_name = 'latency_p99'
  AND _time > now() - INTERVAL '24 hours';
```

The `ANOMALY_SCORE` function returns a normalized score (typically a
Z-score equivalent), and the `HAVING` clause acts as the threshold.

Each detection method is detailed in the following sub-sections.
