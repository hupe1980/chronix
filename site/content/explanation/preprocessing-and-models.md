+++
title = "Preprocessing, models and compute"
description = "Preprocessing pipelines, the model registry and lifecycle, drift detection, and the compute engine and its parallelism budget."
weight = 140
aliases = ["/internals/preprocessing/", "/internals/model-lifecycle/", "/internals/compute-engine/"]
+++

Real series have gaps, jitter and noise, and models go stale. Chronix's
`chronix_analytics::preprocess` turns a raw series into the regular one a model
needs, and `chronix_analytics::lifecycle` keeps fitted models honest: a
registry with champion and challenger versions, A/B comparison, drift
detection and retraining. Both run in process on the compute engine, which
vectorises the numeric kernels and respects the database's parallelism budget.

## Preprocessing

```text
raw ──▸ clock-drift correction ──▸ gap fill ──▸ resample ──▸ smooth ──▸ features / STL ──▸ model
```

| Step | Options | Notes |
|---|---|---|
| Clock drift | blend observed timestamps toward a uniform grid | for a sampler whose clock wanders |
| Gap fill | `Linear`, `Nearest`, `CatmullRom`, `Zero`, `Forward`, `Nocb` | Catmull-Rom is a C¹ spline that respects uneven spacing; needs increasing timestamps |
| Resample | bucket to a fixed width with an aggregation function | down- or upsampling; overflow-safe at the edges of `i64` |
| Smooth | `Exponential`, `MovingAverage`, `WeightedMovingAverage` | |
| Decompose | STL: trend, seasonal, remainder; robust mode for outliers | the robust outer loop stops one spike from bending the whole season |
| Features | lags, rolling statistics, calendar fields; automatic selection | inputs for regression and isolation forests |

`preprocess` runs the chain on a series from the database
(`db.preprocess(…)`); `arrow_adapters` runs it on Arrow arrays without copying.
From SQL the same building blocks are window functions: `rolling_mean`,
`rolling_std`, `ewm`, `diff`, `pct_change`, `stl_trend`, `stl_seasonal`,
`stl_residual`, `stl_decompose` — see the [SQL reference](@/reference/sql.md).

**Missing samples.** A NULL is a missing sample, not a missing row: it is left
out of every statistic, a window function of neighbours still answers at a gap
from the samples around it, and a function of the row's own value is NULL
there. Rolling statistics match pandas' `rolling(window)` on the same input,
NULLs included, and an undefined correlation or spread is `NaN`, never `0`.

## Model lifecycle

A fitted model is an artefact with a history. The lifecycle module gives it one.

| Part | What it does |
|---|---|
| `ModelRegistry` | stores versions per series with a tag: `Champion` (serving), `Challenger` (being evaluated), `Retired` |
| A/B test | scores champion and challenger on the same new data; promotes on `LowerMape`, `LowerRmse` or `LowerBoth` |
| `ForecastAccuracyTracker` | compares past forecasts with what happened: MAE, RMSE, MAPE |
| Drift detection | PSI, two-sample Kolmogorov–Smirnov, or ADWIN on the input or the errors; acts by `NoAction`, `AlertOnly` or `RetriggerFit` |
| `DriftMonitor` | checks every registered model and calls a callback on drift |

Models serialise with `serde` (Chronix uses postcard), so a registry can be
persisted wherever the application keeps state. A/B promotion ignores a NaN
metric rather than letting it decide.

On the server, fitted models are listed and managed under
`/api/v1/admin/analytics/models` (list, get, delete) and
`/api/v1/admin/analytics/retrain`; a retrained model is refitted on the next
data that arrives.

**Declared detectors** are the managed alternative to a detector object you
drive yourself: the database persists and advances them. See
[Forecast and detect](@/guides/forecast-and-detect.md).

## The compute engine

The numeric kernels — sum, mean and variance, min and max, dot products,
timestamp range filters — are vectorised and picked at run time:

| Target | Kernel |
|---|---|
| x86-64 | AVX-512F, else AVX2 + FMA, else SSE2 |
| aarch64 | NEON with FMA |
| other | portable scalar, unrolled four wide |

- **Accurate:** sums use Kahan compensation in every tier; variance is
  two-pass and Bessel-corrected (`simd_population_variance` is the ÷*n* form).
- **NaN-aware:** min and max skip NaN in every tier; all-NaN input gives NaN.
- **Checked:** mismatched lengths are an error, not a panic.
- **Bounded:** large inputs are split across threads only within the
  database's `parallelism` budget, so a gateway configured as serial stays
  serial.

There is no GPU backend: the expensive recursions (smoothing, ARIMA) are
sequential, and nothing in a measured profile is bound by arithmetic a GPU
would speed up.

## Related

- [Forecasting models](@/explanation/forecasting.md)
- [Anomaly detection](@/explanation/anomaly-detection.md)
- [SQL reference](@/reference/sql.md)
