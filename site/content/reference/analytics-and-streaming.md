+++
title = "Analytics & Streaming"
description = "The analytics engine, real-time analytics, the CDC event bus, the pipeline orchestrator and the signal/trigger system."
weight = 70
+++

## Analytics Engine

Chronix embeds a full statistical analytics engine — no Python/R sidecars needed.

### Architecture

```text
SQL Query                    Rust API
    │                            │
    ▼                            ▼
┌────────────────────────────────────────┐
│  DataFusion UDFs / Chronix::forecast() │
│  15 scalar UDFs + 4 aggregate UDAFs    │
│  + runtime register_udf/register_udaf  │
└──────────────────┬─────────────────────┘
                   │
          ┌────────┴────────┐
          ▼                 ▼
┌──────────────┐   ┌──────────────────┐
│ chronix-     │   │ chronix-         │
│ preprocess   │   │ forecast         │
│ ·interpolate │   │ ·SES             │
│ ·Catmull-Rom │   │ ·Holt Linear     │
│ ·smooth      │   │ ·Holt-Winters    │
│ ·resample    │   │ ·ARIMA / SARIMA  │
│ ·features    │   │ ·Linear Reg      │
│ ·STL decomp  │   │ ·parallel fit    │
│ ·auto-feat   │   │                  │
└──────────────┘   └──────────────────┘
          │                 │
          ▼                 ▼
┌──────────────┐   ┌──────────────────┐
│ chronix-     │   │ chronix-         │
│ anomaly      │   │ multivariate     │
│ ·Z-Score     │   │ ·correlation     │
│ ·Modified Z  │   │ ·derived series  │
│ ·IQR         │   │ ·composite sigs  │
│ ·Forecast    │   │ ·Mahalanobis     │
│ ·Moving Avg  │   │ ·IsolationForest │
│ ·Dynamic     │   │ ·PCA anomaly     │
└──────────────┘   │ ·VAR forecast    │
                   └──────────────────┘
          │                 │
          └────────┬────────┘
                   ▼
          ┌──────────────────┐
          │ chronix-analytics::compute  │
          │ ·SIMD (arch)     │
          │ ·Rayon parallel  │
          │ ·BufferPool      │
          │ ·LU solver       │
          └──────────────────┘
                   │
                   ▼
          ┌──────────────────┐
          │ chronix-analytics::lifecycle│
          │ ·Model registry  │
          │ ·A/B testing     │
          │ ·Drift detection │
          │ ·DriftMonitor    │
          │ ·Accuracy track  │
          └──────────────────┘
```

### Forecast Models

| Model | Params | Online Update | Confidence Intervals |
|-------|--------|---------------|---------------------|
| SES | α (auto-optimized) | O(1) | √(1 + (h-1)·α²) |
| Holt Linear | α, β, φ (damped) | O(1) | Widening with horizon |
| Holt-Winters | α, β, γ, period | O(1) | Seasonal-aware |
| ARIMA(p,d,q) | Yule-Walker AR, MA | O(d) | Residual std error |
| SARIMA | + (P,D,Q,m) seasonal | O(d+D·m) | Seasonal undiff |
| Linear Regression | slope, intercept, R² | O(1) Welford | SE widening |

All models implement `ForecastModel` trait for uniform dispatch and support
serialization via `serde` + `bincode`.

**Auto ARIMA order selection** – `auto_arima()` takes the differencing order
`d` from a KPSS level-stationarity test, then ranks `(p, q)` at that fixed `d`
by AICc, returning the best order and all ranked candidates in
`AutoArimaResult`. `AutoArimaOptions` controls the budget: `max_evals` caps the
number of fits and `strategy` selects an exhaustive grid or a Hyndman–Khandakar
stepwise walk. An information criterion cannot compare models at different `d`,
which is why the two decisions are split.

### Anomaly Detectors

| Detector | Approach | Streaming | Complexity |
|----------|----------|-----------|------------|
| Z-Score | μ ± kσ | O(1) per point | O(n) fit |
| Modified Z-Score | MAD-based | O(1) per point | O(n log n) fit |
| IQR | Q1/Q3 fences | O(1) per point | O(n log n) fit |
| Forecast Residual | Model error | O(1) per point | Model-dependent |
| Moving Average | Rolling MA deviation | O(1) amortized | O(n) fit |
| Dynamic Threshold | Adaptive rolling | O(1) amortized | O(n) fit |

**Seasonal-aware dynamic thresholds** – `DynamicThresholdDetector::with_period(lookback, k, period)` configures seasonal bucket statistics. During `fit()`, training data is grouped into `period` buckets (index % period), computing per-bucket mean and standard deviation. Detection then compares each point against its matching seasonal bucket rather than a global rolling window, reducing false positives in data with strong periodicity.`detect_point()` advances `seasonal_phase` after each call so consecutive streaming points map to the correct season bucket. The ring buffer is also updated in `detect_point()` for non-seasonal rolling statistics. Seasonal bucket variance uses Bessel's correction (`/(n-1)` instead of `/n`), eliminating ~15% std underestimation when few cycles are available (e.g., 3–4 observations per bucket). This is consistent with Bessel's correction used elsewhere in the codebase (`simd_variance`, `rolling_std`, `zscore`, `compute_covariance`).

**`AnomalyDetector` takes `&mut self`** – `detect()` and `detect_point()` trait methods take `&mut self`, allowing implementations to maintain state between calls (e.g., ring buffer updates, seasonal phase advancement). `batch_detect()` operates on `&mut [D]`. Constructors for `MovingAverageResidualDetector` and `DynamicThresholdDetector` clamp window_size/lookback to `max(1)` to prevent `% 0` panics. All 7 detectors validate `timestamps.len() == values.len()` via `validate_lengths()` before iteration — returns `AnomalyError::InvalidInput` on mismatch instead of index-out-of-bounds panic.

### Input Validation

All `fit()` methods validate inputs:
- **Finite check**: rejects NaN/Inf values with `ForecastError::InvalidInput`
- **Length consistency**: timestamps and values must match in length
- **Minimum data**: model-specific minimum (e.g., 2 for SES, 2×period for Holt-Winters)

NaN-safe sorting uses `f64::total_cmp` throughout (no `partial_cmp().unwrap()` panics).

**Overflow-safe timestamps** – every `predict()` generates
`last_ts + h · interval_ns` with `saturating_add`/`saturating_mul`, so a large
horizon or interval cannot wrap `i64`. Applies to SES, Holt, Holt-Winters,
ARIMA, SARIMA, linear regression and the multivariate VAR/MLR models.

**Sign-preserving seasonal division** – multiplicative Holt-Winters divides by
seasonal factors through `safe_div(x, y)`, which clamps the divisor's
*magnitude* away from zero and keeps its sign. Clamping with `.max(1e-10)`
instead turns a negative seasonal factor into a tiny positive one and corrupts
the forecast.

**Zero-window guards** – `RollingCorrelation::new()` clamps `window` to
`max(1)`, `RollingStatExpr` uses `saturating_sub`, and `IsolationForestDetector`
clamps `n_trees` and `subsample_size` to `max(1)`, so no window arithmetic can
underflow `usize` and no tree count can reach `log2(0)`.

**Overflow-safe preprocessing** – `chronix-analytics::preprocess` uses overflow-safe arithmetic in three hot paths: (1) **Resampling** — bucket-start alignment uses `offset - offset.rem_euclid(interval)` instead of `(offset / interval) * interval` to avoid multiplication overflow; `bucket_end` uses `saturating_add`; zero-interval inputs are guarded. (2) **Interpolation** — gap-filling timestamp generation uses `saturating_add`/`saturating_mul` instead of bare `+`/`*`; Catmull-Rom spline interpolation (`SplineStrategy::CatmullRom`) provides C¹-smooth curve fitting via non-uniform Barry & Goldman parameterisation that respects actual timestamp spacing. (3) **Clock drift correction** — uses α-blending to smoothly transition between observed and ideal (uniform) timestamps, preventing abrupt jumps; the α factor is clamped to `[0, 1]` and defaults to `0.5`; timestamp generation uses `saturating_add`/`saturating_mul` for overflow safety.

**Error propagation** – `compute_accuracy()` returns
`Result<AccuracyMetrics, AnalyticsError>`, and `ForecastAccuracyTracker::evaluate()`
propagates with `?` rather than swallowing.

### SQL Interface

Chronix registers three kinds of function, and which kind a function is
follows from what it needs to see.

| Shape | When | Call site |
|---|---|---|
| **Scalar** | the answer for a row depends only on that row | `time_bucket('1h', _time)` |
| **Window** | the answer depends on the neighbouring rows, in order | `diff(v, 1) OVER (PARTITION BY host ORDER BY _time)` |
| **Aggregate** | the answer is one value (or one list) for a group | `last(v, _time)`, `forecast(v, _time, 12)` |

This is a correctness distinction, not a stylistic one. DataFusion hands a
scalar UDF one `RecordBatch` at a time, with no ordering, no partitioning and
no guarantee about how the plan split the rows — so a function that depends on
its neighbours cannot be a scalar one without its answer depending on batch
size, partition count and segment layout. Window functions receive one
partition in a stated order; aggregates receive a group. Both are declared by
the caller in SQL, and the `OVER` clause is mandatory, so a query that forgets
to partition **fails to plan** rather than answering wrongly.

#### Scalar

| Function | Description |
|---|---|
| `time_bucket(interval, timestamp)` | Floor a timestamp to an interval (overflow-safe `rem_euclid`) |

#### Window — require `OVER (PARTITION BY … ORDER BY …)`

| Function | Description |
|---|---|
| `diff(value, order)` | n-th order differencing; first `order` rows NULL |
| `pct_change(value)` | Fractional change from the previous row |
| `rolling_mean(value, window)` | Trailing arithmetic mean |
| `rolling_std(value, window)` | Trailing sample standard deviation (Welford's) |
| `rolling_corr(a, b, window)` | Trailing Pearson correlation |
| `zscore(value)` | Standardised against the partition |
| `ewm(value, alpha)` | Exponentially weighted mean, `alpha ∈ (0, 1]` |
| `anomaly_score(value, threshold)` | Z-score anomaly score |
| `stl_trend(value, period)` | STL trend component |
| `stl_seasonal(value, period)` | STL seasonal component |
| `stl_residual(value, period)` | STL residual component |
| `stl_decompose(value, period)` | All three at once, as `STRUCT{trend, seasonal, residual}` |
| `correlation(a, b)` | Pearson correlation over the partition |
| `cross_correlation(a, b, lag)` | Correlation at one lag |
| `multivariate_anomaly(a, b, threshold)` | Mahalanobis distance |

Every window kernel consumes the whole partition, so a `ROWS BETWEEN` frame
would be ignored — the rolling functions take their window length as an
argument instead. A row whose input was NULL gets a NULL result, and the
warm-up rows a rolling window cannot reach are NULL rather than `NaN`, so
`IS NULL` and `avg()` behave.

#### Aggregate

| Function | Description |
|---|---|
| `first(value, timestamp)` | Value at the earliest timestamp |
| `last(value, timestamp)` | Value at the latest timestamp |
| `rate(value, timestamp)` | Per-second rate of a counter |
| `irate(value, timestamp)` | Instantaneous rate from the last two points |
| `forecast(value, timestamp, horizon)` | `LIST(DOUBLE)` of `horizon` predicted values (SES) |
| `multivariate_forecast(target, predictor, timestamp, horizon)` | `LIST(DOUBLE)` from a multiple linear regression |

```sql
-- Twelve points ahead for every host, one row per predicted point.
SELECT host, unnest(forecast(usage, _time, 12)) AS predicted
FROM cpu
GROUP BY host;

-- A per-host z-score beside the raw sample.
SELECT _time, host, usage,
       zscore(usage) OVER (PARTITION BY host ORDER BY _time) AS z
FROM cpu;
```

The `timestamp` argument on the aggregates is load-bearing: an aggregate sees
rows in whatever order the plan delivers them, so the accumulator sorts by it.

### Performance Characteristics

- **Rolling std**: O(n) using Welford's sliding window with periodic correction
- **Rolling min/max**: O(n) using monotonic deque (preprocess + derived series)
- **Rolling correlation**: O(n) using running sums
- **Kendall Tau**: O(n log n) using merge-sort inversion counting (Knight 2006)
- **Cross-correlation matrix**: SIMD-vectorized inner products
- **Parallel fit**: rayon work-stealing across series
- **SIMD**: Architecture-dispatched vectorized operations (x86_64 SSE2, aarch64 NEON, scalar fallback) with 4-wide ILP unrolling (8 elements per iteration)
- **Downscale resampling**: O(n) two-pointer scan (sorted timestamps)
- **Multi-linear regression predict**: uses stored coefficients × last predictors (persistence forecast)
- **PCA anomaly detection**: cached point vectors — single pass over data
- **Isolation Forest**: random subsampling tree ensemble with O(n·t) fit where t = number of trees
- **CorrelationMethod trait**: trait-object dispatch for Pearson/Spearman/KendallTau
- **SeasonalDecomposer trait**: pluggable decomposition strategy via `StlDecomposer`
- **MultivariateConfig**: centralized config with 6 tunable parameters in `ChronixConfig`

**Composite signal inputs** – a `CompositeSignalRule` condition receives
`(&MultiSeriesContext, &AnalyticsResults)`, where `AnalyticsResults` carries
`forecasts: HashMap<String, Vec<f64>>` and
`anomaly_scores: HashMap<String, Vec<f64>>` — so a rule can read upstream
forecasts and anomaly scores per series.

### Arrow Zero-Copy Adapters (`chronix-analytics::preprocess`)

The preprocessing crate exposes `arrow_adapters` for zero-copy interop with
Apache Arrow arrays:

- **`ArrowPreprocessResult`** — `{ values: Float64Array, timestamps: Int64Array, gaps_filled: usize }`
- **`arrow_interpolate()`** — Accepts `&Float64Array` + `&Int64Array`, extracts
  zero-copy slices via `.values().as_ref()`, runs interpolation, returns Arrow arrays
- **`arrow_smooth()`** — Same pattern for smoothing operations
- **`arrow_preprocess()`** — Full pipeline wrapper (interpolate → smooth → resample)

This enables DataFusion UDFs to pass Arrow columnar data directly without
per-element copying.

### SIMD Architecture Dispatch (`chronix-analytics::compute`)

The compute crate uses multi-tier SIMD with runtime feature detection on x86_64:

```text
simd_sum() / simd_variance() / simd_population_variance()
simd_min_max() / simd_dot_product() / simd_range_filter_i64()
    │
    ├── #[cfg(target_arch = "x86_64")]
    │       ├── is_x86_feature_detected!("avx512f")  → AVX-512F (8×f64, 32 elem/iter)
    │       ├── is_x86_feature_detected!("avx2"+"fma") → AVX2+FMA (4×f64, 16 elem/iter)
    │       └── baseline SSE2                          → SSE2 (2×f64, 8 elem/iter)
    │
    ├── #[cfg(target_arch = "aarch64")] → NEON+FMA (2×f64, 8 elem/iter)
    └── fallback                        → Portable 4-wide scalar ILP unroll
```

All tiers use **4 accumulators** to hide FPU latency and maximize ILP.
AVX2/AVX-512 variance and dot product use FMA (`_fmadd_pd`) for fused
multiply-add. `simd_tier()` returns the active tier string for diagnostics.
Horizontal reductions use manual lane extraction (no `_mm512_reduce_*`).

**Two-pass SIMD variance**: `simd_mean_variance()` uses a two-pass approach —
pass 1 computes sum → mean via `simd_sum()`, pass 2 computes sum of squared
deviations using architecture-specific SIMD lanes. Two vectorised passes beat
a scalar Welford loop comfortably on large arrays.

**SIMD timestamp range filter**: `simd_range_filter_i64()` accelerates the most
common query predicate (timestamp range) with a 4-wide portable scalar unroll,
processing 4 i64 elements per loop iteration.

**Kahan compensated summation**: All 5 SIMD sum implementations (scalar,
SSE2, AVX2, AVX-512, NEON) use Kahan compensated summation, achieving O(ε)
rounding error instead of O(nε) from naive accumulation. Each accumulator
lane maintains a separate compensation variable, and compensations are
folded during horizontal reduction. The scalar variance fallback also uses
Kahan compensation for numerical stability on large arrays.

**NaN handling in `simd_min_max()`**: All SIMD tiers consistently skip NaN
values when computing min/max. On x86_64, SSE2/AVX2/AVX-512 use
`_mm*_min_pd` / `_mm*_max_pd` which propagate non-NaN operands. On aarch64,
NEON uses `vminnmq_f64` / `vmaxnmq_f64` (FMINNM/FMAXNM instructions) which
IEEE 754-2008 defines as "minNum/maxNum" — returning the non-NaN argument
when one operand is NaN. The scalar fallback mirrors this with explicit
`is_nan()` checks. If all values are NaN, the result is `(NaN, NaN)`.

`simd_dot_product()` returns `Result<f64, ComputeError>` — mismatched input
lengths produce `ComputeError::DimensionMismatch` rather than a panic,
making it safe for direct use in application code.

`simd_variance()` returns **sample variance** (Bessel-corrected, ÷(n−1)) and
`NaN` for n < 2; `simd_population_variance()` is the ÷n form, for when the
slice is the whole population rather than a sample.

### DriftMonitor (`chronix-analytics::lifecycle`)

`DriftMonitor` provides a background monitoring loop for model drift:

- **`register(name, detector, reference_data)`** — registers a model for monitoring
- **`update_current(name, current_data)`** — updates the latest data distribution
- **`check_all()`** — evaluates all registered detectors, fires callback on drift
- **`DriftCallback`** — `Box<dyn Fn(&str, &DriftReport) + Send + Sync>` for
  flexible event handling (logging, signal emission, auto re-fit trigger)
- Metrics: `chronix_model_drift_detected_total`, `chronix_model_drift_score` gauge

**Statistical correctness**: The two-sample KS test uses proper ECDF computation
(advance pointer, then compute CDFs) for accurate statistic calculation. PSI
`bin_counts` filters NaN values before binning to prevent silent skew. All
A/B test promotion criteria (`LowerMape`, `LowerRmse`, `LowerBoth`) guard
against NaN metrics to avoid permanent block/pass states.

### Lazy Derived Series (`chronix-analytics::multivariate`)

`LazyDerivedSeries` wraps a `DerivedSeriesExpr` with on-demand evaluation and
caching:

- **`get(ctx)`** — evaluates the expression against a `MultiSeriesContext` the
  first time, caches the result for subsequent calls
- **`invalidate()`** — clears the cache, forcing re-evaluation on next `get()`
- **`is_cached()`** — check if a cached result exists
- Thread-safe via `Mutex<Option<Vec<f64>>>` for the cached values

`DerivedSeriesEngine` evaluates a DAG of definitions using **wave-front
parallel execution** via `rayon`. Definitions are topologically sorted into BFS
levels via `topological_sort_levels()` (Kahn's algorithm). Within each level,
independent nodes are evaluated in parallel using `rayon::par_iter()`.
Single-node levels skip parallel overhead. Context is merged between levels.

### Signal Delivery Channel (`chronix-analytics::multivariate`)

`signal_channel(capacity)` creates a bounded `(SignalSender, SignalReceiver)`
pair for delivering composite signals from the evaluation engine to downstream
consumers. The channel applies back-pressure when full to prevent unbounded
memory growth (default capacity: `DEFAULT_SIGNAL_CHANNEL_CAPACITY = 1024`):

- **`SignalSender::send(signal)`** — blocking send via bounded `sync_channel` (blocks when full)
- **`SignalSender::try_send(signal)`** — non-blocking attempt, returns `TrySendError::Full` if at capacity
- **`SignalReceiver::recv()`** — blocking receive
- **`SignalReceiver::try_recv()`** — non-blocking attempt
- **`SignalReceiver::drain()`** — collects all pending signals

### Analytics bounds

`[analytics]` carries two settings, and both are enforced by the SQL forecast
aggregates:

```rust
let config = ChronixConfig::builder()
    .analytics(AnalyticsConfig {
        max_forecast_horizon: 720,     // most points one forecast() may return
        max_training_points: 100_000,  // most input points a model is fed
    })
    .build()?;
```

A `forecast(v, _time, h)` with `h` over the limit is a planning error naming
the setting; the training cap keeps the **newest** points.

The model, the detector and the confidence level are arguments to the
analytics API rather than settings, so a per-call choice stays a per-call
choice.

## Real-Time Analytics (`chronix-analytics`)

Streaming analytics engines that run on the CDC ingest path.

```text
CDC PointWritten events
      │
      ├──→ StreamingAnomalyEngine (per-series detection, < 10 ms)
      │          └──→ AlertEngine (Log, Metric, Webhook, CdcEvent)
      │
      ├──→ ContinuousForecastEngine (incremental model updates)
      │          └──→ ForecastCache (DashMap, TTL-based materialization)
      │
      └──→ AnomalyPrecisionTracker (TP/FP feedback)
           ForecastAccuracyTracker (MAPE/RMSE/MAE)
```

### Key Components

| Component                  | Purpose                                         |
|----------------------------|-------------------------------------------------|
| `StreamingAnomalyEngine`   | Per-series fitted detectors via `DashMap<Key, Arc<Mutex<State>>>` |
| `ContinuousForecastEngine` | Incremental model updates, auto re-fit on drift  |
| `ForecastCache`            | TTL-based materialized forecast views             |
| `AlertEngine`              | Threshold + cooldown, configurable alert actions  |
| `AnomalyPrecisionTracker`  | Rolling precision from TP/FP user feedback        |
| `ForecastAccuracyTracker`  | Retroactive MAPE/RMSE/MAE computation             |

### Concurrency & Data Integrity

- **Shard-lock-free per-series access:** Both `StreamingAnomalyEngine` and
  `ContinuousForecastEngine` store per-series state as
  `DashMap<Key, Arc<parking_lot::Mutex<State>>>`. During processing, the `Arc`
  is cloned and the DashMap shard lock released *before* acquiring the per-series
  Mutex — this prevents expensive `fit()` calls from blocking other series in the
  same shard.

- **O(1) FIFO training buffers:** Timestamps and values use `VecDeque` with
  `push_back()`/`pop_front()` eviction instead of `Vec` + `drain(..excess)`,
  bounded by `max_buffer_size` (default 10,000).

- **NaN/Inf guard:** `extract_numeric_value()` rejects non-finite values via
  `.filter(|v| v.is_finite())` — prevents silent corruption of detector/model
  training data.

- **Cooldown leak prevention:** `AlertEngine::evict_stale_cooldowns()` removes
  expired entries from the `last_fired` DashMap.
## CDC & Event Streaming (`chronix-streaming::cdc`)

Change Data Capture generates events on every data mutation. Events flow through
a bounded MPMC broadcast bus to subscribers.

```text
Write Path (WAL + Memtable)
      │
      ▼
  EventBus (tokio::sync::broadcast, cap: 65 536)
      │
      ├── Subscription (raw)
      ├── FilteredSubscription (measurement / tag / type filters)
      ├── CdcStream (async Stream<Item = CdcEvent>, backed by BroadcastStream)
      └── BusStats { published, lag, filtered_count }
```

### Event Types

| Event                | Trigger                      | Fields                          |
|----------------------|------------------------------|---------------------------------|
| `PointWritten`       | `insert()` / `insert_batch()`| measurement, tags, fields, ts, seq |
| `SeriesDeleted`      | `delete_series()`            | measurement, tags, hash, seq    |
| `MeasurementDropped` | `drop_measurement()`         | measurement, seq                |

### Aggregation over time

There is no aggregation engine on the CDC bus. Bucketed aggregates are
**rollups**, materialised once per final bucket by the storage layer with a
persisted watermark, and read — including the not-yet-final tail, computed
on the fly — through `Chronix::rollup`. A second aggregate implementation
on the event stream was cut: two implementations of one aggregate disagree,
and this one had no watermark.

### Event Log Durability

The durable event log uses `BufWriter` for batched I/O. Both `append()` and
`append_batch()` call `flush()` + `sync_data()` to ensure events survive OS
crashes (not just process crashes). The `compact()` path uses `sync_all()` for
full metadata durability.
## Pipeline Orchestrator (`chronix::pipeline`)

The `Pipeline` struct dispatches CDC events through the signal, analytics,
authz and audit engines as a single processing layer.

```text
Chronix write path
      │
      ▼
   CDC EventBus
      │
      ├──→ TriggerEngine ──→ DeliveryRouter ──→ SignalStore
      │       (signal)          (webhook, log, metrics)
      │
      ├──→ StreamingAnomalyEngine ──→ AlertEngine ──→ DeliveryRouter
      │       (analytics)
      │
      └──→ ContinuousForecastEngine ──→ ForecastCache
              (analytics)

AuthzEngine ──→ AuditLogger (cross-cutting)
```

### API

| Method                    | Description                                        |
|---------------------------|----------------------------------------------------|
| `Pipeline::new()`         | Default config (log + metric delivery, 50K audit)  |
| `Pipeline::with_config()` | Custom `PipelineConfig`                            |
| `process_cdc_event()`     | Dispatches a single event through all engines      |
| `execute_signal_sql()`    | CREATE/SHOW/DROP/ALTER triggers via SQL             |
| `spawn_cdc_listener()`    | Background tokio task subscribing to `EventBus`    |
| `audit(AuditEvent)`       | Logs an audit event through the audit logger       |

### Configuration

```rust
PipelineConfig {
    signal_store_capacity: 100_000,   // max stored signals
    enable_log_delivery: true,        // LogChannel on/off
    enable_metric_delivery: true,     // MetricChannel on/off
    audit_memory_capacity: 50_000,    // 0 = no memory sink
    forecast_horizon: 10,             // forecast steps ahead
}
```

### Event Processing Flow

1. **Trigger evaluation** — `TriggerEngine::process_event_collect()` checks all
   registered triggers, returning fired `SignalEvent`s inline (race-free).
   Signals are delivered via `DeliveryRouter` and persisted to `SignalStore`.
2. **Anomaly detection** — `StreamingAnomalyEngine::process_write()` scores each
   point. Anomalous scores are fed to `AlertEngine::evaluate()` for threshold-
   based alert firing with cooldown deduplication. Fired alerts are converted to
   `SignalEvent`s, delivered through the router, and persisted to the signal store.
3. **Forecast updates** — `ContinuousForecastEngine::process_point()` updates
   models incrementally. On initial fit or re-fit, the forecast is materialized
   into `ForecastCache`.
4. **Audit logging** — Cross-cutting `audit()` helper logs security events via
   the pluggable `AuditSink` chain.

All engines are wrapped in `Arc` for concurrent access. `AuditLogger::add_sink()`
and `DeliveryRouter::add_channel()` accept `&self` (not `&mut self`) for runtime
addition behind `Arc`. The `spawn_cdc_listener` method subscribes to an `EventBus`
and processes events in a background tokio task until the bus is dropped.

### External SSE Endpoint

The `GET /api/v1/cdc/stream` endpoint exposes CDC events as **Server-Sent Events**
for external consumers (dashboards, replication pipelines, audit loggers). It uses
`FilteredSubscription` to push only matching events over HTTP with a 15-second
keepalive heartbeat. Query parameters `measurement` and `event_type` allow
client-side filtering without server overhead.
## Signal System (`chronix-streaming::signal`)

Programmable trigger engine with multi-channel event delivery.

```text
CDC events → TriggerEngine
                │
                ├── Condition evaluation (FieldThreshold, AnomalyScore,
                │   ForecastDeviation, MA Crossover + CrossoverDirection,
                │   RateOfChange)
                │
                ├── Cooldown deduplication (per-trigger)
                │
                └── SignalEvent → DeliveryRouter
                                    ├── WebhookChannel (HMAC-SHA256)
                                    ├── LogChannel
                                    ├── MetricChannel
                                    └── Dead Letter Queue (retry policy)

TriggerManager → TriggerEngine
  └── set_enabled(bool) — globally enable/disable trigger evaluation
      at runtime without removing trigger definitions
```

- **Lock-free delivery:** `DeliveryRouter::deliver()` snapshots `Arc<dyn DeliveryChannel>`
  refs and releases the `RwLock` before iterating — retry backoff sleeps never block
  `add_channel()` or other concurrent delivers. Exponential backoff is capped at 60s.

### Signal SQL

Triggers can be managed via SQL. Operators: `>`, `>=`, `<`, `<=`, `==`, `=`.
Negative number literals (e.g., `-5.0`) are supported.

```sql
CREATE TRIGGER alert_cpu ON cpu_usage
  WHEN anomaly_score > 3.0
  DELIVER webhook('https://...')
  COOLDOWN INTERVAL '5m';

CREATE TRIGGER cold_alert ON temp
  WHEN value < -10.0;

SHOW TRIGGERS;
DROP TRIGGER alert_cpu;
ALTER TRIGGER alert_cpu DISABLE;
```

`TriggerCatalog` persists trigger definitions as JSON for restart survival.
