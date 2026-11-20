+++
title = "SQL"
description = "SQL in Chronix: tables and system columns, the forecasting and anomaly-detection functions, window functions and ASOF joins."
weight = 30
+++

Every measurement is a table. Tags and fields are columns, `_time` is the
timestamp in nanoseconds, and `_start` (when present) is the sample's start
time. SQL runs on DataFusion with Chronix's time-series and analytics
functions added; every network SQL surface accepts read-only queries only.

## Tables and time

```sql
SHOW TABLES;                 -- the measurements
SHOW COLUMNS FROM cpu;       -- tags, fields, _time
SELECT * FROM cpu WHERE _time >= now() - INTERVAL '1 hour' ORDER BY _time DESC LIMIT 10;
```

`_time` compares against an epoch-nanosecond integer, an RFC 3339 string and
`now()` alike, so the numbers the write API returns work unchanged.
`ORDER BY _time [ASC|DESC] LIMIT n` stops after `n` rows instead of reading
the measurement. `EXPLAIN` shows whether a time filter narrowed the scan:

```sql
EXPLAIN SELECT * FROM cpu WHERE _time >= 1700000000000000000;
-- ChronixExec: measurement=cpu, time=[1700000000000000000..9223372036854775807], …
```

### `time_bucket`

```sql
SELECT time_bucket('5m', _time) AS bucket, avg(usage_idle) AS avg_idle
FROM cpu WHERE host = 'web-01'
GROUP BY bucket ORDER BY bucket;
```

With a time zone, a day means a day:

```sql
SELECT time_bucket('1d', _time, 'Europe/Berlin') AS day,
       avg(usage_idle)                           AS avg_idle
FROM cpu
GROUP BY day
ORDER BY day;
```

The **unit decides**: `'30s'`, `'5m'` and `'1h'` are a fixed span that never
varies, while `'1d'`, `'1w'`, `'1mo'` and `'1y'` follow that zone's calendar —
so a transition day is 23 or 25 hours and February is February. A fixed span
in a zone sits on that zone's standard-time grid: `'6h'` in Europe/Berlin
starts at 00:00, 06:00, 12:00 and 18:00 CET all year. The month is `'mo'`;
`'m'` is always the minute. The zone is a constant string, matched
case-insensitively against the IANA database built into Chronix — never the
host's, so a query buckets identically on every machine.

A fourth argument moves the boundary, for a period that does not start at
midnight on the 1st:

```sql
-- A billing month that runs from the 15th to the 15th.
SELECT time_bucket('1mo', _time, '', '2024-01-15') AS period,
       max(reading) - min(reading)                 AS kwh
FROM meter GROUP BY period ORDER BY period;

-- A shift day that starts at 06:00 in Berlin.
SELECT time_bucket('1d', _time, 'Europe/Berlin', '2024-01-01 06:00:00') AS shift,
       avg(usage_idle)                                                  AS avg_idle
FROM cpu GROUP BY shift ORDER BY shift;
```

What matters is where the origin falls in the cycle, not how far away it is.
For a width of one unit — `1d`, `1mo` — that is just its time of day or day
of month, so any date with the right shape will do. For a multi-unit width it
also picks which unit opens a bucket: `3mo` anchored on a January gives
calendar quarters, anchored on a February gives February–April.

A monthly bucket must start on day 1–28: a boundary on the 29th, 30th or 31st
does not exist in every month, and is refused rather than clamped.

The long spellings work too, with or without the space: `'1 hour'`,
`'15 minutes'`, `'3 months'`. A bare `'M'` is refused rather than guessed at.

The time column is `_time` — the same name the schema endpoint, an Arrow
batch and a `.csx` segment use, so what you read back is what you type. It
compares against an epoch-nanosecond integer, an RFC 3339 string and `now()`
alike, so the numbers the write API gave you work unchanged:

```sql
SELECT * FROM cpu WHERE _time >= 1700000000000000000 LIMIT 10;
```

`SHOW TABLES` lists your measurements, `SHOW COLUMNS FROM cpu` their
columns, and `EXPLAIN` tells you whether a time filter actually narrowed the
scan — the difference between reading three segments and reading all of
them:

```sql
EXPLAIN SELECT * FROM cpu WHERE _time >= 1700000000000000000;
```

```text
ChronixExec: measurement=cpu, time=[1700000000000000000..9223372036854775807], filters=0, limit=None
```

## Forecasting functions


`forecast` is an **aggregate** function returning a `LIST(DOUBLE)` of
predicted values. A forecast is not a property of any existing row — it is
`horizon` new values that come *after* the last one — so it produces one list
per group, and `unnest` expands it:

```sql
-- 24 points ahead, per host
SELECT host, unnest(forecast(usage, _time, 24)) AS predicted
FROM cpu_usage
WHERE _time > now() - INTERVAL '7 days'
GROUP BY host;
```

The `_time` argument is not decoration: an aggregate sees rows in whatever order
the plan delivers them, so the accumulator sorts by the timestamp column it is
given. Passing a constant, or a column that is not time, forecasts a shuffled
series.

The SQL model is simple exponential smoothing — the right default for a metric
with no declared seasonality. Choosing a model needs orders, a period and a
seasonal flag, which belong in typed arguments rather than a JSON string, so it
is a Rust-API job (below).

`multivariate_forecast(target, predictor, _time, horizon)` has the same shape
and returns a regression forecast of `target`.

## Anomaly functions


`anomaly_score(value, threshold)` is a **window** function. It takes each
point's Z-score against the mean and standard deviation of its *partition* and
maps it into (0, 1): **0.5 at the threshold**, close to 1 well above it. A point
beyond the threshold is one with a score above 0.5.

```sql
SELECT * FROM (
  SELECT _time, host, usage,
         anomaly_score(usage, 3.0) OVER (PARTITION BY host ORDER BY _time) AS score
  FROM cpu_usage
  WHERE _time > now() - INTERVAL '1 hour'
) WHERE score > 0.5
ORDER BY score DESC;
```

`PARTITION BY host` keeps one host's spike out of another host's baseline, and
`ORDER BY _time` is what makes "previous point" mean anything. Both are
required: a window function without an `OVER` clause fails to plan.

The other detectors (MAD, IQR, CUSUM, forecast-residual, dynamic threshold)
are Rust-API types, because each carries its own configuration.

## Custom SQL Functions

Registered as DataFusion UDFs/UDAFs:

| Function | Type | Description |
|----------|------|-------------|
| `time_bucket(width, timestamp [, timezone [, origin]])` | UDF | Bucket timestamps. Sub-day widths are a fixed span; `d`/`w`/`mo`/`y` follow the calendar of `timezone`. `origin` moves the boundary — a day that starts at 06:00, a month that starts on the 15th |
| `first(field, timestamp)` | UDAF | First value ordered by timestamp |
| `last(field, timestamp)` | UDAF | Last value ordered by timestamp |
| `rate(field, timestamp)` | UDAF | Per-second rate of change |
| `irate(field, timestamp)` | UDAF | Instantaneous rate (last two points) |
| `forecast(field, timestamp, horizon)` | UDAF | `LIST(DOUBLE)` of predicted values |
| `multivariate_forecast(target, predictor, timestamp, horizon)` | UDAF | `LIST(DOUBLE)` from a regression |
| `diff`, `pct_change`, `rolling_mean`, `rolling_std`, `rolling_corr`, `zscore`, `ewm`, `anomaly_score`, `stl_trend`, `stl_seasonal`, `stl_residual`, `stl_decompose`, `correlation`, `cross_correlation`, `multivariate_anomaly` | UDWF | Window functions — require `OVER (PARTITION BY … ORDER BY …)` |

The analytics functions are **window functions**, not scalar ones: their value
for a row depends on the rows around it and on which rows belong to the same
series, and a scalar UDF is handed one `RecordBatch` with neither ordering nor
partitioning. See
[Analytics & Streaming](@/explanation/streaming-and-triggers.md#sql-interface).

### Runtime UDF Registration

Custom scalar and aggregate UDFs can be registered at runtime via the `Chronix`
API and are automatically available in all subsequent SQL queries:

```rust
db.register_udf(Arc::new(my_scalar_udf));
db.register_udaf(Arc::new(my_aggregate_udaf));
```

Runtime-registered UDFs are included when `create_session_context()` builds a
new DataFusion session, alongside the built-in functions above.

**Null handling:** every function respects Arrow's null bitmap. A NULL is a
**missing sample, not a missing row**: it is excluded from every statistic,
and a row still gets an answer wherever that answer can be computed from
samples that do exist. Two groups follow from that:

| Group | Functions | At a NULL row |
|---|---|---|
| Reads the window | `rolling_mean`, `rolling_std`, `rolling_corr`, `ewm`, `correlation`, `cross_correlation` | Answers, from the real samples around it — as `AVG(v) OVER (ROWS n PRECEDING)` does |
| Reads that row's own value | `diff`, `pct_change`, `zscore`, `anomaly_score`, `multivariate_anomaly`, `stl_trend`, `stl_seasonal`, `stl_residual`, `stl_decompose` | NULL |

Either way a gap nulls its own rows and no others. The rolling statistics
match pandas' `rolling(...).mean()/.std()/.corr()` and
`ewm(adjust=False, ignore_na=True).mean()` exactly on the same input.

Positional alignment survives throughout: row `i` of the output belongs to row
`i` of the input. The seasonal routines need that most — dropping a NULL row
would move every row after it into the previous season — so `stl_*`
interpolates interior gaps to keep the spacing even and then reports NULL on
the rows that had no sample. Constant arguments (`window`, `period`, `alpha`,
`horizon`) are validated with descriptive errors rather than panicking.

## Window Functions

All standard SQL window functions are supported via DataFusion's built-in engine:

- **Ranking:** `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`
- **Offset:** `LAG(expr, offset, default)`, `LEAD(expr, offset, default)`
- **Value:** `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE(expr, n)`
- **Frame:** `ROWS BETWEEN`, `RANGE BETWEEN`, `GROUPS BETWEEN` with
  `UNBOUNDED`, `N PRECEDING`, `N FOLLOWING`, `CURRENT ROW`

## ASOF JOIN

Cross-series temporal alignment via a custom DataFusion `ExecutionPlan`.

> **A Rust API, not SQL syntax.** DataFusion's parser has no `ASOF JOIN` and
> chronix adds none. The entry point is `chronix::sql::execute_asof_join`,
> taking the two physical plans `DataFrame::create_physical_plan` gives you.

```rust
let left = ctx.table("cpu").await?.create_physical_plan().await?;
let right = ctx.table("mem").await?.create_physical_plan().await?;
let joined = chronix::sql::execute_asof_join(
    left, right, "_time", vec!["host".into()], 5_000_000_000, ctx.task_ctx(),
).await?;
```

- `AsofJoinExec` implements `ExecutionPlan` with sort-merge nearest-timestamp
  matching.
- For each left row, binary search + backwards scan finds the closest right
  row (≤ tolerance) matching tag columns — **O(L log R)** complexity.
- Tag column indices and array references are resolved once per batch, not per
  row —  avoiding repeated `schema.index_of()` and downcast overhead.
- Tie-breaking: earlier timestamp wins.
- Materializes the right side; streams the left side.
- Rejected at plan creation: a negative tolerance, and a join key that is
  absent from either side or is not a string.
- Both inputs require a single partition (`required_input_distribution`), so
  DataFusion inserts the coalesce.
