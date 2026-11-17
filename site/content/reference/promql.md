+++
title = "PromQL"
description = "The PromQL Chronix answers: Prometheus HTTP API endpoints, supported functions and modifiers, conformance pin and empty-result explanations."
weight = 20
+++

Chronix answers PromQL through the Prometheus HTTP API, so Grafana and other Prometheus clients connect without a plugin. The language follows Prometheus 3.16; this page lists the endpoints, the supported functions and modifiers, and how measurements and fields become metric names.

## PromQL Endpoints

Prometheus-compatible query API, served at the paths a Prometheus client
derives from a base URL. Point a Grafana **Prometheus** data source at
`http://chronix.example.com:8086` and nothing else needs configuring.

| Method | Path | Description |
|--------|------|-------------|
| `GET/POST` | `/api/v1/query` | Instant query |
| `GET/POST` | `/api/v1/query_range` | Range query |
| `GET/POST` | `/api/v1/labels` | List all label names |
| `GET/POST` | `/api/v1/label/{name}/values` | Values for a given label |
| `GET/POST` | `/api/v1/series` | Find series matching label matchers |
| `GET` | `/api/v1/metadata` | Metric metadata (types, help text) |
| `GET` | `/api/v1/status/buildinfo` | Version, probed when a data source is saved |
| `GET` | `/api/v1/status/tsdb` | Series per metric, label and label pair, and the budget left — see [Cardinality](#cardinality) |
| `GET` | `/api/v1/rules`, `/api/v1/alerts`, `/api/v1/query_exemplars` | Empty but well-formed — chronix has no rules or exemplars |

`/api/v1/prom/*` is an alias for every query and discovery path, for
deployments that name it explicitly. Chronix's own JSON query API lives
under `/api/v1/chronix/*`, because `/api/v1/query` is where every Prometheus
client looks.

`POST` bodies are `application/x-www-form-urlencoded`, which is what Grafana
sends by default. `time`, `start` and `end` accept **RFC 3339 or a Unix
timestamp**; `step` accepts a **duration** (`15s`, `1m30s`) or a bare number
of seconds. An unparseable value is a 400 rather than a silent default.

Errors carry Prometheus's status codes, because clients branch on them:
`400` for `bad_data`, `422` for `execution`, `503` for `timeout`. The body is
`{"status":"error","errorType":…,"error":…}` — not the envelope the rest of the
API uses, for the same reason. The discovery endpoints answer in the same
envelope, which is what Grafana branches on.

A parse error starts with its `line:column`, as Prometheus's do, and a
misspelt function names the one meant:

```text
1:11: parse error: expected ")", found "]"
1:1: parse error: unknown function with name "rat" — did you mean "rate"?
```

### Empty results

An empty result is not an error, so explanations go in the response's
`warnings` array, which Grafana shows on the panel. They are computed only when
the result is empty; the rest of the body is unchanged.

- **A probable typo** is always reported:
  `metric "http_requets_total" matches no series — did you mean "http_requests_total"?`,
  and likewise for a label key no series of the metric carries.
- **`explain_empty=true`** on `/query` or `/query_range` names the matcher that
  removed the last series, with the values that label does have:

  ```text
  http_requests_total{job="api", code=~"5.."}: 2 series of "http_requests_total", 2 after job="api", 0 after code=~"5.." — code has: client_error, ok
  ```

  Counts start from the metric's series in the query's window and follow the
  matchers in the order written. When more label sets exist than
  `prom_series_limit`, counts are lower bounds and print as `≥N`.

### Explain a query

`POST /api/v1/chronix/promql/explain` returns the query as a tree of
sub-expressions. With `"analyze": true`, each node is evaluated on its own and
reports the series and samples it produced — read down the tree to find the step
that lost the data.

```bash
curl -s localhost:8086/api/v1/chronix/promql/explain \
  -d '{"query": "sum(rate(cpu_usage[5m]))", "analyze": true}'
```

```json
{"tree": {"node": "sum", "text": "sum (rate(cpu_usage[5m]))", "series": 1, "samples": 1,
  "children": [{"node": "rate", "text": "rate(cpu_usage[5m])", "series": 2, "samples": 2,
    "children": [{"node": "range selector", "text": "cpu_usage[5m]", "series": 2, "samples": 38}]}]}}
```

The body takes `query`, then `time` for an instant query or `start`, `end` and
`step` for a range query, in the Prometheus forms. A range-vector node is read at
the end of the range. Same scope, limits and authorization (`Read`) as `/query`.

### `limit`, and truncation

`/query`, `/query_range`, `/series`, `/labels` and `/label/{name}/values`
accept **`limit`**: a non-negative integer bounding the number of results,
where `0` (or absent) means no limit. On the query endpoints it bounds the
number of **series**, as upstream does, and leaves a scalar or a string alone.

Two ceilings can cut an answer — the request's `limit` and the server's
`prom_series_limit` — and either way the response says so, in the field
Prometheus uses:

```json
{"status": "success",
 "data": [{"__name__": "cpu_usage", "host": "a"}],
 "warnings": ["results truncated due to limit"]}
```

`warnings` is **absent** when nothing was cut, so its presence carries
information. A `limit` above `prom_series_limit` does not raise it: an
operator's ceiling is not a client's to lift.

A **parse** error is `bad_data`, and that includes the two things upstream
also catches in its parser: an unknown function name, and a selector whose
every matcher would be satisfied by an absent label (`{}`, `{host=~".*"}`).
`{host="a"}` is legal and reaches every metric carrying that label.

`/query` and `/query_range` answer `{"status":"success","data":{"resultType":…,"result":…}}`.
`/labels`, `/label/{name}/values` and `/series` answer a **bare array** under
`data`, as Prometheus does, with label names and values **sorted**. All three
are **exact**: a label is offered for a metric only if the metric has a sample
under it in the window. The label endpoints answer that from which rows hold a
value rather than from the values themselves, so they stay cheap enough for a
query editor's every keystroke — and work on an encrypted field the server
cannot read.

All three accept `start` and `end` (defaulting to the last hour) and
repeated **`match[]`** series selectors. Every matcher in a selector is
applied, not only `__name__`:

```
GET /api/v1/label/dc/values?match[]={__name__="cpu_usage",host="a"}
→ {"status":"success","data":["eu"]}
```

Repeated `match[]` parameters are a **union**, as in Prometheus. A selector
that constrains no metric name is skipped rather than treated as
"everything", since it cannot be answered without scanning every measurement —
but a `__name__` **regex** does constrain it, so `match[]={__name__=~".+"}`
is answered here exactly as it is by `/query`.

`/label/__name__/values` lists the **metric names** holding data in the
window, and `/metadata` describes each one — the two calls Grafana makes to
populate a metric browser. Every name they offer is a selector that answers.

### Cardinality

`GET /api/v1/status/tsdb` answers in Prometheus's shape, field for field —
what tools built for Prometheus's TSDB status read — and adds a `chronix`
object: the series budget, how much of it is used, and the headroom left.
`limit` caps each list (1–10000, default 10).

```json
{
  "status": "success",
  "data": {
    "headStats": { "numSeries": 52, "chunkCount": 18, "minTime": 1763251200000, "maxTime": 1763337599000 },
    "seriesCountByMetricName":     [ { "name": "grid_power", "value": 12 } ],
    "labelValueCountByLabelName":  [ { "name": "device", "value": 12 }, { "name": "__name__", "value": 9 } ],
    "memoryInBytesByLabelName":    [ { "name": "device", "value": 96 } ],
    "seriesCountByLabelValuePair": [ { "name": "site=home", "value": 52 } ],
    "chronix": { "budget": 1000000, "used": 52, "headroom": 999948, "scope": "database", "enforced": true }
  }
}
```

- `numSeries` and `chronix.used` are the count the write path admits new
  series against, so `headroom` is exactly how many more it will accept.
- A series is a measurement and its tags; every field of a measurement
  shares it. `seriesCountByMetricName` therefore lists measurements — for
  remote-write and OTLP data, the metric names.
- `chunkCount` is the number of segments; times are milliseconds, `0` when
  there is no data.
- With multi-tenancy, a caller sees only its namespace: `scope` is
  `namespace:<name>`, and `budget` is the namespace's `max_series`, or
  `null` when it has none.

The embedded API returns the same numbers from `db.cardinality(limit, namespace)`.

### Metric names

Chronix stores a *measurement* with many *fields*; PromQL addresses a
*metric*, which carries one value per sample. A metric is one
`(measurement, field)` pair:

| Written as | PromQL metric |
|------------|---------------|
| `cpu,host=a usage=42,load=0.7` | `cpu_usage`, `cpu_load` |
| `temperature,room=hall value=21` | `temperature` |
| Prometheus remote write / OTLP `up{job="api"}` | `up` |

The rule is `<measurement>_<field>`, except that a field named `value` gives
the measurement name alone — which is how the Prometheus remote-write and
OTLP paths store a sample, so a scraped metric round-trips under its own
name.

Two consequences worth knowing:

- **A bare measurement name is not a metric.** `cpu` selects nothing when the
  measurement's fields are `usage` and `load`; `cpu_usage` selects the series.
  Ask `/api/v1/label/__name__/values` — or Grafana's metric browser — for the
  names that exist.
- **Adding a field never renames an existing metric.** A name depends only on
  its own `(measurement, field)` pair, so writing `cpu,host=a temp=61` for the
  first time leaves `cpu_usage` and `cpu_load` exactly where they were.

A vector may not hold two series with the same label set, and a query that
would produce one is an error — `vector cannot contain metrics with the same
labelset`, as in Prometheus. `rate({__name__=~"cpu.+"}[5m])` is the usual way
to hit it: `rate` drops `__name__`, and `cpu_usage` and `cpu_load` then have
identical labels. Aggregate or select one metric instead.

### Semantics

The evaluator targets **Prometheus 3.x**. Two of its differences from 2.x are
visible in returned data:

- **Range selectors and the lookback window are left-open, right-closed**:
  `foo[5m]` at time `t` selects samples in `(t − 5m, t]`. A sample landing
  exactly on the older boundary belongs to the previous window, so evenly
  spaced samples yield a constant count per range.
- **`.` in a matcher regex matches newline.** Matchers remain fully anchored,
  so `{job=~"api"}` matches only the exact value.

A range query returns exactly one point per step per series: every function
over a range vector stamps the step's evaluation timestamp, so the matrix is
step-aligned and strictly increasing. `NaN` ranks last in `topk`, `bottomk`,
`sort` and `sort_desc`.

**Modifiers.** `offset 5m` shifts back, `offset -5m` shifts forward, and
`@ <unix seconds>` / `@ start()` / `@ end()` pin an evaluation instant — `@`
replaces the evaluation time and the offset is then subtracted. Both attach to
a **selector or a subquery** and to nothing else, so `sum(m) offset 5m` is a
parse error naming the rule.

**Duration expressions.** A range, a subquery's range and step, and an offset
accept arithmetic: `rate(m[5m * 2])`, `m[1h30m % 1h]`, `x[range():max_of(step(), 15s)]`.
The operators are `+ - * / % ^` (`^` right-associative), a bare number is
seconds, and `step()`, `range()`, `min_of(a, b)` and `max_of(a, b)` are valid
only inside a duration. `step()` and `range()` are the range query's step and
`end − start`, and `0s` in an instant query. An offset expression needs
parentheses — `m offset 1h / 2` is `(m offset 1h) / 2` — and `@` takes no
expression.

**Extended range selectors.** `rate(m[5m] anchored)` takes the newest sample at
or before the range start as the left boundary and does not extrapolate;
`increase` of an integer counter is an integer. `rate(m[5m] smoothed)`
interpolates linearly at both boundaries, so it needs a sample after the
evaluation time. `anchored` works with `rate`, `increase`, `delta`, `resets`
and `changes`; `smoothed` with `rate`, `increase` and `delta`, and on an instant
selector (`m smoothed`) interpolates at the evaluation time. Neither applies to
a subquery or to native histograms.

**Subqueries.** `expr[5m:1m]` evaluates `expr` at each point of an absolute
step grid; `expr[5m:]` uses the engine's 1-minute interval rather than the
outer step. Every sample carries its **step** timestamp, the window is
left-open on the grid, and a subquery is a range vector — so `rate(x[5m:15s])`
extrapolates over five minutes exactly as `rate(x[5m])` does.

**A comparison keeps the vector element's value**, whichever side the scalar is
written on: `2 < some_metric` is the metric's value, not `2`. A duplicate on
the "one" side of a `group_left` is an error rather than a cross product.
`last_over_time` and `first_over_time` keep `__name__` while every other `_over_time` and
`timestamp()` drop it. `deriv` and `idelta` drop a series with fewer than two
points rather than emitting `NaN`.

The 3.x-only functions `double_exponential_smoothing`, `mad_over_time`,
`sort_by_label` and `sort_by_label_desc` are available **without** a feature
flag (upstream gates them behind `--enable-feature=promql-experimental-functions`).
Two notes on their behaviour:

- `double_exponential_smoothing(v, sf, tf)` requires `0 < sf < 1` and
  `0 < tf < 1` — a value on the boundary is an error, not a clamp — and returns
  nothing for a range holding fewer than two samples.
- `sort_by_label` orders **naturally**, so `pod-2` precedes `pod-10`, with the
  full label set as the tie-break.

The eight date functions — `year`, `month`, `day_of_month`, `day_of_week`,
`day_of_year`, `days_in_month`, `hour`, `minute` — read the sample **value** as
a Unix timestamp in seconds and answer in **UTC**, so `hour() < 9` means the
same thing wherever the server runs. Called with no argument they use
`vector(time())`, and all of them drop `__name__`.

The native-histogram family — `histogram_count`, `histogram_sum`,
`histogram_avg`, `histogram_stddev`, `histogram_stdvar` and
`histogram_fraction` — reads a stored distribution, and `histogram_quantile`
dispatches: a series carrying native histograms answers from its own buckets,
a classic `le`-bucketed one takes the original path. The rest refuse a float
series **by name** — `histogram_count` of a gauge is not the gauge.

A stored native histogram also answers the **classic** names — `foo_bucket`
with cumulative counts ending at `le="+Inf"`, `foo_count` and `foo_sum` — so a
dashboard written before native histograms existed needs no change. Those names
are resolvable but not listed by `/api/v1/label/__name__/values`: listing them
would make `{__name__=~".+"}` return the same observations four times.

The `le` values are the boundaries the histogram **has**. A custom-bucket
histogram — what an OTLP explicit-bucket or classic histogram becomes — carries
the boundaries the instrumentation chose, so an existing `le="0.5"` matches. An
exponential one carries powers of `2^(2^-schema)`, so `le="0.5"` matches
nothing: `histogram_quantile(0.99, foo_bucket)` and a heatmap over the whole
series both work, but a hand-pinned boundary returns an empty result rather
than the nearest bucket.

The surface conforms to **Prometheus 3.16**: every stable function, operator,
modifier and selector form is implemented, checked against upstream's own test
cases. Not implemented: what upstream still gates behind a feature flag — the
variadic `histogram_quantiles`, `limitk`, `limit_ratio`, `info`,
`ts_of_*_over_time`, `integral`, `start_timestamp`, the `fill()` modifiers, and
`step()`, `range()`, `min_of` and `max_of` as standalone expressions. A query
using one returns an error naming it rather than a wrong answer.

### Instant Query

```bash
curl 'http://localhost:8086/api/v1/query?query=cpu_usage{host="srv1"}&time=1700000000'
```

### Range Query

```bash
curl 'http://localhost:8086/api/v1/query_range?query=cpu_usage&start=1700000000&end=1700100000&step=60'
```

### Label Values

```bash
# List all metric names
curl http://localhost:8086/api/v1/label/__name__/values

# List all values for the "host" tag
curl http://localhost:8086/api/v1/label/host/values
```

**Response Format:**

All PromQL endpoints return the standard Prometheus response envelope:

```json
{
  "status": "success",
  "data": {
    "resultType": "vector|matrix|scalar|string|labels|series|metadata",
    "result": [...]
  }
}
```

---

## PromQL Engine

Chronix includes a built-in PromQL parser and evaluator for Prometheus
compatibility.

### Pipeline

```text
PromQL string → Lexer → Tokens → Parser → AST (Expr) → Evaluator → PromQLValue
```

### Supported Features

- **Selectors:** `metric{label="value"}`, `metric{label!="value"}`,
  `metric{label=~"regex"}`, `metric{label!~"regex"}`
- **Label matchers:** all four match operators fully implemented:
  - `=` — exact string equality
  - `!=` — string not-equal
  - `=~` — regex match (compiled via `regex` crate)
  - `!~` — regex not-match
  
  Matchers are compiled once into `CompiledMatcher` structs at evaluation
  time and applied as post-filters after label set construction.
- **Range vectors:** `metric[5m]`
- **Functions:** `rate()`, `irate()`, `increase()`, `delta()`, `deriv()`,
  `predict_linear()`, `abs()`, `ceil()`, `floor()`, `round()`, `clamp()`,
  `label_replace()`, `label_join()`, `histogram_quantile()`
- **Aggregations:** `sum`, `avg`, `min`, `max`, `count`, `topk`, `bottomk`
  with `by` / `without` grouping
- **Binary operators:** `+`, `-`, `*`, `/`, `%`, `^`, comparisons with `bool`
  modifier
- **Vector matching:** `on()`, `ignoring()`, `group_left()`, `group_right()`
  with include label lists
- **Subqueries:** `metric[5m:1m]`, and `metric[5m:]` at the engine's 1-minute
  default step
- **Modifiers:** `offset 5m`, `offset -5m` (forward), `@ 1609746000`,
  `@ start()`, `@ end()`

### Modifiers

`offset` and `@` attach to a **selector or a subquery** and to nothing else, so
`sum(m) offset 5m` is a parse error — as it is in Prometheus.

`@` replaces the evaluation instant and the offset is then subtracted, in that
order. `@ start()` and `@ end()` take a range query's own bounds; for an instant
query both are the evaluation time. A pinned selector reads one fixed window
whatever the step, so it is fetched directly rather than through the range
query's prefetch span.

### Subqueries

`expr[range:step]` evaluates `expr` as an instant query at each point of an
**absolute** step grid — aligned to multiples of the step rather than to the
evaluation time, so a subquery inside a range query samples the same points at
every outer step instead of jittering underneath it.

- Every sample carries its **step** timestamp, so the result is step-aligned
  and strictly increasing.
- The window is **left-open** on the grid, as range selectors are.
- A step-less `[5m:]` uses the engine's 1-minute interval, **not** the outer
  step, so a panel means the same thing at every zoom level.
- A subquery is a range vector, so `rate(x[5m:15s])` extrapolates over five
  minutes exactly as `rate(x[5m])` does.

Inner selectors prefetch over the **outer** query's window widened by the
subquery's reach, which is constant across steps — so a range query containing
a subquery reads storage once.

### Counter functions and extrapolation

`rate()`, `increase()` and `delta()` follow Prometheus's `extrapolatedRate`
exactly, because approximating it produces plausible-looking numbers that are
simply wrong:

1. Walk consecutive samples and sum the deltas. A negative delta is a counter
   reset, so the *new* value is added instead of the difference
   (`rate`/`increase` only — `delta` is for gauges and does not correct).
2. The observed increase covers `sampled_interval = last_ts - first_ts`, which
   is almost never the whole range: the first sample lands after the range
   start and the last before the evaluation time. Scale it up by
   `extrapolate_to / sampled_interval`, where `extrapolate_to` adds the gap at
   each end — each capped at half a sample interval when the gap exceeds
   ~1.1 sample intervals, since a larger gap suggests the series did not exist
   there.
3. Counter zero-clamping: a counter cannot have been negative, so the backward
   extrapolation never reaches further than the point at which the counter
   would have been zero.
4. `rate` then divides by the range. For an evenly scraped counter steps 2 and
   4 cancel, recovering the true per-second slope — which is the invariant to
   test against.

`histogram_quantile()` follows `bucketQuantile`: fewer than two buckets yields
`NaN`, a quantile landing in the top bucket reports the highest finite bound
(the top bound is `+Inf` and carries no information), a lowest bucket with a
non-positive bound is returned as-is, and everything else interpolates
linearly within the containing bucket. Bucket counts are made monotonic first
to absorb scrape artifacts.

### Conformance details that are easy to get subtly wrong

Each is pinned by a test naming the upstream rule it encodes:

- A comparison keeps the **vector element's** value whichever side the scalar
  is written on, so `2 < some_metric` is the metric's value, not `2`.
- A duplicate on the "one" side of `group_left` is an **error**, not a cross
  product.
- `last_over_time` keeps `__name__` (it returns an actual sample of the
  series); every other `_over_time` drops it, and so does `timestamp()`.
- `deriv` and `idelta` **drop** a series with fewer than two points rather than
  emitting `NaN`.
- `clamp(v, min, max)` with `max < min` is an empty vector.
- `label_replace` refuses a destination that is not a legal label name.
- `NaN` sorts **last** in `sort`, `sort_desc`, `topk` and `bottomk`.
- `quantile` outside `[0, 1]` yields ±Inf rather than erroring; `round(v, 0)`
  is `NaN`, which falls out of `Floor(v/0 + 0.5) * 0`.

All of the above are covered in `chronix/tests/promql_conformance.rs`, which
drives parse → select → evaluate against stored samples rather than calling the
compute helpers directly.

### API Endpoints (Prometheus-compatible)

| Endpoint | Description |
|----------|-------------|
| `GET/POST /api/v1/query` | Instant query |
| `GET/POST /api/v1/query_range` | Range query (capped at `max_range_query_points`, default 11,000) |
| `GET /api/v1/series` | Series metadata (defaults to `now - 1h → now` when no time range given) |
| `GET /api/v1/labels` | Label names |
| `GET /api/v1/label/{name}/values` | Label values |

### HTTP Safety Limits

The HTTP server (`chronixd`) enforces multiple safety limits to prevent abuse:

| Setting | Default | Description |
|---------|---------|-------------|
| `max_body_size` | 10 MB | Maximum HTTP request body size |
| `max_write_batch_size` | 50,000 | Maximum points per `POST /api/v1/write` batch |
| `max_range_query_points` | 11,000 | Maximum evaluation steps in `query_range` |
| `sql_max_rows` | 100,000 | Maximum rows returned from SQL query |
| `sql_query_timeout_secs` | 30 | SQL query execution timeout (covers planning, execution, and result streaming) |
| `prom_series_limit` | 10,000 | Maximum label-sets returned by `/api/v1/prom/series` |
| *(prom series default range)* | `now - 1h → now` | Default time range for `/api/v1/prom/series` when no `start`/`end` provided; prevents full-table scans |

The SQL endpoint additionally rejects all DDL/DML statements (only `SELECT` is
allowed). Error responses from the series endpoint now propagate query failures
instead of silently returning partial results.
