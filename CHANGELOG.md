# Changelog

Notable changes to Chronix. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/). Chronix is pre-1.0:
per [CONTRIBUTING.md](CONTRIBUTING.md), a breaking change bumps the minor
version and a fix bumps the patch — there is no stability promise before 1.0,
and no migration tooling for the on-disk format.

## [Unreleased]

### Added

- **`time_bucket()` takes an `origin`**, so a bucket boundary need not be
  midnight on the 1st: `time_bucket('1mo', _time, '', '2024-01-15')` is a
  billing month that runs from the 15th, and
  `time_bucket('1d', _time, 'Europe/Berlin', '2024-01-01 06:00:00')` is a
  shift day that starts at six. Only the origin's phase matters. A monthly
  origin after day 28 is refused rather than clamped — that day is missing
  from some months, so it is not a monthly boundary.
- **Arrow Flight `DoPut` can backfill.** Put `{"backfill": true}` in
  `FlightData.app_metadata`; it was the one write surface that could not
  import history. An unrecognised key is refused rather than ignored.
- `TimeBucket::fixed(Duration)`, because a `Duration` *is* a fixed span —
  which is also why it cannot produce a calendar bucket, and says so.
- **Rollups take an `origin` too**, on the HTTP API and on `RollupBuilder`,
  and the rollup listing now reports it — so what the listing returns can be
  typed straight back into a create request, which is what its documentation
  already promised.

### Changed

- **Breaking:** `QueryBuilder::downsample()` takes a `TimeBucket` instead of
  a `Duration`, and `QueryPlan::Downsample` carries one. The native plan can
  now express a calendar day or month, and — more to the point — it now means
  the same thing as `time_bucket()` in SQL and as a rollup tier. A `1d` there
  used to be 86 400 seconds of UTC while `1d` in SQL was the zone's day, so
  the two surfaces disagreed on every daylight-saving transition.
- **Breaking:** `TimeBucket` and `BucketWidth` moved from `chronix::timebucket`
  to `chronix_core::timebucket`, and are in `chronix::prelude`. They are part
  of the data model, and every crate that buckets time now shares one
  definition.
- **Breaking:** the native query API's `EXPLAIN` reports a `Downsample` node's
  bucket as its width, timezone and whether it is a calendar bucket, replacing
  `interval_ms` — which had to invent a length for a month.

- **Breaking (gRPC):** `SqlValue` and `FieldValue` gain a `json` variant,
  carrying the JSON encoding of a value with no scalar protobuf field — a
  list, struct, map, interval or binary cell. These previously arrived as the
  literal text `"<unsupported: List(Float64)>"` in the `string` field, which
  no client could tell from a string.

### Fixed

- **Every `histogram!` now reaches a Prometheus scrape as a histogram.** The
  exporter was left unconfigured, and `metrics-exporter-prometheus` renders
  histograms as *summaries* unless buckets are set — so no `_bucket` series
  existed, `histogram_quantile()` had nothing to read, and 24 of 45 panel
  targets in the bundled dashboards were permanently empty. A summary's
  quantiles also cannot be aggregated across replicas, and the estimator was
  independently wrong for non-latency distributions
  (`chronix_batch_size{quantile="0.5"}` read `0.9998` for observed values of
  1 and 4320).
- **`dashboards/query-performance.json` queried metrics only a
  `--features cluster` build emits**, so all four panels were empty on every
  standard `chronixd`. Rewritten against the metrics a default build exports:
  query and write latency percentiles, throughput, batch size, SQL plan-cache
  and PromQL scan-cache hit ratios, and segment pruning.
- Write-path counters are published at **zero** from startup, so an
  ingestion-error panel reads `0` rather than "No data" and an alert on it can
  fire from the first scrape.
- A soft delete's grace period no longer depends on the raw wall clock. The
  GC pass compared the deadline against `SystemTime::now()`, so one bad
  reading — a gateway with no battery-backed RTC, an NTP server handing out a
  date in the next century — closed the recovery window instantly and
  hard-deleted the data it existed to protect. It now measures from the same
  reference retention uses: the clock capped by the newest timestamp held.
- `RollupBuilder::bucket()` dropped the bucket's origin. It decomposed the
  `TimeBucket` into width and timezone strings and re-parsed them at
  `build()`, so a tier declared through any protocol handler bucketed from
  midnight on the 1st however the caller had asked.
- Every Arrow type a query can produce now reaches the client as a value.
  Each wire surface carried its own conversion table over a different subset
  of Arrow's types, so an unenumerated type came back as the string
  `"<unsupported: Date32>"` inside a column declared `Date32`, under
  `200 OK` — `SELECT CAST(_time AS DATE) … GROUP BY 1`, an ordinary
  group-by-day, was such a query — or was dropped from the row entirely. One
  encoding now serves every surface, and its match is exhaustive over
  `DataType`, so coverage is a build error rather than a promise. Arrow Flight
  SQL was unaffected throughout: it streams the batch unmodified.
- A decimal column with a **negative scale** (legal in Arrow, produced by a
  cast) reached the client as `null` rather than its digits.
- A SQL mistake now answers `400` with the reason. `SELECT 1/0`,
  `CAST('abc' AS INT)`, `to_timestamp('not-a-date')`,
  `date_trunc('fortnight', …)` and an unknown time zone answered
  `500 … an internal error occurred`, with the message that named the problem
  redacted on the way out. A chronix error raised under a SQL query — a full
  memtable, a query deadline — again keeps its own status instead of being
  flattened to `500`.
- Converting a query result back into points dropped any field column whose
  Arrow type it did not enumerate, `Decimal128` among them. That path serves
  distributed reads **and Raft region snapshots**, so replicating a region
  silently discarded every exact-decimal field it held. It is now one shared
  conversion, and a type it cannot represent fails the snapshot rather than
  disappearing from it.

## [0.4.0]

### Changed

- **Breaking:** webhook delivery now sends a [CloudEvents](https://cloudevents.io)
  1.0 envelope, signed per the [Standard Webhooks](https://www.standardwebhooks.com)
  `v1` scheme (`webhook-id` / `webhook-timestamp` / `webhook-signature`),
  replacing the `X-Chronix-Signature: sha256=<hex>` header. Signing the
  timestamp alongside the body lets a receiver reject a replayed request,
  which the old body-only signature could not express.

### Fixed

- A measurement dropped with `soft_delete_ttl` configured is now actually
  invisible — to SQL, PromQL, the native query API, gRPC and Flight SQL — for
  its whole grace period, and the pending state survives a restart. It was
  previously tracked in memory only and consulted by nothing on the read
  path, so the data stayed fully readable until the background pass
  eventually deleted it.
- HTTP, gRPC and Flight SQL measurement listing, and PromQL discovery, now
  resolve "what measurements exist" through the same accessors the query path
  uses, so a pending-drop measurement can no longer appear in one listing and
  not another.

## [0.3.0] and earlier

Predate this file. See the `v0.1.0` / `v0.2.0` / `v0.3.0` git tags.
