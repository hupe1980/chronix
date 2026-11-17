+++
title = "Streaming and triggers"
description = "The Chronix change-data-capture bus, subscriptions, the trigger DSL and webhook delivery: how events flow from a write to an alert."
weight = 150
aliases = ["/internals/streaming/", "/internals/signal-triggers/", "/reference/analytics-and-streaming/"]
+++

Every committed write publishes a change event on an in-process bus. Subscribers
read the bus directly, over Server-Sent Events, or as Arrow batches. Triggers
evaluate a small condition language against each event and deliver signals to
the log or to a signed webhook. Nothing here polls: an alert is decided on the
write that causes it.

```text
write ──▸ WAL ──▸ memtable
   │
   └──▸ EventBus (broadcast, 65 536 events)
            ├──▸ TriggerEngine ──▸ DeliveryRouter ──▸ log / webhook / metric
            ├──▸ subscriptions (in process, filtered)
            ├──▸ GET /api/v1/cdc/stream (SSE)
            └──▸ CdcBatchExporter (Arrow, `arrow` feature)
```

## Change events

| Event | Published by | Carries |
|---|---|---|
| `point_written` | `insert`, `insert_batch` | measurement, tags, fields, timestamp, sequence |
| `series_deleted` | `delete_series` | measurement, tags, series hash, sequence |
| `measurement_dropped` | `drop_measurement` | measurement, sequence |

**Ordering.** Events of one series arrive in write order. There is no order
across series.

**Delivery.** The bus is a bounded broadcast channel. A subscriber that falls
more than 65 536 events behind skips the oldest ones and the gap is counted
(`BusStats::lag`, `CdcBatchExporter::gap_count`); writers are never blocked by
a slow reader. A consumer that must not miss data reads the store, using the
sequence number to resume.

### Over HTTP

`GET /api/v1/cdc/stream` streams events as JSON Server-Sent Events with a
15-second keep-alive. Filter with `measurement=cpu,mem` and
`event_type=point_written`.

```sh
curl -N 'localhost:8086/api/v1/cdc/stream?measurement=cpu'
```

### As Arrow batches

With `chronix-streaming`'s `arrow` feature, `CdcBatchConverter` turns events
into a `RecordBatch` and `CdcBatchExporter` yields batches from a subscription.
It is an encoding, not a transport: Flight, IPC or Parquet are the caller's
choice.

| Column | Type | Notes |
|---|---|---|
| `seq` | `UInt64` | Sequence number |
| `event_type` | `Utf8` | One of the three event names |
| `measurement` | `Utf8` | |
| `event_timestamp` | `Int64` | Nanoseconds; 0 for non-write events |
| `tags_json` | `Utf8` | JSON tag map |
| `fields_json` | `Utf8` | JSON field map; empty for non-write events |
| `series_hash` | `UInt64` | 0 except for `series_deleted` |

Bucketed aggregates are not computed on the bus. They are
[rollups](@/explanation/storage.md), materialised by the storage layer with a
persisted watermark.

## Triggers

A trigger is a named condition on one measurement, with delivery targets and a
cooldown. Triggers are managed with SQL-like statements through
`Pipeline::execute_signal_sql` or the server's trigger routes, and persist
across restarts.

```sql
CREATE TRIGGER hot_cpu ON cpu
  WHEN usage > 90 AND region = 'eu-west'
  DELIVER webhook('https://alerts.example.com/hook'), log
  COOLDOWN INTERVAL '5m';

CREATE TRIGGER cold ON temp WHEN value < -10.0;
CREATE TRIGGER "high-cpu.alert" ON "system.cpu" WHEN anomaly_score > 3.0;

SHOW TRIGGERS;
ALTER TRIGGER hot_cpu DISABLE;
DROP TRIGGER hot_cpu;
```

### Conditions

| Condition | Written as | Fires when |
|---|---|---|
| Field threshold | `field op number` | the named field compares true; `op` is `>`, `>=`, `<`, `<=`, `=`, `==` |
| Tag match | `tag = 'value'`, `tag <> 'value'` | a quoted right-hand side is a tag comparison |
| Anomaly score | `anomaly_score op number` | the series' detector score compares true |
| Forecast deviation | `forecast_deviation op number` | the value deviates from its forecast by more than the fraction |
| Combination | `AND`, `OR`, parentheses | as written |

Moving-average crossover and rate-of-change conditions exist in the Rust API
(`TriggerCondition`) but not in the statement language. Names with dots,
hyphens or spaces are quoted with `"…"` or `` `…` ``.

### Cooldown

A trigger that fired is silent for its cooldown, **per series**, so one noisy
device does not silence the others. A firing is suppressed only while both
clocks agree it is inside the window: the data's timestamps and the wall clock.
Either alone fails — out-of-order data has a negative delta, a backfill a huge
positive one.

### Signals

A firing produces a `SignalEvent`: `event_id`, `trigger_name`, `measurement`,
`tags`, `timestamp`, `signal_type`, `severity`, `value` and `metadata`.
Severity is `info`, `warning` or `critical`, derived from the value against the
trigger's two severity thresholds. Metadata carries the condition's inputs —
for a forecast deviation, the actual and forecast values and whether the
deviation was relative or, near a zero forecast, absolute.

## Delivery

Each channel owns a bounded queue and a worker. A full queue **drops the oldest
waiting signal** and counts it in `chronix_signal_delivery_dropped_total`, so a
slow webhook never blocks ingestion; `chronix_signal_delivery_queue_depth` shows
the backlog.

| Channel | What it does |
|---|---|
| `log` | Writes the signal to the server log |
| `webhook('…')` | POSTs a CloudEvents JSON body, signed per Standard Webhooks |
| metric | Counts signals by trigger and severity |

Webhook deliveries are retried with exponential backoff (three retries by
default, capped at 60 s); one that still fails lands in the dead-letter queue.
The `webhook-id`, `webhook-timestamp` and `webhook-signature` headers carry an
HMAC-SHA256 signature for every active secret, so secrets rotate without a
missed delivery — see [rotating a signing secret](@/reference/security.md#rotating-a-signing-secret).
Webhook URLs are checked against private and link-local addresses
([SSRF protection](@/reference/security.md#webhook-url-ssrf-protection)).

## The pipeline

`chronix::pipeline::Pipeline` wires the bus to the trigger engine, the
streaming anomaly and forecast engines, and the audit logger, and runs them on
a background task (`spawn_cdc_listener`). `chronixd` uses it as is; an embedded
application opts in with the `pipeline` feature.

## SQL interface

The analytics functions callable from SQL — window functions such as `zscore`
and `rolling_mean`, aggregates such as `forecast` and `last` — are listed with
their signatures in the [SQL reference](@/reference/sql.md).

## Related

- [Anomaly detection](@/explanation/anomaly-detection.md)
- [Forecasting](@/explanation/forecasting.md)
- [HTTP API](@/reference/http-api.md)
