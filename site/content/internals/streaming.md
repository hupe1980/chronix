+++
title = "Streaming & Change Data Capture"
description = "Batch queries answer 'what happened?' Streaming answers 'what is happening right now?' For infrastructure monitoring, near-real-time processing is essential — an alert that arrives 5 minutes late…."
weight = 330
+++

## Motivation

Batch queries answer "what happened?" Streaming answers "what is happening
right now?" For infrastructure monitoring, near-real-time processing is
essential — an alert that arrives 5 minutes late is 5 minutes of downtime.

## Architecture

Chronix implements a **streaming layer** that publishes data changes as
they are ingested, enabling downstream consumers to react immediately:

```text
Ingestion
    │
    ├──▸ WAL (durability)
    │
    ├──▸ Memtable (query path)
    │
    └──▸ Stream Publisher (CDC)
              │
              ├──▸ Signal Trigger Engine
              │
              ├──▸ Continuous Forecast
              │
              └──▸ External Subscribers (SSE: GET /api/v1/cdc/stream)
```

### External Subscribers via SSE

External consumers connect to the `GET /api/v1/cdc/stream` endpoint via
Server-Sent Events (SSE). The server creates a `FilteredSubscription` on
the EventBus and streams matching CDC events as JSON frames with a
15-second keepalive heartbeat. Clients can filter by `measurement` and
`event_type` query parameters.

Typical use-cases: Grafana live dashboards, cross-region replication
triggers, external audit log sinks, and custom analytics pipelines.

### CDC as Arrow batches

`chronix-streaming` can encode CDC events as Arrow `RecordBatch`es, behind
the `arrow` feature flag:

```toml
[dependencies]
chronix-streaming = { version = "0.5", features = ["arrow"] }
```

This is an **encoding, not a transport**: it produces record batches, and
what carries them — Arrow Flight, IPC, a Parquet writer — is yours to
choose. Over the network `chronixd` streams CDC as JSON over SSE at
`/api/v1/cdc/stream`.

**Components:**

- `CdcBatchConverter` — converts a `CdcEvent` sequence into a
  `RecordBatch`.
- `CdcBatchExporter` — subscribes to an `EventBus` and yields batches,
  counting the events a slow consumer missed (`gap_count`).

The schema is fixed:

| Column | Type | Description |
|---|---|---|
| `seq` | `UInt64` | Monotonic sequence number |
| `event_type` | `Utf8` | `point_written`, `series_deleted`, or `measurement_dropped` |
| `measurement` | `Utf8` | Measurement name |
| `event_timestamp` | `Int64` | Nanosecond timestamp (0 for non-write events) |
| `tags_json` | `Utf8` | JSON-encoded tag map |
| `fields_json` | `Utf8` | JSON-encoded field map (empty for non-write events) |
| `series_hash` | `UInt64` | FNV-1a series hash (0 for non-delete events) |

Downstream consumers (Spark, Flink, DataFusion, Pandas) read it as native
Arrow, without JSON serialization in the middle.

## Change Data Capture (CDC)

CDC captures every mutation as a structured event:

```text
ChangeEvent {
    operation: Insert | Delete | Update,
    timestamp: u64,
    metric: String,
    tags: Map<String, String>,
    value: f64,
    sequence_number: u64,
}
```

### Ordering Guarantees

- **Per-series ordering**: Events for the same metric + tag combination
  are delivered in write order (sequence number)
- **Cross-series ordering**: No global ordering guarantee (would require
  global coordination, destroying throughput)

### Delivery Semantics

| Mode | Guarantee | Use Case |
|------|-----------|----------|
| At-most-once | Fast, may lose events on crash | Metrics dashboard |
| At-least-once | May duplicate, never loses | Alert triggers |
| Exactly-once | Requires consumer dedup | Derived aggregations |

Chronix defaults to **at-least-once** with idempotent consumers
(deduplication by sequence number).

## Backpressure

Each delivery channel owns a **bounded queue and a worker thread**. A queue
that fills **drops the oldest waiting event**, counts it in
`chronix_signal_delivery_dropped_total` and keeps accepting — a slow webhook
must not block ingestion, and the newest signal is the one worth delivering.
Depth is `chronix_signal_delivery_queue_depth`; capacity is per channel.

## Integration with Signal Triggers

The streaming layer feeds directly into the [Signal Trigger Engine](@/internals/signal-triggers.md),
which evaluates continuous SQL-like rules against the live event stream.
This enables sub-second alert latency without polling.
