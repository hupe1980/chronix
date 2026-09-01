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

### Arrow Flight CDC Export

For high-throughput, zero-copy CDC delivery, `chronix-streaming` provides an
Arrow Flight transport behind the `flight` feature flag:

```toml
[dependencies]
chronix-streaming = { version = "0.1", features = ["flight"] }
```

**Components:**

- `CdcBatchConverter` — converts `CdcEvent` sequences into Arrow
  `RecordBatch` with a fixed schema (`op: Utf8`, `timestamp: Int64`,
  `metric: Utf8`, `tags: Utf8`, `value: Float64`, `seq: UInt64`).
- `CdcFlightExporter` — wraps `CdcBatchConverter` output in Arrow
  Flight `FlightData` frames suitable for gRPC `DoExchange` streaming.

This enables downstream consumers (Spark, Flink, DataFusion, Pandas) to
consume CDC streams as native Arrow data without JSON serialization
overhead.

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

### Window Finalization Optimization

`finalize_all_series()` avoids cloning all bucket keys by pre-filtering
to only series that have at least one expired window (i.e. a bucket whose
start + interval <= watermark). This reduces allocation overhead under
high-cardinality workloads where most series' windows are still open.

## Windowing

Streaming computations operate over **windows** — finite slices of an
unbounded stream:

### Tumbling Windows

Non-overlapping, fixed-size windows:

```text
time ──▸
├──── W1 ────┤├──── W2 ────┤├──── W3 ────┤
```

### Sliding Windows

Overlapping windows with a slide interval:

```text
time ──▸
├──── W1 ────┤
    ├──── W2 ────┤
        ├──── W3 ────┤
```

### Session Windows

Dynamic windows that close after a gap of inactivity:

```text
time ──▸
├── W1 ──┤    gap    ├── W2 ──────┤  gap  ├ W3 ┤
```

## Watermarks

In distributed systems, events can arrive **out of order**. A
**watermark** $W(t)$ declares that no events with timestamp $< W(t)$
will arrive in the future:

$$
W(t) = \max(\text{observed timestamps}) - \text{allowed\_lateness}
$$

When the watermark advances past a window's end, that window is
**closed** and its result is emitted.

### Late Events

Events arriving after their window has closed are handled by:
1. **Drop** — discard (default for dashboards)
2. **Update** — re-emit corrected result
3. **Side-output** — route to a separate late-event stream

## Backpressure

When consumers cannot keep up with the ingestion rate:

| Strategy | Behavior |
|----------|----------|
| Buffer | Queue events in memory (bounded) |
| Drop oldest | Discard old events to make room |
| Block producer | Slow down ingestion (last resort) |

Chronix implements bounded buffering with configurable queue depth,
falling back to dropping oldest events under sustained overload.

## Integration with Signal Triggers

The streaming layer feeds directly into the [Signal Trigger Engine](@/internals/signal-triggers.md),
which evaluates continuous SQL-like rules against the live event stream.
This enables sub-second alert latency without polling.
