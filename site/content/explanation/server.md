+++
title = "Server architecture"
description = "How chronixd is put together: the HTTP, gRPC and Flight SQL surfaces, wire-protocol decoding, ingestion connectors and tracing."
weight = 160
aliases = ["/reference/server/"]
+++

`chronixd` is the embedded engine behind network surfaces. One shared
`Chronix` handle serves HTTP (the native API, PromQL, the wire protocols, MCP
and the console), gRPC and Arrow Flight SQL, and the optional Kafka and MQTT
connectors write into it. A write over any surface is visible on every other
surface at once. Everything below the surfaces — storage, maintenance,
analytics — is the library, unchanged.

```text
            HTTP :8086            gRPC :8087        Flight SQL :8817     Kafka · MQTT
  (REST, PromQL, Influx, OTLP,       │                   │              (connectors)
   remote write/read, MCP, console)  │                   │                   │
                 └───────────────────┴─────────┬─────────┴───────────────────┘
                                auth → namespace → Cedar → rate limits
                                               │
                                        Arc<Chronix>  (blocking calls run off the async runtime)
```

## Surfaces

| Surface | Routes or service | Protocol |
|---|---|---|
| Native API | `/api/v1/…` | JSON |
| PromQL | `/api/v1/query`, `/query_range`, `/series`, `/labels`, `/status/tsdb` | Prometheus HTTP API |
| Prometheus remote write / read | `/api/v1/prom/write`, `/api/v1/prom/read` | protobuf + snappy, RW 1.0 and 2.0 |
| InfluxDB line protocol | `/write`, `/api/v2/write`, `/api/v1/write` | line protocol, gzip accepted |
| OTLP metrics | `/v1/metrics`, `/api/v1/otlp/metrics` | protobuf or JSON |
| MCP | `/mcp` | Model Context Protocol, streamable HTTP |
| Console | `/console` | a static page that calls the routes above |
| gRPC | `:8087` | write (unary and streaming), query, SQL, schema, delete |
| Flight SQL | `:8817` | SQL queries; bulk writes with `DoPut` |
| Operations | `/health`, `/ready`, `/metrics` | |

Every data route passes the same chain: authentication, namespace resolution,
the Cedar policy, the per-namespace rate and quota limits, then the engine. A
route the policy has not classified is refused, and a test walks the router to
prove each route is classified.

**How it behaves:**

- **One database.** The engine's methods are synchronous; the server runs them
  on blocking threads, so a slow query never stalls the async runtime.
- **Bounded concurrency.** SQL, PromQL and Flight SQL share
  `max_concurrent_queries` slots; a query that cannot get one within its
  deadline gets 503 naming the setting. Writes and health checks never wait.
- **Graceful shutdown.** `SIGTERM` or `SIGINT` stops accepting connections,
  drains requests and connectors, flushes and closes the database.
- **TLS** with rustls on all three listeners when configured, with certificate
  reload; mutual TLS optional.
- **One result encoder.** Every JSON and protobuf answer goes through the same
  Arrow-to-JSON conversion, which has no catch-all arm — a new Arrow type is a
  build error, not a silently wrong cell. Flight SQL streams Arrow unchanged.

## Ingestion connectors

Kafka and MQTT are compile-time features (`kafka`, `mqtt`, or `all-connectors`),
configured under `[kafka]` and `[mqtt]` and listed at `GET /api/v1/connectors`.

| | Kafka (`krafka`, pure Rust) | MQTT (`rumqttc`) |
|---|---|---|
| Payload | line protocol or JSON | line protocol or JSON |
| Measurement | topic name, or `topic_measurement_map` | last topic segment, or `topic_measurement_map`; topic levels become tags |
| Delivery | at least once: offsets are committed only after the batch is written | as the subscription's QoS level provides |
| Transport | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`; SCRAM or PLAIN; private CA | plaintext |
| Reconnect | retries with backoff | retries every 5 s |

A connector reports `Running`, `Idle`, `Reconnecting`, `Failed(reason)` or
`Stopped`. A broker that is down does not make the node unready — `/ready`
asks only whether the database accepts writes — so alert on
`chronix_connector_up` instead. Kafka lag is `chronix_kafka_consumer_lag`.
Configuration reloads reconcile: removed connectors stop, new ones start,
unchanged ones keep running.

## Observability

- **Metrics** at `/metrics` in Prometheus format: request, ingest, query,
  storage, connector and analytics series, with histograms for latencies. The
  full list is in the [metrics reference](@/reference/metrics.md).
- **Logs** in pretty, compact or JSON form, filtered with `RUST_LOG`-style
  directives.
- **Traces** with the `otlp` feature: spans exported over OTLP, sampled always,
  never or by ratio, with errors always sampled if asked. W3C `traceparent`
  is propagated on incoming and outgoing gRPC calls.

## Embedding the server

`chronixd` is a library too. `chronixd::server::run_with(config, extensions)`
starts the same server with additional `Extension`s — each gets the database
handle, can add routes inside the auth and policy layers, and is stopped in
reverse order on shutdown. The cluster binary, `chronixd-cluster`, is built
this way.

## Related

- [HTTP API](@/reference/http-api.md)
- [Configuration](@/reference/configuration.md)
- [Secure a server](@/guides/secure-a-server.md)
- [Ingest data](@/guides/ingest.md)
