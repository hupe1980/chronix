+++
title = "Examples"
description = "Runnable examples covering the embedded API, SQL and PromQL queries, forecasting, anomaly detection, streaming, and the cold tier."
weight = 100
+++

Chronix ships with **27 runnable examples** covering every major feature. CI
compiles and runs every one of them on each change, so they cannot drift away
from the API they demonstrate.

Run any of them with:

```bash
cargo run -p chronix --example <name>

# `cold_tier` needs the object-store feature
cargo run -p chronix --features object-store --example cold_tier
```

---

## Getting Started

| Example | Description |
|---------|-------------|
| [basic_usage](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/basic_usage.rs) | Open a database, insert data points, and query them back |
| [query_and_aggregation](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/query_and_aggregation.rs) | Fluent `QueryBuilder` with aggregation, downsampling, and pruning statistics |
| [sql_queries](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/sql_queries.rs) | SQL via DataFusion: `GROUP BY`, window functions, `ORDER BY`, `LIMIT`, UDFs |
| [promql_queries](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/promql_queries.rs) | PromQL instant/range queries: `rate()`, `avg_over_time()`, label matching |
| [schema_exploration](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/schema_exploration.rs) | Introspect measurements, tag keys, field keys, columns, and schema registry |

## Analytics & Forecasting

| Example | Description |
|---------|-------------|
| [forecast](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/forecast.rs) | All forecast models: Linear Regression, SES, Holt, Holt-Winters, ARIMA, SARIMA |
| [anomaly_detection](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/anomaly_detection.rs) | All anomaly methods: Z-Score, Modified Z-Score, IQR, Dynamic Threshold, Forecast Residual |
| [continuous_forecast](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/continuous_forecast.rs) | Continuous forecasting engine with accuracy tracking and automatic refit |

## Multivariate & Preprocessing

| Example | Description |
|---------|-------------|
| [multivariate_analysis](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/multivariate_analysis.rs) | Cross-series correlation, multivariate anomaly detection, PCA, and VAR forecasting |
| [preprocessing](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/preprocessing.rs) | Gap detection, interpolation, smoothing, STL decomposition, resampling |

## Data Pipeline

| Example | Description |
|---------|-------------|
| [data_pipeline](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/data_pipeline.rs) | CDC-driven real-time pipeline with triggers, alerts, streaming anomaly, and forecasting |
| [alerting](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/alerting.rs) | Streaming anomaly detection with threshold-based alerts and cooldown |
| [signal_triggers](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/signal_triggers.rs) | Trigger engine with SQL triggers, delivery routing, and dead-letter queues |

## Storage & Operations

| Example | Description |
|---------|-------------|
| [gateway_footprint](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/gateway_footprint.rs) | The small preset under the design partner's workload, printing the process's resident set at each step |
| [storage_lifecycle](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/storage_lifecycle.rs) | Manual flush, compaction, rollup materialisation, and retention enforcement |
| [delete_operations](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/delete_operations.rs) | Tombstone-based deletes, predicate deletes, and measurement drops |
| [encoding](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/encoding.rs) | Compression codecs, adaptive encoding, and compression ratio comparison |
| [compute_engine](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/compute_engine.rs) | SIMD-accelerated computation, hardware tier detection, and buffer pool |

## Security & Multi-Tenancy

| Example | Description |
|---------|-------------|
| [encryption](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/encryption.rs) | AES-256-GCM encryption at rest with key rotation |
| [authz](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/authz.rs) | Cedar-based RBAC authorization: policies, principals, and allow/deny decisions |
| [audit_logging](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/audit_logging.rs) | Structured audit logging with multi-sink support and queryable audit trail |
| [tenant_isolation](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/tenant_isolation.rs) | Namespace management, quota enforcement, and resource usage tracking |

## Model Lifecycle & Chaos

| Example | Description |
|---------|-------------|
| [model_lifecycle](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/model_lifecycle.rs) | Model registry, A/B testing, drift detection, and accuracy tracking |
| [chaos_testing](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/chaos_testing.rs) | Fault injection with RAII guards, multiple fault types, and introspection |

## Recently added

| Example | Description |
|---------|-------------|
| [quickstart](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/quickstart.rs) | The program from the Getting Started page — open, write, query, close |
| [quantile_forecast](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/quantile_forecast.rs) | Prediction intervals from empirical residual quantiles, with conformal correction |
| [cold_tier](https://github.com/hupe1980/chronix/blob/main/crates/chronix/examples/cold_tier.rs) | Tiering segments to object storage as Hive-partitioned Parquet, and querying them with SQL (needs `--features object-store`) |
