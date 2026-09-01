+++
title = "Model Lifecycle Management"
description = "Analytical models (forecasters, anomaly detectors) are not static — they must be trained, versioned, retrained as data evolves, and eventually retired. Without lifecycle management, stale models…."
weight = 350
+++

## The Problem

Analytical models (forecasters, anomaly detectors) are **not static** —
they must be trained, versioned, retrained as data evolves, and eventually
retired. Without lifecycle management, stale models produce increasingly
inaccurate results.

## Lifecycle States

```text
  ┌────────┐     ┌──────────┐     ┌────────┐     ┌──────────┐
  │ Created │ ──▸ │ Training │ ──▸ │ Active │ ──▸ │ Retired  │
  └────────┘     └──────────┘     └────────┘     └──────────┘
                       │               │
                       │               ▼
                       │          ┌──────────┐
                       └───────── │Retraining│
                                  └──────────┘
```

| State | Description |
|-------|-------------|
| Created | Model definition registered, not yet trained |
| Training | Model is being fitted on training data |
| Active | Model is serving predictions |
| Retraining | Existing model being updated with new data |
| Retired | Model is archived, no longer serving |

## Model Registry

The registry stores model metadata and serialized parameters:

```text
ModelRecord {
    id: UUID,
    name: "cpu_forecast_hourly",
    model_type: HoltWinters,
    version: 3,
    state: Active,
    created_at: "2024-01-15T00:00:00Z",
    trained_at: "2024-01-15T00:05:00Z",
    training_window: "7 days",
    metrics: { mase: 0.73, rmse: 2.1 },
    parameters: <serialized bytes>,
    config: { alpha: 0.2, beta: 0.05, gamma: 0.1, period: 24 },
}
```

### Versioning

Each retraining creates a new version. Old versions are retained for:
- **Rollback**: If the new version performs worse
- **Comparison**: A/B testing old vs new model
- **Audit**: Reproducing past predictions

#### Rollback & Deletion API

The registry provides explicit lifecycle management operations:

- **`rollback_champion(measurement, model_name, version)`** — Promotes an
  older version back to champion, effectively reverting a bad deployment.
  Delegates to the tagging system and logs the rollback.
- **`delete_version(measurement, model_name, version)`** — Removes a specific
  model version. Refuses to delete the current champion (returns `false` with
  a warning) to prevent accidental production outages.

#### Model Integrity (SHA-256)

Every registered model version stores a SHA-256 hash of its serialized
parameter bytes (`model_hash: [u8; 32]`). This enables:

- **Tamper detection**: Call `version.verify_integrity()` to verify that the
  stored bytes have not been corrupted or modified since registration.
- **Reproducibility**: Hash values can be logged alongside predictions for
  complete audit trails.

```rust
let version = registry.get_champion("cpu", "forecast").unwrap();
assert!(version.verify_integrity(), "model bytes corrupted!");
```

### Thread Safety

The `ModelRegistry` is fully thread-safe, using `parking_lot::RwLock`
with interior mutability. All methods take `&self` (not `&mut self`), so
the registry can be shared across threads via `Arc<ModelRegistry>` without
external synchronization. Read operations (lookups, listings) acquire a
shared read lock; mutations (register, tag, update metrics) acquire an
exclusive write lock.

### Error Handling

`chronix_analytics::lifecycle` provides a dedicated `LifecycleError` enum
with structured variants: `NotFound`, `InvalidConfig`, `ABTest`, `Drift`,
and `Persistence`. This replaces ad-hoc string errors with typed errors
that can be pattern-matched by callers.

## Retraining Strategies

### Scheduled Retraining

Models are retrained on a fixed schedule (e.g. daily, weekly):

| Metric Type | Recommended Interval | Rationale |
|-------------|---------------------|-----------| 
| Infrastructure (CPU, memory) | Daily | Workload patterns shift |
| Business metrics (orders) | Weekly | Seasonal patterns are stable |
| IoT sensors | Monthly | Physical processes change slowly |

### Drift-Triggered Retraining

Monitor prediction accuracy in real-time. Retrain when performance
degrades below a threshold:

$$
\text{MASE}_{\text{recent}} > 1.5 \times \text{MASE}_{\text{baseline}}
$$

This avoids unnecessary retraining when the model is still accurate,
and catches distribution shifts immediately.

### Concept Drift Detection

**Concept drift** occurs when the statistical properties of the target
variable change over time. Types:

| Type | Example | Detection |
|------|---------|-----------|
| Sudden | Deployment changes behavior | Error spike |
| Gradual | Slowly increasing load | Trend in errors |
| Recurring | Seasonal pattern shift | Periodic error peaks |
| Incremental | Hardware degradation | Drift test (Page-Hinkley) |

## Training Data Management

### Sliding Window

Train on the most recent *w* observations, discarding older data:
- Pro: Adapts to recent patterns
- Con: Forgets long-term seasonality

### Growing Window

Train on all historical data:
- Pro: Captures all patterns
- Con: Slow training, may overfit old patterns

### Weighted Window

Weigh recent observations more heavily (exponential decay):
- Pro: Balanced adaptation
- Con: More complex training objective

Chronix defaults to **sliding window** with the window size determined
by the model type (e.g. 7 days for daily patterns, 4 weeks for weekly).

## Serialization

Model parameters are serialized using a compact binary format:

| Field | Encoding |
|-------|----------|
| Model type tag | 1 byte |
| Version | varint |
| Parameter count | varint |
| Parameters | f64 array (IEEE 754) |
| Seasonal components | f64 array |
| Metadata | MessagePack |

Total serialized size is typically 200–2000 bytes per model, enabling
efficient storage and transfer in the distributed cluster.

## Garbage Collection

Retired model versions are garbage-collected after a configurable
retention period (default: 30 days). Active and the most recent retired
version are always retained.
