+++
title = "Signal Processing & Triggers"
description = "Signal triggers are continuous monitoring rules that evaluate conditions against live time-series data and fire alerts when thresholds are breached. They bridge the gap between passive data…."
weight = 340
+++

## Overview

Signal triggers are **continuous monitoring rules** that evaluate conditions
against live time-series data and fire alerts when thresholds are breached.
They bridge the gap between passive data storage and active incident
response.

## Trigger Definition

Triggers are defined using a SQL-like DSL:

```sql
CREATE TRIGGER high_cpu
ON metrics
WHERE metric_name = 'cpu_utilization'
WHEN AVG(value) OVER (INTERVAL '5 minutes') > 90.0
SEVERITY 'critical'
COOLDOWN INTERVAL '120s'
ACTION webhook('https://alerts.example.com/hook');
```

### Components

| Component | Purpose |
|-----------|---------|
| `ON` | Source metric stream |
| `WHERE` | Filter (metric name, tags) |
| `WHEN` | Condition with window aggregation |
| `SEVERITY` | Alert priority level |
| `COOLDOWN` | Minimum time between firings |
| `ACTION` | Response: webhook, log, escalate |

### Quoted Identifiers

Trigger and measurement names containing special characters (hyphens,
dots, spaces) can be enclosed in double quotes or backticks:

```sql
CREATE TRIGGER "high-cpu.alert" ON "system.cpu" WHEN value > 90.0;
CREATE TRIGGER `my trigger` ON `my measurement` WHEN value > 1.0;
```

Double quotes support escaped quotes via doubling: `"say""hello"` produces
the identifier `say"hello`.

## Evaluation Semantics

### Window Functions

Triggers evaluate **sliding window aggregations** against thresholds:

| Function | Description |
|----------|-------------|
| `AVG(value) OVER (INTERVAL '5m')` | Mean over last 5 minutes |
| `MAX(value) OVER (INTERVAL '1m')` | Peak in last minute |
| `COUNT(*) OVER (INTERVAL '10m')` | Event count |
| `PERCENTILE(value, 0.99) OVER (...)` | 99th percentile |
| `RATE(value) OVER (INTERVAL '1m')` | Rate of change per second |

### Evaluation Frequency

Triggers are evaluated at each new data point (push-based, driven by the
streaming CDC layer). This provides **sub-second latency** from data
arrival to alert firing.

## Cooldown Mechanism

Without cooldown, a sustained threshold breach produces a flood of
duplicate alerts. The cooldown period suppresses repeated firings:

```text
Condition breached:  ●────●────●────●────●────●────●
                     ↑         ↑                   ↑
                  FIRE      SUPPRESSED           FIRE
                     ├── cooldown ──┤         (cooldown expired)
```

### Implementation

Each trigger maintains a `last_fired_at` timestamp. A new firing is
suppressed if `now - last_fired_at < cooldown_duration`.

## Alert Severity Levels

| Level | Numeric | Response |
|-------|---------|----------|
| `info` | 0 | Log only |
| `warning` | 1 | Dashboard notification |
| `critical` | 2 | PagerDuty / webhook |
| `emergency` | 3 | Immediate escalation |

## Actions

### Webhook

Sends an HTTP POST with a JSON payload:

```json
{
  "trigger": "high_cpu",
  "severity": "critical",
  "value": 95.3,
  "threshold": 90.0,
  "timestamp": "2024-01-15T14:30:00Z",
  "tags": { "host": "web-01", "region": "us-east" }
}
```

### Composite Triggers

Triggers can reference other triggers for complex logic:

```sql
CREATE TRIGGER memory_pressure_with_high_cpu
WHEN TRIGGER('high_cpu') AND TRIGGER('high_memory')
SEVERITY 'emergency';
```

## Flap Detection

Metrics that oscillate around a threshold cause rapid fire/clear cycles
(**flapping**). Chronix implements **hysteresis** to prevent this:

- **Fire threshold**: 90%
- **Clear threshold**: 80%

The trigger fires when value crosses 90% and does not clear until value
drops below 80%. This creates a dead zone where the trigger state is
sticky.

```text
100% ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 90% ─ ─ ─ FIRE ─ ─ ─ ─ ─ ─
      ═══════════════════════  ← sticky zone
 80% ─ ─ ─ ─ ─ ─ CLEAR ─ ─ ─
  0% ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
```

## Performance

| Metric | Value |
|--------|-------|
| Evaluation latency | < 1 ms per trigger per event |
| Max concurrent triggers | 10,000+ |
| Memory per trigger | ~256 bytes + window buffer |

Triggers are evaluated in the hot path of ingestion, so they must be
extremely lightweight. Window buffers are stored in a ring buffer to
avoid allocations.

## Signal Event Metadata

Each `SignalEvent` carries condition-specific metadata that provides
context for downstream consumers (dashboards, webhooks, runbooks):

| Condition Type | Metadata Fields |
|----------------|----------------|
| All | `condition_type`, `value`, `severity` |
| `ForecastDeviation` | `tolerance_pct`, `actual`, `forecast`, `deviation_mode` |
| `AnomalyScore` | `threshold` |
| `RateOfChange` | `threshold_pct`, `window` |
| `FieldThreshold` | `field`, `threshold` |

### Deviation Mode

The `ForecastDeviation` trigger computes relative deviation when the
forecast is sufficiently large, but falls back to absolute comparison
when the forecast is near zero (to avoid division by near-zero). The
`deviation_mode` field in the event metadata indicates which path was
taken: `"relative"` or `"absolute_fallback"`. This transparency ensures
operators can distinguish between the two semantics.
