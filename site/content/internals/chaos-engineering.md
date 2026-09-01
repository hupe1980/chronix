+++
title = "Chaos Engineering"
description = "Chaos engineering is the discipline of experimenting on a system to build confidence in its ability to withstand turbulent conditions in production. Rather than waiting for failures to happen,…."
weight = 410
+++

## Philosophy

Chaos engineering is the discipline of **experimenting on a system** to
build confidence in its ability to withstand turbulent conditions in
production. Rather than waiting for failures to happen, chaos engineering
proactively injects failures in controlled environments.

> "The best way to build confidence in a complex system is to test it
> under controlled failure conditions." — Principles of Chaos Engineering

## Fault Injection Framework

Chronix includes a built-in fault injection framework for testing
resilience:

```text
┌──────────────┐     ┌─────────────┐     ┌───────────────┐
│  Experiment  │ ──▸ │  Injector   │ ──▸ │  Observer     │
│  Definition  │     │  (applies   │     │  (measures    │
│              │     │   faults)   │     │   impact)     │
└──────────────┘     └─────────────┘     └───────────────┘
                                                │
                                                ▼
                                         ┌──────────────┐
                                         │  Verdict     │
                                         │  (pass/fail) │
                                         └──────────────┘
```

## Fault Types

### Storage Faults

| Fault | Simulates | Impact |
|-------|-----------|--------|
| Disk write failure | Bad sector, full disk | WAL and segment durability |
| Slow I/O | Degraded SSD, noisy neighbor | Latency under load |
| Corrupt segment | Bit rot, partial write | Read path integrity |
| Lost WAL entry | Crash during write | Recovery correctness |

### Network Faults

| Fault | Simulates | Impact |
|-------|-----------|--------|
| Partition | Network split | Raft election, quorum writes |
| High latency | WAN / congested network | Replication lag |
| Packet loss | Lossy link | Retry and timeout behavior |
| DNS failure | DNS outage | Service discovery |

### Process Faults

| Fault | Simulates | Impact |
|-------|-----------|--------|
| Node crash | Kill -9, OOM | Failover, data recovery |
| Slow processing | GC pause, CPU throttle | Timeout handling |
| Clock skew | NTP drift | Timestamp ordering |
| Memory pressure | Container memory limits | OOM behavior |

## Experiment Design

### Steady-State Hypothesis

Define what "normal" looks like before injecting faults:

```text
Hypothesis: "The system can sustain 100K writes/sec with p99
             latency < 10ms and zero data loss"

Metrics:
  - write_throughput >= 100,000 samples/sec
  - write_latency_p99 <= 10 ms
  - data_loss_count == 0
```

### Blast Radius Control

| Control | Purpose |
|---------|---------|
| Target scope | Single node, single shard, or cluster-wide |
| Duration | Limit fault injection time |
| Rollback | Automatic fault removal on timeout |
| Kill switch | Manual abort via API |

### Experiment Lifecycle

```text
1. Define steady-state hypothesis
2. Start observing metrics
3. Wait for steady state (warmup)
4. Inject fault
5. Observe impact
6. Remove fault (automatic or manual)
7. Wait for recovery
8. Evaluate hypothesis
9. Report verdict
```

## Built-In Experiments

Chronix ships with pre-defined experiments for common failure modes:

| Experiment | What It Tests |
|------------|--------------|
| `leader_failover` | Kill leader, verify election < 500ms |
| `segment_corruption` | Corrupt segment file, verify CRC detection |
| `wal_recovery` | Crash during write, verify recovery |
| `network_partition` | Split cluster, verify majority continues |
| `disk_full` | Fill disk, verify graceful degradation |
| `clock_skew` | Offset system clock, verify ordering |
| `cascade_failure` | Kill nodes sequentially, find breaking point |

## Integration with Testing

Chaos experiments run as part of the CI/CD pipeline:

```text
Unit Tests → Integration Tests → Chaos Tests → Deploy
```

Chaos tests run against a multi-node test cluster with synthetic load.
Failures block deployment.

## Observability During Chaos

During experiments, enhanced metrics are collected:

| Metric | Purpose |
|--------|---------|
| `chaos.experiment_active` | Binary: is a fault currently injected? |
| `chaos.fault_type` | Label: which fault is active |
| `chaos.recovery_time_ms` | Time from fault removal to steady state |
| `chaos.data_loss_events` | Count of lost or corrupted data points |
| `chaos.verdict` | pass/fail result of steady-state check |
