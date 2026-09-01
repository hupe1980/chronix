+++
title = "Testing & Hardening"
description = "Chaos testing, the hardening notes, and the invariants the test suite pins."
weight = 110
+++

## Chaos Testing (`chronix-chaos`)

### Fault Types

| Fault | Parameters | Effect |
|-------|------------|--------|
| `KillNode` | delay | Simulates node crash after delay |
| `NetworkPartition` | isolated_nodes | Isolates specific nodes |
| `DiskFull` | — | Simulates full disk |
| `SlowDisk` | latency | Adds latency to disk operations |
| `LatencySpike` | delay | Adds network latency |
| `WriteDropper` | drop_ratio | Randomly drops writes |
| `ReadCorruption` | corruption_ratio | Corrupts read data |

### ChaosAgent

Thread-safe fault injection manager with:
- `inject(config)` → `FaultGuard` (RAII auto-cleanup on drop)
- `is_fault_active(predicate)` — query active faults
- `injected_latency()` — sums all active latency faults
- `should_drop_write()` — probabilistic write dropping
- `gc_expired()` — automatic expiration of timed-out faults
- Max concurrent fault limit enforcement

## See Also

- [Analytics Guide](@/docs/analytics.md) — forecast models, anomaly detection, SQL interface
- [Cluster Operations](@/docs/cluster.md) — setup, scaling, failover, replication
- [Operations Guide](@/docs/operations.md) — deployment, configuration, monitoring
- [Performance Tuning](@/docs/performance.md) — workload profiles, memory tuning
- [Security Guide](@/docs/security.md) — authentication, authorization, mTLS
- [Guide](@/docs/_index.md)
