+++
title = "Distributed Systems"
description = "Chronix operates as a distributed cluster for production deployments, providing horizontal scalability, fault tolerance, and high availability."
weight = 370
+++

Chronix operates as a distributed cluster for production deployments,
providing horizontal scalability, fault tolerance, and high availability.

## Cluster Architecture

```text
           ┌──────────────────┐
           │   Load Balancer  │
           └───────┬──────────┘
                   │
         ┌─────────┼─────────┐
         │         │         │
    ┌────▼───┐ ┌───▼────┐ ┌──▼─────┐
    │ Node 1 │ │ Node 2 │ │ Node 3 │
    │(Leader)│ │(Follow)│ │(Follow)│
    └────┬───┘ └───┬────┘ └──┬─────┘
         │         │         │
    ┌────▼─────────▼─────────▼────┐
    │     Shared Object Store     │
    │     (S3 / MinIO / Local)    │
    └─────────────────────────────┘
```

## Data Distribution

### Sharding

Time-series data is distributed across nodes using **consistent hashing**
on the series key (metric name + tag set):

$$
\text{shard}(\text{series}) = \text{hash}(\text{metric} \| \text{tags}) \mod R
$$

where $R$ is the number of virtual nodes on the hash ring (see
[Consistent Hashing](@/internals/consistent-hashing.md)).

### Replication

Each shard is replicated to $r$ nodes (default $r = 3$ in a cluster
≥ 3 nodes). The replication factor is configurable per namespace.

| Replication Factor | Tolerance | Write Cost |
|-------------------|-----------|------------|
| 1 | No failures | 1× |
| 2 | 1 node | 2× |
| 3 | 2 nodes | 3× |

### Write Path

1. Client sends write to any node
2. Node computes shard assignment via consistent hashing
3. Write is forwarded to the **primary** for that shard
4. Primary appends to local WAL and replicates to followers
5. Write is acknowledged after quorum ($\lfloor r/2 \rfloor + 1$) confirms

### Read Path

Reads are served by any replica. For consistency options:

| Level | Behavior | Latency |
|-------|----------|---------|
| `ONE` | Any replica, may be stale | Lowest |
| `QUORUM` | Majority must agree | Medium |
| `ALL` | All replicas must respond | Highest |

## Metadata Coordination

Cluster metadata (shard assignments, node membership, schema) is managed
by the **Raft consensus** layer (see [Raft Consensus](@/internals/raft.md)). This
ensures all nodes agree on the cluster state even under network partitions.

## Failure Handling

| Failure | Response |
|---------|----------|
| Node crash | Raft elects new leader; replicas continue serving |
| Network partition | Majority partition continues; minority becomes read-only |
| Disk failure | Node marked unhealthy; data rebuilt from replicas |
| Slow node | Removed from quorum after timeout; rejoin after recovery |

## Scaling

### Horizontal Scale-Out

Adding a node triggers **shard rebalancing**:
1. New node joins the hash ring
2. Adjacent shards split, transferring data to the new node
3. Transfer is done in the background without downtime

### Vertical Scaling

Individual nodes can be scaled by:
- More CPU → higher query parallelism
- More RAM → larger memtable and cache
- Faster disk → better write throughput
- More disk → longer local retention

## Multi-Tenancy

Chronix supports **namespace-based multi-tenancy** where each tenant's
data is logically isolated:

- Separate shard rings per namespace
- Independent retention policies
- Per-tenant resource quotas (storage, query concurrency)
- Data isolation enforced at the storage layer

See the [Security Internals](@/internals/security-internals.md) section for
authentication and authorization details.
