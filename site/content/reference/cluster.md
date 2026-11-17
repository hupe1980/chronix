+++
title = "Cluster (frozen)"
description = "The frozen Chronix cluster tier: the chronixd-cluster binary, MetaNode and DataNode roles, Raft metadata, region routing and what is unsupported."
weight = 110
aliases = ["/docs/cluster/", "/internals/raft/", "/internals/consistent-hashing/", "/internals/distributed/"]
+++

**The cluster tier is frozen.** It compiles and its tests run, but it is not
developed, not published, and not part of the supported product. Use a single
`chronixd` or the embedded crate; this page describes what exists for readers
who build it from source.

## Build and run

Cluster mode is its own binary, built from the repository:

```bash
cargo build --release -p chronixd-cluster
```

It accepts every `chronixd` flag plus the role flags below. Each flag also reads
the environment variable shown.

| Flag | Environment | Meaning |
|---|---|---|
| `--mode config\|meta\|data` | `CHRONIXD_MODE` | Role. `config` (the default) takes it from the `[cluster]` section |
| `--node-id N` | `CHRONIXD_NODE_ID` | Unique node id; required for `meta` and `data` |
| `--raft-bind ADDR` | `CHRONIXD_RAFT_BIND` | Raft and admin gRPC address of a MetaNode |
| `--cluster-peers A,B` | `CHRONIXD_CLUSTER_PEERS` | MetaNode peers to bootstrap with |
| `--meta-addrs A,B` | `CHRONIXD_META_ADDRS` | MetaNodes a DataNode registers with |

```bash
# Three MetaNodes, then DataNodes that register with them.
chronixd-cluster --mode meta --node-id 1 --raft-bind 0.0.0.0:9100 \
  --cluster-peers 10.0.0.2:9100,10.0.0.3:9100
chronixd-cluster --mode data --node-id 10 --meta-addrs 10.0.0.1:9100,10.0.0.2:9100
```

`chronixd` itself refuses a `[cluster]` configuration section, so a cluster
config can never start a single-node server by mistake.

## Roles

```text
MetaNodes (Raft group)          DataNodes
  schemas                         regions of measurements
  routing table   ◄─ register ─   WAL, memtables, segments
  node membership    heartbeat    write router, query router
  model catalog                   region replication and migration
```

- **MetaNode** — a Raft group (OpenRaft) holding the cluster's metadata:
  measurement schemas, the routing table from region to DataNode, node
  membership and the analytics model catalog. Run three or five.
- **DataNode** — runs a full embedded Chronix database, hosts one or more
  regions, registers with the MetaNodes and heartbeats. Writes and queries are
  routed to the regions that own each series; a query that spans regions fans
  out and merges.

## How data is placed

A measurement is split into regions, and a series is assigned to a region by
hashing its canonical key modulo the region count. Regions replicate over Raft
per region, migrate between DataNodes, and split when they grow; only splitting
exists, not merging.

## What is not supported

- **Rebalancing cost.** Placement is `hash % region_count`, so changing the
  region count moves most series. Jump consistent hashing is the known fix and
  is not implemented.
- **Region merge.** Regions only split.
- **Operations.** There is no supported upgrade path, monitoring guide or
  sizing guidance for the tier.

The frozen tier is excluded from the default build (`cargo build` and
`cargo test` cover the product; `--workspace` adds it), so nothing in it can
slow down or break the single-node product.
