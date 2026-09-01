+++
title = "Raft Consensus"
description = "In a distributed system, nodes must agree on a shared state (cluster membership, shard assignments, leadership) despite crashes and network partitions. Raft (Ongaro & Ousterhout, 2014) solves this…."
weight = 380
+++

## The Consensus Problem

In a distributed system, nodes must agree on a shared state (cluster
membership, shard assignments, leadership) despite crashes and network
partitions. **Raft** (Ongaro & Ousterhout, 2014) solves this with an
understandable protocol built on three sub-problems.

## Protocol Overview

### Node Roles

```text
           Timeout              Wins election
  ┌──────────────────┐     ┌──────────────────┐
  │    Follower       │ ──▸ │    Candidate     │ ──▸ Leader
  └──────────────────┘     └──────────────────┘
        ▲                         │
        └─────────────────────────┘
              Discovers higher term
```

| Role | Responsibility |
|------|----------------|
| **Leader** | Accepts client writes, replicates log entries |
| **Follower** | Passive; responds to leader's AppendEntries RPCs |
| **Candidate** | Runs election when leader is suspected dead |

### Terms

Time is divided into **terms** — monotonically increasing integers.
Each term begins with an election. Terms act as a logical clock,
allowing nodes to detect stale leaders.

## Leader Election

1. Follower's **election timer** expires (randomized 150–300ms)
2. Follower transitions to Candidate, increments term, votes for self
3. Sends `RequestVote` RPCs to all peers
4. Wins if it receives votes from a **majority** ($\lfloor n/2 \rfloor + 1$)
5. Becomes Leader and starts sending heartbeats

### Safety Properties

- **Election safety**: At most one leader per term
- **Randomized timeouts**: Prevent split-vote scenarios
- **Log completeness**: Candidate must have the most up-to-date log to win

## Log Replication

The leader maintains a **replicated log** of commands:

```text
Leader log:  [1:set a=1] [2:set b=2] [3:set c=3] [4:set a=4]
                                        ↑
                                    commit index

Follower 1:  [1:set a=1] [2:set b=2] [3:set c=3]
Follower 2:  [1:set a=1] [2:set b=2]
```

### Commit Rule

An entry is **committed** when it has been replicated to a majority
of nodes. Once committed, it is guaranteed to appear in the logs of
all future leaders.

### Consistency Check

Each `AppendEntries` RPC includes the index and term of the entry
immediately preceding the new entries. If a follower's log doesn't
match, it rejects the RPC, and the leader backs up until it finds
the matching point.

## Safety Guarantees

| Property | Description |
|----------|-------------|
| **Leader completeness** | A committed entry appears in all future leaders' logs |
| **State machine safety** | All nodes apply the same log entries in the same order |
| **Linearizability** | Reads through the leader see all committed writes |

## Chronix Usage

Chronix uses Raft for **metadata consensus**, not data replication:

| Replicated via Raft | Replicated via Data Path |
|---------------------|------------------------|
| Cluster membership | Time-series data |
| Shard assignments | WAL entries |
| Schema changes | Segment files |
| Leader election | Metric values |

This separation keeps the Raft log small and fast — only metadata
mutations (infrequent) go through consensus, while the high-throughput
data path uses a simpler replication protocol.

### Stale-Leader Fencing

`RegionRaftManager::propose_write()` performs a **leadership pre-check**
before proposing a write to the Raft group. It inspects
`raft.metrics().current_leader` and verifies that the local node is still
the leader. If another node has been elected leader (e.g. after a network
partition heals), the stale leader returns `RaftForwardToLeader` with the
current leader's ID, allowing the client to redirect. This avoids the
latency of a full Raft round-trip for writes that would inevitably be
rejected.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `raft.election_timeout` | 200ms | Base election timeout |
| `raft.heartbeat_interval` | 50ms | Leader heartbeat frequency |
| `raft.snapshot_interval` | 10000 entries | Compact log after N entries |
| `raft.max_batch_size` | 64 | Max entries per AppendEntries |

## Log Compaction

The Raft log grows unboundedly. **Snapshotting** compacts it by
serializing the state machine and truncating the log:

```text
Before:  [1] [2] [3] [4] [5] [6] [7] [8] [9] [10]

After:   [snapshot @ index 7] [8] [9] [10]
```

New nodes or far-behind followers receive the snapshot instead of
replaying the entire log history.
