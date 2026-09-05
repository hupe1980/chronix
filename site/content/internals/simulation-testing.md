+++
title = "Deterministic Simulation Testing"
description = "Chronix includes a deterministic simulation testing framework (chronix-dsim) inspired by FoundationDB's simulation testing and Jepsen. This enables rigorous verification of."
weight = 420
+++

Chronix includes a deterministic simulation testing framework (`chronix-dsim`) inspired by [FoundationDB's simulation testing](https://apple.github.io/foundationdb/testing.html) and [Jepsen](https://jepsen.io/). This enables rigorous verification of distributed system correctness under controlled fault injection.

## Why Simulation Testing?

Traditional integration tests exercise the "happy path" — they start a cluster, perform operations, and check results. But distributed systems fail in complex, non-obvious ways:

- **Network partitions** can cause split-brain or stale reads
- **Clock skew** can violate timestamp ordering assumptions
- **Message reordering** can break causal consistency
- **Partial failures** (e.g., leader crash mid-replication) can leave inconsistent state

Simulation testing addresses these by:

1. **Deterministic replay** — same seed always produces the same sequence of events
2. **Controlled fault injection** — partitions, delays, and failures at precise moments
3. **Formal verification** — linearizability checking and invariant validation
4. **Exhaustive exploration** — run thousands of seeds to cover edge cases

## Architecture

```
┌─────────────────────────────────────────────┐
│                  SimCluster                 │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Node 1  │  │  Node 2  │  │  Node 3  │  │
│  │(Raft+SM) │  │(Raft+SM) │  │(Raft+SM) │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  │
│       │              │              │       │
│  ┌────┴──────────────┴──────────────┴────┐  │
│  │            SimNetwork                 │  │
│  │  (partitions, delays, packet loss)    │  │
│  └───────────────────────────────────────┘  │
│                                             │
│  ┌──────────────┐  ┌─────────────────────┐  │
│  │ VirtualClock │  │  InvariantChecker   │  │
│  │ (skew ctrl)  │  │  + Linearizer       │  │
│  └──────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────┘
```

### Components

| Component | Purpose |
|-----------|---------|
| **`SimCluster`** | Manages N in-process Raft nodes with full lifecycle control |
| **`SimNetwork`** | Simulated network layer — partitions, isolation, packet loss |
| **`VirtualClock`** | Controlled time advancement with per-node skew injection |
| **`Linearizer`** | History-based linearizability checker (Wing & Gong algorithm) |
| **`InvariantChecker`** | Property-based invariant verification during simulation |

## Linearizability Checking

The `Linearizer` implements the [Wing & Gong (1993)](https://doi.org/10.1145/151646.151668) linearizability checking algorithm. Every operation is recorded with:

- **Invocation time** — when the operation was submitted
- **Return time** — when the response arrived
- **Operation type** — `Write(key, value)`, `Read(key, result)`, or `CAS(key, expected, new_value, ok)`

The checker verifies that there exists a sequential ordering of all operations that:

1. Respects real-time ordering (if operation A completes before B starts, A is ordered before B)
2. Is consistent with a single-register model (reads return the most recently written value)

### Example

```rust
use chronix_dsim::{Linearizer, OpKind};

let mut lin = Linearizer::new();

// Client 1 writes "x" = "1" at time [10, 20]
lin.record(1, OpKind::Write { key: "x".into(), value: "1".into() }, 10, 20);

// Client 2 reads "x" = "1" at time [15, 25]
lin.record(2, OpKind::Read { key: "x".into(), result: Some("1".into()) }, 15, 25);

// This is linearizable — the write can be linearized at time 15
lin.check().expect("should be linearizable");
```

## Invariants

Five built-in invariants are checked throughout simulation:

| Invariant | What It Checks |
|-----------|----------------|
| **LeaderUniqueness** | At most one leader per Raft term |
| **ReadYourWrites** | After an acked write, the writer can read their own value |
| **Durability** | Once a write is acknowledged, it is never lost |
| **QuorumSafety** | Writes only succeed in majority partitions |
| **MonotonicReads** | Repeated reads never "go backwards" |

## Running Simulations

### Basic Simulation

```rust
use chronix_dsim::{SimCluster, SimConfig};

#[tokio::test]
async fn test_partition_tolerance() {
    let mut sim = SimCluster::new(SimConfig {
        nodes: 5,
        seed: 42,
        heartbeat_ms: 50,
        election_min_ms: 150,
        election_max_ms: 300,
    }).await;

    sim.start().await;
    let leader = sim.wait_for_leader().await;

    // Write some data.
    sim.propose_schema(leader, "cpu").await;

    // Partition: isolate the leader.
    sim.isolate_node(leader);

    // The remaining 4 nodes should elect a new leader.
    // (Raft requires majority = 3 of 5.)
    sim.heal_network();
    let new_leader = sim.wait_for_leader().await;

    // Verify no invariants were violated.
    sim.check_invariants().expect("no violations");
    sim.check_linearizability().expect("linearizable");
}
```

### Multi-Seed Fuzzing

Run the same scenario with thousands of different seeds to explore non-deterministic edge cases:

```rust
#[tokio::test]
async fn fuzz_partition_recovery() {
    for seed in 0..1000 {
        let mut sim = SimCluster::new(SimConfig {
            seed,
            ..SimConfig::default()
        }).await;

        sim.start().await;
        let leader = sim.wait_for_leader().await;
        sim.propose_schema(leader, "test").await;

        sim.inject_partition(&[leader], &remaining_nodes(leader, 3));
        sim.heal_network();

        sim.check_invariants()
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}
```

### Clock Skew Testing

```rust
// Inject 500ms clock skew on node 1.
sim.set_clock_skew(1, 500);

// Advance global clock.
sim.advance_clock_ms(1000);

// Node 1 sees time as 1500ms, others see 1000ms.
assert_eq!(sim.clock().skew_for_node(1), 1500);
assert_eq!(sim.clock().skew_for_node(2), 1000);
```

## Where this sits

Simulation testing is for the **frozen** distributed tier: a virtual clock and
an in-memory network let a whole cluster's failure modes run deterministically
and fast. The single-node engine is tested differently — with real process
crashes and injected I/O failures — because its failures are real syscalls
rather than message ordering ([Testing & Hardening](@/reference/testing.md)).

There is no runtime fault-injection API — faults are injected in tests, where
a verdict is checked.
