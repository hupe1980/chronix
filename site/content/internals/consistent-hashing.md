+++
title = "Consistent Hashing"
description = "Simple modular hashing () fails when the number of nodes changes — all keys are reassigned."
weight = 390
+++

## The Problem with Modular Hashing

Simple modular hashing ($\text{node} = \text{hash}(key) \mod N$) fails
when the number of nodes changes — **all** keys are reassigned:

| Key hash | 3 nodes ($\mod 3$) | 4 nodes ($\mod 4$) | Moved? |
|----------|-------------------|-------------------|--------|
| 7 | Node 1 | Node 3 | ✓ |
| 12 | Node 0 | Node 0 | ✗ |
| 15 | Node 0 | Node 3 | ✓ |
| 22 | Node 1 | Node 2 | ✓ |

With modular hashing, adding one node moves ~75% of keys. In a TSDB
with terabytes of data, this triggers massive data migration.

## Consistent Hashing

Consistent hashing (Karger et al. 1997) maps both nodes and keys onto
a **hash ring** (a circular space of $[0, 2^{32})$):

```text
            0
          ╱   ╲
        N1      N3
       ╱          ╲
      │    ●k1     │
      │  ●k2       │
       ╲          ╱
        N2      N4
          ╲   ╱
           ∞
```

Each key is assigned to the **first node clockwise** on the ring.

### When a node is added

Only keys between the new node and its predecessor are reassigned:

$$
\text{Keys moved} \approx \frac{K}{N}
$$

where $K$ is the total number of keys and $N$ is the new node count.
Adding one node to a 10-node cluster moves only ~10% of keys.

## Virtual Nodes

Physical nodes with a single hash position create **imbalanced load** —
nodes responsible for large ring arcs handle more data. **Virtual nodes**
solve this by mapping each physical node to $V$ points on the ring:

```text
Physical Node A → { vA₁, vA₂, vA₃, …, vA_V }
Physical Node B → { vB₁, vB₂, vB₃, …, vB_V }
```

With $V \geq 100$, the load variance drops below 10%:

$$
\text{Load std dev} \propto \frac{1}{\sqrt{V}}
$$

### Chronix Default

Chronix uses $V = 256$ virtual nodes per physical node, providing
< 5% load imbalance in typical deployments.

## Rebalancing on Scale-Out

When a new node joins:

1. New node's virtual nodes are placed on the ring
2. For each new virtual node, identify the **predecessor** on the ring
3. Transfer keys in the range (predecessor, new virtual node] from the
   current owner to the new node
4. Update the shard map atomically via Raft

### Transfer Protocol

Data transfer uses a **bulk copy** protocol:
1. Source node creates a snapshot of the affected key range
2. Snapshot is streamed to the new node
3. New writes are dual-written during transfer
4. Once caught up, ownership switches atomically

## Lookup Performance

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Key → node lookup | O(log(N·V)) | Binary search on sorted ring |
| Node addition | O(V · log(N·V)) | Insert V virtual nodes |
| Node removal | O(V · log(N·V)) | Remove V virtual nodes |

With $N = 100$ nodes and $V = 256$, the ring has 25,600 entries.
Lookup is ~15 comparisons (binary search) — sub-microsecond latency.

## Weighted Nodes

Nodes with different capacities (e.g. different disk sizes) can be
assigned proportionally more virtual nodes:

$$
V_i = V_{\text{base}} \times \frac{\text{capacity}_i}{\text{avg\_capacity}}
$$

A node with 2× the disk of the average gets 2× the virtual nodes,
and therefore ~2× the data — matching its capacity.

## Hash Function

Chronix uses **xxHash3** for the ring hash function:
- Deterministic (same key → same position everywhere)
- Uniform distribution (minimal clustering)
- Fast (< 10 ns per hash)
- 64-bit output (folded to 32-bit ring position)
