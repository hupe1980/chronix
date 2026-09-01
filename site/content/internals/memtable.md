+++
title = "Memtable & Skip Lists"
description = "The memtable is the in-memory write buffer that sits between the WAL and on-disk segment files. It must support high-concurrency writes with ordered iteration (for flushing sorted columnar segments)."
weight = 30
+++

The memtable is the in-memory write buffer that sits between the WAL and
on-disk segment files. It must support **high-concurrency writes** with
ordered iteration (for flushing sorted columnar segments).

## Skip List Theory

Chronix uses a **skip list** (Pugh, 1990) as the underlying data structure
for the memtable. A skip list is a probabilistic alternative to balanced
trees that provides:

- **O(log n)** expected time for search, insert, and delete
- **Lock-free concurrent access** with compare-and-swap operations
- **Sequential iteration** in sorted key order

### Structure

A skip list is a hierarchy of linked lists. The bottom level (L0) contains
all elements in sorted order. Each higher level is a random subset of the
level below, with each element independently promoted with probability *p*
(typically *p* = 0.5):

```text
Level 3:  ────────────────────── 40 ──────────────────── ∞
Level 2:  ──── 10 ────────────── 40 ──── 60 ──────────── ∞
Level 1:  ──── 10 ──── 25 ────── 40 ──── 60 ──── 80 ──── ∞
Level 0:  5 ── 10 ── 20 ── 25 ── 40 ── 50 ── 60 ── 75 ── 80 ── 90 ── ∞
```

### Complexity Analysis

For *n* elements with promotion probability *p*:

| Operation | Expected Time | Worst Case |
|-----------|--------------|------------|
| Search | O(log n / log(1/p)) | O(n) |
| Insert | O(log n) | O(n) |
| Delete | O(log n) | O(n) |
| Iteration | O(n) | O(n) |

The expected number of levels is O(log n). With *p* = 0.5, the expected number
of pointers per element is 2 (each level doubles the skip distance), so the
space overhead is O(n).

### Why Not a B-Tree?

Unlike B-trees, skip lists:

1. **Require no rebalancing** — insertions never cascade structural changes
2. **Support lock-free concurrency** — CAS-based insertion avoids global locks
3. **Have simple and cache-friendly iteration** — bottom-level traversal is a
   linear linked list scan

For the write-heavy workload of a time-series database, these properties
matter more than the B-tree's superior worst-case guarantees.

## Freeze-and-Swap Lifecycle

When the memtable reaches the configured size threshold:

```text
1. Current memtable is atomically swapped with a new empty one
2. The frozen memtable becomes read-only (still serves queries)
3. A background task iterates the frozen memtable in sorted order
4. Points are written to columnar segment file(s)
5. After flush completes:
   - Segment is registered in the catalog
   - WAL is truncated
   - Frozen memtable is dropped
```

The atomic swap uses an `Arc<RwLock<Memtable>>` so that writers never block
on flush and readers see a consistent snapshot of either the active or the
frozen memtable.

## Time-Shard Routing

Time-series writes are often slightly out of order (e.g. batch uploads,
clock skew between sources). The `ShardRouter` routes points to per-shard
memtables based on their timestamp:

```text
Point(ts=T)
    │
    ▼
ShardRouter
    ├── ts in shard_0 window → Memtable_0
    ├── ts in shard_1 window → Memtable_1
    └── ts outside tolerance → reject (TooOld / TooNew)
```

This ensures that each memtable covers a contiguous time range, making
segment time boundaries clean and enabling efficient time-range pruning
at query time.

## References

- W. Pugh. "Skip Lists: A Probabilistic Alternative to Balanced Trees."
  *Communications of the ACM*, 33(6), 1990.
- M. Herlihy, N. Shavit. *The Art of Multiprocessor Programming.*
  Morgan Kaufmann, 2012. (Lock-free data structures)
