+++
title = "Write-Ahead Log (WAL)"
description = "The Write-Ahead Log provides crash-safe durability for the Chronix write path. Every mutation is persisted to the WAL before being applied to the memtable, guaranteeing that acknowledged writes…."
weight = 20
+++

The Write-Ahead Log provides **crash-safe durability** for the Chronix write
path. Every mutation is persisted to the WAL before being applied to the
memtable, guaranteeing that acknowledged writes survive process crashes and
power failures.

## Theoretical Foundation

The WAL is a fundamental building block of database systems, formalised in
the **ARIES recovery protocol** (Mohan et al., 1992). The key invariant is
the **Write-Ahead Logging Protocol**:

> A data page must not be flushed to disk until every log record describing
> a modification to that page has been flushed to stable storage.

This ensures that after a crash, the log contains a complete record of all
committed operations, enabling the recovery procedure to reconstruct the
correct state.

### Recovery Guarantees

Chronix implements a simplified form of ARIES suitable for append-only
time-series data:

- **Redo-only recovery** — since data is never updated in-place (segments
  are immutable), there is no need for undo operations. Recovery simply
  replays all WAL records since the last checkpoint.
- **Idempotent replay** — duplicate points are handled by the sort-merge
  deduplication layer, so replaying a WAL record that was already flushed
  to a segment is harmless.

### Sequence Numbers

Each WAL record carries a monotonically increasing 64-bit **sequence number**.
Sequence numbers provide:

- **Total ordering** of all writes across the system
- **Checkpoint coordination** — after a successful flush, the WAL is
  truncated up to the flushed sequence number
- **CDC ordering** — downstream consumers observe events in sequence order

## Record Format

Each WAL record is self-describing with integrity protection.

**v2 format** (current, `WAL_VERSION = 2`):

```text
┌──────────┬──────────┬──────────────┬────────────┬─────────────────┬───────────┐
│ CRC32c   │ Length   │ Sequence No  │ RecordType │ PayloadVersion  │ Payload   │
│ (4 bytes)│ (4 bytes)│ (8 bytes)    │ (1 byte)   │ (1 byte)        │ (variable)│
└──────────┴──────────┴──────────────┴────────────┴─────────────────┴───────────┘
```

- **RecordType** discriminates the payload kind: `Data` (0), `Batch` (1),
  `Schema` (2), `Tombstone` (3). New types can be added without a format bump.
- A record's declared length is bounded twice: by a 256 MB ceiling, and by the
  bytes remaining in the file. The second bound is the one that matters — the
  CRC is verified *after* the payload is read, so a corrupt length field is
  trusted for the allocation, and 256 MB is a quarter of the RAM on the
  gateways this engine targets.
- **Tombstones are not durable here.** The WAL logs them so that a
  point-in-time restore replays a delete, but the WAL is truncated once the
  memtable it covers has been flushed — so the durable copy lives in the
  catalog manifest instead. See the delete lifecycle in the architecture
  guide.
- **PayloadVersion** enables per-type schema evolution (currently `1`).
- The CRC covers `length + sequence_no + record_type + payload_version + payload`.

**v1 format** (legacy, `WAL_VERSION = 1`):

```text
┌──────────┬──────────┬──────────────┬───────────────────┐
│ CRC32c   │ Length   │ Sequence No  │ Payload           │
│ (4 bytes)│ (4 bytes)│ (8 bytes)    │ (variable)        │
└──────────┴──────────┴──────────────┴───────────────────┘
```

The reader accepts both v1 and v2 files. When reading v1 records, the type
defaults to `Data` and the payload version to `1`.

### Payload Encoding

WAL record payloads are serialized using a **binary v1 codec** (`wal_codec`
module in `chronix-core`) via `postcard`. The on-disk format is:

```text
[version: 0x01 (1 byte)][discriminant (1 byte)][postcard payload]
```

The functions `wal_encode()` and `wal_decode()` are exported from
`chronix_core`. For write-path performance, `encode_write_point(&Point)`
serialises a single point directly into the WAL buffer without cloning,
avoiding the allocation overhead of the general-purpose `wal_encode()` path.
The codec is defined by the `WalCodecError` error type.
Only binary v1 records are accepted — legacy JSON records are rejected
with `CodecError::UnknownVersion`.

### CRC32c Checksumming

The CRC32c (Castagnoli) polynomial `0x1EDC6F41` was chosen over the
traditional CRC32 (IEEE) for two reasons:

1. **Better error detection** — CRC32c has a minimum Hamming distance of 6
   for messages up to 16 kB, versus 4 for CRC32.
2. **Hardware acceleration** — Intel SSE4.2 and ARM CRC32 extensions provide
   single-instruction CRC32c computation, achieving throughput of ~1 byte per
   clock cycle.

In v2 format, the checksum covers `length + sequence_no + record_type +
payload_version + payload`. In v1 format it covers `length + sequence_no +
payload`. The CRC itself is not covered (a corrupted CRC will simply fail
verification).

## File Format

WAL files contain a 6-byte header followed by a sequence of records:

```text
┌───────────────┬──────────────┬──────────┬──────────┬─────┐
│ Magic "CXWL"  │ Version (u16)│ Record 0 │ Record 1 │ ... │
│ (4 bytes)     │ (2 bytes)    │          │          │     │
└───────────────┴──────────────┴──────────┴──────────┴─────┘
```

Files are named `wal_{sequence_start}.cxwl` and rotated when they exceed the
configured `max_file_size`. The magic bytes allow quick identification and the
version field enables forward-compatible format evolution.

## Fsync Policies

The `FsyncPolicy` controls the trade-off between durability and write latency:

| Policy | Behaviour | Durability | Latency |
|--------|-----------|------------|---------|
| `EveryWrite` | fsync after each record | Full | ~1 ms |
| `EveryN(n)` | fsync every *n* records | Bounded loss | ~100 μs |
| `Interval(ms)` | fsync on a timer | Bounded loss | ~10 μs |
| `Never` | rely on OS page cache | Best-effort | ~1 μs |

For time-series workloads, `EveryN(1000)` or `Interval(100)` provides a
practical balance: at most 1 000 points (or 100 ms of data) can be lost on
an unclean shutdown, but write throughput is 10–100× higher than `EveryWrite`.

### Bounded Condvar Wait

When group commit is enabled, writers wait on a condition variable for the
current batch to fill or the fsync timer to fire. The condvar wait is bounded
to a **5-second timeout** to prevent indefinite hangs under low write rates
or scheduler stalls. If the timeout expires, the partial batch is flushed
immediately.

### Periodic Background Fsync

`WalWriter` supports periodic background fsync via `start_periodic_sync()`.
When `WalConfig.fsync_interval` is set, a background task calls `fsync()` at
the configured cadence, bounding the window of data that can be lost without
requiring per-write or per-batch syncs.

## Checkpoint and Truncation

After a memtable flush completes, the WAL writer records the flushed sequence
number and truncates (deletes) all WAL files whose records have been fully
persisted to segments. This bounds WAL disk usage to approximately:

```
WAL_size ≈ memtable_threshold + one_file_headroom
```

## Poison Flag and Recovery

The WAL poison flag is queryable via `is_poisoned()` and clearable via
`clear_poison()` for recovery from transient I/O errors. `clear_poison()`
re-opens the file descriptor and resets internal state, allowing the WAL
to resume operation without a full process restart.

## References

- C. Mohan, D. Haderle, B. Lindsay, H. Pirahesh, P. Schwarz. "ARIES:
  A Transaction Recovery Method Supporting Fine-Granularity Locking and
  Partial Rollbacks Using Write-Ahead Logging." *ACM TODS*, 17(1), 1992.
- M. Stonebraker, L. A. Rowe. "The Design of POSTGRES." *SIGMOD*, 1986.
