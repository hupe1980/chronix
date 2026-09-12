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

`WAL_VERSION = 1`:

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
- **A delete has no WAL record.** Tombstones are catalog state: the WAL is
  truncated once the memtable it covers has been flushed, which is sooner
  than a tombstone has to live. Nothing is lost — `execute_delete` flushes
  first, so every point a tombstone covers is already in a segment and below
  the WAL floor, which replay never reads. Discriminant `0x01` is reserved.
- **PayloadVersion** enables per-type schema evolution (currently `1`).
- The CRC covers `length + sequence_no + record_type + payload_version + payload`.

The reader checks the file version for **equality**. There is one format, so
a file that is not it is refused rather than guessed at.

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
Only version `0x01` is accepted; anything else — a JSON document included,
since `{` is `0x7B` — is refused as `CodecError::UnknownVersion`.

### CRC32c Checksumming

The CRC32c (Castagnoli) polynomial `0x1EDC6F41` was chosen over the
traditional CRC32 (IEEE) for two reasons:

1. **Better error detection** — CRC32c has a minimum Hamming distance of 6
   for messages up to 16 kB, versus 4 for CRC32.
2. **Hardware acceleration** — Intel SSE4.2 and ARM CRC32 extensions provide
   single-instruction CRC32c computation, achieving throughput of ~1 byte per
   clock cycle.

The checksum covers `length + sequence_no + record_type + payload_version +
payload`. The CRC itself is not covered (a corrupted CRC simply fails
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

`FsyncPolicy` controls the trade-off between durability and write latency:

| Policy | Behaviour | Durability |
|--------|-----------|------------|
| `PerWrite` | `fsync` before every append returns | Every acknowledged write is on the device |
| `PerBatch` | `fsync` once per group-commit batch (**default**) | Every acknowledged write is on the device |
| `Periodic(d)` | `fsync` on a timer | Up to `d` of the most recent writes can be lost on power failure |

In TOML, `Periodic` is written `periodic_<ms>` — `wal_fsync_policy =
"periodic_5000"`. The small-footprint preset uses `Periodic(5s)`, because
coalescing syncs is what keeps write amplification off eMMC and SD cards.

`PerWrite` and `PerBatch` both sync **before returning**, which is what makes
the rejection contract hold: a write the caller was told succeeded is durable,
and a write it was told failed is not recovered. `Periodic` states the
opposite explicitly, and that is why an `ENOSPC` rewind discards the bytes
past the last successful sync rather than keeping them.

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

## When a write fails

A failed **write** rewinds: the writer truncates back to the offset the last
successful `fsync` covered and rebuilds its buffer, discarding the records in
between. All of them are unacknowledged by construction — `PerWrite` and
`PerBatch` sync before returning, and `Periodic` promises only the OS's
buffers — so the caller is told the truth, and the next write succeeds as soon
as the condition clears. `ENOSPC` therefore costs no restart.

A failed **`fsync`** poisons the writer, and every later write is refused. The
kernel may discard the dirty pages *and* clear the error, so a retry can
succeed over data that is gone: there is no state to return to. `is_poisoned()`
reports it; recovery is to close and reopen the database, which validates the
existing records first. `clear_poison()` is the in-place version, for an
operator who has verified the WAL replays.

Group commit's watermark is epoch-guarded, so a record discarded by a rewind is
never reported as durable by a later sync that passes its sequence number.

## References

- C. Mohan, D. Haderle, B. Lindsay, H. Pirahesh, P. Schwarz. "ARIES:
  A Transaction Recovery Method Supporting Fine-Granularity Locking and
  Partial Rollbacks Using Write-Ahead Logging." *ACM TODS*, 17(1), 1992.
- M. Stonebraker, L. A. Rowe. "The Design of POSTGRES." *SIGMOD*, 1986.
