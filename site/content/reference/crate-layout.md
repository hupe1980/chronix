+++
title = "Crate Layout & Data Model"
description = "How Chronix is split into nine crates, and the data model underneath: series keys, canonical forms, tags versus fields, and time sharding."
weight = 10
+++

## Crate Layout

```text
chronix/
├── crates/
│   ├── chronixd/            # Server daemon (REST, gRPC, Flight SQL, PromQL, OTLP)
│   │   └── src/otel/        #   OTLP trace export & W3C propagation (feature: otlp)
│   ├── chronix/             # Public embedded API (Chronix struct, SQL, PromQL)
│   ├── chronix-core/        # Core data model, schema, config, errors
│   ├── chronix-encoding/    # Column codecs (ALP, Chimp, Gorilla, Patas, DoD, PFOR, …)
│   ├── chronix-engine/      # Storage engine
│   │   ├── wal/             #   Write-Ahead Log
│   │   ├── memtable/        #   Concurrent in-memory write buffer
│   │   ├── segment/         #   Immutable columnar segment files (.csx)
│   │   ├── storage/         #   Pluggable async storage backend
│   │   ├── index/           #   Time index, blooms, tag index, segment catalog
│   │   ├── compaction/      #   TWCS compaction engine (picker, executor)
│   │   ├── cache/           #   LVC, segment cache, metadata cache
│   │   └── objstore/        #   S3/GCS/Azure cold tier (feature: object-store)
│   ├── chronix-query/       # Query planner, pruning, filter, aggregate
│   ├── chronix-analytics/   # Analytics engine
│   │   ├── compute/         #   SIMD compute engine, buffer pool, parallelism
│   │   ├── preprocess/      #   Preprocessing, features, STL decomposition
│   │   ├── forecast/        #   Forecast models (SES → SARIMA → LinReg)
│   │   ├── anomaly/         #   Anomaly detectors (Z-Score → CUSUM → Dynamic)
│   │   ├── multivariate/    #   Correlation, VAR, multivariate anomaly
│   │   ├── lifecycle/       #   Model registry, A/B testing, drift detection
│   │   └── (root)           #   Streaming analytics, alerting, feedback, registry
│   ├── chronix-streaming/   # Reactive layer
│   │   ├── cdc/             #   CDC event bus, subscriptions, aggregations
│   │   └── signal/          #   Programmable triggers, delivery, SQL extensions
│   ├── chronix-security/    # Security layer (feature-gated at the deployment level)
│   │   ├── auth/            #   API key, JWT/OIDC, mTLS, encryption at rest
│   │   ├── authz/           #   Cedar authorization (RBAC + ABAC)
│   │   ├── audit/           #   Tamper-evident audit trail
│   │   └── tenant/          #   Namespaces & quotas
│   ├── chronix-chaos/       # Fault injection (dev-only)
│   ├── chronix-meta/        # FROZEN: Raft cluster metadata (OpenRaft)
│   ├── chronix-cluster/     # FROZEN: DataNode lifecycle, regions, coordination
│   └── chronix-dsim/        # FROZEN: deterministic cluster simulation
├── docs/                    # This directory
├── Cargo.toml               # Workspace manifest
├── rustfmt.toml             # Formatting rules
├── clippy.toml              # Lint configuration
└── deny.toml                # Dependency/license policy
```
## Data Model

### Core Types

| Type          | Description                                              |
|---------------|----------------------------------------------------------|
| `Timestamp`   | `i64` nanosecond-precision Unix epoch                    |
| `FieldValue`  | Enum: `F64`, `I64`, `U64`, `Bool`, `String`              |
| `SeriesKey`   | Measurement name + sorted tag set; FNV-1a hashed         |
| `Point`       | `SeriesKey` + `BTreeMap<String, FieldValue>` + timestamp (encapsulated) |
| `ShardId`     | Time-based partition identifier                          |
| `SegmentId`   | Unique segment identifier within a shard                 |

### Series Identity and the Canonical Form

A `SeriesKey` is identified by its **canonical form**:

```text
measurement \0 key \x01 value \0 key \x01 value        (tags sorted by key)
```

`\0` separates `(key, value)` pairs and `\x01` separates a key from its
value. Both are **reserved** — `SeriesKey::validate_name` rejects them in
measurement names, tag keys and tag values — which makes the encoding
injective. Nothing parses the canonical form back apart from the measurement
prefix, so injectivity is the only property it needs.

**`=` is a legal character in tag keys and values.** InfluxDB Line Protocol
permits it (escaped), and real-world tags carry it: URLs with query strings,
base64 padding, Kubernetes label selectors. An earlier design used `=` as the
key/value separator and therefore had to *forbid* `=` in tag values to stay
unambiguous, which rejected lines InfluxDB accepts. Reserving two control
characters that cannot occur in user data is strictly less restrictive.

The same separators are used by `SeriesKey::hash_fnv`. They must be: hashing
with `=` while `=` is legal inside a value made the byte stream ambiguous, so
`{a: "b=c"}` and `{"a=b": "c"}` — distinct series with distinct canonical
forms — hashed to the same value.

**One definition.** The layout lives in `chronix_core::push_canonical`.
Query-side tombstone matching and the dedup grouper reconstruct canonical
forms from Arrow columns rather than from a `SeriesKey`, and each used to
inline the format; when the separators changed, those copies silently kept
emitting the old layout and tombstone matching broke. They now call the shared
helper. The inverted tag index uses the same reserved separator for its
`key`/`value` index keys, for the same reason.

### Schema-on-Write

Chronix uses a **schema-on-write** approach:

1. The first `Point` for a measurement creates the `MeasurementSchema`
   automatically.
2. Subsequent points may **add** new tags/fields (additive evolution).
3. Changing the type of an existing field produces a `SchemaError::TypeConflict`.

The `SchemaRegistry` is an `Arc<parking_lot::RwLock<HashMap<String, MeasurementSchema>>>`,
making it safe for concurrent access without risk of lock poisoning.
`lookup()` returns `Option<T>`, `register_schema()` returns `()`,
`measurement_count()` returns `usize`, and `measurement_names()` returns
`Vec<String>` — none of these methods can fail with lock-poisoned errors.
## Configuration

All configuration uses the builder pattern:

```rust
let config = ChronixConfig::builder()
    .data_dir("/var/lib/chronix")
    .wal_fsync_policy(FsyncPolicy::PerBatch)
    .wal_max_file_size(32 * 1024 * 1024)
    .wal_max_unflushed(4)
    .build()
    .unwrap();
```

Configuration is also loadable from TOML files via `ChronixConfig::from_toml()`.
Both builder and TOML paths apply the same validation rules (flush threshold,
memory limits, shard duration, retention, concurrency, WAL settings, and
max series cardinality must all be > 0 and internally consistent). Invalid
configurations are rejected with descriptive `ConfigError::Validation` errors.
## Error Handling

Errors are hierarchical. The facade crate's `DbError` composes all sub-crate
errors:

```text
DbError (chronix crate)
├── Core(ChronixError)
│   ├── Wal(WalError)
│   │   ├── Io, Corruption, SequenceSkip
│   │   ├── Full, InvalidHeader, LockPoisoned
│   ├── Encoding(EncodingError)
│   │   ├── InsufficientData, InvalidHeader, InvalidEncoding
│   │   ├── Overflow, DecodeMismatch, Io
│   ├── Segment(SegmentError)
│   │   ├── Io, CorruptFile, InvalidMagic, UnsupportedVersion
│   │   ├── InvalidColumnType, Encoding, Overflow
│   ├── Memtable(MemtableError)
│   │   ├── Frozen, NoFrozenMemtable, Capacity
│   │   ├── ShardOutOfRange, Io
│   ├── Schema(SchemaError)
│   │   ├── EmptyName, NameTooLong, NullByte
│   │   ├── TooManyTags, EmptyFields, TypeConflict
│   │   ├── MeasurementNotFound, LockPoisoned
│   ├── Config(ConfigError)
│   │   ├── MissingField, Validation, Io, Parse
│   ├── Capacity, Io
├── Storage(StorageError)
│   ├── Io, NotFound, InvalidPath
├── Index(IndexError)
│   ├── Io, Corruption, NotFound
├── Query(QueryError)
│   ├── InvalidPlan, InvalidColumn, EmptyBatch
│   ├── UnsupportedType, Execution
├── Closed          — operation on a closed database
├── LockFailed      — exclusive file lock not acquired
├── Internal        — unexpected internal error
└── Io              — direct I/O error
```

All error types use `thiserror` for ergonomic `Display` and `From`
implementations. Library code never panics on lock poisoning — all paths
propagate `LockPoisoned` errors instead.

### Error Handling Principles

- **No silent error swallowing.** All `Result` values are either propagated
  with `?` or logged (`warn!`/`error!`). `let _ = fallible_call()` is
  prohibited — file errors log at `warn!` level (with `NotFound` filtered
  for optional files like bloom sidecars), catalog mutation failures always
  logged.
- **No panics in library code.** Forecast model dispatch,
  segment compression, and query deduplication all return `Result` instead
  of calling `.expect()` or `unreachable!()`.
- **Graceful degradation.** TLS configuration errors log at `error!` and
  skip server startup (no crash). Mutex poisoning returns early rather
  than panicking. Compute failures propagate `ComputeError`.
- **Poison-tolerant mutexes.** Internal `Mutex` locks in buffer pools,
  caches, and monitors use `lock().unwrap_or_else(|e| e.into_inner())`
  to recover data even if a sibling thread panicked while holding the lock.
  This prevents cascading panics across the thread pool.
