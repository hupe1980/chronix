+++
title = "Security Internals"
description = "Authentication, Cedar authorization, the audit trail and multi-tenancy, as they are implemented."
weight = 80
+++

## Authentication (`chronix-security::auth`)

### Architecture

```text
Request → AuthMiddleware → [mTLS → JWT → API Key] → AuthContext
                                                      ├── principal
                                                      ├── method
                                                      ├── claims
                                                      ├── namespaces
                                                      └── admin
```

### API Key Authentication

- Keys stored as Argon2 hashes with optional expiry.
- **Input validation:** key names are trimmed and must be 1–128 characters.
- Runtime key management via REST endpoints:
  - `POST /api/v1/admin/auth/keys` — create new key (validates name)
  - `GET /api/v1/admin/auth/keys` — list keys
  - `DELETE /api/v1/admin/auth/keys/{name}` — revoke key
- Multiple keys valid simultaneously for rotation.
- **Rate limiting:** a sliding-window limiter rejects validation attempts after
  20 failures within a 60-second window, returning `AuthError::RateLimited`.
  Uses lock-free `AtomicU64` counters for zero overhead on successful
  validations.

### JWT/OIDC Authentication

- Validates the HMAC (`HS256`/`384`/`512`), RSA (`RS*`, `PS*`),
  elliptic-curve (`ES256`, `ES384`) and Edwards-curve (`EdDSA`) families.
  **The verification key is chosen by the algorithm's family**, not by
  whichever key field is set: HMAC reads `secret`, the rest read
  `public_key_pem_file`, and pairing them the other way is a startup error
  rather than a token that can never verify.
- **OIDC discovery:** fetches JWKS from `{issuer}/.well-known/openid-configuration`
  with a **10-second HTTP timeout** to prevent hanging on unresponsive providers.
- **JWKS caching:** `JwksCache` stores parsed `DecodingKey`s with configurable TTL.
  Auto-refreshes on expiry via `ensure_fresh()`.
- **Thundering-herd protection:** concurrent callers serialize through an
  `AsyncMutex` with a double-check-after-lock pattern — only the first caller
  performs the HTTP refresh; subsequent callers reuse the just-fetched keys.
- **Stale-on-error:** if a JWKS refresh fails but cached keys are still available,
  the cache continues to serve the stale keys (with a warning log) instead of
  failing open or rejecting all tokens. A **max stale duration** (default 24 h)
  prevents serving indefinitely stale keys — if keys are older than
  `max_stale`, the refresh error is propagated to callers. The 24-hour bound
  is fixed, not configurable.
- **Bearer token extraction** is case-insensitive per RFC 6750 (`Bearer`, `bearer`,
  `BEARER`, etc.).
- `validate_async()` checks keys in order: static key → JWKS cache (by `kid`) →
  try all cached keys.
- Configurable: issuer, audience, JWKS URL, claim mappings (`sub`, `email`,
  custom claims). `jwks_url` is fetched **at startup**, so an unreachable
  issuer or one publishing no keys fails the start rather than every
  subsequent token.
- Two claims are read as capabilities: `namespaces` (an array of strings, or
  a single string) confines the token to those tenants, and `admin` (a
  boolean) permits administrative operations.

### mTLS Authentication

- Server validated with trusted CA certificates.
- Client identity extracted from certificate CN or SAN.
- Maps certificate identity to Chronix principal.
- **Strict mode:** when a client certificate is presented and mTLS is enabled,
  the certificate **must** validate successfully — a failed mTLS check returns
  `AuthError::InvalidCertificate` immediately and **does not** fall through to
  weaker authentication methods (JWT / API key).

### Encryption

Chronix does not encrypt its data directory wholesale, and no configuration
makes it: use filesystem or volume encryption for the data directory and the
bucket's server-side encryption for the cold tier. A named **column** can be
encrypted (below).

- `EncryptionService` — AES-256-GCM with a pluggable `KeyProvider`
  (`FileKeyProvider`, `EnvKeyProvider`, `RotatingKeyProvider`) and key
  rotation: new data uses the newest key, previous keys stay available for
  decryption.
- **Key usage counter** — `EncryptionService` counts invocations in an
  `AtomicU64` and warns at NIST's 2³² threshold, which is when a key has to
  be rotated.
- **Per-column segment encryption** — AES-256-GCM for a `.csx` column's data
  blocks, each block's **column name and segment creation timestamp bound in
  as associated data**, so a block cannot be moved into another's place under
  the same key. Configured by `[database.field_encryption]`, which names a
  column and an **environment variable** rather than a key. Only a field may
  be encrypted; compaction re-encrypts; the export, archive and rollup paths
  refuse. See the
  [security guide](@/docs/security.md#field-level-encryption).

The manifest and every WAL record are protected by **CRC-32C**, which detects
corruption and is not a tamper check. The tamper-evident record is the
[audit chain](#audit-trail-chronix-security-audit), which is HMAC-keyed.

---
## Cedar Authorization (`chronix-security::authz`)

Fine-grained authorization using the [Cedar](https://www.cedarpolicy.com/) policy
language (formally verified pure Rust evaluator).

```text
Request → AuthMiddleware → AuthContext
                               │
                               ▼
                        AuthzEngine (Cedar)
                           ├── PolicySet (hot-reloadable)
                           ├── Entities (principal, resource, roles)
                           └── Decision: Allow | Deny { reasons }
```

- **Default-deny:** No policy match → deny.
- **Entity model:** `Chronix::User`, `Chronix::Role`, `Chronix::Measurement`,
  `Chronix::Action` (Write, Read, Delete, Admin, Forecast, DetectAnomalies,
  Subscribe).
- **Hot-reload:** `load_policies()` replaces the active policy set atomically.
- **Performance:** < 100 µs p99 per decision in release mode for 1 000+ policies.
## Audit Trail (`chronix-security::audit`)

All security-relevant operations are logged via `AuditLogger` with monotonic
sequence numbers. Each audit entry is sealed with a **SHA-256 hash chain** — the
`prev_hash` field contains the hash of the preceding entry, creating a
tamper-evident log. Any modification or deletion breaks the chain, which can be
verified with `verify_hash_chain()`. Sinks are pluggable via the `AuditSink`
trait and can be added at runtime (including after wrapping in `Arc`).

| Sink          | Output                                          |
|---------------|-------------------------------------------------|
| `MemorySink`  | In-memory `Vec` — queryable in tests             |
| `FileSink`    | JSON-lines to an append-only file, `fsync`ed     |
| `WriterSink`  | JSON-lines to any `std::io::Write` (file/stdout) |
| `TracingSink` | Structured `tracing` events                       |

Three properties make the chain worth having, and each was missing:

- **`FileSink` is the durable one.** `WriterSink` over a `File` writes through
  the same handle and never calls `sync_data`, so a power loss takes the tail
  of the log — which is where the interesting events are, since an attacker's
  last act is what crashed the box.
- **The chain continues across restarts.** `last_event()` reads the file's
  last sealed record and `resume_from()` anchors the new logger on it. A chain
  that restarts at `None` each process is indistinguishable, to a verifier,
  from a truncation. `verify_chain_from()` verifies a tail against the link it
  hangs from.
- **`log()` seals with the configured HMAC key**, as `log_strict()` always
  did. Sealing one way in one method and another way in the other produced a
  file that verified under neither, and a bare SHA-256 is recomputable by
  anyone who can write it.
## Multi-Tenancy (`chronix-security::tenant`)

### Namespace Isolation

Each tenant gets an isolated namespace with independent:
- Series, measurements, schemas
- Resource quotas (series count, storage, ingestion rate)

**Data-level isolation is the whole of it.** There is no storage-path
isolation behind it — segments are not partitioned by tenant on disk, so the
tag stops a *request* from crossing tenants and does nothing about read access
to the data directory. `chronixd` injects a hidden `__namespace__` tag into
every written point
and a matching tag filter into every query. That means **every** server query
is a tag-filtered query, which is a code path the embedded API only reaches
when a caller supplies tags — and it is why a defect in tag-filter pruning was
total on the server and intermittent in the library. Segment pruning consults
a series bloom only when the query's tag filters cover every tag in the
measurement's schema; the injected `__namespace__` filter alone never does, so
that level is skipped rather than probed with a key it cannot match.

### Components

| Component | Purpose |
|-----------|---------|
| `NamespaceRegistry` | Thread-safe registry (DashMap) for namespace CRUD, persisted to `namespaces.json`. `chronixd` always opens it from `<data_dir>/namespaces`, and a failure to do so is fatal rather than a silent fall back to an empty in-memory registry — which answered `400` for every non-default tenant after a restart |
| `QuotaEnforcer` | Validates operations against namespace quotas |
| `NamespaceQuota` | Configurable limits: max_series, max_storage_bytes, max_ingestion_rate, max_measurements |
| `NamespaceUsage` | Real-time counters: series_count, storage_bytes, ingestion_rate |

### Quota Enforcement

```text
Write Request → QuotaEnforcer::check_and_increment_write()
                 ├── series_count < max_series?
                 ├── storage_bytes < max_storage_bytes?
                 └── ingestion_rate < max_ingestion_rate?
                      ├── Yes → Atomically increment usage + Allow
                      └── No → TenantError::QuotaExceeded
```

### Namespace Persistence

`NamespaceRegistry::open(dir)` enables durable namespace state:

- **Snapshot file** — `namespaces.json` stores all namespace definitions and
  quotas as a JSON object
- **Atomic writes** — snapshots are written to a `.tmp` file first, then renamed
  to the final path for crash-safe persistence
- **Auto-recovery** — on `open()`, existing snapshot is loaded and all
  namespaces are restored; if no snapshot exists, the default namespace is
  created and an initial snapshot is written
- **Mutation persistence** — `create_namespace()`, `delete_namespace()`, and
  `update_quota()` save a new snapshot after each change. A delete saves
  synchronously and joins any in-flight background snapshot first, so a
  destructive change is durable before the call returns and cannot be undone
  by a stale write landing after it

---
