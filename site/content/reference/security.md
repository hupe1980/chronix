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
                                                      └── claims
```

### API Key Authentication

- Keys stored as Argon2 hashes with optional expiry.
- **Input validation:** key names are trimmed and must be 1–128 characters.
- Runtime key management via REST endpoints:
  - `POST /api/v1/auth/keys` — create new key (validates name)
  - `GET /api/v1/auth/keys` — list keys
  - `DELETE /api/v1/auth/keys/{name}` — revoke key
- Multiple keys valid simultaneously for rotation.
- **Rate limiting:** a sliding-window limiter rejects validation attempts after
  20 failures within a 60-second window, returning `AuthError::RateLimited`.
  Uses lock-free `AtomicU64` counters for zero overhead on successful
  validations.

### JWT/OIDC Authentication

- Validates RS256/HS256/ES256 JWT tokens.
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
  `max_stale`, the refresh error is propagated to callers.
- **Bearer token extraction** is case-insensitive per RFC 6750 (`Bearer`, `bearer`,
  `BEARER`, etc.).
- `validate_async()` checks keys in order: static key → JWKS cache (by `kid`) →
  try all cached keys.
- Configurable: issuer, audience, JWKS URL, claim mappings (`sub`, `email`,
  custom claims).

### mTLS Authentication

- Server validated with trusted CA certificates.
- Client identity extracted from certificate CN or SAN.
- Maps certificate identity to Chronix principal.
- **Strict mode:** when a client certificate is presented and mTLS is enabled,
  the certificate **must** validate successfully — a failed mTLS check returns
  `AuthError::InvalidCertificate` immediately and **does not** fall through to
  weaker authentication methods (JWT / API key).

### Encryption at Rest

- AES-256-GCM authenticated encryption for segment data blocks.
- Per-entry nonce for WAL encryption.
- HMAC-SHA256 manifest integrity verification.
- Key rotation: new segments use latest key; old segments readable with previous keys.
- Pluggable `KeyProvider` trait: `FileKeyProvider`, `EnvKeyProvider`, `KmsKeyProvider`.
- **Key usage counter:** `EncryptionService` tracks invocations via `AtomicU64`. Logs `tracing::warn!` at NIST's 2³² threshold, signalling time for key rotation.

The `EncryptingBackend` wraps any `StorageBackend` with transparent AES-256-GCM authenticated encryption. Each stored object receives a unique random 96-bit nonce. Wire format: `[12-byte nonce][ciphertext][16-byte GCM tag]`. Keys are derived from `EncryptionService` in `chronix-security::auth`.

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
| `WriterSink`  | JSON-lines to any `std::io::Write` (file/stdout) |
| `TracingSink` | Structured `tracing` events                       |
## Multi-Tenancy (`chronix-security::tenant`)

### Namespace Isolation

Each tenant gets an isolated namespace with independent:
- Series, measurements, schemas
- Resource quotas (series count, storage, ingestion rate)

**Data-level isolation** sits behind the storage-path isolation as defence in
depth: `chronixd` injects a hidden `__namespace__` tag into every written point
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
| `NamespaceRegistry` | Thread-safe registry (DashMap) for namespace CRUD; optionally persisted to `namespaces.json` via `open(dir)` |
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
  `update_quota()` automatically save a new snapshot after each change

---
