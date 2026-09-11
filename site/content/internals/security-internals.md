+++
title = "Security Internals"
description = "How Chronix authenticates and authorises: Argon2 API keys, JWT/OIDC with fail-closed algorithm selection, mTLS, Cedar policies, and encryption at rest."
weight = 400
+++

## Security Model

Chronix implements defense-in-depth with four layers:

```text
┌─────────────────────────────────────┐
│         Authentication (AuthN)       │ ← Who are you?
├─────────────────────────────────────┤
│         Authorization (AuthZ)        │ ← What can you do?
├─────────────────────────────────────┤
│         Tenant Isolation             │ ← Data separation
├─────────────────────────────────────┤
│         Audit Logging                │ ← What happened?
└─────────────────────────────────────┘
```

## Authentication

### Token-Based Authentication

Chronix uses **JWT (JSON Web Token)** for API authentication:

```text
Client                          Chronix
  │                                │
  │── POST /auth/login ──────────▸│
  │   { user, password }          │
  │                                │
  │◂── 200 { access_token, ─────  │
  │         refresh_token }        │
  │                                │
  │── GET /query ────────────────▸│
  │   Authorization: Bearer <jwt> │
  │                                │
  │◂── 200 { results } ─────────  │
```

### Token Structure

```text
Header:  { "alg": "HS256", "typ": "JWT" }
Payload: { "sub": "user-123", "ns": "production",
           "roles": ["reader", "writer"],
           "exp": 1705334400 }
Signature: HMAC-SHA256(header + payload, secret)
```

### Key Rotation

Encryption keys rotate under a `RotationPolicy`, whose default `max_age` is
**24 hours**; a policy may also rotate on invocation count, which is what
keeps AES-GCM below its 2^32 limit for a random nonce. During rotation the
previous key stays available for decryption, so data written under it stays
readable.

Chronix does not issue JWTs, so it rotates no signing keys; a JWT issuer's
rotation is its own, and Chronix follows it through JWKS.

### JWKS Cache Staleness

When using an external JWKS endpoint for key discovery, Chronix caches the
fetched key set with a **24-hour maximum stale duration**, fixed rather than
configurable. If the endpoint becomes unreachable, cached keys continue to be
used within that window; once it is exceeded they are discarded and JWT
validation fails closed until the endpoint recovers.

The endpoint is fetched once at startup, so an unreachable issuer or one
publishing no keys fails the start. Accepting the URL and never fetching it —
which is what happened before — produced a server that authenticated nobody
and gave no reason.

### API Key Rate Limiting

API key authentication is protected by a **per-key-prefix** rate limiter to
mitigate brute-force attacks. After **20 failed attempts** within a
**60-second sliding window**, further attempts against that key are rejected
with `AuthError::RateLimited`, and the window resets after 60 seconds without
a failure. A global counter runs alongside it as a backstop against a
distributed attempt. Limiting per prefix rather than per source is what stops
one targeted key from locking out every other one.

## Authorization

### Two layers, no built-in role table

Roles are **names in a policy**, not a fixed set with fixed permissions.
There is no `viewer`/`writer`/`admin` hierarchy.

- **Cedar policies**, when `authz_policy_dir` is configured. A principal
  carries whatever roles its JWT's `role_claim` names, and the policies
  decide. Cedar is default-deny, so a role no policy mentions grants nothing.
- **Capabilities on the credential**, which apply whether or not Cedar is
  configured: the namespaces a credential may act in, and whether it may
  perform administrative operations.

The second layer exists because the first is optional. An authorization
model enforced only when an optional component is present is not an
authorization model.

### Policy evaluation

```text
Request: alice, write, namespace "production"

1. Is alice's credential allowed to act in "production"?   ← always checked
2. If a Cedar policy directory is configured, does a policy
   permit (alice + her roles, Write, production)?          ← default-deny
3. Otherwise: allowed, unless the endpoint is administrative,
   which requires the admin capability.
```

## Tenant Isolation

### Data-level isolation

Every data point carries a **namespace tag**, applied by the one write
function every ingestion surface goes through, and every read is scoped where
the data is reached rather than where the request is parsed.

Segment files are **not** partitioned by namespace on disk: a tenant's
segments sit alongside every other tenant's, named by flush time.
`SegmentPath` carries a `NamespaceId`, but only the object-store tiering
path constructs one and always with the default namespace, so it is not a
second line of defence.

So the tag stops a *request* from crossing tenants and does nothing about
read access to the data directory. Encryption at rest and filesystem
permissions cover that.

### Resource Isolation

Per-tenant quotas prevent noisy-neighbor effects:

| Resource | Quota Type |
|----------|-----------|
| Storage | Maximum bytes per namespace |
| Write rate | Max samples/second |
| Query concurrency | Max parallel queries |
| Retention | Max age of data |

## Audit Logging

All security-relevant events are logged to an immutable audit trail:

| Event Type | Fields Logged |
|------------|--------------|
| `auth.login` | User, IP, success/failure, timestamp |
| `auth.token_refresh` | User, token ID |
| `data.write` | User, namespace, metric count |
| `data.query` | User, namespace, query text, rows returned |
| `admin.role_change` | Operator, target user, old/new roles |
| `admin.namespace_create` | Operator, namespace name |

### Sink Backends

Audit events are dispatched through configurable sinks:

| Sink | Use Case |
|------|----------|
| File | Local development, debugging |
| Stdout | Container logging (stdout → aggregator) |
| Syslog | Enterprise integration |
| Webhook | SIEM integration |

## Transport Security

All network communication uses **TLS 1.3**:

- Client ↔ Server: TLS with certificate validation
- Node ↔ Node: Mutual TLS (mTLS) with cluster CA
- Node ↔ Object Store: TLS (S3/MinIO)

### Certificate Management

Chronix supports:
- Self-signed certificates (development)
- ACME / Let's Encrypt (managed)
- Customer-provided CA (enterprise)

### TLS Certificate Hot-Reload

`spawn_tls_watcher()` polls certificate and key files on a configurable
interval (`reload_interval_secs`) and calls
`RustlsConfig::reload_from_config()` when file modification times
change. This enables **zero-downtime certificate rotation** — new
connections use the updated certificate immediately while existing
connections continue uninterrupted.

Configuration:

```toml
[tls]
cert = "/etc/chronix/tls/server.crt"
key  = "/etc/chronix/tls/server.key"
client_ca = "/etc/chronix/tls/ca.crt"     # optional, enables mTLS
reload_interval_secs = 300                 # poll every 5 minutes
```

Or via CLI: `--tls-reload-interval-secs 300`

The watcher tracks the `mtime` of each file independently and only
triggers a reload when at least one file changes. Reload success and
failure counts are exposed via the `chronix_tls_reloads_total` metric
(labels: `status=success|error`).

## Encryption Key Rotation

The `RotatingKeyProvider` in `chronix_security::auth` provides automated encryption
key lifecycle management. It implements the `KeyProvider` trait and is a
drop-in replacement for `FileKeyProvider` or `EnvKeyProvider`.

### Rotation Triggers

Keys are rotated when either threshold is exceeded:

1. **Time-based** — key age exceeds `max_age` (checked on each `current_key()` call)
2. **Usage-based** — encryption count exceeds `max_encryptions` (tracked via `AtomicU64`)

### Key Lifecycle

```text
[Generate initial key] → [Active key]
                              │
                    (threshold exceeded)
                              │
                    [Generate new key] → [New active key]
                              │
                    [Old key → retained list]
                              │
                    (retention limit exceeded)
                              │
                    [Oldest key zeroized + purged]
```

### Old Key Retention

Previous keys are retained (default: 5) so that data encrypted with older
keys can still be decrypted. The `key_by_id()` method looks up any retained
key by its 8-character hex ID (first 4 bytes of SHA-256). When the retention
limit is exceeded, the oldest key is zeroized via `SecretKey::drop()` and
removed.

### Concurrency Model

- **Read path** (`current_key()`, `key_by_id()`): acquires `RwLock` read lock
- **Write path** (rotation): acquires `RwLock` write lock
- **Usage counter**: `AtomicU64` with `Relaxed` ordering (exact count not required)
- Rotation is triggered lazily on the next `current_key()` call after threshold

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `chronix_auth_key_rotations_total` | Counter | Total number of key rotations |
| `chronix_auth_keys_purged_total` | Counter | Total keys purged beyond retention |
