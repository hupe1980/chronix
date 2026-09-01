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

Signing keys are rotated on a configurable schedule (default: 90 days).
During rotation, both old and new keys are accepted for a grace period.

### JWKS Cache Staleness

When using an external JWKS endpoint for key discovery, Chronix caches
the fetched key set and enforces a **maximum stale duration of 24 hours**
(configurable via `jwks_max_stale`). If the JWKS endpoint becomes
unreachable, cached keys continue to be used within this window. Once
the stale duration is exceeded, cached keys are discarded and all JWT
validations fail-closed until the endpoint recovers.

### API Key Rate Limiting

API key authentication is protected by a per-source rate limiter to
mitigate brute-force attacks. After **20 failed authentication attempts**
within a **60-second sliding window**, subsequent attempts from the same
source are immediately rejected with `AuthError::RateLimited`. The
window resets after 60 seconds of no failures.

## Authorization

### Role-Based Access Control (RBAC)

Permissions are grouped into roles:

| Role | Read | Write | Admin | Query | Manage |
|------|------|-------|-------|-------|--------|
| `viewer` | ✓ | ✗ | ✗ | ✓ | ✗ |
| `writer` | ✓ | ✓ | ✗ | ✓ | ✗ |
| `analyst` | ✓ | ✗ | ✗ | ✓ | ✗ |
| `admin` | ✓ | ✓ | ✓ | ✓ | ✓ |

### Namespace-Scoped Roles

Roles are scoped to **namespaces** (tenants). A user can be `admin`
in namespace `development` but only `viewer` in `production`:

```text
user: alice
  ├── development: admin
  ├── staging: writer
  └── production: viewer
```

### Policy Evaluation

```text
Request: { user: "alice", action: "write", namespace: "production" }

1. Lookup user's roles for namespace "production" → ["viewer"]
2. Check if any role grants "write" permission → No
3. Decision: DENY
```

## Tenant Isolation

### Data-Level Isolation

Every data point is tagged with a **namespace ID**. The storage layer
enforces isolation:

- Segment files are partitioned by namespace
- Queries are automatically scoped to the authenticated namespace
- Cross-namespace access requires explicit `admin` permission

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

The `TlsWatcher` polls certificate and key files on a configurable
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
