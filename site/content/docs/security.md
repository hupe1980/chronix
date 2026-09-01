+++
title = "Security"
description = "Authenticate with API keys, JWT/OIDC or mTLS; authorise with Cedar policies; encrypt segments and the WAL at rest; and keep a tamper-evident audit trail."
weight = 60
+++

This guide covers authentication, authorization, Cedar policies, and mTLS
configuration for Chronix.

## Overview

Chronix provides a layered security model:

```text
┌──────────────────────────────────────────────────────┐
│                   Client Request                     │
└───────┬──────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────┐
│  1. Transport Security (mTLS)                        │
│     Client cert validation · TLS 1.3                 │
└───────┬──────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────┐
│  2. Authentication                                   │
│     JWT / API key → Principal identity               │
└───────┬──────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────┐
│  3. Authorization (Cedar)                            │
│     Policy evaluation → permit / forbid              │
└───────┬──────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────┐
│  4. Namespace Isolation                              │
│     Tenant boundary enforcement · quota checks       │
└───────┬──────────────────────────────────────────────┘
        │
┌───────▼──────────────────────────────────────────────┐
│  5. Audit Logging                                    │
│     All access decisions recorded                    │
└──────────────────────────────────────────────────────┘
```

## Authentication

### JWT Authentication

Chronix validates JSON Web Tokens (JWT) for API authentication:

```toml
[auth]
enabled = true
method = "jwt"

[auth.jwt]
issuer = "https://auth.example.com"
audience = "chronix"
jwks_url = "https://auth.example.com/.well-known/jwks.json"
# require_audience = true  # default; set false only if tokens lack aud
```

The JWT `sub` claim maps to the Chronix principal identity. Additional claims
can be mapped to roles via configuration.

**JWKS cache staleness:** When using `jwks_url`, Chronix enforces a maximum
stale duration of **24 hours** (configurable via `jwks_max_stale`). If the
JWKS endpoint is unreachable for longer than this window, cached keys are
discarded and all JWT validations fail-closed until the endpoint recovers.

**Supported algorithms:** HS256, HS384, HS512, RS256, RS384, RS512, ES256,
ES384. Configuration with an unrecognized algorithm string will cause startup
to fail (fail-closed). When no algorithm is specified, HS256 is used as the
default.

**Replay protection (jti):** When a JWT includes a `jti` (JWT ID) claim,
Chronix tracks seen token IDs in a bounded in-memory cache keyed by expiration
time. Tokens with a previously-seen `jti` are rejected with
`AuthError::TokenReplayed`, preventing replay attacks. Expired entries are
evicted automatically on each validation. The cache is bounded at 100K entries;
if the cache is full, new tokens are **rejected** (fail-closed) to prevent
overflow from being exploited to bypass replay protection.
The `jti` claim is optional per RFC 7519 — tokens without it are accepted
normally.

### API Key Authentication

For service-to-service communication:

```toml
[auth]
enabled = true
method = "api_key"

[auth.api_key]
header = "X-API-Key"
# Keys stored in the MetaNode cluster, managed via admin API
```

API key management endpoints (`/api/v1/admin/auth/keys`) require admin-level
authorization and are only accessible through the admin route group.

**Rate limiting:** API key authentication is protected by a per-source rate
limiter. After **20 failed attempts** within a **60-second window**, further
authentication attempts from the same source are rejected with
`AuthError::RateLimited` until the window expires.

**Performance:** API key validation uses a two-stage strategy to prevent
CPU-exhaustion denial-of-service attacks. A SHA-256 prefix index narrows
the candidate set to ~1 entry in O(1) time before running the expensive
Argon2 verification (~100 ms). Without this, an attacker sending invalid
keys would trigger O(N × 100 ms) scans across all stored keys.

**Key entropy:** API keys are generated with **256-bit** (32-byte)
cryptographic randomness via `OsRng`, base64url-encoded. This provides full
cryptographic strength. Keys are hashed with Argon2 before storage.

### Disabling Authentication (Development)

```toml
[auth]
enabled = false
```

**Warning:** Never disable authentication in production.

## Authorization (Cedar)

Chronix uses [Cedar](https://www.cedarpolicy.com/) for fine-grained,
formally verified authorization policies.

**Error safety**: All Cedar entity construction (`to_entity()` for principals,
resources, and namespaces) returns `Result<Entity, AuthzError>`. Construction
failures are logged and result in `Decision::Deny` — the system never panics
on authorization and always defaults to deny.

### Principal Model

```text
Principal hierarchy:
  User("alice")
  ServiceAccount("ingest-pipeline")
  Group("platform-team")
    ├── User("alice")
    └── User("bob")
```

### Action Model

| Action | Description |
|--------|-------------|
| `Action::"Write"` | Write time-series data |
| `Action::"Query"` | Read/query time-series data |
| `Action::"CreateMeasurement"` | Create a new measurement schema |
| `Action::"DeleteMeasurement"` | Delete a measurement |
| `Action::"Admin"` | Administrative operations |
| `Action::"ManageNamespace"` | Create/delete/modify namespaces |
| `Action::"ManageModel"` | Train/delete forecast/anomaly models |

### Resource Model

```text
Resource hierarchy:
  Namespace("team-platform")
    ├── Measurement("cpu")
    ├── Measurement("memory")
    └── Model("cpu-forecast")
```

### Policy Examples

**Allow a team to read/write their namespace:**

```cedar
permit(
  principal in Group::"platform-team",
  action in [Action::"Write", Action::"Query"],
  resource in Namespace::"team-platform"
);
```

**Allow admin operations for specific users:**

```cedar
permit(
  principal == User::"alice",
  action == Action::"Admin",
  resource
);
```

**Deny cross-namespace queries by default:**

```cedar
forbid(
  principal,
  action == Action::"Query",
  resource
) unless {
  resource in principal.namespace
};
```

**Rate-limited write access:**

```cedar
permit(
  principal in Group::"external-ingest",
  action == Action::"Write",
  resource in Namespace::"external"
) when {
  context.points_per_second <= 10000
};
```

### Policy Management

```bash
# Add a Cedar policy
curl -X POST http://chronix:8086/api/v1/admin/policies \
  -H "Content-Type: application/json" \
  -d '{
    "id": "platform-team-access",
    "policy": "permit(principal in Group::\"platform-team\", action in [Action::\"Write\", Action::\"Query\"], resource in Namespace::\"team-platform\");"
  }'

# List all policies
curl http://chronix:8086/api/v1/admin/policies

# Delete a policy
curl -X DELETE http://chronix:8086/api/v1/admin/policies/platform-team-access
```

Policy mutations (add, remove, load from directory) emit structured audit logs
via `tracing::info!` with policy IDs and counts. Metrics counters
(`chronix_authz_policy_load`) track policy operations by type.

### Policy Versioning and Rollback

Every policy mutation (add, remove, load) is automatically versioned.
The authorization engine maintains a ring buffer of the last 10 policy
versions, enabling instant rollback if a bad policy causes an outage.

```rust
// Get current version number
let version = engine.current_version();

// List recent policy versions
let versions = engine.policy_versions(); // Vec<PolicyVersion>

// Revert to a previous version
engine.revert_to_version(3)?;

// Dry-run a policy load without modifying the active set
engine.dryrun_load_policies("./policies/")?;
```

Each `PolicyVersion` records the version number, the complete `PolicySet`
snapshot, a human-readable source label, and a Unix timestamp.

### Schema validation

Cedar schema validation ensures that policies reference valid entity types,
actions, and attributes. Typos in policy definitions are caught at load time
rather than silently failing at authorization time.

```rust
// Loading a schema automatically enables strict validation
let mut engine = AuthzEngine::new();
engine.load_schema(r#"
    entity User;
    entity Measurement;
    action Read appliesTo { principal: User, resource: Measurement };
"#)?;

// This will now FAIL — "Wrte" is not a valid action
engine.add_policy("typo-policy",
    r#"permit(principal, action == Action::"Wrte", resource);"#
)?; // → Err(PolicyValidation(...))
```

**Behavior:**
- `AuthzEngine::new()` starts with `require_schema = false` (built-in
  policies load without a schema)
- Calling `with_schema()` or `load_schema()` auto-enables
  `require_schema = true`
- All subsequent `add_policy()` and `load_policies()` calls are validated
  against the schema
- Use `with_require_schema(false)` to opt out (not recommended)

### Authorization Configuration

```toml
[authz]
enabled = true
engine = "cedar"
default_decision = "deny"

[authz.cedar]
policy_dir = "/etc/chronix/policies/"
# Policies are also stored in MetaNode cluster for replication
```

### Namespace-Level Authorization (Rust API)

The `chronix-security::authz` crate provides dedicated namespace authorization via
`ChronixNamespace` and `AuthzEngine::authorize_namespace()`. The engine
never panics — all Cedar evaluation errors (malformed entities, invalid
requests) produce `Decision::Deny` with error logging, ensuring a
fail-closed security posture. Deny responses return only a generic
"access denied" message; detailed Cedar diagnostics (policy IDs,
evaluation errors) are logged server-side at debug level.

The namespace middleware automatically maps HTTP methods to Cedar actions:
- `GET`, `HEAD`, `OPTIONS` → `Action::"Read"`
- `POST`, `PUT`, `PATCH` → `Action::"Write"`
- `DELETE` → `Action::"Delete"`

### Namespace-Scoped Resource Authorization

Resources can be scoped to a namespace using `with_namespace()`, enabling
Cedar policies that restrict access based on namespace membership:

```rust
use chronix_security::authz::model::{ChronixResource, ChronixAction, ChronixPrincipal};

// Create a resource scoped to a namespace
let resource = ChronixResource::new("cpu")
    .with_namespace("production");

// The Cedar entity hierarchy becomes:
// Chronix::Measurement::"cpu" in Chronix::Namespace::"production"
```

This enables Cedar policies like:

```cedar
// Allow reads only for resources in the "production" namespace
permit(
  principal in Group::"ops-team",
  action == Action::"Query",
  resource in Namespace::"production"
);

// Deny writes to staging namespace
forbid(
  principal,
  action == Action::"Write",
  resource in Namespace::"staging"
) unless {
  principal in Group::"staging-deployers"
};
```

When `authorize()` is called with a namespace-scoped resource, the namespace
entity is automatically included in the Cedar entity set, so `resource in
Namespace::"..."` conditions resolve correctly.

```rust
use chronix_security::authz::{AuthzEngine, ChronixNamespace};
use chronix_security::authz::model::{ChronixAction, ChronixPrincipal};

let engine = AuthzEngine::new();
// ... add policies ...

// Namespace-scoped authorization check
let principal = ChronixPrincipal::new("alice").with_role("platform-admin");
let decision = engine.authorize_namespace(
    &principal,
    ChronixAction::Read,
    &ChronixNamespace::new("team-platform"),
);

if decision.is_allowed() {
    // Proceed with namespace operation
}
```

The `namespace_layer` middleware in `chronixd` automatically performs
this check on every request when the authz engine is configured,
using the `X-Namespace` header value as the target namespace.

### Storage-Layer Tenant Isolation (Defense-in-Depth)

In addition to Cedar policy-based authorization, Chronix enforces tenant
isolation at the **storage layer** itself. Every `SegmentPath` includes a
`namespace: NamespaceId` field that physically separates tenant data on disk
and in object stores:

```
# On-disk layout
data_dir/
  ns_production/
    shard_0/
      segment_001.csx
    shard_1/
      segment_002.csx
  ns_staging/
    shard_0/
      segment_003.csx

# Object store layout
ns_production/shard_0/segment_001.csx
ns_staging/shard_0/segment_003.csx
```

This defense-in-depth design means that even if a Cedar policy is
misconfigured or bypassed, a tenant's queries can only access segments
within their namespace's storage prefix. The `list_segments()` API requires
an explicit `&NamespaceId` parameter, preventing accidental cross-tenant
enumeration.

## Namespace isolation

Multi-tenancy is enforced by a tag stamped on every ingested point and applied
to every read. It is off by default; `multi_tenancy = true` turns on both
halves at once.

```text
write   X-Namespace: tenant-a  ──▶  point.tags["__namespace__"] = "tenant-a"
read    X-Namespace: tenant-a  ──▶  scan filtered to __namespace__ = "tenant-a"
```

Every ingestion surface — REST JSON, Line Protocol, OTLP, Prometheus remote
write, gRPC unary and streaming, Flight SQL `DoPut`, and the Kafka and MQTT
connectors — goes through one write path that applies the tag, so no surface
can forget it. gRPC and Flight SQL read the namespace from `x-namespace`
metadata; connectors take theirs from their own configuration.

Reads are scoped where the data is reached, not where the request is parsed:

| Surface | How it is scoped |
|---------|------------------|
| `/api/v1/query`, `/api/v1/delete` | Namespace filter on the query plan |
| `/api/v1/sql`, Flight SQL, gRPC `SqlQuery` | A session context whose table providers carry a mandatory filter |
| PromQL instant and range queries | An evaluator scoped before any selector runs |
| `/api/v1/prom/labels`, `/label/<name>/values`, `/series`, `/metadata` | Enumerated from the namespace's own data |
| Prometheus remote read | Namespace filter on the query plan |
| `/api/v1/measurements` | Listed from the namespace's own data |

Three properties follow, and each is a test:

- **A client cannot choose its namespace.** A supplied `__namespace__` tag is
  overwritten, not merged.
- **The tag is invisible.** It is absent from SQL schemas — so no SQL text can
  name it — from PromQL label sets, and from schema listings.
- **A scoped read cannot be widened.** The scope lives in the table provider
  and the evaluator, not in the query text.

**Decide before ingesting.** Points written with tenancy off carry no
namespace and are invisible to a scoped read; points written with it on are
not addressable with it off.

**What it does not cover:** measurement *names* are process-wide. A tenant can
learn that another tenant's measurement exists — and reads no rows from it.

## Transport Security (mTLS)

### Generating Certificates

```bash
# Generate CA
openssl req -x509 -newkey rsa:4096 -keyout ca-key.pem -out ca-cert.pem \
  -days 3650 -nodes -subj "/CN=Chronix CA"

# Generate server certificate
openssl req -newkey rsa:4096 -keyout server-key.pem -out server-csr.pem \
  -nodes -subj "/CN=chronix-server"
openssl x509 -req -in server-csr.pem -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -days 365

# Generate client certificate
openssl req -newkey rsa:4096 -keyout client-key.pem -out client-csr.pem \
  -nodes -subj "/CN=chronix-client"
openssl x509 -req -in client-csr.pem -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out client-cert.pem -days 365
```

### Server Configuration

```toml
[tls]
enabled = true
cert_file = "/etc/chronix/certs/server-cert.pem"
key_file = "/etc/chronix/certs/server-key.pem"
ca_file = "/etc/chronix/certs/ca-cert.pem"
require_client_cert = true  # Enforce mTLS
min_version = "1.3"

[mtls]
enabled = true
use_cn = true          # Extract identity from Common Name
use_san_dns = false    # Extract identity from SAN DNS entries
use_san_email = false  # Extract identity from SAN email entries

# Optional: restrict accepted Common Names (allowlist)
allowed_cns = ["chronix-client", "ingest-pipeline", "grafana-reader"]

# Optional: maximum identity string length (default: 256)
max_identity_len = 256
```

> **Note:** At least one of `use_cn`, `use_san_dns`, or `use_san_email` must be
> enabled. Identity extraction methods that are not enabled will be rejected
> even if the certificate contains the corresponding fields.

### Identity Validation

All extracted identities (from CN, SAN DNS, or SAN email) are validated
against the following security rules before acceptance:

- **Non-empty** — empty identity strings are rejected
- **Length limit** — identities exceeding `max_identity_len` (default 256
  characters) are rejected
- **No control characters** — ASCII control characters (0x00–0x1F, 0x7F) are
  rejected to prevent log injection and display attacks
- **No path separators** — identities containing `..`, `/`, or `\` are rejected
  to prevent path traversal via identity values

When `allowed_cns` is configured, only certificates whose Common Name appears
in the allowlist are accepted. This provides defense-in-depth beyond CA trust —
even if a valid CA-signed certificate is presented, it will be rejected unless
its CN is explicitly listed.

**Type-level enforcement:** The `CertificateIdentity` struct has private
fields — instances can only be created through `MtlsValidator` methods that
require TLS-layer inputs. This prevents callers from fabricating identities
via spoofed HTTP headers. Access the verified identity via `principal()` and
`source()` accessor methods.

### Inter-Node TLS

All inter-node gRPC communication (MetaNode ↔ MetaNode, MetaNode ↔ DataNode,
DataNode ↔ DataNode) uses mTLS:

```toml
[cluster.tls]
enabled = true
cert_file = "/etc/chronix/certs/node-cert.pem"
key_file = "/etc/chronix/certs/node-key.pem"
ca_file = "/etc/chronix/certs/ca-cert.pem"
```

### Certificate rotation

Chronix watches certificate files for changes and hot-reloads **all endpoints**
(HTTP, gRPC, and Flight SQL) without restart:

```toml
[tls]
cert_reload_interval_secs = 300  # Check every 5 minutes
```

**Implementation:**
- **HTTP** — `axum-server` reload handle for seamless certificate swap
- **gRPC / Flight SQL** — Custom `ReloadableCertResolver` implementing
  `rustls::server::ResolvesServerCert` with `RwLock`-based atomic cert
  swap. The gRPC endpoints use manual TLS via `tokio-rustls::TlsAcceptor`
  with the shared resolver, so certificate updates take effect for all new
  connections immediately after the watcher detects file changes.
- **mTLS** — Client certificate verification is also reloaded;
  `WebPkiClientVerifier` is rebuilt with the updated CA bundle.

### Certificate Revocation (CRL/OCSP)

Chronix does not embed a CRL or OCSP responder client. Certificate
revocation is treated as an infrastructure-layer concern best handled by
the deployment environment. Recommended approaches:

| Strategy | How to Deploy |
|---|---|
| **Short-lived certificates** | Issue certificates with TTL ≤ 24 h via a CA like Vault PKI or cert-manager. Revocation is implicit at expiry. This is the preferred approach for Kubernetes deployments. |
| **Service mesh sidecar** | Envoy, Linkerd, or Istio sidecars terminate mTLS and can enforce CRL/OCSP on behalf of Chronix. Configure `crl` or `ocsp_staple` in the sidecar's TLS context. |
| **Reverse proxy** | Place NGINX or HAProxy in front of Chronix with `ssl_crl /path/to/crl.pem;` to reject revoked client certificates before they reach Chronix. |
| **Allowed CN list** | Restrict `allowed_cns` in `[mtls]` config (see above). When a certificate is compromised, remove its CN from the allow-list and reload. |

### Connector Credential Rotation

Kafka and MQTT connectors support hot-reload credential rotation via an
external JSON credential file. This avoids embedding secrets in the main
configuration and allows credential updates without restarting the service.

**Kafka SASL authentication:**

```toml
[kafka]
sasl_mechanism = "PLAIN"        # or "SCRAM-SHA-256", "SCRAM-SHA-512"
sasl_username = "kafka-user"    # inline fallback
sasl_password = "kafka-pass"
credential_file = "/run/secrets/kafka-creds.json"
```

**MQTT authentication:**

```toml
[mqtt]
username = "mqtt-user"          # inline fallback
password = "mqtt-pass"
credential_file = "/run/secrets/mqtt-creds.json"
```

**Credential file format:**

```json
{"username": "rotated-user", "password": "rotated-pass"}
```

**Resolution order:**

1. If `credential_file` is set and readable, its `username`/`password` are used.
2. Otherwise, inline `sasl_username`/`sasl_password` (Kafka) or
   `username`/`password` (MQTT) are used as fallback.
3. If neither is configured, the connector runs without authentication.

When the credential file is updated on disk, the connector picks up the new
credentials on the next reconnect cycle — no restart required. This
integrates naturally with secret managers (Vault, AWS Secrets Manager,
Kubernetes projected secrets) that write rotated secrets to the filesystem.

For maximum security, combine short-lived certificates with an allowed-CN
list. This eliminates the window between revocation and CRL propagation
entirely.

### TLS Error Handling

TLS configuration errors (missing/invalid certificates, key mismatch) are
handled gracefully — the server logs an `error!`-level message and skips
binding the affected listener rather than crashing:

```text
ERROR chronixd::cluster: failed to configure raft server TLS: ...
ERROR chronixd::cluster: failed to configure data server TLS: ...
```

This means a misconfigured TLS setup results in a degraded (non-TLS) start
rather than a hard crash, allowing operators to diagnose and fix the issue
without a cold restart. Check logs for `failed to configure` messages when
TLS connections are rejected.

## Audit Logging

All authorization decisions are recorded to an immutable audit log:

```toml
[audit]
enabled = true
log_file = "/var/log/chronix/audit.log"
format = "json"  # or "text"
```

### Audit Log Format

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "principal": "User::alice",
  "action": "Query",
  "resource": "Namespace::team-platform/Measurement::cpu",
  "decision": "permit",
  "policy_id": "platform-team-access",
  "source_ip": "10.0.1.42",
  "duration_us": 150,
  "prev_hash": "a3f2...9b1e"
}
```

### Audit Events

The `AuditAction` enum provides **26 built-in action types** covering all
security-relevant operations:

| Event | Logged When |
|-------|------------|
| `write` | Data write operation |
| `read` | Data read / query |
| `delete` | Data deletion |
| `admin` | Administrative operation |
| `create_rollup` | Rollup rule created |
| `forecast` | Forecast model execution |
| `detect_anomalies` | Anomaly detection run |
| `subscribe` | CDC subscription created |
| `login_success` | Successful authentication |
| `login_failure` | Failed authentication attempt (HTTP + gRPC/Flight) |
| `key_rotation` | Encryption key rotated |
| `token_refresh` | JWT token refreshed |
| `trigger_create` | Signal trigger created |
| `trigger_drop` | Signal trigger dropped |
| `signal_fired` | Composite signal fired |
| `schema_change` | Schema created/altered/dropped |
| `drop_rollup` | Rollup rule dropped |
| `data_export` | Data exported (e.g., Parquet) |
| `permission_change` | Permission or role change |
| `policy_load` | Cedar/authz policy loaded or updated |
| `api_key_create` | API key created |
| `api_key_revoke` | API key revoked |
| `namespace_create` | Namespace created |
| `namespace_delete` | Namespace deleted |
| `quota_change` | Namespace quota changed |
| `custom(…)` | User-defined custom action |

### Hash Chain Integrity

Each audit entry is sealed with a cryptographic hash of the previous entry via
the `prev_hash` field, forming a tamper-evident chain:

- **First entry** — `prev_hash` is a zero hash (all zeros)
- **Subsequent entries** — `prev_hash = Hash(previous_entry)`
- **Verification** — `verify_hash_chain()` walks the log and checks that each
  entry's `prev_hash` matches the computed hash of its predecessor
- **Tamper detection** — any modification, insertion, or deletion of entries
  breaks the chain, making unauthorized changes immediately detectable

#### HMAC-Keyed Hash Chain (SEC-07)

By default, the chain uses plain SHA-256. For production deployments,
`AuditLogger` supports HMAC-SHA256 keyed hashing via `with_hmac_key()`:

```rust
let logger = AuditLogger::new(sink)
    .with_hmac_key(hmac_key_bytes.to_vec());
```

When an HMAC key is set, each chain hash is computed as
`HMAC-SHA256(key, prev_hash || event_data)` instead of plain
`SHA-256(prev_hash || event_data)`. This prevents an attacker with
write-access from recomputing the chain — they would need the secret key.

**Recommendations:**
- Store the HMAC key in a KMS (AWS KMS, HashiCorp Vault, etc.)
- Rotate keys periodically; old entries remain verifiable with the old key
- The key material is protected with `Zeroize` to prevent memory leaks

## Hardening

### Key Material Protection (SecretKey)

All cryptographic key material in `chronix-security::auth` is wrapped in a `SecretKey`
type that automatically zeroizes memory on drop. This prevents sensitive keys
from lingering in process memory after use — mitigating cold-boot attacks and
core-dump key recovery:

- **`SecretKey`** wraps `Vec<u8>` with `impl Drop` that calls `zeroize()`
- **`Debug`** prints `[REDACTED; N bytes]` — never the actual key material
- All `KeyProvider` trait methods return `SecretKey` instead of raw `Vec<u8>`
- Sub-key derivation (`derive_subkey()`) also returns `SecretKey`

```rust
use chronix_security::auth::SecretKey;

// SecretKey is automatically cleared when dropped
let key = SecretKey::new(raw_bytes);
encrypt(&key.as_bytes()); // use the key
drop(key);                // memory zeroed here
```

### Path Traversal Protection

The Parquet export API restricts output to `{data_dir}/exports/`. Only the
filename component of the requested path is used — any directory traversal
attempts (e.g., `../`) are stripped. This prevents attackers from writing
arbitrary files outside the designated export directory.

**Admin API path validation**: The backup, restore, and point-in-time-restore
admin endpoints (`POST /api/v1/admin/backup`, `/restore`, `/pitr_restore`)
validate paths via `validate_admin_path()`, which rejects paths containing
`..` components and enforces absolute paths. This prevents directory traversal
attacks from authenticated administrators.

### Encryption at Rest

Chronix provides transparent encryption at rest via the `EncryptingBackend` storage layer, wrapping any `StorageBackend` implementation with AES-256-GCM authenticated encryption.

**Architecture:**
- **Algorithm:** AES-256-GCM with random 96-bit nonces
- **Scope:** Per-object encryption — each stored blob gets a unique nonce
- **Wire format:** `[12-byte nonce][ciphertext][16-byte GCM tag]`
- **Key source:** Derives from the existing `EncryptionService` in `chronix-security::auth`
- **Backend agnostic:** Works with local filesystem, S3, GCS, or any custom `StorageBackend`

**Data flow:**
1. `put(path, data)` → generate random nonce → AES-256-GCM encrypt → store `nonce || ciphertext || tag`
2. `get(path)` → read blob → split nonce/ciphertext/tag → AES-256-GCM decrypt → return plaintext
3. `get_range(path, range)` → full-object decrypt then slice (range queries require full decryption)

**Guarantees:**
- Authenticated encryption prevents both tampering and information disclosure
- Each object uses a unique random nonce — no nonce reuse across objects
- `list()`, `exists()`, and `delete()` pass through unmodified (no encryption needed)
- Zero-copy where possible; encryption/decryption happens in-memory

**Compliance:** Meets HIPAA, SOC 2, and PCI-DSS encryption-at-rest requirements when combined with proper key management.

### Field-Level Encryption

Beyond full-object encryption, Chronix supports **per-column AES-256-GCM encryption** (`FieldEncryptionConfig`). Individual columns can be encrypted while the rest of the segment remains in plaintext. This allows fine-grained control over which data is protected.

**How it works:**
1. During writes, columns listed in `FieldEncryptionConfig` are encrypted per-block with random nonces.
2. During reads, the `FieldKeyProvider` resolves key IDs to key material for decryption.
3. Key IDs are stored alongside each encrypted block, enabling seamless key rotation.

**Statistics suppression (security/performance tradeoff):**

Encrypted columns have their zone-map statistics (min, max, sum, distinct count) and bloom filters **deliberately zeroed**. This prevents information leakage — publishing plaintext statistics would reveal data about the encrypted values.

> **⚠️ Performance impact:** Without zone-map statistics, predicate pushdown cannot prune row groups based on encrypted columns. Queries that filter primarily by an encrypted column will scan and decrypt every matching segment, causing read amplification proportional to the selectivity loss.

**Best practices to minimize impact:**
1. **Filter on plaintext columns first** — time range and non-sensitive tag filters still benefit from pruning
2. **Encrypt only sensitive fields** — keep frequently-filtered columns in plaintext (e.g. `host`, `region`) and encrypt payload fields (e.g. `patient_id`, `ssn`)
3. **Partition by sensitive dimension** — if you must filter on an encrypted tag, use it as a partition key so each segment has a single value

**Example configuration:**
```rust
use chronix_engine::segment::{FieldEncryptionConfig, FieldEncryptionKey};

let mut config = FieldEncryptionConfig::new();
config.encrypt_column("patient_id", FieldEncryptionKey::new("key-1", key_bytes));
config.encrypt_column("diagnosis",  FieldEncryptionKey::new("key-1", key_bytes));
// host, region, timestamp remain in plaintext for efficient filtering
```

### Automated Secret Rotation

The `RotatingKeyProvider` in `chronix-security::auth` automates encryption key lifecycle management:

- **Time-based rotation** — keys rotate after a configurable `max_age` (e.g., 24 hours)
- **Usage-based rotation** — keys rotate after `max_encryptions` operations
- **Old key retention** — configurable number of previous keys kept for decrypting historical data
- **Audit trail** — bounded log of `RotationEvent` entries with reason, key IDs, and timestamps
- **Metrics** — `chronix_auth_key_rotations_total` and `chronix_auth_keys_purged_total`
- **Migration** — `RotatingKeyProvider::with_initial_key()` for transitioning from static key providers

See the [Security Guide](@/docs/security.md#automated-secret-rotation) for configuration examples.

### Authentication Path Normalization

The authentication middleware normalizes request paths before checking
exemption rules, preventing path-traversal bypass attacks:

- `//` sequences are collapsed to `/`
- `.` and `..` segments are resolved
- Query strings are stripped before path matching
- Exempt paths require exact match or segment-boundary prefix match
  (e.g., `/health` exempts `/health` and `/health/live` but not
  `/healthz` or `/healthcheck`)

This prevents attackers from bypassing authentication by using paths like
`/api/../health/../api/v1/write` or `//health`.

### mTLS Identity Hardening

Client certificate identities extracted via mTLS are validated against
strict security rules (see [Identity Validation](#identity-validation)):

- **Strict mode (no fallthrough):** when a client presents a certificate and
  mTLS is enabled, identity extraction **must** succeed — failures return
  `AuthError::InvalidCertificate` immediately without falling through to
  weaker JWT / API-key authentication
- CN allowlist enforcement restricts accepted certificates beyond CA trust
- Path separator rejection prevents identity-based traversal attacks
- Control character rejection prevents log injection
- Length limits prevent resource exhaustion via oversized identity strings

### Read-Only SQL Admission Control

Every network-facing SQL surface — HTTP `/api/v1/sql`, gRPC `ExecuteSql`, and
Flight SQL `DoGet` — admits **only pure read queries**. The check lives in one
place, `chronix::sql::readonly`, and all three handlers call it.

Rejected: DDL, DML, `COPY`, `Statement` plans (`SET`, `PREPARE`, `BEGIN`),
`EXPLAIN`, `ANALYZE` and `DESCRIBE`. The first four can mutate state; the last
three are side-effect-free but disclose internal plan shape, catalog layout and
statistics.

Two properties make this sound:

1. **Verification happens before execution.** `SessionContext::sql()` is
   `sql_with_options(sql, SQLOptions::new())` with everything permitted — it
   plans *and then executes*, applying DDL and `Statement` side effects
   eagerly. Inspecting the returned `DataFrame`'s plan is therefore too late.
   Because `chronixd` shares one `Arc<SessionContext>` across all requests,
   tenants and namespaces, a caller holding nothing but query access could run
   `SET datafusion.catalog.information_schema = true` and permanently re-enable
   the catalog introspection that `create_session_context` deliberately
   disables — process-wide. `CREATE EXTERNAL TABLE … LOCATION '<path>'`
   likewise reached the object store before rejection, and returned
   distinguishable errors for existing versus missing paths: a filesystem
   existence oracle.

   Admission now runs against the logical plan produced by
   `create_logical_plan()`, which applies nothing, before
   `execute_logical_plan()` is ever called.

2. **The whole plan tree is checked, including subqueries.**
   `SQLOptions::verify_plan` uses `visit_with_subqueries`, so a mutation nested
   inside a subquery cannot slip past a check that only looked at the root
   node.

The HTTP surface additionally caches logical plans per `(namespace, sql)`.
Only verified read-only plans are admitted to that cache, and cache hits are
re-verified before execution.

### SQL Query Resource Limits

Both the REST and gRPC query interfaces enforce configurable limits to prevent
resource exhaustion:

| Setting | Default | Description |
|---------|---------|-------------|
| `sql_query_timeout_secs` | `30` | Maximum seconds for SQL query execution including result streaming |
| `prom_query_timeout_secs` | `30` | Maximum seconds for PromQL query execution (0 = disabled) |
| `sql_max_rows` | `100000` | Maximum rows returned by a single SQL query |
| `prom_series_limit` | `10000` | Maximum label-sets returned by `/api/v1/prom/series` |

Queries exceeding the timeout are cancelled; queries exceeding the row limit
return a truncated result set with a warning header.

### Write Timeout (DoS Protection)

The `write_timeout_secs` setting (default: 30 s) limits how long any write
operation may run. This prevents slow or malicious clients from holding server
resources indefinitely. All 7 write paths (HTTP, gRPC, Flight SQL, Prometheus
remote write, OTLP) are covered. Zero disables the timeout (not recommended
in production).

### 5xx error redaction

Server errors (HTTP 5xx) return a generic error message to clients. Full error
details — including stack context and internal state — are logged server-side
only. This mitigates CWE-209 (Generation of Error Message Containing Sensitive
Information) and prevents leaking implementation details to potential attackers.

### Object Store Range Overflow Protection

The `get_range()` implementation in `chronix-engine::objstore` uses `checked_add()` for
the offset + length calculation, preventing integer overflow that could lead to
reading unintended memory regions or triggering panics.

### SQL Identifier Validation

SQL trigger names and other user-supplied identifiers are validated to prevent
injection attacks. Only alphanumeric characters and underscores (`[a-zA-Z0-9_]`)
are accepted. Names containing SQL metacharacters (quotes, semicolons,
parentheses, etc.) are rejected at parse time with a descriptive error.

### CARGO_MANIFEST_DIR Guard

References to `CARGO_MANIFEST_DIR` (used for locating test fixtures) are
guarded with `#[cfg(debug_assertions)]` so they are compiled out of release
builds. This prevents leaking the build machine’s filesystem layout in
production binaries.

### ForecastResidualDetector Deserialization Validation

The `ForecastResidualDetector` validates field constraints during
deserialization (e.g., from JSON config). Invalid configurations — such as
negative thresholds, zero window sizes, or NaN values — are rejected with
structured errors rather than silently producing incorrect detection results.
This prevents misconfigured detectors from entering the model catalog.

## Best Practices

### Production Checklist

- [ ] Enable authentication (`auth.enabled = true`)
- [ ] Enable Cedar authorization (`authz.enabled = true`)
- [ ] Enable mTLS for client connections (`tls.require_client_cert = true`)
- [ ] Enable inter-node TLS (`cluster.tls.enabled = true`)
- [ ] Enable audit logging (`audit.enabled = true`)
- [ ] Use dedicated CA for Chronix certificates
- [ ] Configure `allowed_cns` for mTLS to restrict accepted client certificates
- [ ] Rotate certificates before expiry
- [ ] Set `default_decision = "deny"` for authorization
- [ ] Define least-privilege Cedar policies per team/service
- [ ] Monitor `chronix_auth_failures_total` for brute-force attempts
- [ ] Separate admin credentials from application credentials
- [ ] Use environment variables or secrets manager for sensitive config
- [ ] Enable encryption at rest (`storage.encryption.enabled = true`) with proper key management
- [ ] Verify JWT `jti` replay protection is active if tokens include `jti` claims
- [ ] Confirm webhook URLs use HTTPS (SSRF protection enabled by default)
- [ ] Use `${ENV_VAR}` syntax for webhook `auth_header` — never embed literal tokens in config
- [ ] Review audit event coverage (26 built-in action types)

### Namespace Security

- Each team should have a dedicated namespace
- Cross-namespace access should be explicitly denied by default
- Quota limits prevent resource exhaustion attacks
- Monitor `chronix_namespace_usage_ratio` for approaching limits

### Network Security

- Deploy MetaNodes in a private network segment
- Use network policies to restrict inter-node communication
- Expose only the client-facing port (8086) to application traffic
- Use a load balancer with TLS termination for external access

---

## See Also

- [Operations Guide](@/docs/operations.md) — deployment modes, configuration
- [Cluster Operations](@/docs/cluster.md) — inter-node mTLS, namespace isolation
- [Architecture Reference](@/reference/_index.md) — authentication and authorization internals
- [Guide](@/docs/_index.md)

---

## Encryption Key Rotation

The `EnvKeyProvider` supports online key rotation with a configurable
grace period (default 5 seconds). During rotation the old key is cached
briefly so in-flight operations do not fail.

```rust
use chronix_security::auth::encryption::EnvKeyProvider;
use std::time::Duration;

let provider = EnvKeyProvider::new("CHRONIX_DATA_KEY", "key-2")
    .with_grace_period(Duration::from_secs(2));

// After swapping the env var to the new key:
provider.clear_cache(); // discard the old key immediately
```

- The `chronix.auth.env_key.grace_hit` counter is emitted every time a
  cached key is served, enabling alerts for prolonged rotation windows.

## Custom Model Source Verification

Custom forecast/anomaly model registrations (`chronix-analytics::registry`)
can be protected with an SHA-256 hash allowlist. When the allowlist is
active, only registrations whose `sha256_hash` matches are accepted:

```rust
use chronix_analytics::registry::{PluginInfo, PluginRegistry};

let reg = PluginRegistry::new();

// Enable allowlist — only trusted hashes accepted
reg.set_model_allowlist(vec![
    "a1b2c3d4e5f678...".to_string(),
]);

// Registration includes the hash
let info = PluginInfo::new("my_model")
    .with_sha256("a1b2c3d4e5f678...");
reg.register_model_with_info(info, factory)?;
```

## Webhook Signing

All webhook deliveries are HMAC-SHA256 signed. The `signing_secret` is a
mandatory parameter — unsigned webhooks are not permitted.

```rust
use chronix_streaming::signal::WebhookConfig;

let config = WebhookConfig::new(
    "https://alerts.example.com/webhook",
    "my-signing-secret",  // required
);
```

Receivers verify the `X-Chronix-Signature` header:

```text
X-Chronix-Signature: sha256=<hex HMAC-SHA256 of the JSON body>
```

### Webhook Auth Header — Environment Variable Expansion

The `auth_header` field on `WebhookSink` supports `${VAR_NAME}` syntax so
that bearer tokens and API keys never appear as plaintext in configuration
files or source control:

```rust
use chronix_security::audit::WebhookSink;

// At sink creation the placeholder is resolved from the process environment.
// If the variable is unset, construction returns an error.
let sink = WebhookSink::new(
    "https://siem.example.com/ingest",
    "Bearer ${SIEM_TOKEN}",   // resolved at startup
    "my-signing-secret",
);
```

Rules:

- **Patterns** — `${VAR}` is expanded; literal strings without `${…}` are
  passed through unchanged.
- **Multiple variables** — `Token ${PART_A}:${PART_B}` works.
- **Missing variable** — returns an error at construction time rather than
  silently sending an empty header.
- **Unclosed brace** — `${VAR` (no closing `}`) is treated as an error.

> **Best practice:** Store webhook credentials in a secrets manager and
> inject them as environment variables at deploy time. Never commit literal
> tokens.

### Webhook URL SSRF Protection

Webhook URLs in signal trigger `DELIVER` clauses are validated to prevent
Server-Side Request Forgery (SSRF):

- **HTTPS required** — webhook URLs must use `https://` (HTTP is only
  permitted for `localhost`/`127.0.0.1`/`::1` during development)
- **Private IP blocking** — resolved IP addresses in RFC 1918 ranges
  (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), loopback
  (`127.0.0.0/8`), and link-local (`169.254.0.0/16`) are rejected
  (with an exception for localhost)
- **Scheme validation** — only `https` and `http` schemes are accepted;
  `file://`, `ftp://`, etc. are blocked

Invalid webhook URLs are rejected at SQL parse time with a descriptive error.

## Dashboard HTTPS Enforcement

`ChronixDataSource::new()` requires an `https://` URL. For local
development only, use `ChronixDataSource::new_insecure()` which
accepts any scheme but logs a security warning.
