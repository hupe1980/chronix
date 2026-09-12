+++
title = "Security"
description = "Authenticate with API keys, JWT/OIDC or mTLS; authorise with Cedar policies; scope every request to a namespace by credential; and keep a tamper-evident audit trail."
weight = 60
+++

This guide covers authentication, authorization, Cedar policies, and mTLS
configuration for Chronix.


> ### Before a multi-tenant deployment
>
> Each of these is permissive when omitted, so none announces itself:
>
> - **`namespaces` on every API key**, or a `namespaces` claim on every JWT.
>   A credential naming none may act in every tenant; `chronixd` refuses to
>   start with `multi_tenancy = true` and an unconfined key.
> - **`admin` on exactly the keys that need it.** Restore, namespace
>   management and key management require it.
> - **An `[audit]` section.** Without one the audit trail lives only in the
>   process log and does not survive a restart.
>
> A namespace is a **tag**, not a storage boundary: segments are not
> partitioned by tenant on disk, so it stops a request from crossing tenants
> and does nothing about read access to the data directory.

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

Authentication is on when the config has an `[auth]` section and off when it
does not. There is no `enabled` flag and no `method` selector: every
configured provider is tried, strongest first, so a deployment can accept
mTLS, JWTs and API keys at once. An `[auth]` section that configures neither
a key nor a JWT is a startup error, because it would otherwise start
successfully and reject every request.

Credentials arrive as `Authorization: Bearer <token>` in all cases. Chronix
tells a JWT from an API key by structure — two or more dots means a JWT, and
a JWT that fails validation is **never** retried as an API key, which would
be an authentication downgrade.

### JWT Authentication

```toml
[auth.jwt]
algorithm = "RS256"
issuer = "https://auth.example.com"
audience = "chronix"
public_key_pem_file = "/etc/chronix/jwt-public.pem"
# jwks_url = "https://auth.example.com/.well-known/jwks.json"
```

The JWT `sub` claim maps to the Chronix principal identity. Roles come from
`role_claim` (dot-separated paths are supported, e.g. `realm_access.roles`
for Keycloak). Two further claims are read directly: `namespaces` (an array
of strings, or a single string) confines the token to those tenants, and
`admin` (a boolean) grants administrative operations.

**Key material follows the algorithm's family.** `HS256`/`HS384`/`HS512`
read `secret`; `RS*`, `PS*`, `ES256`, `ES384` and `EdDSA` read
`public_key_pem_file`; `jwks_url` replaces the file for providers that
publish their own keys, and is fetched once at startup, so an unreachable
issuer fails the start rather than every subsequent token. Pairing an
algorithm with the wrong family's material is a startup error.

**Supported algorithms:** HS256, HS384, HS512, RS256, RS384, RS512, PS256,
PS384, PS512, ES256, ES384, EdDSA. An unrecognised algorithm string fails the
start. With none specified, HS256 is the default.

**Replay protection (jti):** When a JWT includes a `jti` claim, Chronix
tracks seen token IDs in a bounded cache keyed by expiration time and rejects
a repeat with `AuthError::TokenReplayed`. Expired entries are evicted on each
validation. The cache holds 100K entries; when full, new tokens are
**rejected** rather than admitted, so overflowing it cannot bypass replay
protection. The claim is optional per RFC 7519; tokens without it are
accepted normally.

### API Key Authentication

For service-to-service communication:

```toml
[[auth.api_keys]]
name = "ingest"
key = "$CHRONIX_INGEST_KEY"   # env-var reference, Argon2 PHC string, or plain text
namespaces = ["tenant-a"]     # tenants this key may act in
admin = false                 # restore, namespace and key management
```

`key` takes three forms: `$VAR` or `${VAR}` reads the value from the
environment at startup, a string beginning `$argon2` is a pre-hashed PHC
string loaded as-is, and anything else is plain text hashed with Argon2 on
load. Prefer the first, so a key never sits in a config file.

Keys can also be minted at runtime through `/api/v1/admin/auth/keys`, which
requires an administrative credential. Under multi-tenancy the request must
name the namespaces the new key may act in.

**`namespaces` and `admin` both fail open when omitted**, which is why each
has a guard. A key naming no namespaces reads every tenant, so a server with
`multi_tenancy = true` refuses to start while one exists. A key without
`admin` cannot reach the administrative endpoints.

**Rate limiting:** API key authentication is protected by a per-prefix rate
limiter. After **20 failed attempts** within a **60-second window**, further
attempts against that key are rejected with `AuthError::RateLimited` until
the window expires. The limit is per prefix rather than global, so
brute-forcing one key cannot lock out unrelated ones.

**Performance:** validation uses a two-stage strategy to prevent
CPU-exhaustion denial of service. A SHA-256 prefix index narrows the
candidate set to ~1 entry in O(1) before running the expensive Argon2
verification (~100 ms). Without it, an attacker sending invalid keys would
trigger O(N × 100 ms) scans across every stored key.

**Key entropy:** keys are generated with **256-bit** randomness via `OsRng`,
base64url-encoded, and hashed with Argon2 before storage.

### Running without authentication

Omit the `[auth]` section. Every endpoint is then open, which is right for a
laptop and wrong for anything reachable by anyone else.

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
# Cedar authorization is on when a policy directory is set, and off when it
# is not. There is no `enabled` flag: an empty directory would be
# default-deny and refuse everything.
authz_policy_dir = "/etc/chronix/policies/"
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

## Namespace isolation

Multi-tenancy is enforced by a tag stamped on every ingested point and applied
to every read. It is off by default; `multi_tenancy = true` turns on both
halves at once.

**It is a tag, not a storage boundary.** Segments are not partitioned by
tenant on disk, so the tag stops a request from crossing tenants and does
nothing about read access to the data directory. Encrypt at rest and
restrict directory permissions if that is in your threat model.

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
| `/api/v1/chronix/query`, `/api/v1/delete` | Namespace filter on the query plan |
| `/api/v1/chronix/sql`, Flight SQL, gRPC `SqlQuery` | A session context whose table providers carry a mandatory filter, **and whose catalog lists only this namespace's measurements** |
| PromQL instant and range queries | An evaluator scoped before any selector runs |
| `/api/v1/labels`, `/label/<name>/values`, `/series`, `/metadata` | Enumerated from the namespace's own data |
| Prometheus remote read | Namespace filter on the query plan |
| `/api/v1/measurements` | Listed from the namespace's own data |

Three properties follow, and each is a test:

- **A client cannot choose its namespace.** A supplied `__namespace__` tag is
  overwritten, not merged.
- **The tag is invisible.** It is absent from SQL schemas — so no SQL text can
  name it — from PromQL label sets, and from schema listings.
- **A scoped read cannot be widened.** The scope lives in the table provider
  and the evaluator, not in the query text.
- **A name that exists elsewhere fails exactly as one that does not.** The
  SQL catalog answers from a per-namespace index, so
  `SELECT * FROM another_tenants_measurement` is "table not found" rather
  than an empty result — the difference between those two is an oracle for
  enumerating every other tenant's measurement names, and it is what kept
  `information_schema` (and therefore `SHOW TABLES`) switched off.

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
cert = "/etc/chronix/certs/server-cert.pem"
key = "/etc/chronix/certs/server-key.pem"

# Setting a client CA turns on mTLS: a client certificate is then required
# and verified against it. Identity comes from the certificate's CN.
client_ca = "/etc/chronix/certs/ca-cert.pem"
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
cert = "/etc/chronix/certs/node-cert.pem"
key = "/etc/chronix/certs/node-key.pem"
ca_cert = "/etc/chronix/certs/ca-cert.pem"
```

### Certificate rotation

Chronix watches certificate files for changes and hot-reloads **all endpoints**
(HTTP, gRPC, and Flight SQL) without restart. It is **on by default** — a
`[tls]` section you did not tune polls once a minute:

```toml
[tls]
reload_interval_secs = 60  # the default; 0 disables polling
```

A certificate that fails to parse — one caught half-written — leaves the
running configuration in place, counts
`chronix_tls_reloads_total{status="error"}` and retries on the next tick, so
reloading cannot cause the outage it prevents.

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
path = "/var/log/chronix/audit.jsonl"   # JSON lines, append-only, fsynced
hmac_key_env = "CHRONIX_AUDIT_KEY"      # names the env var holding the key
sync_each = true
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

**Admin API path validation**: the backup and restore endpoints
(`POST /api/v1/admin/backup`, `/api/v1/admin/restore`) resolve every
caller-supplied path inside `backup_root` — a canonicalised directory,
`<data_dir>/backups` unless configured otherwise. Canonicalising the root is
what makes it confinement: rejecting a `..` in the string accepts any
absolute path, and it also misses a symlink planted inside the root, which
contains no `..` at all.

### Encryption at Rest

Chronix does not encrypt its data directory wholesale, and no setting makes
it. Use full-disk or filesystem encryption (LUKS, dm-crypt, FileVault, an
encrypted ZFS dataset) for the data directory, and the bucket's own
server-side encryption for the cold tier. That covers segments, WAL, catalog,
temporary files, core dumps and swap.

**Per-column encryption** (below) is what Chronix adds on top: a named column
encrypted with a key that is not on the machine, so a stolen disk or backup
is unreadable while ordinary queries keep working.

### Field-Level Encryption

A named column's data blocks are encrypted with AES-256-GCM in the `.csx`
segment format; the rest of the segment stays in plaintext.

```toml
[database.field_encryption]
columns = { patient_id = "phi-2026", diagnosis = "phi-2026" }
keys    = { phi-2026 = "CHRONIX_FIELD_KEY_PHI" }
```

`columns` maps a column to a key id. `keys` maps a key id to the **name of an
environment variable** holding the base64 of 32 bytes — the configuration file
holds no key material, so inject it from a secrets manager and a stolen disk
or backup stays unreadable. Column names are global, not per measurement.

**What it covers:**

| | |
|---|---|
| `.csx` segments, and every backup taken from them | Encrypted |
| Compaction output | Encrypted — the merge re-encrypts |
| The WAL | Plaintext, until the memtable it covers is flushed and it is truncated |
| The memtable | Plaintext, in memory |
| Query results | Plaintext — the server holds the key |

It protects the long-lived copy. It is not a substitute for authorisation and
does not hide data from the running server.

**Only a field can be encrypted.** A tag is part of the series key, which is
written in plaintext in the segment's `.series` sidecar, the inverted tag
index and its bloom filter. Declaring a tag — or the time column — is a write
error naming the column.

**An encrypted column does not leave the segment format.** A Parquet export, a
cold-tier archive and a rollup over the measurement are each refused, naming
the column. Project the column away, or drop the declaration.

**Statistics are suppressed** for an encrypted column — min, max, sum,
distinct count and bloom filters are zeroed. Predicate pushdown cannot prune
on it, so filter on plaintext columns (time range, `host`, `region`).

**Rotation** is a second key id: declare both, and segments written under the
old one stay readable. Every declared key must resolve at startup, including
one no column names. A missing variable, a value that is not base64, or one
that is not 32 bytes stops the server with the variable named and the value
never printed.

Each block carries a random 96-bit nonce, with the column name and the
segment's creation timestamp bound in as associated data.

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

Every network-facing SQL surface — HTTP `/api/v1/chronix/sql`, gRPC `ExecuteSql`, and
Flight SQL `DoGet` — admits **only pure read queries**. The check lives in one
place, `chronix::sql::readonly`, and all three handlers call it.

Rejected: DDL, DML, `COPY` and `Statement` plans (`SET`, `PREPARE`,
`BEGIN`) — everything that can mutate state.

`EXPLAIN`, `EXPLAIN ANALYZE` and `DESCRIBE` are **permitted**. They were
refused as information disclosure, which cost the only way to find out
whether a time filter pruned — the one question worth asking about a query
against a time-series database — and disclosed nothing: the scan node's
display is the caller's own query echoed back (a measurement name, a time
range, a filter count, a limit), and under multi-tenancy the catalog it
names is already scoped to the caller's namespace.

What they wrap is held to the same rules. That distinction matters:
`verify_plan` inspects the node it is handed, and an `Explain` around an
`Insert` is not a DML node, so verifying the wrapper would have made
`EXPLAIN ANALYZE INSERT` — which executes — a write with a fig leaf.

Two properties make this sound:

1. **Verification happens before execution.** `SessionContext::sql()` is
   `sql_with_options(sql, SQLOptions::new())` with everything permitted — it
   plans *and then executes*, applying DDL and `Statement` side effects
   eagerly. Inspecting the returned `DataFrame`'s plan is therefore too late.
   Because `chronixd` shares one `Arc<SessionContext>` across all requests,
   tenants and namespaces, a caller holding nothing but query access could run
   `SET` and permanently change execution settings for every other tenant
   — process-wide. `CREATE EXTERNAL TABLE … LOCATION '<path>'`
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
| `prom_series_limit` | `10000` | Maximum label-sets returned by `/api/v1/series` |

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

There is no `enabled` flag on any of these: a section is configured or it is
absent, and absent means off. Naming a phantom flag is how a checklist gets
ticked without the control being on.

- [ ] Enable authentication — set `[auth] api_keys` and/or `[auth] jwt`
- [ ] Enable Cedar authorization — point `authz_policy_dir` at a directory of
      `.cedar` files. With it unset, every authenticated request is permitted
- [ ] Enable TLS — set `[tls] cert` and `[tls] key`
- [ ] Enable mTLS for client connections — set `[tls] client_ca`
- [ ] Enable inter-node TLS (cluster builds) — set `[cluster.tls] ca_cert`,
      `cert` and `key`
- [ ] Enable audit logging — set `[audit] path`, and `hmac_key_env` so the
      chain is keyed rather than a recomputable hash
- [ ] Use a dedicated CA for Chronix certificates
- [ ] Rotate certificates before expiry — `[tls] reload_interval_secs` picks
      up a new pair without a restart
- [ ] Write least-privilege Cedar policies per team or service; a request
      matching no policy is denied
- [ ] Define least-privilege Cedar policies per team/service
- [ ] Monitor `chronix_auth_failures_total` for brute-force attempts
- [ ] Separate admin credentials from application credentials
- [ ] Use environment variables or secrets manager for sensitive config
- [ ] Encrypt the data directory at the filesystem or volume layer, and the cold-tier bucket with its own server-side encryption
- [ ] Declare `[database.field_encryption]` for columns whose key must not live on the machine, and inject the key from a secrets manager
- [ ] Verify JWT `jti` replay protection is active if tokens include `jti` claims
- [ ] Confirm webhook URLs use HTTPS (enforced; the SSRF address rule is on by default and `triggers.webhook_allow_private_targets` is the named way off)
- [ ] Use `$VAR` syntax for connector credentials, and `CHRONIX_WEBHOOK_SIGNING_SECRET` for the webhook signing key — never embed literal secrets in config
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

Every webhook delivery is a [CloudEvents](https://cloudevents.io) 1.0
structured-mode JSON envelope, signed per the
[Standard Webhooks](https://www.standardwebhooks.com) `v1` scheme. At least
one signing secret is mandatory — unsigned webhooks are not permitted.

```rust
use chronix_streaming::signal::WebhookConfig;

let config = WebhookConfig::new(
    "https://alerts.example.com/webhook",
    "my-signing-secret",  // required
);

// During a rotation, sign with both — newest first.
let rotating = WebhookConfig::with_secrets(
    "https://alerts.example.com/webhook",
    vec!["new-secret".into(), "old-secret".into()],
);
```

The body carries the fired [`SignalEvent`] under `data`, so anything already
parsing that shape keeps working; `type`, `source`, `id` and `time` are what
make it recognisable to a CloudEvents-aware receiver with no code specific to
chronix:

```json
{
  "specversion": "1.0",
  "id": "3f9c1e2a-...",
  "source": "chronix:trigger/high_cpu",
  "type": "io.chronix.signal.fired",
  "time": "2026-01-15T10:30:00.123456789+00:00",
  "datacontenttype": "application/json",
  "data": { "trigger_id": "high_cpu", "measurement": "cpu", "value": 92.5, "..." : "..." }
}
```

Receivers verify the `webhook-signature` header, which — unlike a signature
over the body alone — also binds the request's age, so a captured request
cannot be replayed indefinitely:

```text
webhook-id: msg_3f9c1e2a5b6d4e8f9a0b1c2d3e4f5a6b
webhook-timestamp: 1768473000
webhook-signature: v1,base64(HMAC-SHA256(secret, "{webhook-id}.{webhook-timestamp}.{body}"))
```

Any [Standard Webhooks reference library](https://github.com/standard-webhooks/standard-webhooks/tree/main/libraries)
verifies this without any code specific to chronix.

### Rotating a signing secret

A delivery is signed with every configured secret and carries one `v1,<sig>`
per secret in the space-delimited `webhook-signature` header, so a receiver
holding any one of them verifies.

```toml
[triggers]
# Newest first. Both are sent; either verifies.
webhook_signing_secrets = ["$CHRONIX_WEBHOOK_KEY_NEW", "$CHRONIX_WEBHOOK_KEY_OLD"]
```

1. add the new secret at the front and restart;
2. roll it out to the receivers;
3. drop the old one.

`CHRONIX_WEBHOOK_SIGNING_SECRET` sets the whole list, comma-separated.

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

A URL the database fetches is a request-forgery primitive pointed at your own
network, so it is checked twice.

**At `CREATE TRIGGER`** — where the URL may come from a tenant:

- **HTTPS only.** A payload carries the alert's contents and its HMAC
  signature; there is no localhost exception.
- **No userinfo.** `https://evil.com@169.254.169.254/` is refused — credentials
  belong in a header.
- **No internal host name.** `localhost`, and anything under `.localhost`,
  `.local`, `.internal`, `.home.arpa` or `.onion`.
- **No non-routable IP literal.** Loopback, RFC1918, link-local (the cloud
  metadata service), carrier-grade NAT `100.64.0.0/10`, multicast, broadcast
  and reserved space — IPv4 and IPv6, including IPv4-mapped, IPv4-compatible
  and 6to4 forms.

URLs are parsed with the same WHATWG parser the HTTP client uses, so the host
the check sees is the host the request reaches — including decimal, octal and
hexadecimal IPv4 literals, and bracketed IPv6.

**At the connection**, in `WebhookChannel`: the host is resolved, **every**
resolved address is checked, and the client is pinned to exactly those
addresses. Checking a name and then letting the client resolve it again would
leave DNS rebinding open.

`WebhookConfig::allow_private_targets` waives the address rule for an operator
pointing a webhook at their own network. Off by default; the trigger DSL never
sets it.

## Dashboard HTTPS Enforcement

`ChronixDataSource::new()` requires an `https://` URL. For local
development only, use `ChronixDataSource::new_insecure()` which
accepts any scheme but logs a security warning.
