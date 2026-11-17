+++
title = "Secure a server"
description = "Turn on authentication, TLS and mTLS, API keys, Cedar policies and namespace isolation for chronixd, with a production checklist."
weight = 80
aliases = ["/docs/security/"]
+++

`chronixd` with no security section accepts every request — right for
`--demo` on loopback, wrong anywhere else. This guide turns on TLS,
authentication, authorization and tenant isolation, then lists what to check
before production. Every option is in the [security reference](@/reference/security.md).


{% <callout kind="warning" title="Before a multi-tenant deployment"> %}
Each of these is permissive when omitted, so none announces itself:

- **`namespaces` on every API key**, or a `namespaces` claim on every JWT.
  A credential naming none may act in every tenant; `chronixd` refuses to
  start with `multi_tenancy = true` and an unconfined key.
- **`admin` on exactly the keys that need it.** Restore, namespace
  management and key management require it.
- **An `[audit]` section.** Without one the audit trail lives only in the
  process log and does not survive a restart.

A namespace is a **tag**, not a storage boundary: segments are not
partitioned by tenant on disk, so it stops a request from crossing tenants
and does nothing about read access to the data directory.
{% </callout> %}

## How the layers fit

A request passes four checks, in order, and each is configured below:

1. **Transport** — TLS, and with a client CA, mutual TLS.
2. **Authentication** — an API key, a JWT (or OIDC), or a client certificate.
3. **Authorization** — a Cedar policy decides the action on the route.
4. **Namespace scope** — the request reads and writes only its tenant's series.

Every decision can be written to a tamper-evident [audit log](@/reference/security.md#audit-logging).

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
admin = false                 # the administrative *capability*; policies decide which
roles = ["ingest-service"]    # Chronix::Role memberships, for Cedar policies
```

`roles` is what makes a policy written `principal in Chronix::Role::"…"`
apply to this key. Without it the only way to name the key in a policy is
`principal == Chronix::User::"ingest"`.

`key` takes three forms: `$VAR` or `${VAR}` reads the value from the
environment at startup, a string beginning `$argon2` is a pre-hashed PHC
string loaded as-is, and anything else is plain text hashed with Argon2 on
load. Prefer the first, so a key never sits in a config file.

Keys can also be minted at runtime through `/api/v1/admin/auth/keys`, which
requires an administrative credential. Under multi-tenancy the request must
name the namespaces the new key may act in.

**Runtime key changes are durable.** `[[auth.api_keys]]` is a *declaration*
and is re-read on every start, so both halves of runtime key management used
to last exactly as long as the process: a minted key — shown once, in the
creation response, and unrecoverable — stopped working at the next restart,
and a **revocation was undone by one**. Chronix keeps the record in
`<data_dir>/auth/api_keys.json`: Argon2 hashes for keys the API created, and
names for keys it revoked, applied *after* the config so a revocation wins
over a declaration. Revoking a configured key therefore takes effect
immediately and stays, without editing the config and redeploying during an
incident.

Both operations are written before they are acknowledged, and roll back if
the write fails — a revocation the caller was told succeeded and the disk did
not take is the failure the record exists to remove. Remove the key from
`[[auth.api_keys]]` as well when convenient; the entry in the record is what
keeps it revoked until you do.

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

## Best Practices

### Production Checklist

There is no `enabled` flag on any of these: a section is configured or it is
absent, and absent means off. Naming a phantom flag is how a checklist gets
ticked without the control being on.

- [ ] Enable authentication — set `[auth] api_keys` and/or `[auth] jwt`
- [ ] Enable Cedar authorization — point `authz_policy_dir` at a directory of
      `.cedar` files. With it unset, every authenticated request is permitted.
      Policies are validated against the compiled-in schema at startup, so a
      typo is a refusal to start rather than a rule that never fires
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
- [ ] Grant administrative credentials **one capability each** —
      `action == Chronix::Action::"ManageBackups"` rather than
      `action in Chronix::Action::"Admin"` — and give each its own key
- [ ] Monitor `chronix_auth_failures_total` for brute-force attempts, and
      `chronix_authz_denied_total` and `chronix_namespace_denied_total` for a
      credential reaching for something it is not allowed. All three are
      published **at zero** at startup, so an alert on them can be tested
      before the incident rather than during it
- [ ] Separate admin credentials from application credentials
- [ ] Use environment variables or secrets manager for sensitive config
- [ ] Encrypt the data directory at the filesystem or volume layer, and the cold-tier bucket with its own server-side encryption
- [ ] Declare `[database.field_encryption]` for columns whose key must not live on the machine, and inject the key from a secrets manager
- [ ] Verify JWT `jti` replay protection is active if tokens include `jti` claims
- [ ] Confirm webhook URLs use HTTPS (enforced; the SSRF address rule is on by default and `triggers.webhook_allow_private_targets` is the named way off)
- [ ] Use `$VAR` syntax for connector credentials, and `CHRONIX_WEBHOOK_SIGNING_SECRET` for the webhook signing key — never embed literal secrets in config
- [ ] Review audit event coverage — every category the server declares is one it emits, checked by `every_audit_action_has_a_producer`; the list read "26 built-in action types" while nineteen of them were constructed by nothing

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
