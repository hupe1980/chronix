+++
title = "Security reference"
description = "Cedar actions and policy schema, the audit log format, hardening settings, key rotation, webhook signing and how each control is enforced."
weight = 90
+++

The reference for every security control in `chronixd`: Cedar actions and policies, the audit log, hardening settings, key rotation, webhook signing and transport details. For a step-by-step setup, start with [Secure a server](@/guides/secure-a-server.md).

## Authorization (Cedar)

Chronix authorizes with [Cedar](https://www.cedarpolicy.com/), a formally
verified policy language. Two decisions are put to it, and the model contains
exactly those two:

| Decision | Resource | Actions |
|---|---|---|
| May this principal touch this tenant's data? | `Chronix::Namespace` | `Read`, `Write`, `Delete` |
| May this principal administer the server? | `Chronix::System` | the capabilities in the `Admin` group |

**There is no `Measurement` resource.** The unit of authorization is the
namespace, because that is the unit everything else uses: points carry a
`__namespace__` tag, SQL sessions are built per namespace, quotas are counted
per namespace, and a credential is bound to a set of them. A finer unit that
only the policy layer understood would have to be remembered by each of forty
routes, and the ones that forgot would read as protected.

### The schema is compiled in, and every policy is validated against it

`chronixd` loads [`chronix.cedarschema`][schema] at startup and validates
every policy against it. A rule naming an action or an entity type this
server never asks about is a **startup error**, not a rule that quietly never
fires — which is the failure this whole mechanism exists to prevent, and the
one this guide itself shipped for several releases.

[schema]: https://github.com/hupe1980/chronix/blob/main/crates/chronix-security/src/authz/chronix.cedarschema

Read the schema for the authoritative list of entity types and actions. Two
things to know before writing a policy:

* **Everything is namespaced under `Chronix::`.** `Action::"Read"` is a
  different type from `Chronix::Action::"Read"` and matches nothing.
* **Roles are `Chronix::Role`, not groups.** A principal is in a role
  because its credential says so — `roles = [...]` on an API key, or the
  JWT claim named by `auth.jwt.role_claim`.

### Principals

```text
Chronix::User::"alice"                  a JWT subject, an API key name, a cert CN
  └── in Chronix::Role::"platform-team" from the credential's role list
```

### Policy examples

**A team reads and writes its own namespace:**

```cedar
permit(
  principal in Chronix::Role::"platform-team",
  action in [Chronix::Action::"Read", Chronix::Action::"Write"],
  resource == Chronix::Namespace::"team-platform"
);
```

**An ingest key writes, and only writes, and only there:**

```cedar
permit(
  principal == Chronix::User::"telegraf",
  action == Chronix::Action::"Write",
  resource == Chronix::Namespace::"metrics"
);
```

**Nobody deletes production except the on-call role:**

```cedar
forbid(
  principal,
  action == Chronix::Action::"Delete",
  resource == Chronix::Namespace::"production"
) unless {
  principal in Chronix::Role::"oncall"
};
```

**Every administrative capability at once** — `Admin` is an action *group*,
so this is `in`, not `==`:

```cedar
permit(
  principal in Chronix::Role::"root",
  action in Chronix::Action::"Admin",
  resource
);
```

**One capability, which is the point of having twelve:**

```cedar
permit(
  principal == Chronix::User::"backup-runner",
  action == Chronix::Action::"ManageBackups",
  resource
);
```

That credential can take, verify and restore backups. It cannot mint an API
key, change the log level or delete a namespace — each of those is a separate
capability asked for by the route group that performs it.

### Two gates, and both have to pass

An administrative request needs **both** the capability on its credential
(`admin = true` on the key, or `"admin": true` in the token) **and** a policy
permitting the specific action. Adding policies can therefore only ever
*narrow* what a credential can do — a permissive file in `authz_policy_dir`
never widens it.

Data requests need the credential's namespace binding — which is what keeps
tenants apart, and is enforced whether or not Cedar is configured — and then
the policy.

### What the method asks for

The data gate maps the HTTP method, and every gRPC and Flight SQL RPC names
its action directly:

| Method | Action |
|---|---|
| `GET`, `HEAD`, `OPTIONS` | `Read` |
| `POST`, `PUT`, `PATCH` | `Write` |
| `DELETE` | `Delete` |

Health, readiness, the metrics scrape, the OpenAPI document and the
administrative routes are outside the data gate. The first four carry no
tenant rows, and a liveness probe that fails because a policy file changed is
an outage a policy file should not be able to cause. The administrative
routes are outside it because an administrative request is not a request *in*
a namespace — it is governed by its capability on `Chronix::System`, and
applying both would mean a key bound to `tenant-a` also needed `Write` on
`default` before it could take a backup. Everything else refuses when no
policy permits it — `every_route_refuses_under_a_deny_all_policy` walks the
router and checks it.

### Turning it on

```toml
[server]
# Cedar authorization is on when a policy directory is set, and off when it
# is not. There is no `enabled` flag: an empty directory would be
# default-deny and refuse everything.
authz_policy_dir = "/etc/chronix/policies/"
```

Every `.cedar` file in the directory is loaded as one policy set, in filename
order. An `[auth]` section is required: a policy names a principal, and
without authentication every request is anonymous.

Policies are loaded at startup. There is no API to add, remove or roll them
back at run time — the files are the source of truth, and a deployment that
needs a change restarts or reloads with a new directory.

### Cross-tenant isolation is not a policy

It is a property of the credential. A key or token carries the namespaces it
may act in, and a request naming any other is refused before Cedar is
consulted — so an operator cannot forget to deploy the rule that keeps
tenants apart. `chronixd` refuses to start multi-tenant with an unconfined
key.

### Failure behaviour

Cedar evaluation never panics. Entity-construction failures, malformed
requests and evaluation errors all produce a denial, logged server-side;
the caller is told `403` and the action that was refused, never a policy id
or an evaluation trace.

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

Every category below is one `chronixd` **emits**. That is a recent property
and worth stating plainly: the enum had twenty-five variants and the server
constructed four, so this table listed `namespace_delete`, `data_export`,
`policy_load`, `api_key_create` and fifteen others as coverage while nothing
produced them. An absent event is what an auditor reads as *it did not
happen*, so a category nothing emits is worse than a missing one.
`every_audit_action_has_a_producer` walks the enum and fails on the next.

| Event | Logged when | Emitted by |
|-------|------------|-----------|
| `write` | A data write | the embedded API — `chronixd` does not log one event per request |
| `read` | A data read | the embedded API, as above |
| `delete` | A delete, the one operation nothing can undo | `POST /api/v1/delete`, `delete_batch`, a measurement drop |
| `schema_change` | A column declared — permanent, and shared between tenants | `POST /api/v1/measurements/{name}/schema/fields` |
| `data_export` | Data leaving the database | `POST /api/v1/export/parquet` |
| `login_failure` | A rejected credential | HTTP, gRPC and Flight SQL |
| `admin` | An administrative operation, **and every administrative refusal** | backup, restore, the log level, and the capability gate |
| `api_key_create` | A key minted at runtime | `POST /api/v1/admin/auth/keys` |
| `api_key_revoke` | A key revoked | `DELETE /api/v1/admin/auth/keys/{name}` |
| `policy_load` | The Cedar policy set the server is enforcing | startup, with every policy id |
| `namespace_create` | A tenant created | `POST /api/v1/namespaces` |
| `namespace_delete` | A tenant deleted — every credential's route to its data | `DELETE /api/v1/namespaces/{name}` |
| `quota_change` | A namespace quota changed | the namespace API |
| `create_rollup` / `drop_rollup` | A rollup rule created or dropped | `/api/v1/rollups` |
| `trigger_create` / `trigger_drop` | A trigger created or dropped — a trigger sends data somewhere | `/api/v1/triggers` |
| `signal_fired` | A signal fired | the pipeline |
| `custom(…)` | Anything an embedded consumer wants to record | the embedded API |

**There is deliberately no `login_success`.** A database authenticates every
request; recording the successes would make the trail a second request log
and bury the events it exists for.

### Hash Chain Integrity

Each audit entry is sealed with a cryptographic hash of the previous entry via
the `prev_hash` field, forming a tamper-evident chain:

- **First entry** — `prev_hash` is a zero hash (all zeros)
- **Subsequent entries** — `prev_hash = Hash(previous_entry)`
- **Verification** — `verify_hash_chain()` walks the log and checks that each
  entry's `prev_hash` matches the computed hash of its predecessor
- **Tamper detection** — any modification, insertion, or deletion of entries
  breaks the chain, making unauthorized changes immediately detectable

**A broken chain means tampering, and nothing else.** That is the whole value
of the mechanism, so it has no benign way to fail:

- Sealing an event and writing it are **one step**, so concurrent requests
  cannot seal in one order and write in the other.
- An event that reaches **no durable sink** does not advance the chain. The
  gap is counted by `chronix_audit_chain_gaps_total` and logged at `error`,
  because a lost audit event is worth an alert of its own.
- A line is written **whole or not at all**, so a failed write cannot swallow
  the event after it.

A check that can fail for a benign reason is a check whose failures mean
nothing; alert on `verify_hash_chain` only once its benign failures are
gone.

#### HMAC-keyed hash chain

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

| Control | What it does |
|---|---|
| Key material | Keys live in `SecretKey`, zeroised on drop and printed as `[REDACTED; N bytes]` |
| Path confinement | Parquet exports go to `<data_dir>/exports/` (only the file name of a request is used); backup and restore paths resolve inside a canonicalised `backup_root`, so neither `..` nor a planted symlink escapes it |
| Path normalisation | Before auth exemptions are matched, `//`, `.` and `..` are resolved and the query string dropped; `/health` exempts `/health/live` but not `/healthcheck` |
| mTLS identity | A presented certificate must yield a valid identity — no fall-through to a weaker credential; identities with control characters, path separators or over 256 characters are refused; `allowed_cns` narrows trust beyond the CA |
| Read-only SQL | HTTP SQL, gRPC `ExecuteSql` and Flight SQL `DoGet` admit only reads, checked in one place, including the plan inside `EXPLAIN` |
| Query limits | Per-query memory (`per_query_memory_limit`), result size (`sql_max_rows`), timeouts, and the shared `max_concurrent_queries` slots |
| Write deadline | `write_timeout_secs` bounds how long a request waits (504, outcome unknown — the write is not abandoned) |
| Error redaction | Internal 5xx errors return a generic message and are logged in full server-side; deployment conditions (disk full, overload) are shown, since each has a remedy |
| Identifiers | In the trigger language, names are identifiers (`[A-Za-z_][A-Za-z0-9_]*`) unless quoted, so a name cannot smuggle syntax; measurement, tag and field names may not contain the reserved `\0` and `\x01` separators |
| Webhook targets | `https` only, no userinfo, no internal or non-routable addresses — see [below](#webhook-url-ssrf-protection) |

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
| The WAL | Sealed per key (AES-256-GCM) from the append onwards — no plaintext window before the flush |
| The memtable | Plaintext, in memory |
| Query results | Plaintext — the server holds the key |

It protects every copy on disk. It is not a substitute for authorisation and
does not hide data from the running server.

**After a crash the key is needed to open.** A write-ahead log record holds an
encrypted column's values sealed under that column's key, so reopening without
the key — or with the wrong key bytes — fails and names the key, rather than
replaying the points without their values and losing part of an acknowledged
write.

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

`RotatingKeyProvider` rotates the data-encryption key on age (`max_age`, 24 h
by default) or on use (`max_encryptions`, which keeps AES-GCM inside its
random-nonce limit). Previous keys are kept (five by default) so older blocks
stay readable; the oldest is zeroised when it leaves. Each rotation is
recorded, and `chronix_auth_key_rotations_total` and
`chronix_auth_keys_purged_total` count them. Moving from a static key is
`RotatingKeyProvider::with_initial_key()`.

`EnvKeyProvider` reads the key from an environment variable and keeps the
previous one for a short grace period (5 s by default) while it is swapped, so
in-flight operations finish; `chronix_auth_env_key_grace_hit` counts reads
served from the grace copy.

### Custom model allowlist

`PluginRegistry::set_model_allowlist` restricts custom forecasting and
detection models to registrations whose SHA-256 is listed.

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
  "time": "2026-01-15T10:30:00.123456789Z",
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

## How it is enforced

The request path — authentication, the two authorization gates, namespace
scoping, quotas, column encryption and the audit chain — is explained in
[Security internals](@/explanation/security.md).

## Transport security details

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

### TLS errors

A certificate, key or CA that does not load stops `chronixd` at start with the
reason. The server never falls back to plaintext.

### Connector Credential Rotation

Set `credential_file` in `[kafka]` or `[mqtt]` to the path of a JSON file:

```json
{"username": "connector", "password": "..."}
```

The connector reads the file at startup. Replace the file and restart the
connector to rotate credentials. An unreadable or invalid file fails startup.

For Kafka, set `security_protocol = "SASL_SSL"` and choose a SASL mechanism.
TLS uses the bundled WebPKI roots by default. An explicit `ca_cert` PEM bundle
replaces those roots. See the [connector settings](@/reference/configuration.md#connectors).
