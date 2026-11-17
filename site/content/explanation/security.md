+++
title = "Security internals"
description = "How Chronix enforces security: request authentication, Cedar evaluation per route, namespace scoping, column encryption and the audit chain."
weight = 180
aliases = ["/internals/security-internals/"]
+++

Security in `chronixd` is enforced in layers on every request: TLS on the
connection, authentication of the caller, authorization of the action on the
namespace, then namespace scoping where the data is reached. Chosen columns are
encrypted at rest, in segments and in the write-ahead log, and every decision is
recorded in a tamper-evident audit chain. In the embedded library all of this
sits behind the `security` feature; `chronixd` always has it. How to set it up
is in [Secure a server](@/guides/secure-a-server.md); every setting is in the
[security reference](@/reference/security.md).

```text
request ──▸ TLS ──▸ authenticate ──▸ credential's namespaces ──▸ Cedar (if configured) ──▸ quotas ──▸ engine (scoped to the namespace)
                                                                                                  └──▸ audit chain
```

## Authentication

| Credential | How it is checked |
|---|---|
| API key | Argon2 hash; carries its namespaces, its roles and an `admin` flag |
| JWT | signature against a secret (HS256/384/512) or a public key (RS, PS, ES); claims map to roles and namespaces |
| JWKS | the issuer's key set is fetched at start — an unreachable issuer fails the start — and cached; keys older than 24 h without a successful refresh are dropped and validation fails closed |
| mTLS | a client certificate from the configured CA, optionally restricted to listed names |

Chronix validates tokens; it does not issue them. A failing API key is limited
per key prefix — 20 failures in 60 s and that key is refused for the window —
with a global counter as a backstop against a distributed attempt, so one
targeted key cannot lock out the others.

## Authorization

Two layers, both of which must allow a request:

1. **The credential's capabilities**, always: the namespaces it may act in,
   and whether it may perform administrative actions.
2. **Cedar policies**, when `authz_policy_dir` is set: the principal, its
   roles, the action (`Read`, `Write`, `ManageBackups`, …) and the resource
   (`Namespace::"production"`). Cedar denies by default.

Configuring Cedar can therefore only narrow access, never widen it. Roles are
names that policies give meaning to; there is no built-in role table.

- **Every surface:** HTTP derives the action from the route, and each gRPC and
  Flight SQL call names its own. MCP tools map to the action of the route they
  mirror. A route without a classification is refused, and a test walks the
  whole router to keep it that way.
- **Validated:** the entity types and actions are a Cedar schema compiled into
  the binary; a policy naming something the server never asks about is a
  startup error, not a rule that silently never fires.
- **Never on probes:** health, readiness, the metrics scrape and the OpenAPI
  document are outside Cedar, so a policy file cannot fail a liveness check.

## Tenants

A namespace is applied on the one write path every surface uses, and every
read is scoped where data is reached — SQL catalogs, PromQL selectors,
discovery endpoints and error suggestions only ever see the caller's own
measurements.

| Quota | Enforced |
|---|---|
| `max_series`, `max_measurements` | at admission, against the exact live series set the global budget uses |
| `max_ingestion_rate` (points/s) | per batch, before the write |
| `max_request_rps`, `max_request_burst` | on HTTP, gRPC and Flight SQL |

A series or measurement quota is refused with 400 (retrying cannot help); a
rate limit with 429 and `Retry-After`. Zero means unlimited.

Namespaces share segment files on disk. The namespace stops a *request* from
crossing tenants; it does nothing for someone with read access to the data
directory — encryption at rest and filesystem permissions cover that.

## Encryption at rest

Fields listed under `[database.field_encryption]` are encrypted with
AES-256-GCM per column block, and the same values are **sealed in the
write-ahead log**, so no copy of them sits on disk in the clear. Validity
bitmaps stay unencrypted — they reveal which rows have values, never what the
values are — which keeps `/labels` exact without a key. A database opened
without the key refuses to read the encrypted columns and names the key.

Keys come from a file, the environment or a rotating provider. Rotation is by
age (24 h by default) or by number of encryptions, which keeps AES-GCM within
its random-nonce limit; previous keys are kept (five by default) so older data
stays readable, and the oldest is zeroised when it leaves.

## The audit chain

Writes, deletes, reads, administrative changes, policy loads, failed logins
and quota changes are recorded as audit events. Each event carries the SHA-256
hash of the previous one, so a removed or altered event breaks the chain, and
`verify_hash_chain` finds where. Events are fsynced before the request
completes and the chain continues across restarts. Sinks are a file, the
tracing log, or memory for tests.

## Transport

All three listeners use rustls — TLS 1.2 and 1.3 — when `[tls]` is set, with
mutual TLS when `client_ca` is. Certificate and key files are watched; a change
is picked up for new connections without a restart, and
`chronix_tls_reloads_total{status}` counts reloads.

## Related

- [Secure a server](@/guides/secure-a-server.md)
- [Security reference](@/reference/security.md)
- [Server architecture](@/explanation/server.md)
