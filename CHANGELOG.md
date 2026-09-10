# Changelog

Notable changes to Chronix. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/). Chronix is pre-1.0:
per [CONTRIBUTING.md](CONTRIBUTING.md), a breaking change bumps the minor
version and a fix bumps the patch — there is no stability promise before 1.0,
and no migration tooling for the on-disk format.

## [Unreleased]

## [0.4.0]

### Changed

- **Breaking:** webhook delivery now sends a [CloudEvents](https://cloudevents.io)
  1.0 envelope, signed per the [Standard Webhooks](https://www.standardwebhooks.com)
  `v1` scheme (`webhook-id` / `webhook-timestamp` / `webhook-signature`),
  replacing the `X-Chronix-Signature: sha256=<hex>` header. Signing the
  timestamp alongside the body lets a receiver reject a replayed request,
  which the old body-only signature could not express.

### Fixed

- A measurement dropped with `soft_delete_ttl` configured is now actually
  invisible — to SQL, PromQL, the native query API, gRPC and Flight SQL — for
  its whole grace period, and the pending state survives a restart. It was
  previously tracked in memory only and consulted by nothing on the read
  path, so the data stayed fully readable until the background pass
  eventually deleted it.
- HTTP, gRPC and Flight SQL measurement listing, and PromQL discovery, now
  resolve "what measurements exist" through the same accessors the query path
  uses, so a pending-drop measurement can no longer appear in one listing and
  not another.

## [0.3.0] and earlier

Predate this file. See the `v0.1.0` / `v0.2.0` / `v0.3.0` git tags.
