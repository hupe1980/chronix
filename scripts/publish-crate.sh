#!/usr/bin/env bash
# Publish one crate, treating "already at this version" as success.
#
# A release is not atomic: crates.io rate-limits new crates (a burst of five,
# then one per ten minutes), so a first release of this workspace stops
# part-way and the job is re-run. Every crate already uploaded at this version
# must then be a no-op rather than a failure.
#
# `DRY_RUN=true` adds `--dry-run`. That only works once a first release exists:
# a dry run resolves dependencies against crates.io, so it fails with "no
# matching package named …" for a crate whose siblings are not published yet.
set -euo pipefail

crate="${1:?usage: publish-crate.sh <crate>}"

[ -n "${CARGO_REGISTRY_TOKEN:-}" ] || {
  echo "::error::CARGO_REGISTRY_TOKEN is not set" >&2
  exit 1
}

args=(--locked)
[ "${DRY_RUN:-false}" = "true" ] && args+=(--dry-run)

out=$(cargo publish -p "$crate" "${args[@]}" 2>&1) && rc=0 || rc=$?
printf '%s\n' "$out"
[ "$rc" -eq 0 ] && exit 0

if printf '%s' "$out" | grep -qiE "already (uploaded|exists)"; then
  echo "$crate is already published at this version — nothing to do"
  exit 0
fi

if printf '%s' "$out" | grep -q "too many new crates"; then
  echo "::notice::crates.io rate limit reached. It allows five new crates in a \
burst, then one per ten minutes. Re-run this job after the time above; the \
crates already published will be skipped."
fi

exit "$rc"
