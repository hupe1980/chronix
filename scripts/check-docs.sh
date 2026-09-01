#!/usr/bin/env bash
# The documentation must describe *this* tree.
#
# Every check here exists because the claim it makes failed silently. The site
# documented three different sets of listen ports and none of them matched the
# server; it linked to 25 examples at a path that had not existed since the
# crate layout changed; it named a dozen crates that were merged away; and it
# documented a GPU backend that had been deleted. None of that is visible to a
# compiler, a test, or a link checker — the links were syntactically fine and
# resolved to real pages. Only comparing the prose to the code finds it.
#
# `zola check` covers link and anchor integrity; this covers factual drift.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
note() { printf '  %s\n' "$1"; fail=1; }

# ── 1. Crates named in the docs must exist ──────────────────────────────
# The tree went from 33 crates to 9; the docs kept citing the old ones, so a
# reader following them would depend on packages that were never published.
echo "checking crate names…"
existing=$(ls crates | sort -u)
# `chronix-client` is the Python package on PyPI, not a workspace crate.
allow_extra=$'chronix-client'
# `chronix-*` also appears as certificate CNs, Kafka group ids, MQTT client ids,
# hostnames and bucket paths. Those are sample *values*, not package names, so
# the lines that carry them are excluded before the names are collected.
cited=$(grep -rhE '\bchronix-[a-z]+\b' site/content README.md 2>/dev/null \
        | grep -vE 'CN=|allowed_cns|group_id|client_id|://|<chronix-|chronix-data' \
        | grep -oE '\bchronix-[a-z]+\b' \
        | sort -u \
        | grep -vE '^chronix-(hero|accent)$' || true)
for c in $cited; do
  if ! grep -qx "$c" <<<"$existing" && ! grep -qx "$c" <<<"$allow_extra"; then
    note "docs cite crate '$c', which does not exist in crates/"
  fi
done

# ── 2. Documented default ports must match the code ─────────────────────
# The docs claimed 4242 and 5555/5556/5557 in different files while the server
# listened on 8086/8087/8817, so every copy-pasteable example failed.
echo "checking default ports…"
cfg=crates/chronixd/src/config.rs
for pair in "default_http_addr:HTTP" "default_grpc_addr:gRPC" "default_flight_addr:Flight SQL"; do
  fn=${pair%%:*}; label=${pair##*:}
  port=$(grep -A 2 "fn ${fn}()" "$cfg" | grep -oE '0\.0\.0\.0:[0-9]+' | cut -d: -f2 | head -1)
  [ -n "$port" ] || { note "could not read ${label} default port from ${cfg}"; continue; }
  if ! grep -rqF "$port" site/content/docs/getting-started.md; then
    note "${label} default port ${port} is not mentioned in the Getting Started page"
  fi
done
# Ports the docs used to claim, none of which the server has ever bound.
if grep -rnE '\b(4242|5555|5556|5557)\b' site/content README.md dashboards 2>/dev/null; then
  note "documentation references a port the server does not listen on"
fi

# ── 3. Every linked example must exist ──────────────────────────────────
echo "checking example links…"
grep -rhoE 'crates/chronix/examples/[a-z_0-9]+\.rs' site/content 2>/dev/null \
  | sort -u \
  | while read -r path; do
      [ -f "$path" ] || note "docs link to '$path', which does not exist"
    done

# ── 4. Public docs must not cite internal planning notes ────────────────
# Those live outside the published tree; a link to one is a dead link for
# every reader who is not the author.
echo "checking for internal references…"
if grep -rn 'concepts/' site/content README.md CONTRIBUTING.md RELEASING.md 2>/dev/null; then
  note "public documentation references internal planning notes"
fi

# ── 5. Deleted subsystems must stay deleted in prose ────────────────────
# The GPU backend, the WASM plugin runtime and the in-house dashboards were
# all built and then removed; each lingered in the docs afterwards.
echo "checking for removed subsystems…"
if grep -rniE '\b(wgpu|WGSL|wasmtime|GPU acceleration|GPU compute)\b' site/content 2>/dev/null \
     | grep -viE 'no GPU backend|why there is no|was built and then|deleted'; then
  note "documentation describes a subsystem that was removed"
fi


# ── 6. Documented configuration keys must exist ─────────────────────────
# The whole "Configuration Reference" was fiction: `[storage.objstore]`,
# `[storage.tiering]`, `[storage.warm]`, `[compute]`, `admission.*` and
# `segment.*` were documented with defaults and units, and none of them was a
# field of any config struct. Every configuration struct is
# `deny_unknown_fields`, so an operator following the docs got a startup error
# rather than the behaviour the page described.
#
# Scope: TOML blocks on the pages that document configuration. Keys elsewhere
# are illustrative payloads, not settings.
echo "checking documented configuration keys…"
config_fields=$(
  grep -hoE '^[[:space:]]+pub [a-z_0-9]+:' \
    crates/chronixd/src/config.rs \
    crates/chronix-core/src/config.rs \
    crates/chronixd/src/connector.rs \
    crates/chronixd/src/otel/config.rs \
    crates/chronix-security/src/auth/jwt.rs \
    | sed -E 's/^[[:space:]]+pub //; s/://' | sort -u
)

for page in site/content/docs/operations.md site/content/docs/configuration.md; do
  [ -f "$page" ] || continue
  keys=$(awk '/^```toml/ {t=1; next} /^```/ {t=0} t' "$page" \
         | grep -oE '^[a-z_0-9.]+[[:space:]]*=' \
         | sed -E 's/[[:space:]]*=$//' | sort -u)
  for key in $keys; do
    # Section prefixes are stripped: `auth.jwt.issuer` is `issuer`.
    leaf=${key##*.}
    grep -qx "$leaf" <<<"$config_fields" \
      || note "$page documents setting '$key', which is not a field of any config struct"
  done
done

if [ "$fail" -eq 0 ]; then
  echo "documentation matches the tree"
else
  echo
  echo "documentation drift detected — see above" >&2
fi
exit "$fail"
