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

# ── 1b. Crate *paths* in code samples must resolve too ──────────────────
# `use chronix_anomaly::…` and `use chronix_compaction::…` sat in the internals
# pages long after those crates were merged away. Check 1 could not see them:
# it matches the hyphenated package name, and a Rust path is underscored.
echo "checking crate paths in code samples…"
existing_paths=$(ls crates | tr '-' '_' | sort -u)
for path in $(grep -rhoE '\buse chronix_[a-z_]+' site/content README.md 2>/dev/null \
              | sed 's/^use //' | sort -u); do
  if ! grep -qx "$path" <<<"$existing_paths"; then
    note "a code sample imports '$path', which is not a crate in crates/"
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

# ── 4. Published artifacts must not cite internal planning notes ────────
# The architecture notes are not published with the crates, so a reference to
# one is a dead end for every reader who is not the author. That includes doc
# comments, which docs.rs renders verbatim: `(D41)` on docs.rs points at a
# document nobody outside this checkout can open, and `concepts/QUERY.md` is a
# path that does not exist in the published source.
#
# Both halves are checked, because both have leaked: a path, and a bare `D<n>`
# or `R<n>` identifier.
# A document that has gone missing fails silently everywhere else: the grep
# below simply finds nothing in a file that is not there.
for f in README.md CONTRIBUTING.md; do
  [ -f "$f" ] || note "$f is missing"
done

echo "checking for internal references…"
if grep -rn 'concepts/' \
     site/content README.md CONTRIBUTING.md \
     Cargo.toml crates .github 2>/dev/null; then
  note "a published artifact references the internal architecture notes"
fi
if grep -rnE '\b[DR][0-9]{1,3}\b' --include='*.rs' crates 2>/dev/null; then
  note "a doc comment cites a D/R identifier, which resolves only in concepts/"
fi
# The same citation in prose is the same defect: `concepts/` is not published,
# so "see D41" sends a reader nowhere. Scanned separately because the cluster
# pages legitimately draw R1/R2/R3 as *region* names in ASCII diagrams, which
# a bare identifier scan cannot tell apart from a citation.
if grep -rnE '\((D[0-9]{1,3}(, ?[DR][0-9]{1,3})*)\)|\bsee [DR][0-9]{1,3}\b' \
     README.md CONTRIBUTING.md site/content 2>/dev/null; then
  note "a published document cites a D/R identifier, which resolves only in concepts/"
fi

# ── 5. Deleted subsystems must stay deleted in prose ────────────────────
# The GPU backend, the WASM plugin runtime and the in-house dashboards were
# all built and then removed; each lingered in the docs afterwards.
echo "checking for removed subsystems…"
if grep -rniE '\b(wgpu|WGSL|wasmtime|GPU acceleration|GPU compute)\b' site/content 2>/dev/null \
     | grep -viE 'no GPU backend|why there is no|was built and then|deleted'; then
  note "documentation describes a subsystem that was removed"
fi

# The warm tier was cut (D77). The pages may explain that it is gone; they may
# not describe it as something an operator can configure or call.
if grep -rniE 'warm_tier_migrate|WarmTierConfig|warm_after|shards_to_warm' \
     site/content 2>/dev/null; then
  note "documentation describes the warm tier, which was removed"
fi

# Every public facade method the docs name in prose must exist. The docs have
# outlived an API twice: `warm_tier_migrate` survived its own deletion in three
# pages, and a `rollup_registry.json` that is no longer written was still
# documented as where the watermark lives.
echo "checking documented facade methods…"
facade_methods=$(
  grep -rhoE '^\s*pub (async )?fn [a-z_0-9]+' crates/chronix/src/db/*.rs crates/chronix/src/*.rs \
    | sed -E 's/.*fn //' | sort -u
)
for m in $(grep -rhoE 'db\.[a-z_0-9]+\(' site/content concepts README.md 2>/dev/null \
             | sed -E 's/^db\.//; s/\($//' | sort -u); do
  case "$m" in
    # Builder and iterator chains, not facade methods.
    query|iter|next|collect|unwrap|clone|schema|sql|promql) continue ;;
  esac
  if ! printf '%s\n' "$facade_methods" | grep -qx "$m"; then
    note "documentation calls db.$m(), which is not a public method"
  fi
done


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
# Every `pub` field in the tree, so a setting on a nested struct in any
# crate resolves. A documented key that matches none of them is a key an
# operator would copy into a config that does nothing.
config_fields=$(
  grep -rhoE '^[[:space:]]+pub [a-z_0-9]+:' crates/ \
    | sed -E 's/^[[:space:]]+pub //; s/://' | sort -u
)

# The cluster tier is frozen and its guide documents the frozen design;
# `api-reference.md` shows Telegraf's own output config, which is not ours.
for page in site/content/docs/*.md; do
  case "$page" in
    */cluster.md|*/api-reference.md) continue ;;
  esac
  [ -f "$page" ] || continue
  keys=$(awk '/^```toml/ {t=1; next} /^```/ {t=0} t' "$page" \
         | { grep -oE '^[a-z_0-9.]+[[:space:]]*=' || true; } \
         | sed -E 's/[[:space:]]*=$//' | sort -u)
  for key in $keys; do
    # Section prefixes are stripped: `auth.jwt.issuer` is `issuer`.
    leaf=${key##*.}
    grep -qx "$leaf" <<<"$config_fields" \
      || note "$page documents setting '$key', which is not a field of any config struct"
  done
done

# ── 8. A documented setting must be *read* somewhere ────────────────────
# Check 7 asks whether a documented key names a real field; this asks whether
# anything ever looks at it. Eleven did not — the whole `[multivariate]`
# section, four `[analytics]` defaults and the per-measurement overrides were
# parsed, validated and documented, and read by nothing, so a value set there
# changed no behaviour and raised no error. That is the failure mode of every
# setting: it does not fail, it is simply ignored.
echo "checking that documented settings are read…"
for page in site/content/docs/*.md; do
  case "$page" in
    */cluster.md|*/api-reference.md) continue ;;
  esac
  [ -f "$page" ] || continue
  keys=$(awk '/^```toml/ {t=1; next} /^```/ {t=0} t' "$page" \
         | { grep -oE '^[a-z_0-9.]+[[:space:]]*=' || true; } \
         | sed -E 's/[[:space:]]*=$//' | sort -u)
  for key in $keys; do
    leaf=${key##*.}
    # Where is it named, other than in a `config.rs` that declares it?
    #
    # No `head` on this pipeline: it closes the pipe while `grep -r` is still
    # writing, and the SIGPIPE that follows fails the script under
    # `pipefail` — which is how this check passed locally and exited 2 in CI.
    # The second `grep` consumes the whole stream, so nothing is left writing
    # into a closed pipe.
    users=$(grep -rl "\b${leaf}\b" crates/ --include='*.rs' 2>/dev/null \
            | { grep -vE 'config\.rs$' || true; })
    if [ -z "$users" ]; then
      note "$page documents setting '$key', which no code outside a config module reads"
    fi
  done
done

if [ "$fail" -eq 0 ]; then
  echo "documentation matches the tree"
else
  echo
  echo "documentation drift detected — see above" >&2
fi
exit "$fail"
