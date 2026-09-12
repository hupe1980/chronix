#!/usr/bin/env bash
#
# Everything CI gates on, in one command, cheapest first.
#
# This exists because the checks CONTRIBUTING listed were a strict *subset*
# of the ones CI runs, and four failures in a row landed in the gap: a
# broken rustdoc link (nothing ran `cargo doc`), an example whose
# `required-features` had grown (nothing ran the examples), a `#[cfg(test)]`
# assertion true only with a feature on (nothing ran the default build), and
# a guard asserting a dependency count that varies by platform.
#
# Each of those is "I verified this in the one configuration I was in".
# The fix is not a guard — it is having one command that covers the matrix.
#
# Usage: ./scripts/preflight.sh [--quick]
#   --quick  skip the examples and the frozen cluster tier (~3 min instead
#            of ~15); run the full thing before opening a pull request.
set -uo pipefail
cd "$(dirname "$0")/.."

QUICK=0
[ "${1:-}" = "--quick" ] && QUICK=1

fail=0
step() {
    local name=$1; shift
    printf '\n\033[1m── %s\033[0m\n' "$name"
    if "$@"; then
        printf '   ok\n'
    else
        printf '\033[31m   FAILED: %s\033[0m\n' "$name"
        fail=1
    fi
}

# Prose and manifests — seconds, and the commonest thing to forget.
step "documentation vs the tree"   ./scripts/check-docs.sh
step "feature dependencies"        ./scripts/check-features.sh
step "package counts"              ./scripts/check-package-counts.sh
step "D/R references"              ./scripts/check-references.sh
step "unused dependencies"         cargo machete

step "formatting"                  cargo fmt --all --check
step "clippy"                      cargo clippy --all-targets -- -D warnings

# `cargo doc` is its own gate: an intra-doc link resolves against the item
# tree, which neither `check` nor `clippy` walks.
step "rustdoc links"               env RUSTDOCFLAGS="-D warnings" \
                                       cargo doc --no-deps --workspace --all-features

# Both sides of the connector features. A `#[cfg(test)]` assertion can be
# true in one and false in the other, which is how `connector_lifecycle`
# passed locally and failed in CI.
step "tests (default build)"       cargo test
step "tests (connector features)"  cargo test -p chronixd --features kafka,mqtt --lib

if [ "$QUICK" -eq 0 ]; then
    step "tests (workspace, all features)" \
        cargo test --workspace --all-features -- --test-threads=2
    # `cargo run` *skips* an example whose required-features are off, so the
    # feature set here has to be everything.
    step "examples" bash -c '
        set -euo pipefail
        for f in crates/chronix/examples/*.rs; do
            cargo run -q -p chronix --all-features --example "$(basename "$f" .rs)" >/dev/null
        done'
fi

printf '\n'
if [ "$fail" -eq 0 ]; then
    printf '\033[32mpreflight passed\033[0m\n'
else
    printf '\033[31mpreflight failed — see above\033[0m\n'
fi
exit "$fail"
