#!/usr/bin/env bash
#
# Everything CI gates on, in one command, cheapest first.
#
# This exists because the checks CONTRIBUTING listed were a strict *subset*
# of the ones CI runs, and four failures in a row landed in the gap: a
# broken rustdoc link (nothing ran `cargo doc`), an example whose
# `required-features` had grown (nothing ran the examples), a `#[cfg(test)]`
# assertion true only with a feature on (nothing ran the default build), and
# and a guard asserting a dependency count that is not portable.
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
step "D/R references"              ./scripts/check-references.sh
# A release-day check that only runs on release day finds its problem on
# release day. This one costs a `cargo metadata`.
step "publish order"               ./scripts/publish-order.sh --check
step "unused dependencies"         cargo machete

step "formatting"                  cargo fmt --all --check
step "clippy"                      cargo clippy --all-targets -- -D warnings

# The *other* architecture. `compute/simd.rs` has an AVX2/AVX-512 tier behind
# `#[cfg(target_arch = "x86_64")]` and a NEON tier behind `aarch64`, so a
# developer only ever compiles one of them — and 103 edition-2024
# `unsafe_op_in_unsafe_fn` errors sat in the x86_64 tier, invisible on an
# aarch64 machine, until CI rejected the push. `clippy` needs no linker for a
# cross target, so this costs a compile and nothing else.
step "clippy (other architecture)" bash -c '
    host=$(rustc -vV | sed -n "s/^host: //p")
    case "$host" in
        aarch64-*) other=x86_64-apple-darwin ;;
        x86_64-*)  other=aarch64-apple-darwin ;;
        *)         echo "   skipped: unknown host $host"; exit 0 ;;
    esac
    case "$host" in *-apple-darwin) ;; *)
        other=$(echo "$other" | sed s/apple-darwin/unknown-linux-gnu/) ;;
    esac
    if ! rustup target list --installed 2>/dev/null | grep -qx "$other"; then
        echo "   skipped: rustup target add $other"; exit 0
    fi
    cargo clippy --all-targets --target "$other" -- -D warnings'

# The embedded build, without DataFusion. `sql` is on by default everywhere
# else, so a `#[cfg(feature = "sql")]` that stops compiling — or a test that
# reaches `chronix::sql` without declaring `required-features` — is invisible
# to every step above. That is not hypothetical: it is how
# `analytics_null_semantics` reached CI, where this is the line that caught
# it. `--all-targets`, because the test targets are the half that broke.
step "embedded build (no sql)"     cargo clippy -p chronix --no-default-features \
                                       --all-targets -- -D warnings

# `cargo doc` is its own gate: an intra-doc link resolves against the item
# tree, which neither `check` nor `clippy` walks.
step "rustdoc links"               env RUSTDOCFLAGS="-D warnings" \
                                       cargo doc --no-deps --workspace --all-features

# Both sides of the connector features. A `#[cfg(test)]` assertion can be
# true in one and false in the other, which is how `connector_lifecycle`
# passed locally and failed in CI.
step "tests (default build)"       cargo test
# **Not `--lib`.** CI runs `cargo test -p chronixd --features kafka`, which
# builds every target — including `--test suite`, where the races that
# reached CI three times actually live. Restricting this to the library was
# a hole in exactly the shape of the failures it was meant to catch.
step "tests (connector features)"  cargo test -p chronixd --features kafka,mqtt

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

    # `fuzz/` is its own workspace on nightly, so nothing else in this script
    # or in the pull-request CI ever compiles it — and `fuzz.yml` runs on a
    # schedule, where a failure blocks no one and is seen by no one. It had
    # been failing every night before it compiled a line: the package
    # declared no `[workspace]` and was in neither `members` nor `exclude`,
    # which cargo refuses outright. Building is enough to catch that class;
    # actually fuzzing stays on the schedule where it belongs.
    step "fuzz targets build" bash -c '
        if ! rustup toolchain list 2>/dev/null | grep -q nightly; then
            echo "   skipped: no nightly toolchain"; exit 0
        fi
        if ! command -v cargo-fuzz >/dev/null; then
            echo "   skipped: cargo-fuzz not installed"; exit 0
        fi
        cargo +nightly fuzz build >/dev/null'
fi

printf '\n'
if [ "$fail" -eq 0 ]; then
    printf '\033[32mpreflight passed\033[0m\n'
else
    printf '\033[31mpreflight failed — see above\033[0m\n'
fi
exit "$fail"
