#!/usr/bin/env bash
#
# Every optional dependency a feature turns on must be named by the code
# that feature unlocks.
#
# `cargo machete` reads source text and cannot see the `[features]` table;
# `cargo udeps` sees them but needs nightly and a full build. Neither runs
# on a pull request, so a `dep:` entry that nothing names is invisible.
#
# Second half: a `[package.metadata.cargo-machete] ignored` list is allowed
# only in a crate with a `build.rs`. Generated code is the one honest reason
# a dependency is used but unnameable, and stating that mechanically beats
# trusting the prose in the entry.
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
tmpflag=$(mktemp)
# Aggregates whose members are each built on their own, and the frozen
# cluster tier, which has its own compile-only job.
EXEMPT_FEATURES="chronixd/all-connectors chronix/pipeline"
note() { printf '%s\n' "$1" >&2; fail=1; }

for manifest in crates/*/Cargo.toml; do
    crate=$(basename "$(dirname "$manifest")")
    src="crates/$crate/src"
    [ -d "$src" ] || continue

    # The crate's machete exemptions, as a space-padded string to match against.
    ignored=" $(sed -n '/^\[package.metadata.cargo-machete\]/,/^\[/p' "$manifest" \
        | sed -n 's/^ignored *= *\[\(.*\)\].*/\1/p' \
        | tr -d '"' | tr ',' ' ') "

    if [ "$(printf '%s' "$ignored" | tr -d ' ')" != "" ] && [ ! -f "crates/$crate/build.rs" ]; then
        note "$manifest: a cargo-machete 'ignored' list in a crate with no build.rs."
        note "  Generated code is the only thing a source scanner cannot see."
    fi

    # Every `dep:x` named anywhere in the [features] table.
    deps=$(sed -n '/^\[features\]/,/^\[[a-z]/p' "$manifest" \
        | grep -o 'dep:[A-Za-z0-9_-]*' | sed 's/^dep://' | sort -u)

    for dep in $deps; do
        ident=${dep//-/_}
        case "$ignored" in *" $dep "*) continue ;; esac
        if ! grep -rqw "$ident" "$src" 2>/dev/null; then
            note "$manifest: feature dependency '$dep' is named by no line under $src."
            note "  Either the code that needs it was never written, or the feature"
            note "  should stop declaring it."
        fi
    done
done

# A workflow step naming a feature that does not exist fails only when that
# job runs, which for the feature matrix is minutes into CI and after
# everything cheap has passed. Renaming `chronix-streaming`'s `flight` to
# `arrow` left one such step behind; `check-docs.sh` reads the README's
# feature table and nothing read the workflows.
for wf in .github/workflows/*.yml; do
    # `-p <crate> ... --features a,b` — the crate and the features it asks
    # for have to be read together, so take the whole cargo invocation.
    grep -oE 'cargo [a-z]+ -p [a-z0-9-]+[^|]*--features[= ][a-z0-9,_-]+' "$wf" \
    | while IFS= read -r cmd; do
        crate=$(printf '%s' "$cmd" | sed -n 's/.*-p \([a-z0-9-]*\).*/\1/p')
        feats=$(printf '%s' "$cmd" | sed -n 's/.*--features[= ]\([a-z0-9,_-]*\).*/\1/p' | tr ',' ' ')
        manifest="crates/$crate/Cargo.toml"
        [ -f "$manifest" ] || continue
        table=$(sed -n '/^\[features\]/,/^\[[a-z]/p' "$manifest")
        for feat in $feats; do
            # A feature may also be implied by an optional dependency.
            if ! printf '%s' "$table" | grep -qE "^$feat *=" \
               && ! grep -qE "^$feat = \{.*optional = true" "$manifest"; then
                printf '%s\n' "$wf: runs \`$crate --features $feat\`, which $manifest does not declare" >&2
                echo "FEATURE_MISMATCH" >> "$tmpflag"
            fi
        done
    done
done
if [ -s "$tmpflag" ]; then
    fail=1
fi
rm -f "$tmpflag"

# And the other direction: a feature no workflow names is a feature CI does
# not test. `rust_decimal` was one — off by default, named nowhere, so its
# three conversion tests had never run; only `cargo doc --all-features`
# touched the module, and that does not build test code. A feature nothing
# builds is a feature that does not work.
for manifest in crates/*/Cargo.toml; do
    crate=$(basename "$(dirname "$manifest")")
    table=$(sed -n '/^\[features\]/,/^\[[a-z]/p' "$manifest")
    defaults=$(printf '%s' "$table" | sed -n 's/^default *= *\[\(.*\)\].*/\1/p' | tr -d '"' | tr ',' ' ')
    for feat in $(printf '%s' "$table" | grep -oE '^[a-z0-9_-]+ *=' | tr -d ' ='); do
        [ "$feat" = "default" ] && continue
        case " $defaults " in *" $feat "*) continue ;; esac   # on by default
        case " $EXEMPT_FEATURES " in *" $crate/$feat "*) continue ;; esac
        if ! grep -qE -- "--features[= ][a-z0-9,_-]*\b$feat\b" .github/workflows/*.yml; then
            note "$crate declares the optional feature '$feat' and no workflow names it."
            note "  A feature nothing builds is a feature that does not work."
        fi
    done
done

if [ "$fail" -eq 0 ]; then
    echo "every feature dependency is named by the code it unlocks"
fi
exit "$fail"
