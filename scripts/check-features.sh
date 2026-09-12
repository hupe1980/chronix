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

if [ "$fail" -eq 0 ]; then
    echo "every feature dependency is named by the code it unlocks"
fi
exit "$fail"
