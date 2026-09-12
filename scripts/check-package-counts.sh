#!/usr/bin/env bash
#
# "+N packages" in the documentation must be what `cargo tree` says today.
#
# These numbers are the whole argument for the feature gates, and they are
# the kind that rots silently: removing three unused dependencies moved two
# of them by 11 and 13 and nothing in the tree could see it. `check-docs.sh`
# reads names, not arithmetic.
#
# Slow (one resolve per feature), so it is its own script rather than part
# of check-docs.
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
count() { cargo tree "$@" -e normal --prefix none 2>/dev/null | sed 's/ (\*)$//' | sort -u | grep -c .; }

base=$(count -p chronix --no-default-features)
declare -a CLAIMS=(
    "streaming|README.md site/content/docs/getting-started.md crates/chronix/Cargo.toml"
    "security|README.md site/content/docs/getting-started.md crates/chronix/Cargo.toml"
)

for claim in "${CLAIMS[@]}"; do
    feat=${claim%%|*}
    files=${claim#*|}
    actual=$(( $(count -p chronix --no-default-features --features "$feat") - base ))
    for f in $files; do
        # The documented delta sits on the line naming the feature, or within
        # the paragraph that does; search the file for "<n> packages" near it.
        if ! grep -q "$actual packages" "$f"; then
            printf '%s\n' "$f: no '$actual packages' found, but '$feat' costs $actual over the default-free build" >&2
            fail=1
        fi
    done
done

arrow_base=$(count -p chronix-streaming --no-default-features)
arrow=$(( $(count -p chronix-streaming --features arrow) - arrow_base ))
if ! grep -q "$arrow packages" crates/chronix-streaming/Cargo.toml; then
    printf '%s\n' "crates/chronix-streaming/Cargo.toml: 'arrow' costs $arrow packages, not what is documented" >&2
    fail=1
fi

if [ "$fail" -eq 0 ]; then
    echo "documented package counts match cargo tree"
fi
exit "$fail"
