#!/usr/bin/env bash
# Every publishable crate still packages, and carries its readme.
#
# The packaging step is otherwise exercised only on release day, which is the
# worst time to find out it does not work: a release once stopped on its first
# crate. `cargo package --list` runs the same checks a real publish does —
# including the working-directory cleanliness check — without compiling or
# uploading anything, so the whole sweep costs seconds on every commit.
#
# There is **one** changelog, at the repository root, read in git. It is
# deliberately not bundled into the crates: nothing may write into a package
# directory to get it there, because a dirty package directory is precisely
# what cargo refuses, and refusing it is a protection rather than an obstacle.
#
# What this does **not** cover: `cargo publish --dry-run`, which resolves
# dependencies against crates.io and so cannot work for a crate whose siblings
# are not published at the new version yet — which is every release.
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0

# Same definition of "publishable" as `publish-order.sh`: `publish` unset.
crates=$(cargo metadata --format-version 1 --no-deps | python3 -c '
import json, sys
meta = json.load(sys.stdin)
for p in meta["packages"]:
    if p.get("publish") is None:
        print(p["name"])
' | sort)

[ -n "$crates" ] || { echo "no publishable crates found" >&2; exit 1; }

for crate in $crates; do
    listing=$(cargo package -p "$crate" --no-verify --list 2>&1)
    if [ $? -ne 0 ]; then
        echo "  $crate: cargo package failed"
        printf '%s\n' "$listing" | grep -E "^error" | head -3
        fail=1
        continue
    fi

    if ! printf '%s\n' "$listing" | grep -qx "README.md"; then
        echo "  $crate: README.md is missing from the package"
        echo "     it is what docs.rs and the crates.io page render"
        fail=1
    fi
done

if [ "$fail" -eq 0 ]; then
    echo "every publishable crate packages with its readme"
fi
exit "$fail"
