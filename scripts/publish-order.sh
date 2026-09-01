#!/usr/bin/env bash
# Print the workspace's publishable crates in dependency order, one per line.
#
# crates.io rejects a crate whose dependencies it cannot resolve, so a release
# has to upload them bottom-up. Deriving the order from the manifests keeps it
# correct across dependency changes; a hand-maintained list is only checked on
# release day, which is the worst time to find out it is stale.
set -euo pipefail

cargo metadata --format-version 1 --no-deps | python3 -c '
import json, sys

meta = json.load(sys.stdin)
pkgs = {p["name"]: p for p in meta["packages"]}
# `publish` is null when unrestricted; `[]` for `publish = false`.
publishable = {n for n, p in pkgs.items() if p.get("publish") is None}

deps = {
    n: {d["name"] for d in pkgs[n]["dependencies"]
        if d["name"] in publishable and d["kind"] is None}
    for n in publishable
}

order, seen, visiting = [], set(), set()
def visit(n):
    if n in seen:
        return
    if n in visiting:
        sys.exit(f"dependency cycle through {n}")
    visiting.add(n)
    for d in sorted(deps[n]):
        visit(d)
    visiting.discard(n)
    seen.add(n)
    order.append(n)

for n in sorted(publishable):
    visit(n)
print("\n".join(order))
'
