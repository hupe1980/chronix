#!/usr/bin/env bash
# Print the workspace's publishable crates in dependency order, one per line.
#
# crates.io rejects a crate whose dependencies it cannot resolve, so a release
# has to upload them bottom-up. Deriving the order from the manifests keeps it
# correct across dependency changes; a hand-maintained list is only checked on
# release day, which is the worst time to find out it is stale.
#
# `--check` validates the publish steps in `.github/workflows/release.yml`
# against the manifests: same set of crates, and every crate after the ones it
# depends on. **Not** against this script's own output, which is one
# linearisation of several correct ones — comparing to it failed a release for
# a reshuffle that would have published perfectly, when removing the
# `chronix-engine → chronix-security` edge freed those two to swap.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-order}"

cargo metadata --format-version 1 --no-deps | MODE="$MODE" python3 -c '
import json, os, pathlib, re, sys

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

if os.environ["MODE"] != "--check":
    print("\n".join(order))
    raise SystemExit(0)

wf = pathlib.Path(".github/workflows/release.yml").read_text()
declared = re.findall(r"publish-crate\.sh ([a-z0-9-]+)", wf)

problems = []
missing = publishable - set(declared)
extra = set(declared) - publishable
if missing:
    problems.append(f"publishable but never published: {sorted(missing)}")
if extra:
    problems.append(f"published but not a publishable workspace crate: {sorted(extra)}")
if len(declared) != len(set(declared)):
    problems.append("a crate is published twice")

# The property that matters: every dependency is already on crates.io.
position = {n: i for i, n in enumerate(declared)}
for n in declared:
    for d in sorted(deps.get(n, ())):
        if d in position and position[d] > position[n]:
            problems.append(f"{n} is published before its dependency {d}")

if problems:
    print("the publish steps in release.yml do not work:", file=sys.stderr)
    for p in problems:
        print(f"  {p}", file=sys.stderr)
    print("\na correct order (one of several):", file=sys.stderr)
    print("  " + " ".join(order), file=sys.stderr)
    raise SystemExit(1)

print(f"release.yml publishes {len(declared)} crates in a workable order")
'
