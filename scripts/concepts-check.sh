#!/usr/bin/env bash
# Local only: concepts/ and specs/ are gitignored, so this does not run in CI.
# 1. Every roadmap item (### `#anchor`) in concepts/ROADMAP.md names one spec folder on a
#    **Specified as:** line, and every folder under specs/ (outside archive/) is named once.
# 2. Nothing published cites a path into concepts/, specs/, .specify/ or .claude/: its
#    reader cannot open it. check-docs.sh holds the same rule in CI; this repeats it so a
#    local run of the planning checks is complete on its own.
set -euo pipefail
cd "$(dirname "$0")/.."
fail=0; roadmap=concepts/ROADMAP.md
[ -f "$roadmap" ] && [ -d specs ] || { echo "concepts-check needs concepts/ROADMAP.md and specs/ (not in a clone)" >&2; exit 1; }
items=$(grep -cE '^### `#[a-z0-9-]+`' "$roadmap" || true)
named=$(grep -oE '^\*\*Specified as:\*\* `[0-9]{3}-[a-z0-9-]+`' "$roadmap" | sed -E 's/.*`([^`]+)`/\1/' | sort)
n=$(printf '%s\n' "$named" | grep -c . || true)
[ "$items" -eq "$n" ] || { echo "✗ $roadmap: $items items but $n **Specified as:** lines" >&2; fail=1; }
dupes=$(printf '%s\n' "$named" | uniq -d)
[ -z "$dupes" ] || { echo "✗ named by more than one item: $dupes" >&2; fail=1; }
for s in $named; do [ -f "specs/$s/spec.md" ] || { echo "✗ $roadmap names $s, but specs/$s/spec.md does not exist" >&2; fail=1; }; done
for d in specs/[0-9][0-9][0-9]-*/; do
    [ -d "$d" ] || continue; s=$(basename "$d")
    printf '%s\n' "$named" | grep -qx "$s" || { echo "✗ specs/$s is named by no roadmap item" >&2; fail=1; }
    a=$(grep -oE '^\*\*Input\*\*: `concepts/ROADMAP.md` `#[a-z0-9-]+`' "specs/$s/spec.md" | grep -oE '#[a-z0-9-]+' || true)
    if [ -z "$a" ]; then echo "✗ specs/$s/spec.md has no **Input** line naming its roadmap anchor" >&2; fail=1
    elif ! grep -qE "^### \`$a\`.*" "$roadmap" || [ "$(awk -v a="### \`$a\`" 'index($0,a)==1{f=1;next} f&&/^\*\*Specified as:\*\*/{print;exit}' "$roadmap" | grep -oE '[0-9]{3}-[a-z0-9-]+')" != "$s" ]; then
        echo "✗ specs/$s/spec.md cites $a, which does not name it" >&2; fail=1
    fi
done
# Stable identifiers: one C/H/N/S number per item, never two items with one number.
ids=$(grep -oE '^### `#[a-z0-9-]+` [CHNS][0-9]+' "$roadmap" | awk '{print $3}' | sort)
iddupes=$(printf '%s\n' "$ids" | uniq -d)
[ -z "$iddupes" ] || { echo "✗ identifier used by more than one item: $iddupes" >&2; fail=1; }
leaks=$(grep -rnE '(^|[^A-Za-z0-9_-])(concepts/|specs/|\.specify/|\.claude/)' \
    crates README.md CONTRIBUTING.md CHANGELOG.md site/content dashboards/README.md sdks/python/README.md 2>/dev/null || true)
[ -z "$leaks" ] || { echo "✗ the published tree cites a path its reader does not have:" >&2; echo "$leaks" | sed 's/^/  /' >&2; fail=1; }
[ "$fail" -eq 0 ] || exit 1
echo "✓ $items roadmap items, each with its specification; no published path into concepts/, specs/, .specify/ or .claude/"
