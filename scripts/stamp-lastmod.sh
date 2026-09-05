#!/usr/bin/env bash
# Stamp each documentation page's `updated` date from git, for the sitemap.
#
# Zola emits `<lastmod>` from a page's `updated` front-matter field, and no
# page had one — so the sitemap listed 69 URLs with no freshness signal at
# all, and a crawler had to refetch everything to discover that nothing had
# changed.
#
# The date comes from the last commit that touched the file rather than being
# written by hand, because a hand-maintained date is a date that goes stale,
# and Google ignores `lastmod` it finds untrustworthy — which is worse than
# omitting it. This is run in CI immediately before `zola build`, against an
# ephemeral checkout; it rewrites files in place and is idempotent.
#
# Requires full history: a shallow clone reports no commit for most files, and
# those are left alone rather than stamped with a wrong date.
#
# Section indexes (`_index.md`) are skipped: `updated` is a *page* field, and
# Zola rejects the front matter of a section that carries one.
set -euo pipefail
cd "$(dirname "$0")/.."

stamped=0
skipped=0

while IFS= read -r file; do
  # An explicit `updated` in the source is an author's decision; leave it.
  if awk '/^\+\+\+$/{n++; next} n==1 && /^updated *=/{found=1} END{exit !found}' "$file"; then
    continue
  fi

  date=$(git log -1 --format=%cI -- "$file" 2>/dev/null || true)
  if [ -z "$date" ]; then
    # Never committed, or a shallow clone. A guessed date is worse than none.
    skipped=$((skipped + 1))
    continue
  fi

  # Insert into the TOML front matter, immediately after its opening `+++`.
  tmp=$(mktemp)
  awk -v d="$date" '
    NR == 1 && $0 == "+++" { print; print "updated = " "\"" d "\""; next }
    { print }
  ' "$file" > "$tmp"
  mv "$tmp" "$file"
  stamped=$((stamped + 1))
done < <(find site/content -name '*.md' ! -name '_index.md' | sort)

echo "stamped $stamped page(s); skipped $skipped with no commit date"
