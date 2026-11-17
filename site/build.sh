#!/usr/bin/env bash
# Build the whole site: Zola, the llms files, and the Pagefind search index.
#
#   ./build.sh                          # local build into public/
#   ./build.sh --base-url "$URL"        # CI; arguments go to `zola build`
#   NO_SEARCH=1 ./build.sh              # skip the index (no Node needed)
#
# `zola serve` is the fast loop while writing; it has no search index, and the
# search dialog says so.
set -euo pipefail
cd "$(dirname "$0")"

zola build "$@"

# Zola renders a page whose path has an extension as `<path>/index.html`; the
# llms files are served from the root, so move them there.
for f in llms.txt llms-full.txt; do
    if [ -f "public/$f/index.html" ]; then
        mv "public/$f/index.html" "public/$f.tmp"
        rmdir "public/$f"
        mv "public/$f.tmp" "public/$f"
    fi
done

if [ "${NO_SEARCH:-0}" != "1" ]; then
    npx --yes pagefind@1.5.2 --site public --quiet
fi
