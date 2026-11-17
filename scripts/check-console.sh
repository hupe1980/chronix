#!/usr/bin/env bash
# The console's static rules, checked without a browser.
#
# 1. Budget: every asset, gzip -9, at most 300 KB together.
# 2. No markup from strings and no code from strings in app.js: the page
#    renders through textContent only, and the CSP forbids inline script, so
#    the lint and the policy say the same thing.
# 3. Storage: only the theme is stored; the API key never touches storage,
#    the URL or the history.
# 4. Every route app.js calls is one other clients use (the contract below).
set -euo pipefail
cd "$(dirname "$0")/../crates/chronixd/console"
fail=0
note() { printf '  %s\n' "$1"; fail=1; }

total=0
for f in index.html app.css app.js uplot.min.js uplot.min.css snippets.json LICENSE-uPlot; do
  [ -f "$f" ] || { note "missing asset: $f"; continue; }
  n=$(gzip -9 -c "$f" | wc -c | tr -d ' ')
  total=$((total + n))
done
[ "$total" -le $((300 * 1024)) ] || note "the console is $total bytes gzipped; the budget is 307200"

if grep -nE 'innerHTML|outerHTML|insertAdjacentHTML|document\.write|eval\(|new Function|\bon[a-z]+\s*=\s*' app.js \
   | grep -vE '^\s*[0-9]+:\s*//' ; then
  note "app.js builds markup or code from a string (or assigns an inline handler)"
fi
if grep -nE '<script>|<style|\son[a-z]+=|style=' index.html; then
  note "index.html has inline script, style or a handler; the CSP refuses all three"
fi

if grep -nE 'sessionStorage|document\.cookie|indexedDB' app.js; then
  note "app.js stores something other than the theme"
fi
if grep -n 'localStorage\.' app.js | grep -v '"chronix.theme"'; then
  note "localStorage is used for something other than the theme"
fi
if grep -nE '(history\.(push|replace)State|location\.(hash|href)\s*=).*session\.key' app.js; then
  note "the API key reaches the URL or the history"
fi

contract='^api/v1/(query|query_range|labels|label/|label/__name__/values|chronix/sql|measurements|status/tsdb|chronix/promql/explain)$'
calls=$(grep -oE 'api/v1/[A-Za-z0-9_/]*' app.js | sort -u)
for c in $calls; do
  echo "$c" | grep -qE "$contract" || note "app.js calls $c, which is not in the console's contract"
done

[ "$fail" -eq 0 ] || exit 1
echo "✓ console: ${total} bytes gzipped (budget 307200); no string-built markup; stores only the theme; calls only shared routes"
