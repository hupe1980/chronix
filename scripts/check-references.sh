#!/usr/bin/env bash
# Every `D<n>` and `R<n>` cited in the tree must resolve.
#
# `concepts/README.md` states that these are stable identifiers cited from code
# comments, and that "renumbering one breaks a reference nothing checks". This
# is that check. It exists because the tree carried 130 comments citing `R26`,
# `R29`, `R31`, `R32` and `R34` — round numbers from a superseded audit scheme,
# which read to anyone following them as citations of risks that do not exist.
#
# In Markdown, an identifier inside backticks is being *discussed* rather than
# cited — that is how the documents explain the retired numbers — so code spans
# are stripped before scanning. A real citation is written bare: `(D25)`, `R7`.
set -euo pipefail
cd "$(dirname "$0")/.."

defined=$(
  { grep -oE '^\| (D[0-9]+) \|' concepts/DECISIONS.md
    grep -oE '^\| (R[0-9]+) \|' concepts/RISKS.md
  } | tr -d '| ' | sort -u
)

scan_markdown() {
  # Drop fenced code blocks, then inline code spans.
  awk '/^```/ {fence = !fence; next} !fence' "$1" | sed 's/`[^`]*`//g'
}

cited=$(
  { find crates -name '*.rs' -print0 | xargs -0 grep -hoE '\b[DR][0-9]{1,3}\b'
    # The manifests cite decisions too: several dependency pins exist only
    # because of one, and a pin whose reason has been renumbered away is a pin
    # nobody will dare touch.
    grep -hoE '\b[DR][0-9]{1,3}\b' Cargo.toml crates/*/Cargo.toml 2>/dev/null
    for f in concepts/*.md docs/*.md docs/src/**/*.md README.md; do
      [ -f "$f" ] && scan_markdown "$f" | grep -oE '\b[DR][0-9]{1,3}\b' || true
    done
  } 2>/dev/null | sort -u
)

missing=$(comm -23 <(echo "$cited") <(echo "$defined") || true)

if [ -n "$missing" ]; then
  echo "error: these identifiers are cited but not defined in concepts/DECISIONS.md or concepts/RISKS.md:" >&2
  echo "$missing" | sed 's/^/  /' >&2
  echo >&2
  echo "Either define them, or — if they are leftovers from an older numbering" >&2
  echo "scheme — say what the comment means instead of citing a dead number." >&2
  echo "To discuss a retired number in prose, put it in backticks." >&2
  for id in $missing; do
    echo "── $id" >&2
    { find crates -name '*.rs' -print0 | xargs -0 grep -nE "\b$id\b" || true; } 2>/dev/null | head -3 >&2
    { grep -rnE "\b$id\b" concepts/ docs/ README.md || true; } 2>/dev/null | head -3 >&2
  done
  exit 1
fi

echo "all $(echo "$cited" | wc -l | tr -d ' ') cited D/R identifiers resolve"
