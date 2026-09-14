#!/usr/bin/env bash
# Every `D<n>` and `R<n>` cited inside the architecture notes must resolve.
#
# `D` and `R` are the identifiers of DECISIONS.md and RISKS.md. Those notes are
# internal and are not published with the crates, so nothing outside
# `concepts/` may cite one — a `(D41)` in a doc comment renders on docs.rs as a
# reference to a document the reader cannot open, which is the same defect as
# the audit-scheme numbers that used to litter the tree. That direction is
# checked by `check-docs.sh`, which runs in CI; this script checks the other
# one, and can only run where the notes are present.
#
# Identifiers must also be unique — a number reused by a second decision
# resolves fine and means two different things.
#
# In Markdown an identifier inside backticks is being *discussed* rather than
# cited, so code spans are stripped before scanning. A real citation is written
# bare: `(D25)`, `R7`.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d concepts ]; then
  echo "concepts/ is not present — nothing to check"
  exit 0
fi

raw=$(
  { grep -oE '^\| (D[0-9]+) \|' concepts/DECISIONS.md
    grep -oE '^\| (R[0-9]+) \|' concepts/RISKS.md
  } | tr -d '| '
)
defined=$(echo "$raw" | sort -u)

# A duplicate identifier is worse than a dangling one: every citation still
# resolves, so a "cited but not defined" check passes while two different
# decisions answer to the same number.
duplicated=$(echo "$raw" | sort | uniq -d)
if [ -n "$duplicated" ]; then
  echo "error: these identifiers are defined more than once:" >&2
  echo "$duplicated" | sed 's/^/  /' >&2
  for id in $duplicated; do
    echo "── $id" >&2
    grep -nE "^\| $id \|" concepts/DECISIONS.md concepts/RISKS.md 2>/dev/null | cut -c1-100 >&2
  done
  exit 1
fi

scan_markdown() {
  # Drop fenced code blocks, then inline code spans.
  awk '/^```/ {fence = !fence; next} !fence' "$1" | sed 's/`[^`]*`//g'
}

cited=$(
  for f in concepts/*.md; do
    [ -f "$f" ] && scan_markdown "$f" | grep -oE '\b[DR][0-9]{1,3}\b' || true
  done | sort -u
)

missing=$(comm -23 <(echo "$cited") <(echo "$defined") || true)

if [ -n "$missing" ]; then
  echo "error: cited in concepts/ but not defined in DECISIONS.md or RISKS.md:" >&2
  echo "$missing" | sed 's/^/  /' >&2
  echo >&2
  echo "Either define them, or say what the sentence means instead of citing a" >&2
  echo "number. To discuss a retired number in prose, put it in backticks." >&2
  for id in $missing; do
    echo "── $id" >&2
    { grep -rnE "\b$id\b" concepts/ || true; } 2>/dev/null | head -3 >&2
  done
  exit 1
fi

echo "all $(echo "$cited" | wc -l | tr -d ' ') cited D/R identifiers resolve"

# ── Every claim in QUALITY.md §2 names a test, and that test must exist ──
#
# The table's whole premise is "a number without a test has already started
# rotting", and nothing checked that the test it names is *there*. Two were
# not: `header_older_version_rejected` had been renamed, and
# `a_tiered_segment_is_readable_as_plain_parquet` has never existed — the
# property it claims is real and is pinned by `cold_archive.rs`, but the
# citation was fiction. A claim citing a test that does not exist is worse
# than an uncited one, because it reads as evidence.
#
# Scope: the backticked identifiers in the table rows of §2, which is where
# the citations live. An identifier resolves if it is a file stem or appears
# in the source of any crate or SDK.
echo "checking that every test QUALITY.md §2 names exists…"
quality_missing=0
section=$(awk '/^## 2\. Measured numbers/{f=1} /^### Throughput and latency/{f=0} f' concepts/QUALITY.md)
ids=$(
  printf '%s\n' "$section" \
    | grep -E '^\|' \
    | sed -E 's/^\|[^|]*\|[^|]*\|//' \
    | grep -oE '`[^`]+`' | tr -d '`' \
    | grep -oE '\b[a-z_][a-z_0-9]{8,}\b' \
    | grep -vE '^(cargo|test|tests|nocapture|chronix|features|example|bench|lib|release|package)$' \
    | sort -u
)
for id in $ids; do
  if find crates sdks -name "$id.rs" -o -name "$id.py" 2>/dev/null | grep -q .; then
    continue
  fi
  if grep -rq --include='*.rs' --include='*.py' "$id" crates sdks 2>/dev/null; then
    continue
  fi
  echo "error: QUALITY.md §2 cites '$id' as pinning a claim, and it is nowhere in the tree" >&2
  quality_missing=1
done
if [ "$quality_missing" -ne 0 ]; then
  echo >&2
  echo "A claim that names a test which does not exist reads as evidence and is" >&2
  echo "not. Either point it at the test that really pins it, or write the test." >&2
  exit 1
fi
echo "every test named in QUALITY.md §2 exists"
