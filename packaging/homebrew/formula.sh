#!/usr/bin/env bash
# Render the Homebrew formula for a release from the archives' .sha256 files.
#
#   packaging/homebrew/formula.sh 0.9.0 dist/ > Formula/chronixd.rb
set -euo pipefail
version="${1:?usage: formula.sh <version> <dir with chronixd-*.tar.gz.sha256>}"
dir="${2:?usage: formula.sh <version> <dir with chronixd-*.tar.gz.sha256>}"
out="$(sed "s/@VERSION@/$version/g" "$(dirname "$0")/chronixd.rb.in")"
for target in aarch64-apple-darwin aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu; do
  file="$dir/chronixd-v$version-$target.tar.gz.sha256"
  sum="$(awk '{ print $1 }' "$file" 2>/dev/null || true)"
  if ! [[ "$sum" =~ ^[0-9a-f]{64}$ ]]; then
    echo "formula: no checksum in $file" >&2
    exit 1
  fi
  out="${out//@SHA256_$target@/$sum}"
done
if grep -q '@[A-Za-z0-9_-]*@' <<<"$out"; then
  echo "formula: a placeholder was left" >&2
  exit 1
fi
printf '%s\n' "$out"
