#!/usr/bin/env bash
#
# The "+N packages" figures in the documentation must still be roughly true.
#
# **Roughly, deliberately.** A dependency count is not a property of the code
# alone: resolving `chronix --features streaming` gives 63 packages on
# macOS and 62 on x86_64-linux, because platform-gated crates differ. An
# exact-match check must therefore fail on one platform or the other, which
# is how the first version of this script was wrong. The figures are
# documented rounded, and this checks the order of magnitude — enough to
# catch a feature that quietly doubles, which is the drift that matters.
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
count() { cargo tree "$@" -e normal --prefix none 2>/dev/null | sed 's/ (\*)$//' | sort -u | grep -c .; }

# Documented figure → how to measure it.
check() {
    local label=$1 documented=$2 actual=$3
    local lo=$(( documented * 80 / 100 ))
    local hi=$(( documented * 120 / 100 ))
    if [ "$actual" -lt "$lo" ] || [ "$actual" -gt "$hi" ]; then
        printf '%s\n' "$label: documented ~$documented packages, measured $actual here" >&2
        printf '%s\n' "  Outside ±20%, so this is drift rather than platform variance." >&2
        fail=1
    fi
}

base=$(count -p chronix --no-default-features)
check "chronix/streaming" 60 \
    $(( $(count -p chronix --no-default-features --features streaming) - base ))
check "chronix/security" 125 \
    $(( $(count -p chronix --no-default-features --features security) - base ))

arrow_base=$(count -p chronix-streaming --no-default-features)
check "chronix-streaming/arrow" 45 \
    $(( $(count -p chronix-streaming --features arrow) - arrow_base ))

if [ "$fail" -eq 0 ]; then
    echo "documented package counts are within range of cargo tree"
fi
exit "$fail"
