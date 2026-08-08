#!/usr/bin/env bash
#
# Compile the TSIGMA Tailwind v4 stylesheet using the vendored Linux
# standalone CLI binary.
#
# The Linux build path. scripts/build_css.ps1 is the Windows equivalent and
# takes the same --check / -Check flag; contribute from either platform. Both
# pin the same Tailwind release (v4.3.0), so either script produces a
# committable tailwind.css. --check is byte-exact, so if the two platforms'
# binaries ever do diverge it surfaces as a stale-CSS failure rather than
# silent drift.
#
# Compiles tsigma/static/css/tailwind.src.css into the committed
# tsigma/static/css/tailwind.css. Deployers never run this - they serve the
# committed tailwind.css as-is (no Node, no Tailwind, no extra packages).
#
# The standalone binary is NOT committed (~119 MB, platform-specific).
# Download the pinned version once into tools/tailwind/tailwindcss:
#
#   Tailwind CSS v4.3.0 (pinned)
#   https://github.com/tailwindlabs/tailwindcss/releases/download/v4.3.0/tailwindcss-linux-x64
#
# Usage:
#   scripts/build_css.sh            Build and overwrite tailwind.css
#   scripts/build_css.sh --check    Build to a temp file and diff against the
#                                   committed tailwind.css instead of
#                                   overwriting. Non-zero exit if stale.
#                                   (Currency gate for CI.)

set -euo pipefail

check_mode=0
if [[ "${1:-}" == "--check" ]]; then
    check_mode=1
elif [[ $# -gt 0 ]]; then
    echo "Unknown argument: $1" >&2
    echo "Usage: $0 [--check]" >&2
    exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

binary="$repo_root/tools/tailwind/tailwindcss"
src="$repo_root/tsigma/static/css/tailwind.src.css"
out="$repo_root/tsigma/static/css/tailwind.css"
gen_script="$script_dir/gen_safelist.py"

# Regenerate the safelist (the human-readable utility vocabulary) BEFORE
# compiling, so tailwind.src.css's @source "./safelist.txt" force-emits the
# full vocabulary. Prefer the project venv; fall back to python3 on PATH.
if [[ -x "$repo_root/.venv/bin/python" ]]; then
    python_exe="$repo_root/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    python_exe="$(command -v python3)"
else
    echo "No Python interpreter found (looked for .venv/bin/python and python3 on PATH) to run gen_safelist.py" >&2
    exit 1
fi
"$python_exe" "$gen_script"

if [[ ! -x "$binary" ]]; then
    cat >&2 <<EOF
Tailwind binary not found (or not executable) at $binary
Download Tailwind v4.3.0 (Linux x64) once:
  curl -sSL -o tools/tailwind/tailwindcss \\
    https://github.com/tailwindlabs/tailwindcss/releases/download/v4.3.0/tailwindcss-linux-x64
  chmod +x tools/tailwind/tailwindcss
EOF
    exit 1
fi

if [[ "$check_mode" -eq 1 ]]; then
    tmp="$(mktemp --suffix=.css)"
    trap 'rm -f "$tmp"' EXIT
    "$binary" -i "$src" -o "$tmp" --minify
    if [[ ! -f "$out" ]]; then
        echo "Committed tailwind.css missing at $out" >&2
        exit 1
    fi
    if ! cmp -s "$tmp" "$out"; then
        echo "tailwind.css is STALE - rerun scripts/build_css.sh and commit the result." >&2
        exit 1
    fi
    echo "tailwind.css is up to date."
    exit 0
fi

"$binary" -i "$src" -o "$out" --minify
echo "Built $out"
