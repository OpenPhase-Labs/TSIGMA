#!/usr/bin/env bash
#
# Generate Python gRPC stubs from the TSIGMA plugin contract into
# tsigma/plugins/gen/ (Phase 1 of plans/2026-06-27-grpc-plugin-migration.md).
#
# The contract lives outside this repo so plugin authors consume the same source.
# Override its location with TSIGMA_CONTRACT_PROTO.
#
# auth and storage are NOT generated: their proto packages are tsigma.auth.v1 /
# tsigma.storage.v1, and this repo's tsigma/auth/ and tsigma/storage/ are regular
# packages, so the generated v1 sibling is unreachable. Both are deferred anyway.
#
# grpc.health.v1 is not generated - it comes from grpcio-health-checking.
#
# Usage:
#   scripts/gen_proto.sh            Regenerate stubs in place
#   scripts/gen_proto.sh --check    Fail if the committed stubs are stale

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

proto_root="${TSIGMA_CONTRACT_PROTO:-/opt/webpages/TSIGMA-Contract/proto}"
vendor_root="$proto_root/vendor/go-plugin"
subsystems=(decoder method notify report)
gen_rel="tsigma/plugins/gen"

for d in "$proto_root" "$vendor_root"; do
    if [[ ! -d "$d" ]]; then
        echo "Contract protos not found at $d" >&2
        echo "Set TSIGMA_CONTRACT_PROTO to the contract's proto/ directory." >&2
        exit 1
    fi
done

if [[ -x "$repo_root/.venv/bin/python" ]]; then
    python_exe="$repo_root/.venv/bin/python"
else
    python_exe="$(command -v python3)"
fi

if ! "$python_exe" -c "import grpc_tools" 2>/dev/null; then
    echo "grpcio-tools not installed. Run: pip install -e '.[dev]'" >&2
    exit 1
fi

generate() {
    local out="$1"
    mkdir -p "$out"
    local protos=()
    for sub in "${subsystems[@]}"; do
        protos+=("$proto_root/tsigma/$sub/v1/$sub.proto")
    done
    "$python_exe" -m grpc_tools.protoc \
        -I "$proto_root" --python_out="$out" --grpc_python_out="$out" "${protos[@]}"
    "$python_exe" -m grpc_tools.protoc \
        -I "$vendor_root" --python_out="$out" --grpc_python_out="$out" "$vendor_root"/*.proto
    # Only the v1 leaf dirs get __init__.py. tsigma/ and tsigma/<sub>/ inside the
    # generated tree MUST stay namespace packages: an __init__.py there makes them
    # regular packages that shadow the real tsigma/ app package once gen/ is on
    # sys.path, and tsigma.config / tsigma.storage stop resolving.
    touch "$out/__init__.py"
    for sub in "${subsystems[@]}"; do
        touch "$out/tsigma/$sub/v1/__init__.py"
    done
}

if [[ "$check_mode" -eq 1 ]]; then
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    generate "$tmp"
    if ! diff -rq -x '__pycache__' "$tmp" "$repo_root/$gen_rel" >/dev/null 2>&1; then
        echo "Contract stubs are stale - rerun scripts/gen_proto.sh and commit the result." >&2
        diff -rq -x '__pycache__' "$tmp" "$repo_root/$gen_rel" >&2 || true
        exit 1
    fi
    echo "Contract stubs are up to date."
    exit 0
fi

rm -rf "${repo_root:?}/$gen_rel"
generate "$repo_root/$gen_rel"
echo "Generated stubs from $proto_root into $gen_rel"
