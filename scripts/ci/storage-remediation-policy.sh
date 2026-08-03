#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

case "${1:-}" in
    --policy)
        [[ $# -eq 2 ]] || { echo "usage: $0 --policy remediation|closure" >&2; exit 2; }
        exec python3 scripts/ci/storage-remediation-policy.py --policy "$2"
        ;;
    --mode-file)
        [[ $# -eq 2 ]] || { echo "usage: $0 --mode-file <path>" >&2; exit 2; }
        exec python3 scripts/ci/storage-remediation-policy.py --mode-file "$2"
        ;;
    --self-test)
        [[ $# -eq 1 ]] || { echo "usage: $0 --self-test" >&2; exit 2; }
        exec python3 scripts/ci/storage-remediation-policy.py --self-test
        ;;
    *)
        echo "usage: $0 --policy remediation|closure | --mode-file <path> | --self-test" >&2
        exit 2
        ;;
esac
