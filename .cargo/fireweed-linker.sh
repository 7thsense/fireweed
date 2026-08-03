#!/usr/bin/env bash
set -euo pipefail

if command -v clang >/dev/null 2>&1 && command -v mold >/dev/null 2>&1; then
    exec clang -fuse-ld=mold "$@"
fi

exec cc "$@"
