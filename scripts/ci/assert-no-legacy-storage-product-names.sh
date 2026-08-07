#!/usr/bin/env bash
# Fail if legacy storage product names appear on public product surfaces.
#
# Public product axes (only):
#   log:        memory | sqlite | postgres | filesystem | s3
#   projection: memory | sqlite | turso | postgres
#
# Hard-rejected legacy product names (no long-lived aliases):
#   objectlog, inmemory, hybrid, hybrid-async, hybrid-strict
#
# turso is a public default projection (TD-010 / ADR-016) and must remain
# accepted on chart schema, values, and env surfaces.
#
# Scans operator-facing docs, chart schema/defaults, and env default literals.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# Product-selection residue: assignment/defaults that still pick legacy names.
# Documentation may *name* rejected aliases when describing exclusion; only
# selection forms (env/Helm assignment, YAML backend: values) fail the scan.
# Library crate / API symbols (open_objectlog, fireweed-objectlog, objectLog.*)
# are out of scope.
PATTERNS=(
    'FIREWEED_LOG_BACKEND[[:space:]]*=[[:space:]]*objectlog'
    'FIREWEED_PROJECTION_BACKEND[[:space:]]*=[[:space:]]*(inmemory|hybrid(-async|-strict)?)'
    'storage\.log\.backend[[:space:]]*=[[:space:]]*objectlog'
    'storage\.projection\.backend[[:space:]]*=[[:space:]]*(inmemory|hybrid(-async|-strict)?)'
    'backend:[[:space:]]*objectlog\b'
    'backend:[[:space:]]*inmemory\b'
    'backend:[[:space:]]*hybrid(-async|-strict)?\b'
    # Public option lists that re-admit demoted projection aliases as selectable.
    'memory[[:space:]]*\|[[:space:]]*sqlite[[:space:]]*\|[[:space:]]*hybrid'
    'memory[[:space:]]*,[[:space:]]*sqlite[[:space:]]*,[[:space:]]*hybrid'
)

PUBLIC_PATHS=(
    docs/deployment
    charts/fireweed-queue/values.schema.json
    charts/fireweed-queue/values.yaml
    charts/fireweed-queue/README.md
    charts/fireweed-queue/ci
    charts/fireweed-queue/templates
)

# Schema enums must never re-admit legacy product names.
# turso is public and intentionally present in the projection enum.
SCHEMA_ENUM_PATTERN='"objectlog"|"inmemory"|"hybrid-async"|"hybrid-strict"|"hybrid"'

# Env adapter product defaults must stay on public axes.
ENV_DEFAULT_PATH="crates/fireweed-server/src/env_config.rs"

failures=0

scan_paths() {
    local pattern="$1"
    shift
    local paths=("$@")
    local existing=()
    local p
    for p in "${paths[@]}"; do
        if [[ -e "$p" ]]; then
            existing+=("$p")
        fi
    done
    if ((${#existing[@]} == 0)); then
        return 0
    fi
    if rg -n --glob '!**/.git/**' --glob '!**/node_modules/**' -e "${pattern}" "${existing[@]}"; then
        echo "assert-no-legacy-storage-product-names: forbidden product surface pattern: ${pattern}" >&2
        failures=$((failures + 1))
    fi
}

echo "=== assert-no-legacy-storage-product-names ==="

for pattern in "${PATTERNS[@]}"; do
    scan_paths "${pattern}" "${PUBLIC_PATHS[@]}"
done

if [[ -f charts/fireweed-queue/values.schema.json ]]; then
    if rg -n -e "${SCHEMA_ENUM_PATTERN}" charts/fireweed-queue/values.schema.json; then
        echo "assert-no-legacy-storage-product-names: values.schema.json must not enumerate legacy product names" >&2
        failures=$((failures + 1))
    fi
    if ! rg -n '"turso"' charts/fireweed-queue/values.schema.json >/dev/null; then
        echo "assert-no-legacy-storage-product-names: values.schema.json must enumerate public turso projection" >&2
        failures=$((failures + 1))
    fi
fi

if [[ -f charts/fireweed-queue/values.yaml ]]; then
    if ! awk '
        /^  projection:/ { in_proj=1; next }
        in_proj && /^  [a-z]/ { in_proj=0 }
        in_proj && /^    backend:[[:space:]]*turso[[:space:]]*$/ { found=1 }
        END { exit(found ? 0 : 1) }
    ' charts/fireweed-queue/values.yaml; then
        echo "assert-no-legacy-storage-product-names: values.yaml must default storage.projection.backend to turso" >&2
        failures=$((failures + 1))
    fi
fi

if [[ -f "${ENV_DEFAULT_PATH}" ]]; then
    if ! rg -n 'env_or\(env, "FIREWEED_LOG_BACKEND", "filesystem"\)' "${ENV_DEFAULT_PATH}" >/dev/null; then
        echo "assert-no-legacy-storage-product-names: missing public FIREWEED_LOG_BACKEND default filesystem in ${ENV_DEFAULT_PATH}" >&2
        failures=$((failures + 1))
    fi
    if ! rg -n 'env_or\(env, "FIREWEED_PROJECTION_BACKEND", "turso"\)' "${ENV_DEFAULT_PATH}" >/dev/null; then
        echo "assert-no-legacy-storage-product-names: missing public FIREWEED_PROJECTION_BACKEND default turso in ${ENV_DEFAULT_PATH}" >&2
        failures=$((failures + 1))
    fi
    # Defaults must not reintroduce legacy product names.
    if rg -n 'env_or\(env, "FIREWEED_LOG_BACKEND", "objectlog"\)|env_or\(env, "FIREWEED_PROJECTION_BACKEND", "inmemory"\)' "${ENV_DEFAULT_PATH}"; then
        echo "assert-no-legacy-storage-product-names: env defaults still use legacy product names" >&2
        failures=$((failures + 1))
    fi
fi

if ((failures > 0)); then
    echo "assert-no-legacy-storage-product-names: FAILED (${failures} check(s))" >&2
    exit 1
fi

echo "assert-no-legacy-storage-product-names: OK (no legacy product names on public surfaces)"
