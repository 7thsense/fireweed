#!/usr/bin/env bash
# Deterministic static validation gate for the pqueue Helm chart.
#
# This gate is suitable for local development and GitHub Actions. It catches
# chart schema / template / Kubernetes API errors BEFORE the (expensive) kind
# install smoke tests run. It performs NO cluster operations:
#
#   1. helm lint            -- chart + values.schema.json validation per profile
#   2. helm template        -- render manifests for every supported profile
#   3. kubeconform          -- validate rendered manifests against the pinned
#                              Kubernetes API schema set
#
# kubeconform is pinned to ${KUBECONFORM_VERSION} and the rendered manifests are
# validated against Kubernetes API version ${KUBERNETES_VERSION}. If kubeconform
# is not already on PATH it is auto-installed (download is checksum-verified
# against the pinned release) into a git-ignored cache under target/.
#
# Prerequisites (see docs/deployment/helm-static-validation.md):
#   - helm (v3.8+ / v4+)
#   - curl + tar           (only when kubeconform must be auto-installed)
#   - network access to github.com (kubeconform binary + JSON schema fetch)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART_DIR="${REPO_ROOT}/charts/pqueue"
CACHE_DIR="${REPO_ROOT}/target/helm-gate/bin"

# Pinned tool / schema versions. Bump deliberately; the checksum table below
# must be updated in lockstep with KUBECONFORM_VERSION.
KUBECONFORM_VERSION="v0.6.7"
KUBERNETES_VERSION="1.31.0"

# sha256 of the kubeconform ${KUBECONFORM_VERSION} release tarballs, keyed by
# "<os>-<arch>". Sourced from the release CHECKSUMS file.
declare -A KUBECONFORM_SHA256=(
    [linux-amd64]="95f14e87aa28c09d5941f11bd024c1d02fdc0303ccaa23f61cef67bc92619d73"
    [linux-arm64]="dc82f79bb03c5479b1ae5fd4af221e4b5a3111f62bf01a2795d9c5c20fa96644"
    [darwin-amd64]="3b5324ac4fd38ac60a49823b4051ff42ff7eb70144f1e9741fed1d14bc4fdb4e"
    [darwin-arm64]="cbb47d938a8d18eb5f79cb33663b2cecdee0c8ac0bf562ebcfca903df5f0802f"
)

# Profiles to validate. Each maps to a CI values file under charts/pqueue/ci/.
PROFILES=(postgres_native object_log_sqlite_projection)

err() { echo "helm-gate: $*" >&2; }

require() {
    command -v "$1" >/dev/null 2>&1 || { err "required tool not found: $1"; exit 1; }
}

detect_platform() {
    local os arch
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    case "$(uname -m)" in
        x86_64 | amd64) arch="amd64" ;;
        aarch64 | arm64) arch="arm64" ;;
        *) err "unsupported architecture: $(uname -m)"; exit 1 ;;
    esac
    case "$os" in
        linux | darwin) ;;
        *) err "unsupported OS: ${os}"; exit 1 ;;
    esac
    PLATFORM="${os}-${arch}"
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

ensure_kubeconform() {
    # Prefer an already-installed kubeconform so local/dev setups and pre-baked
    # CI images are used as-is.
    if command -v kubeconform >/dev/null 2>&1; then
        KUBECONFORM_BIN="$(command -v kubeconform)"
        echo "--- kubeconform: using $($KUBECONFORM_BIN -v) from PATH (${KUBECONFORM_BIN}) ---"
        return
    fi

    local bin="${CACHE_DIR}/kubeconform"
    if [[ -x "$bin" ]]; then
        KUBECONFORM_BIN="$bin"
        echo "--- kubeconform: using cached $($KUBECONFORM_BIN -v) (${bin}) ---"
        return
    fi

    detect_platform
    local expected="${KUBECONFORM_SHA256[$PLATFORM]:-}"
    if [[ -z "$expected" ]]; then
        err "no pinned kubeconform checksum for platform ${PLATFORM}; install kubeconform ${KUBECONFORM_VERSION} manually and re-run"
        exit 1
    fi

    require curl
    require tar

    local url="https://github.com/yannh/kubeconform/releases/download/${KUBECONFORM_VERSION}/kubeconform-${PLATFORM}.tar.gz"
    local tmp
    tmp="$(mktemp -d)"

    echo "--- kubeconform: installing ${KUBECONFORM_VERSION} (${PLATFORM}) ---"
    echo "    ${url}"
    curl -fsSL "$url" -o "${tmp}/kubeconform.tar.gz"

    local actual
    actual="$(sha256_of "${tmp}/kubeconform.tar.gz")"
    if [[ "$actual" != "$expected" ]]; then
        err "kubeconform checksum mismatch for ${PLATFORM}"
        err "  expected: ${expected}"
        err "  actual:   ${actual}"
        exit 1
    fi

    mkdir -p "$CACHE_DIR"
    tar -xzf "${tmp}/kubeconform.tar.gz" -C "$tmp" kubeconform
    mv "${tmp}/kubeconform" "$bin"
    chmod +x "$bin"
    rm -rf "$tmp"
    KUBECONFORM_BIN="$bin"
    echo "--- kubeconform: installed $($KUBECONFORM_BIN -v) (${bin}) ---"
}

values_file_for() {
    local profile="$1"
    case "$profile" in
        postgres_native) echo "${CHART_DIR}/ci/postgres-native-values.yaml" ;;
        object_log_sqlite_projection) echo "${CHART_DIR}/ci/object-log-sqlite-projection-values.yaml" ;;
        *) err "no CI values file for profile: ${profile}"; exit 1 ;;
    esac
}

main() {
    require helm

    echo "=== helm static validation gate ==="
    echo "chart:               ${CHART_DIR}"
    echo "kubeconform version: ${KUBECONFORM_VERSION}"
    echo "kubernetes schema:   v${KUBERNETES_VERSION}"
    echo "profiles:            ${PROFILES[*]}"

    ensure_kubeconform

    for profile in "${PROFILES[@]}"; do
        local values
        values="$(values_file_for "$profile")"
        [[ -f "$values" ]] || { err "missing values file: ${values}"; exit 1; }

        echo "--- helm lint [${profile}] ---"
        helm lint "$CHART_DIR" --strict --values "$values"

        echo "--- helm template + kubeconform [${profile}] ---"
        # -strict rejects unknown fields; -kubernetes-version pins the API
        # schema set; reading from stdin keeps the render deterministic.
        helm template "pqueue-${profile//_/-}" "$CHART_DIR" --values "$values" \
            | "$KUBECONFORM_BIN" \
                -strict \
                -summary \
                -kubernetes-version "$KUBERNETES_VERSION" \
                -
    done

    echo "=== helm static validation gate PASSED ==="
}

main "$@"
