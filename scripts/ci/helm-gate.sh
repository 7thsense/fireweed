#!/usr/bin/env bash
# Deterministic static validation gate for the pqueue Helm chart.
#
# This gate is suitable for local development and GitHub Actions. It catches
# chart schema / template / Kubernetes API errors BEFORE the (expensive) kind
# install smoke tests run. It performs NO cluster operations:
#
#   1. helm lint            -- chart + values.schema.json validation per storage combination
#   2. helm template        -- render manifests for supported storage combinations
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
PACKAGE_DIR="${REPO_ROOT}/target/helm-gate/release-dist"

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

# Storage combinations to validate. Each maps to a CI values file under charts/pqueue/ci/.
COMBINATIONS=(objectlog-inmemory objectlog-sqlite postgres-inmemory postgres-sqlite postgres-postgres)

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
    local combination="$1"
    case "$combination" in
        objectlog-inmemory) echo "${CHART_DIR}/ci/objectlog-inmemory-values.yaml" ;;
        objectlog-sqlite) echo "${CHART_DIR}/ci/objectlog-sqlite-values.yaml" ;;
        postgres-inmemory) echo "${CHART_DIR}/ci/postgres-inmemory-values.yaml" ;;
        postgres-sqlite) echo "${CHART_DIR}/ci/postgres-sqlite-values.yaml" ;;
        postgres-postgres) echo "${CHART_DIR}/ci/postgres-postgres-values.yaml" ;;
        *) err "no CI values file for storage combination: ${combination}"; exit 1 ;;
    esac
}

assert_contains() {
    local file="$1"
    local needle="$2"
    local description="$3"
    if ! grep -Fq -- "$needle" "$file"; then
        err "missing ${description}: ${needle}"
        exit 1
    fi
}

assert_not_contains() {
    local file="$1"
    local needle="$2"
    local description="$3"
    if grep -Fq -- "$needle" "$file"; then
        err "unexpected ${description}: ${needle}"
        exit 1
    fi
}

assert_no_fixture_credentials() {
    local file="$1"
    local description="$2"
    local forbidden
    for forbidden in \
        "minioadmin" \
        "minioadmin-secret" \
        "postgres://pqueue:pqueue@postgres:5432/pqueue"
    do
        assert_not_contains "$file" "$forbidden" "${description} fixture credential"
    done
}

assert_objectlog_inmemory_contract() {
    local rendered="$1"

    assert_contains "$rendered" 'PQUEUE_LOG_BACKEND: "objectlog"' "objectlog log axis"
    assert_contains "$rendered" 'PQUEUE_PROJECTION_BACKEND: "inmemory"' "in-memory projection axis"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_ROOT: "/var/lib/pqueue/projection/object-log"' "object-log root"
    assert_contains "$rendered" 'kind: PersistentVolumeClaim' "storage PVC"
    assert_contains "$rendered" 'name: storage' "storage volume"
    assert_contains "$rendered" 'mountPath: "/var/lib/pqueue/projection"' "SQLite projection volume mount"
    assert_not_contains "$rendered" 'PQUEUE_SQLITE_PROJECTION_PATH' "sqlite projection path"
    assert_not_contains "$rendered" 'name: PQUEUE_POSTGRES_DATABASE_URL' "Postgres env"
    assert_no_fixture_credentials "$rendered" "object-log rendered manifest"
}

assert_objectlog_sqlite_contract() {
    local rendered="$1"

    assert_contains "$rendered" 'PQUEUE_LOG_BACKEND: "objectlog"' "objectlog log axis"
    assert_contains "$rendered" 'PQUEUE_PROJECTION_BACKEND: "sqlite"' "sqlite projection axis"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_ROOT: "/var/lib/pqueue/projection/object-log"' "object-log root"
    assert_contains "$rendered" 'PQUEUE_SQLITE_PROJECTION_PATH: "/var/lib/pqueue/projection/projection.db"' "sqlite projection path"
    assert_contains "$rendered" 'kind: PersistentVolumeClaim' "storage PVC"
    assert_contains "$rendered" 'name: storage' "storage volume"
    assert_no_fixture_credentials "$rendered" "objectlog/sqlite rendered manifest"
}

assert_postgres_contract() {
    local rendered="$1"
    local projection="$2"

    assert_contains "$rendered" 'PQUEUE_LOG_BACKEND: "postgres"' "postgres log axis"
    assert_contains "$rendered" "PQUEUE_PROJECTION_BACKEND: \"${projection}\"" "${projection} projection axis"
    assert_contains "$rendered" 'name: PQUEUE_POSTGRES_LOG_DATABASE_URL' "postgres log env"
    assert_contains "$rendered" 'secretKeyRef:' "postgres Secret reference"
    if [[ "$projection" == "postgres" ]]; then
        assert_contains "$rendered" 'name: PQUEUE_POSTGRES_PROJECTION_DATABASE_URL' "postgres projection env"
    fi
    if [[ "$projection" == "sqlite" ]]; then
        assert_contains "$rendered" 'PQUEUE_SQLITE_PROJECTION_PATH: "/var/lib/pqueue/projection/projection.db"' "sqlite projection path"
    fi
    assert_not_contains "$rendered" 'PQUEUE_BACKEND_PROFILE' "legacy profile env"
    assert_no_fixture_credentials "$rendered" "postgres rendered manifest"
}

assert_combination_contract() {
    local combination="$1"
    local rendered="$2"

    echo "--- rendered contract assertions [${combination}] ---"
    case "$combination" in
        objectlog-inmemory) assert_objectlog_inmemory_contract "$rendered" ;;
        objectlog-sqlite) assert_objectlog_sqlite_contract "$rendered" ;;
        postgres-inmemory) assert_postgres_contract "$rendered" "inmemory" ;;
        postgres-sqlite) assert_postgres_contract "$rendered" "sqlite" ;;
        postgres-postgres) assert_postgres_contract "$rendered" "postgres" ;;
        *) err "no rendered contract assertions for storage combination: ${combination}"; exit 1 ;;
    esac
}

main() {
    require helm

    echo "=== helm static validation gate ==="
    echo "chart:               ${CHART_DIR}"
    echo "kubeconform version: ${KUBECONFORM_VERSION}"
    echo "kubernetes schema:   v${KUBERNETES_VERSION}"
    echo "storage combinations: ${COMBINATIONS[*]}"

    ensure_kubeconform
    assert_no_fixture_credentials "${CHART_DIR}/values.yaml" "chart default values"

    echo "--- helm package ---"
    rm -rf "$PACKAGE_DIR"
    mkdir -p "$PACKAGE_DIR"
    helm package "$CHART_DIR" --destination "$PACKAGE_DIR"

    for combination in "${COMBINATIONS[@]}"; do
        local values
        values="$(values_file_for "$combination")"
        [[ -f "$values" ]] || { err "missing values file: ${values}"; exit 1; }

        echo "--- helm lint [${combination}] ---"
        helm lint "$CHART_DIR" --strict --values "$values"

        echo "--- helm template + kubeconform [${combination}] ---"
        # -strict rejects unknown fields; -kubernetes-version pins the API
        # schema set; reading from stdin keeps the render deterministic.
        local rendered
        rendered="$(mktemp)"
        helm template "pqueue-${combination}" "$CHART_DIR" --values "$values" >"$rendered"
        assert_combination_contract "$combination" "$rendered"
        "$KUBECONFORM_BIN" \
                -strict \
                -summary \
                -kubernetes-version "$KUBERNETES_VERSION" \
                - <"$rendered"
        rm -f "$rendered"
    done

    echo "=== helm static validation gate PASSED ==="
}

main "$@"
