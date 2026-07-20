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
COMBINATIONS=(objectlog-inmemory objectlog-sqlite objectlog-hybrid objectlog-hybrid-async shared-s3-postgres-control-plane postgres-inmemory postgres-sqlite postgres-postgres lakebase-postgres)

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
        objectlog-hybrid) echo "${CHART_DIR}/ci/objectlog-hybrid-values.yaml" ;;
        objectlog-hybrid-async) echo "${CHART_DIR}/ci/objectlog-hybrid-async-values.yaml" ;;
        shared-s3-postgres-control-plane) echo "${CHART_DIR}/ci/shared-s3-postgres-control-plane-values.yaml" ;;
        postgres-inmemory) echo "${CHART_DIR}/ci/postgres-inmemory-values.yaml" ;;
        postgres-sqlite) echo "${CHART_DIR}/ci/postgres-sqlite-values.yaml" ;;
        postgres-postgres) echo "${CHART_DIR}/ci/postgres-postgres-values.yaml" ;;
        lakebase-postgres) echo "${CHART_DIR}/ci/lakebase-postgres-values.yaml" ;;
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

assert_objectlog_hybrid_contract() {
    local rendered="$1"

    assert_contains "$rendered" 'PQUEUE_LOG_BACKEND: "objectlog"' "objectlog log axis"
    assert_contains "$rendered" 'PQUEUE_PROJECTION_BACKEND: "hybrid"' "hybrid projection axis"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_ROOT: "/var/lib/pqueue/projection/object-log"' "object-log root"
    assert_contains "$rendered" 'PQUEUE_SQLITE_PROJECTION_PATH: "/var/lib/pqueue/projection/projection.db"' "hybrid sqlite projection path"
    assert_contains "$rendered" 'kind: PersistentVolumeClaim' "storage PVC"
    assert_contains "$rendered" 'name: storage' "storage volume"
    assert_no_fixture_credentials "$rendered" "objectlog/hybrid rendered manifest"
}

assert_objectlog_hybrid_async_contract() {
    local rendered="$1"

    assert_contains "$rendered" 'PQUEUE_LOG_BACKEND: "objectlog"' "objectlog log axis"
    assert_contains "$rendered" 'PQUEUE_PROJECTION_BACKEND: "hybrid-async"' "hybrid-async projection axis"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_ROOT: "/var/lib/pqueue/projection/object-log"' "object-log root"
    assert_contains "$rendered" 'PQUEUE_SQLITE_PROJECTION_PATH: "/var/lib/pqueue/projection/projection.db"' "hybrid-async sqlite projection path"
    assert_contains "$rendered" 'PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS: "100000"' "hybrid-async command-lag bound"
    assert_contains "$rendered" 'PQUEUE_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES: "536870912"' "hybrid-async byte-debt bound"
    assert_contains "$rendered" 'PQUEUE_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX: "1024"' "hybrid-async queue-depth bound"
    assert_contains "$rendered" 'PQUEUE_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS: "60000"' "hybrid-async oldest-unapplied bound"
    assert_contains "$rendered" 'PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD: "3"' "hybrid-async poison retry threshold"
    assert_contains "$rendered" 'kind: PersistentVolumeClaim' "storage PVC"
    assert_contains "$rendered" 'name: storage' "storage volume"
    assert_contains "$rendered" 'mountPath: "/var/lib/pqueue/projection"' "hybrid-async projection volume mount"
    assert_not_contains "$rendered" 'PQUEUE_BACKEND_PROFILE' "legacy profile env"
    assert_no_fixture_credentials "$rendered" "objectlog/hybrid-async rendered manifest"
}

assert_shared_s3_postgres_control_plane_contract() {
    local rendered="$1"

    assert_contains "$rendered" 'replicas: 3' "shared profile replica count"
    assert_contains "$rendered" 'PQUEUE_REPLICA_COUNT: "3"' "replica count env"
    assert_contains "$rendered" 'PQUEUE_LOG_BACKEND: "objectlog"' "objectlog log axis"
    assert_contains "$rendered" 'PQUEUE_CONTROL_PLANE: "postgres"' "postgres control-plane axis"
    assert_contains "$rendered" 'PQUEUE_PROJECTION_BACKEND: "sqlite"' "sqlite projection axis"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_STORE: "s3"' "shared object-log store selection"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_S3_ENDPOINT: "https://s3.example.com"' "S3 endpoint"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_S3_BUCKET: "pqueue-shared"' "S3 bucket"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_S3_REGION: "us-east-1"' "S3 region"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_S3_CREDENTIAL_SOURCE: "static"' "S3 credential source"
    assert_contains "$rendered" 'PQUEUE_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP: "false"' "S3 TLS setting"
    assert_contains "$rendered" 'PQUEUE_CONTROL_PLANE_HEARTBEAT_TTL_MS: "5000"' "control-plane heartbeat ttl"
    assert_contains "$rendered" 'PQUEUE_CONTROL_PLANE_LEASE_TTL_MS: "15000"' "control-plane lease ttl"
    assert_contains "$rendered" 'PQUEUE_SQLITE_PROJECTION_PATH: "/var/lib/pqueue/projection/projection.db"' "pod-local SQLite path"
    assert_contains "$rendered" 'name: PQUEUE_OBJECT_LOG_S3_ACCESS_KEY_ID' "S3 access key secret env"
    assert_contains "$rendered" 'name: PQUEUE_OBJECT_LOG_S3_SECRET_ACCESS_KEY' "S3 secret key secret env"
    assert_contains "$rendered" 'name: PQUEUE_POSTGRES_CONTROL_PLANE_DATABASE_URL' "postgres control-plane secret env"
    assert_contains "$rendered" 'name: PQUEUE_ADVERTISE_ADDR' "pod-reachable endpoint env"
    assert_contains "$rendered" 'fieldPath: status.podIP' "pod IP downward API"
    assert_contains "$rendered" 'value: "$(POD_IP):8080"' "pod-reachable endpoint value"
    assert_contains "$rendered" 'name: PQUEUE_OWNER_ID' "full-width per-pod owner identity env"
    assert_contains "$rendered" 'fieldPath: metadata.uid' "unique owner identity downward API"
    assert_not_contains "$rendered" 'PQUEUE_OWNER_ID:' "static shared owner identity"
    assert_not_contains "$rendered" 'PQUEUE_OBJECT_LOG_ROOT' "shared object-log root"
    assert_not_contains "$rendered" 'kind: PersistentVolumeClaim' "shared PVC"
    assert_contains "$rendered" 'emptyDir: {}' "pod-local projection volume"
    assert_no_fixture_credentials "$rendered" "shared S3/postgres rendered manifest"
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

assert_lakebase_postgres_contract() {
    local rendered="$1"

    # The binary connects to Lakebase from the self-sufficient log DSN alone (host/port/db +
    # sslmode=require in the Secret). It does NOT read PQUEUE_LAKEBASE_* metadata, and the only
    # wired postgres combination is log=postgres + projection=inmemory, so the Lakebase profile
    # renders neither PQUEUE_LAKEBASE_* nor a projection DSN.
    assert_postgres_contract "$rendered" "inmemory"
    assert_not_contains "$rendered" 'PQUEUE_LAKEBASE_' "Lakebase metadata env (binary ignores it)"
    assert_not_contains "$rendered" 'name: PQUEUE_POSTGRES_PROJECTION_DATABASE_URL' "projection DSN env (binary ignores it)"
    assert_contains "$rendered" 'name: DATABRICKS_HOST' "Databricks host Secret env"
    assert_contains "$rendered" 'name: PQUEUE_DATABRICKS_DATABASE_INSTANCE_NAME' "Databricks instance Secret env"
    assert_contains "$rendered" 'name: DATABRICKS_CLIENT_ID' "Databricks service principal client id"
    assert_contains "$rendered" 'name: DATABRICKS_CLIENT_SECRET' "Databricks service principal client secret"
    assert_contains "$rendered" 'name: "pqueue-lakebase-dsn"' "Lakebase DSN Secret"
    assert_contains "$rendered" 'name: "pqueue-lakebase-oauth"' "Lakebase OAuth Secret"
    assert_not_contains "$rendered" 'password=' "inline Lakebase password"
    assert_no_fixture_credentials "$rendered" "Lakebase rendered manifest"
}

assert_generated_bootstrap_contract() {
    local rendered
    rendered="$(mktemp)"
    helm template pqueue-density "$CHART_DIR" \
        --set bootstrap.generated.count=1001 \
        --set bootstrap.generated.tenant=density \
        --set bootstrap.generated.prefix=q >"$rendered"

    assert_contains "$rendered" 'PQUEUE_BOOTSTRAP_GENERATED_COUNT: "1001"' "generated bootstrap count"
    assert_contains "$rendered" 'PQUEUE_BOOTSTRAP_GENERATED_TENANT: "density"' "generated bootstrap tenant"
    assert_contains "$rendered" 'PQUEUE_BOOTSTRAP_GENERATED_PREFIX: "q"' "generated bootstrap prefix"
    assert_not_contains "$rendered" 'PQUEUE_BOOTSTRAP_QUEUES:' "explicit bootstrap list when generation is selected"
    rm -f "$rendered"
}

assert_hybrid_strict_schema_exclusion() {
    local output
    output="$(mktemp)"

    if helm template pqueue-hybrid-strict "$CHART_DIR" \
        --set storage.log.backend=objectlog \
        --set storage.projection.backend=hybrid-strict >"$output" 2>&1; then
        err "objectlog/hybrid-strict unexpectedly rendered; the profile is runtime-only and must remain outside the chart schema"
        cat "$output" >&2
        rm -f "$output"
        exit 1
    fi

    # Helm 3 and Helm 4 format schema failures differently. Require the exact
    # path and allowed enum from either formatter so a schema expansion, a
    # template-time rejection, or an unrelated render failure cannot satisfy
    # this public-support boundary.
    local helm4_error="- at '/storage/projection/backend': value must be one of 'inmemory', 'sqlite', 'hybrid', 'hybrid-async', 'postgres'"
    local helm3_error='storage.projection.backend: storage.projection.backend must be one of the following: "inmemory", "sqlite", "hybrid", "hybrid-async", "postgres"'
    if ! grep -Fq -- "$helm4_error" "$output" && ! grep -Fq -- "$helm3_error" "$output"; then
        err "objectlog/hybrid-strict did not fail with the exact projection enum-exclusion error"
        cat "$output" >&2
        rm -f "$output"
        exit 1
    fi

    rm -f "$output"
}

assert_combination_contract() {
    local combination="$1"
    local rendered="$2"

    echo "--- rendered contract assertions [${combination}] ---"
    case "$combination" in
        objectlog-inmemory) assert_objectlog_inmemory_contract "$rendered" ;;
        objectlog-sqlite) assert_objectlog_sqlite_contract "$rendered" ;;
        objectlog-hybrid) assert_objectlog_hybrid_contract "$rendered" ;;
        objectlog-hybrid-async) assert_objectlog_hybrid_async_contract "$rendered" ;;
        shared-s3-postgres-control-plane) assert_shared_s3_postgres_control_plane_contract "$rendered" ;;
        postgres-inmemory) assert_postgres_contract "$rendered" "inmemory" ;;
        postgres-sqlite) assert_postgres_contract "$rendered" "sqlite" ;;
        postgres-postgres) assert_postgres_contract "$rendered" "postgres" ;;
        lakebase-postgres) assert_lakebase_postgres_contract "$rendered" ;;
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

    echo "--- generated bootstrap inventory contract ---"
    assert_generated_bootstrap_contract

    echo "--- objectlog/hybrid-strict chart exclusion contract ---"
    assert_hybrid_strict_schema_exclusion

    echo "--- local profile fail-closed contract ---"
    local scaled_local
    scaled_local="$(mktemp)"
    if helm template pqueue-local-scaled "$CHART_DIR" --values "${CHART_DIR}/ci/objectlog-sqlite-values.yaml" --set replicaCount=2 >"$scaled_local" 2>&1; then
        err "scaled local objectlog/sqlite profile unexpectedly rendered"
        cat "$scaled_local" >&2
        rm -f "$scaled_local"
        exit 1
    fi
    assert_contains "$scaled_local" 'replicaCount > 1 requires storage.log.backend=objectlog' "scaled local fail-closed message"
    rm -f "$scaled_local"

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
