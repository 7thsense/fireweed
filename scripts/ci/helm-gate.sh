#!/usr/bin/env bash
# Deterministic static validation gate for the Fireweed Queue Helm chart.
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
CHART_DIR="${REPO_ROOT}/charts/fireweed-queue"
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

# Storage combinations to validate. Each maps to a CI values file under charts/fireweed-queue/ci/.
# Public axes only: logs memory|sqlite|postgres|filesystem|s3; projections memory|sqlite|turso|postgres.
# Full 20-cell matrix fixtures (plus shared multi-replica S3/control-plane and lakebase variants).
# MATRIX_COMBINATIONS is the injective map onto the 20 canonical cell IDs (log--projection).
MATRIX_COMBINATIONS=(
    memory-memory memory-sqlite memory-turso memory-postgres
    sqlite-memory sqlite-sqlite sqlite-turso sqlite-postgres
    postgres-memory postgres-sqlite postgres-turso postgres-postgres
    filesystem-memory filesystem-sqlite filesystem-turso filesystem-postgres
    s3-memory s3-sqlite s3-turso s3-postgres
)
VARIANT_COMBINATIONS=(
    shared-s3-postgres-control-plane
    s3-sqlite-postgres-control-plane
    lakebase-postgres
)
COMBINATIONS=("${MATRIX_COMBINATIONS[@]}" "${VARIANT_COMBINATIONS[@]}")

# Canonical cell ID separator from storage-authority-manifest.json.
CELL_ID_SEP="--"

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
        memory-memory) echo "${CHART_DIR}/ci/memory-memory-values.yaml" ;;
        memory-sqlite) echo "${CHART_DIR}/ci/memory-sqlite-values.yaml" ;;
        memory-turso) echo "${CHART_DIR}/ci/memory-turso-values.yaml" ;;
        memory-postgres) echo "${CHART_DIR}/ci/memory-postgres-values.yaml" ;;
        filesystem-memory) echo "${CHART_DIR}/ci/filesystem-memory-values.yaml" ;;
        filesystem-sqlite) echo "${CHART_DIR}/ci/filesystem-sqlite-values.yaml" ;;
        filesystem-turso) echo "${CHART_DIR}/ci/filesystem-turso-values.yaml" ;;
        filesystem-postgres) echo "${CHART_DIR}/ci/filesystem-postgres-values.yaml" ;;
        sqlite-memory) echo "${CHART_DIR}/ci/sqlite-memory-values.yaml" ;;
        sqlite-sqlite) echo "${CHART_DIR}/ci/sqlite-sqlite-values.yaml" ;;
        sqlite-turso) echo "${CHART_DIR}/ci/sqlite-turso-values.yaml" ;;
        sqlite-postgres) echo "${CHART_DIR}/ci/sqlite-postgres-values.yaml" ;;
        s3-memory) echo "${CHART_DIR}/ci/s3-memory-values.yaml" ;;
        s3-sqlite) echo "${CHART_DIR}/ci/s3-sqlite-values.yaml" ;;
        s3-turso) echo "${CHART_DIR}/ci/s3-turso-values.yaml" ;;
        s3-postgres) echo "${CHART_DIR}/ci/s3-postgres-values.yaml" ;;
        shared-s3-postgres-control-plane) echo "${CHART_DIR}/ci/shared-s3-postgres-control-plane-values.yaml" ;;
        s3-sqlite-postgres-control-plane) echo "${CHART_DIR}/ci/s3-sqlite-postgres-control-plane-values.yaml" ;;
        postgres-memory) echo "${CHART_DIR}/ci/postgres-memory-values.yaml" ;;
        postgres-sqlite) echo "${CHART_DIR}/ci/postgres-sqlite-values.yaml" ;;
        postgres-turso) echo "${CHART_DIR}/ci/postgres-turso-values.yaml" ;;
        postgres-postgres) echo "${CHART_DIR}/ci/postgres-postgres-values.yaml" ;;
        lakebase-postgres) echo "${CHART_DIR}/ci/lakebase-postgres-values.yaml" ;;
        *) err "no CI values file for storage combination: ${combination}"; exit 1 ;;
    esac
}

# Map a matrix combination name (log-projection) to the canonical cell_id (log--projection).
canonical_cell_id_for() {
    local combination="$1"
    local log proj
    case "$combination" in
        memory-memory) log=memory; proj=memory ;;
        memory-sqlite) log=memory; proj=sqlite ;;
        memory-turso) log=memory; proj=turso ;;
        memory-postgres) log=memory; proj=postgres ;;
        sqlite-memory) log=sqlite; proj=memory ;;
        sqlite-sqlite) log=sqlite; proj=sqlite ;;
        sqlite-turso) log=sqlite; proj=turso ;;
        sqlite-postgres) log=sqlite; proj=postgres ;;
        postgres-memory) log=postgres; proj=memory ;;
        postgres-sqlite) log=postgres; proj=sqlite ;;
        postgres-turso) log=postgres; proj=turso ;;
        postgres-postgres) log=postgres; proj=postgres ;;
        filesystem-memory) log=filesystem; proj=memory ;;
        filesystem-sqlite) log=filesystem; proj=sqlite ;;
        filesystem-turso) log=filesystem; proj=turso ;;
        filesystem-postgres) log=filesystem; proj=postgres ;;
        s3-memory) log=s3; proj=memory ;;
        s3-sqlite) log=s3; proj=sqlite ;;
        s3-turso) log=s3; proj=turso ;;
        s3-postgres) log=s3; proj=postgres ;;
        *) err "not a canonical matrix combination: ${combination}"; exit 1 ;;
    esac
    printf '%s%s%s\n' "$log" "$CELL_ID_SEP" "$proj"
}

assert_projection_path_contract() {
    local rendered="$1"
    local projection="$2"

    if [[ "$projection" == "sqlite" ]]; then
        assert_contains "$rendered" 'FIREWEED_SQLITE_PROJECTION_PATH: "/var/lib/fireweed/projection/projection.db"' "sqlite projection path"
        assert_not_contains "$rendered" 'FIREWEED_TURSO_PROJECTION_PATH' "turso path on sqlite projection"
    fi
    if [[ "$projection" == "turso" ]]; then
        assert_contains "$rendered" 'FIREWEED_TURSO_PROJECTION_PATH: "/var/lib/fireweed/projection/projection.turso"' "turso projection path"
        assert_not_contains "$rendered" 'FIREWEED_SQLITE_PROJECTION_PATH' "sqlite path on turso projection"
    fi
    if [[ "$projection" == "postgres" ]]; then
        assert_contains "$rendered" 'name: FIREWEED_POSTGRES_PROJECTION_DATABASE_URL' "postgres projection DSN env"
        assert_not_contains "$rendered" 'FIREWEED_TURSO_PROJECTION_PATH' "turso path on postgres projection"
    fi
    if [[ "$projection" == "memory" ]]; then
        assert_not_contains "$rendered" 'FIREWEED_SQLITE_PROJECTION_PATH' "sqlite projection path on memory projection"
        assert_not_contains "$rendered" 'FIREWEED_TURSO_PROJECTION_PATH' "turso projection path on memory projection"
    fi
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
        "postgres://fireweed:fireweed@postgres:5432/fireweed"
    do
        assert_not_contains "$file" "$forbidden" "${description} fixture credential"
    done
}

# Class B memory log cells (memory × {memory,sqlite,turso,postgres}).
assert_memory_log_contract() {
    local rendered="$1"
    local projection="$2"

    assert_contains "$rendered" 'FIREWEED_LOG_BACKEND: "memory"' "memory log axis"
    assert_contains "$rendered" "FIREWEED_PROJECTION_BACKEND: \"${projection}\"" "${projection} projection axis"
    assert_not_contains "$rendered" 'FIREWEED_OBJECT_LOG_ROOT' "filesystem root on memory log"
    assert_not_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_' "S3 object-log env on memory log"
    assert_not_contains "$rendered" 'FIREWEED_SQLITE_LOG_PATH' "sqlite log path on memory log"
    assert_not_contains "$rendered" 'FIREWEED_BACKEND_PROFILE' "legacy profile env"
    assert_projection_path_contract "$rendered" "$projection"
    if [[ "$projection" == "sqlite" || "$projection" == "turso" ]]; then
        assert_contains "$rendered" 'kind: PersistentVolumeClaim' "storage PVC for durable local projection"
        assert_contains "$rendered" 'name: storage' "storage volume for durable local projection"
    fi
    assert_no_fixture_credentials "$rendered" "memory/${projection} rendered manifest"
}

# Class A filesystem log × projection cells.
assert_filesystem_cell_contract() {
    local rendered="$1"
    local projection="$2"

    assert_contains "$rendered" 'FIREWEED_LOG_BACKEND: "filesystem"' "filesystem log axis"
    assert_contains "$rendered" "FIREWEED_PROJECTION_BACKEND: \"${projection}\"" "${projection} projection axis"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_ROOT: "/var/lib/fireweed/projection/object-log"' "filesystem object-log root"
    assert_contains "$rendered" 'kind: PersistentVolumeClaim' "storage PVC"
    assert_contains "$rendered" 'name: storage' "storage volume"
    assert_contains "$rendered" 'mountPath: "/var/lib/fireweed/projection"' "filesystem volume mount"
    assert_not_contains "$rendered" 'FIREWEED_OBJECT_LOG_STORE' "legacy objectlog store env on first-class filesystem"
    assert_projection_path_contract "$rendered" "$projection"
    assert_no_fixture_credentials "$rendered" "filesystem/${projection} rendered manifest"
}

assert_sqlite_log_contract() {
    local rendered="$1"
    local projection="$2"

    assert_contains "$rendered" 'FIREWEED_LOG_BACKEND: "sqlite"' "sqlite log axis"
    assert_contains "$rendered" "FIREWEED_PROJECTION_BACKEND: \"${projection}\"" "${projection} projection axis"
    assert_contains "$rendered" 'FIREWEED_SQLITE_LOG_PATH: "/var/lib/fireweed/projection/fireweed-log.db"' "sqlite log path"
    assert_contains "$rendered" 'kind: PersistentVolumeClaim' "storage PVC"
    assert_contains "$rendered" 'name: storage' "storage volume"
    assert_projection_path_contract "$rendered" "$projection"
    assert_not_contains "$rendered" 'FIREWEED_BACKEND_PROFILE' "legacy profile env"
    assert_no_fixture_credentials "$rendered" "sqlite/${projection} rendered manifest"
}

# Single-replica chart-installable s3 log cells (s3 × {memory,sqlite,turso,postgres}).
assert_s3_cell_contract() {
    local rendered="$1"
    local projection="$2"

    assert_contains "$rendered" 'FIREWEED_LOG_BACKEND: "s3"' "s3 log axis"
    assert_contains "$rendered" "FIREWEED_PROJECTION_BACKEND: \"${projection}\"" "${projection} projection axis"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_ENDPOINT: "https://s3.example.com"' "S3 endpoint"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_BUCKET: "fireweed-matrix"' "S3 bucket"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_REGION: "us-east-1"' "S3 region"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE: "static"' "S3 credential source"
    assert_contains "$rendered" 'name: FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID' "S3 access key secret env"
    assert_contains "$rendered" 'name: FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY' "S3 secret key secret env"
    assert_not_contains "$rendered" 'FIREWEED_OBJECT_LOG_STORE' "legacy objectlog store on first-class s3"
    assert_not_contains "$rendered" 'FIREWEED_OBJECT_LOG_ROOT' "filesystem root on s3 cell"
    assert_projection_path_contract "$rendered" "$projection"
    assert_no_fixture_credentials "$rendered" "s3/${projection} rendered manifest"
}

assert_shared_s3_postgres_control_plane_contract() {
    local rendered="$1"

    assert_contains "$rendered" 'replicas: 3' "shared profile replica count"
    assert_contains "$rendered" 'FIREWEED_REPLICA_COUNT: "3"' "replica count env"
    assert_contains "$rendered" 'FIREWEED_LOG_BACKEND: "s3"' "s3 log axis"
    assert_contains "$rendered" 'FIREWEED_CONTROL_PLANE: "postgres"' "postgres control-plane axis"
    assert_contains "$rendered" 'FIREWEED_PROJECTION_BACKEND: "sqlite"' "sqlite projection axis"
    assert_not_contains "$rendered" 'FIREWEED_OBJECT_LOG_STORE' "legacy store env"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_ENDPOINT: "https://s3.example.com"' "S3 endpoint"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_BUCKET: "fireweed-shared"' "S3 bucket"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_REGION: "us-east-1"' "S3 region"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE: "static"' "S3 credential source"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP: "false"' "S3 TLS setting"
    assert_contains "$rendered" 'FIREWEED_CONTROL_PLANE_HEARTBEAT_TTL_MS: "5000"' "control-plane heartbeat ttl"
    assert_contains "$rendered" 'FIREWEED_CONTROL_PLANE_LEASE_TTL_MS: "15000"' "control-plane lease ttl"
    assert_contains "$rendered" 'FIREWEED_SQLITE_PROJECTION_PATH: "/var/lib/fireweed/projection/projection.db"' "pod-local SQLite path"
    assert_contains "$rendered" 'name: FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID' "S3 access key secret env"
    assert_contains "$rendered" 'name: FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY' "S3 secret key secret env"
    assert_contains "$rendered" 'name: FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL' "postgres control-plane secret env"
    assert_contains "$rendered" 'name: FIREWEED_ADVERTISE_ADDR' "pod-reachable endpoint env"
    assert_contains "$rendered" 'fieldPath: status.podIP' "pod IP downward API"
    assert_contains "$rendered" 'value: "$(POD_IP):8080"' "pod-reachable endpoint value"
    assert_contains "$rendered" 'name: FIREWEED_OWNER_ID' "full-width per-pod owner identity env"
    assert_contains "$rendered" 'fieldPath: metadata.uid' "unique owner identity downward API"
    assert_not_contains "$rendered" 'FIREWEED_OWNER_ID:' "static shared owner identity"
    assert_not_contains "$rendered" 'FIREWEED_OBJECT_LOG_ROOT' "shared object-log root"
    assert_not_contains "$rendered" 'kind: PersistentVolumeClaim' "shared PVC"
    assert_contains "$rendered" 'emptyDir: {}' "pod-local projection volume"
    assert_no_fixture_credentials "$rendered" "shared S3/postgres rendered manifest"
}

assert_s3_sqlite_postgres_control_plane_contract() {
    local rendered="$1"

    assert_contains "$rendered" 'replicas: 3' "shared profile replica count"
    assert_contains "$rendered" 'FIREWEED_LOG_BACKEND: "s3"' "first-class s3 log axis"
    assert_contains "$rendered" 'FIREWEED_CONTROL_PLANE: "postgres"' "postgres control-plane axis"
    assert_contains "$rendered" 'FIREWEED_PROJECTION_BACKEND: "sqlite"' "sqlite projection axis"
    assert_not_contains "$rendered" 'FIREWEED_OBJECT_LOG_STORE' "legacy objectlog store on first-class s3"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_ENDPOINT: "https://s3.example.com"' "S3 endpoint"
    assert_contains "$rendered" 'FIREWEED_OBJECT_LOG_S3_BUCKET: "fireweed-shared"' "S3 bucket"
    assert_contains "$rendered" 'name: FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID' "S3 access key secret env"
    assert_contains "$rendered" 'name: FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL' "postgres control-plane secret env"
    assert_not_contains "$rendered" 'kind: PersistentVolumeClaim' "shared PVC"
    assert_contains "$rendered" 'emptyDir: {}' "pod-local projection volume"
    assert_no_fixture_credentials "$rendered" "first-class s3 rendered manifest"
}

assert_postgres_contract() {
    local rendered="$1"
    local projection="$2"

    assert_contains "$rendered" 'FIREWEED_LOG_BACKEND: "postgres"' "postgres log axis"
    assert_contains "$rendered" "FIREWEED_PROJECTION_BACKEND: \"${projection}\"" "${projection} projection axis"
    assert_contains "$rendered" 'name: FIREWEED_POSTGRES_LOG_DATABASE_URL' "postgres log env"
    assert_contains "$rendered" 'secretKeyRef:' "postgres Secret reference"
    assert_projection_path_contract "$rendered" "$projection"
    if [[ "$projection" == "sqlite" || "$projection" == "turso" ]]; then
        assert_contains "$rendered" 'kind: PersistentVolumeClaim' "storage PVC for durable local projection"
        assert_contains "$rendered" 'name: storage' "storage volume for durable local projection"
    fi
    assert_not_contains "$rendered" 'FIREWEED_BACKEND_PROFILE' "legacy profile env"
    assert_no_fixture_credentials "$rendered" "postgres rendered manifest"
}

assert_lakebase_postgres_contract() {
    local rendered="$1"

    # The binary connects to Lakebase from the self-sufficient log DSN alone (host/port/db +
    # sslmode=require in the Secret). It does NOT read FIREWEED_LAKEBASE_* metadata, and a common
    # wired postgres combination is log=postgres + projection=memory, so the Lakebase profile
    # renders neither FIREWEED_LAKEBASE_* nor a projection DSN.
    assert_postgres_contract "$rendered" "memory"
    assert_not_contains "$rendered" 'FIREWEED_LAKEBASE_' "Lakebase metadata env (binary ignores it)"
    assert_not_contains "$rendered" 'name: FIREWEED_POSTGRES_PROJECTION_DATABASE_URL' "projection DSN env (binary ignores it)"
    assert_contains "$rendered" 'name: DATABRICKS_HOST' "Databricks host Secret env"
    assert_contains "$rendered" 'name: FIREWEED_DATABRICKS_DATABASE_INSTANCE_NAME' "Databricks instance Secret env"
    assert_contains "$rendered" 'name: DATABRICKS_CLIENT_ID' "Databricks service principal client id"
    assert_contains "$rendered" 'name: DATABRICKS_CLIENT_SECRET' "Databricks service principal client secret"
    assert_contains "$rendered" 'name: "fireweed-lakebase-dsn"' "Lakebase DSN Secret"
    assert_contains "$rendered" 'name: "fireweed-lakebase-oauth"' "Lakebase OAuth Secret"
    assert_not_contains "$rendered" 'password=' "inline Lakebase password"
    assert_no_fixture_credentials "$rendered" "Lakebase rendered manifest"
}

assert_generated_bootstrap_contract() {
    local rendered
    rendered="$(mktemp)"
    helm template fireweed-density "$CHART_DIR" \
        --set bootstrap.generated.count=1001 \
        --set bootstrap.generated.tenant=density \
        --set bootstrap.generated.prefix=q >"$rendered"

    assert_contains "$rendered" 'FIREWEED_BOOTSTRAP_GENERATED_COUNT: "1001"' "generated bootstrap count"
    assert_contains "$rendered" 'FIREWEED_BOOTSTRAP_GENERATED_TENANT: "density"' "generated bootstrap tenant"
    assert_contains "$rendered" 'FIREWEED_BOOTSTRAP_GENERATED_PREFIX: "q"' "generated bootstrap prefix"
    assert_not_contains "$rendered" 'FIREWEED_BOOTSTRAP_QUEUES:' "explicit bootstrap list when generation is selected"
    rm -f "$rendered"
}

assert_demoted_projection_schema_exclusion() {
    # Public projection enum is memory|sqlite|turso|postgres.
    # Demoted aliases (hybrid, hybrid-async, hybrid-strict, inmemory) must fail schema validation.
    # turso is public and must NOT be re-added to this rejected-name guard.
    local demoted
    for demoted in hybrid hybrid-async hybrid-strict inmemory; do
        local output
        output="$(mktemp)"

        if helm template "fireweed-demoted-${demoted}" "$CHART_DIR" \
            --set storage.log.backend=filesystem \
            --set "storage.projection.backend=${demoted}" >"$output" 2>&1; then
            err "filesystem/${demoted} unexpectedly rendered; demoted projections must remain outside the chart schema"
            cat "$output" >&2
            rm -f "$output"
            exit 1
        fi

        # Helm 3 and Helm 4 format schema failures differently. Require the exact
        # path and allowed public enum from either formatter so a schema expansion, a
        # template-time rejection, or an unrelated render failure cannot satisfy
        # this public-support boundary.
        local helm4_error="- at '/storage/projection/backend': value must be one of 'memory', 'sqlite', 'turso', 'postgres'"
        local helm3_error='storage.projection.backend: storage.projection.backend must be one of the following: "memory", "sqlite", "turso", "postgres"'
        if ! grep -Fq -- "$helm4_error" "$output" && ! grep -Fq -- "$helm3_error" "$output"; then
            err "filesystem/${demoted} did not fail with the exact public projection enum-exclusion error"
            cat "$output" >&2
            rm -f "$output"
            exit 1
        fi

        rm -f "$output"
    done

    # turso must be accepted by schema (cannot be re-added to the rejected-name guard above).
    local turso_output
    turso_output="$(mktemp)"
    if ! helm template fireweed-public-turso "$CHART_DIR" \
        --set storage.log.backend=filesystem \
        --set storage.projection.backend=turso \
        --set storage.projection.turso.path=/var/lib/fireweed/projection/projection.turso \
        >"$turso_output" 2>&1; then
        err "filesystem/turso must render; turso is a public projection value"
        cat "$turso_output" >&2
        rm -f "$turso_output"
        exit 1
    fi
    assert_contains "$turso_output" 'FIREWEED_PROJECTION_BACKEND: "turso"' "public turso projection axis"
    assert_contains "$turso_output" 'FIREWEED_TURSO_PROJECTION_PATH:' "public turso projection path"
    rm -f "$turso_output"

    # Legacy log name objectlog must fail schema validation.
    local log_output
    log_output="$(mktemp)"
    if helm template fireweed-demoted-objectlog "$CHART_DIR" \
        --set storage.log.backend=objectlog \
        --set storage.projection.backend=memory >"$log_output" 2>&1; then
        err "objectlog log backend unexpectedly rendered; must remain outside the chart schema"
        cat "$log_output" >&2
        rm -f "$log_output"
        exit 1
    fi
    rm -f "$log_output"
}

# Named gate case: chart defaults, schema, and default render agree on turso projection.
helm_defaults_to_turso_projection() {
    echo "--- helm_defaults_to_turso_projection ---"
    local values_default schema_body rendered
    values_default="$(mktemp)"
    # Extract the projection.backend default from values.yaml (not a comment).
    if ! awk '
        /^  projection:/ { in_proj=1; next }
        in_proj && /^  [a-z]/ { in_proj=0 }
        in_proj && /^    backend:[[:space:]]*turso[[:space:]]*$/ { found=1 }
        END { exit(found ? 0 : 1) }
    ' "${CHART_DIR}/values.yaml"; then
        err "values.yaml must default storage.projection.backend to turso"
        exit 1
    fi
    assert_contains "${CHART_DIR}/values.yaml" 'path: /var/lib/fireweed/projection/projection.turso' "default turso path in values.yaml"

    schema_body="$(cat "${CHART_DIR}/values.schema.json")"
    if ! grep -Fq '"turso"' <<<"$schema_body"; then
        err "values.schema.json must enumerate turso in the public projection enum"
        exit 1
    fi
    # Rejected aliases must not appear in the schema enum.
    if grep -E '"hybrid"|"hybrid-async"|"hybrid-strict"|"inmemory"' <<<"$schema_body" >/dev/null; then
        err "values.schema.json must not re-admit demoted projection names"
        exit 1
    fi

    rendered="$(mktemp)"
    helm template fireweed-default-turso "$CHART_DIR" >"$rendered"
    assert_contains "$rendered" 'FIREWEED_LOG_BACKEND: "filesystem"' "default log axis"
    assert_contains "$rendered" 'FIREWEED_PROJECTION_BACKEND: "turso"' "default projection axis"
    assert_contains "$rendered" 'FIREWEED_TURSO_PROJECTION_PATH: "/var/lib/fireweed/projection/projection.turso"' "default turso path in ConfigMap"
    assert_contains "$rendered" 'kind: PersistentVolumeClaim' "default PVC for turso projection"
    assert_contains "$rendered" 'mountPath: "/var/lib/fireweed/projection"' "default volume mount"
    assert_no_fixture_credentials "$rendered" "default turso render"
    rm -f "$rendered" "$values_default"
    echo "helm_defaults_to_turso_projection: OK"
}

assert_canonical_matrix_mapping() {
    echo "--- canonical 20-cell T4 fixture mapping ---"
    local -A seen_cells=()
    local combo cell_id expected_count=20
    if ((${#MATRIX_COMBINATIONS[@]} != expected_count)); then
        err "MATRIX_COMBINATIONS must have exactly ${expected_count} entries (got ${#MATRIX_COMBINATIONS[@]})"
        exit 1
    fi
    for combo in "${MATRIX_COMBINATIONS[@]}"; do
        cell_id="$(canonical_cell_id_for "$combo")"
        if [[ -n "${seen_cells[$cell_id]:-}" ]]; then
            err "duplicate canonical cell mapping for ${cell_id} (combinations ${seen_cells[$cell_id]} and ${combo})"
            exit 1
        fi
        seen_cells["$cell_id"]="$combo"
        local values
        values="$(values_file_for "$combo")"
        [[ -f "$values" ]] || { err "missing T4 values fixture for ${cell_id}: ${values}"; exit 1; }
        echo "  ${combo} -> ${cell_id} (${values#"${REPO_ROOT}/"})"
    done
    if ((${#seen_cells[@]} != expected_count)); then
        err "expected ${expected_count} distinct canonical cell IDs, got ${#seen_cells[@]}"
        exit 1
    fi
    echo "canonical 20-cell T4 mapping: OK"
}

assert_combination_contract() {
    local combination="$1"
    local rendered="$2"

    echo "--- rendered contract assertions [${combination}] ---"
    case "$combination" in
        memory-memory) assert_memory_log_contract "$rendered" "memory" ;;
        memory-sqlite) assert_memory_log_contract "$rendered" "sqlite" ;;
        memory-turso) assert_memory_log_contract "$rendered" "turso" ;;
        memory-postgres) assert_memory_log_contract "$rendered" "postgres" ;;
        filesystem-memory) assert_filesystem_cell_contract "$rendered" "memory" ;;
        filesystem-sqlite) assert_filesystem_cell_contract "$rendered" "sqlite" ;;
        filesystem-turso) assert_filesystem_cell_contract "$rendered" "turso" ;;
        filesystem-postgres) assert_filesystem_cell_contract "$rendered" "postgres" ;;
        sqlite-memory) assert_sqlite_log_contract "$rendered" "memory" ;;
        sqlite-sqlite) assert_sqlite_log_contract "$rendered" "sqlite" ;;
        sqlite-turso) assert_sqlite_log_contract "$rendered" "turso" ;;
        sqlite-postgres) assert_sqlite_log_contract "$rendered" "postgres" ;;
        s3-memory) assert_s3_cell_contract "$rendered" "memory" ;;
        s3-sqlite) assert_s3_cell_contract "$rendered" "sqlite" ;;
        s3-turso) assert_s3_cell_contract "$rendered" "turso" ;;
        s3-postgres) assert_s3_cell_contract "$rendered" "postgres" ;;
        shared-s3-postgres-control-plane) assert_shared_s3_postgres_control_plane_contract "$rendered" ;;
        s3-sqlite-postgres-control-plane) assert_s3_sqlite_postgres_control_plane_contract "$rendered" ;;
        postgres-memory) assert_postgres_contract "$rendered" "memory" ;;
        postgres-sqlite) assert_postgres_contract "$rendered" "sqlite" ;;
        postgres-turso) assert_postgres_contract "$rendered" "turso" ;;
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

    assert_canonical_matrix_mapping
    helm_defaults_to_turso_projection

    echo "--- generated bootstrap inventory contract ---"
    assert_generated_bootstrap_contract

    echo "--- demoted projection/log chart exclusion contract ---"
    assert_demoted_projection_schema_exclusion

    echo "--- local profile fail-closed contract ---"
    local scaled_local
    scaled_local="$(mktemp)"
    if helm template fireweed-local-scaled "$CHART_DIR" --values "${CHART_DIR}/ci/filesystem-sqlite-values.yaml" --set replicaCount=2 >"$scaled_local" 2>&1; then
        err "scaled local filesystem/sqlite profile unexpectedly rendered"
        cat "$scaled_local" >&2
        rm -f "$scaled_local"
        exit 1
    fi
    assert_contains "$scaled_local" 'replicaCount > 1 requires storage.log.backend=s3' "scaled local fail-closed message"
    rm -f "$scaled_local"

    # Multi-replica accepts pod-local rebuildable projections (sqlite|turso), not only sqlite.
    echo "--- multi-replica turso durability rule contract ---"
    local scaled_turso
    scaled_turso="$(mktemp)"
    if ! helm template fireweed-shared-turso "$CHART_DIR" \
        --values "${CHART_DIR}/ci/shared-s3-postgres-control-plane-values.yaml" \
        --set storage.projection.backend=turso \
        --set storage.projection.turso.path=/var/lib/fireweed/projection/projection.turso \
        >"$scaled_turso" 2>&1; then
        err "shared multi-replica profile with turso projection must render"
        cat "$scaled_turso" >&2
        rm -f "$scaled_turso"
        exit 1
    fi
    assert_contains "$scaled_turso" 'FIREWEED_PROJECTION_BACKEND: "turso"' "multi-replica turso projection"
    assert_contains "$scaled_turso" 'FIREWEED_TURSO_PROJECTION_PATH:' "multi-replica turso path"
    assert_contains "$scaled_turso" 'emptyDir: {}' "pod-local emptyDir for multi-replica turso"
    rm -f "$scaled_turso"

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
        helm template "fireweed-${combination}" "$CHART_DIR" --values "$values" >"$rendered"
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
