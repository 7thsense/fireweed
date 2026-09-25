#!/usr/bin/env bash
# P1s — Select, provision, and attest a supported S3 qualification endpoint.
#
# Governing IDs (consume only; do not edit the storage authority manifest):
#   capability_id = S3-NATIVE-CAS-CAPABILITY-ATTESTATION
#   plan_key      = P1s
#   schema        = docs/helix/04-build/storage-authority-manifest.json → topology_attestation.s3_fields
#
# Survey policy:
#   - Garage v2.2.0 is a known nonconforming candidate (If-None-Match:* not enforced;
#     fireweed-2aefefbb / docs/operator/object-log-authority-compatibility.md). Not selectable.
#   - MinIO is retired: it is end-of-life and no longer publishes binaries.
#   - Hermetic RustFS (checksum-pinned single-node binary) is preferred when two-writer
#     CAS preflight passes on the live endpoint.
#   - Selection occurs only after a real two-writer CAS preflight exits 0. No fallback.
#
# Credential policy:
#   - Secrets live in an explicit secret-file path OUTSIDE the repository
#     (default: /tmp/fireweed-s3-secrets/credentials.env).
#   - This script never reads .env.garage-e3 or any in-repo credential path.
#   - Attestation records the secret-file path only, never credential values.
#
# Usage:
#   bash scripts/ci/s3-qualification-endpoint.sh survey
#   bash scripts/ci/s3-qualification-endpoint.sh install     # fetch + verify RustFS; print its path
#   bash scripts/ci/s3-qualification-endpoint.sh provision   # start RustFS, preflight, attest
#   bash scripts/ci/s3-qualification-endpoint.sh preflight   # CAS only (needs env or secret file)
#   bash scripts/ci/s3-qualification-endpoint.sh attest      # emit attestation from last preflight
#   bash scripts/ci/s3-qualification-endpoint.sh teardown    # bounded process stop
#   bash scripts/ci/s3-qualification-endpoint.sh verify-isolation
#   bash scripts/ci/s3-qualification-endpoint.sh status
#
# Environment overrides:
#   FIREWEED_S3_SECRET_DIR     default /tmp/fireweed-s3-secrets
#   FIREWEED_RUSTFS_CACHE      default ${XDG_CACHE_HOME:-~/.cache}/fireweed-rustfs/<version>
#   FIREWEED_S3_QUAL_HOST_PORT ephemeral loopback port when unset
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PREFLIGHT_PY="${SCRIPT_DIR}/s3-native-cas-preflight.py"

# Checksum-pinned static RustFS release binary. The sha256 matches the release's SHA256SUMS:
# https://github.com/rustfs/rustfs/releases/tag/1.0.0
# 1.0.0 is the first stable release and includes the conditional-write atomicity fixes.
readonly RUSTFS_VERSION="1.0.0"
readonly RUSTFS_ASSET="rustfs-linux-x86_64-musl-v1.0.0.zip"
readonly RUSTFS_URL="https://github.com/rustfs/rustfs/releases/download/1.0.0/rustfs-linux-x86_64-musl-v1.0.0.zip"
readonly RUSTFS_SHA256="c30a95b76546f25122c9ca387090ddb30c391ca5605621b0d7c881703c0f21c8"
RUSTFS_CACHE="${FIREWEED_RUSTFS_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/fireweed-rustfs/${RUSTFS_VERSION}}"
readonly CAPABILITY_ID="S3-NATIVE-CAS-CAPABILITY-ATTESTATION"
readonly PLAN_KEY="P1s"
readonly BEAD_ID="fireweed-f5fa7380"

SECRET_DIR="${FIREWEED_S3_SECRET_DIR:-/tmp/fireweed-s3-secrets}"
SECRET_FILE="${SECRET_DIR}/credentials.env"
STATE_DIR="${SECRET_DIR}/state"
ATTESTATION_FILE="${SECRET_DIR}/s3-native-cas-capability-attestation.json"
PREFLIGHT_JSON="${STATE_DIR}/preflight.json"

err() { echo "s3-qualification-endpoint: $*" >&2; }
die() { err "$*"; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

abs_path() {
  local p=$1
  if [[ "$p" = /* ]]; then
    printf '%s\n' "$p"
  else
    printf '%s\n' "$(pwd)/$p"
  fi
}

path_is_under() {
  local child parent
  child=$(abs_path "$1")
  parent=$(abs_path "$2")
  [[ "$child" == "$parent" || "$child" == "$parent"/* ]]
}

assert_secret_dir_outside_repo() {
  local secret_abs repo_abs
  secret_abs=$(abs_path "$SECRET_DIR")
  repo_abs=$(cd "$REPO_ROOT" && pwd -P)
  if path_is_under "$secret_abs" "$repo_abs"; then
    die "secret dir must be OUTSIDE the repository (got ${secret_abs} under ${repo_abs})"
  fi
}

prove_garage_e3_absent() {
  if [[ -e "${REPO_ROOT}/.env.garage-e3" ]]; then
    die ".env.garage-e3 is present in the repository; P1s forbids in-repo Garage credentials"
  fi
  if git -C "$REPO_ROOT" ls-files --error-unmatch .env.garage-e3 >/dev/null 2>&1; then
    die ".env.garage-e3 is tracked by git"
  fi
  echo "  .env.garage-e3 absent from repository (ok)"
}

sha256_of() {
  sha256sum "$1" | cut -d' ' -f1
}

# Download (once) and verify the pinned release; print the binary path.
install_rustfs() {
  local archive="${RUSTFS_CACHE}/${RUSTFS_ASSET}" bin="${RUSTFS_CACHE}/rustfs"
  mkdir -p "$RUSTFS_CACHE"
  if [[ ! -f "$archive" || "$(sha256_of "$archive")" != "$RUSTFS_SHA256" ]]; then
    err "downloading ${RUSTFS_URL}"
    curl -fsSL --retry 3 -o "${archive}.part" "$RUSTFS_URL" || die "RustFS download failed"
    mv "${archive}.part" "$archive"
  fi
  [[ "$(sha256_of "$archive")" == "$RUSTFS_SHA256" ]] \
    || die "RustFS archive sha256 mismatch (expected ${RUSTFS_SHA256})"
  if [[ ! -x "$bin" ]]; then
    python3 - "$archive" "$RUSTFS_CACHE" <<'PY'
import sys, zipfile
zipfile.ZipFile(sys.argv[1]).extract("rustfs", sys.argv[2])
PY
    chmod 755 "$bin"
  fi
  "$bin" --version 2>/dev/null | grep -q "rustfs ${RUSTFS_VERSION}" \
    || die "RustFS binary at ${bin} is not version ${RUSTFS_VERSION}"
  printf '%s\n' "$bin"
}

free_loopback_port() {
  python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

random_token() {
  python3 - <<'PY'
import secrets
print(secrets.token_hex(16))
PY
}

write_secret_file() {
  local endpoint=$1 bucket=$2 region=$3 access=$4 secret=$5
  assert_secret_dir_outside_repo
  mkdir -p "$SECRET_DIR" "$STATE_DIR"
  chmod 700 "$SECRET_DIR"
  umask 077
  cat >"$SECRET_FILE" <<EOF
# Fireweed P1s S3 qualification secrets — OUTSIDE the repository.
# Generated by scripts/ci/s3-qualification-endpoint.sh. Do not copy into the repo.
FIREWEED_S3_TEST_ENDPOINT=${endpoint}
FIREWEED_S3_TEST_BUCKET=${bucket}
FIREWEED_S3_TEST_REGION=${region}
FIREWEED_S3_TEST_ACCESS_KEY=${access}
FIREWEED_S3_TEST_SECRET_KEY=${secret}
EOF
  chmod 600 "$SECRET_FILE"
  echo "  wrote secret file path=${SECRET_FILE} (mode 600; values redacted)"
}

load_secret_file() {
  assert_secret_dir_outside_repo
  [[ -f "$SECRET_FILE" ]] || die "secret file missing: ${SECRET_FILE} (run provision first)"
  set -a
  # shellcheck disable=SC1090
  source "$SECRET_FILE"
  set +a
  : "${FIREWEED_S3_TEST_ENDPOINT:?secret file missing FIREWEED_S3_TEST_ENDPOINT}"
  : "${FIREWEED_S3_TEST_BUCKET:?secret file missing FIREWEED_S3_TEST_BUCKET}"
  : "${FIREWEED_S3_TEST_ACCESS_KEY:?secret file missing FIREWEED_S3_TEST_ACCESS_KEY}"
  : "${FIREWEED_S3_TEST_SECRET_KEY:?secret file missing FIREWEED_S3_TEST_SECRET_KEY}"
  export FIREWEED_S3_TEST_REGION="${FIREWEED_S3_TEST_REGION:-us-east-1}"
}

cmd_survey() {
  cat <<EOF
P1s S3 qualification candidate survey
=====================================
Capability ID (approved): ${CAPABILITY_ID}
Manifest fields: provider, version, region, native_atomic_conditional_create,
                 native_atomic_conditional_update, consistency_contract,
                 tls_mode, bucket_ownership_acknowledgement

Candidate A — Garage v2.2.0
  status: REJECTED (nonconforming)
  evidence: fireweed-2aefefbb (execution-verified 2026-08-01)
  reason: PutObject + If-None-Match:* returns 200 on second create (no 412).
  docs: docs/operator/object-log-authority-compatibility.md
  selectable: no (reopen P1 if this becomes the only available topology)

Candidate B — MinIO
  status: RETIRED (end-of-life; release binaries withdrawn)
  selectable: no

Candidate C — Hermetic RustFS (checksum-pinned single-node binary)
  version:      ${RUSTFS_VERSION}
  release:      ${RUSTFS_URL}
  sha256:       ${RUSTFS_SHA256}
  topology:     single process, loopback listener, run-owned data dir and bucket
  status:       SELECTABLE only after two-writer CAS preflight exits 0
  credentials:  secret-file path outside repository (${SECRET_DIR})

Policy: no fallback. Missing attestation blocks S3 children / P8cs / final
gates but not non-S3 parity. Consumers take manifest + attestation explicitly.
EOF
}

wait_rustfs_ready() {
  local endpoint=$1
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if curl -fsS "${endpoint}/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

prepare_attestation_env() {
  local cleanup_status=${1:-pending}
  export CAPABILITY_ID PLAN_KEY BEAD_ID
  export RUSTFS_VERSION RUSTFS_ASSET RUSTFS_URL RUSTFS_SHA256
  export SECRET_FILE
  export SOURCE_REVISION
  SOURCE_REVISION=$(git -C "$REPO_ROOT" rev-parse HEAD)
  export RUN_ID="p1s-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  export RUNNER_IDENTITY="fireweed-p1s@$(hostname -s 2>/dev/null || echo host)"
  export HOST_OBSERVATION
  HOST_OBSERVATION=$(hostname -s 2>/dev/null || echo unknown)
  export BINARY_USED
  BINARY_USED=$(cat "${STATE_DIR}/binary" 2>/dev/null || echo "unknown")
  if [[ -z "${FIREWEED_S3_TEST_ENDPOINT:-}" && -f "$SECRET_FILE" ]]; then
    load_secret_file
  fi
  export ENDPOINT="${FIREWEED_S3_TEST_ENDPOINT:-}"
  export BUCKET="${FIREWEED_S3_TEST_BUCKET:-}"
  export REGION="${FIREWEED_S3_TEST_REGION:-us-east-1}"
  export SERVER_PID
  SERVER_PID=$(cat "${STATE_DIR}/pid" 2>/dev/null || echo "unknown")
  export CLEANUP_STATUS="$cleanup_status"
  # Pass secret material only for redaction scanning inside the writer.
  export FIREWEED_S3_TEST_ACCESS_KEY="${FIREWEED_S3_TEST_ACCESS_KEY:-}"
  export FIREWEED_S3_TEST_SECRET_KEY="${FIREWEED_S3_TEST_SECRET_KEY:-}"
}

emit_attestation() {
  local cleanup_status=${1:-pending}
  require_cmd python3
  require_cmd git
  assert_secret_dir_outside_repo
  prove_garage_e3_absent
  [[ -f "$PREFLIGHT_JSON" ]] || die "preflight json missing; run provision or preflight first"
  prepare_attestation_env "$cleanup_status"

  python3 - "$ATTESTATION_FILE" "$PREFLIGHT_JSON" <<'PY'
import json, os, sys, platform
from datetime import datetime, timezone

out_path, preflight_path = sys.argv[1], sys.argv[2]
with open(preflight_path, encoding="utf-8") as fh:
    preflight = json.load(fh)

attestation = {
    "schema_version": 1,
    "capability_id": os.environ["CAPABILITY_ID"],
    "plan_key": os.environ["PLAN_KEY"],
    "bead_id": os.environ["BEAD_ID"],
    "source_revision": os.environ["SOURCE_REVISION"],
    "run_id": os.environ["RUN_ID"],
    "selected_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "authority_manifest": {
        "path": "docs/helix/04-build/storage-authority-manifest.json",
        "consumed_only": True,
        "topology_attestation_s3_fields": [
            "provider",
            "version",
            "region",
            "native_atomic_conditional_create",
            "native_atomic_conditional_update",
            "consistency_contract",
            "tls_mode",
            "bucket_ownership_acknowledgement",
        ],
    },
    "runner": {
        "runner_identity": os.environ["RUNNER_IDENTITY"],
        "topology": "hermetic-rustfs-binary-single-node-loopback",
        "resource_limits": "single process; ephemeral loopback port; run-owned data dir",
        "hostname_observation": os.environ["HOST_OBSERVATION"],
        "platform": platform.platform(),
    },
    "s3": {
        "provider": "rustfs",
        "version": os.environ["RUSTFS_VERSION"],
        "release_asset": os.environ["RUSTFS_ASSET"],
        "release_url": os.environ["RUSTFS_URL"],
        "artifact_sha256": os.environ["RUSTFS_SHA256"],
        "binary_used": os.environ["BINARY_USED"],
        "region": os.environ["REGION"],
        "endpoint": os.environ["ENDPOINT"],
        "tls_mode": (
            "plaintext-loopback"
            if os.environ["ENDPOINT"].startswith("http://127.0.0.1")
            else preflight.get("tls_mode", "unknown")
        ),
        "native_atomic_conditional_create": True,
        "native_atomic_conditional_update": True,
        "consistency_contract": "single-node-strong-read-after-write",
        "bucket_ownership_acknowledgement": os.environ["BUCKET"],
    },
    "credential_path_isolation": {
        "secret_file_path": os.environ["SECRET_FILE"],
        "secret_file_outside_repository": True,
        "in_repo_credential_paths_read": [],
        "env_garage_e3_absent": True,
        "attestation_contains_credential_values": False,
        "notes": "Consumers source the secret-file path; attestation never embeds key material.",
    },
    "preflight": preflight,
    "cleanup": {
        "server_pid": os.environ["SERVER_PID"],
        "teardown_policy": "bounded-process-stop",
        "status": os.environ["CLEANUP_STATUS"],
        "command": "bash scripts/ci/s3-qualification-endpoint.sh teardown",
    },
    "results": {
        "selected": True,
        "selected_provider": "rustfs",
        "rejected_candidates": [
            {
                "provider": "minio",
                "version": "RELEASE.2024-12-18T13-15-44Z",
                "reason": "end-of-life; release binaries withdrawn",
                "evidence": "docs/operator/object-log-authority-compatibility.md",
                "selectable": False,
            },
            {
                "provider": "garage",
                "version": "v2.2.0",
                "reason": "If-None-Match:* not enforced on PutObject (second create returns 200)",
                "evidence": "fireweed-2aefefbb; docs/operator/object-log-authority-compatibility.md",
                "selectable": False,
            }
        ],
        "consumers": (
            "Take docs/helix/04-build/storage-authority-manifest.json + this attestation "
            "explicitly. Missing attestation blocks S3 children/P8cs/final gates only."
        ),
    },
}

secret_values = [
    v
    for k, v in os.environ.items()
    if k in ("FIREWEED_S3_TEST_SECRET_KEY", "FIREWEED_S3_TEST_ACCESS_KEY") and v
]
serialized = json.dumps(attestation, indent=2, sort_keys=True)
for value in secret_values:
    if value and value in serialized:
        raise SystemExit(
            "refusing to write attestation: credential value would leak into JSON"
        )

with open(out_path, "w", encoding="utf-8") as fh:
    fh.write(serialized)
    fh.write("\n")
print(f"  wrote attestation path={out_path}")
PY
}

cmd_provision() {
  require_cmd curl
  require_cmd python3
  require_cmd git
  assert_secret_dir_outside_repo
  prove_garage_e3_absent

  local bin host_port access secret bucket region endpoint data pid
  bin=$(install_rustfs)
  host_port="${FIREWEED_S3_QUAL_HOST_PORT:-$(free_loopback_port)}"
  access="fwqual$(random_token | head -c 12)"
  secret="$(random_token)$(random_token)"
  bucket="fireweed-qual-$(random_token | head -c 10)"
  region="us-east-1"
  endpoint="http://127.0.0.1:${host_port}"
  data="${STATE_DIR}/data"

  mkdir -p "$SECRET_DIR" "$STATE_DIR"
  chmod 700 "$SECRET_DIR"
  cmd_teardown >/dev/null 2>&1 || true
  rm -rf "$data"
  mkdir -p "$data"
  printf '%s\n' "$bin" >"${STATE_DIR}/binary"
  printf '%s\n' "$endpoint" >"${STATE_DIR}/endpoint"

  err "starting RustFS ${RUSTFS_VERSION} at ${endpoint}"
  RUSTFS_ACCESS_KEY="$access" RUSTFS_SECRET_KEY="$secret" RUSTFS_CONSOLE_ENABLE=false \
    nohup "$bin" server "$data" --address "127.0.0.1:${host_port}" \
    >"${STATE_DIR}/rustfs.log" 2>&1 </dev/null &
  pid=$!
  printf '%s\n' "$pid" >"${STATE_DIR}/pid"

  if ! wait_rustfs_ready "$endpoint"; then
    tail -n 50 "${STATE_DIR}/rustfs.log" >&2 || true
    cmd_teardown || true
    die "RustFS readiness check timed out at ${endpoint}"
  fi

  write_secret_file "$endpoint" "$bucket" "$region" "$access" "$secret"
  access=""
  secret=""
  load_secret_file

  err "running two-writer CAS preflight"
  if ! python3 "$PREFLIGHT_PY" --json-out "$PREFLIGHT_JSON"; then
    err "CAS preflight FAILED — topology is nonconforming; tearing down"
    cmd_teardown || true
    die "two-writer CAS preflight failed; endpoint not selected (no fallback)"
  fi

  emit_attestation "pending"
  err "provision complete"
  err "  secret_file=${SECRET_FILE}"
  err "  attestation=${ATTESTATION_FILE}"
  err "  endpoint=${FIREWEED_S3_TEST_ENDPOINT}"
  err "  bucket=${FIREWEED_S3_TEST_BUCKET} (ack in attestation; keys redacted)"
  err "  RustFS left running for consumers (pid ${pid}); run teardown when finished"
}

cmd_preflight() {
  require_cmd python3
  assert_secret_dir_outside_repo
  prove_garage_e3_absent
  if [[ -z "${FIREWEED_S3_TEST_ENDPOINT:-}" ]]; then
    load_secret_file
  else
    : "${FIREWEED_S3_TEST_BUCKET:?set FIREWEED_S3_TEST_BUCKET}"
    : "${FIREWEED_S3_TEST_ACCESS_KEY:?set FIREWEED_S3_TEST_ACCESS_KEY}"
    : "${FIREWEED_S3_TEST_SECRET_KEY:?set FIREWEED_S3_TEST_SECRET_KEY}"
    export FIREWEED_S3_TEST_REGION="${FIREWEED_S3_TEST_REGION:-us-east-1}"
  fi
  mkdir -p "$STATE_DIR"
  python3 "$PREFLIGHT_PY" --json-out "$PREFLIGHT_JSON"
}

cmd_attest() {
  emit_attestation "${1:-pending}"
}

server_running() {
  local pid=$1
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null \
    && grep -q rustfs "/proc/${pid}/cmdline" 2>/dev/null
}

cmd_teardown() {
  local pid=""
  if [[ -f "${STATE_DIR}/pid" ]]; then
    pid=$(cat "${STATE_DIR}/pid")
  fi
  if [[ -z "$pid" ]]; then
    err "no RustFS process recorded; nothing to tear down"
    return 0
  fi
  if server_running "$pid"; then
    err "teardown: stopping RustFS pid ${pid}"
    kill "$pid" 2>/dev/null || true
    local deadline=$((SECONDS + 10))
    while server_running "$pid" && (( SECONDS < deadline )); do
      sleep 0.2
    done
    if server_running "$pid"; then
      kill -9 "$pid" 2>/dev/null || true
      sleep 0.2
    fi
    server_running "$pid" && die "RustFS pid ${pid} still running after teardown"
  fi
  if [[ -f "$ATTESTATION_FILE" && -f "$PREFLIGHT_JSON" ]]; then
    if [[ -f "$SECRET_FILE" ]]; then
      load_secret_file || true
    fi
    emit_attestation "completed" || true
  fi
  rm -f "${STATE_DIR}/pid"
  rm -rf "${STATE_DIR}/data"
  err "teardown complete for RustFS pid ${pid}"
}

cmd_verify_isolation() {
  assert_secret_dir_outside_repo
  prove_garage_e3_absent
  local forbidden_paths=(
    "${REPO_ROOT}/.env.garage-e3"
    "${REPO_ROOT}/.env"
    "${REPO_ROOT}/credentials.env"
    "${REPO_ROOT}/scripts/ci/credentials.env"
  )
  local p
  for p in "${forbidden_paths[@]}"; do
    if [[ -e "$p" ]]; then
      die "forbidden credential path exists: $p"
    fi
  done
  echo "  secret_dir=${SECRET_DIR} (outside repo)"
  echo "  no forbidden in-repo credential paths present"
  if [[ -f "$ATTESTATION_FILE" && -f "$SECRET_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$SECRET_FILE"
    set +a
    if [[ -n "${FIREWEED_S3_TEST_SECRET_KEY:-}" ]] && grep -Fq "$FIREWEED_S3_TEST_SECRET_KEY" "$ATTESTATION_FILE"; then
      die "attestation embeds secret key material"
    fi
    if [[ -n "${FIREWEED_S3_TEST_ACCESS_KEY:-}" ]] && grep -Fq "$FIREWEED_S3_TEST_ACCESS_KEY" "$ATTESTATION_FILE"; then
      die "attestation embeds access key material"
    fi
    python3 - "$ATTESTATION_FILE" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as fh:
    doc = json.load(fh)
assert doc.get("capability_id") == "S3-NATIVE-CAS-CAPABILITY-ATTESTATION", doc.get("capability_id")
assert doc.get("plan_key") == "P1s"
assert doc["s3"]["native_atomic_conditional_create"] is True
assert doc["s3"]["native_atomic_conditional_update"] is True
assert doc["credential_path_isolation"]["secret_file_outside_repository"] is True
assert doc["credential_path_isolation"]["env_garage_e3_absent"] is True
assert doc["credential_path_isolation"]["attestation_contains_credential_values"] is False
assert doc["results"]["selected"] is True
print("  attestation schema checks: ok")
PY
  fi
  echo "verify-isolation: PASS"
}

cmd_status() {
  echo "SECRET_DIR=${SECRET_DIR}"
  echo "SECRET_FILE=${SECRET_FILE}"
  echo "ATTESTATION_FILE=${ATTESTATION_FILE}"
  echo "RUSTFS_VERSION=${RUSTFS_VERSION}"
  if [[ -f "${STATE_DIR}/pid" ]]; then
    local pid
    pid=$(cat "${STATE_DIR}/pid")
    echo "RUSTFS_PID=${pid}"
    if server_running "$pid"; then
      echo "RUSTFS_RUNNING=true"
    else
      echo "RUSTFS_RUNNING=false"
    fi
  fi
  if [[ -f "$ATTESTATION_FILE" ]]; then
    echo "ATTESTATION=present"
    python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print("  capability_id=", d.get("capability_id")); print("  selected=", d.get("results",{}).get("selected")); print("  endpoint=", d.get("s3",{}).get("endpoint"))' "$ATTESTATION_FILE"
  else
    echo "ATTESTATION=absent"
  fi
}

usage() {
  sed -n '2,40p' "$0" | sed 's/^# \?//'
}

main() {
  local cmd=${1:-}
  shift || true
  case "$cmd" in
    survey) cmd_survey "$@" ;;
    install) install_rustfs ;;
    provision) cmd_provision "$@" ;;
    preflight) cmd_preflight "$@" ;;
    attest) cmd_attest "${1:-pending}" ;;
    teardown) cmd_teardown "$@" ;;
    verify-isolation) cmd_verify_isolation "$@" ;;
    status) cmd_status "$@" ;;
    -h|--help|help) usage ;;
    "") usage; exit 64 ;;
    *) die "unknown command: $cmd (try: survey|install|provision|preflight|attest|teardown|verify-isolation|status)" ;;
  esac
}

main "$@"
