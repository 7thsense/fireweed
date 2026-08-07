#!/usr/bin/env bash
# P6p — provider-neutral Snorri runner preflight.
#
# Verifies host reachability to:
#   1) P1s live supported-S3 endpoint (credentials outside the repo)
#   2) isolated PostgreSQL control plane for Snorri live rows
#
# Never prints secret values. Never writes credentials into the repository.
#
# Usage:
#   bash scripts/ci/snorri-runner-preflight.sh
#   eval "$(bash scripts/ci/snorri-runner-preflight.sh --export-env)"
#
# Environment:
#   FIREWEED_S3_SECRET_DIR          default /tmp/fireweed-s3-secrets
#   SNORRI_FIREWEED_POSTGRES_URL    optional; default local isolated DB URL template
#                                   uses host-managed password fireweed/fireweed for
#                                   the local docker control plane only
#   SNORRI_RUNNER_SKIP_S3_PROBE     set to 1 to skip authenticated S3 probe
#   SNORRI_RUNNER_SKIP_PG_PROBE     set to 1 to skip Postgres probe
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SECRET_DIR="${FIREWEED_S3_SECRET_DIR:-/tmp/fireweed-s3-secrets}"
SECRET_FILE="${SECRET_DIR}/credentials.env"
ATTESTATION_FILE="${SECRET_DIR}/s3-native-cas-capability-attestation.json"

# Isolated control-plane default for this runner (password is host-local only).
DEFAULT_PG_URL="postgres://fireweed:fireweed@127.0.0.1:55432/fireweed_snorri_p6p"
PG_URL="${SNORRI_FIREWEED_POSTGRES_URL:-$DEFAULT_PG_URL}"

EXPORT_ENV=0
if [[ "${1:-}" == "--export-env" ]]; then
  EXPORT_ENV=1
fi

err() { echo "snorri-runner-preflight: $*" >&2; }
die() { err "$*"; exit 1; }

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
  if path_is_under "$SECRET_DIR" "$REPO_ROOT"; then
    die "secret dir must be outside the repository: $SECRET_DIR"
  fi
  for forbidden in \
    "${REPO_ROOT}/credentials.env" \
    "${REPO_ROOT}/scripts/ci/credentials.env" \
    "${REPO_ROOT}/.env.garage-e3"
  do
    if [[ -f "$forbidden" ]]; then
      die "forbidden in-repo credential path present: $forbidden"
    fi
  done
}

redact_url() {
  python3 - "$1" <<'PY'
import sys, urllib.parse
u = urllib.parse.urlparse(sys.argv[1])
host = u.hostname or ""
port = f":{u.port}" if u.port else ""
user = (u.username + "@") if u.username else ""
print(f"{u.scheme}://{user}***@{host}{port}{u.path}")
PY
}

load_s3_secrets() {
  [[ -f "$SECRET_FILE" ]] || die "missing P1s credentials file: $SECRET_FILE (run scripts/ci/s3-qualification-endpoint.sh provision)"
  [[ -f "$ATTESTATION_FILE" ]] || die "missing P1s attestation: $ATTESTATION_FILE"
  # shellcheck disable=SC1090
  set -a
  # Credentials file is KEY=VALUE lines; never echo it.
  source "$SECRET_FILE"
  set +a
  : "${FIREWEED_S3_TEST_ENDPOINT:?FIREWEED_S3_TEST_ENDPOINT missing in secret file}"
  : "${FIREWEED_S3_TEST_BUCKET:?FIREWEED_S3_TEST_BUCKET missing in secret file}"
  : "${FIREWEED_S3_TEST_REGION:?FIREWEED_S3_TEST_REGION missing in secret file}"
  : "${FIREWEED_S3_TEST_ACCESS_KEY:?FIREWEED_S3_TEST_ACCESS_KEY missing in secret file}"
  : "${FIREWEED_S3_TEST_SECRET_KEY:?FIREWEED_S3_TEST_SECRET_KEY missing in secret file}"
}

probe_s3() {
  python3 - <<'PY'
import datetime
import hashlib
import hmac
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

endpoint = os.environ["FIREWEED_S3_TEST_ENDPOINT"].rstrip("/")
bucket = os.environ["FIREWEED_S3_TEST_BUCKET"]
region = os.environ.get("FIREWEED_S3_TEST_REGION", "us-east-1")
ak = os.environ["FIREWEED_S3_TEST_ACCESS_KEY"]
sk = os.environ["FIREWEED_S3_TEST_SECRET_KEY"]
u = urllib.parse.urlparse(endpoint)
host = u.hostname
port = u.port or (443 if u.scheme == "https" else 80)
if not host:
    raise SystemExit("endpoint missing host")

# TCP reachability
sock = socket.socket()
sock.settimeout(5)
t0 = time.time()
sock.connect((host, port))
sock.close()
print(f"s3_tcp_ok host={host} port={port} ms={int((time.time() - t0) * 1000)}")

algorithm = "AWS4-HMAC-SHA256"
service = "s3"


def sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def signed_request(method: str, canonical_uri: str, body: bytes = b""):
    payload_hash = hashlib.sha256(body).hexdigest()
    amz_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]
    canonical_headers = (
        f"host:{u.netloc}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        [method, canonical_uri, "", canonical_headers, signed_headers, payload_hash]
    )
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    k_date = sign(("AWS4" + sk).encode("utf-8"), date_stamp)
    k_region = sign(k_date, region)
    k_service = sign(k_region, service)
    k_signing = sign(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
        f"{algorithm} Credential={ak}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    url = f"{endpoint}{canonical_uri}"
    req = urllib.request.Request(url, data=body if method in {"PUT", "POST"} else None, method=method)
    req.add_header("x-amz-date", amz_date)
    req.add_header("x-amz-content-sha256", payload_hash)
    req.add_header("Authorization", authorization)
    if body and method == "PUT":
        req.add_header("Content-Length", str(len(body)))
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


status = signed_request("HEAD", f"/{bucket}")
if status not in (200, 301, 302):
    raise SystemExit(f"head_bucket_failed status={status}")
print(f"s3_head_bucket_ok status={status} bucket={bucket}")

key = f"fireweed-p6p-reachability/{uuid.uuid4().hex}/probe.txt"
body = b"p6p-reachability-ok"
put_status = signed_request("PUT", f"/{bucket}/{key}", body)
if put_status not in (200, 201):
    raise SystemExit(f"put_probe_failed status={put_status}")
del_status = signed_request("DELETE", f"/{bucket}/{key}")
if del_status not in (200, 204):
    raise SystemExit(f"delete_probe_failed status={del_status}")
print("s3_put_delete_probe_ok key_prefix=fireweed-p6p-reachability/")
print(f"s3_endpoint_nonsecret={u.scheme}://{u.netloc}")
print(f"s3_region={region}")
print("s3_provider_note=use P1s attestation; garage/eldir not accepted as implicit provisioning")
PY
}

probe_postgres() {
  command -v psql >/dev/null 2>&1 || die "psql client required for postgres probe"
  local redacted
  redacted=$(redact_url "$PG_URL")
  local out
  if ! out=$(psql "$PG_URL" -v ON_ERROR_STOP=1 -Atc "SELECT current_database() || '|' || current_user;" 2>&1); then
    die "postgres probe failed for ${redacted}: ${out}"
  fi
  local db user
  db=${out%%|*}
  user=${out#*|}
  echo "postgres_ok url=${redacted} database=${db} user=${user}"
  if [[ "$db" != "fireweed_snorri_p6p" && -z "${SNORRI_FIREWEED_POSTGRES_URL:-}" ]]; then
    err "warning: expected default isolated database fireweed_snorri_p6p, got ${db}"
  fi
}

export_mapped_env() {
  # Emit shell assignments for eval; values come from already-loaded env.
  # Also map into legacy Snorri live harness names still used on Snorri main.
  cat <<EOF
export FIREWEED_S3_TEST_ENDPOINT=$(printf '%q' "${FIREWEED_S3_TEST_ENDPOINT}")
export FIREWEED_S3_TEST_BUCKET=$(printf '%q' "${FIREWEED_S3_TEST_BUCKET}")
export FIREWEED_S3_TEST_REGION=$(printf '%q' "${FIREWEED_S3_TEST_REGION}")
export FIREWEED_S3_TEST_ACCESS_KEY=$(printf '%q' "${FIREWEED_S3_TEST_ACCESS_KEY}")
export FIREWEED_S3_TEST_SECRET_KEY=$(printf '%q' "${FIREWEED_S3_TEST_SECRET_KEY}")
export SNORRI_FIREWEED_POSTGRES_URL=$(printf '%q' "${PG_URL}")
export SNORRI_S3_TEST_ENDPOINT=$(printf '%q' "${FIREWEED_S3_TEST_ENDPOINT}")
export SNORRI_S3_TEST_BUCKET=$(printf '%q' "${FIREWEED_S3_TEST_BUCKET}")
export SNORRI_S3_TEST_REGION=$(printf '%q' "${FIREWEED_S3_TEST_REGION}")
export SNORRI_S3_TEST_ACCESS_KEY=$(printf '%q' "${FIREWEED_S3_TEST_ACCESS_KEY}")
export SNORRI_S3_TEST_SECRET_KEY=$(printf '%q' "${FIREWEED_S3_TEST_SECRET_KEY}")
# Legacy Snorri main live harness names (provider must still be P1s MinIO).
export SNORRI_GARAGE_S3_ENDPOINT=$(printf '%q' "${FIREWEED_S3_TEST_ENDPOINT}")
export SNORRI_GARAGE_S3_BUCKET=$(printf '%q' "${FIREWEED_S3_TEST_BUCKET}")
export SNORRI_GARAGE_S3_REGION=$(printf '%q' "${FIREWEED_S3_TEST_REGION}")
export SNORRI_GARAGE_S3_ACCESS_KEY=$(printf '%q' "${FIREWEED_S3_TEST_ACCESS_KEY}")
export SNORRI_GARAGE_S3_SECRET_KEY=$(printf '%q' "${FIREWEED_S3_TEST_SECRET_KEY}")
export SNORRI_GARAGE_TEST=1
export SNORRI_S3_TEST=1
EOF
}

main() {
  assert_secret_dir_outside_repo
  echo "snorri-runner-preflight: runner_identity=fireweed-p6p-snorri@$(hostname -s 2>/dev/null || hostname)"
  echo "snorri-runner-preflight: secret_dir=${SECRET_DIR} (outside repo)"
  echo "snorri-runner-preflight: attestation_present=$([[ -f $ATTESTATION_FILE ]] && echo yes || echo no)"

  if [[ "${SNORRI_RUNNER_SKIP_S3_PROBE:-0}" != "1" ]]; then
    load_s3_secrets
    # Reject accidental Garage-only qualification when attestation says so.
    if python3 - "$ATTESTATION_FILE" <<'PY'
import json, sys
att = json.load(open(sys.argv[1]))
provider = (att.get("s3") or {}).get("provider") or (att.get("results") or {}).get("selected_provider")
selected = (att.get("results") or {}).get("selected")
if provider and str(provider).lower() == "garage":
    raise SystemExit("P1s attestation selects garage; not accepted for P6p runner")
if selected is False:
    raise SystemExit("P1s attestation results.selected is false")
print(f"p1s_provider={provider} selected={selected}")
PY
    then
      :
    else
      die "P1s attestation not usable for provider-neutral runner"
    fi
    probe_s3
  else
    err "skipping S3 probe (SNORRI_RUNNER_SKIP_S3_PROBE=1)"
    # Still load secrets if export requested
    if [[ "$EXPORT_ENV" -eq 1 ]]; then
      load_s3_secrets
    fi
  fi

  if [[ "${SNORRI_RUNNER_SKIP_PG_PROBE:-0}" != "1" ]]; then
    probe_postgres
  else
    err "skipping Postgres probe (SNORRI_RUNNER_SKIP_PG_PROBE=1)"
  fi

  if [[ "$EXPORT_ENV" -eq 1 ]]; then
    load_s3_secrets
    export_mapped_env
  else
    echo "snorri-runner-preflight: ok"
    echo "snorri-runner-preflight: to export env for Snorri live harness: eval \"\$(bash scripts/ci/snorri-runner-preflight.sh --export-env)\""
  fi
}

main "$@"
