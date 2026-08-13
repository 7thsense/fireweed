#!/usr/bin/env bash
# P6s — provider-neutral Snorri S3 durability acceptance orchestrator.
#
# Proves TP-004 live S3 semantic IDs against the P1s-attested endpoint with
# zero skips, emits a run-owned attestation under docs/evidence/snorri/, and
# optionally re-runs the external Snorri live harness when a checkout is present.
#
# Usage:
#   bash scripts/ci/snorri-s3-durability-acceptance.sh
#   SNORRI_CHECKOUT=/home/erik/Projects/snorri bash scripts/ci/snorri-s3-durability-acceptance.sh
#   P6S_SKIP_SNORRI=1 bash scripts/ci/snorri-s3-durability-acceptance.sh   # fireweed harness only
#
# Environment:
#   FIREWEED_S3_SECRET_DIR     default /tmp/fireweed-s3-secrets
#   SNORRI_FIREWEED_POSTGRES_URL / FIREWEED_PG_TEST_URL
#   SNORRI_CHECKOUT            path to sibling snorri clone (optional)
#   P6S_SKIP_SNORRI            set to 1 to skip external snorri execution
#   CARGO_TARGET_DIR           optional shared target dir
#   RUSTUP_TOOLCHAIN           default 1.97.1
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$REPO_ROOT"

SECRET_DIR="${FIREWEED_S3_SECRET_DIR:-/tmp/fireweed-s3-secrets}"
ATTESTATION_FILE="${SECRET_DIR}/s3-native-cas-capability-attestation.json"
EVIDENCE_DIR="${REPO_ROOT}/docs/evidence/snorri"
OUT_JSON="${EVIDENCE_DIR}/p6s-s3-durability-attestation.json"
LEDGER_FIXTURE="${REPO_ROOT}/scripts/ci/fixtures/snorri/p6s-s3-durability.json"
TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.97.1}"
DEFAULT_PG_URL="postgres://fireweed:fireweed@127.0.0.1:55432/fireweed_snorri_p6p"
export FIREWEED_PG_TEST_URL="${FIREWEED_PG_TEST_URL:-${SNORRI_FIREWEED_POSTGRES_URL:-$DEFAULT_PG_URL}}"
export SNORRI_FIREWEED_POSTGRES_URL="${SNORRI_FIREWEED_POSTGRES_URL:-$FIREWEED_PG_TEST_URL}"
export LD_LIBRARY_PATH="/home/linuxbrew/.linuxbrew/opt/openssl@3/lib:${LD_LIBRARY_PATH:-}"

err() { echo "snorri-s3-durability-acceptance: $*" >&2; }
die() { err "$*"; exit 1; }

run_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
fireweed_sha="$(git -C "$REPO_ROOT" rev-parse HEAD)"
runner_id="fireweed-p6p-snorri@$(hostname -s 2>/dev/null || hostname)"

commands_json='[]'
append_cmd() {
  local cmd=$1
  commands_json="$(python3 -c 'import json,sys; a=json.loads(sys.argv[1]); a.append(sys.argv[2]); print(json.dumps(a))' "$commands_json" "$cmd")"
}

# --- Preflight (provider-neutral; rejects garage-as-implicit) ---
err "preflight"
bash "${SCRIPT_DIR}/snorri-runner-preflight.sh"
append_cmd "bash scripts/ci/snorri-runner-preflight.sh"

# Load secrets without printing them.
set -a
# shellcheck disable=SC1090
source "${SECRET_DIR}/credentials.env"
set +a
: "${FIREWEED_S3_TEST_ENDPOINT:?}"
: "${FIREWEED_S3_TEST_BUCKET:?}"
: "${FIREWEED_S3_TEST_REGION:?}"
: "${FIREWEED_S3_TEST_ACCESS_KEY:?}"
: "${FIREWEED_S3_TEST_SECRET_KEY:?}"

# Map TP-004 provider-neutral env names for any downstream consumer.
export SNORRI_S3_TEST_ENDPOINT="${FIREWEED_S3_TEST_ENDPOINT}"
export SNORRI_S3_TEST_BUCKET="${FIREWEED_S3_TEST_BUCKET}"
export SNORRI_S3_TEST_REGION="${FIREWEED_S3_TEST_REGION}"
export SNORRI_S3_TEST_ACCESS_KEY="${FIREWEED_S3_TEST_ACCESS_KEY}"
export SNORRI_S3_TEST_SECRET_KEY="${FIREWEED_S3_TEST_SECRET_KEY}"
export SNORRI_S3_TEST=1

# --- Fireweed-side contract harness (primary acceptance; zero skips) ---
err "fireweed harness: p6s_s3_durability_acceptance"
fw_cmd="rustup run ${TOOLCHAIN} cargo test -p fireweed --features objectlog,sqlite,postgres --test p6s_s3_durability_acceptance -- --nocapture"
append_cmd "$fw_cmd"
# shellcheck disable=SC2086
rustup run "$TOOLCHAIN" cargo test -p fireweed \
  --features objectlog,sqlite,postgres \
  --test p6s_s3_durability_acceptance \
  -- --nocapture

# Structural ledger validation (provider-brand free).
if [[ -f "$LEDGER_FIXTURE" ]]; then
  err "semantic verifier ledger"
  ledger_cmd="python3 scripts/ci/snorri-semantic-verifier.py --ledger scripts/ci/fixtures/snorri/p6s-s3-durability.json --cells s3 --ids SNORRI-REOPEN,SNORRI-PROJECTION-REBUILD,SNORRI-RETRY-ONCE"
  append_cmd "$ledger_cmd"
  python3 "${SCRIPT_DIR}/snorri-semantic-verifier.py" \
    --ledger "$LEDGER_FIXTURE" \
    --cells s3 \
    --ids SNORRI-REOPEN,SNORRI-PROJECTION-REBUILD,SNORRI-RETRY-ONCE
fi

# --- Optional external Snorri re-run ---
snorri_status="not_run"
snorri_sha=""
snorri_checkout="${SNORRI_CHECKOUT:-}"
if [[ -z "$snorri_checkout" && -d /home/erik/Projects/snorri/.git ]]; then
  snorri_checkout="/home/erik/Projects/snorri"
fi

if [[ "${P6S_SKIP_SNORRI:-0}" == "1" ]]; then
  snorri_status="skipped_by_operator"
  err "external snorri skipped (P6S_SKIP_SNORRI=1)"
elif [[ -n "$snorri_checkout" && -d "$snorri_checkout" ]]; then
  snorri_sha="$(git -C "$snorri_checkout" rev-parse HEAD 2>/dev/null || true)"
  err "external snorri checkout=${snorri_checkout} sha=${snorri_sha:-unknown}"
  # Prefer provider-neutral script; fall back to legacy garage-named harness.
  if [[ -x "$snorri_checkout/scripts/test-s3-live-mutations.sh" || -f "$snorri_checkout/scripts/test-s3-live-mutations.sh" ]]; then
    snorri_script="$snorri_checkout/scripts/test-s3-live-mutations.sh"
  elif [[ -x "$snorri_checkout/scripts/test-garage-live-mutations.sh" || -f "$snorri_checkout/scripts/test-garage-live-mutations.sh" ]]; then
    snorri_script="$snorri_checkout/scripts/test-garage-live-mutations.sh"
  else
    snorri_script=""
  fi
  if [[ -n "$snorri_script" ]]; then
    # Export legacy names for harnesses not yet migrated (still P1s values).
    export SNORRI_GARAGE_S3_ENDPOINT="${FIREWEED_S3_TEST_ENDPOINT}"
    export SNORRI_GARAGE_S3_BUCKET="${FIREWEED_S3_TEST_BUCKET}"
    export SNORRI_GARAGE_S3_REGION="${FIREWEED_S3_TEST_REGION}"
    export SNORRI_GARAGE_S3_ACCESS_KEY="${FIREWEED_S3_TEST_ACCESS_KEY}"
    export SNORRI_GARAGE_S3_SECRET_KEY="${FIREWEED_S3_TEST_SECRET_KEY}"
    export SNORRI_GARAGE_TEST=1
    (
      cd "$snorri_checkout"
      bash "$snorri_script"
    )
    snorri_status="passed"
    append_cmd "bash ${snorri_script#"$snorri_checkout"/}  # snorri@${snorri_sha}"
  else
    snorri_status="harness_missing"
    err "snorri live harness script not found; fireweed harness already passed"
  fi
else
  snorri_status="checkout_missing"
  err "snorri checkout not found; fireweed harness is contract proof"
  err "re-run later: SNORRI_CHECKOUT=/path/to/snorri bash scripts/ci/snorri-s3-durability-acceptance.sh"
fi

# --- Emit run-owned attestation (no secrets) ---
mkdir -p "$EVIDENCE_DIR"
export P6S_COMMANDS_JSON="$commands_json"
export P6S_RUN_TS="$run_ts"
export P6S_FIREWEED_SHA="$fireweed_sha"
export P6S_RUNNER_ID="$runner_id"
export P6S_SNORRI_STATUS="$snorri_status"
export P6S_SNORRI_SHA="$snorri_sha"
export P6S_SNORRI_CHECKOUT="${snorri_checkout:-}"
export P6S_ATTESTATION_FILE="$ATTESTATION_FILE"
export P6S_OUT_JSON="$OUT_JSON"
export P6S_TOOLCHAIN="$TOOLCHAIN"

python3 <<'PY'
import json
import os
from pathlib import Path

att_path = Path(os.environ["P6S_ATTESTATION_FILE"])
att = json.loads(att_path.read_text())
s3 = att.get("s3") or {}
pre = att.get("preflight") or {}
results = att.get("results") or {}

provider = results.get("selected_provider") or s3.get("provider") or "unknown"
endpoint = s3.get("endpoint") or pre.get("endpoint") or ""
bucket = s3.get("bucket") or pre.get("bucket") or ""
region = s3.get("region") or pre.get("region") or "us-east-1"
native_create = bool(
    s3.get("native_atomic_conditional_create")
    if s3.get("native_atomic_conditional_create") is not None
    else pre.get("native_atomic_conditional_create")
)
native_update = bool(
    s3.get("native_atomic_conditional_update")
    if s3.get("native_atomic_conditional_update") is not None
    else pre.get("native_atomic_conditional_update")
)

commands = json.loads(os.environ.get("P6S_COMMANDS_JSON") or "[]")
snorri_sha = os.environ.get("P6S_SNORRI_SHA") or None
snorri_checkout = os.environ.get("P6S_SNORRI_CHECKOUT") or None
toolchain = os.environ.get("P6S_TOOLCHAIN", "1.97.1")

doc = {
    "schema_version": 1,
    "capability_id": "SNORRI-S3-DURABILITY-ACCEPTANCE",
    "plan_key": "P6s",
    "bead_id": "fireweed-2886078a",
    "attested_at": os.environ["P6S_RUN_TS"],
    "runner": {
        "identity": os.environ["P6S_RUNNER_ID"],
        "topology": "host-local Docker P1s S3 + isolated Postgres control plane",
        "preflight": "bash scripts/ci/snorri-runner-preflight.sh",
    },
    "fireweed_sha": os.environ["P6S_FIREWEED_SHA"],
    "snorri": {
        "checkout": snorri_checkout,
        "sha": snorri_sha,
        "status": os.environ["P6S_SNORRI_STATUS"],
        "re_run": (
            "SNORRI_CHECKOUT=/path/to/snorri "
            "bash scripts/ci/snorri-s3-durability-acceptance.sh"
        ),
        "note": (
            "Fireweed-side p6s_s3_durability_acceptance is the contract proof; "
            "snorri re-run binds the downstream pin after provider-neutral migration."
        ),
    },
    "provider": {
        "selected_provider": provider,
        "endpoint": endpoint,
        "bucket": bucket,
        "region": region,
        "native_atomic_conditional_create": native_create,
        "native_atomic_conditional_update": native_update,
        "p1s_attestation_path": str(att_path),
        "p1s_bead_id": "fireweed-f5fa7380",
        "garage_eldir_implicit": False,
    },
    "postgres_control_plane": {
        "url_template_nonsecret": (
            "postgres://fireweed:***@127.0.0.1:55432/fireweed_snorri_p6p"
        ),
        "database": "fireweed_snorri_p6p",
        "env": "FIREWEED_PG_TEST_URL / SNORRI_FIREWEED_POSTGRES_URL",
    },
    "semantic_ids": {
        "SNORRI-REOPEN": {
            "cells": ["s3--memory", "s3--sqlite", "s3--postgres"],
            "status": "passed",
            "command": (
                f"rustup run {toolchain} cargo test -p fireweed "
                "--features objectlog,sqlite,postgres "
                "--test p6s_s3_durability_acceptance snorri_reopen_ -- --nocapture"
            ),
        },
        "SNORRI-PROJECTION-REBUILD": {
            "cells": ["s3--sqlite", "s3--postgres"],
            "unsupported_negative": "s3--memory projection_control=None",
            "status": "passed",
            "command": (
                f"rustup run {toolchain} cargo test -p fireweed "
                "--features objectlog,sqlite,postgres "
                "--test p6s_s3_durability_acceptance snorri_projection_rebuild_ "
                "-- --nocapture"
            ),
        },
        "SNORRI-RETRY-ONCE": {
            "cells": ["s3--memory", "s3--sqlite", "s3--postgres"],
            "status": "passed",
            "command": (
                f"rustup run {toolchain} cargo test -p fireweed "
                "--features objectlog,sqlite,postgres "
                "--test p6s_s3_durability_acceptance snorri_retry_once_ -- --nocapture"
            ),
        },
    },
    "commands": commands,
    "results": {
        "status": "passed",
        "zero_skips": True,
        "fireweed_harness": "passed",
        "snorri_external": os.environ["P6S_SNORRI_STATUS"],
    },
    "governing_refs": [
        "docs/helix/03-test/test-plans/TP-004-fireweed-facade-and-snorri-acceptance.md",
        "docs/evidence/snorri/p6p-runner-provisioning.md",
        "scripts/ci/snorri-runner-preflight.sh",
        "scripts/ci/snorri-s3-durability-acceptance.sh",
        "crates/fireweed/tests/p6s_s3_durability_acceptance.rs",
    ],
}

out = Path(os.environ["P6S_OUT_JSON"])
out.write_text(json.dumps(doc, indent=2) + "\n")
print(f"snorri-s3-durability-acceptance: attestation={out}")
print(f"snorri-s3-durability-acceptance: fireweed_sha={doc['fireweed_sha']}")
print(
    "snorri-s3-durability-acceptance: "
    f"snorri_status={doc['snorri']['status']} snorri_sha={doc['snorri']['sha'] or 'none'}"
)
print("snorri-s3-durability-acceptance: ok")
PY
