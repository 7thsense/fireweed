#!/usr/bin/env bash
# Contract tests for P1s S3 qualification endpoint selection + attestation.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO_ROOT"

QUAL="$REPO_ROOT/scripts/ci/s3-qualification-endpoint.sh"
PREFLIGHT="$REPO_ROOT/scripts/ci/s3-native-cas-preflight.py"
MANIFEST="$REPO_ROOT/docs/helix/04-build/storage-authority-manifest.json"

SECRET_DIR=$(mktemp -d /tmp/fireweed-s3-secrets-test-XXXXXX)
export FIREWEED_S3_SECRET_DIR="$SECRET_DIR"
CONTAINER_NAME="fireweed-s3-qual-test-$$"
export FIREWEED_S3_QUAL_CONTAINER="$CONTAINER_NAME"

cleanup() {
  bash "$QUAL" teardown >/dev/null 2>&1 || true
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  rm -rf "$SECRET_DIR"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

echo "=== s3-qualification-endpoint contract tests ==="

# ---------------------------------------------------------------------------
# Static contracts (no docker required beyond later live section)
# ---------------------------------------------------------------------------
[[ -x "$QUAL" || -f "$QUAL" ]] || fail "qualification script missing"
[[ -f "$PREFLIGHT" ]] || fail "preflight script missing"
[[ -f "$MANIFEST" ]] || fail "storage authority manifest missing"

# Manifest still owns the capability ID / s3_fields; we consume only.
python3 - "$MANIFEST" <<'PY' || fail "manifest capability / s3_fields drift"
import json, sys
path = sys.argv[1]
doc = json.load(open(path, encoding="utf-8"))
text = open(path, encoding="utf-8").read()
assert "S3-NATIVE-CAS-CAPABILITY-ATTESTATION" in text
assert '"P1s"' in text
topo = doc["topology_attestation"]
for field in (
    "provider",
    "version",
    "region",
    "native_atomic_conditional_create",
    "native_atomic_conditional_update",
    "consistency_contract",
    "tls_mode",
    "bucket_ownership_acknowledgement",
):
    assert field in topo["s3_fields"], field
assert ".env.garage-e3" in doc["tracked_ignore_policy"]["forbidden_in_repository_paths"]
print("manifest capability + s3_fields + forbidden path: ok")
PY
pass "manifest capability schema consumed without edit"

# Image pin constants present and digest-shaped.
grep -q 'MINIO_IMAGE_DIGEST="sha256:1dce27c494a16bae114774f1cec295493f3613142713130c2d22dd5696be6ad3"' "$QUAL" \
  || fail "MinIO image digest pin missing or drifted"
grep -q 'minio/minio@sha256:' "$QUAL" || fail "pinned image form missing"
grep -q 'S3-NATIVE-CAS-CAPABILITY-ATTESTATION' "$QUAL" || fail "capability id missing from script"
pass "image digest pin + capability id constants"

# Survey rejects Garage and names MinIO.
SURVEY_OUT=$(bash "$QUAL" survey)
echo "$SURVEY_OUT" | grep -q 'Garage v2.2.0' || fail "survey missing Garage candidate"
echo "$SURVEY_OUT" | grep -q 'REJECTED' || fail "survey must reject Garage"
echo "$SURVEY_OUT" | grep -q 'Hermetic MinIO' || fail "survey missing MinIO candidate"
echo "$SURVEY_OUT" | grep -q 'S3-NATIVE-CAS-CAPABILITY-ATTESTATION' || fail "survey missing capability id"
pass "survey candidates"

# .env.garage-e3 must be absent; isolation check fails closed if secret dir under repo.
test ! -e "$REPO_ROOT/.env.garage-e3" || fail ".env.garage-e3 must remain absent"
bash "$QUAL" verify-isolation >/dev/null
pass "verify-isolation (pre-provision; garage-e3 absent)"

# Secret dir under repo must be refused.
set +e
FIREWEED_S3_SECRET_DIR="$REPO_ROOT/tmp-secrets-should-fail" \
  bash "$QUAL" verify-isolation >/tmp/fw-s3-isol-fail.$$ 2>&1
STATUS=$?
set -e
[[ "$STATUS" -ne 0 ]] || fail "expected in-repo secret dir rejection"
grep -q 'OUTSIDE the repository' /tmp/fw-s3-isol-fail.$$ || fail "rejection message missing"
rm -f /tmp/fw-s3-isol-fail.$$
pass "refuses in-repo secret directory"

# ---------------------------------------------------------------------------
# Live provision → two-writer CAS preflight → attestation → teardown
# ---------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  fail "docker is required for live P1s qualification"
fi

echo "--- live provision + CAS preflight ---"
bash "$QUAL" provision

[[ -f "$SECRET_DIR/credentials.env" ]] || fail "secret file not written"
[[ -f "$SECRET_DIR/s3-native-cas-capability-attestation.json" ]] || fail "attestation not written"
[[ -f "$SECRET_DIR/state/preflight.json" ]] || fail "preflight json not written"

# Secret file mode must be owner-only.
perm=$(stat -c '%a' "$SECRET_DIR/credentials.env" 2>/dev/null || stat -f '%Lp' "$SECRET_DIR/credentials.env")
[[ "$perm" == "600" ]] || fail "secret file mode must be 600, got $perm"
pass "secret file mode 600 outside repo"

# Attestation checks
python3 - "$SECRET_DIR/s3-native-cas-capability-attestation.json" "$SECRET_DIR/credentials.env" <<'PY'
import json, sys
att_path, secret_path = sys.argv[1], sys.argv[2]
doc = json.load(open(att_path, encoding="utf-8"))
secrets = {}
for line in open(secret_path, encoding="utf-8"):
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    secrets[k] = v
raw = open(att_path, encoding="utf-8").read()
for k in ("FIREWEED_S3_TEST_ACCESS_KEY", "FIREWEED_S3_TEST_SECRET_KEY"):
    assert secrets[k] not in raw, f"attestation leaked {k}"
assert doc["capability_id"] == "S3-NATIVE-CAS-CAPABILITY-ATTESTATION"
assert doc["plan_key"] == "P1s"
assert doc["bead_id"] == "fireweed-f5fa7380"
assert doc["s3"]["provider"] == "minio"
assert doc["s3"]["native_atomic_conditional_create"] is True
assert doc["s3"]["native_atomic_conditional_update"] is True
assert doc["s3"]["bucket_ownership_acknowledgement"] == secrets["FIREWEED_S3_TEST_BUCKET"]
assert doc["s3"]["endpoint"] == secrets["FIREWEED_S3_TEST_ENDPOINT"]
assert doc["credential_path_isolation"]["secret_file_outside_repository"] is True
assert doc["credential_path_isolation"]["env_garage_e3_absent"] is True
assert doc["credential_path_isolation"]["attestation_contains_credential_values"] is False
assert doc["preflight"]["status"] == "passed"
assert doc["preflight"]["sequential_create_only"]["second_status"] == 412
assert doc["preflight"]["two_writer_create_only_race"]["winner_status"] in (200, 204)
assert doc["results"]["selected"] is True
assert any(c["provider"] == "garage" and c["selectable"] is False for c in doc["results"]["rejected_candidates"])
print("attestation content + redaction: ok")
PY
pass "attestation keyed to approved capability id with redaction"

bash "$QUAL" verify-isolation
pass "verify-isolation post-provision"

# Repeat preflight against the live secret file (idempotent CAS proof).
bash "$QUAL" preflight
pass "repeat preflight from secret file"

# Teardown must remove the container.
bash "$QUAL" teardown
if docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" >/dev/null 2>&1; then
  running=$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME")
  [[ "$running" != "true" ]] || fail "container still running after teardown"
fi
pass "bounded teardown"

echo "=== all s3-qualification-endpoint contract tests passed ==="
