#!/usr/bin/env bash
# P11 — generate distinct *current* TP-003 T3 and TD-008 evidence outside the repo.
#
# Historical paths under docs/perf/evidence/tp003-*.jsonl and
# docs/perf/evidence/td008-terminal-reap-frontier.jsonl are immutable (never rewritten).
# This script writes RunOwned artifacts under a caller-provided external directory.
#
# Usage:
#   export FIREWEED_PG_TEST_URL=postgres://...
#   export FIREWEED_P11_EVIDENCE_DIR=/tmp/fireweed-p11-current   # must be outside the repo
#   # optional S3 for object-log T0–T3:
#   # source /path/to/credentials.env
#   bash scripts/ci/record-current-tp003-td008-evidence.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cargo_cmd=(rustup run 1.92.0 cargo)

: "${FIREWEED_PG_TEST_URL:?set FIREWEED_PG_TEST_URL for Class A T3 cells that need Postgres}"
: "${FIREWEED_P11_EVIDENCE_DIR:?set FIREWEED_P11_EVIDENCE_DIR to a newly created external directory}"

if [[ ! -d "$FIREWEED_P11_EVIDENCE_DIR" ]]; then
  echo "P11 evidence directory must already exist" >&2
  exit 2
fi
evidence_dir=$(cd "$FIREWEED_P11_EVIDENCE_DIR" && pwd -P)
case "$evidence_dir/" in
  "$repo_root"/*)
    echo "P11 evidence directory must be outside the repository (RunOwned)" >&2
    exit 2
    ;;
esac

mkdir -p "$evidence_dir/tp003" "$evidence_dir/td008"
sqlite_out="$evidence_dir/tp003/tp003-ac-txn-matrix-sqlite-storage-pairs.jsonl"
postgres_out="$evidence_dir/tp003/tp003-ac-txn-matrix-postgres-storage-pairs.jsonl"
manifest_out="$evidence_dir/CURRENT-TP003-TRANSACTION-MATRIX.manifest.json"
td008_note="$evidence_dir/td008/CURRENT-TD008-DELIVERY-MATRIX.note.json"

cd "$repo_root"
source_revision=$(git rev-parse HEAD)

echo "--- P11 TP-003 T3: sqlite log exact pairs ---"
FIREWEED_TP003_SQLITE_EVIDENCE_OUT="$sqlite_out" \
  "${cargo_cmd[@]}" test --locked -p fireweed-server --features postgres --lib \
  sqlite_log_matrix_tests::sqlite_log_t3_tp003_ac_txn_exact_pairs -- --exact --nocapture
test -s "$sqlite_out"

echo "--- P11 TP-003 T3: postgres log exact pairs ---"
FIREWEED_TP003_POSTGRES_EVIDENCE_OUT="$postgres_out" \
  "${cargo_cmd[@]}" test --locked -p fireweed-server --features postgres --lib \
  postgres_log_matrix_tests::postgres_log_t3_tp003_ac_txn_exact_pairs -- --exact --nocapture
test -s "$postgres_out"

echo "--- P11 filesystem Class A T0–T3 (3 cells) ---"
"${cargo_cmd[@]}" test --locked -p fireweed --features memory,sqlite,objectlog,postgres,turso \
  --test storage_matrix_t0_t2 filesystem_log_three_cells_t0_t3_contract -- --exact --nocapture

if [[ -n "${FIREWEED_S3_TEST_ENDPOINT:-}" ]]; then
  echo "--- P11 S3 Class A T0–T3 (3 cells) ---"
  "${cargo_cmd[@]}" test --locked -p fireweed --features memory,sqlite,objectlog,postgres,turso \
    --test storage_matrix_t0_t2 s3_log_three_cells_t0_t3_contract -- --exact --nocapture
else
  echo "--- P11 S3 T0–T3 skipped (FIREWEED_S3_TEST_ENDPOINT unset; not claimed) ---"
fi

echo "--- P11 TD-008 evidence harness (run-owned, rejects static/historical writes) ---"
"${cargo_cmd[@]}" test --locked -p fireweed-release --test td008_evidence -- --nocapture

python3 - "$sqlite_out" "$postgres_out" "$manifest_out" "$source_revision" <<'PY'
import hashlib, json, sys
from pathlib import Path

sqlite_out, postgres_out, manifest_out, source_revision = sys.argv[1:5]
rows = []
for path in (sqlite_out, postgres_out):
    body = Path(path).read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    parsed = [json.loads(line) for line in body.decode().splitlines() if line.strip()]
    rows.append({
        "path": path,
        "sha256": digest,
        "row_count": len(parsed),
        "passes": sum(1 for r in parsed if r.get("result") == "pass"),
        "fails": sum(1 for r in parsed if r.get("result") != "pass"),
        "backends": sorted({r.get("backend") for r in parsed}),
        "acs": sorted({r.get("ac") for r in parsed}),
    })
    if any(r.get("result") != "pass" for r in parsed):
        raise SystemExit(f"non-pass rows in {path}")

manifest = {
    "semantic_current_id": "CURRENT-TP003-TRANSACTION-MATRIX",
    "plan_key": "P11",
    "source_revision": source_revision,
    "historical_paths_untouched": [
        "docs/perf/evidence/tp003-ac-txn-matrix.jsonl",
        "docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl",
        "docs/perf/evidence/tp003-ac-txn-matrix-postgres-storage-pairs.jsonl",
        "docs/perf/evidence/tp003-ac-txn-matrix-sqlite-storage-pairs.jsonl",
        "docs/perf/evidence/tp003-ac-txn-matrix-s3-storage-pairs.jsonl",
        "docs/perf/evidence/tp003-ac-txn-parity-postgres-storage-pairs.jsonl",
    ],
    "artifacts": rows,
    "class_a_t3_cells_claimed": [
        "sqlite×memory", "sqlite×sqlite", "sqlite×postgres",
        "postgres×memory", "postgres×sqlite", "postgres×postgres",
    ],
    "notes": [
        "Turso projection T3 is covered by product default + Class-B/server leaves separately.",
        "Object-log filesystem/S3 T0–T3 contracts run via storage_matrix_t0_t2.",
        "P18 promotes allowlisted current paths; this script only produces RunOwned outputs.",
    ],
}
Path(manifest_out).write_text(json.dumps(manifest, indent=2) + "\n")
print(f"wrote {manifest_out}")
PY

python3 - "$td008_note" "$source_revision" <<'PY'
import json, sys
from pathlib import Path
out, rev = sys.argv[1:3]
Path(out).write_text(json.dumps({
    "semantic_current_id": "CURRENT-TD008-DELIVERY-MATRIX",
    "plan_key": "P11",
    "source_revision": rev,
    "historical_path_untouched": "docs/perf/evidence/td008-terminal-reap-frontier.jsonl",
    "producer": "cargo test -p fireweed-release --test td008_evidence",
    "claims": [
        "td008_evidence_bundle_recorded (observed run-owned ledger)",
        "td008_observed_evidence_row_matches_run",
        "td008_evidence_ledger_rejects_static_attestation",
        "td008_tracked_artifact_rejects_write_and_delete_authority",
    ],
    "notes": [
        "Historical td008-terminal-reap-frontier.jsonl remains Fixture/historical only.",
        "Current TD-008 qualification is run-owned and rejects static seeding/string proof.",
    ],
}, indent=2) + "\n")
print(f"wrote {out}")
PY

echo "P11 current evidence root: $evidence_dir"
echo "  TP-003 sqlite:   $sqlite_out"
echo "  TP-003 postgres: $postgres_out"
echo "  TP-003 manifest: $manifest_out"
echo "  TD-008 note:     $td008_note"
