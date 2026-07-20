#!/usr/bin/env bash
# Stage an exact-revision TP-002 composite from explicit producer outputs.
# This script does not invent evidence and never scans target/ for substitutes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source_dir="" e3_dir="" out="" revision="" tag="" produced_at="" reviewed_at=""
while (($#)); do
  case "$1" in
    --source-dir) source_dir="$2"; shift 2 ;;
    --e3-source-dir) e3_dir="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --revision) revision="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --produced-at) produced_at="$2"; shift 2 ;;
    --reviewed-at) reviewed_at="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
done
[[ -d "$source_dir" && -d "$e3_dir" && -n "$out" && "$revision" =~ ^[0-9a-f]{40}$ ]] || {
  echo "usage: $0 --source-dir <dir> --e3-source-dir <dir> --out <dir> --revision <sha>" >&2; exit 64;
}
[[ "$revision" == "$(git -C "$REPO_ROOT" rev-parse HEAD)" ]] || { echo "revision must equal checked-out HEAD" >&2; exit 1; }
for name in e0.jsonl e1.jsonl e2-scale.jsonl e2-density.jsonl e2-failover.json; do
  [[ -f "$source_dir/$name" && ! -L "$source_dir/$name" ]] || { echo "missing regular source artifact: $source_dir/$name" >&2; exit 1; }
done
for name in e3.jsonl tp003.jsonl fencing.json; do
  [[ -f "$e3_dir/$name" && ! -L "$e3_dir/$name" ]] || { echo "missing E3 producer artifact: $e3_dir/$name" >&2; exit 1; }
done
if find "$e3_dir" -type l -print -quit | grep -q .; then echo "E3 source contains a symlink" >&2; exit 1; fi

[[ ! -e "$out" ]] || { echo "output already exists; choose a fresh staging directory: $out" >&2; exit 1; }
mkdir -p "$out/e3"
for name in e0.jsonl e1.jsonl e2-scale.jsonl e2-density.jsonl e2-failover.json; do install -m 0644 "$source_dir/$name" "$out/$name"; done
cp -R "$e3_dir/." "$out/e3/"
rustup run 1.92.0 cargo run -q -p pqueue-release --bin pqueue-build-e3-contract -- \
  --out "$out/e3/e3-contract.json" --source-revision "$revision" \
  --e3-ledger "$out/e3/e3.jsonl" --transaction-evidence "$out/e3/tp003.jsonl" \
  --fencing-evidence "$out/e3/fencing.json"

python3 - "$out/composite-contract.json" "$revision" <<'PY'
import json, sys
path, revision = sys.argv[1:]
contract = {"schema_version": 1, "source_revision": revision, "authorities": {
  "e0": "e0.jsonl", "e1": "e1.jsonl", "e2_scale": "e2-scale.jsonl",
  "e2_density": "e2-density.jsonl", "e2_failover": "e2-failover.json",
  "e3_contract": "e3/e3-contract.json"}}
with open(path, "w", encoding="utf-8") as f: json.dump(contract, f, indent=2, sort_keys=True); f.write("\n")
PY

bash "$REPO_ROOT/scripts/ci/verify-governed-release-composite.sh" \
  --contract "$out/composite-contract.json" --expected-revision "$revision"
if [[ -n "$tag$produced_at$reviewed_at" ]]; then
  [[ -n "$tag" && -n "$produced_at" && -n "$reviewed_at" ]] || { echo "tag/timestamps must be supplied together" >&2; exit 64; }
  bundle_rel="$(realpath -m --relative-to="$REPO_ROOT" "$out")"
  [[ "$bundle_rel" != .. && "$bundle_rel" != ../* ]] || { echo "attested output must be inside the repository" >&2; exit 1; }
  rustup run 1.92.0 cargo run -q -p pqueue-release --bin pqueue-build-evidence-attestation -- \
    --repo-root "$REPO_ROOT" --bundle "$bundle_rel" --tag "$tag" --commit "$revision" \
    --produced-at "$produced_at" --reviewed-at "$reviewed_at" --out "$out/attestation.json"
  rustup run 1.92.0 cargo run -q -p pqueue-release --bin pqueue-verify-evidence-attestation -- \
    --manifest "$out/attestation.json" --repo-root "$REPO_ROOT" --tag "$tag" --commit "$revision"
fi
echo "staged governed evidence bundle: $out"
