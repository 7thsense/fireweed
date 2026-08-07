#!/usr/bin/env bash
# Stage an exact-revision TP-002 composite from explicit producer outputs.
# This script does not invent evidence and never scans target/ for substitutes.
# Measured source S is bound through the shared source predicate (no ambient SHA).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source_dir="" e3_dir="" out="" revision="" tag="" produced_at="" reviewed_at=""
source_root="$REPO_ROOT" expected_source="" expected_remote="" expected_ref=""
while (($#)); do
  case "$1" in
    --source-dir) source_dir="$2"; shift 2 ;;
    --e3-source-dir) e3_dir="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --revision) revision="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --produced-at) produced_at="$2"; shift 2 ;;
    --reviewed-at) reviewed_at="$2"; shift 2 ;;
    --source-root) source_root="$2"; shift 2 ;;
    --expected-source) expected_source="$2"; shift 2 ;;
    --expected-remote) expected_remote="$2"; shift 2 ;;
    --expected-ref) expected_ref="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
done
[[ -d "$source_dir" && -d "$e3_dir" && -n "$out" && "$revision" =~ ^[0-9a-f]{40}$ ]] || {
  echo "usage: $0 --source-dir <dir> --e3-source-dir <dir> --out <dir> --revision <sha> --expected-source <sha> --expected-remote <url-or-name> --expected-ref <ref> [--source-root <dir>]" >&2
  exit 64
}
[[ -n "$expected_source" && -n "$expected_remote" && -n "$expected_ref" ]] || {
  echo "usage: $0 requires --expected-source --expected-remote --expected-ref (no ambient SHA)" >&2
  exit 64
}
[[ "$revision" == "$expected_source" ]] || {
  echo "revision must equal --expected-source (${expected_source})" >&2
  exit 1
}

source_root="$(cd "$source_root" && pwd -P)"
bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source \
  --source-root "$source_root" \
  --expected-source "$expected_source" \
  --expected-remote "$expected_remote" \
  --expected-ref "$expected_ref"

out="$(realpath -m "$out")"
case "$out" in
  "$source_root"/*) echo "output must be outside the source checkout: $out" >&2; exit 1 ;;
esac
tooling_root="$(cd "$REPO_ROOT" && pwd -P)"
case "$out" in
  "$tooling_root"/*) echo "output must be outside the repository: $out" >&2; exit 1 ;;
esac
[[ "$(basename "$out")" == tp002-release ]] || {
  echo "output basename must be tp002-release so the release workflow extracts the governed path" >&2
  exit 1
}
if [[ -n "$tag$produced_at$reviewed_at" ]]; then
  [[ -n "$tag" && -n "$produced_at" && -n "$reviewed_at" ]] || {
    echo "tag/timestamps must be supplied together" >&2
    exit 64
  }
  archive_dir="$(dirname "$out")"
  archive="$archive_dir/$revision.tar.gz"
  sidecar="$archive.sha256"
  [[ ! -e "$archive" && ! -e "$sidecar" ]] || {
    echo "exact-revision archive or digest already exists; refusing stale substitution" >&2
    exit 1
  }
fi
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
for name in e3.jsonl tp003.jsonl fencing.json; do install -m 0644 "$e3_dir/$name" "$out/e3/$name"; done
rustup run 1.92.0 cargo run -q -p fireweed-release --bin fireweed-build-e3-contract -- \
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
  rustup run 1.92.0 cargo run -q -p fireweed-release --bin fireweed-build-evidence-attestation -- \
    --repo-root "$source_root" --bundle-root "$out" --tag "$tag" --commit "$revision" \
    --produced-at "$produced_at" --reviewed-at "$reviewed_at" --out "$out/attestation.json"
  rustup run 1.92.0 cargo run -q -p fireweed-release --bin fireweed-verify-evidence-attestation -- \
    --manifest "$out/attestation.json" --repo-root "$source_root" --evidence-root "$out" \
    --tag "$tag" --commit "$revision"

  archive_tmp="$(mktemp "$archive_dir/.${revision}.tar.XXXXXX")"
  gzip_tmp="$(mktemp "$archive_dir/.${revision}.tar.gz.XXXXXX")"
  sidecar_tmp="$(mktemp "$archive_dir/.${revision}.sha256.XXXXXX")"
  cleanup_archive_temps() { rm -f "$archive_tmp" "$gzip_tmp" "$sidecar_tmp"; }
  trap cleanup_archive_temps EXIT
  tar --sort=name --format=gnu --mode='u+rwX,go+rX,go-w' \
    --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -C "$archive_dir" -cf "$archive_tmp" tp002-release
  gzip -n -9 <"$archive_tmp" >"$gzip_tmp"
  archive_sha="$(sha256sum "$gzip_tmp" | awk '{print $1}')"
  [[ "$archive_sha" =~ ^[0-9a-f]{64}$ ]]
  printf '%s  %s\n' "$archive_sha" "$(basename "$archive")" >"$sidecar_tmp"
  mv "$gzip_tmp" "$archive"
  mv "$sidecar_tmp" "$sidecar"
  rm -f "$archive_tmp"
  trap - EXIT
  echo "wrote governed evidence archive: $archive"
  echo "wrote governed evidence digest: $sidecar"
fi
echo "staged governed evidence bundle: $out"
