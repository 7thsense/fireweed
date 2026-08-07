#!/usr/bin/env bash
# Sole versioned promoter for governed evidence (P17e).
#
# Given explicit S, campaign, external bundle root, and manifest allowlist:
#   - verifies source predicate at S
#   - verifies content digests for allowlisted bundle artifacts
#   - copies only exact new-current paths into a fresh promotion worktree
#   - rejects historical-path writes and non-allowlisted diffs
#   - creates single evidence commit E with measured-source S metadata
#   - reports E on stdout; never mutates source/tooling checkout
#
# This script never writes tracked evidence into the product source checkout.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source_root="" bundle_root="" allowlist="" expected_source="" expected_remote="" expected_ref=""
campaign="" promotion_root="" message_prefix="chore(evidence): promote governed bundle"

usage() {
  cat <<'EOF' >&2
usage: promote-governed-evidence.sh \
  --source-root <S-checkout> \
  --expected-source <40-hex> \
  --expected-remote <url-or-name> \
  --expected-ref <ref> \
  --campaign <name> \
  --bundle-root <external-dir> \
  --allowlist <json> \
  --promotion-root <external-empty-dir>
EOF
}

while (($#)); do
  case "$1" in
    --source-root) source_root="$2"; shift 2 ;;
    --expected-source) expected_source="$2"; shift 2 ;;
    --expected-remote) expected_remote="$2"; shift 2 ;;
    --expected-ref) expected_ref="$2"; shift 2 ;;
    --campaign) campaign="$2"; shift 2 ;;
    --bundle-root) bundle_root="$2"; shift 2 ;;
    --allowlist) allowlist="$2"; shift 2 ;;
    --promotion-root) promotion_root="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 64 ;;
  esac
done

[[ -n "$source_root" && -n "$expected_source" && -n "$expected_remote" && -n "$expected_ref" \
  && -n "$campaign" && -n "$bundle_root" && -n "$allowlist" && -n "$promotion_root" ]] || {
  usage
  exit 64
}

source_root="$(cd "$source_root" && pwd -P)"
bundle_root="$(cd "$bundle_root" && pwd -P)"
allowlist="$(realpath "$allowlist")"
promotion_root="$(realpath -m "$promotion_root")"

case "$bundle_root" in
  "$source_root"/*) echo "bundle-root must be outside source-root" >&2; exit 1 ;;
esac
case "$promotion_root" in
  "$source_root"/*) echo "promotion-root must be outside source-root" >&2; exit 1 ;;
  "$bundle_root"/*) echo "promotion-root must be outside bundle-root" >&2; exit 1 ;;
esac
[[ ! -e "$promotion_root" ]] || { echo "promotion-root must not already exist: $promotion_root" >&2; exit 1; }

bash "$SCRIPT_DIR/verify-source-predicate.sh" \
  --mode source \
  --source-root "$source_root" \
  --expected-source "$expected_source" \
  --expected-remote "$expected_remote" \
  --expected-ref "$expected_ref"

python3 - "$source_root" "$bundle_root" "$allowlist" "$expected_source" "$campaign" "$promotion_root" "$expected_ref" <<'PY'
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

source_root = Path(sys.argv[1]).resolve()
bundle_root = Path(sys.argv[2]).resolve()
allowlist_path = Path(sys.argv[3]).resolve()
expected_source = sys.argv[4]
campaign = sys.argv[5]
promotion_root = Path(sys.argv[6]).resolve()
expected_ref = sys.argv[7]

raw = json.loads(allowlist_path.read_text(encoding="utf-8"))
if not isinstance(raw, dict):
    raise SystemExit("allowlist must be a JSON object")
paths = raw.get("paths") or []
historical = set(raw.get("historical_paths") or [])
if raw.get("expected_source") not in (None, expected_source):
    raise SystemExit("allowlist expected_source mismatch")
if raw.get("campaign") not in (None, campaign):
    raise SystemExit("allowlist campaign mismatch")
if not paths:
    raise SystemExit("allowlist paths empty")

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

# Verify every allowlisted path exists as a regular file in the external bundle
# with the declared digest; reject extras required by allowlist digests map.
digests = raw.get("digests") or {}
for rel in paths:
    if rel in historical:
        raise SystemExit(f"allowlist path is historical and cannot be promoted: {rel}")
    src = (bundle_root / rel).resolve()
    if not str(src).startswith(str(bundle_root) + os.sep):
        raise SystemExit(f"bundle path escapes bundle-root: {rel}")
    if src.is_symlink() or not src.is_file():
        raise SystemExit(f"bundle path must be a regular file: {rel}")
    actual = sha256(src)
    expected = digests.get(rel)
    if expected is None:
        raise SystemExit(f"missing digest for allowlisted path: {rel}")
    if actual != expected:
        raise SystemExit(f"digest mismatch for {rel}: {actual} != {expected}")

# Reject unexpected files under bundle root (extras).
bundle_files = []
for dirpath, _, filenames in os.walk(bundle_root):
    for name in filenames:
        full = Path(dirpath) / name
        if full.is_symlink():
            raise SystemExit(f"symlink not permitted in bundle: {full}")
        rel = full.relative_to(bundle_root).as_posix()
        bundle_files.append(rel)
extra = sorted(set(bundle_files) - set(paths))
missing = sorted(set(paths) - set(bundle_files))
if missing:
    raise SystemExit(f"bundle missing allowlisted paths: {missing}")
if extra:
    raise SystemExit(f"bundle contains non-allowlisted paths: {extra}")

# Create promotion worktree at S, copy only allowlisted new-current paths, commit E.
promotion_root.mkdir(parents=True)
subprocess.check_call(
    ["git", "-C", str(source_root), "worktree", "add", "--detach", str(promotion_root), expected_source],
    stdout=subprocess.DEVNULL,
)
try:
    for rel in paths:
        dest = promotion_root / rel
        if any(part == ".." for part in Path(rel).parts):
            raise SystemExit(f"path escape in allowlist: {rel}")
        # Historical overwrite/delete protection
        if rel in historical:
            raise SystemExit(f"refusing historical overwrite: {rel}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        data = (bundle_root / rel).read_bytes()
        dest.write_bytes(data)
        subprocess.check_call(["git", "-C", str(promotion_root), "add", "--", rel])

    # Reject any staged path outside allowlist
    staged = subprocess.check_output(
        ["git", "-C", str(promotion_root), "diff", "--cached", "--name-only"],
        text=True,
    ).splitlines()
    unexpected = sorted(set(staged) - set(paths))
    if unexpected:
        raise SystemExit(f"non-allowlisted staged paths: {unexpected}")
    if sorted(staged) != sorted(paths):
        raise SystemExit(f"staged set mismatch: {staged} vs {paths}")

    message = (
        f"chore(evidence): promote governed bundle for campaign {campaign}\n\n"
        f"Measured-source: {expected_source}\n"
        f"Source-ref: {expected_ref}\n"
        f"Campaign: {campaign}\n"
    )
    env = os.environ.copy()
    env.setdefault("GIT_AUTHOR_NAME", "Fireweed Evidence Promoter")
    env.setdefault("GIT_AUTHOR_EMAIL", "evidence-promoter@fireweed.invalid")
    env.setdefault("GIT_COMMITTER_NAME", env["GIT_AUTHOR_NAME"])
    env.setdefault("GIT_COMMITTER_EMAIL", env["GIT_AUTHOR_EMAIL"])
    subprocess.check_call(
        ["git", "-C", str(promotion_root), "commit", "-m", message],
        env=env,
        stdout=subprocess.DEVNULL,
    )
    e_sha = subprocess.check_output(
        ["git", "-C", str(promotion_root), "rev-parse", "HEAD"], text=True
    ).strip()
    # Prove exact parent is S
    parent = subprocess.check_output(
        ["git", "-C", str(promotion_root), "rev-parse", "HEAD^"], text=True
    ).strip()
    if parent != expected_source:
        raise SystemExit(f"E parent {parent} != S {expected_source}")
    # diff(S,E) must equal allowlist paths only
    diff_names = subprocess.check_output(
        ["git", "-C", str(promotion_root), "diff", "--name-only", expected_source, e_sha],
        text=True,
    ).splitlines()
    if sorted(diff_names) != sorted(paths):
        raise SystemExit(f"diff(S,E) mismatch: {diff_names} vs {paths}")
    print(e_sha)
finally:
    # Keep promotion worktree for caller inspection; do not delete commit.
    pass
PY
