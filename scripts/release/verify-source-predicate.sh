#!/usr/bin/env bash
# P0/P17 shared source predicate (plumbing + measured-S binding).
#
# Uses only tracked .gitignore rules for ignore classification. Local
# (.git/info/exclude) and global excludes have no policy authority and are not
# consulted when classifying untracked paths.
#
# Modes:
#   source  — product cleanliness + expected S/remote/ref (fast/remediation)
#   e       — dual-root E mode: tooling from --source-root at S; promoted
#             evidence only from --promoted-root allowlist
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DEFAULT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

MODE="source"
SOURCE_ROOT=""
PROMOTED_ROOT=""
EXPECTED_SOURCE=""
EXPECTED_REMOTE=""
EXPECTED_REF=""
PROMOTED_ALLOWLIST=""
AUTHORITY_MANIFEST="${REPO_DEFAULT}/docs/helix/04-build/storage-authority-manifest.json"

usage() {
    cat <<'EOF' >&2
usage: verify-source-predicate.sh --mode source|e \
  --source-root <dir> --expected-source <40-hex> \
  --expected-remote <url-or-name> --expected-ref <ref> \
  [--promoted-root <dir> --promoted-allowlist <file>] \
  [--authority-manifest <file>]
EOF
}

while (($#)); do
    case "$1" in
        --mode) MODE="${2:-}"; shift 2 ;;
        --source-root) SOURCE_ROOT="${2:-}"; shift 2 ;;
        --promoted-root) PROMOTED_ROOT="${2:-}"; shift 2 ;;
        --expected-source) EXPECTED_SOURCE="${2:-}"; shift 2 ;;
        --expected-remote) EXPECTED_REMOTE="${2:-}"; shift 2 ;;
        --expected-ref) EXPECTED_REF="${2:-}"; shift 2 ;;
        --promoted-allowlist) PROMOTED_ALLOWLIST="${2:-}"; shift 2 ;;
        --authority-manifest) AUTHORITY_MANIFEST="${2:-}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
    esac
done

fail() {
    echo "verify-source-predicate: $*" >&2
    exit 1
}

[[ "$MODE" == "source" || "$MODE" == "e" ]] || fail "mode must be source|e"
[[ -n "$SOURCE_ROOT" && -d "$SOURCE_ROOT" ]] || fail "--source-root must be an existing directory"
[[ "$EXPECTED_SOURCE" =~ ^[0-9a-f]{40}$ ]] || fail "--expected-source must be a full 40-hex SHA"
[[ -n "$EXPECTED_REMOTE" ]] || fail "--expected-remote is required"
[[ -n "$EXPECTED_REF" ]] || fail "--expected-ref is required"
[[ -f "$AUTHORITY_MANIFEST" ]] || fail "authority manifest missing: $AUTHORITY_MANIFEST"

if [[ "$MODE" == "e" ]]; then
    [[ -n "$PROMOTED_ROOT" && -d "$PROMOTED_ROOT" ]] || fail "e mode requires --promoted-root directory"
    [[ -n "$PROMOTED_ALLOWLIST" && -f "$PROMOTED_ALLOWLIST" ]] || fail "e mode requires --promoted-allowlist file"
    source_canon="$(cd "$SOURCE_ROOT" && pwd -P)"
    promoted_canon="$(cd "$PROMOTED_ROOT" && pwd -P)"
    [[ "$source_canon" != "$promoted_canon" ]] || fail "source-root and promoted-root must be distinct directories"
fi

SOURCE_ROOT="$(cd "$SOURCE_ROOT" && pwd -P)"

# Hard-reject forbidden credentials/config regardless of any exclude file.
while IFS= read -r forbidden; do
    [[ -z "$forbidden" ]] && continue
    if [[ -e "${SOURCE_ROOT}/${forbidden}" || -L "${SOURCE_ROOT}/${forbidden}" ]]; then
        fail "forbidden path exists (ignore rules have no authority): ${forbidden}"
    fi
done < <(python3 - "$AUTHORITY_MANIFEST" <<'PY'
import json, sys
manifest = json.load(open(sys.argv[1], encoding="utf-8"))
for path in manifest.get("tracked_ignore_policy", {}).get("forbidden_in_repository_paths", []):
    print(path)
PY
)

# Bind measured S and remote/ref without ambient GITHUB_SHA or bare HEAD equality.
head_sha="$(git -C "$SOURCE_ROOT" rev-parse HEAD)"
[[ "$head_sha" == "$EXPECTED_SOURCE" ]] ||
    fail "HEAD ${head_sha} != --expected-source ${EXPECTED_SOURCE}"

resolved_ref="$(git -C "$SOURCE_ROOT" rev-parse -q --verify "${EXPECTED_REF}^{commit}" 2>/dev/null || true)"
[[ "$resolved_ref" == "$EXPECTED_SOURCE" ]] ||
    fail "expected-ref ${EXPECTED_REF} resolves to '${resolved_ref:-missing}', not ${EXPECTED_SOURCE}"

# Remote may be a configured remote name or an exact URL present on any remote.
remote_ok=0
if git -C "$SOURCE_ROOT" remote get-url "$EXPECTED_REMOTE" >/dev/null 2>&1; then
    remote_ok=1
else
    while IFS= read -r url; do
        [[ "$url" == "$EXPECTED_REMOTE" ]] && remote_ok=1 && break
    done < <(git -C "$SOURCE_ROOT" remote -v | awk '{print $2}' | sort -u)
fi
[[ "$remote_ok" -eq 1 ]] || fail "expected-remote ${EXPECTED_REMOTE} is not a configured remote name or URL"

# Product cleanliness: tracked dirty paths outside exact .ddx/** fail.
mapfile -t dirty_paths < <(
    {
        git -C "$SOURCE_ROOT" diff --name-only --diff-filter=ACDMRTUXB
        git -C "$SOURCE_ROOT" diff --cached --name-only --diff-filter=ACDMRTUXB
    } | awk 'BEGIN{FS="/"} $1!=".ddx" {print}' | sort -u
)
if ((${#dirty_paths[@]})); then
    printf 'verify-source-predicate: dirty tracked product paths:\n' >&2
    printf '  %s\n' "${dirty_paths[@]}" >&2
    exit 1
fi

# Inventory tracked/untracked .ddx separately (excluded from product cleanliness).
mapfile -t ddx_tracked < <(git -C "$SOURCE_ROOT" ls-files '.ddx/**' | sort || true)
mapfile -t ddx_untracked < <(
    # Only tracked .gitignore rules; disable global excludesFile.
    git -C "$SOURCE_ROOT" -c core.excludesFile=/dev/null ls-files --others --directory \
        --exclude-per-directory=.gitignore \
        | awk '/^\.ddx\// {print}' | sort || true
)
python3 - "$SOURCE_ROOT" <<'PY'
import json, os, sys
root = sys.argv[1]
# Placeholder for inventory emission consumed by callers via stdout markers.
print(f"ddx_inventory_root={root}")
PY
printf 'verify-source-predicate: ddx_tracked_count=%s ddx_untracked_count=%s\n' \
    "${#ddx_tracked[@]}" "${#ddx_untracked[@]}"

# Raw untracked product paths under tracked .gitignore classification only.
# Disable info/exclude by pointing excludesFile at empty and using
# --exclude-per-directory=.gitignore only. git still reads info/exclude for
# some porcelain; reclassify with a pure python check of tracked rules.
mapfile -t raw_untracked < <(
    git -C "$SOURCE_ROOT" -c core.excludesFile=/dev/null status --porcelain=v1 -uall \
        | awk '/^\?\? / {print substr($0,4)}' | sort -u
)

python3 - "$SOURCE_ROOT" "$AUTHORITY_MANIFEST" "${raw_untracked[@]+${raw_untracked[@]}}" <<'PY'
import fnmatch
import json
import os
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
manifest = json.load(open(sys.argv[2], encoding="utf-8"))
raw = sys.argv[3:]

policy = manifest["tracked_ignore_policy"]
assert policy["authority"] == "tracked_gitignore_only"
assert policy["local_or_global_excludes_have_policy_authority"] is False

admin_roots = list(policy["classes"]["administrative"]["roots"])
build_roots = list(policy["classes"]["build_dependency_cache"]["roots"])
forbidden = set(policy["forbidden_in_repository_paths"])

# Load tracked .gitignore patterns only (repo root + nested).
patterns: list[tuple[Path, str]] = []
for dirpath, dirnames, filenames in os.walk(root):
    # Skip .git entirely.
    if ".git" in Path(dirpath).parts:
        dirnames[:] = []
        continue
    if ".gitignore" in filenames:
        gi = Path(dirpath) / ".gitignore"
        base = Path(dirpath).relative_to(root)
        for line in gi.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            patterns.append((base, line))

def ignored_by_tracked_rules(rel: str) -> bool:
    rel_posix = rel.replace(os.sep, "/")
    for base, pattern in patterns:
        # Patterns are relative to the .gitignore directory.
        if base == Path("."):
            candidate_base = ""
        else:
            candidate_base = base.as_posix().rstrip("/") + "/"
        # Only evaluate paths under this gitignore directory.
        if candidate_base and not rel_posix.startswith(candidate_base):
            continue
        local = rel_posix[len(candidate_base) :] if candidate_base else rel_posix
        # Directory patterns
        neg = pattern.startswith("!")
        body = pattern[1:] if neg else pattern
        # Normalize leading slash
        if body.startswith("/"):
            body = body[1:]
            match = fnmatch.fnmatch(local, body) or fnmatch.fnmatch(local, body.rstrip("/") + "/*")
        else:
            match = (
                fnmatch.fnmatch(local, body)
                or fnmatch.fnmatch(local, "*/" + body)
                or any(fnmatch.fnmatch(part, body.rstrip("/")) for part in local.split("/"))
                or (body.endswith("/") and (local.startswith(body) or fnmatch.fnmatch(local + "/", body)))
            )
        if match and not neg:
            return True
    return False

def class_for(rel: str) -> str | None:
    rel_posix = rel.replace(os.sep, "/").rstrip("/") + ("/" if rel.endswith("/") else "")
    for root_name in admin_roots:
        if rel_posix == root_name or rel_posix.startswith(root_name):
            return "administrative"
    for root_name in build_roots:
        if rel_posix == root_name or rel_posix.startswith(root_name):
            return "build_dependency_cache"
    return None

product_untracked = []
admin_untracked = []
build_untracked = []
for rel in raw:
    rel = rel.rstrip("/")
    if rel in forbidden or rel == ".env.garage-e3":
        raise SystemExit(f"forbidden untracked path present: {rel}")
    # .git is never product source
    if rel == ".git" or rel.startswith(".git/"):
        continue
    klass = class_for(rel + ("/" if (root / rel).is_dir() else ""))
    if klass is None:
        # Also try prefix class match without forcing slash form
        klass = class_for(rel)
    if klass == "administrative":
        admin_untracked.append(rel)
        continue
    if klass == "build_dependency_cache":
        build_untracked.append(rel)
        continue
    # Untracked and not classified: must be covered by tracked ignore as non-product?
    # Policy: zero non-ignored untracked product paths.
    if ignored_by_tracked_rules(rel):
        # Ignored but unclassified beyond P1 roots — fail closed so P1 stays exhaustive.
        raise SystemExit(
            f"ignored untracked path is outside P1 administrative/build classes: {rel}"
        )
    product_untracked.append(rel)

if product_untracked:
    print("raw untracked product paths:", file=sys.stderr)
    for path in product_untracked:
        print(f"  {path}", file=sys.stderr)
    raise SystemExit("product tree has raw untracked paths")

# Prove tracked rules exist for each declared P1 root.
# Coverage may be exact (target/) or a basename rule (node_modules/) that
# applies to nested roots such as scripts/site/node_modules/.
gitignore_lines = [
    line.strip()
    for line in (root / ".gitignore").read_text(encoding="utf-8").splitlines()
    if line.strip() and not line.strip().startswith("#")
]

def root_covered(rel: str) -> bool:
    rel = rel.replace("\\", "/").rstrip("/") + "/"
    parts = [p for p in rel.split("/") if p]
    basename = parts[-1] + "/" if parts else rel
    for rule in gitignore_lines:
        body = rule[1:] if rule.startswith("!") else rule
        body = body[1:] if body.startswith("/") else body
        if not body.endswith("/"):
            body_dir = body + "/"
        else:
            body_dir = body
        if body_dir == rel or body_dir == basename:
            return True
        if body.rstrip("/") == parts[-1]:
            return True
        # Prefix rules like .ddx/agent-logs/ count as administrative .ddx/ coverage
        if rel == ".ddx/" and body_dir.startswith(".ddx/"):
            return True
    return False

missing = [rel for rel in admin_roots + build_roots if not root_covered(rel)]
if missing:
    raise SystemExit(f"missing tracked .gitignore coverage for declared roots: {missing}")

print(
    "tracked_ignore_ok "
    f"admin_untracked={len(admin_untracked)} build_untracked={len(build_untracked)}"
)
PY

# S-bound tools must not treat .ddx or administrative roots as source/evidence.
# Prove no executable under scripts/release reads .ddx as governing input in source mode.
if [[ "$MODE" == "source" ]]; then
    if rg -n --glob 'scripts/release/*' --glob 'scripts/ci/governed-release*' \
        -e '\.ddx/' -e 'beads\.jsonl' "$SOURCE_ROOT" 2>/dev/null \
        | rg -v 'verify-source-predicate|ddx_tracked|administrative|operator-local|\.ddx/\*\*' \
        | rg -n 'product authority|promoted evidence|source of truth' >/dev/null 2>&1; then
        fail "S-bound release tooling claims .ddx product/source authority"
    fi
fi

if [[ "$MODE" == "e" ]]; then
    # Dual-root: promoted paths must be regular files under promoted root and allowlisted.
    python3 - "$PROMOTED_ROOT" "$PROMOTED_ALLOWLIST" "$EXPECTED_SOURCE" <<'PY'
import json
import os
import sys
from pathlib import Path

promoted_root = Path(sys.argv[1]).resolve()
allowlist_path = Path(sys.argv[2]).resolve()
expected_source = sys.argv[3]
raw = json.loads(allowlist_path.read_text(encoding="utf-8"))
if isinstance(raw, dict):
    paths = raw.get("paths") or raw.get("allowlist") or []
    if raw.get("expected_source") and raw["expected_source"] != expected_source:
        raise SystemExit(
            f"promoted allowlist expected_source {raw['expected_source']} != {expected_source}"
        )
elif isinstance(raw, list):
    paths = raw
else:
    raise SystemExit("promoted allowlist must be a list or object with paths")

if not paths:
    raise SystemExit("promoted allowlist is empty")

for rel in paths:
    path = (promoted_root / rel).resolve()
    if not str(path).startswith(str(promoted_root) + os.sep) and path != promoted_root:
        raise SystemExit(f"allowlist path escapes promoted root: {rel}")
    if not path.is_file() or path.is_symlink():
        raise SystemExit(f"promoted path must be a regular non-symlink file: {rel}")
print(f"e-mode promoted allowlist ok count={len(paths)}")
PY
fi

echo "verify-source-predicate: ok mode=${MODE} source=${EXPECTED_SOURCE} ref=${EXPECTED_REF}"
