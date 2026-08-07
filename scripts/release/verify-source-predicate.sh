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
# IMPORTANT: git status / ls-files still honor .git/info/exclude and global
# excludesFile even with -c core.excludesFile=/dev/null. Enumerate untracked
# paths by filesystem walk against `git ls-files` so local/global excludes
# cannot mask product residue (P0/P17a policy).
python3 - "$SOURCE_ROOT" "$AUTHORITY_MANIFEST" <<'PY'
import fnmatch
import json
import os
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
manifest = json.load(open(sys.argv[2], encoding="utf-8"))

policy = manifest["tracked_ignore_policy"]
assert policy["authority"] == "tracked_gitignore_only"
assert policy["local_or_global_excludes_have_policy_authority"] is False

admin_roots = list(policy["classes"]["administrative"]["roots"])
build_roots = list(policy["classes"]["build_dependency_cache"]["roots"])
forbidden = set(policy["forbidden_in_repository_paths"])

tracked = set(
    subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "-z"],
        text=True,
    ).split("\0")
)
tracked.discard("")

# Filesystem walk: every non-.git path not in the index is untracked for policy,
# regardless of info/exclude or global excludes.
raw: list[str] = []
for dirpath, dirnames, filenames in os.walk(root):
    rel_dir = Path(dirpath).relative_to(root)
    # Never descend into .git
    dirnames[:] = [d for d in dirnames if d != ".git" and not str(rel_dir / d).startswith(".git/")]
    for name in filenames:
        full = Path(dirpath) / name
        if full.is_symlink() and not full.exists():
            # dangling symlink still counts as untracked product residue
            rel = full.relative_to(root).as_posix()
        else:
            rel = full.relative_to(root).as_posix()
        if rel in tracked:
            continue
        if rel == ".git" or rel.startswith(".git/"):
            continue
        raw.append(rel)
raw.sort()

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
        # Path is ignored by tracked rules. If it falls under a P1 class root by
        # prefix (e.g. target/foo), reclass; else fail closed so P1 stays exhaustive.
        reclass = class_for(rel)
        if reclass == "administrative":
            admin_untracked.append(rel)
            continue
        if reclass == "build_dependency_cache":
            build_untracked.append(rel)
            continue
        # Basename-style ignore (node_modules/, __pycache__/) under nested roots:
        # map to build_dependency_cache when any path component matches a build root basename.
        parts = Path(rel).parts
        for br in build_roots:
            base = br.rstrip("/").split("/")[-1]
            if base in parts:
                build_untracked.append(rel)
                break
        else:
            for ar in admin_roots:
                base = ar.rstrip("/").split("/")[-1]
                if base in parts:
                    admin_untracked.append(rel)
                    break
            else:
                raise SystemExit(
                    f"ignored untracked path is outside P1 administrative/build classes: {rel}"
                )
        continue
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
print("local_global_exclude_masking_has_no_authority=true")
PY

# S-bound tools must not treat .ddx or administrative roots as source/evidence.
# Inventory administrative roots with no-reader proof and build/cache roots with
# non-authority disposition. Local/global excludes have zero policy authority:
# product untracked paths remain product even when listed only in info/exclude.
if [[ "$MODE" == "source" ]]; then
    python3 - "$SOURCE_ROOT" "$AUTHORITY_MANIFEST" <<'PY'
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
manifest = json.load(open(sys.argv[2], encoding="utf-8"))
policy = manifest["tracked_ignore_policy"]
admin_roots = list(policy["classes"]["administrative"]["roots"])
build_roots = list(policy["classes"]["build_dependency_cache"]["roots"])

# Collect S-bound release/CI reader surfaces that must never treat .ddx as product.
scan_globs = [
    root / "scripts" / "release",
    root / "scripts" / "ci",
]
forbidden_claim = re.compile(
    r"(product\s+authority|promoted\s+evidence|source\s+of\s+truth|governing\s+input)",
    re.I,
)
ddx_token = re.compile(r"(?:\.ddx/|beads\.jsonl)")
allow_comment = re.compile(
    r"(verify-source-predicate|ddx_tracked|administrative|operator-local|"
    r"operator metadata|not\s+product|\.ddx/\*\*|campaign tracking only)",
    re.I,
)
violations = []
for base in scan_globs:
    if not base.is_dir():
        continue
    for path in base.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in {".sh", ".py", ".rs", ".json", ".toml", ".md"}:
            continue
        rel = path.relative_to(root).as_posix()
        # Predicate and inventory tooling may mention .ddx for exclusion proofs.
        if "verify-source-predicate" in rel or "inventory" in rel or "storage-remediation" in rel:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line_no, line in enumerate(text.splitlines(), start=1):
            if not ddx_token.search(line):
                continue
            if allow_comment.search(line):
                continue
            if forbidden_claim.search(line):
                violations.append(f"{rel}:{line_no}:{line.strip()[:160]}")
if violations:
    print("S-bound tooling claims .ddx product/source authority:", file=sys.stderr)
    for row in violations[:40]:
        print(f"  {row}", file=sys.stderr)
    raise SystemExit("no-reader proof failed for administrative .ddx roots")

# Emit admin/build root inventory with required proof labels (P1 classes).
admin_proofs = policy["classes"]["administrative"]["required_proofs"]
build_proofs = policy["classes"]["build_dependency_cache"]["required_proofs"]
print(
    "admin_roots_ok roots="
    + ",".join(admin_roots)
    + " proofs="
    + ",".join(admin_proofs)
)
print(
    "build_cache_roots_ok roots="
    + ",".join(build_roots)
    + " proofs="
    + ",".join(build_proofs)
)
print("no_s_bound_reader_ok administrative=.ddx/")
PY
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
