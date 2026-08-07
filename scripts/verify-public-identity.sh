#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir/.." rev-parse --show-toplevel)"
allowlist="$script_dir/public-identity-allowlist.json"
files_from=""
scan_root="$repo_root"
mode="source"
source_root=""
promoted_root=""
expected_source=""
expected_remote=""
expected_ref=""
promoted_allowlist=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --allowlist)
            allowlist="$2"
            shift 2
            ;;
        --files-from)
            files_from="$2"
            shift 2
            ;;
        --root)
            scan_root="$2"
            shift 2
            ;;
        --mode)
            mode="$2"
            shift 2
            ;;
        --source-root)
            source_root="$2"
            shift 2
            ;;
        --promoted-root)
            promoted_root="$2"
            shift 2
            ;;
        --expected-source)
            expected_source="$2"
            shift 2
            ;;
        --expected-remote)
            expected_remote="$2"
            shift 2
            ;;
        --expected-ref)
            expected_ref="$2"
            shift 2
            ;;
        --promoted-allowlist)
            promoted_allowlist="$2"
            shift 2
            ;;
        -h|--help)
            echo "usage: verify-public-identity.sh [--mode source|e] [--allowlist FILE] [--root DIR] [--files-from FILE] [--source-root DIR --expected-source SHA --expected-remote R --expected-ref REF] [--promoted-root DIR --promoted-allowlist FILE]" >&2
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

# Dual-root E mode: tooling/predicate from dedicated S checkout; scan only the
# promoted evidence root under an explicit allowlist.
if [[ "$mode" == "e" ]]; then
    [[ -n "$source_root" && -n "$expected_source" && -n "$expected_remote" && -n "$expected_ref" ]] || {
        echo "e mode requires --source-root --expected-source --expected-remote --expected-ref" >&2
        exit 2
    }
    [[ -n "$promoted_root" && -n "$promoted_allowlist" ]] || {
        echo "e mode requires --promoted-root --promoted-allowlist" >&2
        exit 2
    }
    bash "$script_dir/release/verify-source-predicate.sh" \
        --mode e \
        --source-root "$source_root" \
        --expected-source "$expected_source" \
        --expected-remote "$expected_remote" \
        --expected-ref "$expected_ref" \
        --promoted-root "$promoted_root" \
        --promoted-allowlist "$promoted_allowlist"
    scan_root="$promoted_root"
elif [[ -n "$expected_source" ]]; then
    # Optional source-mode binding when callers pass measured S.
    [[ -n "$source_root" && -n "$expected_remote" && -n "$expected_ref" ]] || {
        echo "source binding requires --source-root --expected-remote --expected-ref" >&2
        exit 2
    }
    bash "$script_dir/release/verify-source-predicate.sh" \
        --mode source \
        --source-root "$source_root" \
        --expected-source "$expected_source" \
        --expected-remote "$expected_remote" \
        --expected-ref "$expected_ref"
    scan_root="${scan_root:-$source_root}"
fi

python3 - "$scan_root" "$allowlist" "$files_from" <<'PY'
import json
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

root = Path(sys.argv[1]).resolve()
allowlist_path = Path(sys.argv[2]).resolve()
files_from = sys.argv[3]

required_entry_fields = {
    "id",
    "class",
    "reason",
    "owner_surface",
    "removal_condition",
    "paths",
    "match_pattern",
}
allowed_classes = {"historical/audit", "negative-test"}
immutable_bead_id = re.compile(r"\bpqueue-[0-9a-f]{8}\b")
identity_patterns = (
    (
        "retired CamelCase identifier",
        re.compile(r"\b[A-Za-z0-9_]*Pqueue[A-Za-z0-9_]*\b"),
    ),
    (
        "retired uppercase identifier",
        re.compile(r"\bPQUEUE(?:_[A-Z0-9]+)*\b"),
    ),
    (
        "retired lowercase identifier",
        re.compile(r"\b[A-Za-z0-9_]*pqueue[A-Za-z0-9_-]*\b"),
    ),
    (
        "retired Queueyard identity",
        re.compile(r"(?i)\bqueueyard(?:[-_][A-Za-z0-9]+)*\b"),
    ),
    (
        "retired RESP command",
        re.compile(r"\bPQ(?:\*|\.[A-Za-z][A-Za-z0-9_.-]*)|[\"'`]pq\.[A-Za-z][A-Za-z0-9_.-]*"),
    ),
    (
        "retired short identifier",
        re.compile(r"(?<![A-Za-z0-9])pq(?:[-_][A-Za-z0-9_-]+)?\b|\bPQ(?!UEUE(?:\b|_))[A-Z*][A-Z0-9_*.-]*\b"),
    ),
)
text_suffixes = {
    ".c",
    ".css",
    ".dockerignore",
    ".h",
    ".html",
    ".json",
    ".jsonl",
    ".lock",
    ".md",
    ".py",
    ".rs",
    ".sh",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}


def load_allowlist(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(f"allowlist not found: {path}")
    if data.get("schema") != "fireweed-public-identity-allowlist-v2":
        raise SystemExit("allowlist schema must be fireweed-public-identity-allowlist-v2")
    if data.get("adr") != "ADR-023":
        raise SystemExit("allowlist must be tied to ADR-023")

    entries = data.get("entries")
    if not isinstance(entries, list) or not entries:
        raise SystemExit("allowlist entries must be a non-empty list")

    compiled = []
    ids = set()
    governed_paths = set()
    for index, entry in enumerate(entries, start=1):
        missing = sorted(required_entry_fields - set(entry))
        if missing:
            raise SystemExit(f"allowlist entry {index} missing fields: {', '.join(missing)}")
        unexpected = sorted(set(entry) - required_entry_fields)
        if unexpected:
            raise SystemExit(
                f"allowlist entry {entry.get('id', index)} has unsupported fields: "
                f"{', '.join(unexpected)}"
            )
        if entry["class"] not in allowed_classes:
            raise SystemExit(
                f"allowlist entry {entry['id']} has unsupported class: {entry['class']}"
            )
        if entry["id"] in ids:
            raise SystemExit(f"duplicate allowlist entry id: {entry['id']}")
        ids.add(entry["id"])
        for field in ("reason", "owner_surface", "removal_condition"):
            if not isinstance(entry[field], str) or not entry[field].strip():
                raise SystemExit(
                    f"allowlist entry {entry['id']} field {field} must be non-empty"
                )

        paths = entry["paths"]
        if not isinstance(paths, list) or not paths:
            raise SystemExit(f"allowlist entry {entry['id']} paths must be a non-empty list")
        normalized_paths = []
        for rel in paths:
            if not isinstance(rel, str) or not rel or rel != PurePosixPath(rel).as_posix():
                raise SystemExit(f"allowlist entry {entry['id']} has invalid path: {rel!r}")
            parts = PurePosixPath(rel).parts
            if rel.startswith("/") or ".." in parts or any(ch in rel for ch in "*?[]"):
                raise SystemExit(
                    f"allowlist entry {entry['id']} paths must be exact repository-relative paths: {rel}"
                )
            if rel in governed_paths:
                raise SystemExit(f"allowlist path appears in multiple entries: {rel}")
            governed_paths.add(rel)
            normalized_paths.append(rel)
        try:
            match_re = re.compile(entry["match_pattern"])
        except re.error as exc:
            raise SystemExit(f"allowlist entry {entry['id']} has invalid regex: {exc}")
        compiled.append((entry, frozenset(normalized_paths), match_re))
    return compiled


def tracked_files():
    if files_from:
        return [
            line.strip()
            for line in Path(files_from).read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    out = subprocess.check_output(["git", "-C", str(root), "ls-files", "--cached"], text=True)
    return [
        line
        for line in out.splitlines()
        if line
        and not line.startswith(".ddx/")
        and (root / line).exists()
    ]


def is_probably_text(path: Path) -> bool:
    if path.name in {"Dockerfile", "Dockerfile.e2", "Dockerfile.prebuilt", "Cargo.lock", ".gitignore"}:
        return True
    return path.suffix in text_suffixes


def approved(rel: str, token: str, allowlist) -> str | None:
    for entry, paths, match_re in allowlist:
        if rel in paths and match_re.fullmatch(token):
            return entry["id"]
    return None


def identity_matches(value: str):
    for identity_class, pattern in identity_patterns:
        for found in pattern.finditer(value):
            token = found.group(0)
            if identity_class == "retired lowercase identifier" and immutable_bead_id.fullmatch(token):
                continue
            yield identity_class, token


allowlist = load_allowlist(allowlist_path)
allowlist_rel = os.path.relpath(allowlist_path, root)
violations = []
checked_files = 0
approved_occurrences = 0

for rel in tracked_files():
    if rel == allowlist_rel:
        continue

    for identity_class, token in identity_matches(rel):
        if approved(rel, token, allowlist):
            approved_occurrences += 1
        else:
            violations.append((rel, 0, f"{identity_class} in path", token, rel))

    path = (root / rel).resolve()
    if not path.is_file() or not is_probably_text(path):
        continue
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        continue
    checked_files += 1
    for line_no, line in enumerate(lines, start=1):
        for identity_class, token in identity_matches(line):
            if approved(rel, token, allowlist):
                approved_occurrences += 1
            else:
                violations.append((rel, line_no, identity_class, token, line.strip()))

if violations:
    print("unapproved retired public identity residue found:", file=sys.stderr)
    for rel, line_no, identity_class, token, line in violations[:200]:
        print(f"{rel}:{line_no}: {identity_class}: {token}: {line}", file=sys.stderr)
    remaining = len(violations) - 200
    if remaining > 0:
        print(f"... {remaining} additional violation(s) omitted", file=sys.stderr)
    sys.exit(1)

# P17a structural scan: exact historical release-note `.ddx` hyperlink bytes are
# inert non-governing provenance. Classify them; any new markdown hyperlink to
# `.ddx/**` outside the frozen historical allowlist fails. Prose that names
# `.ddx/` as administrative/non-product is permitted.
inert_ddx_hyperlink_files = {
    "docs/releases/v0.14.0.md",
}
# Exact hyperlink targets frozen from the immutable v0.14.0 note.
allowed_ddx_link_targets = {
    ".ddx/executions/20260715T043214-936c36b0/release-evidence-correction.md",
    ".ddx/executions/20260714T184540-9153544a/review-gate.md",
    ".ddx/executions/20260715T043214-936c36b0/pr-gate-enforcing.log",
}
md_link = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
ddx_href = re.compile(r"(?:^|/|\.\./)*(?:\.ddx/[^)\s#]+)")
structural = []
classified = 0
for rel in tracked_files():
    if not rel.endswith(".md"):
        continue
    path = root / rel
    if not path.is_file():
        continue
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        continue
    for line_no, line in enumerate(text.splitlines(), start=1):
        for match in md_link.finditer(line):
            href = match.group(1).strip()
            # Ignore external URLs.
            if "://" in href:
                continue
            # Normalize relative markdown links that point into .ddx/
            if ".ddx/" not in href and not href.startswith(".ddx/"):
                continue
            # Strip anchors/query
            href_path = href.split("#", 1)[0].split("?", 1)[0]
            # Collapse ../ segments only for classification of the .ddx tail.
            parts = []
            for part in href_path.split("/"):
                if part in ("", "."):
                    continue
                if part == "..":
                    if parts:
                        parts.pop()
                    continue
                parts.append(part)
            # Find .ddx/... suffix
            try:
                idx = parts.index(".ddx")
            except ValueError:
                continue
            target = "/".join(parts[idx:])
            if rel in inert_ddx_hyperlink_files and target in allowed_ddx_link_targets:
                classified += 1
                continue
            structural.append(
                f"{rel}:{line_no}: new or non-inert .ddx hyperlink: {href} -> {target}"
            )

if structural:
    print("unapproved .ddx hyperlink structural scan failures:", file=sys.stderr)
    for row in structural[:100]:
        print(row, file=sys.stderr)
    sys.exit(1)

print(
    f"public identity residue verified: {checked_files} files scanned, "
    f"{approved_occurrences} approved historical/negative-test occurrence(s); "
    f"inert .ddx hyperlinks classified={classified}"
)
PY
