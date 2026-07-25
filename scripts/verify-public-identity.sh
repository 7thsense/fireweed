#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir/.." rev-parse --show-toplevel)"
allowlist="$script_dir/public-identity-allowlist.json"
files_from=""
scan_root="$repo_root"

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
        -h|--help)
            echo "usage: verify-public-identity.sh [--allowlist FILE] [--root DIR] [--files-from FILE]" >&2
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

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

print(
    f"public identity residue verified: {checked_files} files scanned, "
    f"{approved_occurrences} approved historical/negative-test occurrence(s)"
)
PY
