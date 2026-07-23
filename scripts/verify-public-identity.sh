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
from pathlib import Path

root = Path(sys.argv[1]).resolve()
allowlist_path = Path(sys.argv[2]).resolve()
files_from = sys.argv[3]

required_entry_fields = {
    "id",
    "class",
    "reason",
    "owner_surface",
    "removal_condition",
    "match_pattern",
}
allowed_classes = {"historical/audit", "persisted/wire", "temporary compatibility"}
identity_patterns = {
    "lowercase pqueue": re.compile(r"\bpqueue(?:[-_][A-Za-z0-9]+)*\b"),
    "uppercase PQUEUE": re.compile(r"\bPQUEUE(?:_[A-Z0-9]+)*\b"),
    "Queueyard": re.compile(r"\bQueueyard\b"),
    "old repository coordinate": re.compile(
        r"(?:https://)?github\.com/telepathdata/7thsense-pqueue(?:\.git)?|7thsense-pqueue"
    ),
}
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
    if data.get("schema") != "fireweed-public-identity-allowlist-v1":
        raise SystemExit("allowlist schema must be fireweed-public-identity-allowlist-v1")
    if data.get("adr") != "ADR-020":
        raise SystemExit("allowlist must be tied to ADR-020")
    if data.get("source_inventory") != "docs/helix/02-design/public-namespace-migration.yaml":
        raise SystemExit("allowlist must cite docs/helix/02-design/public-namespace-migration.yaml")
    entries = data.get("entries")
    if not isinstance(entries, list) or not entries:
        raise SystemExit("allowlist entries must be a non-empty list")

    compiled = []
    ids = set()
    for index, entry in enumerate(entries, start=1):
        missing = sorted(required_entry_fields - set(entry))
        if missing:
            raise SystemExit(f"allowlist entry {index} missing fields: {', '.join(missing)}")
        if entry["class"] not in allowed_classes:
            raise SystemExit(f"allowlist entry {entry['id']} has unsupported class: {entry['class']}")
        if entry["id"] in ids:
            raise SystemExit(f"duplicate allowlist entry id: {entry['id']}")
        ids.add(entry["id"])
        if not (entry.get("path") or entry.get("path_pattern")):
            raise SystemExit(f"allowlist entry {entry['id']} must define path or path_pattern")
        for field in ("reason", "owner_surface", "removal_condition"):
            if not isinstance(entry[field], str) or not entry[field].strip():
                raise SystemExit(f"allowlist entry {entry['id']} field {field} must be non-empty")
        try:
            path_re = re.compile(rf"^(?:{re.escape(entry['path'])})$") if entry.get("path") else re.compile(entry["path_pattern"])
            match_re = re.compile(entry["match_pattern"])
        except re.error as exc:
            raise SystemExit(f"allowlist entry {entry['id']} has invalid regex: {exc}")
        compiled.append((entry, path_re, match_re))
    return compiled


def tracked_files():
    if files_from:
        return [line.strip() for line in Path(files_from).read_text(encoding="utf-8").splitlines() if line.strip()]
    out = subprocess.check_output(["git", "-C", str(root), "ls-files"], text=True)
    return [line for line in out.splitlines() if line and not line.startswith(".ddx/")]


def is_probably_text(path: Path) -> bool:
    if path.name in {"Dockerfile", "Dockerfile.e2", "Dockerfile.prebuilt", "Cargo.lock"}:
        return True
    return path.suffix in text_suffixes


allowlist = load_allowlist(allowlist_path)
violations = []
checked_files = 0
matches = 0

for rel in tracked_files():
    if rel == os.path.relpath(allowlist_path, root):
        continue
    path = (root / rel).resolve()
    if not path.is_file() or not is_probably_text(path):
        continue
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        continue
    checked_files += 1
    for line_no, line in enumerate(lines, start=1):
        for identity_class, pattern in identity_patterns.items():
            for found in pattern.finditer(line):
                token = found.group(0)
                matches += 1
                approved_by = None
                for entry, path_re, match_re in allowlist:
                    if path_re.search(rel) and match_re.search(token):
                        approved_by = entry["id"]
                        break
                if approved_by is None:
                    violations.append((rel, line_no, identity_class, token, line.strip()))

if violations:
    print("unapproved public identity residue found:", file=sys.stderr)
    for rel, line_no, identity_class, token, line in violations[:200]:
        print(f"{rel}:{line_no}: {identity_class}: {token}: {line}", file=sys.stderr)
    remaining = len(violations) - 200
    if remaining > 0:
        print(f"... {remaining} additional violation(s) omitted", file=sys.stderr)
    sys.exit(1)

print(f"public identity residue verified: {checked_files} files scanned, {matches} approved occurrence(s)")
PY
