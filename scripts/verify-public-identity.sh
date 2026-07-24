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
rust_crate_suffixes = (
    "bench|conformance|core|engine|loadgen|memory|objectlog|postgres|"
    "projection|relational|release|resp|server|sim_support|sqlite|turso"
)
old_rust_identifier = re.compile(rf"\bpqueue_(?:{rust_crate_suffixes})\b|\bpqueue::")
old_cargo_coordinate = re.compile(
    r"\bpqueue(?:-(?:bench|conformance|core|engine|loadgen|memory|objectlog|"
    r"postgres|projection|relational|release|resp|server|sim-support|sqlite|turso))?\b"
)
old_rust_binary = re.compile(
    r"\bpqueue-(?:service|postgres-migrate|build-e3-contract|"
    r"build-evidence-attestation|cost-model|verify-density-evidence|"
    r"verify-e0-e1-evidence|verify-e2-failover|verify-e2-scale-evidence|"
    r"verify-e3-contract|verify-evidence-attestation|verify-ledger|"
    r"verify-transaction-evidence)\b"
)
compatibility_binary_alias = re.compile(
    r'^name\s*=\s*"pqueue-(?:bench|service|loadgen|postgres-migrate|build-e3-contract|'
    r'build-evidence-attestation|cost-model|verify-density-evidence|verify-e0-e1-evidence|'
    r'verify-e2-failover|verify-e2-scale-evidence|verify-e3-contract|'
    r'verify-evidence-attestation|verify-ledger|verify-transaction-evidence)"$'
)
runtime_env_files = re.compile(
    r"^(?:crates/fireweed-server/src/(?:bin/fireweed-service|lib)\.rs|"
    r"crates/fireweed-postgres/src/bin/fireweed-postgres-migrate\.rs|"
    r"crates/fireweed-release/src/lib\.rs|crates/fireweed-bench/src/main\.rs)$"
)
legacy_process_env_read = re.compile(r'env::var(?:_os)?\("PQUEUE_([A-Z0-9_]+)"\)')
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
rust_namespace_violations = []
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
    cargo_section = ""
    if re.search(r"^(?:crates/pqueue(?:-[^/]+)?|tools/turso-compat-probe)(?:/|$)", rel):
        rust_namespace_violations.append((rel, 0, "old Cargo crate path", rel))
    for line_no, line in enumerate(lines, start=1):
        stripped = line.strip()
        if (rel == "Cargo.toml" or rel.endswith("/Cargo.toml")) and stripped.startswith("["):
            cargo_section = stripped
        if (rel == "Cargo.toml" or rel == "Cargo.lock" or rel.endswith("/Cargo.toml") or rel.endswith("/Cargo.lock")):
            found = old_cargo_coordinate.search(line)
            governed_binary_alias = (
                rel.endswith("Cargo.toml")
                and cargo_section == "[[bin]]"
                and compatibility_binary_alias.fullmatch(stripped)
            )
            if found and not governed_binary_alias:
                rust_namespace_violations.append(
                    (rel, line_no, "old Cargo package/dependency coordinate", found.group(0))
                )
        if runtime_env_files.search(rel):
            for legacy_read in legacy_process_env_read.finditer(line):
                primary = f'FIREWEED_{legacy_read.group(1)}'
                if primary not in "\n".join(lines):
                    rust_namespace_violations.append(
                        (rel, line_no, "legacy runtime env read without Fireweed primary", legacy_read.group(0))
                    )
        if rel.endswith(".rs"):
            found = old_rust_identifier.search(line)
            if found:
                rust_namespace_violations.append(
                    (rel, line_no, "old Rust crate identifier", found.group(0))
                )
            found = old_rust_binary.search(line)
            governed_runtime_alias_test = (
                rel == "crates/fireweed-server/tests/env_config.rs"
                and found
                and found.group(0) == "pqueue-service"
            )
            if found and not governed_runtime_alias_test:
                rust_namespace_violations.append(
                    (rel, line_no, "old Rust-owned binary coordinate", found.group(0))
                )
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

if rust_namespace_violations:
    print("unapproved Cargo or Rust namespace residue found:", file=sys.stderr)
    for rel, line_no, identity_class, token in rust_namespace_violations[:200]:
        print(f"{rel}:{line_no}: {identity_class}: {token}", file=sys.stderr)
    remaining = len(rust_namespace_violations) - 200
    if remaining > 0:
        print(f"... {remaining} additional violation(s) omitted", file=sys.stderr)
    sys.exit(1)

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
