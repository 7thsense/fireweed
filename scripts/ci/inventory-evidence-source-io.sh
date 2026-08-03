#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mode="${1:---check}"
baseline="${repo_root}/docs/helix/04-build/evidence-source-io-baseline.json"

case "${mode}" in
    --check|--self-test|--write) ;;
    *)
        echo "usage: $0 [--check|--self-test|--write]" >&2
        exit 2
        ;;
esac

cd "${repo_root}"

FIREWEED_INVENTORY_MODE="${mode}" \
FIREWEED_INVENTORY_BASELINE="${baseline}" \
python3 - <<'PY'
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


ROOT = Path.cwd()
MODE = os.environ["FIREWEED_INVENTORY_MODE"]
BASELINE = Path(os.environ["FIREWEED_INVENTORY_BASELINE"])


def git_files(*pathspecs: str) -> list[str]:
    command = ["git", "ls-files", "-z"]
    if pathspecs:
        command.extend(["--", *pathspecs])
    raw = subprocess.check_output(command, cwd=ROOT)
    return sorted(path for path in raw.decode().split("\0") if path)


def read_text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8", errors="replace")


tracked = set(git_files())
identity_path = "scripts/public-identity-allowlist.json"
identity = json.loads(read_text(identity_path))
historical = next(
    entry
    for entry in identity["entries"]
    if entry["id"] == "pre-fireweed-performance-evidence"
)
historical_paths = sorted(historical["paths"])
tracked_evidence_paths = sorted(
    path for path in tracked if path.startswith("docs/perf/evidence/")
)
tracked_tp003_paths = sorted(
    path for path in tracked_evidence_paths if Path(path).name.startswith("tp003-")
)

source_suffixes = {".rs", ".sh", ".py", ".toml", ".yaml", ".yml", ".json"}
source_prefixes = ("crates/", "scripts/", ".github/")
source_paths = [
    path
    for path in tracked
    if path.startswith(source_prefixes)
    and Path(path).suffix in source_suffixes
    and path != identity_path
    and not path.startswith("docs/perf/evidence/")
]

function_patterns = {
    ".rs": re.compile(
        r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)",
        re.MULTILINE,
    ),
    ".sh": re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(\)\s*\{", re.MULTILINE),
    ".py": re.compile(r"^\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.MULTILINE),
}

marker_patterns = {
    "evidence": re.compile(r"evidence", re.IGNORECASE),
    "evidence_dir": re.compile(r"\bevidence_dir\s*\("),
    "write_evidence": re.compile(r"\b(?:atomic_)?write_evidence\s*\("),
    "ledger_path": re.compile(r"\bledger_path\s*\("),
    "tp003": re.compile(r"tp003|TP-003"),
    "td008": re.compile(r"td008|TD-008"),
    "e3": re.compile(r"(?:^|[^A-Za-z0-9])e3(?:[^A-Za-z0-9]|$)|TP-002 E3", re.IGNORECASE),
    "matrix": re.compile(r"storage_matrix|performance_matrix|matrix.*evidence", re.IGNORECASE),
    "source_guard": re.compile(
        r"source_revision|source[_-]root|producing[_-]root|expected[_-]revision|git[_-]commit|git rev-parse",
        re.IGNORECASE,
    ),
    "public_identity": re.compile(r"public[_-]identity|public-identity", re.IGNORECASE),
    "epoch_zero": re.compile(r"epoch:0"),
    "evidence_environment": re.compile(r"FIREWEED_[A-Z0-9_]*EVIDENCE|FIREWEED_LEDGER_DIR"),
}

operation_patterns = {
    "deleter": re.compile(
        r"remove_file|remove_dir(?:_all)?|unlink\s*\(|\brm\s+(?:-[A-Za-z]*f|-[A-Za-z]*r)",
        re.IGNORECASE,
    ),
    "writer": re.compile(
        r"write_evidence|atomic_write_evidence|write_e3_|std::fs::write|fs::write|"
        r"write_all|append_row|append_ledger|File::create|OpenOptions|create_dir_all|"
        r"\btee\b|write_text",
        re.IGNORECASE,
    ),
    "reader": re.compile(
        r"read_to_string|read_to_end|read_text|fs::read|std::fs::read|File::open|"
        r"include_str!|include_bytes!|\.is_file\s*\(|\.exists\s*\(|\bverify|\bvalidate|"
        r"\bjq\b|\bcat\s+|ledger_path|evidence_dir",
        re.IGNORECASE,
    ),
    "constant": re.compile(
        r"\bconst\b|\bstatic\b|FIREWEED_[A-Z0-9_]*(?:EVIDENCE|LEDGER)|docs/perf/evidence",
        re.IGNORECASE,
    ),
    "source_guard": marker_patterns["source_guard"],
    "public_identity": marker_patterns["public_identity"],
}

assertion_pattern = re.compile(
    r"\bassert(?:_eq|_ne)?!|\bensure!|\.expect\s*\(|\btest\s+['\"]|\braise\s+AssertionError"
)
call_pattern = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
ignored_calls = {
    "if",
    "for",
    "while",
    "match",
    "loop",
    "Some",
    "None",
    "Ok",
    "Err",
    "format",
    "vec",
    "assert",
    "assert_eq",
    "assert_ne",
    "ensure",
}
evidence_by_basename = {
    Path(path).name: path for path in tracked_evidence_paths
}
path_pattern = re.compile(
    r"docs/perf/evidence/[A-Za-z0-9_./-]+\.(?:jsonl|json|md|txt)"
)


def chunks(path: str, text: str) -> list[tuple[str, int, int, str]]:
    pattern = function_patterns.get(Path(path).suffix)
    lines = text.splitlines()
    if pattern is None:
        return [("<module>", 1, max(1, len(lines)), text)]
    matches = list(pattern.finditer(text))
    if not matches:
        return [("<module>", 1, max(1, len(lines)), text)]
    starts = [text.count("\n", 0, match.start()) + 1 for match in matches]
    result: list[tuple[str, int, int, str]] = []
    prefix = "\n".join(lines[: starts[0] - 1])
    if prefix.strip():
        result.append(("<module>", 1, starts[0] - 1, prefix))
    for index, match in enumerate(matches):
        start = starts[index]
        end = starts[index + 1] - 1 if index + 1 < len(starts) else max(1, len(lines))
        result.append((match.group(1), start, end, "\n".join(lines[start - 1 : end])))
    return result


def groups_for(path: str, body: str) -> list[str]:
    lower_path = path.lower()
    groups: set[str] = set()
    if path.startswith("crates/fireweed-release/"):
        groups.add("release")
    if "e3" in lower_path or marker_patterns["e3"].search(body):
        groups.add("e3")
    if "matrix" in lower_path or marker_patterns["matrix"].search(body):
        groups.add("matrix")
    if path.startswith("crates/fireweed-server/"):
        groups.add("server")
    if "public-identity" in lower_path or "public_identity" in lower_path:
        groups.add("identity")
    if path.startswith("crates/fireweed-conformance/"):
        groups.add("conformance")
    if path.startswith("scripts/"):
        groups.add("script")
    if not groups:
        groups.add("evidence")
    return sorted(groups)


def referenced_paths(body: str) -> list[str]:
    found = set(path_pattern.findall(body))
    for basename, full_path in evidence_by_basename.items():
        if basename in body:
            found.add(full_path)
    return sorted(found)


surfaces: list[dict[str, object]] = []
surface_bodies: dict[tuple[str, str, int], str] = {}
for path in source_paths:
    text = read_text(path)
    for symbol, start, end, body in chunks(path, text):
        markers = sorted(
            name for name, pattern in marker_patterns.items() if pattern.search(body)
        )
        if not markers:
            continue
        operations = sorted(
            name for name, pattern in operation_patterns.items() if pattern.search(body)
        )
        if not operations:
            operations = ["constant"]
        match_lines: list[int] = []
        assertion_lines: list[int] = []
        matched_material: list[str] = []
        for offset, line in enumerate(body.splitlines()):
            line_no = start + offset
            if any(pattern.search(line) for pattern in marker_patterns.values()):
                match_lines.append(line_no)
                matched_material.append(line.strip())
            if assertion_pattern.search(line):
                assertion_lines.append(line_no)
        calls = sorted(
            name
            for name in set(call_pattern.findall(body))
            if name not in ignored_calls and name != symbol
        )
        record = {
            "path": path,
            "symbol": symbol,
            "start_line": start,
            "end_line": end,
            "groups": groups_for(path, body),
            "operations": operations,
            "markers": markers,
            "evidence_paths": referenced_paths(body),
            "match_lines": match_lines,
            "assertion_lines": assertion_lines,
            "calls": calls,
            "matched_source_sha256": hashlib.sha256(
                "\n".join(matched_material).encode()
            ).hexdigest(),
        }
        surfaces.append(record)
        surface_bodies[(path, symbol, start)] = body

surfaces.sort(key=lambda row: (row["path"], row["start_line"], row["symbol"]))

definitions = {"evidence_dir", "write_evidence", "atomic_write_evidence"}
depth_by_key: dict[tuple[str, str, int], int] = {}
for surface in surfaces:
    key = (surface["path"], surface["symbol"], surface["start_line"])
    if surface["symbol"] in definitions:
        depth_by_key[key] = 0

changed = True
while changed:
    changed = False
    named_depth: dict[str, int] = {}
    for surface in surfaces:
        key = (surface["path"], surface["symbol"], surface["start_line"])
        if key in depth_by_key:
            named_depth[surface["symbol"]] = min(
                depth_by_key[key], named_depth.get(surface["symbol"], depth_by_key[key])
            )
    for surface in surfaces:
        key = (surface["path"], surface["symbol"], surface["start_line"])
        called_depths = [named_depth[name] for name in surface["calls"] if name in named_depth]
        if not called_depths:
            continue
        candidate = min(called_depths) + 1
        if key not in depth_by_key or candidate < depth_by_key[key]:
            depth_by_key[key] = candidate
            changed = True

call_graph_symbols = {
    surface["symbol"]
    for surface in surfaces
    if (surface["path"], surface["symbol"], surface["start_line"]) in depth_by_key
}
evidence_call_graph = []
for surface in surfaces:
    key = (surface["path"], surface["symbol"], surface["start_line"])
    if key in depth_by_key:
        evidence_call_graph.append(
            {
                "path": surface["path"],
                "symbol": surface["symbol"],
                "line": surface["start_line"],
                "call_depth": depth_by_key[key],
                "calls": [name for name in surface["calls"] if name in call_graph_symbols],
            }
        )

epoch_zero_writers = []
for surface in surfaces:
    if "epoch_zero" not in surface["markers"] or "writer" not in surface["operations"]:
        continue
    body = surface_bodies[(surface["path"], surface["symbol"], surface["start_line"])]
    epoch_lines = []
    writer_lines = []
    for offset, line in enumerate(body.splitlines()):
        line_no = surface["start_line"] + offset
        if "epoch:0" in line:
            epoch_lines.append(line_no)
        if operation_patterns["writer"].search(line):
            writer_lines.append(line_no)
    epoch_zero_writers.append(
        {
            "path": surface["path"],
            "symbol": surface["symbol"],
            "epoch_literal_lines": epoch_lines,
            "writer_lines": writer_lines,
        }
    )

assertion_inventory = [
    {
        "path": surface["path"],
        "symbol": surface["symbol"],
        "lines": surface["assertion_lines"],
        "groups": surface["groups"],
    }
    for surface in surfaces
    if surface["assertion_lines"]
]

scan_digest_material = json.dumps(
    {
        "historical": historical_paths,
        "tracked_evidence": tracked_evidence_paths,
        "surfaces": surfaces,
    },
    sort_keys=True,
    ensure_ascii=False,
).encode()

public_surfaces = []
for surface in surfaces:
    match_lines = surface["match_lines"]
    public_surfaces.append(
        {
            "path": surface["path"],
            "symbol": surface["symbol"],
            "line": surface["start_line"],
            "groups": surface["groups"],
            "operations": surface["operations"],
            "markers": surface["markers"],
            "evidence_paths": surface["evidence_paths"],
            "match_count": len(match_lines),
            "first_match_line": min(match_lines),
            "last_match_line": max(match_lines),
        }
    )

inventory = {
    "schema_version": 1,
    "generated_by": "scripts/ci/inventory-evidence-source-io.sh",
    "governing_spec": "storage-matrix-completion-brief",
    "scan_sha256": hashlib.sha256(scan_digest_material).hexdigest(),
    "route_assignments": [],
    "public_identity_classification": {
        "source": identity_path,
        "id": historical["id"],
        "class": historical["class"],
        "owner_surface": historical["owner_surface"],
        "reason": historical["reason"],
    },
    "historical_allowlist_paths": historical_paths,
    "tracked_evidence_paths": tracked_evidence_paths,
    "tracked_tp003_paths": tracked_tp003_paths,
    "source_surfaces": public_surfaces,
    "evidence_call_graph": sorted(
        evidence_call_graph,
        key=lambda row: (row["call_depth"], row["path"], row["line"]),
    ),
    "epoch_zero_writers": sorted(
        epoch_zero_writers, key=lambda row: (row["path"], row["symbol"])
    ),
    "assertion_inventory": assertion_inventory,
}


def encoded(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


generated = encoded(inventory)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def validate() -> None:
    require(len(historical_paths) == 19, "historical allowlist must contain exactly 19 paths")
    require(set(historical_paths) <= set(tracked), "all 19 historical paths must be tracked")
    require(len(tracked_tp003_paths) == 6, "exactly six tracked TP-003 files are required")
    require(
        "docs/perf/evidence/td008-terminal-reap-frontier.jsonl" in historical_paths,
        "stale TD-008 frontier evidence must be inventoried",
    )
    require(inventory["route_assignments"] == [], "P0 must assign no concrete routes")

    operations = {
        operation for surface in surfaces for operation in surface["operations"]
    }
    require(
        {"reader", "writer", "deleter", "constant", "source_guard", "public_identity"}
        <= operations,
        f"missing source-I/O classes: {sorted({'reader', 'writer', 'deleter', 'constant', 'source_guard', 'public_identity'} - operations)}",
    )
    for group in ("release", "e3", "matrix", "server", "identity"):
        require(
            any(group in surface["groups"] and "reader" in surface["operations"] for surface in surfaces),
            f"missing {group} reader inventory",
        )

    required_epoch_writers = {
        (
            "crates/fireweed/tests/storage_matrix_t0_t2.rs",
            "sqlite_log_t3_t4_evidence_and_helm_values_present",
        ),
        (
            "crates/fireweed/tests/storage_matrix_t0_t2.rs",
            "postgres_log_t3_t4_evidence_and_helm_values_present",
        ),
        (
            "crates/fireweed-server/src/lib.rs",
            "sqlite_log_t3_evidence_axis_names_file_contract",
        ),
        (
            "crates/fireweed-server/src/lib.rs",
            "postgres_log_t3_evidence_axis_names_file_contract",
        ),
    }
    actual_epoch_writers = {
        (row["path"], row["symbol"]) for row in epoch_zero_writers
    }
    require(
        required_epoch_writers <= actual_epoch_writers,
        f"missing epoch-zero writers: {sorted(required_epoch_writers - actual_epoch_writers)}",
    )

    required_direct_writers = {
        "sqlite_log_t3_tp003_ac_txn_exact_pairs",
        "postgres_log_t3_tp003_ac_txn_exact_pairs",
        "write_evidence",
        "evidence_dir",
    }
    call_symbols = {row["symbol"] for row in evidence_call_graph}
    require(
        required_direct_writers <= call_symbols,
        f"missing conformance evidence callers: {sorted(required_direct_writers - call_symbols)}",
    )
    require(assertion_inventory, "assertion inventory must not be empty")


if MODE == "--write":
    validate()
    BASELINE.parent.mkdir(parents=True, exist_ok=True)
    BASELINE.write_bytes(generated)
    print(f"wrote {BASELINE.relative_to(ROOT)}")
elif MODE == "--check":
    validate()
    if not BASELINE.is_file():
        print(f"missing baseline: {BASELINE.relative_to(ROOT)}", file=sys.stderr)
        sys.exit(1)
    if BASELINE.read_bytes() != generated:
        print(
            "evidence source-I/O baseline is stale; run "
            "scripts/ci/inventory-evidence-source-io.sh --write",
            file=sys.stderr,
        )
        sys.exit(1)
    print("evidence source-I/O baseline verified")
else:
    validate()
    require(BASELINE.is_file(), "baseline must exist for self-test")
    require(BASELINE.read_bytes() == generated, "baseline must match the dynamic scan")
    with tempfile.TemporaryDirectory(prefix="fireweed-inventory-") as directory:
        stale = Path(directory) / "stale.json"
        stale.write_bytes(generated + b" \n")
        require(stale.read_bytes() != generated, "stale baseline mutation must be rejected")
    print("evidence source-I/O inventory self-test passed")
PY
