#!/usr/bin/env python3
"""Provider-neutral Snorri semantic verifier (P7N).

Maps TP-004 stable Snorri semantic IDs to Fireweed-owned proof commands and
optional evidence ledgers. Provider brand strings (Garage, MinIO, eldir, …) are
forbidden in fixtures and cell IDs — only authority-manifest cell IDs and
capability tokens are accepted.

Usage:
  # Validate a static evidence ledger (no cargo execution):
  python3 scripts/ci/snorri-semantic-verifier.py \\
      --ledger scripts/ci/fixtures/snorri/p7n-non-s3-lifecycle.json

  # Print the non-S3 lifecycle command matrix and exit 0 after structural checks:
  python3 scripts/ci/snorri-semantic-verifier.py --print-matrix --cells non-s3

  # Optionally execute mapped cargo filters (requires toolchain + fixtures):
  python3 scripts/ci/snorri-semantic-verifier.py --execute --cells non-s3-local

Exit codes:
  0  all required semantic IDs covered / commands exit 0
  1  missing coverage, forbidden brand, or command failure
  2  usage error
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]

# TP-004 stable IDs — governing identities, not implementation filenames.
SEMANTIC_IDS = (
    "SNORRI-MATRIX-LIFECYCLE",
    "SNORRI-REOPEN",
    "SNORRI-PROJECTION-REBUILD",
    "SNORRI-RETRY-ONCE",
)

# 12 non-S3 product cells of the public log × projection matrix excluding s3
# (memory|sqlite|postgres|filesystem × memory|sqlite|postgres).
NON_S3_CELLS = (
    "memory--memory",
    "memory--sqlite",
    "memory--postgres",
    "sqlite--memory",
    "sqlite--sqlite",
    "sqlite--postgres",
    "postgres--memory",
    "postgres--sqlite",
    "postgres--postgres",
    "filesystem--memory",
    "filesystem--sqlite",
    "filesystem--postgres",
)

# Local-deterministic subset (no live Postgres required).
NON_S3_LOCAL_CELLS = (
    "memory--memory",
    "memory--sqlite",
    "sqlite--memory",
    "sqlite--sqlite",
    "filesystem--memory",
    "filesystem--sqlite",
)

# Forbidden provider-brand tokens in fixtures / cell IDs (P1s / P4 neutrality).
FORBIDDEN_BRANDS = re.compile(
    r"(?i)\b(garage|minio|eldir|aws|gcs|azure|r2|cloudflare|digitalocean)\b"
)

# Map semantic ID → Fireweed-owned proof commands. Commands are provider-neutral
# cargo filters; live PG/S3 fixtures are gated by the environment, not brand names.
LIFECYCLE_COMMANDS: dict[str, list[list[str]]] = {
    "SNORRI-MATRIX-LIFECYCLE": [
        [
            "rustup",
            "run",
            "1.92.0",
            "cargo",
            "test",
            "-p",
            "fireweed",
            "--test",
            "public_interface_conformance",
            "--",
            "--nocapture",
        ],
        [
            "rustup",
            "run",
            "1.92.0",
            "cargo",
            "test",
            "-p",
            "fireweed",
            "--lib",
            "epoch::",
            "--",
            "--nocapture",
        ],
    ],
    "SNORRI-REOPEN": [
        [
            "rustup",
            "run",
            "1.92.0",
            "cargo",
            "test",
            "-p",
            "fireweed",
            "--test",
            "storage_matrix_t0_t2",
            "storage_matrix_t0_t2_all_twenty_cells",
            "--",
            "--nocapture",
        ],
    ],
    "SNORRI-PROJECTION-REBUILD": [
        [
            "rustup",
            "run",
            "1.92.0",
            "cargo",
            "test",
            "-p",
            "fireweed",
            "--test",
            "public_interface_conformance",
            "filesystem_sqlite",
            "--",
            "--nocapture",
        ],
    ],
    "SNORRI-RETRY-ONCE": [
        [
            "rustup",
            "run",
            "1.92.0",
            "cargo",
            "test",
            "-p",
            "fireweed",
            "--test",
            "request_id_idempotency",
            "--",
            "--nocapture",
        ],
    ],
}


def die(msg: str, code: int = 1) -> None:
    print(f"snorri-semantic-verifier: {msg}", file=sys.stderr)
    raise SystemExit(code)


def forbid_brands(blob: str, where: str) -> None:
    m = FORBIDDEN_BRANDS.search(blob)
    if m:
        die(f"forbidden provider brand {m.group(0)!r} in {where}")


def resolve_cells(name: str) -> tuple[str, ...]:
    if name == "non-s3":
        return NON_S3_CELLS
    if name == "non-s3-local":
        return NON_S3_LOCAL_CELLS
    if name == "all-listed":
        return NON_S3_CELLS
    die(f"unknown cell set {name!r} (expected non-s3|non-s3-local)", code=2)
    return ()  # unreachable


def validate_ledger(path: Path, required_ids: tuple[str, ...], cells: tuple[str, ...]) -> None:
    if not path.is_file():
        die(f"ledger not found: {path}")
    text = path.read_text()
    forbid_brands(text, f"ledger {path}")
    data = json.loads(text)
    if not isinstance(data, dict):
        die("ledger must be a JSON object")

    entries = data.get("entries")
    if not isinstance(entries, list):
        die("ledger.entries must be a list")

    by_id: dict[str, list[dict[str, Any]]] = {sid: [] for sid in required_ids}
    for entry in entries:
        if not isinstance(entry, dict):
            die("ledger entry must be an object")
        sid = entry.get("semantic_id")
        if sid not in by_id:
            # Extra IDs are allowed only when they are known SEMANTIC_IDS.
            if sid not in SEMANTIC_IDS:
                die(f"unknown semantic_id {sid!r}")
            continue
        cell = entry.get("cell_id", "")
        forbid_brands(str(cell), f"entry cell_id for {sid}")
        # Aggregate rows (reopen / retry / rebuild) may use cell_id "matrix".
        if cell and cell != "matrix":
            if cell not in cells and not any(cell.startswith(c) for c in cells):
                # Allow variant suffixes (e.g. filesystem--sqlite--strict).
                base = "--".join(cell.split("--")[:2])
                if base not in cells:
                    die(f"cell_id {cell!r} not in required cell set for {sid}")
        status = entry.get("status")
        if status != "passed":
            die(f"{sid} cell {cell!r}: status must be 'passed', got {status!r}")
        cmd = entry.get("command")
        if not cmd:
            die(f"{sid} cell {cell!r}: missing command")
        forbid_brands(str(cmd), f"command for {sid}/{cell}")
        by_id[sid].append(entry)

    for sid in required_ids:
        if not by_id[sid]:
            die(f"missing ledger coverage for {sid}")
        # Lifecycle must cover every required cell at least once (variant OK).
        if sid == "SNORRI-MATRIX-LIFECYCLE":
            covered = set()
            for entry in by_id[sid]:
                cell = str(entry.get("cell_id", ""))
                base = "--".join(cell.split("--")[:2]) if cell else ""
                covered.add(base)
            missing = [c for c in cells if c not in covered]
            if missing:
                die(f"SNORRI-MATRIX-LIFECYCLE missing cells: {', '.join(missing)}")

    print(
        f"snorri-semantic-verifier: ledger OK "
        f"({path}; ids={','.join(required_ids)}; cells={len(cells)})"
    )


def print_matrix(cells: tuple[str, ...], ids: tuple[str, ...]) -> None:
    print("semantic_id\tcell_count\tcommand_count")
    for sid in ids:
        cmds = LIFECYCLE_COMMANDS.get(sid, [])
        print(f"{sid}\t{len(cells)}\t{len(cmds)}")
    print("cells:")
    for c in cells:
        print(f"  {c}")


def execute_commands(ids: tuple[str, ...]) -> None:
    for sid in ids:
        cmds = LIFECYCLE_COMMANDS.get(sid)
        if not cmds:
            die(f"no commands registered for {sid}")
        for cmd in cmds:
            print(f"+ {' '.join(cmd)}", flush=True)
            proc = subprocess.run(cmd, cwd=REPO_ROOT, check=False)
            if proc.returncode != 0:
                die(f"{sid}: command failed with exit {proc.returncode}: {' '.join(cmd)}")
        print(f"snorri-semantic-verifier: {sid} commands ok")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ledger",
        type=Path,
        help="Evidence ledger JSON to validate (no cargo execution)",
    )
    parser.add_argument(
        "--cells",
        default="non-s3",
        choices=("non-s3", "non-s3-local", "all-listed"),
        help="Cell set to require for SNORRI-MATRIX-LIFECYCLE (default non-s3 = 12 cells)",
    )
    parser.add_argument(
        "--ids",
        default="SNORRI-MATRIX-LIFECYCLE",
        help="Comma-separated semantic IDs (default: SNORRI-MATRIX-LIFECYCLE)",
    )
    parser.add_argument(
        "--print-matrix",
        action="store_true",
        help="Print the provider-neutral command matrix and exit",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute mapped cargo filters (requires toolchain)",
    )
    args = parser.parse_args(argv)

    ids = tuple(s.strip() for s in args.ids.split(",") if s.strip())
    for sid in ids:
        if sid not in SEMANTIC_IDS:
            die(f"unknown semantic id {sid!r}", code=2)

    cells = resolve_cells(args.cells)

    if args.print_matrix:
        print_matrix(cells, ids)
        return 0

    if args.ledger is None and not args.execute:
        die("require --ledger, --print-matrix, and/or --execute", code=2)

    if args.ledger is not None:
        validate_ledger(args.ledger, ids, cells)

    if args.execute:
        execute_commands(ids)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
