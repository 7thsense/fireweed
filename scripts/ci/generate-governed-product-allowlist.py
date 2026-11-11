#!/usr/bin/env python3
"""P13: generate the sole final governed-product allowlist from manifest sources.

Owns population of scripts/ci/governed-product-allowlist.json from the Queue test
targets (fireweed lib, simple_queue_postgres, fireweed-postgres queue_* targets) and the
queue module boundary check. Everything here is functional, never authoritative performance.

Does not claim product-release readiness (P13b) and never authors performance
or authoritative timing commands (forbidden_in_lane).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ALLOWLIST = ROOT / "scripts/ci/governed-product-allowlist.json"

SCHEMA_VERSION = 1
PLAN_KEY = "P13"
CARGO = ["rustup", "run", "1.99.0", "cargo"]

FORBIDDEN_IN_LANE = [
    "scripts/perf/",
    "fireweed-bench",
    "cargo bench",
    "authoritative-performance",
    "performance_",
]

class AllowlistError(Exception):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AllowlistError(message)


def command_text(command: list[str]) -> str:
    return " ".join(command)


def assert_not_forbidden(command: list[str], entry_id: str) -> None:
    joined = command_text(command)
    for token in FORBIDDEN_IN_LANE:
        require(
            token not in joined,
            f"allowlist entry {entry_id} hits forbidden_in_lane token {token!r}: {joined}",
        )


def entry(
    *,
    entry_id: str,
    category: str,
    source: str,
    command: list[str],
    notes: str = "",
    requires_fixtures: list[str] | None = None,
) -> dict:
    require(isinstance(command, list) and all(isinstance(p, str) for p in command), entry_id)
    require(len(command) > 0, f"{entry_id} empty command")
    assert_not_forbidden(command, entry_id)
    row = {
        "id": entry_id,
        "category": category,
        "source": source,
        "command": command,
        "notes": notes,
    }
    if requires_fixtures:
        row["requires_fixtures"] = requires_fixtures
    return row


def build_commands() -> list[dict]:
    commands: list[dict] = []

    commands.append(
        entry(
            entry_id="queue-boundaries",
            category="functional",
            source="API-007",
            command=["python3", "scripts/ci/check_queue_boundaries.py", "--controls"],
            notes="Queue module boundary check and negative controls",
        )
    )

    commands.append(
        entry(
            entry_id="queue-lib-s3-turso",
            category="functional",
            source="API-007",
            command=CARGO + ["test", "-p", "fireweed", "--all-features", "--lib"],
            notes="queue_recovery_pipeline and queue::s3_turso tests; live S3 cases use FIREWEED_RUSTFS_BIN",
            requires_fixtures=["FIREWEED_RUSTFS_BIN"],
        )
    )

    commands.append(
        entry(
            entry_id="queue-postgres-simple",
            category="functional",
            source="API-007",
            command=CARGO
            + [
                "test",
                "-p",
                "fireweed",
                "--features",
                "postgres",
                "--test",
                "simple_queue_postgres",
            ],
            notes="Queue::postgres over the PostgreSQL control and store",
            requires_fixtures=["FIREWEED_PG_TEST_URL"],
        )
    )

    commands.append(
        entry(
            entry_id="queue-postgres-store",
            category="functional",
            source="API-007",
            command=CARGO
            + [
                "test",
                "-p",
                "fireweed-postgres",
                "--test",
                "queue_contract",
                "--test",
                "queue_process_loss",
                "--test",
                "queue_server_crash",
            ],
            notes="PostgreSQL QueueStore contract, process-loss and server-crash targets",
            requires_fixtures=["FIREWEED_PG_TEST_URL"],
        )
    )

    # Policy verifier remains zero-arg and proves no promotion/authoritative mode.
    commands.append(
        entry(
            entry_id="github-actions-policy",
            category="policy",
            source="P13a+P13",
            command=["bash", "scripts/ci/verify-github-actions-policy.sh"],
            notes="Zero-arg policy: no promotion claim; no authoritative performance",
        )
    )

    return commands


def build_allowlist() -> dict:
    commands = build_commands()
    require(len(commands) >= 5, "allowlist must contain the governed command set")
    return {
        "schema_version": SCHEMA_VERSION,
        "workflow": ".github/workflows/governed-product.yml",
        "lane": "governed-product",
        "product_release_readiness_claimed": False,
        "disclaimer": (
            "Repository-side governed allowlist populated by P13. "
            "Live ruleset/required-check proof is P13b. "
            "product_release_readiness_claimed remains false until P13b external proof."
        ),
        "commands": commands,
        "command_population_owner": "P13",
        "forbidden_in_lane": FORBIDDEN_IN_LANE,
        "notes": [
            "P13 populates commands from the Queue test targets and the queue boundary check.",
            "No authoritative performance, fireweed-bench, or scripts/perf entries.",
            "Regenerate with: python3 scripts/ci/generate-governed-product-allowlist.py --write",
        ],
        "plan_key": PLAN_KEY,
    }


def write_json(path: Path, document: dict) -> None:
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="write allowlist JSON")
    parser.add_argument(
        "--check",
        action="store_true",
        help="require on-disk allowlist match regeneration",
    )
    parser.add_argument("--emit", action="store_true", help="print allowlist JSON")
    args = parser.parse_args()
    try:
        allowlist = build_allowlist()

        if args.write:
            write_json(ALLOWLIST, allowlist)
            print(
                f"wrote {ALLOWLIST.relative_to(ROOT)} "
                f"({len(allowlist['commands'])} commands)"
            )

        if args.check:
            require(ALLOWLIST.is_file(), "allowlist missing; run --write")
            on_disk_allow = json.loads(ALLOWLIST.read_text())
            require(
                on_disk_allow == allowlist,
                "governed-product-allowlist.json drift; regenerate with --write",
            )
            print(
                f"governed-product allowlist check passed "
                f"({len(allowlist['commands'])} commands)"
            )

        if args.emit:
            print(json.dumps({"allowlist": allowlist}, indent=2, sort_keys=True))
        elif not (args.write or args.check):
            print(
                f"governed-product allowlist valid "
                f"({len(allowlist['commands'])} commands; "
                f"product_release_readiness_claimed=false)"
            )
        return 0
    except AllowlistError as error:
        print(f"generate-governed-product-allowlist failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
