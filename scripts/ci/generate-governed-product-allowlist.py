#!/usr/bin/env python3
"""P13: generate the sole final governed-product allowlist from manifest sources.

Owns population of scripts/ci/governed-product-allowlist.json from:
  - P10r functional-matrix route sources (exact leaf cargo groups)
  - reduced-count functional live matrix (storage_matrix_t0_t2; non-authoritative)

Does not claim product-release readiness (P13b) and never authors performance
or authoritative timing commands (forbidden_in_lane).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ROUTE_SOURCES = ROOT / "scripts/ci/manifests/functional-matrix-route-sources.json"
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


def load_route_sources() -> dict:
    require(ROUTE_SOURCES.is_file(), f"missing {ROUTE_SOURCES}")
    return json.loads(ROUTE_SOURCES.read_text())


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


def build_commands(route_doc: dict) -> list[dict]:
    commands: list[dict] = []

    # --- Functional (manifest-generated) ---
    commands.append(
        entry(
            entry_id="functional-route-sources-check",
            category="functional",
            source="P10r",
            command=[
                "python3",
                "scripts/ci/functional_matrix_route_sources.py",
                "--check",
                "--self-test",
            ],
            notes="Manifest registry cardinality and exact-leaf self-test",
        )
    )

    features = "memory,objectlog,postgres,turso"
    commands.append(
        entry(
            entry_id="functional-matrix-dry-run-leaves",
            category="functional",
            source="P10r",
            command=CARGO
            + [
                "test",
                "-p",
                "fireweed",
                "--features",
                features,
                "--test",
                "functional_matrix_route_sources",
                "--",
                "--nocapture",
            ],
            notes="Exact dry-run leaves: 12 strict + 6 async + 6 invalid + AC-TXN + guards",
        )
    )

    # Live reduced-count functional matrix (T0–T2). Item counts are fixture-scale,
    # never authoritative performance. Assert ran=20 skipped=0 under live fixtures.
    commands.append(
        entry(
            entry_id="functional-matrix-t0-t2-live-reduced-count",
            category="reduced-count",
            source="P10",
            command=CARGO
            + [
                "test",
                "-p",
                "fireweed",
                "--features",
                features,
                "--test",
                "storage_matrix_t0_t2",
                "storage_matrix_t0_t2_all_twelve_cells",
                "--",
                "--exact",
                "--nocapture",
            ],
            notes=(
                "Live 12-cell T0–T2 reduced-count functional matrix; "
                "require ran=20 skipped=0 with PG+S3 fixtures (fail-closed)"
            ),
            requires_fixtures=["FIREWEED_PG_TEST_URL", "FIREWEED_S3_TEST_ENDPOINT"],
        )
    )

    # Registration leaf (always offline-safe).
    commands.append(
        entry(
            entry_id="functional-matrix-registers-12-cells",
            category="functional",
            source="P10r",
            command=CARGO
            + [
                "test",
                "-p",
                "fireweed",
                "--features",
                features,
                "--test",
                "storage_matrix_t0_t2",
                "storage_matrix_registers_exactly_20_distinct_cells",
                "--",
                "--exact",
                "--nocapture",
            ],
            notes="Offline registration: exactly 20 distinct cell IDs",
        )
    )

    # Aggregate storage-matrix gate (functional list; non-REQUIRE_FULL local shape).
    commands.append(
        entry(
            entry_id="storage-matrix-gate",
            category="functional",
            source="P10r+P12",
            command=["bash", "scripts/ci/storage-matrix-gate.sh"],
            notes="Route-source check, dry-run leaves, legacy product-name hygiene",
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


def build_allowlist(route_doc: dict) -> dict:
    commands = build_commands(route_doc)
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
            "P13 populates commands from functional-matrix-route-sources.json and the "
            "reduced-count live T0–T2 matrix.",
            "No authoritative performance, fireweed-bench, or scripts/perf entries.",
            "Regenerate with: python3 scripts/ci/generate-governed-product-allowlist.py --write",
        ],
        "plan_key": PLAN_KEY,
        "route_sources_sha256_field": "authority_manifest_sha256",
        "route_sources_authority_manifest_sha256": route_doc.get("authority_manifest_sha256"),
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
        route_doc = load_route_sources()
        allowlist = build_allowlist(route_doc)

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
