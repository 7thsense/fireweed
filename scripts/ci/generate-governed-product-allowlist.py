#!/usr/bin/env python3
"""P13: generate the sole final governed-product allowlist from manifest sources.

Owns population of scripts/ci/governed-product-allowlist.json from:
  - P10r functional-matrix route sources (exact leaf cargo groups)
  - P12/T4 helm static deploy gate
  - reduced-count functional live matrix (storage_matrix_t0_t2; non-authoritative)
  - P8k ExternalKafka fixture readiness/sentinel + feature-on route bindings

Does not claim product-release readiness (P13b) and never authors performance
or authoritative timing commands (forbidden_in_lane).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ROUTE_SOURCES = ROOT / "docs/helix/04-build/functional-matrix-route-sources.json"
ALLOWLIST = ROOT / "scripts/ci/governed-product-allowlist.json"
SERVICES = ROOT / "scripts/ci/governed-product-services.json"
P8K_FIXTURE = ROOT / "crates/fireweed-server/tests/support/external_kafka_fixture.rs"

SCHEMA_VERSION = 1
PLAN_KEY = "P13"
CARGO = ["rustup", "run", "1.92.0", "cargo"]

FORBIDDEN_IN_LANE = [
    "scripts/perf/",
    "fireweed-bench",
    "cargo bench",
    "authoritative-performance",
    "performance_",
]

# P8k immutable pin (must match external_kafka_fixture.rs constants).
REDPANDA_IMAGE_REPOSITORY = "redpandadata/redpanda"
REDPANDA_IMAGE_DIGEST = (
    "sha256:f60d828ed6cafd7ce4c9b987ff71699895b81fe53f1d0e27ebf045277fcff21a"
)
# Static redpanda argv after the image (advertise-kafka-addr is port-bound at start).
REDPANDA_COMMAND = [
    "redpanda",
    "start",
    "--overprovisioned",
    "--smp",
    "1",
    "--memory",
    "512M",
    "--reserve-memory",
    "0M",
    "--node-id",
    "0",
    "--check=false",
    "--kafka-addr",
    "INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093",
]


class AllowlistError(Exception):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AllowlistError(message)


def load_route_sources() -> dict:
    require(ROUTE_SOURCES.is_file(), f"missing {ROUTE_SOURCES}")
    return json.loads(ROUTE_SOURCES.read_text())


def p8k_digest_from_fixture() -> str:
    text = P8K_FIXTURE.read_text()
    match = re.search(
        r'pub const REDPANDA_IMAGE_DIGEST: &str =\s*"((?:sha256:)[0-9a-f]{64})"',
        text,
    )
    require(match is not None, "REDPANDA_IMAGE_DIGEST not found in P8k fixture")
    return match.group(1)


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

    features = "memory,sqlite,objectlog,postgres,turso"
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
            notes="Exact dry-run leaves: 20 strict + 8 async + 12 invalid + AC-TXN + guards",
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
                "storage_matrix_t0_t2_all_twenty_cells",
                "--",
                "--exact",
                "--nocapture",
            ],
            notes=(
                "Live 20-cell T0–T2 reduced-count functional matrix; "
                "require ran=20 skipped=0 with PG+S3 fixtures (fail-closed)"
            ),
            requires_fixtures=["FIREWEED_PG_TEST_URL", "FIREWEED_S3_TEST_ENDPOINT"],
        )
    )

    # Registration leaf (always offline-safe).
    commands.append(
        entry(
            entry_id="functional-matrix-registers-20-cells",
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

    # Aggregate storage-matrix gate (functional list + helm T4; non-REQUIRE_FULL local shape).
    commands.append(
        entry(
            entry_id="storage-matrix-gate",
            category="functional",
            source="P10r+P12",
            command=["bash", "scripts/ci/storage-matrix-gate.sh"],
            notes="Route-source check, dry-run leaves, exact server list, helm T4 fixtures",
        )
    )

    # --- T4 ---
    commands.append(
        entry(
            entry_id="t4-helm-gate",
            category="T4",
            source="P12",
            command=["bash", "scripts/ci/helm-gate.sh"],
            notes="20-cell Helm lint/render/kubeconform + Turso default",
        )
    )

    # --- P8k readiness/sentinel preflight ---
    commands.append(
        entry(
            entry_id="p8k-external-kafka-fixture-preflight",
            category="external-kafka",
            source="P8k",
            command=CARGO
            + [
                "test",
                "-p",
                "fireweed-server",
                "--features",
                "postgres,external-kafka",
                "--test",
                "external_kafka_fixture",
                "--",
                "--nocapture",
            ],
            notes="Digest-pinned broker start, rskafka sentinel produce/fetch, teardown",
            requires_fixtures=["docker", "REDPANDA_IMAGE"],
        )
    )

    # --- Feature-on route bindings from P10r registry ---
    feature_on = [
        leaf
        for leaf in route_doc["leaves"]
        if leaf.get("kind") == "external_kafka" and leaf.get("feature_tuple") == "feature-on"
    ]
    require(len(feature_on) >= 1, "expected ≥1 external_kafka feature-on leaves")
    for leaf in feature_on:
        leaf_id = leaf["leaf_id"]
        cargo_args = list(leaf["cargo_args"])
        filt = leaf["test_filter"]
        commands.append(
            entry(
                entry_id=f"external-kafka-feature-on:{leaf_id}",
                category="external-kafka",
                source="P10r",
                command=CARGO + cargo_args + ["--", filt, "--exact", "--nocapture"],
                notes=leaf.get("notes") or "Feature-on ExternalKafka route binding",
            )
        )

    # Feature-off counterparts stay bound so the pair cannot alias.
    feature_off = [
        leaf
        for leaf in route_doc["leaves"]
        if leaf.get("kind") == "external_kafka" and leaf.get("feature_tuple") == "feature-off"
    ]
    require(len(feature_off) >= 1, "expected ≥1 external_kafka feature-off leaves")
    for leaf in feature_off:
        leaf_id = leaf["leaf_id"]
        cargo_args = list(leaf["cargo_args"])
        filt = leaf["test_filter"]
        commands.append(
            entry(
                entry_id=f"external-kafka-feature-off:{leaf_id}",
                category="external-kafka",
                source="P10r",
                command=CARGO + cargo_args + ["--", filt, "--exact", "--nocapture"],
                notes=leaf.get("notes") or "Feature-off ExternalKafka negative binding",
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
    require(len(commands) >= 8, "allowlist must contain the governed command set")
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
            "P13 populates commands from functional-matrix-route-sources.json, T4 helm-gate, "
            "reduced-count live T0–T2 matrix, and P8k/feature-on ExternalKafka bindings.",
            "No authoritative performance, fireweed-bench, or scripts/perf entries.",
            "Regenerate with: python3 scripts/ci/generate-governed-product-allowlist.py --write",
        ],
        "plan_key": PLAN_KEY,
        "route_sources_sha256_field": "authority_manifest_sha256",
        "route_sources_authority_manifest_sha256": route_doc.get("authority_manifest_sha256"),
    }


def build_services() -> dict:
    fixture_digest = p8k_digest_from_fixture()
    require(
        fixture_digest == REDPANDA_IMAGE_DIGEST,
        f"P8k fixture digest {fixture_digest} != generator pin {REDPANDA_IMAGE_DIGEST}",
    )
    return {
        "schema_version": 1,
        "workflow": ".github/workflows/governed-product.yml",
        "lane": "governed-product",
        "services": {
            "kafka_compatible_broker": {
                "authorized": True,
                "purpose": "P8k hermetic ExternalKafka qualification fixture surface",
                "image_repository": REDPANDA_IMAGE_REPOSITORY,
                "image_digest": REDPANDA_IMAGE_DIGEST,
                "image_pinned": f"{REDPANDA_IMAGE_REPOSITORY}@{REDPANDA_IMAGE_DIGEST}",
                "command": list(REDPANDA_COMMAND),
                "digest_population_owner": "P13",
                "command_population_owner": "P13",
                "requirements": {
                    "image_must_be_digest_pinned": True,
                    "tag_only_image_forbidden": True,
                    "shared_external_cluster_forbidden": True,
                    "immutable_digest_form": "repository@sha256:<64-hex>",
                },
                "notes": [
                    "Digest and static redpanda argv match crates/fireweed-server/tests/support/external_kafka_fixture.rs.",
                    "advertise-kafka-addr is bound at container start to the published loopback port.",
                    "Readiness/sentinel preflight is allowlist entry p8k-external-kafka-fixture-preflight.",
                ],
            }
        },
        "forbidden_service_names_outside_governed_lane": [
            "postgres",
            "kafka",
            "redpanda",
            "minio",
            "zookeeper",
        ],
    }


def write_json(path: Path, document: dict) -> None:
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="write allowlist + services JSON")
    parser.add_argument(
        "--check",
        action="store_true",
        help="require on-disk allowlist/services match regeneration",
    )
    parser.add_argument("--emit", action="store_true", help="print allowlist JSON")
    args = parser.parse_args()
    try:
        route_doc = load_route_sources()
        allowlist = build_allowlist(route_doc)
        services = build_services()

        if args.write:
            write_json(ALLOWLIST, allowlist)
            write_json(SERVICES, services)
            print(
                f"wrote {ALLOWLIST.relative_to(ROOT)} "
                f"({len(allowlist['commands'])} commands) and "
                f"{SERVICES.relative_to(ROOT)}"
            )

        if args.check:
            require(ALLOWLIST.is_file(), "allowlist missing; run --write")
            require(SERVICES.is_file(), "services missing; run --write")
            on_disk_allow = json.loads(ALLOWLIST.read_text())
            on_disk_services = json.loads(SERVICES.read_text())
            require(
                on_disk_allow == allowlist,
                "governed-product-allowlist.json drift; regenerate with --write",
            )
            require(
                on_disk_services == services,
                "governed-product-services.json drift; regenerate with --write",
            )
            print(
                f"governed-product allowlist/services check passed "
                f"({len(allowlist['commands'])} commands; "
                f"kafka digest={services['services']['kafka_compatible_broker']['image_digest'][:19]}…)"
            )

        if args.emit:
            print(json.dumps({"allowlist": allowlist, "services": services}, indent=2, sort_keys=True))
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
