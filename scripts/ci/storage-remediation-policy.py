#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

from fireweed_test_placement import PlacementError
from fireweed_test_placement import self_test as fireweed_placement_self_test
from fireweed_test_placement import validate as validate_fireweed_test_placement


ROOT = Path(__file__).resolve().parents[2]
INVENTORY = ROOT / "scripts/ci/storage-remediation-inventory.json"
AUTHORITY = ROOT / "docs/helix/04-build/storage-authority-manifest.json"
MODE_FILE = ROOT / "scripts/ci/storage-remediation-policy.mode"
GENERATOR = ROOT / "scripts/ci/inventory-storage-remediation.py"
CI = ROOT / ".github/workflows/ci.yml"
PR_GATE = ROOT / "scripts/ci/pr-gate.sh"
CARGO_MANIFEST = ROOT / "Cargo.toml"


class PolicyError(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise PolicyError(message)


def parse_mode(path: Path) -> str:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        require("=" in line, f"malformed mode line: {line}")
        key, value = line.split("=", 1)
        require(key not in values, f"duplicate mode key: {key}")
        values[key] = value
    require(values == {"schema_version": "1", "policy": values.get("policy", "")}, "mode schema")
    require(values["policy"] in {"remediation", "closure"}, "unknown policy mode")
    return values["policy"]


def current_inventory() -> dict[str, object]:
    completed = subprocess.run(
        [sys.executable, str(GENERATOR), "--emit"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    require(completed.returncode == 0, f"inventory refresh failed: {completed.stderr}")
    return json.loads(completed.stdout)


def validate_inventory(document: object, policy: str, *, check_repository: bool) -> int:
    require(isinstance(document, dict), "inventory must be an object")
    required = {
        "schema_version",
        "generated_by",
        "authority_manifest_sha256",
        "source_inventory_sha256",
        "workspaces",
        "harness_routes",
        "rustdoc_routes",
        "fireweed_test_placement",
        "debt_registries",
        "release_repeat_quarantine",
        "discovery_negatives",
    }
    require(set(document) == required, "inventory schema drift")
    require(document["schema_version"] == 1, "inventory schema_version")
    require(
        document["generated_by"] == "scripts/ci/inventory-storage-remediation.py",
        "inventory generator identity",
    )
    manifests = [row["manifest"] for row in document["workspaces"]]
    require(
        manifests
        == [
            "Cargo.toml",
            "crates/fireweed-bench/Cargo.toml",
            "tools/fireweed-turso-compat-probe/Cargo.toml",
        ],
        "workspace routing drift",
    )
    require(document["workspaces"][0]["routing"] == "root", "root workspace classification")
    require(
        all(row["routing"] == "independent" for row in document["workspaces"][1:]),
        "independent workspace classification",
    )
    validate_fireweed_test_placement(document["fireweed_test_placement"])
    for workspace in document["workspaces"]:
        require(
            workspace["listing_status"] in {"listed", "compile_failure_debt"},
            f"workspace was not route-listed: {workspace['manifest']}",
        )
        if workspace["listing_status"] == "compile_failure_debt":
            require(workspace["listing_error_sha256"], "workspace failure lacks diagnostic digest")
    if check_repository:
        require(
            document["authority_manifest_sha256"] == hashlib.sha256(AUTHORITY.read_bytes()).hexdigest(),
            "authority manifest changed; regenerate inventory",
        )
        refreshed = current_inventory()
        require(
            document["source_inventory_sha256"] == refreshed["source_inventory_sha256"],
            "source inventory changed; regenerate remediation inventory",
        )
        require(
            document["debt_registries"] == refreshed["debt_registries"],
            "discovered debt changed; regenerate remediation inventory",
        )
        require(
            document["release_repeat_quarantine"] == refreshed["release_repeat_quarantine"],
            "release-repeat quarantine changed; regenerate remediation inventory",
        )
        require(
            document["fireweed_test_placement"] == refreshed["fireweed_test_placement"],
            "Fireweed test placement changed; regenerate remediation inventory with --with-cargo",
        )

    route_ids = [route["id"] for route in document["harness_routes"]]
    require(len(route_ids) == len(set(route_ids)), "duplicate harness route")
    for route in document["harness_routes"]:
        require(route["expected_ran"] == 1, f"route lacks ran=1 contract: {route['id']}")
        require(route["exact_invocation"][-1] == "--exact", f"route not exact: {route['id']}")
    doc_ids = [route["id"] for route in document["rustdoc_routes"]]
    require(len(doc_ids) == len(set(doc_ids)), "duplicate rustdoc route")
    for route in document["rustdoc_routes"]:
        require(route["expected_ran"] == 1, f"rustdoc lacks ran=1: {route['id']}")
        if policy == "closure":
            require(route["observed_ran"] == 1, f"rustdoc exact route did not run once: {route['id']}")
        require(route["exact_invocation"][-1] == "--exact", f"rustdoc not exact: {route['id']}")
        require(route["normalized_block_sha256"], f"rustdoc lacks block digest: {route['id']}")
        require(route["owner_item"], f"rustdoc lacks owner item: {route['id']}")

    debt_count = 0
    debt_ids: set[str] = set()
    registries = document["debt_registries"]
    required_registries = {
        "source_registration",
        "test_boundary_debt",
        "cargo_machete_exceptions",
        "ignored_tests",
        "harness_skips",
        "quarantine",
        "opt_ins",
        "loud_skips",
        "no_ops",
        "source_guards",
        "workflow_inline",
        "release_repeat_contract",
        "rustdoc_unlisted_or_compile_only",
        "workspace_listing_failures",
        "public_release_gate_failures",
    }
    require(set(registries) == required_registries, "debt registry set drift")
    machete_exceptions = registries["cargo_machete_exceptions"]
    # P12a bound Turso as a real server feature dependency (`dep:fireweed-turso`); the
    # cargo-machete ignore exception is retired. Empty is the only legal end state.
    # While any residual ignore remains, it must still be the single P12a-owned Turso row.
    if machete_exceptions:
        require(
            len(machete_exceptions) == 1
            and machete_exceptions[0]["path"] == "crates/fireweed-server/Cargo.toml"
            and machete_exceptions[0]["identity"] == "fireweed-turso"
            and machete_exceptions[0]["dependency_chain"] == ["P12a", "P2f"],
            "cargo-machete exceptions must be empty or the single P12a-bound Turso dependency",
        )
    for category, rows in registries.items():
        require(isinstance(rows, list), f"{category} registry")
        for row in rows:
            require(row["id"] not in debt_ids, f"duplicate debt id {row['id']}")
            debt_ids.add(row["id"])
            require(row["owner"], f"unassigned debt {row['id']}")
            require(row["dependency_chain"], f"unwired debt {row['id']}")
            require(row["status"] in {"debt", "legacy_false_green", "discovery_negative"}, "debt status")
            if row["status"] != "discovery_negative":
                debt_count += 1
    quarantine = document["release_repeat_quarantine"]
    require(len(quarantine["legacy_rows"]) == 9, "legacy repeat row count drift")
    require(
        all(row["kind"] == "legacy_false_green" and row["executable"] is False for row in quarantine["legacy_rows"]),
        "legacy repeat rows must be non-executable debt fixtures",
    )
    require(len(quarantine["required_contract_debts"]) == 11, "contract debt count drift")
    require(quarantine["current_verifier_semantics"] == "missing_only_required_minus_names", "verifier characterization")
    require(quarantine["required_jobs_executed_or_counted"] is False, "false execution claim")

    if policy == "closure":
        require(debt_count == 0, f"closure blocked by {debt_count} assigned debt rows")
        require(not quarantine["legacy_rows"], "closure blocked by legacy false-green rows")
    return debt_count


def validate_cargo_scope(text: str) -> None:
    require("No crate is excluded" not in text, "root Cargo comment makes false complete claim")
    require(
        "crates/fireweed-bench/Cargo.toml" in text,
        "root Cargo comment omits independent benchmark workspace",
    )
    require(
        "tools/fireweed-turso-compat-probe/Cargo.toml" in text,
        "root Cargo comment omits independent Turso workspace",
    )
    require(
        "members below" in text,
        "root Cargo comment does not bound cargo test --workspace",
    )


def validate_shape() -> None:
    require(parse_mode(MODE_FILE) in {"remediation", "closure"}, "versioned mode file invalid")
    ci = CI.read_text()
    workflow_marker = "run: bash scripts/ci/verify-github-actions-policy.sh"
    release_marker = "python3 scripts/ci/public-release-gate.py"
    policy_marker = "bash scripts/ci/storage-remediation-policy.sh --mode-file scripts/ci/storage-remediation-policy.mode"
    require(ci.count(workflow_marker) == 1, "workflow-policy invocation missing/duplicated")
    require(ci.count(release_marker) == 1, "public-release invocation missing/duplicated")
    require(ci.count(policy_marker) == 1, "mode-file policy invocation missing/duplicated")
    require(
        ci.index(workflow_marker) < ci.index(release_marker) < ci.index(policy_marker),
        "functional gate invocation order drift",
    )
    pr_gate = PR_GATE.read_text()
    require("bootstrap|enforcing|remediation|closure" in pr_gate, "legacy/new mode composition missing")
    require('POLICY_MODE="remediation"' in pr_gate, "bootstrap remediation composition missing")
    require('POLICY_MODE="closure"' in pr_gate, "enforcing closure composition missing")
    fast_block = pr_gate[pr_gate.index('if [[ "$MODE" == "remediation"'):]
    fast_block = fast_block[: fast_block.index('echo "--- fmt ---"')]
    require(re.search(r"\bcargo\s+(?:test|check|build|clippy)", fast_block) is None, "fast modes execute Cargo")
    validate_cargo_scope(CARGO_MANIFEST.read_text())


def self_test(document: dict[str, object]) -> None:
    validate_shape()
    fireweed_placement_self_test(document["fireweed_test_placement"])
    try:
        validate_cargo_scope(
            "# No crate is excluded, so cargo test covers everything\n[workspace]\nmembers=[]\n"
        )
    except PolicyError:
        pass
    else:
        raise PolicyError("false-complete Cargo scope fixture passed")
    try:
        parse_mode(Path("scripts/ci/fixtures/closure/all-closed.json"))
    except PolicyError:
        pass
    else:
        raise PolicyError("malformed policy mode fixture passed")
    malformed = dict(document)
    malformed.pop("workspaces")
    try:
        validate_inventory(malformed, "remediation", check_repository=False)
    except PolicyError:
        pass
    else:
        raise PolicyError("malformed inventory fixture passed")
    debt = validate_inventory(document, "remediation", check_repository=False)
    require(debt > 0, "remediation fixture must report debt")
    try:
        validate_inventory(document, "closure", check_repository=False)
    except PolicyError:
        pass
    else:
        raise PolicyError("closure fixture passed with debt")


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--policy", choices=["remediation", "closure"])
    group.add_argument("--mode-file", type=Path)
    group.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        document = json.loads(INVENTORY.read_text())
        if args.self_test:
            self_test(document)
            print("storage remediation policy self-test passed")
            return 0
        policy = args.policy or parse_mode(args.mode_file)
        validate_shape()
        debt_count = validate_inventory(document, policy, check_repository=True)
        if policy == "remediation":
            print(f"storage remediation policy: {debt_count} assigned debt rows (report-only; not closure)")
        else:
            print("storage remediation policy: zero debt; closure enabled")
        return 0
    except (PolicyError, PlacementError, json.JSONDecodeError, KeyError, TypeError) as error:
        print(f"storage remediation policy failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
