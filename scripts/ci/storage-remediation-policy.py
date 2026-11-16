#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import runpy
import subprocess
import sys
import tomllib

from stock_turso_policy import StockTursoError, read_and_validate as validate_stock_turso, self_test as stock_turso_self_test

from queue_pg_evidence import verify_report as verify_queue_report
from queue_pg_workflow import validate as validate_queue_workflow, self_test as queue_workflow_self_test


ROOT = Path(__file__).resolve().parents[2]
INVENTORY = ROOT / "scripts/ci/storage-remediation-inventory.json"
AUTHORITY = ROOT / "scripts/ci/manifests/storage-authority-manifest.json"
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


# The retired fork has no surviving dependency-ignore exceptions.
VENDOR_MACHETE_PAIRS: set[tuple[str, str]] = set()


def validate_external_dependencies(
    observations: object, registries: set[str], product_ids: set[str], *, check_repository: bool
) -> None:
    require(isinstance(observations, dict), "external dependency observations must be an object")
    require(set(observations) == {"scope", "qualification", "findings"}, "external observation schema drift")
    require(observations["scope"] == "vendor/", "external scope must be vendor/ only")
    require(
        observations["qualification"] == "not_qualified_by_product_closure",
        "external observations must not claim product qualification",
    )
    require(isinstance(observations["findings"], list), "external observations findings must be a list")
    seen = set(product_ids)
    machete_pairs = []
    for observation in observations["findings"]:
        require(isinstance(observation, dict) and set(observation) == {"registry", "finding"}, "external finding schema")
        registry, row = observation["registry"], observation["finding"]
        require(registry in registries, "unknown external source registry")
        require(isinstance(row, dict), "external finding must retain its original debt row")
        path = row.get("path", "")
        require(isinstance(path, str), "external finding path must be a string")
        parts = path.split("/")
        require(
            len(parts) > 1 and parts[0] == "vendor" and "\\" not in path
            and all(part not in {"", ".", ".."} for part in parts),
            f"external finding is outside normalized vendor scope: {path}",
        )
        if check_repository:
            require((ROOT / path).resolve().is_relative_to((ROOT / "vendor").resolve()), "external path escapes vendor via symlink")
        require(row["id"] not in seen, f"duplicate product/external finding id {row['id']}")
        seen.add(row["id"])
        require(row["owner"] and row["dependency_chain"], "external finding lost original ownership")
        require(row["status"] in {"debt", "legacy_false_green", "discovery_negative"}, "external observations cannot be labeled passes")
        if registry == "cargo_machete_exceptions" or row.get("category") == "cargo_machete_exception":
            require(registry == "cargo_machete_exceptions" and row.get("category") == "cargo_machete_exception", "external machete category mismatch")
            machete_pairs.append((path, row["identity"]))
    require(
        len(machete_pairs) == len(VENDOR_MACHETE_PAIRS) and set(machete_pairs) == VENDOR_MACHETE_PAIRS,
        "external observations cannot restore retired vendor dependency ignores",
    )
    if check_repository:
        validate_stock_turso(ROOT)


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


def validate_inventory(document: object, policy: str, *, check_repository: bool,
                       executed_queue_ids: frozenset[str] = frozenset()) -> int:
    require(isinstance(document, dict), "inventory must be an object")
    required = {
        "schema_version",
        "generated_by",
        "authority_manifest_sha256",
        "source_inventory_sha256",
        "workspaces",
        "harness_routes",
        "rustdoc_routes",
        "debt_registries",
        "external_dependency_observations",
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
        ],
        "workspace routing drift",
    )
    require(document["workspaces"][0]["routing"] == "root", "root workspace classification")
    require(
        all(row["routing"] == "independent" for row in document["workspaces"][1:]),
        "independent workspace classification",
    )
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
            document["external_dependency_observations"] == refreshed["external_dependency_observations"],
            "external dependency observations changed; regenerate remediation inventory",
        )

    route_ids = [route["id"] for route in document["harness_routes"]]
    require(len(route_ids) == len(set(route_ids)), "duplicate harness route")
    harness_route = runpy.run_path(str(GENERATOR))["cargo_harness_route"]
    for route in document["harness_routes"]:
        features = route.get("features")
        require(isinstance(features, list) and all(isinstance(feature, str) for feature in features),
                f"route feature selection missing/invalid: {route['id']}")
        require(features == sorted(set(features)), f"route features not canonical: {route['id']}")
        expected = harness_route(route["workspace"], route["package"],
                                 {"name": route["target"], "kind": route["target_kind"]},
                                 route["harness_id"], route["listed_kind"], features)
        require(route["exact_invocation"] == expected["exact_invocation"],
                f"route feature/selector command differs: {route['id']}")
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
        "cargo_machete_exceptions",
        "ignored_tests",
        "harness_skips",
        "quarantine",
        "opt_ins",
        "loud_skips",
        "no_ops",
        "source_guards",
        "workflow_inline",
        "rustdoc_unlisted_or_compile_only",
        "workspace_listing_failures",
        "public_release_gate_failures",
    }
    require(set(registries) == required_registries, "debt registry set drift")
    machete_exceptions = registries["cargo_machete_exceptions"]
    # The cargo-machete ignore exception is retired. Empty is the only legal end state.
    require(not machete_exceptions, "cargo-machete exceptions must be empty")
    eligible_queue_ids = {row['id'] for row in registries['ignored_tests']
        if row['status'] == 'debt' and row['category'] == 'ignored_test'
        and row['path'].startswith(('crates/fireweed-postgres/', 'crates/fireweed/'))}
    require(executed_queue_ids <= eligible_queue_ids,
            'queue execution cannot exempt unrelated categories or packages')
    for category, rows in registries.items():
        require(isinstance(rows, list), f"{category} registry")
        for row in rows:
            require(not str(row["path"]).startswith("vendor/"), "vendor finding must remain visible in external observations")
            require(row["id"] not in debt_ids, f"duplicate debt id {row['id']}")
            debt_ids.add(row["id"])
            require(row["owner"], f"unassigned debt {row['id']}")
            require(row["dependency_chain"], f"unwired debt {row['id']}")
            require(row["status"] in {"debt", "legacy_false_green", "discovery_negative"}, "debt status")
            if row["status"] != "discovery_negative":
                if (category != "ignored_tests" or row.get("category") != "ignored_test"
                        or row["id"] not in executed_queue_ids or row["status"] != "debt"):
                    debt_count += 1
    require(executed_queue_ids <= debt_ids, "execution evidence contains unknown debt IDs")
    validate_external_dependencies(
        document["external_dependency_observations"], required_registries, debt_ids,
        check_repository=check_repository,
    )
    # P10w: after exclusive workflow owners land, every workflow_inline hit is either
    # an executed policy-positive (discovery_negative) or residual exclusive-owner debt.
    # Residual debt is forbidden in the checked-in inventory; the zero-debt report
    # re-proves the classification. No row may pass by status label alone.
    workflow_inline_debt = [
        row
        for row in registries["workflow_inline"]
        if row["status"] != "discovery_negative"
    ]
    require(
        not workflow_inline_debt,
        "workflow_inline residual debt after P10w: "
        + ", ".join(
            f"{row['path']}:{row['line']}:{row.get('detail', '')[:80]}"
            for row in workflow_inline_debt
        ),
    )
    for row in registries["workflow_inline"]:
        require(
            str(row.get("detail", "")).startswith("policy_positive:"),
            f"workflow_inline row lacks executed policy_positive detail: {row['id']}",
        )
    if policy == "closure":
        require(debt_count == 0, f"closure blocked by {debt_count} assigned debt rows")
    return debt_count


def validate_cargo_scope(text: str) -> None:
    require("No crate is excluded" not in text, "root Cargo comment makes false complete claim")
    require(
        "crates/fireweed-bench/Cargo.toml" in text,
        "root Cargo comment omits independent benchmark workspace",
    )
    require(
        "members below" in text,
        "root Cargo comment does not bound cargo test --workspace",
    )


def validate_shape() -> None:
    require(parse_mode(MODE_FILE) in {"remediation", "closure"}, "versioned mode file invalid")
    ci = CI.read_text()
    validate_queue_workflow(ci)
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
    queue_workflow_self_test(CI.read_text())
    cargo = CARGO_MANIFEST.read_text()
    validate_cargo_scope(cargo)
    for label, text in (
        ("false completeness", cargo + "\n# No crate is excluded\n"),
        ("missing benchmark", cargo.replace("crates/fireweed-bench/Cargo.toml", "benchmark workspace")),
        ("missing member bound", cargo.replace("members below", "all crates")),
    ):
        try:
            validate_cargo_scope(text)
        except PolicyError:
            pass
        else:
            raise PolicyError(f"Cargo scope fixture passed: {label}")
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
    generator = runpy.run_path(str(GENERATOR))
    route_for = generator["cargo_harness_route"]
    for features in ([], ["local"], ["default", "local"]):
        route = route_for("Cargo.toml", "fireweed-turso", {"name": "fireweed_turso", "kind": ["lib"]},
                          "queue_projection::tests::feature_fixture", "test", features)
        prefix = ["rustup", "run", "1.99.0", "cargo", "test", "--manifest-path", "Cargo.toml",
                  "--locked", "-p", "fireweed-turso", "--no-default-features"]
        if features:
            prefix += ["--features", ",".join(features)]
        require(route["features"] == features and route["exact_invocation"] == prefix +
                ["--lib", "queue_projection::tests::feature_fixture", "--", "--exact"],
                "Cargo artifact feature selection lost")
        fixture = copy.deepcopy(document)
        fixture["harness_routes"] = [route]
        validate_inventory(fixture, "remediation", check_repository=False)
        mutants = []
        omitted = copy.deepcopy(fixture)
        omitted["harness_routes"][0].pop("features")
        mutants.append(omitted)
        extra = copy.deepcopy(fixture)
        extra["harness_routes"][0]["features"] = sorted(features + ["unexpected"])
        mutants.append(extra)
        omitted_flag = copy.deepcopy(fixture)
        omitted_flag["harness_routes"][0]["exact_invocation"].remove("--no-default-features")
        mutants.append(omitted_flag)
        if features:
            mismatch = copy.deepcopy(fixture)
            position = mismatch["harness_routes"][0]["exact_invocation"].index("--features") + 1
            mismatch["harness_routes"][0]["exact_invocation"][position] = "unexpected"
            mutants.append(mismatch)
        for mutant in mutants:
            try:
                validate_inventory(mutant, "remediation", check_repository=False)
            except PolicyError as error:
                require("feature" in str(error), "feature control failed for unrelated reason")
            else:
                raise PolicyError("Cargo artifact feature omission/mismatch fixture passed")
    for invalid in (None, "local", [1]):
        try:
            route_for("Cargo.toml", "fireweed-turso", {"name": "fireweed_turso", "kind": ["lib"]},
                      "fixture", "test", invalid)
        except ValueError:
            pass
        else:
            raise PolicyError("missing/invalid Cargo artifact feature metadata accepted")
    diagnostic_passed = generator["sql_timing_diagnostic_log_passed"]
    summary = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n"
    ordinary = "running 1 test\ntest module::diagnostic ... ok\n\n" + summary
    interleaved = "running 1 test\ntest module::diagnostic ... measured=1\nmore output\nok\n\n" + summary
    require(diagnostic_passed(ordinary, "diagnostic"), "ordinary diagnostic success rejected")
    require(diagnostic_passed(interleaved, "diagnostic"), "interleaved diagnostic success rejected")
    failed = "running 1 test\ntest module::diagnostic ... FAILED\n\n"
    for malformed_log in (
        failed + ordinary.replace("module::diagnostic", "module::unrelated"),
        failed + summary,
        ordinary + ordinary,
        ordinary.replace("... ok", "... FAILED"),
        interleaved.replace("more output", "FAILED"),
        ordinary.replace("module::diagnostic", "module::unrelated"),
    ):
        require(not diagnostic_passed(malformed_log, "diagnostic"), "failed or concatenated diagnostic fixture passed")

    baseline = validate_inventory(document, "remediation", check_repository=False)
    product_debt = {
        "id": "policy-self-test-product-debt", "category": "ignored_test",
        "path": "crates/fireweed/src/policy_fixture.rs", "line": 1,
        "identity": '#[ignore = "unresolved product failure"]', "status": "debt",
        "owner": "P2f", "dependency_chain": ["P2f"], "detail": "negative fixture",
    }
    fixture = copy.deepcopy(document)
    fixture["debt_registries"]["ignored_tests"].append(product_debt)
    require(
        validate_inventory(fixture, "remediation", check_repository=False) == baseline + 1,
        "product ignored-test debt was not counted",
    )
    try:
        validate_inventory(fixture, "closure", check_repository=False)
    except PolicyError:
        pass
    else:
        raise PolicyError("closure fixture passed with product debt")

    def reject_external(label: str, row: dict[str, object]) -> None:
        fixture = copy.deepcopy(document)
        fixture["external_dependency_observations"]["findings"].append(
            {"registry": "ignored_tests", "finding": row}
        )
        try:
            validate_inventory(fixture, "remediation", check_repository=False)
        except PolicyError:
            return
        raise PolicyError(f"external observation fixture passed: {label}")

    reject_external("product debt smuggled into vendor", product_debt)
    reject_external("vendor traversal", {**product_debt, "path": "vendor/../crates/fireweed/src/policy_fixture.rs"})
    # Ordinary upstream failures remain visible, unresolved and outside product closure.
    upstream = {**product_debt, "path": "vendor/turso_core/policy_fixture.rs"}
    fixture = copy.deepcopy(document)
    fixture["external_dependency_observations"]["findings"].append({"registry": "ignored_tests", "finding": upstream})
    require(validate_inventory(fixture, "remediation", check_repository=False) == baseline, "upstream observation changed product debt count")
    reject_external("upstream finding falsely called a pass", {**upstream, "status": "passed"})
    fixture = copy.deepcopy(document)
    fixture["external_dependency_observations"]["findings"].append({
        "registry": "cargo_machete_exceptions",
        "finding": {**upstream, "category": "cargo_machete_exception", "identity": "arbitrary_unused_dependency"},
    })
    try:
        validate_inventory(fixture, "remediation", check_repository=False)
    except PolicyError:
        pass
    else:
        raise PolicyError("arbitrary vendor dependency ignore passed")

    retired = (
        ("vendor/turso_core/Cargo.toml", "antithesis_sdk"),
        ("vendor/turso_sdk_kit/Cargo.toml", "parking_lot"),
    )
    for pairs in ((retired[0],), (retired[1],), retired):
        fixture = copy.deepcopy(document)
        for path, dependency in pairs:
            fixture["external_dependency_observations"]["findings"].append({
                "registry": "cargo_machete_exceptions",
                "finding": {**upstream, "path": path, "category": "cargo_machete_exception", "identity": dependency},
            })
        try:
            validate_inventory(fixture, "remediation", check_repository=False)
        except PolicyError:
            pass
        else:
            raise PolicyError(f"retired vendor dependency ignores passed: {pairs}")
    unrelated = copy.deepcopy(document)
    row = next(row for row in unrelated['debt_registries']['ignored_tests'] if row['status'] == 'debt')
    unrelated['debt_registries']['ignored_tests'].remove(row)
    row['category'] = 'no_op'
    unrelated['debt_registries']['no_ops'].append(row)
    try:
        validate_inventory(unrelated, 'remediation', check_repository=False,
                           executed_queue_ids=frozenset({row['id']}))
    except PolicyError:
        pass
    else:
        raise PolicyError('queue execution incorrectly exempted unrelated debt category')
    stock_turso_self_test()
    validate_stock_turso(ROOT)


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--policy", choices=["remediation", "closure"])
    group.add_argument("--mode-file", type=Path)
    group.add_argument("--self-test", action="store_true")
    parser.add_argument("--queue-evidence", type=Path)
    args = parser.parse_args()
    try:
        document = json.loads(INVENTORY.read_text())
        if args.self_test:
            self_test(document)
            print("storage remediation policy self-test passed")
            return 0
        policy = args.policy or parse_mode(args.mode_file)
        validate_shape()
        executed = frozenset(verify_queue_report(args.queue_evidence)) if args.queue_evidence else frozenset()
        debt_count = validate_inventory(document, policy, check_repository=True, executed_queue_ids=executed)
        if policy == "remediation":
            print(f"storage remediation policy: {debt_count} assigned product debt rows (report-only; not closure)")
        else:
            print(f"storage remediation policy: zero unexecuted product debt; {len(executed)} queue rows verified by execution and retained in inventory")
        count = len(document["external_dependency_observations"]["findings"])
        print(f"external dependency observations: {count}; not qualified by product closure")
        return 0
    except (ValueError, PolicyError, StockTursoError, json.JSONDecodeError, KeyError, TypeError, OSError, tomllib.TOMLDecodeError) as error:
        print(f"storage remediation policy failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
