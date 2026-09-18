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


# These two ignores compensate for macro expansion / Cargo feature unification,
# not unused dependencies. Keep the allowlist independent of vendor debt scope.
VENDOR_MACHETE_PAIRS = {
    ("vendor/turso_core/Cargo.toml", "antithesis_sdk"),
    ("vendor/turso_sdk_kit/Cargo.toml", "parking_lot"),
}


def validate_vendor_dependency_invariants(
    manifests: dict[str, dict[str, object]], core_source: str, sdk_source: str
) -> None:
    pairs = []
    for path, manifest in manifests.items():
        ignored = manifest.get("package", {}).get("metadata", {}).get("cargo-machete", {}).get("ignored", [])
        require(isinstance(ignored, list), f"vendor cargo-machete ignored list: {path}")
        pairs.extend((path, dependency) for dependency in ignored)
    require(
        len(pairs) == len(VENDOR_MACHETE_PAIRS) and set(pairs) == VENDOR_MACHETE_PAIRS,
        "vendor cargo-machete ignores must be exactly the reviewed macro/feature pairs",
    )
    core = manifests["vendor/turso_core/Cargo.toml"]
    antithesis = core.get("target", {}).get("cfg(antithesis)", {}).get("dependencies", {}).get("antithesis_sdk", {})
    require(
        antithesis.get("version") and antithesis.get("features") == ["full"]
        and antithesis.get("default-features") is False,
        "antithesis_sdk ignore requires the cfg(antithesis) full-feature dependency",
    )
    require(core.get("dependencies", {}).get("turso_macros", {}).get("version"), "assertion macro dependency missing")
    exports = re.search(r"pub use turso_macros::\{([^}]+)\};", core_source, re.DOTALL)
    require(
        exports is not None and {"turso_assert", "turso_assert_sometimes"}.issubset(
            {name.strip() for name in exports.group(1).split(",")}
        ),
        "antithesis_sdk ignore requires Turso assertion macro exports",
    )
    sdk = manifests["vendor/turso_sdk_kit/Cargo.toml"]
    dependencies = sdk.get("dependencies", {})
    parking_lot = dependencies.get("parking_lot", {})
    require(
        parking_lot.get("version") and parking_lot.get("features") == ["send_guard"]
        and dependencies.get("turso_core", {}).get("path") == "../turso_core",
        "parking_lot ignore requires send_guard feature unification with vendored core",
    )
    require(
        "assert_send!(TursoDatabase, TursoConnection, TursoStatement);" in sdk_source,
        "parking_lot ignore requires the SDK's Send assertions",
    )


def vendor_dependency_sources() -> tuple[dict[str, dict[str, object]], str, str]:
    paths = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "vendor/"], cwd=ROOT, text=True
    ).split("\0")
    manifests = {
        path: tomllib.loads((ROOT / path).read_text())
        for path in sorted(paths) if Path(path).name == "Cargo.toml"
    }
    return manifests, (ROOT / "vendor/turso_core/lib.rs").read_text(), (ROOT / "vendor/turso_sdk_kit/src/rsapi.rs").read_text()


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
        "external observations must retain exactly the reviewed vendor macro/feature ignores",
    )
    if check_repository:
        validate_vendor_dependency_invariants(*vendor_dependency_sources())


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
        "external_dependency_observations",
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
            document["external_dependency_observations"] == refreshed["external_dependency_observations"],
            "external dependency observations changed; regenerate remediation inventory",
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
            require(not str(row["path"]).startswith("vendor/"), "vendor finding must remain visible in external observations")
            require(row["id"] not in debt_ids, f"duplicate debt id {row['id']}")
            debt_ids.add(row["id"])
            require(row["owner"], f"unassigned debt {row['id']}")
            require(row["dependency_chain"], f"unwired debt {row['id']}")
            require(row["status"] in {"debt", "legacy_false_green", "discovery_negative"}, "debt status")
            if row["status"] != "discovery_negative":
                debt_count += 1
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
    quarantine = document["release_repeat_quarantine"]
    legacy_rows = quarantine["legacy_rows"]
    if legacy_rows:
        # Pre-P2r quarantine: nine false-green fixtures and eleven contract debts.
        require(len(legacy_rows) == 9, "legacy repeat row count drift")
        require(
            all(
                row["kind"] == "legacy_false_green" and row["executable"] is False
                for row in legacy_rows
            ),
            "legacy repeat rows must be non-executable debt fixtures",
        )
        require(len(quarantine["required_contract_debts"]) == 11, "contract debt count drift")
        require(
            quarantine["current_verifier_semantics"] == "missing_only_required_minus_names",
            "verifier characterization",
        )
    else:
        # Post-P2r: real generated product_workflow + operator_validation bindings.
        require(
            quarantine["current_verifier_semantics"]
            == "exact_set_product_workflow_namespace",
            "post-P2r verifier characterization",
        )
        require(
            not quarantine["required_contract_debts"],
            "post-P2r residual release_repeat_contract debt",
        )
        generated = quarantine.get("generated_product_workflow_names") or []
        require(len(generated) == 10, "post-P2r product workflow name count")
        require(
            quarantine.get("generated_operator_job") == "operator_validation_tests",
            "post-P2r operator job missing",
        )
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
        "vendor/turso_core/Cargo.toml" in text,
        "root Cargo comment omits vendored Turso tests",
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
    diagnostic_passed = runpy.run_path(str(GENERATOR))["sql_timing_diagnostic_log_passed"]
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

    manifests, core_source, sdk_source = vendor_dependency_sources()
    validate_vendor_dependency_invariants(manifests, core_source, sdk_source)
    for label in ("extra_ignore", "missing_send_guard", "missing_antithesis_cfg", "missing_assertion_macros", "missing_send_assertions"):
        changed = copy.deepcopy(manifests)
        changed_core, changed_sdk = core_source, sdk_source
        if label == "extra_ignore":
            changed["vendor/turso_sdk_kit/Cargo.toml"]["package"]["metadata"]["cargo-machete"]["ignored"].append("unused")
        elif label == "missing_send_guard":
            changed["vendor/turso_sdk_kit/Cargo.toml"]["dependencies"]["parking_lot"]["features"] = []
        elif label == "missing_antithesis_cfg":
            changed["vendor/turso_core/Cargo.toml"]["target"].pop("cfg(antithesis)")
        elif label == "missing_assertion_macros":
            changed_core = ""
        else:
            changed_sdk = ""
        try:
            validate_vendor_dependency_invariants(changed, changed_core, changed_sdk)
        except PolicyError:
            pass
        else:
            raise PolicyError(f"vendor ignore invariant fixture passed: {label}")


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
            print(f"storage remediation policy: {debt_count} assigned product debt rows (report-only; not closure)")
        else:
            print("storage remediation policy: zero product debt; product closure enabled")
        count = len(document["external_dependency_observations"]["findings"])
        print(f"external dependency observations: {count}; not qualified by product closure")
        return 0
    except (PolicyError, PlacementError, json.JSONDecodeError, KeyError, TypeError) as error:
        print(f"storage remediation policy failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
