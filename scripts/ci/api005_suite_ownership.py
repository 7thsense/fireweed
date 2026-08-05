#!/usr/bin/env python3
"""Provider-neutral API-005 suite ownership map (P4).

Discovers Fireweed public methods dynamically from the concrete facade and
registers each suite cell/profile/variant/method with exactly one semantic
owner and one test ID. Never encodes a fixed method count.

Governing boundary (bead fireweed-3557be90 / plan-key P4):
  - method_contracts come from discovery (facade inherent methods)
  - P6 owns queries / projection-control
  - P7 owns lifecycle (append/claim/finalize/queue create)
  - P8 owns mutations / maintenance
  - P9 owns commits
  - P5 / P5a own reopen scenarios
  - shared/source-neutral fixtures must not use positive Garage provider IDs
  - no live S3 provenance claim in this suite (P4s owns attested positives)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FACADE = ROOT / "crates/fireweed/src/facade.rs"
MANIFEST = ROOT / "docs/helix/04-build/storage-authority-manifest.json"
SUITE_SUPPORT = ROOT / "crates/fireweed/tests/support/public_interface.rs"
SUITE_LOCAL = ROOT / "crates/fireweed/tests/public_interface_conformance.rs"
SUITE_EXTERNAL = ROOT / "crates/fireweed/tests/public_interface_external_conformance.rs"
OWNERSHIP_MAP = ROOT / "docs/helix/04-build/api005-suite-ownership-map.json"

# Family → semantic owner. Method membership is discovered; families are fixed
# policy from the storage-closure plan, not a method count.
FAMILY_OWNERS: dict[str, str] = {
    "queue_and_ownership": "P6",
    "append_and_replace": "P7",
    "claim": "P7",
    "finalize": "P7",
    "commit": "P9",
    "read_and_discovery": "P6",
    "metrics_and_projection_query": "P6",
    "mutation_and_maintenance": "P8",
    "projection_control": "P6",
}

# Prefix/exact rules map a discovered method to a family. Order matters: first match wins.
FAMILY_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("projection_control", re.compile(r"^(projection_control|capabilities|verify|delete|rebuild)$")),
    ("commit", re.compile(r"^(commit|commit_multi_claim|commit_capabilities|explain_commit|side_record)$")),
    (
        "mutation_and_maintenance",
        re.compile(
            r"^(renew|reassign|update_fields|batch_update|mutate_items|update|set_gates|"
            r"reclaim_expired|reclaim_expired_at|purge|bounded_mutation|upsert)$"
        ),
    ),
    (
        "finalize",
        re.compile(
            r"^(ack|complete|nack|retry|release|nack_retry_after|retry_after|fail|"
            r"rearm|rearm_at|rearm_after)$"
        ),
    ),
    (
        "claim",
        re.compile(
            r"^(claim|claim_with|claim_response_with|claim_at|claim_response_at|"
            r"claim_across_queues|claim_by_query|claim_by_query_at|claim_by_item_ids)$"
        ),
    ),
    (
        "append_and_replace",
        re.compile(r"^(push|push_with_request_id|push_batch|push_batch_with_request_id)$"),
    ),
    (
        "queue_and_ownership",
        re.compile(r"^(ownership|renew_owned|create_queue|queue_definition|ensure_queue)$"),
    ),
    (
        "metrics_and_projection_query",
        re.compile(
            r"^(metrics|metrics_by_query|hot_projection_capabilities|range_scan|"
            r"grouped_aggregate|declared_bucket_segment)$"
        ),
    ),
    (
        "read_and_discovery",
        re.compile(
            r"^(peek|current_position|discover_active_scopes|discover_active_scopes_stamped|"
            r"discover|live_item|live_items|query_index_unique|query_index|"
            r"query_index_unique_typed|query_index_typed|claimed)$"
        ),
    ),
]

# Aliases used inside the shared suite for nested projection-control verbs.
METHOD_ALIASES: dict[str, str] = {
    "projection.verify": "verify",
    "projection.delete": "delete",
    "projection.rebuild": "rebuild",
}

# Positive Garage brand strings forbidden in shared/source-neutral suite fixtures.
FORBIDDEN_GARAGE = re.compile(r"\bgarage\b", re.IGNORECASE)

# Paths that may retain Garage only as unsupported/denylist/negative residual.
GARAGE_ALLOW_PATHS = {
    # P15 weak-credential denylist residual is outside this suite.
    "crates/fireweed-bench/src/performance_matrix_services.rs",
    "crates/fireweed-server/tests/performance_object_log_e3_live_tests.rs",
    "crates/fireweed-server/tests/production_s3_object_log_config.rs",
}

SUITE_SOURCES = (SUITE_SUPPORT, SUITE_LOCAL, SUITE_EXTERNAL)

# Match test functions regardless of whether #[ignore] precedes or follows #[test].
TEST_FN_RE = re.compile(
    r"^#\[(?:tokio::)?test[^\]]*\]\s*(?:\n#\[[^\]]+\]\s*)*(?:async\s+)?fn\s+([a-zA-Z0-9_]+)",
    re.MULTILINE,
)
CELL_LITERAL_RE = re.compile(
    r'(?:assert_cell|public_interface::run(?:_with_commit_boundary)?|run_s3_sqlite|run_postgres_runtime|'
    r'run_sync_constructor|seed_reopen_probe)\(\s*"([^"]+)"'
)
CELL_ID_RE = re.compile(r"^[a-z0-9]+(?:--[a-z0-9]+)+(?:--[a-z0-9-]+)?$")
METHOD_FN_RE = re.compile(r"^    pub (?:async )?fn ([a-zA-Z0-9_]+)")
# Coverage sites: call(cell, "method"), check(cell, "method"), fw.method(, control.method(
COVERAGE_SITE_RE = re.compile(
    r'(?:call|check)\(\s*[a-z_]+,\s*"([a-zA-Z0-9_\[\]\.-]+)"'
    r"|\bfw\.([a-zA-Z0-9_]+)\s*\("
    r"|\bfireweed\.([a-zA-Z0-9_]+)\s*\("
    r"|\bcontrol\.([a-zA-Z0-9_]+)\s*\("
)


class OwnershipError(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise OwnershipError(message)


def discover_methods(facade_text: str) -> list[str]:
    """Discovery-derived method_contracts: public inherent methods on Fireweed."""
    methods: list[str] = []
    in_impl = False
    for line in facade_text.splitlines():
        if re.match(r"^impl Fireweed \{", line):
            in_impl = True
            continue
        if in_impl and line.startswith("}"):
            # End of inherent impl block (nested braces are indented).
            if line.strip() == "}":
                in_impl = False
            continue
        if not in_impl:
            continue
        match = METHOD_FN_RE.match(line)
        if match:
            methods.append(match.group(1))
    # Unique preserve order
    seen: set[str] = set()
    ordered: list[str] = []
    for method in methods:
        if method not in seen:
            seen.add(method)
            ordered.append(method)
    require(ordered, "discovered zero Fireweed methods; facade layout drift")
    return ordered


def family_for(method: str) -> str:
    for family, pattern in FAMILY_RULES:
        if pattern.match(method):
            return family
    raise OwnershipError(f"no family rule for discovered method: {method}")


def owner_for_method(method: str) -> str:
    return FAMILY_OWNERS[family_for(method)]


def discover_suite_coverage(support_text: str) -> set[str]:
    covered: set[str] = set()
    for match in COVERAGE_SITE_RE.finditer(support_text):
        raw = next(g for g in match.groups() if g is not None)
        # Strip scenario suffixes like push[gates] → push
        base = raw.split("[", 1)[0]
        base = METHOD_ALIASES.get(base, base)
        if base in METHOD_ALIASES:
            base = METHOD_ALIASES[base]
        # Nested projection names already handled.
        covered.add(base)
    return covered


def discover_test_fns(source: Path) -> list[str]:
    text = source.read_text()
    return TEST_FN_RE.findall(text)


def discover_cell_literals(source: Path) -> list[str]:
    return CELL_LITERAL_RE.findall(source.read_text())


def load_manifest() -> dict[str, object]:
    return json.loads(MANIFEST.read_text())


def build_method_contracts(methods: list[str]) -> list[dict[str, str]]:
    rows = []
    for method in methods:
        family = family_for(method)
        rows.append(
            {
                "method": method,
                "family": family,
                "semantic_owner": FAMILY_OWNERS[family],
            }
        )
    return rows


def build_ownership_map(
    methods: list[str],
    *,
    local_tests: list[dict[str, str]],
    external_tests: list[dict[str, str]],
) -> dict[str, object]:
    """One owner/test ID per cell/profile/variant/method (+ reopen scenarios)."""
    entries: list[dict[str, str]] = []
    seen_keys: set[str] = set()

    def add(
        *,
        cell_id: str,
        profile: str,
        variant: str,
        method: str,
        owner: str,
        test_id: str,
        kind: str,
    ) -> None:
        key = f"{cell_id}|{profile}|{variant}|{method}|{kind}"
        require(key not in seen_keys, f"duplicate ownership key: {key}")
        seen_keys.add(key)
        entries.append(
            {
                "cell_id": cell_id,
                "profile": profile,
                "variant": variant,
                "method": method,
                "semantic_owner": owner,
                "test_id": test_id,
                "kind": kind,
            }
        )

    for registration in local_tests + external_tests:
        cell_id = registration["cell_id"]
        profile = registration["profile"]
        variant = registration["variant"]
        test_id = registration["test_id"]
        for method in methods:
            add(
                cell_id=cell_id,
                profile=profile,
                variant=variant,
                method=method,
                owner=owner_for_method(method),
                test_id=f"{test_id}::{method}",
                kind="method",
            )
        # Reopen is a scenario owned by P5 (Class B) / P5a (Class A durable log).
        reopen_owner = registration.get("reopen_owner", "P5a")
        add(
            cell_id=cell_id,
            profile=profile,
            variant=variant,
            method="reopen",
            owner=reopen_owner,
            test_id=f"{test_id}::reopen",
            kind="reopen",
        )

    return {
        "schema_version": 1,
        "generated_by": "scripts/ci/api005_suite_ownership.py",
        "plan_key": "P4",
        "spec_id": "storage-matrix-completion-brief",
        "fixed_method_count_forbidden": True,
        "method_contract_count": len(methods),
        "method_contracts": build_method_contracts(methods),
        "entries": entries,
    }


def parse_suite_registrations() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Discover registered suite cells from the local and external harnesses."""
    local: list[dict[str, str]] = []
    external: list[dict[str, str]] = []

    # Local conformance: map test fn → cell id via assert_cell("…")
    local_text = SUITE_LOCAL.read_text()
    for fn in discover_test_fns(SUITE_LOCAL):
        # Find the function body slice heuristically.
        m = re.search(rf"(?:async\s+)?fn\s+{re.escape(fn)}\s*\([^\)]*\)[^{{]*\{{", local_text)
        if not m:
            continue
        start = m.end()
        # crude body until next top-level test or EOF
        nxt = re.search(r"\n#\[(?:tokio::)?test", local_text[start:])
        body = local_text[start : start + nxt.start()] if nxt else local_text[start:]
        cells = re.findall(r'assert_cell(?:_async)?\(\s*"([^"]+)"', body)
        if not cells:
            # Authority validation test is not a matrix cell.
            continue
        cell_id = cells[0]
        require(
            CELL_ID_RE.match(cell_id),
            f"local suite cell id must use manifest separator form log--projection[…]: {cell_id}",
        )
        log = cell_id.split("--", 1)[0]
        reopen_owner = "P5" if log == "memory" else "P5a"
        local.append(
            {
                "cell_id": cell_id,
                "profile": "public_interface",
                "variant": cell_id.split("--")[-1] if cell_id.count("--") >= 2 else "default",
                "test_id": f"Cargo.toml::fireweed::public_interface_conformance::test::{fn}",
                "test_fn": fn,
                "reopen_owner": reopen_owner,
                "source": "local",
            }
        )

    external_text = SUITE_EXTERNAL.read_text()
    for fn in discover_test_fns(SUITE_EXTERNAL):
        m = re.search(rf"(?:async\s+)?fn\s+{re.escape(fn)}\s*\([^\)]*\)[^{{]*\{{", external_text)
        if not m:
            continue
        start = m.end()
        nxt = re.search(r"\n#\[(?:tokio::)?test", external_text[start:])
        body = external_text[start : start + nxt.start()] if nxt else external_text[start:]
        # Cell id may appear as string literal to run/assert helpers.
        candidates = re.findall(
            r'(?:public_interface::run(?:_with_commit_boundary)?|run_s3_sqlite|run_postgres_runtime|'
            r'run_sync_constructor|seed_reopen_probe)\(\s*"([^"]+)"',
            body,
        )
        if not candidates:
            # Some tests pass a local variable; recover from unique_name / FixtureRoot labels.
            labels = re.findall(r'(?:unique_name|FixtureRoot::new)\(\s*"([^"]+)"', body)
            if not labels:
                continue
            # Convert snake labels to cell ids if already using -- form elsewhere.
            continue
        cell_id = candidates[0]
        require(
            CELL_ID_RE.match(cell_id),
            f"external suite cell id must use manifest separator form: {cell_id}",
        )
        require(
            "garage" not in cell_id.lower(),
            f"positive Garage identity in external suite cell id: {cell_id}",
        )
        log = cell_id.split("--", 1)[0]
        reopen_owner = "P5" if log == "memory" else "P5a"
        external.append(
            {
                "cell_id": cell_id,
                "profile": "public_interface_external",
                "variant": cell_id.split("--")[-1] if cell_id.count("--") >= 2 else "default",
                "test_id": f"Cargo.toml::fireweed::public_interface_external_conformance::test::{fn}",
                "test_fn": fn,
                "reopen_owner": reopen_owner,
                "source": "external",
            }
        )

    return local, external


def assert_no_garage_in_suite() -> None:
    for path in SUITE_SOURCES:
        text = path.read_text()
        for lineno, line in enumerate(text.splitlines(), 1):
            if FORBIDDEN_GARAGE.search(line):
                # Allow comments that only document the forbid rule.
                if "forbid" in line.lower() or "provider-neutral" in line.lower():
                    continue
                raise OwnershipError(
                    f"positive Garage string in shared suite fixture {path.relative_to(ROOT)}:{lineno}: {line.strip()}"
                )


def assert_no_duplicate_lists(local: list[dict[str, str]], external: list[dict[str, str]]) -> None:
    """Forbid duplicate applicability/provenance registrations for the same cell/profile/variant."""
    keys: dict[str, str] = {}
    for row in local + external:
        key = f"{row['cell_id']}|{row['profile']}|{row['variant']}"
        if key in keys:
            raise OwnershipError(
                f"duplicate applicability registration for {key}: {keys[key]} and {row['test_fn']}"
            )
        keys[key] = row["test_fn"]


def assert_method_contracts_manifest(manifest: dict[str, object], methods: list[str]) -> None:
    contracts = manifest.get("method_contracts")
    require(isinstance(contracts, dict), "manifest missing method_contracts object")
    assert isinstance(contracts, dict)
    require(contracts.get("mode") == "dynamic", "method_contracts.mode must be dynamic")
    require(
        contracts.get("fixed_cardinality_forbidden") is True,
        "method_contracts must forbid fixed cardinality",
    )
    require(contracts.get("suite_owner") == "P4", "method_contracts.suite_owner must be P4")
    family_owners = contracts.get("family_owners")
    require(isinstance(family_owners, dict), "method_contracts.family_owners required")
    assert isinstance(family_owners, dict)
    require(
        set(family_owners) == set(FAMILY_OWNERS),
        f"method_contracts.family_owners drift: {sorted(family_owners)} vs {sorted(FAMILY_OWNERS)}",
    )
    for family, owner in FAMILY_OWNERS.items():
        require(
            family_owners.get(family) == owner,
            f"family owner drift for {family}: {family_owners.get(family)} != {owner}",
        )
    # Discovery roots must include facade.
    roots = contracts.get("discovery_roots")
    require(isinstance(roots, list) and roots, "method_contracts.discovery_roots required")
    assert isinstance(roots, list)
    require(
        any(str(r).endswith("facade.rs") for r in roots),
        "method_contracts.discovery_roots must include facade.rs",
    )
    # No fixed method count encoded in the manifest.
    require(
        "required_method_count" not in contracts and "method_count" not in contracts,
        "method_contracts must not encode a fixed method count",
    )
    require(len(methods) > 0, "discovery produced empty method_contracts")


def validate(document: dict[str, object], *, methods: list[str], covered: set[str]) -> None:
    require(document["schema_version"] == 1, "ownership map schema")
    require(document["fixed_method_count_forbidden"] is True, "fixed count flag")
    require(
        document["method_contract_count"] == len(methods),
        "method_contract_count must equal discovered methods",
    )
    contracts = document["method_contracts"]
    require(isinstance(contracts, list), "method_contracts list")
    assert isinstance(contracts, list)
    require(len(contracts) == len(methods), "method_contracts length drift")
    require(len(contracts) == len({c["method"] for c in contracts}), "duplicate method_contract")

    missing = sorted(set(methods) - covered)
    require(
        not missing,
        "shared suite does not exercise discovered methods: " + ", ".join(missing),
    )

    entries = document["entries"]
    require(isinstance(entries, list) and entries, "ownership entries required")
    assert isinstance(entries, list)
    keys = [f"{e['cell_id']}|{e['profile']}|{e['variant']}|{e['method']}|{e['kind']}" for e in entries]
    require(len(keys) == len(set(keys)), "duplicate ownership map entry")
    test_ids = [e["test_id"] for e in entries]
    require(len(test_ids) == len(set(test_ids)), "duplicate ownership test_id")

    for entry in entries:
        if entry["kind"] == "method":
            require(
                entry["semantic_owner"] == owner_for_method(entry["method"]),
                f"owner drift for {entry['method']}",
            )
        elif entry["kind"] == "reopen":
            require(entry["semantic_owner"] in {"P5", "P5a"}, f"reopen owner must be P5/P5a: {entry}")
        else:
            raise OwnershipError(f"unknown ownership kind: {entry['kind']}")


def generate() -> dict[str, object]:
    facade = FACADE.read_text()
    methods = discover_methods(facade)
    covered = discover_suite_coverage(SUITE_SUPPORT.read_text())
    # create_queue is invoked via helper create() which uses the method name in call labels.
    # Treat create_queue covered if any create_queue label exists.
    if any(c.startswith("create_queue") for c in covered) or "create_queue" in SUITE_SUPPORT.read_text():
        covered.add("create_queue")
    # projection_control methods via control handle
    for alias in ("capabilities", "verify", "delete", "rebuild", "projection_control"):
        if alias in covered or f"control.{alias}" in SUITE_SUPPORT.read_text() or f".{alias}(" in SUITE_SUPPORT.read_text():
            covered.add(alias)
    # ensure discover coverage for create_queue via create helper body
    if "create_queue" in SUITE_SUPPORT.read_text():
        covered.add("create_queue")

    local, external = parse_suite_registrations()
    require(local, "no local suite cell registrations discovered")
    require(external, "no external suite cell registrations discovered")
    assert_no_duplicate_lists(local, external)
    assert_no_garage_in_suite()

    manifest = load_manifest()
    assert_method_contracts_manifest(manifest, methods)

    document = build_ownership_map(methods, local_tests=local, external_tests=external)
    document["suite_registrations"] = {
        "local": local,
        "external": external,
    }
    document["covered_methods"] = sorted(covered & set(methods))
    validate(document, methods=methods, covered=covered)
    return document


def self_test(document: dict[str, object]) -> None:
    methods = [c["method"] for c in document["method_contracts"]]
    covered = set(document["covered_methods"])
    validate(document, methods=methods, covered=covered)

    # Negative: fixed count must not appear as an allowlist equality in policy.
    require(document["fixed_method_count_forbidden"] is True, "self-test fixed flag")

    # Negative: drop a method from coverage.
    broken = json.loads(json.dumps(document))
    if broken["covered_methods"]:
        broken["covered_methods"] = broken["covered_methods"][1:]
        try:
            validate(
                broken,
                methods=[c["method"] for c in broken["method_contracts"]],
                covered=set(broken["covered_methods"]),
            )
        except OwnershipError:
            pass
        else:
            raise OwnershipError("coverage gap negative fixture passed")

    # Negative: duplicate ownership key.
    dup = json.loads(json.dumps(document))
    dup["entries"].append(dict(dup["entries"][0]))
    try:
        validate(
            dup,
            methods=[c["method"] for c in dup["method_contracts"]],
            covered=set(dup["covered_methods"]),
        )
    except OwnershipError:
        pass
    else:
        raise OwnershipError("duplicate entry negative fixture passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit", action="store_true", help="print ownership map JSON")
    parser.add_argument("--write", action="store_true", help="write ownership map artifact")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        document = generate()
        if args.write:
            OWNERSHIP_MAP.parent.mkdir(parents=True, exist_ok=True)
            OWNERSHIP_MAP.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        if args.emit:
            print(json.dumps(document, indent=2, sort_keys=True))
        if args.self_test:
            self_test(document)
            print(
                f"API-005 suite ownership self-test passed "
                f"({document['method_contract_count']} methods, "
                f"{len(document['entries'])} ownership entries)"
            )
        elif not args.emit:
            print(
                f"API-005 suite ownership valid "
                f"({document['method_contract_count']} methods, "
                f"{len(document['suite_registrations']['local'])} local cells, "
                f"{len(document['suite_registrations']['external'])} external cells, "
                f"{len(document['entries'])} ownership entries)"
            )
        return 0
    except OwnershipError as error:
        print(f"API-005 suite ownership failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
