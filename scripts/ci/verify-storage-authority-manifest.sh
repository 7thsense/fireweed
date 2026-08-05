#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mode="${1:---check}"

case "${mode}" in
    --check|--self-test) ;;
    *)
        echo "usage: $0 [--check|--self-test]" >&2
        exit 2
        ;;
esac

cd "${repo_root}"
FIREWEED_AUTHORITY_VERIFY_MODE="${mode}" python3 - <<'PY'
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path.cwd()
MODE = os.environ["FIREWEED_AUTHORITY_VERIFY_MODE"]
MANIFEST_PATH = Path("docs/helix/04-build/storage-authority-manifest.json")
BUILD_PATH = Path("docs/helix/04-build/BUILD-001-implementation-plan.md")

EXPECTED_TOP_LEVEL = {
    "schema_version",
    "spec_id",
    "authority_revision",
    "governed_documents",
    "canonical_axes",
    "durability",
    "response_barriers",
    "validation_contract",
    "storage_authority",
    "configuration_surface",
    "help_parser_bijections",
    "delivery_contract",
    "error_vocabulary",
    "requirement_dispositions",
    "evidence_contract",
    "private_surface_discovery",
    "tracked_ignore_policy",
    "topology_attestation",
    "public_identity_classification",
    "route_binding_policy",
}

EXPECTED_GOVERNED = {
    "docs/helix/00-discover/product-vision.md",
    "docs/helix/01-frame/prd.md",
    "docs/helix/02-design/orthogonal-storage-matrix-brief.md",
    "docs/helix/02-design/adr/ADR-002-auth-tenancy-and-storage-isolation.md",
    "docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md",
    "docs/helix/02-design/adr/ADR-012-orthogonal-log-projection-composition.md",
    "docs/helix/02-design/adr/ADR-014-fjord-embedded-change-log-consumer-surface.md",
    "docs/helix/02-design/adr/ADR-015-full-async-storage-boundaries.md",
    "docs/helix/02-design/adr/ADR-016-turso-derived-projection.md",
    "docs/helix/02-design/adr/ADR-017-async-commit-strategy-and-dispatch.md",
    "docs/helix/02-design/adr/ADR-020-public-namespace-and-compatibility.md",
    "docs/helix/02-design/contracts/API-001-native-client-interface.md",
    "docs/helix/02-design/contracts/API-002-operator-repair-contract.md",
    "docs/helix/02-design/contracts/API-005-fireweed-rust-facade.md",
    "docs/helix/02-design/technical-designs/TD-001-storage-architecture-backend-contracts.md",
    "docs/helix/02-design/technical-designs/TD-003-sharding-and-shard-ownership.md",
    "docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md",
    "docs/helix/02-design/technical-designs/TD-006-resp-wire-adapter.md",
    "docs/helix/02-design/technical-designs/TD-008-queue-history-change-records.md",
    "docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md",
    "docs/helix/03-test/test-plans/TP-001-governing-test-traceability.md",
    "docs/helix/03-test/test-plans/TP-002-scale-substantiation.md",
    "docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md",
    "docs/helix/03-test/test-plans/TP-004-fireweed-facade-and-snorri-acceptance.md",
    "docs/helix/03-test/test-plans/TP-005-fireweed-performance-matrix.md",
}

EXPECTED_DISPOSITIONS = {
    "TD004-LEGACY-DURABLE-MANIFEST-REOPEN",
    "TD004-LEGACY-CURRENT-EPOCH-FENCING",
    "TD004-LEGACY-MISSING-CAS-REJECTION",
    "TD004-LEGACY-POSTGRES-MANIFEST-POINTER",
    "TD004-LEGACY-ONE-OBJECT-PER-COMMAND",
    "AC-TURSO-1",
    "AC-TURSO-2",
    "AC-TURSO-3",
    "AC-TURSO-4",
    "AC-TURSO-5.ENABLED",
    "AC-TURSO-5.DISABLED",
    "AC-TURSO-6",
    "AC-TXN-5-HYBRID-SELECTOR",
    "AC-TXN-5A-HYBRID-SELECTOR",
    "LEGACY-PUBLIC-PROJECTION-SELECTORS",
    "LEGACY-PUBLIC-LOG-SELECTOR-OBJECTLOG",
    "LEGACY-HYBRID-ASYNC-THRESHOLDS",
    "LEGACY-POSTGRES-PUBLICATION-AUTHORITY",
    "TP004-GARAGE-REOPEN",
    "TP004-GARAGE-PROJECTION-REBUILD",
    "TP004-GARAGE-RETRY-IDEMPOTENCY",
    "TP004-ELDIR-HOST",
    "TP004-GARAGE-PROVIDER",
    "LEGACY-HYBRID-EVIDENCE",
    "TD008-CLASS-B-HISTORY",
    "SERVER-PROJECTION-HELP-PARSER-MISMATCH",
    "SERVER-LOG-HELP-PARSER-MISMATCH",
    "OPERATOR-VALIDATION-CAMPAIGN-BINDING",
    "PRIVATE-SURFACE-FIXED-COUNT",
    "TRACKED-IGNORE-LOCAL-EXCLUDE-AUTHORITY",
}

DISPOSITION_KEYS = {
    "id",
    "disposition",
    "current_requirement_ids",
    "semantic_owners",
    "qualifies_current_product",
}

FORBIDDEN_ROUTE_KEYS = {
    "routes",
    "route_id",
    "executable_id",
    "command",
    "commands",
    "test_binary",
    "test_filter",
}

CONCRETE_ROUTE_PATTERN = re.compile(
    r"(?:^|\s)cargo\s+(?:test|nextest)|--test\s+|#\[test\]|"
    r"[A-Za-z0-9_]+::[A-Za-z0-9_]+|scripts/[A-Za-z0-9_./-]+[.]sh"
)


class ContractError(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def exact_keys(value: object, expected: set[str], label: str) -> None:
    require(isinstance(value, dict), f"{label} must be an object")
    actual = set(value)
    require(actual == expected, f"{label} keys {sorted(actual)} != {sorted(expected)}")


def unique_strings(value: object, label: str, *, nonempty: bool = True) -> list[str]:
    require(isinstance(value, list), f"{label} must be an array")
    require(all(isinstance(item, str) and item for item in value), f"{label} entries")
    require(len(value) == len(set(value)), f"{label} contains duplicates")
    if nonempty:
        require(bool(value), f"{label} must not be empty")
    return value


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_tracked(path: str) -> bool:
    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", path],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def walk_for_routes(value: object, label: str = "manifest") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            require(key not in FORBIDDEN_ROUTE_KEYS, f"{label} embeds route key {key}")
            walk_for_routes(child, f"{label}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            walk_for_routes(child, f"{label}[{index}]")
    elif isinstance(value, str):
        require(
            CONCRETE_ROUTE_PATTERN.search(value) is None,
            f"{label} embeds concrete executable route {value!r}",
        )


def expected_historical_paths() -> set[str]:
    identity = json.loads(Path("scripts/public-identity-allowlist.json").read_text())
    historical = next(
        entry
        for entry in identity["entries"]
        if entry["id"] == "pre-fireweed-performance-evidence"
    )
    baseline = json.loads(
        Path("docs/helix/04-build/evidence-source-io-baseline.json").read_text()
    )
    return set(historical["paths"]) | set(baseline["tracked_tp003_paths"])


def validate_document(document: object, *, check_repository: bool) -> None:
    exact_keys(document, EXPECTED_TOP_LEVEL, "manifest")
    assert isinstance(document, dict)
    require(document["schema_version"] == 1, "schema_version must equal 1")
    require(
        document["spec_id"] == "storage-matrix-completion-brief",
        "spec_id drift",
    )
    require(
        isinstance(document["authority_revision"], str)
        and document["authority_revision"],
        "authority_revision",
    )
    walk_for_routes(document)

    governed = document["governed_documents"]
    require(isinstance(governed, list), "governed_documents must be an array")
    governed_paths: list[str] = []
    for index, entry in enumerate(governed):
        exact_keys(entry, {"path", "sha256"}, f"governed_documents[{index}]")
        path = entry["path"]
        digest = entry["sha256"]
        require(isinstance(path, str) and path, f"governed_documents[{index}].path")
        require(
            isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest),
            f"governed_documents[{index}].sha256",
        )
        governed_paths.append(path)
        if check_repository:
            resolved = Path(path)
            require(resolved.is_file(), f"governed document missing: {path}")
            require(sha256(resolved) == digest, f"governed document drift: {path}")
    require(len(governed_paths) == len(set(governed_paths)), "duplicate governed document")
    require(set(governed_paths) == EXPECTED_GOVERNED, "governed document set drift")

    axes = document["canonical_axes"]
    exact_keys(
        axes,
        {
            "logs",
            "projections",
            "control_planes",
            "cell_id_separator",
            "required_cell_count",
            "profile_skus_are_public",
        },
        "canonical_axes",
    )
    logs = unique_strings(axes["logs"], "canonical_axes.logs")
    projections = unique_strings(axes["projections"], "canonical_axes.projections")
    require(logs == ["memory", "sqlite", "postgres", "filesystem", "s3"], "log axis drift")
    require(
        projections == ["memory", "sqlite", "turso", "postgres"],
        "projection axis drift",
    )
    require(
        axes["control_planes"] == ["in_process", "postgres"],
        "control-plane axis drift",
    )
    require(axes["cell_id_separator"] == "--", "cell separator drift")
    cells = {f"{log}--{projection}" for log in logs for projection in projections}
    require(len(cells) == axes["required_cell_count"] == 20, "matrix must contain 20 cells")
    require(axes["profile_skus_are_public"] is False, "profile SKUs cannot be public")

    durability = document["durability"]
    require(durability["class_a_logs"] == logs[1:], "Class A log set drift")
    require(durability["class_b_logs"] == ["memory"], "Class B log set drift")
    require(durability["class_a_cell_count"] == 16, "Class A cell count drift")
    require(durability["class_b_cell_count"] == 4, "Class B cell count drift")
    require(durability["history_requires_class_a"] is True, "history must require Class A")
    require(durability["silent_null_log_forbidden"] is True, "silent null log forbidden")

    barriers = document["response_barriers"]
    strict = barriers["strict"]
    async_projection = barriers["async_projection"]
    deferred = barriers["sqlite_projection_deferred_flush"]
    require(strict["applicable_logs"] == logs, "Strict applicability drift")
    require(strict["required_cell_count"] == 20, "Strict count drift")
    require(
        async_projection["applicable_logs"] == ["filesystem", "s3"],
        "AsyncProjection applicability drift",
    )
    require(async_projection["required_positive_cell_count"] == 8, "async positive count")
    require(async_projection["required_pre_io_rejection_count"] == 12, "async rejection count")
    require(
        len(unique_strings(async_projection["bounds"], "AsyncProjectionSpec.bounds")) == 5,
        "AsyncProjectionSpec must have five bounds",
    )
    require(async_projection["all_bounds_positive"] is True, "async bounds must be positive")
    require(deferred["applicable_projections"] == ["sqlite"], "deferred projection scope")
    require(
        deferred["applicable_barriers"] == ["Strict", "AsyncProjection"],
        "deferred barrier scope",
    )
    require(deferred["part_of_async_projection_spec"] is False, "deferred field must be separate")

    validation = document["validation_contract"]
    require(validation["before_storage_io"] is True, "validation must precede I/O")
    require(
        validation["precedence"]
        == [
            "endpoint_syntax",
            "response_barrier",
            "tuple_coherence",
            "feature_availability",
            "durability_and_provider_capability",
        ],
        "validation precedence drift",
    )
    require(
        validation["transitional_engine_error_reasons"]
        == [
            "objectlog-memory-async-pending",
            "legacy-projection-change-record-delivery-retired",
        ],
        "transitional rejection registry drift",
    )

    authority = document["storage_authority"]
    require(authority["object_log_publication"] == "NativeConditionalWrite", "authority drift")
    require(authority["postgres_manifest_pointer_fallback"] == "retired", "fallback revived")
    require(authority["projection_is_never_log_authority"] is True, "projection authority drift")
    require(
        authority["provider_without_native_conditional_write"] == "reject_before_io",
        "missing-CAS behavior drift",
    )

    config = document["configuration_surface"]
    require(config["default_projection"] == "turso", "default projection drift")
    turso = config["turso_supported_boundary"]
    require(turso["version"] == "0.7.0", "Turso version boundary drift")
    require(
        turso["mode"] == "embedded_local_ordinary_wal",
        "Turso supported mode drift",
    )
    require(
        turso["unsupported_modes"] == ["remote", "sync", "embedded_replica", "mvcc"],
        "Turso unsupported mode drift",
    )
    require(turso["sqlite_is_differential_reference"] is True, "SQLite reference drift")
    require(
        len(unique_strings(config["async_projection_environment_keys"], "async env keys")) == 5,
        "five async environment keys required",
    )
    require(config["retired_public_type"] == "HybridAsyncThresholds", "retired type drift")
    require(config["legacy_environment_keys_are_aliases"] is False, "legacy env aliases forbidden")

    bijections = document["help_parser_bijections"]
    for axis_name, expected in (("log", logs), ("projection", projections)):
        axis = bijections[axis_name]
        require(axis["help_values"] == expected, f"{axis_name} help drift")
        require(axis["parser_values"] == expected, f"{axis_name} parser drift")
    rejected = set(bijections["projection"]["rejected_legacy_values"])
    require(
        rejected == {"inmemory", "hybrid", "hybrid-strict", "hybrid-async"},
        "projection legacy rejection drift",
    )
    require(
        bijections["log"]["rejected_legacy_values"] == ["objectlog"],
        "log legacy rejection drift",
    )

    delivery = document["delivery_contract"]
    require(delivery["modes"] == ["Disabled", "Embedded", "ExternalKafka", "Http"], "delivery modes")
    require(delivery["enabled_modes"] == delivery["modes"][1:], "enabled delivery modes")
    formula = delivery["positive_profile_formula"]
    require(formula["sqlite_or_postgres_log_strict_enabled"] == 8 * 1 * 3, "relational profile count")
    require(formula["filesystem_or_s3_log_both_barriers_enabled"] == 8 * 2 * 3, "object profile count")
    require(formula["total"] == 72, "TD-008 positive profile count")
    require(delivery["class_b_strict_enabled_durability_negatives"] == 12, "Class B negatives")
    require(delivery["class_b_strict_disabled_positives"] == 4, "Class B disabled positives")

    errors = document["error_vocabulary"]
    require(errors["new_startup_only_engine_error"] == "ChangeRecordsRequireDurableLog", "new error")
    require(
        errors["new_resp_token"] == "-ERR fireweed change_records_require_durable_log",
        "new RESP token",
    )
    require(errors["startup_only_error_may_escape_commit"] is False, "startup error commit leak")
    require(
        errors["existing_direct_resp_tokens"]
        == {"QueueDefinitionConflict": "-ERR fireweed queue_conflict"},
        "existing direct RESP token drift",
    )

    dispositions = document["requirement_dispositions"]
    require(isinstance(dispositions, list), "requirement_dispositions must be an array")
    disposition_ids: list[str] = []
    for index, entry in enumerate(dispositions):
        exact_keys(entry, DISPOSITION_KEYS, f"requirement_dispositions[{index}]")
        requirement_id = entry["id"]
        require(isinstance(requirement_id, str) and requirement_id, f"disposition[{index}].id")
        disposition_ids.append(requirement_id)
        current = unique_strings(
            entry["current_requirement_ids"],
            f"disposition[{index}].current_requirement_ids",
            nonempty=False,
        )
        unique_strings(
            entry["semantic_owners"],
            f"disposition[{index}].semantic_owners",
            nonempty=False,
        )
        state = entry["disposition"]
        require(
            state in {"retained", "replaced", "retired", "internal", "historical", "negative"},
            f"unknown disposition {state}",
        )
        require(isinstance(entry["qualifies_current_product"], bool), "qualification flag")
        if state in {"retired", "historical"}:
            require(not current, f"{requirement_id} is retired/historical but claims current binding")
            require(
                entry["qualifies_current_product"] is False,
                f"{requirement_id} is retired/historical but qualifies current product",
            )
        else:
            require(current, f"{requirement_id} lacks current semantic binding")
        if state == "internal":
            require(
                entry["qualifies_current_product"] is False,
                f"{requirement_id} internal evidence cannot qualify product",
            )
    require(len(disposition_ids) == len(set(disposition_ids)), "duplicate P1 disposition")
    require(set(disposition_ids) == EXPECTED_DISPOSITIONS, "missing or extra P1 disposition")

    evidence = document["evidence_contract"]
    require(evidence["artifact_classes"] == ["Fixture", "RunOwned", "Promoted"], "artifact classes")
    require(
        len(unique_strings(evidence["semantic_current_ids"], "current evidence IDs")) == 4,
        "current evidence IDs must be distinct",
    )
    require(evidence["wall_clock_threshold_owner"] == "TP-002-E3", "threshold ownership")
    require(evidence["observational_performance_owner"] == "TP-005", "TP-005 ownership")
    historical_paths = unique_strings(evidence["historical_paths"], "historical paths")
    require(set(historical_paths) == expected_historical_paths(), "historical corpus drift")
    require(evidence["historical_files_may_qualify_current"] is False, "history cannot qualify")
    require(evidence["silent_skip_allowed"] is False, "silent skips forbidden")
    if check_repository:
        for path in historical_paths:
            require(Path(path).is_file(), f"historical evidence missing: {path}")
            require(git_tracked(path), f"historical evidence not tracked: {path}")
        companion = evidence["historical_provenance_companion"]
        require(Path(companion).is_file(), f"historical companion missing: {companion}")

    private = document["private_surface_discovery"]
    require(private["mode"] == "dynamic", "private surface must be dynamic")
    require(private["lower_bounds_are_allowlist"] is False, "private lower bounds cannot be allowlist")
    require(private["every_discovered_reference_requires_binding"] is True, "private binding coverage")
    require(private["fixed_cardinality_forbidden"] is True, "private fixed cardinality forbidden")
    if check_repository:
        for root in private["roots"]:
            require(Path(root).is_dir(), f"private discovery root missing: {root}")

    ignore = document["tracked_ignore_policy"]
    require(ignore["authority"] == "tracked_gitignore_only", "ignore authority drift")
    require(
        ignore["local_or_global_excludes_have_policy_authority"] is False,
        "local/global excludes cannot be policy authority",
    )
    require(ignore["forbidden_in_repository_paths"] == [".env.garage-e3"], "forbidden path drift")
    require(ignore["classes_are_disjoint"] is True, "ignore classes must be disjoint")
    administrative = set(ignore["classes"]["administrative"]["roots"])
    caches = set(ignore["classes"]["build_dependency_cache"]["roots"])
    require(administrative.isdisjoint(caches), "ignored root classes overlap")
    if check_repository:
        for path in ignore["forbidden_in_repository_paths"]:
            require(not Path(path).exists(), f"forbidden in-repository path exists: {path}")

    topology = document["topology_attestation"]
    require(topology["provider_brand_is_contractual"] is False, "provider brand cannot be contractual")
    require(topology["host_name_is_contractual"] is False, "host name cannot be contractual")
    require(topology["missing_live_s3_or_postgres"] == "qualification_failure", "live service fail-open")
    require("native_atomic_conditional_create" in topology["s3_fields"], "S3 create CAS attestation")
    require("native_atomic_conditional_update" in topology["s3_fields"], "S3 update CAS attestation")

    identity = document["public_identity_classification"]
    paths = [entry["path"] for entry in identity]
    require(len(paths) == len(set(paths)), "duplicate public identity classification")
    if check_repository:
        for entry in identity:
            if entry["state"] != "future_current_source":
                require(Path(entry["path"]).is_file(), f"classified path missing: {entry['path']}")

    route_policy = document["route_binding_policy"]
    require(route_policy["concrete_executable_routes_present"] is False, "route leakage declared")
    require(route_policy["semantic_requirements_are_route_independent"] is True, "route independence")
    require(route_policy["route_overlay_owner"] == "P2r", "route overlay owner drift")
    require(route_policy["applicable_requirement_binding_cardinality"] == "exactly_one", "route cardinality")

    if check_repository:
        build = BUILD_PATH.read_text()
        for marker in (
            "## Canonical storage authority manifest",
            "storage-authority-manifest.json",
            "bash scripts/ci/verify-storage-authority-manifest.sh",
            "not test binaries, filters, commands, or other concrete executable route IDs",
        ):
            require(marker in build, f"BUILD-001 authority marker missing: {marker}")


def expect_rejection(callback, label: str) -> None:
    try:
        callback()
    except (ContractError, json.JSONDecodeError, KeyError, TypeError):
        print(f"self-test rejected {label}")
        return
    raise ContractError(f"negative fixture unexpectedly passed: {label}")


try:
    manifest = json.loads(MANIFEST_PATH.read_text())
    validate_document(manifest, check_repository=True)

    if MODE == "--self-test":
        expect_rejection(lambda: json.loads('{"schema_version":'), "malformed manifest")

        duplicate = copy.deepcopy(manifest)
        duplicate["requirement_dispositions"].append(
            copy.deepcopy(duplicate["requirement_dispositions"][0])
        )
        expect_rejection(
            lambda: validate_document(duplicate, check_repository=False),
            "duplicate disposition",
        )

        missing = copy.deepcopy(manifest)
        missing["requirement_dispositions"] = missing["requirement_dispositions"][:-1]
        expect_rejection(
            lambda: validate_document(missing, check_repository=False),
            "missing disposition",
        )

        retired_current = copy.deepcopy(manifest)
        retired = next(
            entry
            for entry in retired_current["requirement_dispositions"]
            if entry["disposition"] == "retired"
        )
        retired["current_requirement_ids"] = ["ILLEGAL-CURRENT-BINDING"]
        retired["qualifies_current_product"] = True
        expect_rejection(
            lambda: validate_document(retired_current, check_repository=False),
            "retired requirement presented as current",
        )

        turso_rejected = copy.deepcopy(manifest)
        turso_rejected["help_parser_bijections"]["projection"][
            "rejected_legacy_values"
        ].append("turso")
        expect_rejection(
            lambda: validate_document(turso_rejected, check_repository=False),
            "Turso classified as a rejected selector",
        )

        turso_not_default = copy.deepcopy(manifest)
        turso_not_default["configuration_surface"]["default_projection"] = "sqlite"
        expect_rejection(
            lambda: validate_document(turso_not_default, check_repository=False),
            "Turso removed as default projection",
        )

    print("storage authority manifest verified")
except (ContractError, json.JSONDecodeError, KeyError, TypeError) as error:
    print(f"storage authority manifest verification failed: {error}", file=sys.stderr)
    raise SystemExit(1)
PY
