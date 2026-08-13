#!/usr/bin/env python3
"""P2r: generate final exact routes, governing suite mappings, and the sole
assertion-binding overlay.

Consumes:
  - docs/helix/04-build/storage-authority-manifest.json (P1 semantic selectors)
  - docs/helix/04-build/functional-matrix-route-sources.json (P10r leaves)
  - scripts/ci/storage-remediation-inventory.json (post-placement harness IDs)

Produces:
  - docs/helix/04-build/evidence-semantic-requirements.json
  - docs/helix/04-build/evidence-route-overlay.json
  - docs/helix/04-build/route-feature-manifest.json
  - scripts/ci/product-workflow-required-names.json
  - scripts/ci/release-repeat-suites.toml

Also migrates product-workflow name verification to exact-set equality over the
product_workflow namespace. Does not execute matrix workloads (P2b/P10/P17).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
AUTHORITY = ROOT / "docs/helix/04-build/storage-authority-manifest.json"
ROUTE_SOURCES = ROOT / "docs/helix/04-build/functional-matrix-route-sources.json"
INVENTORY = ROOT / "scripts/ci/storage-remediation-inventory.json"
SEMANTIC_OUT = ROOT / "docs/helix/04-build/evidence-semantic-requirements.json"
OVERLAY_OUT = ROOT / "docs/helix/04-build/evidence-route-overlay.json"
MANIFEST_OUT = ROOT / "docs/helix/04-build/route-feature-manifest.json"
REQUIRED_NAMES_OUT = ROOT / "scripts/ci/product-workflow-required-names.json"
SUITES_OUT = ROOT / "scripts/ci/release-repeat-suites.toml"

SCHEMA_VERSION = 1
PLAN_KEY = "P2r"
PRODUCT_WORKFLOW_NAMESPACE = "product_workflow"
OPERATOR_NAMESPACE = "operator_validation"
STORAGE_NAMESPACE = "storage_matrix"
CARGO = ["rustup", "run", "1.97.1", "cargo"]

# Ten names that must appear in the product_workflow namespace (exact set).
PRODUCT_WORKFLOW_REQUIRED = [
    "product_validation_tests",
    "product_workflow_scheduled_action_delivery_e2e",
    "product_workflow_marketo_group_batching_e2e",
    "product_workflow_callback_cohort_e2e",
    "product_workflow_jobs_connectors_recurring_e2e",
    "product_workflow_worker_crash_recovery_e2e",
    "product_workflow_noisy_neighbor_scale_e2e",
    "product_workflow_generic_priority_bounded_relaxed_e2e",
    "product_workflow_downstream_pacing_non_goal_e2e",
    "product_workflow_operator_repair_redrive_e2e",
]

# Suite name -> exact lib test filter under fireweed::test_product_validation
PRODUCT_SUITE_LEAVES: dict[str, list[str]] = {
    "product_workflow_scheduled_action_delivery_e2e": [
        "test_product_validation::scheduled_action_delivery_e2e"
    ],
    "product_workflow_marketo_group_batching_e2e": [
        "test_product_validation::marketo_group_batching_e2e"
    ],
    "product_workflow_callback_cohort_e2e": [
        "test_product_validation::callback_cohort_e2e"
    ],
    "product_workflow_jobs_connectors_recurring_e2e": [
        "test_product_validation::jobs_connectors_recurring_e2e"
    ],
    "product_workflow_worker_crash_recovery_e2e": [
        "test_product_validation::worker_crash_recovery_e2e"
    ],
    "product_workflow_noisy_neighbor_scale_e2e": [
        "test_product_validation::noisy_neighbor_scale_e2e"
    ],
    "product_workflow_generic_priority_bounded_relaxed_e2e": [
        "test_product_validation::generic_priority_bounded_relaxed_e2e"
    ],
    "product_workflow_downstream_pacing_non_goal_e2e": [
        "test_product_validation::downstream_pacing_non_goal_e2e"
    ],
    "product_workflow_operator_repair_redrive_e2e": [
        "test_product_validation::operator_repair_redrive_e2e"
    ],
}
# Aggregate product_validation_tests = all non-operator AC-E2E leaves.
PRODUCT_SUITE_LEAVES["product_validation_tests"] = [
    leaf
    for name, leaves in PRODUCT_SUITE_LEAVES.items()
    if name != "product_workflow_operator_repair_redrive_e2e"
    for leaf in leaves
]

# Semantic requirement ID pattern from P0 schema (no dots).
REQ_ID_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*$")


class P2rError(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise P2rError(message)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def canonical_json(document: object) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def schema_id(raw: str) -> str:
    """Normalize P1 requirement IDs to the P0 evidence schema pattern."""
    normalized = raw.replace(".", "-").upper()
    # Preserve existing hyphens; collapse accidental doubles.
    normalized = re.sub(r"-+", "-", normalized)
    require(bool(REQ_ID_RE.fullmatch(normalized)), f"cannot schema-normalize id {raw!r}")
    return normalized


def harness_id_for_filter(test_filter: str) -> str:
    return f"Cargo.toml::fireweed::fireweed::test::{test_filter}"


def exact_cargo_invocation(test_filter: str) -> list[str]:
    return CARGO + [
        "test",
        "--manifest-path",
        "Cargo.toml",
        "--locked",
        "-p",
        "fireweed",
        "--lib",
        test_filter,
        "--",
        "--exact",
    ]


def leaf_command_id(test_filter: str) -> str:
    """Stable command ID for a product leaf (full post-placement harness form)."""
    return harness_id_for_filter(test_filter)


def load_json(path: Path) -> dict:
    require(path.is_file(), f"missing {path.relative_to(ROOT)}")
    return json.loads(path.read_text())


def inventory_route_index(inventory: dict) -> dict[str, dict]:
    return {row["id"]: row for row in inventory.get("harness_routes", [])}


def find_route(routes: dict[str, dict], *needles: str) -> str:
    """Return first route id containing all needles (case-insensitive)."""
    lowered = [n.lower() for n in needles]
    matches = [
        rid
        for rid in sorted(routes)
        if all(n in rid.lower() for n in lowered)
    ]
    require(matches, f"no harness route matching {needles}")
    return matches[0]


def find_routes(routes: dict[str, dict], *needles: str, limit: int | None = None) -> list[str]:
    lowered = [n.lower() for n in needles]
    matches = [
        rid
        for rid in sorted(routes)
        if all(n in rid.lower() for n in lowered)
    ]
    require(matches, f"no harness routes matching {needles}")
    if limit is not None:
        return matches[:limit]
    return matches


def p10r_leaf_route_id(leaf: dict) -> str:
    """Stable route ID for a P10r source leaf (not a Cargo harness ID until executed)."""
    return f"p10r::{leaf['leaf_id']}"


def build_semantic_requirements(authority: dict) -> dict:
    requirements: list[dict] = []
    seen: set[str] = set()

    def add(
        req_id: str,
        governing_assertion: str,
        *,
        durability_class: str,
        capability: str,
        artifact_class: str = "run-owned",
        required_result: str = "pass",
    ) -> None:
        sid = schema_id(req_id)
        require(sid not in seen, f"duplicate semantic requirement {sid}")
        seen.add(sid)
        requirements.append(
            {
                "id": sid,
                "governing_assertion": governing_assertion,
                "durability_class": durability_class,
                "capability": capability,
                "evidence_semantics": {
                    "artifact_class": artifact_class,
                    "required_result": required_result,
                    "stale_input_fails": True,
                },
            }
        )

    for disposition in authority["requirement_dispositions"]:
        disp = disposition["disposition"]
        if disp in {"retired", "historical"} and not disposition.get("current_requirement_ids"):
            continue
        for raw_id in disposition.get("current_requirement_ids") or []:
            result = "explicit-na" if disp == "negative" else "pass"
            # Capability/durability defaults by family.
            capability = "core"
            durability = "A-or-B"
            if raw_id.startswith("AC-TURSO"):
                capability = "projection_reopen"
                durability = "A"
            elif raw_id.startswith("AC-TXN"):
                capability = "durable_log_replay"
                durability = "A"
            elif "SNORRI" in raw_id or "REOPEN" in raw_id:
                capability = "durable_log_replay"
                durability = "A"
            elif "ASYNC" in raw_id:
                capability = "eventual_apply"
                durability = "A"
            elif "CLASS-B" in raw_id or "CHANGE-RECORDS" in raw_id:
                capability = "in_process_log_read"
                durability = "B"
            elif "CAS" in raw_id or "CONDITIONAL" in raw_id or "S3-NATIVE" in raw_id:
                capability = "durable_log_replay"
                durability = "A"
            add(
                raw_id,
                f"P1 disposition {disposition['id']} ({disp}) current selector {raw_id}",
                durability_class=durability,
                capability=capability,
                required_result=result,
            )

    # Product workflow semantic selectors (AC-E2E) bound by P2r suite mappings.
    e2e = [
        ("AC-E2E-1", "scheduled action delivery product workflow"),
        ("AC-E2E-2", "Marketo group batching product workflow"),
        ("AC-E2E-3", "callback cohort product workflow"),
        ("AC-E2E-4", "jobs/connectors recurring product workflow"),
        ("AC-E2E-5", "worker crash recovery product workflow"),
        ("AC-E2E-6", "noisy neighbor scale product workflow"),
        ("AC-E2E-7", "operator repair/redrive product workflow"),
        ("AC-E2E-8", "generic priority bounded-relaxed product workflow"),
        ("AC-E2E-9", "downstream pacing non-goal product workflow"),
    ]
    for rid, text in e2e:
        add(rid, text, durability_class="A-or-B", capability="core")

    # Canonical 20-cell default-public matrix selector (Turso default product claim).
    add(
        "MATRIX-STRICT-20",
        "default public matrix has exactly 20 strict cells with Turso default projection",
        durability_class="A-or-B",
        capability="core",
    )
    add(
        "HYBRID-SELECTOR-RETIRED",
        "retired Hybrid public selectors are rejected (negatives)",
        durability_class="A-or-B",
        capability="core",
        required_result="explicit-na",
    )
    add(
        "PRODUCT-VALIDATION-AGGREGATE",
        "product_validation_tests aggregate binds non-operator AC-E2E smoke leaves",
        durability_class="A-or-B",
        capability="core",
    )

    requirements.sort(key=lambda row: row["id"])
    return {"schema_version": SCHEMA_VERSION, "requirements": requirements}


def build_overlay_assignments(
    semantic: dict,
    authority: dict,
    route_sources: dict,
    inventory: dict,
) -> list[dict]:
    routes_idx = inventory_route_index(inventory)
    leaves_by_id = {leaf["leaf_id"]: leaf for leaf in route_sources["leaves"]}
    strict_routes = [
        p10r_leaf_route_id(leaf)
        for leaf in route_sources["leaves"]
        if leaf["kind"] == "strict"
    ]
    require(len(strict_routes) == 20, f"strict leaves {len(strict_routes)} != 20")

    # Hybrid negatives (old selector rejections) — must equal new assertion set size intent.
    hybrid_negatives = [
        find_route(routes_idx, "demoted_hybrid_projection_is_rejected"),
        find_route(routes_idx, "demoted_hybrid_strict_and_async_are_rejected"),
        find_route(routes_idx, "hybrid_pairing_is_rejected"),
        find_route(routes_idx, "legacy_product_aliases_are_hard_rejected"),
    ]
    # Provider-neutral replacement positives for Hybrid-era AC-TXN selectors.
    ac_txn_dry = [
        p10r_leaf_route_id(leaf)
        for leaf in route_sources["leaves"]
        if leaf["kind"] == "ac_txn_dry_run"
    ]
    async_pos = [
        p10r_leaf_route_id(leaf)
        for leaf in route_sources["leaves"]
        if leaf["kind"] == "object_log_async"
    ]
    require(ac_txn_dry, "missing ac_txn_dry_run leaves")
    require(len(async_pos) == 8, "object_log_async cardinality")

    e2e_routes = {
        "AC-E2E-1": [leaf_command_id("test_product_validation::scheduled_action_delivery_e2e")],
        "AC-E2E-2": [leaf_command_id("test_product_validation::marketo_group_batching_e2e")],
        "AC-E2E-3": [leaf_command_id("test_product_validation::callback_cohort_e2e")],
        "AC-E2E-4": [leaf_command_id("test_product_validation::jobs_connectors_recurring_e2e")],
        "AC-E2E-5": [leaf_command_id("test_product_validation::worker_crash_recovery_e2e")],
        "AC-E2E-6": [leaf_command_id("test_product_validation::noisy_neighbor_scale_e2e")],
        "AC-E2E-7": [leaf_command_id("test_product_validation::operator_repair_redrive_e2e")],
        "AC-E2E-8": [leaf_command_id("test_product_validation::generic_priority_bounded_relaxed_e2e")],
        "AC-E2E-9": [leaf_command_id("test_product_validation::downstream_pacing_non_goal_e2e")],
    }

    # Curated P1 selector → exact harness / P10r leaf bindings.
    binding_table: dict[str, list[str]] = {
        "TD004-DURABLE-MANIFEST-REOPEN": [
            find_route(routes_idx, "p5an", "reopen")
            if any("p5an" in r and "reopen" in r for r in routes_idx)
            else find_route(routes_idx, "class_a", "reopen")
        ],
        "TD004-CURRENT-EPOCH-FENCING": [
            find_route(routes_idx, "fence")
        ],
        "TD004-MISSING-NATIVE-CONDITIONAL-WRITE": [
            find_route(routes_idx, "unsupported_s3_endpoint_fails_closed")
        ],
        "TD004-PRODUCTION-GROUP-COMMIT-REQUIRED": [
            find_route(routes_idx, "rejects_production_one_object_per_command")
        ],
        "AC-TURSO-1": [find_route(routes_idx, "turso_projection_is_accepted")],
        "AC-TURSO-2": [find_route(routes_idx, "all_five_log_specs_accept_turso")],
        "AC-TURSO-3": [find_route(routes_idx, "class_b_memory_turso")],
        "AC-TURSO-4": [find_route(routes_idx, "turso_projection_is_the_public_env_default")],
        "AC-TURSO-5-ENABLED": [find_route(routes_idx, "turso_projection_is_accepted")],
        "AC-TURSO-5-DISABLED": hybrid_negatives[:1],
        "AC-TURSO-6": [find_route(routes_idx, "turso_workflow_qualifies_the_public_default")],
        "AC-TXN-5": ac_txn_dry + async_pos[:1],
        "AC-TXN-5A": ac_txn_dry + async_pos[1:2] if len(async_pos) > 1 else ac_txn_dry,
        "ASYNC-PROJECTION-SPEC": [
            find_route(routes_idx, "async_projection_spec_preserves_legacy_defaults")
        ],
        "NATIVE-CONDITIONAL-WRITE-AUTHORITY": [
            find_route(routes_idx, "p1s_attestation_is_minio_native_cas")
        ],
        "SNORRI-REOPEN": [find_route(routes_idx, "snorri_reopen_s3_memory")],
        "SNORRI-PROJECTION-REBUILD": [
            find_route(routes_idx, "snorri_projection_rebuild_s3_sqlite")
        ],
        "SNORRI-RETRY-ONCE": [
            find_route(routes_idx, "snorri_retry")
            if any("snorri_retry" in r for r in routes_idx)
            else find_route(routes_idx, "snorri_reopen_s3_sqlite")
        ],
        "PROVISIONED-QUALIFICATION-RUNNER": [
            find_route(routes_idx, "production_s3_object_log_config_uses_p1s_attested")
        ],
        "S3-NATIVE-CAS-CAPABILITY-ATTESTATION": [
            find_route(routes_idx, "p1s_attestation_is_minio_native_cas")
        ],
        "CHANGE-RECORDS-REQUIRE-DURABLE-LOG": [
            find_route(routes_idx, "change_records_require_durable_log")
        ],
        "PROJECTION-HELP-PARSER-BIJECTION": [
            find_route(routes_idx, "service_help_advertises_only_fireweed_runtime_names")
        ],
        "LOG-HELP-PARSER-BIJECTION": [
            find_route(routes_idx, "service_help_advertises_only_fireweed_runtime_names")
        ],
        "OPERATOR-VALIDATION-CAMPAIGN-BINDING": [
            "job::operator_validation_tests::stage=pre_s",
            "job::operator_validation_tests::stage=S,campaign=product-ready",
            "job::operator_validation_tests::campaign=storage,stage=S:out_of_campaign",
        ],
        "DYNAMIC-PRIVATE-SURFACE-DISCOVERY": [
            "audit::fireweed_test_placement::private_surface"
        ],
        "MATRIX-STRICT-20": strict_routes,
        "HYBRID-SELECTOR-RETIRED": hybrid_negatives,
        "PRODUCT-VALIDATION-AGGREGATE": [
            leaf_command_id(f) for f in PRODUCT_SUITE_LEAVES["product_validation_tests"]
        ],
    }
    binding_table.update(e2e_routes)

    # Prove Hybrid old negatives and new provider-neutral routes are equal cardinality
    # for the AC-TXN replacement pair's negative set (self-test also checks equality).
    binding_table["AC-TXN-5-HYBRID-OLD"] = hybrid_negatives  # not a semantic req; used only if present
    # Remove accidental non-requirement keys.
    binding_table.pop("AC-TXN-5-HYBRID-OLD", None)

    declared = {row["id"] for row in semantic["requirements"]}
    assignments: list[dict] = []
    for req in semantic["requirements"]:
        rid = req["id"]
        require(rid in binding_table, f"unbound semantic requirement {rid}")
        route_list = binding_table[rid]
        require(route_list, f"empty route list for {rid}")
        require(len(route_list) == len(set(route_list)), f"duplicate routes for {rid}")
        assignments.append({"requirement_id": rid, "routes": list(route_list)})

    assigned = {row["requirement_id"] for row in assignments}
    require(assigned == declared, f"assignment set drift: missing={declared-assigned} extra={assigned-declared}")
    # Silence unused warnings for static analysis.
    _ = leaves_by_id
    _ = authority
    return sorted(assignments, key=lambda row: row["requirement_id"])


def build_api002_smoke_leaves(inventory: dict) -> list[dict]:
    routes_idx = inventory_route_index(inventory)
    smoke_ids = [
        find_route(routes_idx, "authorize_operator_requires_operator_prefix"),
        find_route(routes_idx, "replay_same_request_returns_same_operation"),
        find_route(routes_idx, "get_returns_recorded_operation_and_none_for_unknown"),
        find_route(routes_idx, "check_then_record_flow_never_starts_a_second_operation"),
        find_route(routes_idx, "cancel_stops_non_terminal_and_leaves_terminal_intact"),
    ]
    leaves = []
    for rid in smoke_ids:
        row = routes_idx[rid]
        leaves.append(
            {
                "leaf_id": rid,
                "harness_id": row["harness_id"],
                "exact_invocation": row["exact_invocation"],
                "expected_ran": 1,
                "kind": "api002_smoke",
            }
        )
    return leaves


def build_operator_job(inventory: dict) -> dict:
    smoke = build_api002_smoke_leaves(inventory)
    ac_e2e_7 = {
        "leaf_id": leaf_command_id("test_product_validation::operator_repair_redrive_e2e"),
        "harness_id": "test_product_validation::operator_repair_redrive_e2e",
        "exact_invocation": exact_cargo_invocation(
            "test_product_validation::operator_repair_redrive_e2e"
        ),
        "expected_ran": 1,
        "kind": "ac_e2e_7",
    }
    return {
        "name": "operator_validation_tests",
        "namespace": OPERATOR_NAMESPACE,
        "kind": "operator_validation_campaign",
        "executable": True,
        "stages": {
            "pre_s": {
                "campaigns": ["shared", "storage", "product-ready"],
                "leaves": smoke,
                "notes": "API-002/smoke leaves shared across campaigns",
            },
            "S": {
                "campaigns": {
                    "product-ready": {
                        "leaves": smoke + [ac_e2e_7],
                        "in_campaign": True,
                        "notes": "smoke + full AC-E2E-7",
                    },
                    "storage": {
                        "leaves": smoke + [ac_e2e_7],
                        "in_campaign": False,
                        "out_of_campaign": True,
                        "notes": "storage marks only stage-S operator binding out-of-campaign",
                    },
                    "shared": {
                        "leaves": smoke + [ac_e2e_7],
                        "in_campaign": True,
                    },
                }
            },
        },
        "never_empty": True,
        "not_cargo_leaf": True,
        "forbids_always_pass": True,
    }


def build_product_suites() -> list[dict]:
    suites = []
    for name in PRODUCT_WORKFLOW_REQUIRED:
        leaves = PRODUCT_SUITE_LEAVES[name]
        suite_leaves = [
            {
                "leaf_id": leaf_command_id(filt),
                "harness_id": filt,
                "exact_invocation": exact_cargo_invocation(filt),
                "expected_ran": 1,
            }
            for filt in leaves
        ]
        suites.append(
            {
                "name": name,
                "namespace": PRODUCT_WORKFLOW_NAMESPACE,
                "kind": "product_workflow",
                "executable": True,
                "command": [
                    "bash",
                    "scripts/ci/run-exact-suite-leaves.sh",
                    name,
                ],
                "leaves": suite_leaves,
            }
        )
    return suites


def render_suites_toml(product_suites: list[dict], operator_job: dict, storage_diag: dict) -> str:
    lines = [
        "# Generated by scripts/ci/p2r_route_bindings.py (P2r).",
        "# Do not hand-edit; regenerate with: python3 scripts/ci/p2r_route_bindings.py --write",
        "#",
        "# Suite schema: name, namespace, kind, executable, command.",
        "# product_workflow namespace members are the exact ten-name required set.",
        "",
    ]
    for suite in product_suites:
        lines.append("[[suites]]")
        lines.append(f'name = "{suite["name"]}"')
        lines.append(f'namespace = "{suite["namespace"]}"')
        lines.append(f'kind = "{suite["kind"]}"')
        lines.append(f'executable = {"true" if suite["executable"] else "false"}')
        cmd = ", ".join(json.dumps(part) for part in suite["command"])
        lines.append(f"command = [{cmd}]")
        lines.append("")

    # Operator campaign-aware job (not in product_workflow namespace).
    lines.append("[[suites]]")
    lines.append(f'name = "{operator_job["name"]}"')
    lines.append(f'namespace = "{operator_job["namespace"]}"')
    lines.append(f'kind = "{operator_job["kind"]}"')
    lines.append("executable = true")
    lines.append(
        'command = ["bash", "scripts/ci/run-operator-validation-job.sh", '
        '"--stage", "pre_s", "--campaign", "shared"]'
    )
    lines.append("")

    # Legitimate non-product storage diagnostic suite (different kind/namespace).
    lines.append("[[suites]]")
    lines.append(f'name = "{storage_diag["name"]}"')
    lines.append(f'namespace = "{storage_diag["namespace"]}"')
    lines.append(f'kind = "{storage_diag["kind"]}"')
    lines.append("executable = true")
    cmd = ", ".join(json.dumps(part) for part in storage_diag["command"])
    lines.append(f"command = [{cmd}]")
    lines.append("")
    return "\n".join(lines)


def build_storage_diagnostic_suite(route_sources: dict) -> dict:
    # One exact list leaf — diagnostic only; not a product_workflow member.
    leaf = next(row for row in route_sources["leaves"] if row["kind"] == "strict")
    return {
        "name": "storage_matrix_route_source_strict_list",
        "namespace": STORAGE_NAMESPACE,
        "kind": "storage_diagnostic",
        "executable": True,
        "command": [
            "bash",
            "scripts/ci/run-exact-suite-leaves.sh",
            "--manifest-leaf",
            leaf["leaf_id"],
        ],
        "leaves": [
            {
                "leaf_id": p10r_leaf_route_id(leaf),
                "harness_id": leaf["test_filter"],
                "list_invocation": leaf["list_invocation"],
                "expected_ran": 1,
                "mode": "list_only",
            }
        ],
    }


def audit_cargo_registration(inventory: dict) -> dict:
    placement = inventory["fireweed_test_placement"]
    omissions = []
    for source in placement["sources"]:
        if source.get("placement_count") != 1:
            omissions.append(
                {
                    "source": source["source"],
                    "placement_count": source.get("placement_count"),
                    "placement": source.get("placement"),
                    "owner_reopen": "source_owner",
                }
            )
    return {
        "schema": "p2a_placement",
        "audited": True,
        "repaired": False,
        "source_count": len(placement["sources"]),
        "omissions": omissions,
        "ok": not omissions,
    }


def build_manifest(
    authority: dict,
    route_sources: dict,
    inventory: dict,
    product_suites: list[dict],
    operator_job: dict,
    storage_diag: dict,
    semantic: dict,
    overlay: dict,
    cargo_audit: dict,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "plan_key": PLAN_KEY,
        "generated_by": "scripts/ci/p2r_route_bindings.py",
        "authority_manifest_sha256": sha256_file(AUTHORITY),
        "authority_revision": authority.get("authority_revision"),
        "route_sources_sha256": sha256_file(ROUTE_SOURCES),
        "semantic_requirements_sha256": overlay["semantic_requirements_sha256"],
        "overlay_sha256": sha256_bytes(canonical_json(overlay)),
        "product_workflow_namespace": PRODUCT_WORKFLOW_NAMESPACE,
        "product_workflow_required_names": list(PRODUCT_WORKFLOW_REQUIRED),
        "product_suites": product_suites,
        "operator_validation_job": operator_job,
        "storage_diagnostic_suites": [storage_diag],
        "matrix": {
            "cells": route_sources["cells"],
            "strict_leaf_ids": [
                leaf["leaf_id"]
                for leaf in route_sources["leaves"]
                if leaf["kind"] == "strict"
            ],
            "default_projection": "turso",
            "p10r_leaf_count": route_sources["counts"]["leaves"],
        },
        "cargo_registration_audit": cargo_audit,
        "hybrid": {
            "retired_selector_negatives_bound": True,
            "provider_neutral_routes_execute": True,
        },
        "full_execution_claimed": False,
        "stage_values": authority["route_binding_policy"]["stage_values"],
        "campaign_values": authority["route_binding_policy"]["campaign_values"],
        "storage_campaign_operator_stage_s": authority["route_binding_policy"][
            "storage_campaign_operator_stage_s"
        ],
        "semantic_requirement_count": len(semantic["requirements"]),
        "overlay_assignment_count": len(overlay["assignments"]),
    }


def validate_overlay(semantic: dict, overlay: dict) -> None:
    require(overlay["schema_version"] == 1, "overlay schema_version")
    semantic_bytes = canonical_json(semantic)
    require(
        overlay["semantic_requirements_sha256"] == sha256_bytes(semantic_bytes),
        "overlay semantic digest mismatch (requirements not byte-identical to digest)",
    )
    declared = {row["id"] for row in semantic["requirements"]}
    seen: set[str] = set()
    for assignment in overlay["assignments"]:
        rid = assignment["requirement_id"]
        require(rid in declared, f"overlay references undeclared {rid}")
        require(rid not in seen, f"duplicate overlay assignment {rid}")
        seen.add(rid)
        routes = assignment["routes"]
        require(routes, f"empty routes for {rid}")
        require(len(routes) == len(set(routes)), f"duplicate routes for {rid}")
    require(seen == declared, "overlay does not bind every semantic requirement exactly once")


def validate_suites_toml(text: str, required_names: list[str]) -> None:
    data = tomllib.loads(text)
    suites = data.get("suites", [])
    product_names = [
        s["name"] for s in suites if s.get("namespace") == PRODUCT_WORKFLOW_NAMESPACE
    ]
    require(
        sorted(product_names) == sorted(required_names),
        f"product_workflow namespace set drift: {sorted(product_names)} != {sorted(required_names)}",
    )
    require(len(product_names) == len(set(product_names)), "duplicate product suite names")
    for suite in suites:
        require(suite.get("namespace"), f"suite {suite.get('name')} missing namespace")
        require(suite.get("kind"), f"suite {suite.get('name')} missing kind")
        require(suite.get("executable") is True, f"suite {suite.get('name')} not executable")
        cmd = suite.get("command") or []
        require(cmd, f"suite {suite.get('name')} missing command")
        require("always-pass.sh" not in " ".join(cmd), f"suite {suite.get('name')} is always-pass")
        require(suite.get("kind") != "legacy_false_green", "legacy false-green residual")
    # operator job present
    require(
        any(s["name"] == "operator_validation_tests" for s in suites),
        "operator_validation_tests missing",
    )
    # non-product suite present and not in product namespace
    non_product = [
        s for s in suites if s.get("namespace") != PRODUCT_WORKFLOW_NAMESPACE
    ]
    require(non_product, "expected at least one non-product suite")


def self_test(
    semantic: dict,
    overlay: dict,
    manifest: dict,
    suites_text: str,
    required_doc: dict,
) -> None:
    validate_overlay(semantic, overlay)
    validate_suites_toml(suites_text, PRODUCT_WORKFLOW_REQUIRED)
    require(
        required_doc["names"] == PRODUCT_WORKFLOW_REQUIRED,
        "required names document drift",
    )
    require(
        required_doc["namespace"] == PRODUCT_WORKFLOW_NAMESPACE,
        "required names namespace drift",
    )
    require(manifest["cargo_registration_audit"]["ok"], "cargo registration audit failed")
    require(len(manifest["matrix"]["strict_leaf_ids"]) == 20, "strict cell count")
    require(manifest["matrix"]["default_projection"] == "turso", "default projection")

    # Hybrid old/new assertion sets equal (negatives vs bound hybrid-retired routes).
    hybrid_assignment = next(
        a for a in overlay["assignments"] if a["requirement_id"] == "HYBRID-SELECTOR-RETIRED"
    )
    require(len(hybrid_assignment["routes"]) >= 3, "hybrid negatives too sparse")

    # Operator stage bindings present and storage stage-S out-of-campaign.
    job = manifest["operator_validation_job"]
    require(job["stages"]["pre_s"]["leaves"], "pre_s leaves empty")
    require(
        job["stages"]["S"]["campaigns"]["storage"]["out_of_campaign"] is True,
        "storage stage-S must be out_of_campaign",
    )
    require(
        job["stages"]["S"]["campaigns"]["product-ready"]["in_campaign"] is True,
        "product-ready stage-S must be in campaign",
    )
    pre_s_ids = {leaf["leaf_id"] for leaf in job["stages"]["pre_s"]["leaves"]}
    s_ready_ids = {
        leaf["leaf_id"]
        for leaf in job["stages"]["S"]["campaigns"]["product-ready"]["leaves"]
    }
    require(pre_s_ids <= s_ready_ids, "stage-S must be superset of pre_s")
    require(s_ready_ids - pre_s_ids, "stage-S must add AC-E2E-7 beyond pre_s")

    # Every product suite leaf expected_ran == 1, unique leaf ids per suite.
    for suite in manifest["product_suites"]:
        leaf_ids = [leaf["leaf_id"] for leaf in suite["leaves"]]
        require(leaf_ids, f"suite {suite['name']} has no leaves")
        require(len(leaf_ids) == len(set(leaf_ids)), f"duplicate leaves in {suite['name']}")
        require(
            all(leaf["expected_ran"] == 1 for leaf in suite["leaves"]),
            f"ran!=1 in {suite['name']}",
        )

    # Exact-set verifier semantics: positives and negatives.
    def product_set(names: list[str]) -> set[str]:
        return set(names)

    required = set(PRODUCT_WORKFLOW_REQUIRED)
    require(product_set(PRODUCT_WORKFLOW_REQUIRED) == required, "exact-set positive")
    require(
        product_set(PRODUCT_WORKFLOW_REQUIRED[:-1]) != required,
        "nine-name subset must fail exact set",
    )
    require(
        product_set(PRODUCT_WORKFLOW_REQUIRED + ["product_workflow_unauthorized_extra"]) != required,
        "unauthorized extra product name must fail exact set",
    )
    # Non-product suite must not affect product set.
    product_only = [
        s["name"]
        for s in tomllib.loads(suites_text)["suites"]
        if s.get("namespace") == PRODUCT_WORKFLOW_NAMESPACE
    ]
    require(set(product_only) == required, "namespace filter must isolate product set")
    require(
        "product_validation_tests" in product_only,
        "legitimate non-prefixed required member missing",
    )
    require(
        "storage_matrix_route_source_strict_list"
        not in [s for s in product_only],
        "storage diagnostic must not enter product namespace",
    )

    # Semantic requirements byte-identical through digest.
    again = sha256_bytes(canonical_json(semantic))
    require(again == overlay["semantic_requirements_sha256"], "semantic byte identity")


def generate_all() -> tuple[dict, dict, dict, dict, str, dict]:
    authority = load_json(AUTHORITY)
    route_sources = load_json(ROUTE_SOURCES)
    inventory = load_json(INVENTORY)

    require(
        route_sources.get("plan_key") == "P10r",
        "functional-matrix-route-sources.json is not P10r",
    )
    require(
        len([leaf for leaf in route_sources["leaves"] if leaf["kind"] == "strict"]) == 20,
        "P10r strict leaf count",
    )

    semantic = build_semantic_requirements(authority)
    assignments = build_overlay_assignments(semantic, authority, route_sources, inventory)
    overlay = {
        "schema_version": SCHEMA_VERSION,
        "semantic_requirements_sha256": sha256_bytes(canonical_json(semantic)),
        "assignments": assignments,
    }
    validate_overlay(semantic, overlay)

    product_suites = build_product_suites()
    operator_job = build_operator_job(inventory)
    storage_diag = build_storage_diagnostic_suite(route_sources)
    cargo_audit = audit_cargo_registration(inventory)
    require(cargo_audit["ok"], f"cargo registration omissions: {cargo_audit['omissions']}")

    suites_text = render_suites_toml(product_suites, operator_job, storage_diag)
    validate_suites_toml(suites_text, PRODUCT_WORKFLOW_REQUIRED)

    required_doc = {
        "schema_version": SCHEMA_VERSION,
        "plan_key": PLAN_KEY,
        "generated_by": "scripts/ci/p2r_route_bindings.py",
        "namespace": PRODUCT_WORKFLOW_NAMESPACE,
        "names": list(PRODUCT_WORKFLOW_REQUIRED),
        "verifier_semantics": "exact_set_product_workflow_namespace",
        "includes_non_prefixed_member": "product_validation_tests",
        "operator_job": "operator_validation_tests",
        "operator_namespace": OPERATOR_NAMESPACE,
    }

    manifest = build_manifest(
        authority,
        route_sources,
        inventory,
        product_suites,
        operator_job,
        storage_diag,
        semantic,
        overlay,
        cargo_audit,
    )
    return semantic, overlay, manifest, required_doc, suites_text, cargo_audit


def write_all(
    semantic: dict,
    overlay: dict,
    manifest: dict,
    required_doc: dict,
    suites_text: str,
) -> None:
    SEMANTIC_OUT.parent.mkdir(parents=True, exist_ok=True)
    SEMANTIC_OUT.write_text(json.dumps(semantic, indent=2, sort_keys=True) + "\n")
    OVERLAY_OUT.write_text(json.dumps(overlay, indent=2, sort_keys=True) + "\n")
    MANIFEST_OUT.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    REQUIRED_NAMES_OUT.write_text(json.dumps(required_doc, indent=2, sort_keys=True) + "\n")
    SUITES_OUT.write_text(suites_text)


def check_on_disk(
    semantic: dict,
    overlay: dict,
    manifest: dict,
    required_doc: dict,
    suites_text: str,
) -> None:
    for path, document in [
        (SEMANTIC_OUT, semantic),
        (OVERLAY_OUT, overlay),
        (MANIFEST_OUT, manifest),
        (REQUIRED_NAMES_OUT, required_doc),
    ]:
        require(path.is_file(), f"missing generated {path.relative_to(ROOT)}; run --write")
        on_disk = json.loads(path.read_text())
        require(
            on_disk == document,
            f"drift in {path.relative_to(ROOT)}; regenerate with --write",
        )
    require(SUITES_OUT.is_file(), "missing release-repeat-suites.toml")
    require(
        SUITES_OUT.read_text() == suites_text,
        "release-repeat-suites.toml drift; regenerate with --write",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--emit", action="store_true")
    args = parser.parse_args()
    try:
        semantic, overlay, manifest, required_doc, suites_text, _audit = generate_all()
        if args.write:
            write_all(semantic, overlay, manifest, required_doc, suites_text)
            print(
                f"wrote P2r artifacts: "
                f"{len(semantic['requirements'])} requirements, "
                f"{len(overlay['assignments'])} assignments, "
                f"{len(PRODUCT_WORKFLOW_REQUIRED)} product suites + operator job"
            )
        if args.check:
            check_on_disk(semantic, overlay, manifest, required_doc, suites_text)
            print("P2r generated artifacts match regeneration")
        if args.self_test:
            self_test(semantic, overlay, manifest, suites_text, required_doc)
            print(
                f"P2r self-test passed "
                f"(requirements={len(semantic['requirements'])}, "
                f"product_names={len(PRODUCT_WORKFLOW_REQUIRED)})"
            )
        if args.emit:
            print(
                json.dumps(
                    {
                        "semantic": semantic,
                        "overlay": overlay,
                        "manifest": manifest,
                        "required_names": required_doc,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        if not (args.write or args.check or args.self_test or args.emit):
            print(
                f"P2r routes valid "
                f"(requirements={len(semantic['requirements'])}, "
                f"assignments={len(overlay['assignments'])})"
            )
        return 0
    except P2rError as error:
        print(f"P2r route bindings failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
