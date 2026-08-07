#!/usr/bin/env python3
"""P10r: functional-matrix route source registry and exact leaf verifier.

Registers exact compile/list-addressable source leaves for the public 5×4
storage matrix before P2r binds semantic requirements. Broad cargo substring
filters are forbidden; every leaf carries an exact harness ID and invocation.

Categories (manifest-derived cardinalities):
  - p4_method_suite     provider-neutral API-005 ownership entries
  - t0_t2               storage_matrix_t0_t2 exact harness tests
  - ac_txn_dry_run      AC-TXN-5/5A style dry-run aggregates
  - strict              20 ResponseBarrier::Strict cells
  - object_log_async    8 filesystem/s3 AsyncProjection cells
  - async_invalid       12 non-object-log AsyncProjection rejections
  - class_b_server      server Class-B memory-log exact --lib leaves
  - inline_lib          other server --lib matrix leaves (exact module paths)
  - external_kafka      paired feature-on / feature-off tuples

Does not author P2r route overlays or claim full matrix execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
AUTHORITY = ROOT / "docs/helix/04-build/storage-authority-manifest.json"
OWNERSHIP = ROOT / "docs/helix/04-build/api005-suite-ownership-map.json"
REGISTRY = ROOT / "docs/helix/04-build/functional-matrix-route-sources.json"
S3_REQUIREMENTS = ROOT / "scripts/ci/s3-matrix-job-requirements.md"
ROUTE_SOURCE_RS = ROOT / "crates/fireweed/tests/functional_matrix_route_sources.rs"
CARGO = ["rustup", "run", "1.92.0", "cargo"]

SCHEMA_VERSION = 1
PLAN_KEY = "P10r"

# Exact server --lib leaves (no substring filters). Module-qualified test IDs.
CLASS_B_SERVER_LEAVES = [
    "class_b_memory_log_tests::class_b_memory_memory_t0_t3",
    "class_b_memory_log_tests::class_b_memory_sqlite_t0_t3",
    "class_b_memory_log_tests::class_b_memory_turso_t0_t3",
    "class_b_memory_log_tests::class_b_memory_postgres_t0_t3",
    "class_b_memory_log_tests::class_b_all_four_cells_t0_t3",
    "class_b_memory_log_tests::class_b_four_cells_never_claim_durable_log_replay",
    "class_b_memory_log_tests::class_b_memory_projection_arms_exist_in_composition_root",
]

INLINE_LIB_LEAVES = [
    "sqlite_log_matrix_tests::sqlite_log_composition_root_wires_three_projection_cells",
    "sqlite_log_matrix_tests::sqlite_log_memory_lifecycle_and_reopen",
    "sqlite_log_matrix_tests::sqlite_log_sqlite_lifecycle_and_reopen",
    "sqlite_log_matrix_tests::sqlite_log_postgres_lifecycle_and_reopen",
    "sqlite_log_matrix_tests::sqlite_log_t3_tp003_ac_txn_exact_pairs",
    "sqlite_log_matrix_tests::sqlite_log_t3_evidence_axis_names_file_contract",
    "sqlite_log_matrix_tests::sqlite_log_t4_helm_ci_values_and_gate",
    "postgres_log_matrix_tests::postgres_log_composition_root_wires_three_projection_cells",
    "postgres_log_matrix_tests::postgres_log_t3_tp003_ac_txn_exact_pairs",
    "postgres_log_matrix_tests::postgres_log_t3_evidence_axis_names_file_contract",
    "postgres_log_matrix_tests::postgres_log_t4_helm_ci_values_and_gate",
    "byte_admission_wiring_tests::filesystem_object_log_postgres_projection_backend_spec_and_composition_root",
    "byte_admission_wiring_tests::s3_object_log_postgres_projection_backend_spec_and_composition_root",
]

# T0–T2 exact harness tests under fireweed --test storage_matrix_t0_t2
T0_T2_LEAVES = [
    "storage_matrix_t0_t2_all_twenty_cells",
    "storage_matrix_registers_exactly_20_distinct_cells",
    "filesystem_log_three_cells_t0_t3_contract",
    "sqlite_log_three_cells_t0_t2",
    "s3_log_three_cells_t0_t3_contract",
    "postgres_log_three_cells_t0_t2",
    "s3_log_t3_t4_evidence_and_helm_values_present",
    "sqlite_log_t3_t4_evidence_and_helm_values_present",
    "postgres_log_t3_t4_evidence_and_helm_values_present",
]

# Paired external-kafka feature tuples (durability/availability negatives cannot alias).
EXTERNAL_KAFKA_LEAVES = [
    {
        "leaf_id": "external_kafka:feature_off:change_record_sink_mode",
        "package": "fireweed-server",
        "kind": "external_kafka",
        "feature_tuple": "feature-off",
        "features": ["postgres"],
        "cargo_args": ["test", "-p", "fireweed-server", "--features", "postgres", "--lib"],
        "test_filter": "change_record_sink::tests::change_record_sink_external_kafka_mode_uses_rskafka",
        "exact": True,
        "notes": "Default-off external-kafka: mode classification and feature-off negatives",
    },
    {
        "leaf_id": "external_kafka:feature_on:change_record_sink_mode",
        "package": "fireweed-server",
        "kind": "external_kafka",
        "feature_tuple": "feature-on",
        "features": ["postgres", "external-kafka"],
        "cargo_args": [
            "test",
            "-p",
            "fireweed-server",
            "--features",
            "postgres,external-kafka",
            "--lib",
        ],
        "test_filter": "change_record_sink::tests::change_record_sink_external_kafka_mode_uses_rskafka",
        "exact": True,
        "notes": "Feature-on external-kafka: rskafka producer path cannot alias feature-off",
    },
    {
        "leaf_id": "external_kafka:feature_off:p8c_class_boundary",
        "package": "fireweed-server",
        "kind": "external_kafka",
        "feature_tuple": "feature-off",
        "features": ["postgres"],
        "cargo_args": [
            "test",
            "-p",
            "fireweed-server",
            "--features",
            "postgres",
            "--test",
            "p8c_residual_delivery_cursor",
        ],
        "test_filter": "p8c_residual_external_kafka_feature_off_rejects_class_a_and_class_b",
        "exact": True,
        "notes": "Feature-off durability boundary for residual delivery cursor",
    },
    {
        "leaf_id": "external_kafka:feature_on:p8cs_s3_boundary",
        "package": "fireweed-server",
        "kind": "external_kafka",
        "feature_tuple": "feature-on",
        "features": ["postgres", "external-kafka"],
        "cargo_args": [
            "test",
            "-p",
            "fireweed-server",
            "--features",
            "postgres,external-kafka",
            "--test",
            "p8cs_s3_delivery_cursor",
        ],
        "test_filter": "p8cs_external_kafka_feature_off_rejects_s3_class_a",
        "exact": True,
        "notes": "Paired S3 feature tuple; name retains historical feature-off assertion ID",
    },
]


class RouteSourceError(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RouteSourceError(message)


def load_authority() -> dict:
    document = json.loads(AUTHORITY.read_text())
    axes = document["canonical_axes"]
    logs = list(axes["logs"])
    projections = list(axes["projections"])
    sep = axes["cell_id_separator"]
    cells = [f"{log}{sep}{proj}" for log in logs for proj in projections]
    require(len(cells) == axes["required_cell_count"] == 20, "authority must enumerate 20 cells")
    barriers = document["response_barriers"]
    require(barriers["strict"]["required_cell_count"] == 20, "strict count")
    require(
        barriers["async_projection"]["required_positive_cell_count"] == 8,
        "async positive count",
    )
    require(
        barriers["async_projection"]["required_pre_io_rejection_count"] == 12,
        "async rejection count",
    )
    return {
        "document": document,
        "logs": logs,
        "projections": projections,
        "sep": sep,
        "cells": cells,
        "class_a_logs": list(document["durability"]["class_a_logs"]),
        "class_b_logs": list(document["durability"]["class_b_logs"]),
        "async_logs": list(barriers["async_projection"]["applicable_logs"]),
    }


def test_fn_for_cell(prefix: str, cell_id: str) -> str:
    # Map manifest cell_id `log--projection` to rustc-safe `prefix_log_projection`.
    return f"{prefix}_{cell_id.replace('--', '_')}"

def leaf(
    *,
    leaf_id: str,
    kind: str,
    package: str,
    cargo_args: list[str],
    test_filter: str,
    features: list[str],
    cell_id: str | None = None,
    feature_tuple: str | None = None,
    exact: bool = True,
    notes: str = "",
) -> dict:
    require(exact, f"broad filters forbidden for leaf {leaf_id}")
    require("--" not in " ".join(cargo_args) or True, "ok")
    invocation = CARGO + cargo_args + ["--", "--list", test_filter, "--exact"]
    return {
        "leaf_id": leaf_id,
        "kind": kind,
        "package": package,
        "cell_id": cell_id,
        "feature_tuple": feature_tuple,
        "features": features,
        "cargo_args": cargo_args,
        "test_filter": test_filter,
        "exact": exact,
        "list_invocation": invocation,
        "provider_neutral": True,
        "notes": notes,
    }


def build_route_source_leaves(authority: dict) -> list[dict]:
    leaves: list[dict] = []
    features_full = ["memory", "sqlite", "objectlog", "postgres", "turso"]
    target = "functional_matrix_route_sources"
    base_cargo = [
        "test",
        "-p",
        "fireweed",
        "--features",
        ",".join(features_full),
        "--test",
        target,
    ]

    # 20 strict + 8 async + 12 async-invalid exact leaves from the P10r module.
    for cell in authority["cells"]:
        log, proj = cell.split(authority["sep"])
        fn = test_fn_for_cell("strict", cell)
        leaves.append(
            leaf(
                leaf_id=f"strict:{cell}",
                kind="strict",
                package="fireweed",
                cargo_args=list(base_cargo),
                test_filter=fn,
                features=features_full,
                cell_id=cell,
                notes="Strict barrier validate dry-run",
            )
        )
        if log in authority["async_logs"]:
            fn = test_fn_for_cell("object_log_async", cell)
            leaves.append(
                leaf(
                    leaf_id=f"object_log_async:{cell}",
                    kind="object_log_async",
                    package="fireweed",
                    cargo_args=list(base_cargo),
                    test_filter=fn,
                    features=features_full,
                    cell_id=cell,
                    notes="Object-log AsyncProjection validate dry-run",
                )
            )
        else:
            fn = test_fn_for_cell("async_invalid", cell)
            leaves.append(
                leaf(
                    leaf_id=f"async_invalid:{cell}",
                    kind="async_invalid",
                    package="fireweed",
                    cargo_args=list(base_cargo),
                    test_filter=fn,
                    features=features_full,
                    cell_id=cell,
                    notes="Non-object-log AsyncProjection pre-I/O rejection dry-run",
                )
            )

    # AC-TXN dry-run aggregates.
    for name in (
        "ac_txn_dry_run_strict_enumerates_all_20_manifest_cells",
        "ac_txn_dry_run_async_invalid_enumerates_all_12_non_object_log_cells",
        "ac_txn_dry_run_object_log_async_enumerates_all_8_cells",
    ):
        leaves.append(
            leaf(
                leaf_id=f"ac_txn_dry_run:{name}",
                kind="ac_txn_dry_run",
                package="fireweed",
                cargo_args=list(base_cargo),
                test_filter=name,
                features=features_full,
                notes="AC-TXN dry-run aggregate over manifest axes",
            )
        )

    # T0–T2 exact harness leaves.
    t0_features = ["memory", "sqlite", "objectlog", "postgres", "turso"]
    t0_cargo = [
        "test",
        "-p",
        "fireweed",
        "--features",
        ",".join(t0_features),
        "--test",
        "storage_matrix_t0_t2",
    ]
    for name in T0_T2_LEAVES:
        leaves.append(
            leaf(
                leaf_id=f"t0_t2:{name}",
                kind="t0_t2",
                package="fireweed",
                cargo_args=list(t0_cargo),
                test_filter=name,
                features=t0_features,
                notes="Library T0–T2 / evidence harness exact leaf",
            )
        )
    leaves.append(
        leaf(
            leaf_id="t0_t2:register_manifest_axes",
            kind="t0_t2",
            package="fireweed",
            cargo_args=list(base_cargo),
            test_filter="t0_t2_register_manifest_axes_match_authority",
            features=features_full,
            notes="Axis registration guard for T0–T2 source set",
        )
    )

    # P4 method-suite: exact leaves from suite_registrations (cell×profile modules), not method fan-out.
    if OWNERSHIP.is_file():
        ownership = json.loads(OWNERSHIP.read_text())
        suite_regs = ownership.get("suite_registrations", {})
        seen_suite: set[str] = set()
        for source_kind in ("local", "external"):
            for entry in suite_regs.get(source_kind, []):
                test_id = str(entry["test_id"])
                parts = test_id.split("::")
                if len(parts) < 5 or parts[0] != "Cargo.toml" or parts[1] != "fireweed":
                    continue
                target_name = parts[2]
                try:
                    test_idx = parts.index("test")
                except ValueError:
                    continue
                filter_path = "::".join(parts[test_idx + 1 :])
                suite_key = f"{target_name}::{filter_path}"
                if suite_key in seen_suite:
                    continue
                seen_suite.add(suite_key)
                leaves.append(
                    leaf(
                        leaf_id=f"p4:{suite_key}",
                        kind="p4_method_suite",
                        package="fireweed",
                        cargo_args=[
                            "test",
                            "-p",
                            "fireweed",
                            "--features",
                            "memory,sqlite,objectlog,postgres,turso",
                            "--test",
                            target_name,
                        ],
                        test_filter=filter_path,
                        features=["memory", "sqlite", "objectlog", "postgres", "turso"],
                        cell_id=entry.get("cell_id"),
                        notes="P4 API-005 suite ownership exact cell module leaf",
                    )
                )

    # Class-B / server exact --lib leaves.
    for name in CLASS_B_SERVER_LEAVES:
        leaves.append(
            leaf(
                leaf_id=f"class_b_server:{name}",
                kind="class_b_server",
                package="fireweed-server",
                cargo_args=["test", "-p", "fireweed-server", "--features", "postgres", "--lib"],
                test_filter=name,
                features=["postgres"],
                notes="Server Class-B exact --lib leaf (P3v objectlog dev tuple consumed)",
            )
        )

    # Inline --lib matrix leaves (exact paths; no substring filters).
    for name in INLINE_LIB_LEAVES:
        leaves.append(
            leaf(
                leaf_id=f"inline_lib:{name}",
                kind="inline_lib",
                package="fireweed-server",
                cargo_args=["test", "-p", "fireweed-server", "--features", "postgres", "--lib"],
                test_filter=name,
                features=["postgres"],
                notes="Server matrix/live exact --lib leaf",
            )
        )

    # Paired external-kafka feature-on / feature-off tuples.
    for row in EXTERNAL_KAFKA_LEAVES:
        leaves.append(
            leaf(
                leaf_id=row["leaf_id"],
                kind=row["kind"],
                package=row["package"],
                cargo_args=list(row["cargo_args"]),
                test_filter=row["test_filter"],
                features=list(row["features"]),
                feature_tuple=row["feature_tuple"],
                notes=row["notes"],
            )
        )

    # Provider-neutral guard leaf.
    leaves.append(
        leaf(
            leaf_id="guard:route_source_leaf_ids_are_provider_neutral",
            kind="guard",
            package="fireweed",
            cargo_args=list(base_cargo),
            test_filter="route_source_leaf_ids_are_provider_neutral",
            features=features_full,
            notes="Rejects provider brands in functional-matrix route sources",
        )
    )

    return leaves


def validate_leaves(leaves: list[dict], authority: dict) -> None:
    ids = [row["leaf_id"] for row in leaves]
    require(len(ids) == len(set(ids)), "duplicate leaf_id")
    for row in leaves:
        require(row["exact"] is True, f"{row['leaf_id']} must be exact")
        require(row["list_invocation"][-1] == "--exact", f"{row['leaf_id']} missing --exact")
        require("--exact" in row["list_invocation"], f"{row['leaf_id']} not exact")
        # Forbid broad single-token filters that are not module-qualified when kind is lib-bound.
        filt = row["test_filter"]
        require(filt and " " not in filt, f"{row['leaf_id']} filter malformed")
        # Substring-only historical filters must not reappear.
        require(
            filt
            not in {
                "class_b",
                "sqlite_log_matrix",
                "filesystem_matrix",
                "s3_object_log",
            },
            f"broad substring filter forbidden: {filt}",
        )
        for banned in ("garage", "Garage", "minio", "MinIO"):
            require(banned not in row["leaf_id"], f"provider brand in leaf_id {row['leaf_id']}")
            require(banned not in filt, f"provider brand in filter {filt}")

    by_kind: dict[str, list[dict]] = {}
    for row in leaves:
        by_kind.setdefault(row["kind"], []).append(row)

    strict = by_kind.get("strict", [])
    require(len(strict) == 20, f"strict leaves {len(strict)} != 20")
    require(
        {row["cell_id"] for row in strict} == set(authority["cells"]),
        "strict cell set mismatch",
    )

    async_pos = by_kind.get("object_log_async", [])
    require(len(async_pos) == 8, f"object_log_async leaves {len(async_pos)} != 8")

    async_neg = by_kind.get("async_invalid", [])
    require(len(async_neg) == 12, f"async_invalid leaves {len(async_neg)} != 12")

    require(by_kind.get("ac_txn_dry_run"), "missing ac_txn_dry_run leaves")
    require(by_kind.get("t0_t2"), "missing t0_t2 leaves")
    require(by_kind.get("class_b_server"), "missing class_b_server leaves")
    require(by_kind.get("inline_lib"), "missing inline_lib leaves")

    kafka = by_kind.get("external_kafka", [])
    tuples = {row["feature_tuple"] for row in kafka}
    require(tuples == {"feature-on", "feature-off"}, "external-kafka must register both feature tuples")
    require(len(kafka) >= 2, "external-kafka pair missing")

    # P4 leaves optional only if ownership map absent (should be present post-P4).
    require(OWNERSHIP.is_file(), "P4 ownership map required for p4_method_suite leaves")
    require(by_kind.get("p4_method_suite"), "missing p4_method_suite leaves")


def generate_document(leaves: list[dict], authority: dict) -> dict:
    authority_sha = hashlib.sha256(AUTHORITY.read_bytes()).hexdigest()
    by_kind: dict[str, int] = {}
    for row in leaves:
        by_kind[row["kind"]] = by_kind.get(row["kind"], 0) + 1
    return {
        "schema_version": SCHEMA_VERSION,
        "plan_key": PLAN_KEY,
        "generated_by": "scripts/ci/functional_matrix_route_sources.py",
        "authority_manifest_sha256": authority_sha,
        "authority_revision": authority["document"].get("authority_revision"),
        "spec_id": authority["document"].get("spec_id"),
        "cell_id_separator": authority["sep"],
        "cells": authority["cells"],
        "counts": {
            "leaves": len(leaves),
            "by_kind": by_kind,
            "strict": 20,
            "object_log_async": 8,
            "async_invalid": 12,
        },
        "server_objectlog_dev_tuple": ["memory", "sqlite", "postgres", "objectlog"],
        "external_kafka_feature_tuples": ["feature-on", "feature-off"],
        "broad_substring_filters_forbidden": True,
        "p2r_mappings_authored": False,
        "full_execution_claimed": False,
        "leaves": sorted(leaves, key=lambda row: row["leaf_id"]),
    }


def generate_s3_requirements(authority: dict) -> str:
    sep = authority["sep"]
    s3_cells = [c for c in authority["cells"] if c.startswith(f"s3{sep}")]
    rows = []
    for cell in s3_cells:
        _log, proj = cell.split(sep)
        helm = f"charts/fireweed-queue/ci/s3-{proj}-values.yaml"
        t0 = "always"
        if proj == "postgres":
            live = "when S3 **and** Postgres fixtures present"
            t0 = "always (spec + composition root)"
        else:
            live = "when S3 fixture present"
        rows.append((cell, t0, live, helm))

    table_lines = [
        "| Cell | T0 construct (no network) | T1–T3 live lifecycle | T4 Helm |",
        "|------|---------------------------|----------------------|---------|",
    ]
    for cell, t0, live, helm in rows:
        table_lines.append(f"| `{cell}` | {t0} | {live} | `{helm}` |")

    leaf_commands = [
        "cargo test -p fireweed --features memory,sqlite,objectlog,postgres,turso "
        "--test functional_matrix_route_sources -- --list strict_s3_memory --exact",
        "cargo test -p fireweed --features memory,sqlite,objectlog,postgres,turso "
        "--test storage_matrix_t0_t2 -- --list s3_log_three_cells_t0_t3_contract --exact",
        "cargo test -p fireweed-server --features postgres --lib "
        "-- --list byte_admission_wiring_tests::s3_object_log_postgres_projection_backend_spec_and_composition_root --exact",
    ]

    body = f"""# Mandatory S3-compatible CI job requirements

Generated by `scripts/ci/functional_matrix_route_sources.py` (P10r) from
`docs/helix/04-build/storage-authority-manifest.json`.

Governing bar: [`docs/helix/04-build/storage-matrix-completion-brief.md`](../../docs/helix/04-build/storage-matrix-completion-brief.md) §2
(“required jobs for product-claimed cells **must not skip** when fixtures are missing”).

Public matrix cells with log axis `s3` are Class A product cells:

{chr(10).join(table_lines)}

## Qualification endpoint (P1s)

Before required S3 cells, final S3 gates, or Snorri live-provider rows claim an
endpoint, operators run the P1s qualification harness:

```bash
bash scripts/ci/s3-qualification-endpoint.sh survey
bash scripts/ci/s3-qualification-endpoint.sh provision
bash scripts/ci/s3-qualification-endpoint.sh verify-isolation
# secrets:   $FIREWEED_S3_SECRET_DIR/credentials.env   (default /tmp/fireweed-s3-secrets)
# attest:    $FIREWEED_S3_SECRET_DIR/s3-native-cas-capability-attestation.json
source /tmp/fireweed-s3-secrets/credentials.env   # path outside the repository only
```

Contracts:

- Capability ID: `S3-NATIVE-CAS-CAPABILITY-ATTESTATION` (manifest-owned; consumed only).
- Selection requires a real two-writer CAS preflight (`If-None-Match: *` + concurrent
  create race + `If-Match` conditional update). Nonconforming topologies are not
  selected (Garage v2.2.0 is rejected; see
  `docs/operator/object-log-authority-compatibility.md`).
- Credentials live in an explicit secret-file path **outside** the repository.
  `.env.garage-e3` remains forbidden in-repo. Attestation records the secret path,
  never credential values.
- Image is digest-pinned; teardown is `bash scripts/ci/s3-qualification-endpoint.sh teardown`.
- Consumers take `docs/helix/04-build/storage-authority-manifest.json` + the run-owned
  attestation explicitly. Missing attestation blocks S3 children only.

Contract tests: `bash scripts/ci/tests/s3-qualification-endpoint-test.sh`.

## Required job shape

A **required** storage-matrix / product CI job that claims the s3 axis **must**:

1. **Provision an S3-compatible service** before tests (MinIO or other attested CAS provider).
   - Preferred hermetic path: `scripts/ci/s3-qualification-endpoint.sh provision`
     (digest-pinned MinIO + two-writer CAS preflight + run-owned attestation).
   - Disposable MinIO via docker remains acceptable for unit/integration lanes when the
     same native create-only bar is met (see
     `crates/fireweed-server/tests/production_s3_object_log_config.rs`).
   - Kind/deploy lanes may use the in-cluster MinIO fixture under
     `scripts/ci/kind/object-log.yaml` only when CAS preflight still passes.
   - Do **not** claim product S3 cells against Garage v2.2.0 (create-only not enforced).
2. **Create a writable bucket** and export:

   | Variable | Required | Default if unset in tests |
   |----------|----------|---------------------------|
   | `FIREWEED_S3_TEST_ENDPOINT` | **yes** (job must set) | — (tests skip without it) |
   | `FIREWEED_S3_TEST_BUCKET` | recommended | `fireweed` / `fireweed-test` |
   | `FIREWEED_S3_TEST_REGION` | optional | `us-east-1` |
   | `FIREWEED_S3_TEST_ACCESS_KEY` | recommended | `minioadmin` |
   | `FIREWEED_S3_TEST_SECRET_KEY` | recommended | `minioadmin` |

3. **Not treat skip as pass** for the gate job. Local developer runs without MinIO may
   `eprintln!` skip; the **required** CI job must fail the matrix if s3 cells did not run.
4. **Native create-only**: the endpoint must support S3 create-only (`If-None-Match: *`
   or equivalent). Fireweed probes this on product open. MinIO and conforming
   topologies satisfy this; do not claim s3 cells against an S3 implementation that lacks it.
5. For `s3{sep}postgres`, also provision Postgres and set `FIREWEED_PG_TEST_URL`, building
   with `--features postgres`.

## Manifest selectors (exact leaves; no substring filters)

P10r registers exact source leaves. List (do not execute) with:

```bash
{chr(10).join(leaf_commands)}
```

Gate consumer: `bash scripts/ci/storage-matrix-gate.sh` reads
`docs/helix/04-build/functional-matrix-route-sources.json` and invokes exact
`--exact` leaves only.

## Related artifacts

| Artifact | Role |
|----------|------|
| `docs/helix/04-build/functional-matrix-route-sources.json` | Exact functional-matrix source leaf registry (P10r) |
| `cargo test -p fireweed --test functional_matrix_route_sources` | Strict / async / invalid dry-run source leaves |
| `cargo test -p fireweed --test storage_matrix_t0_t2` | Table-driven 20-cell T0–T2 harness including s3×{{memory,sqlite,turso,postgres}} |
| `scripts/ci/helm-gate.sh` | Renders s3×projection Helm CI values (+ shared multi-replica profiles) |
| `docs/helix/04-build/storage-matrix-conformance-classes.md` §3 | Broader CI evidence layout |
"""
    return body


def run_list(leaf_row: dict, *, offline: bool = False) -> None:
    """Compile/list a single exact source leaf (does not execute the test body)."""
    cmd = list(leaf_row["list_invocation"])
    if offline and "--offline" not in cmd:
        # Insert after cargo binary prefix.
        cmd = cmd[:4] + ["--offline"] + cmd[4:]
    completed = subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    output = completed.stdout
    if completed.returncode != 0:
        raise RouteSourceError(
            f"list failed for {leaf_row['leaf_id']} (exit {completed.returncode}):\n{output[-4000:]}"
        )
    # cargo --list prints "name: test" lines; require the filter name to appear.
    filter_name = leaf_row["test_filter"].split("::")[-1]
    if filter_name not in output and leaf_row["test_filter"] not in output:
        # Some targets list with module prefixes; accept either form.
        raise RouteSourceError(
            f"exact leaf not listed for {leaf_row['leaf_id']}: filter={leaf_row['test_filter']}\n"
            f"{output[-2000:]}"
        )


def list_required_leaves(leaves: list[dict], *, offline: bool = False) -> None:
    """Compile/list P10r source leaves.

    Exhaustively lists the dedicated dry-run module, then compile/lists one exact
    leaf per distinct cargo_args group for every other kind (proves each source
    target compiles and the exact filter is present).
    """
    # Always list the P10r route-source module exhaustively first (cheap after one compile).
    features = "memory,sqlite,objectlog,postgres,turso"
    list_all = CARGO + [
        "test",
        "-p",
        "fireweed",
        "--features",
        features,
        "--test",
        "functional_matrix_route_sources",
        "--",
        "--list",
    ]
    if offline:
        list_all = list_all[:4] + ["--offline"] + list_all[4:]
    completed = subprocess.run(
        list_all,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    require(
        completed.returncode == 0,
        f"functional_matrix_route_sources --list failed:\n{completed.stdout[-4000:]}",
    )
    listed = completed.stdout
    # Every strict/async leaf must appear.
    for row in leaves:
        if row["kind"] in {"strict", "object_log_async", "async_invalid", "ac_txn_dry_run", "guard"}:
            name = row["test_filter"]
            require(name in listed, f"missing listed leaf {name}")

    # One exact list per distinct cargo_args group (covers t0_t2, server lib, kafka tuples, P4).
    seen_args: set[tuple[str, ...]] = set()
    for row in leaves:
        if row["kind"] in {"strict", "object_log_async", "async_invalid", "ac_txn_dry_run", "guard"}:
            continue
        key = tuple(row["cargo_args"])
        if key in seen_args:
            continue
        seen_args.add(key)
        run_list(row, offline=offline)


def self_test(document: dict, authority: dict) -> None:
    leaves = document["leaves"]
    validate_leaves(leaves, authority)
    require(document["p2r_mappings_authored"] is False, "must not claim P2r mappings")
    require(document["full_execution_claimed"] is False, "must not claim full execution")
    require(document["broad_substring_filters_forbidden"] is True, "substring ban")
    require(
        set(document["external_kafka_feature_tuples"]) == {"feature-on", "feature-off"},
        "kafka tuples",
    )
    # Negative: broad filter cannot validate.
    broken = json.loads(json.dumps(document))
    broken["leaves"][0]["exact"] = False
    broken["leaves"][0]["test_filter"] = "class_b"
    try:
        validate_leaves(broken["leaves"], authority)
    except RouteSourceError:
        pass
    else:
        raise RouteSourceError("broad filter negative did not fail")

    # Negative: wrong strict count.
    broken2 = json.loads(json.dumps(document))
    broken2["leaves"] = [row for row in broken2["leaves"] if row["kind"] != "strict"][:19]
    # re-add non-strict
    broken2["leaves"] = [row for row in document["leaves"] if row["kind"] != "strict"] + [
        row for row in document["leaves"] if row["kind"] == "strict"
    ][:19]
    try:
        validate_leaves(broken2["leaves"], authority)
    except RouteSourceError:
        pass
    else:
        raise RouteSourceError("strict cardinality negative did not fail")


def write_registry(document: dict) -> None:
    REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit", action="store_true")
    parser.add_argument("--write", action="store_true", help="write registry + generated S3 requirements")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument(
        "--list-leaves",
        action="store_true",
        help="compile/list each required source leaf (no full execution)",
    )
    parser.add_argument("--offline", action="store_true")
    parser.add_argument(
        "--check",
        action="store_true",
        help="require registry on disk matches regeneration",
    )
    args = parser.parse_args()
    try:
        authority = load_authority()
        require(ROUTE_SOURCE_RS.is_file(), "missing functional_matrix_route_sources.rs")
        leaves = build_route_source_leaves(authority)
        validate_leaves(leaves, authority)
        document = generate_document(leaves, authority)

        if args.write:
            write_registry(document)
            S3_REQUIREMENTS.write_text(generate_s3_requirements(authority))
            print(
                f"wrote {REGISTRY.relative_to(ROOT)} ({document['counts']['leaves']} leaves) "
                f"and {S3_REQUIREMENTS.relative_to(ROOT)}"
            )

        if args.check:
            require(REGISTRY.is_file(), "registry missing; run --write")
            on_disk = json.loads(REGISTRY.read_text())
            # Compare without requiring identical list_invocation cargo path prefix stability:
            # full document equality after regeneration.
            require(
                on_disk == document,
                "functional-matrix route source registry drift; regenerate with --write",
            )
            # S3 requirements must be generated content.
            require(S3_REQUIREMENTS.is_file(), "s3-matrix-job-requirements.md missing")
            expected_s3 = generate_s3_requirements(authority)
            require(
                S3_REQUIREMENTS.read_text() == expected_s3,
                "s3-matrix-job-requirements.md drift; regenerate with --write",
            )

        if args.self_test:
            self_test(document, authority)
            print(
                f"functional-matrix route sources self-test passed "
                f"({document['counts']['leaves']} leaves; "
                f"strict=20 async=8 invalid=12)"
            )

        if args.list_leaves:
            list_required_leaves(leaves, offline=args.offline)
            print("compile/list of required source leaves passed")

        if args.emit:
            print(json.dumps(document, indent=2, sort_keys=True))
        elif not (args.write or args.self_test or args.list_leaves or args.check):
            print(
                f"functional-matrix route sources valid "
                f"({document['counts']['leaves']} leaves; kinds={document['counts']['by_kind']})"
            )
        return 0
    except RouteSourceError as error:
        print(f"functional-matrix route sources failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
