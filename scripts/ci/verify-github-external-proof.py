#!/usr/bin/env python3
"""Repository-side GitHub external-proof schema/CLI (P13a).

Owns:
  - schema validation against schemas/github-external-proof.schema.json
  - state-based freshness (not wall-clock TTL)
  - API validation against injected or live snapshots
  - optional Snorri external-acceptance injection
  - candidate closure (never reads .ddx/**)

P13b emits run-owned proof JSON outside the repository. P20 re-queries via
explicit --github-proof / --expected-source / --repository-id.

Usage:
  python3 scripts/ci/verify-github-external-proof.py --self-test
  python3 scripts/ci/verify-github-external-proof.py \\
      --github-proof /run-owned/proof.json \\
      --expected-source <40-hex> \\
      --repository-id <id> \\
      --api-fixture scripts/ci/fixtures/github-external-proof/api-fresh.json \\
      --candidate-closure

Exit codes:
  0 pass
  1 validation / freshness / API / candidate failure
  2 usage error
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas/github-external-proof.schema.json"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class ProofError(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ProofError(message)


def exact_keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{label} must be an object")
    actual = set(value)  # type: ignore[arg-type]
    require(actual == expected, f"{label} keys {sorted(actual)} != {sorted(expected)}")
    return value  # type: ignore[return-value]


def sha256_hex(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def refuse_ddx_path(path: Path, label: str) -> None:
    resolved = str(path.resolve())
    if "/.ddx/" in resolved or resolved.endswith("/.ddx") or path.parts and ".ddx" in path.parts:
        raise ProofError(f"{label} must never read .ddx/** (path={path})")


def validate_schema_document(schema: dict[str, Any]) -> None:
    require(schema.get("$schema") == "https://json-schema.org/draft/2020-12/schema", "schema draft")
    require(schema.get("additionalProperties") is False, "schema must reject unknown fields")
    require(schema.get("properties", {}).get("schema", {}).get("const") == "fireweed.github_external_proof.v1", "schema id")


def validate_proof_shape(proof: dict[str, Any]) -> None:
    required = {
        "schema",
        "schema_version",
        "repository_id",
        "ruleset",
        "required_contexts",
        "candidate_source",
        "checks",
        "response_digests",
        "recorded_at",
    }
    # Optional keys allowed by schema.
    optional = {"snorri_external_acceptance", "product_release_readiness_claimed"}
    require(isinstance(proof, dict), "proof must be object")
    unknown = set(proof) - required - optional
    require(not unknown, f"unknown proof fields: {sorted(unknown)}")
    missing = required - set(proof)
    require(not missing, f"missing proof fields: {sorted(missing)}")

    require(proof["schema"] == "fireweed.github_external_proof.v1", "schema identity")
    require(proof["schema_version"] == 1, "schema_version")
    require(isinstance(proof["repository_id"], str) and proof["repository_id"], "repository_id")
    require(isinstance(proof["recorded_at"], str) and proof["recorded_at"], "recorded_at")
    require(isinstance(proof["candidate_source"], str) and SHA40.fullmatch(proof["candidate_source"]), "candidate_source")

    ruleset = exact_keys(proof["ruleset"], {"id", "version", "digest"}, "ruleset")
    require(isinstance(ruleset["id"], str) and ruleset["id"], "ruleset.id")
    require(isinstance(ruleset["version"], str) and ruleset["version"], "ruleset.version")
    require(isinstance(ruleset["digest"], str) and DIGEST.fullmatch(ruleset["digest"]), "ruleset.digest")

    contexts = proof["required_contexts"]
    require(isinstance(contexts, list) and contexts, "required_contexts non-empty")
    require(all(isinstance(c, str) and c for c in contexts), "required_contexts entries")
    require(len(contexts) == len(set(contexts)), "required_contexts unique")

    digests = exact_keys(proof["response_digests"], {"ruleset", "check_runs"}, "response_digests")
    require(DIGEST.fullmatch(digests["ruleset"]), "response_digests.ruleset")
    require(DIGEST.fullmatch(digests["check_runs"]), "response_digests.check_runs")

    checks = proof["checks"]
    require(isinstance(checks, list) and checks, "checks non-empty")
    seen_contexts: set[str] = set()
    for index, check in enumerate(checks):
        label = f"checks[{index}]"
        require(isinstance(check, dict), f"{label} object")
        allowed = {
            "workflow_id",
            "context",
            "check_suite_id",
            "check_run_id",
            "conclusion",
            "head_sha",
            "attempt",
            "status",
        }
        require(set(check) <= allowed, f"{label} unknown fields")
        for key in (
            "workflow_id",
            "context",
            "check_suite_id",
            "check_run_id",
            "conclusion",
            "head_sha",
            "attempt",
        ):
            require(key in check, f"{label}.{key} required")
        require(check["conclusion"] == "success", f"{label}.conclusion must be success")
        require(isinstance(check["head_sha"], str) and SHA40.fullmatch(check["head_sha"]), f"{label}.head_sha")
        require(check["head_sha"] == proof["candidate_source"], f"{label}.head_sha must equal candidate_source")
        require(isinstance(check["attempt"], int) and check["attempt"] >= 1, f"{label}.attempt")
        require(check["context"] in contexts, f"{label}.context not in required_contexts")
        require(check["context"] not in seen_contexts, f"{label}.context duplicate")
        seen_contexts.add(check["context"])
        if "status" in check:
            require(check["status"] == "completed", f"{label}.status")

    missing_contexts = set(contexts) - seen_contexts
    require(not missing_contexts, f"missing checks for required contexts: {sorted(missing_contexts)}")

    if "snorri_external_acceptance" in proof:
        snorri = exact_keys(
            proof["snorri_external_acceptance"],
            {"schema", "source_revision", "suite_id", "conclusion", "evidence_digest"},
            "snorri_external_acceptance",
        )
        require(snorri["schema"] == "fireweed.snorri_external_acceptance.v1", "snorri schema")
        require(SHA40.fullmatch(snorri["source_revision"]), "snorri source_revision")
        require(snorri["source_revision"] == proof["candidate_source"], "snorri source must equal candidate_source")
        require(snorri["conclusion"] == "success", "snorri conclusion")
        require(DIGEST.fullmatch(snorri["evidence_digest"]), "snorri evidence_digest")
        require(isinstance(snorri["suite_id"], str) and snorri["suite_id"], "snorri suite_id")


def validate_against_api(proof: dict[str, Any], api: dict[str, Any]) -> None:
    """State-based freshness + API validation against an injected snapshot.

    Fresh means:
      - ruleset id+version+digest still current
      - selected run is latest attempt for workflow+context+S
      - suite/run conclusion successful
      - no newer queued/in-progress/completed attempt or rerequest
    """
    require(isinstance(api, dict), "api fixture must be object")
    required_api = {"repository_id", "ruleset", "check_attempts"}
    optional_api = {"expected_check_runs_digest"}
    require(required_api <= set(api), f"api fixture missing keys: {sorted(required_api - set(api))}")
    unknown_api = set(api) - required_api - optional_api
    require(not unknown_api, f"api fixture unknown keys: {sorted(unknown_api)}")
    require(api["repository_id"] == proof["repository_id"], "API repository_id mismatch")

    api_ruleset = exact_keys(api["ruleset"], {"id", "version", "digest", "current"}, "api.ruleset")
    require(api_ruleset["current"] is True, "ruleset is no longer current (stale proof)")
    require(api_ruleset["id"] == proof["ruleset"]["id"], "ruleset id moved")
    require(api_ruleset["version"] == proof["ruleset"]["version"], "ruleset version changed")
    require(api_ruleset["digest"] == proof["ruleset"]["digest"], "ruleset digest changed")
    # response_digests.ruleset binds the immutable ruleset digest (or a payload digest
    # equal to it when the fixture uses the digest as the response body hash).
    require(
        proof["response_digests"]["ruleset"] == api_ruleset["digest"],
        "ruleset response digest must match current ruleset digest",
    )

    attempts = api["check_attempts"]
    require(isinstance(attempts, list) and attempts, "api.check_attempts non-empty")

    # Index attempts by (workflow_id, context, head_sha)
    by_tuple: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for attempt in attempts:
        require(isinstance(attempt, dict), "attempt object")
        for key in ("workflow_id", "context", "head_sha", "attempt", "conclusion", "status", "check_suite_id", "check_run_id"):
            require(key in attempt, f"attempt missing {key}")
        key = (attempt["workflow_id"], attempt["context"], attempt["head_sha"])
        by_tuple.setdefault(key, []).append(attempt)

    check_run_payload = []
    for check in proof["checks"]:
        key = (check["workflow_id"], check["context"], check["head_sha"])
        family = by_tuple.get(key, [])
        require(family, f"no API attempts for {key}")
        # Latest attempt by attempt number.
        family_sorted = sorted(family, key=lambda row: int(row["attempt"]))
        latest = family_sorted[-1]
        require(int(latest["attempt"]) == int(check["attempt"]), f"proof attempt is not latest for {key}")
        require(latest["conclusion"] == "success", f"latest conclusion not success for {key}")
        require(latest["status"] == "completed", f"latest status not completed for {key}")
        require(str(latest["check_suite_id"]) == str(check["check_suite_id"]), f"check_suite_id mismatch for {key}")
        require(str(latest["check_run_id"]) == str(check["check_run_id"]), f"check_run_id mismatch for {key}")
        # No newer non-success supersession allowed (already latest).
        for row in family_sorted[:-1]:
            require(int(row["attempt"]) < int(latest["attempt"]), "attempt ordering")
        check_run_payload.append(
            {
                "workflow_id": check["workflow_id"],
                "context": check["context"],
                "check_run_id": check["check_run_id"],
                "attempt": check["attempt"],
            }
        )

    expected_check_digest = sha256_hex(canonical_json(check_run_payload))
    if "expected_check_runs_digest" in api:
        require(
            api["expected_check_runs_digest"] == expected_check_digest,
            "api fixture expected_check_runs_digest is inconsistent with attempts",
        )
        require(
            proof["response_digests"]["check_runs"] == api["expected_check_runs_digest"],
            "check_runs digest mismatch vs api fixture",
        )
    else:
        require(
            proof["response_digests"]["check_runs"] == expected_check_digest,
            "check_runs response digest mismatch",
        )


def inject_snorri(proof: dict[str, Any], snorri_path: Path) -> dict[str, Any]:
    refuse_ddx_path(snorri_path, "snorri proof")
    snorri = load_json(snorri_path)
    require(isinstance(snorri, dict), "snorri proof must be object")
    # Allow either bare snorri object or wrapper with snorri_external_acceptance key.
    if "snorri_external_acceptance" in snorri and set(snorri) == {"snorri_external_acceptance"}:
        snorri = snorri["snorri_external_acceptance"]
    merged = dict(proof)
    merged["snorri_external_acceptance"] = snorri
    validate_proof_shape(merged)
    return merged


def candidate_closure(expected_source: str, *, fixture: Path | None) -> None:
    """Candidate closure: prove S identity without reading .ddx/**."""
    require(SHA40.fullmatch(expected_source), "candidate_source form")
    if fixture is not None:
        refuse_ddx_path(fixture, "candidate-closure fixture")
        document = load_json(fixture)
        require(isinstance(document, dict), "candidate fixture object")
        require(document.get("candidate_source") == expected_source, "candidate fixture source mismatch")
        require(document.get("tracker_state_read") is False, "candidate fixture must not claim tracker reads")
        require(document.get("closed") is True, "candidate fixture not closed")
    # Always refuse ambient .ddx inspection in this mode.
    ddx = ROOT / ".ddx" / "beads.jsonl"
    # Existence is fine; reading it is forbidden. We deliberately do not open it.


def build_sample_proof() -> tuple[dict[str, Any], dict[str, Any]]:
    source = "a" * 40
    ruleset = {
        "id": "ruleset-1",
        "version": "3",
        "digest": "sha256:" + ("b" * 64),
    }
    check_run_payload = [
        {
            "workflow_id": "wf-governed-product",
            "context": "governed-product / product",
            "check_run_id": "cr-9",
            "attempt": 2,
        }
    ]
    proof = {
        "schema": "fireweed.github_external_proof.v1",
        "schema_version": 1,
        "repository_id": "repo-42",
        "ruleset": ruleset,
        "required_contexts": ["governed-product / product"],
        "candidate_source": source,
        "checks": [
            {
                "workflow_id": "wf-governed-product",
                "context": "governed-product / product",
                "check_suite_id": "cs-1",
                "check_run_id": "cr-9",
                "conclusion": "success",
                "head_sha": source,
                "attempt": 2,
                "status": "completed",
            }
        ],
        "response_digests": {
            "ruleset": ruleset["digest"],
            "check_runs": sha256_hex(canonical_json(check_run_payload)),
        },
        "recorded_at": "2026-08-07T00:00:00Z",
        "product_release_readiness_claimed": True,
    }
    api = {
        "repository_id": "repo-42",
        "ruleset": {**ruleset, "current": True},
        "check_attempts": [
            {
                "workflow_id": "wf-governed-product",
                "context": "governed-product / product",
                "head_sha": source,
                "attempt": 1,
                "conclusion": "failure",
                "status": "completed",
                "check_suite_id": "cs-0",
                "check_run_id": "cr-1",
            },
            {
                "workflow_id": "wf-governed-product",
                "context": "governed-product / product",
                "head_sha": source,
                "attempt": 2,
                "conclusion": "success",
                "status": "completed",
                "check_suite_id": "cs-1",
                "check_run_id": "cr-9",
            },
        ],
        "expected_check_runs_digest": proof["response_digests"]["check_runs"],
    }
    return proof, api


def self_test() -> None:
    schema = load_json(SCHEMA_PATH)
    validate_schema_document(schema)

    proof, api = build_sample_proof()
    validate_proof_shape(proof)
    validate_against_api(proof, api)
    candidate_closure(proof["candidate_source"], fixture=None)

    # Snorri injection positive.
    snorri = {
        "schema": "fireweed.snorri_external_acceptance.v1",
        "source_revision": proof["candidate_source"],
        "suite_id": "SNORRI-MATRIX-LIFECYCLE",
        "conclusion": "success",
        "evidence_digest": "sha256:" + ("c" * 64),
    }
    with_snorri = dict(proof)
    with_snorri["snorri_external_acceptance"] = snorri
    validate_proof_shape(with_snorri)

    # Negatives.
    def must_fail(label: str, fn: Callable[[], None]) -> None:
        try:
            fn()
        except ProofError:
            return
        raise ProofError(f"negative fixture unexpectedly passed: {label}")

    broken = dict(proof)
    broken["candidate_source"] = "deadbeef"
    must_fail("bad source form", lambda: validate_proof_shape(broken))

    stale_api = json.loads(json.dumps(api))
    stale_api["ruleset"]["current"] = False
    must_fail("stale ruleset", lambda: validate_against_api(proof, stale_api))

    moved = json.loads(json.dumps(api))
    moved["ruleset"]["id"] = "other"
    must_fail("moved ruleset", lambda: validate_against_api(proof, moved))

    superseding = json.loads(json.dumps(api))
    superseding["check_attempts"].append(
        {
            "workflow_id": "wf-governed-product",
            "context": "governed-product / product",
            "head_sha": proof["candidate_source"],
            "attempt": 3,
            "conclusion": "failure",
            "status": "completed",
            "check_suite_id": "cs-2",
            "check_run_id": "cr-10",
        }
    )
    must_fail("superseded attempt", lambda: validate_against_api(proof, superseding))

    wrong_repo = json.loads(json.dumps(api))
    wrong_repo["repository_id"] = "other-repo"
    must_fail("repository mismatch", lambda: validate_against_api(proof, wrong_repo))

    snorri_bad = dict(snorri)
    snorri_bad["source_revision"] = "f" * 40
    bad_merge = dict(proof)
    bad_merge["snorri_external_acceptance"] = snorri_bad
    must_fail("snorri source mismatch", lambda: validate_proof_shape(bad_merge))

    must_fail(
        "ddx path refused",
        lambda: refuse_ddx_path(ROOT / ".ddx" / "beads.jsonl", "fixture"),
    )

    # Display names never authorize targets.
    name_only = dict(proof)
    name_only["repository_id"] = "7thsense/fireweed"
    # Shape still accepts strings, but API must match immutable id.
    must_fail(
        "display-name repo not matched by api id",
        lambda: validate_against_api(name_only, api),
    )

    print("verify-github-external-proof self-test: PASS")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--github-proof", type=Path)
    parser.add_argument("--expected-source", type=str)
    parser.add_argument("--repository-id", type=str)
    parser.add_argument("--api-fixture", type=Path, help="Injected GitHub API snapshot (repository-side tests)")
    parser.add_argument("--snorri-proof", type=Path, help="Inject Snorri external-acceptance into proof validation")
    parser.add_argument("--candidate-closure", action="store_true")
    parser.add_argument("--candidate-fixture", type=Path)
    parser.add_argument("--schema", type=Path, default=SCHEMA_PATH)
    args = parser.parse_args(argv)

    try:
        if args.self_test:
            self_test()
            return 0

        if args.github_proof is None or not args.expected_source or not args.repository_id:
            print(
                "usage: require --github-proof --expected-source --repository-id "
                "(or --self-test)",
                file=sys.stderr,
            )
            return 2

        refuse_ddx_path(args.github_proof, "github-proof")
        if args.api_fixture is not None:
            refuse_ddx_path(args.api_fixture, "api-fixture")
        if args.candidate_fixture is not None:
            refuse_ddx_path(args.candidate_fixture, "candidate-fixture")

        schema = load_json(args.schema)
        validate_schema_document(schema)

        proof = load_json(args.github_proof)
        require(isinstance(proof, dict), "proof must be object")
        if args.snorri_proof is not None:
            proof = inject_snorri(proof, args.snorri_proof)

        validate_proof_shape(proof)
        require(proof["candidate_source"] == args.expected_source, "expected-source mismatch")
        require(proof["repository_id"] == args.repository_id, "repository-id mismatch")

        if args.api_fixture is not None:
            api = load_json(args.api_fixture)
            validate_against_api(proof, api)
        else:
            print(
                "verify-github-external-proof: schema/identity only "
                "(pass --api-fixture for freshness/API validation)",
                file=sys.stderr,
            )

        if args.candidate_closure:
            candidate_closure(args.expected_source, fixture=args.candidate_fixture)

        print("verify-github-external-proof: PASS")
        return 0
    except ProofError as error:
        print(f"verify-github-external-proof failed: {error}", file=sys.stderr)
        return 1
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        print(f"verify-github-external-proof failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
