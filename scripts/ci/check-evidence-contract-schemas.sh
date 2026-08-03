#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

python3 - <<'PY'
import copy
import hashlib
import json
from pathlib import Path
import re


semantic_schema_path = Path("schemas/evidence-semantic-requirement.schema.json")
overlay_schema_path = Path("schemas/evidence-route-overlay.schema.json")
semantic_schema = json.loads(semantic_schema_path.read_text())
overlay_schema = json.loads(overlay_schema_path.read_text())

identifier = re.compile(r"^[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*$")
capabilities = {
    "core",
    "durable_log_replay",
    "projection_reopen",
    "relational_reconnect",
    "eventual_apply",
    "in_process_log_read",
}
durability_classes = {"A", "B", "A-or-B"}
artifact_classes = {"fixture", "run-owned", "promoted"}
required_results = {"pass", "explicit-na", "measured"}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def exact_keys(value, expected, label):
    require(isinstance(value, dict), f"{label} must be an object")
    actual = set(value)
    require(actual == set(expected), f"{label} keys {sorted(actual)} != {sorted(expected)}")


def validate_schema_shape(schema, required, item_required):
    require(schema["$schema"] == "https://json-schema.org/draft/2020-12/schema", "draft mismatch")
    require(schema["additionalProperties"] is False, "top-level unknown fields must fail")
    require(set(schema["required"]) == set(required), "top-level required fields drift")
    item = schema["properties"][required[-1]]["items"]
    require(item["additionalProperties"] is False, "item unknown fields must fail")
    require(set(item["required"]) == set(item_required), "item required fields drift")


def validate_semantic(document):
    exact_keys(document, {"schema_version", "requirements"}, "semantic document")
    require(document["schema_version"] == 1, "semantic schema_version must equal 1")
    require(isinstance(document["requirements"], list), "requirements must be an array")
    seen = set()
    for index, requirement in enumerate(document["requirements"]):
        label = f"requirement[{index}]"
        exact_keys(
            requirement,
            {
                "id",
                "governing_assertion",
                "durability_class",
                "capability",
                "evidence_semantics",
            },
            label,
        )
        requirement_id = requirement["id"]
        require(isinstance(requirement_id, str) and identifier.fullmatch(requirement_id), f"{label} id")
        require(requirement_id not in seen, f"duplicate requirement id {requirement_id}")
        seen.add(requirement_id)
        require(
            isinstance(requirement["governing_assertion"], str)
            and requirement["governing_assertion"],
            f"{label} governing assertion",
        )
        require(requirement["durability_class"] in durability_classes, f"{label} durability")
        require(requirement["capability"] in capabilities, f"{label} capability")
        semantics = requirement["evidence_semantics"]
        exact_keys(
            semantics,
            {"artifact_class", "required_result", "stale_input_fails"},
            f"{label}.evidence_semantics",
        )
        require(semantics["artifact_class"] in artifact_classes, f"{label} artifact class")
        require(semantics["required_result"] in required_results, f"{label} result")
        require(semantics["stale_input_fails"] is True, f"{label} stale input policy")
    return seen


def validate_overlay(document, declared):
    exact_keys(
        document,
        {"schema_version", "semantic_requirements_sha256", "assignments"},
        "overlay document",
    )
    require(document["schema_version"] == 1, "overlay schema_version must equal 1")
    require(
        isinstance(document["semantic_requirements_sha256"], str)
        and re.fullmatch(r"[0-9a-f]{64}", document["semantic_requirements_sha256"]),
        "overlay semantic digest",
    )
    require(isinstance(document["assignments"], list), "assignments must be an array")
    seen = set()
    for index, assignment in enumerate(document["assignments"]):
        label = f"assignment[{index}]"
        exact_keys(assignment, {"requirement_id", "routes"}, label)
        requirement_id = assignment["requirement_id"]
        require(requirement_id in declared, f"overlay references undeclared {requirement_id}")
        require(requirement_id not in seen, f"duplicate overlay assignment {requirement_id}")
        seen.add(requirement_id)
        routes = assignment["routes"]
        require(isinstance(routes, list) and routes, f"{label} routes must be non-empty")
        require(all(isinstance(route, str) and route for route in routes), f"{label} route")
        require(len(routes) == len(set(routes)), f"{label} routes must be unique")


def must_reject(callback, label):
    try:
        callback()
    except (AssertionError, KeyError, TypeError):
        return
    raise AssertionError(f"negative fixture unexpectedly passed: {label}")


validate_schema_shape(
    semantic_schema,
    ["schema_version", "requirements"],
    ["id", "governing_assertion", "durability_class", "capability", "evidence_semantics"],
)
validate_schema_shape(
    overlay_schema,
    ["schema_version", "semantic_requirements_sha256", "assignments"],
    ["requirement_id", "routes"],
)

semantic = {
    "schema_version": 1,
    "requirements": [
        {
            "id": "AC-TXN-1",
            "governing_assertion": "successful Class A mutation is durable and visible",
            "durability_class": "A",
            "capability": "durable_log_replay",
            "evidence_semantics": {
                "artifact_class": "run-owned",
                "required_result": "pass",
                "stale_input_fails": True,
            },
        }
    ],
}
declared = validate_semantic(semantic)
semantic_bytes = json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
empty_overlay = {
    "schema_version": 1,
    "semantic_requirements_sha256": hashlib.sha256(semantic_bytes).hexdigest(),
    "assignments": [],
}
validate_overlay(empty_overlay, declared)

unknown = copy.deepcopy(semantic)
unknown["unexpected"] = True
must_reject(lambda: validate_semantic(unknown), "unknown semantic field")

duplicate = copy.deepcopy(semantic)
duplicate["requirements"].append(copy.deepcopy(duplicate["requirements"][0]))
duplicate["requirements"][1]["governing_assertion"] = "different text cannot hide duplicate identity"
must_reject(lambda: validate_semantic(duplicate), "duplicate requirement id")

routed_semantic = copy.deepcopy(semantic)
routed_semantic["requirements"][0]["routes"] = ["cargo test --workspace"]
must_reject(lambda: validate_semantic(routed_semantic), "route embedded in semantic requirement")

undeclared_overlay = copy.deepcopy(empty_overlay)
undeclared_overlay["assignments"] = [
    {"requirement_id": "AC-TXN-999", "routes": ["cargo test --workspace"]}
]
must_reject(lambda: validate_overlay(undeclared_overlay, declared), "undeclared overlay requirement")

print("evidence contract schemas verified")
PY
