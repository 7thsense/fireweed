#!/usr/bin/env python3
"""Generated Fireweed test-placement and private-surface guard.

The checked-in baseline names assertions that existed before P2a placement.  All
source, Cargo-target, internal-module, and crate-private-item cardinalities are
discovered from the tree; none is encoded here.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib


SCHEMA_VERSION = 1
BASELINE = "scripts/ci/fireweed-test-assertion-baseline.tsv"
SUCCESSORS = "scripts/ci/fireweed-test-assertion-successors.tsv"
MANIFEST = "crates/fireweed/Cargo.toml"
LIB = "crates/fireweed/src/lib.rs"
TEST_ROOT = "crates/fireweed/tests"

OWNER_PATTERNS = {
    "P3b": re.compile(
        r"(?:barrier|async_projection|strict|segment_(?:config|setting)|"
        r"composed_(?:storage|projection)|objectlog_config|open_composed)"
    ),
    "P5a": re.compile(r"(?:reopen|replay|recover|rebuild|restart|rehydrat)"),
    "P6": re.compile(
        r"(?:query|metric|index|peek|read|discover|verify|verification|capabilit|ownership|"
        r"active_scope|range|aggregate|bucket|distribution|segments?|lookup)"
    ),
    "P7": re.compile(
        r"(?:push|claim|ack|nack|complete|fail|retry|release|rearm|renew|reassign|lifecycle|recurr|create_queue|ensure)"
    ),
    "P8": re.compile(r"(?:mutat|update|upsert|gate|reclaim|purge|delete|maintenance)"),
    "P9": re.compile(r"(?:commit|transaction|side_record|explain|multi_claim)"),
}
PRIVATE_CONFIGURATION_ITEMS = {
    "CommitResponseBarrier",
    "ComposedProjectionConfig",
    "ComposedStorageConfig",
    "ObjectLogAuthorityConfig",
    "ObjectLogConfig",
    "ProjectionRecoveryAction",
    "ProjectionRecoveryPolicy",
    "SecretValue",
    "SegmentSettings",
    "open_composed_postgres",
    "open_composed_postgres_async",
    "open_composed_sqlite",
}


class PlacementError(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise PlacementError(message)


def run(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def route_id(target: str, test_id: str) -> str:
    return f"Cargo.toml::fireweed::{target}::test::{test_id}"


def strip_rust_non_code(text: str) -> str:
    """Blank comments and literals while preserving positions and braces."""
    chars = list(text)
    index = 0
    block_depth = 0
    while index < len(chars):
        if block_depth:
            if text.startswith("/*", index):
                chars[index : index + 2] = "  "
                block_depth += 1
                index += 2
            elif text.startswith("*/", index):
                chars[index : index + 2] = "  "
                block_depth -= 1
                index += 2
            else:
                if chars[index] != "\n":
                    chars[index] = " "
                index += 1
            continue
        if text.startswith("//", index):
            end = text.find("\n", index)
            end = len(text) if end < 0 else end
            for offset in range(index, end):
                chars[offset] = " "
            index = end
            continue
        if text.startswith("/*", index):
            chars[index : index + 2] = "  "
            block_depth = 1
            index += 2
            continue

        raw = re.match(r"(?:br|r)(#{0,32})\"", text[index:])
        if raw:
            delimiter = '"' + raw.group(1)
            start = index
            index += raw.end()
            end = text.find(delimiter, index)
            index = len(text) if end < 0 else end + len(delimiter)
            for offset in range(start, index):
                if chars[offset] != "\n":
                    chars[offset] = " "
            continue

        prefix = 1 if text.startswith(('b"', "b'"), index) else 0
        quote_index = index + prefix
        if quote_index < len(text) and chars[quote_index] in {'"', "'"}:
            quote = chars[quote_index]
            # A lifetime such as 'a is not a character literal.
            if quote == "'" and prefix == 0:
                lifetime = re.match(r"'[A-Za-z_][A-Za-z0-9_]*", text[index:])
                if lifetime and not text.startswith("'static'", index):
                    index += lifetime.end()
                    continue
            start = index
            index = quote_index + 1
            escaped = False
            while index < len(text):
                current = chars[index]
                index += 1
                if escaped:
                    escaped = False
                elif current == "\\":
                    escaped = True
                elif current == quote:
                    break
            for offset in range(start, index):
                if chars[offset] != "\n":
                    chars[offset] = " "
            continue
        index += 1
    return "".join(chars)


def discover_private_surface(
    root: Path, sources: list[dict[str, object]]
) -> list[dict[str, object]]:
    path = root / LIB
    raw = path.read_text()
    code = strip_rust_non_code(raw)
    explicit_root_refs: set[str] = set()
    for source in sources:
        source_path = root / str(source["source"])
        if not source_path.is_file():
            continue
        source_code = strip_rust_non_code(source_path.read_text())
        explicit_root_refs.update(
            re.findall(
                r"\b(?:crate|super)\s*::\s*([A-Za-z_][A-Za-z0-9_]*)",
                source_code,
            )
        )
    pattern = re.compile(
        r"\b(?:(pub(?:\s*\([^)]*\))?)\s+)?(?:(?:async|const)\s+)*"
        r"(struct|enum|union|trait|type|fn)\s+([A-Za-z_][A-Za-z0-9_]*)"
    )
    rows = []
    depth = 0
    cursor = 0
    for match in pattern.finditer(code):
        for char in code[cursor : match.start()]:
            if char == "{":
                depth += 1
            elif char == "}":
                depth = max(0, depth - 1)
        cursor = match.start()
        if depth != 0:
            continue
        visibility, declared_kind, item = match.groups()
        normalized_visibility = re.sub(r"\s+", "", visibility or "")
        crate_private = normalized_visibility == "pub(crate)"
        referenced_root_private = item in explicit_root_refs and normalized_visibility != "pub"
        if not crate_private and not referenced_root_private:
            continue
        kind = "function" if declared_kind == "fn" else "type"
        rows.append(
            {
                "id": f"{kind}:{item}",
                "item": item,
                "kind": kind,
                "declaration_kind": declared_kind,
                "path": LIB,
                "line": code.count("\n", 0, match.start()) + 1,
                "fixture_id": "private-" + hashlib.sha256(
                    f"{kind}:{item}".encode()
                ).hexdigest()[:16],
                "expected_diagnostic": "E0603",
                "observed_diagnostic": "not_run",
                "observed_rejected": False,
            }
        )
    return sorted(rows, key=lambda row: str(row["id"]))


def parse_baseline(root: Path) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for line in (root / BASELINE).read_text().splitlines():
        if not line or line.startswith("#") or line == "source\ttest_id":
            continue
        fields = line.split("\t")
        require(len(fields) == 2 and all(fields), f"malformed P2a baseline row: {line}")
        rows.append((fields[0], fields[1]))
    require(len(rows) == len(set(rows)), "duplicate P2a baseline assertion")
    return rows


def parse_successors(root: Path) -> dict[tuple[str, str], dict[str, object]]:
    path = root / SUCCESSORS
    if not path.is_file():
        return {}
    rows: dict[tuple[str, str], dict[str, object]] = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#") or line == "source\ttest_id\tfinal_id\tboundary_owners":
            continue
        fields = line.split("\t")
        require(len(fields) == 4 and all(fields), f"malformed P2a successor row: {line}")
        key = (fields[0], fields[1])
        require(key not in rows, f"duplicate P2a successor: {key}")
        owners = fields[3].split(",")
        require(
            set(owners) <= set(OWNER_PATTERNS) and len(owners) == len(set(owners)),
            f"invalid P2a boundary owner row: {line}",
        )
        rows[key] = {"final_id": fields[2], "boundary_owners": owners}
    return rows


def discover_placements(root: Path) -> list[dict[str, object]]:
    manifest = tomllib.loads((root / MANIFEST).read_text())
    external: dict[str, list[str]] = {}
    for target in manifest.get("test", []):
        path = str(target["path"])
        external.setdefault(path, []).append(str(target["name"]))

    lib_text = (root / LIB).read_text()
    internal: dict[str, list[str]] = {}
    module_pattern = re.compile(
        r"#\s*\[\s*path\s*=\s*\"([^\"]+)\"\s*\]\s*"
        r"(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;"
    )
    for path_text, module in module_pattern.findall(lib_text):
        resolved = ((root / LIB).parent / path_text).resolve()
        try:
            relative = resolved.relative_to(root).as_posix()
        except ValueError:
            continue
        if relative.startswith(TEST_ROOT + "/"):
            internal.setdefault(relative.removeprefix("crates/fireweed/"), []).append(module)

    candidates = set(external) | set(internal)
    candidates.update(
        path.relative_to(root / "crates/fireweed").as_posix()
        for path in (root / TEST_ROOT).glob("*.rs")
    )
    whitebox = root / TEST_ROOT / "whitebox"
    if whitebox.is_dir():
        candidates.update(
            path.relative_to(root / "crates/fireweed").as_posix()
            for path in whitebox.glob("*.rs")
        )

    rows = []
    for source in sorted(candidates):
        full_path = root / "crates/fireweed" / source
        placements = [
            {"kind": "external", "target": target}
            for target in external.get(source, [])
        ] + [
            {"kind": "internal", "module": module}
            for module in internal.get(source, [])
        ]
        rows.append(
            {
                "source": f"crates/fireweed/{source}",
                "logical_source": Path(source).name,
                "exists": full_path.is_file(),
                "placements": placements,
                "placement_count": len(placements),
                "placement": placements[0]["kind"] if len(placements) == 1 else "invalid",
                "private_refs": [],
                "listed_test_ids": [],
                "listed_test_count": 0,
            }
        )
    return rows


def input_digest(root: Path, sources: list[dict[str, object]]) -> str:
    paths = [BASELINE, MANIFEST, LIB]
    if (root / SUCCESSORS).is_file():
        paths.append(SUCCESSORS)
    paths.extend(str(row["source"]) for row in sources if row["exists"])
    digest = hashlib.sha256()
    for relative in sorted(set(paths)):
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update((root / relative).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def discover_private_refs(root: Path, sources: list[dict[str, object]], items: set[str]) -> None:
    for source in sources:
        path = root / str(source["source"])
        if not path.is_file():
            continue
        identifiers = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", strip_rust_non_code(path.read_text())))
        source["private_refs"] = sorted(identifiers & items)


def cargo_fireweed_routes(root: Path) -> tuple[list[dict[str, object]], dict[str, object] | None]:
    command = [
        "rustup", "run", "1.92.0", "cargo", "test", "--manifest-path", "Cargo.toml",
        "--locked", "-p", "fireweed", "--all-features", "--no-run", "--message-format=json",
    ]
    completed = run(command, cwd=root)
    if completed.returncode != 0:
        diagnostics = [
            line.strip() for line in completed.stderr.splitlines()
            if "error" in line.lower()
        ][:20]
        return [], {
            "exit_code": completed.returncode,
            "stderr_sha256": hashlib.sha256(completed.stderr.encode()).hexdigest(),
            "diagnostics": diagnostics,
        }

    routes = []
    seen_executables: set[str] = set()
    for line in completed.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") != "compiler-artifact" or not message.get("executable"):
            continue
        target = message.get("target", {})
        if target.get("name") not in {"fireweed"} and "test" not in target.get("kind", []):
            continue
        executable = str(message["executable"])
        if executable in seen_executables:
            continue
        seen_executables.add(executable)
        listing = run([executable, "--list", "--format", "terse"], cwd=root)
        if listing.returncode != 0:
            return [], {
                "exit_code": listing.returncode,
                "stderr_sha256": hashlib.sha256(listing.stderr.encode()).hexdigest(),
                "diagnostics": [f"failed to list {target.get('name')}: {listing.stderr[-500:]}"],
            }
        for listed in listing.stdout.splitlines():
            if not listed.endswith(": test"):
                continue
            test_id = listed[: -len(": test")]
            target_name = str(target["name"])
            routes.append(
                {
                    "id": route_id(target_name, test_id),
                    "target": target_name,
                    "test_id": test_id,
                    "exact_invocation": [
                        "rustup", "run", "1.92.0", "cargo", "test", "--locked",
                        "-p", "fireweed", "--all-features",
                        *( ["--test", target_name] if "test" in target.get("kind", []) else ["--lib"] ),
                        test_id, "--", "--exact",
                    ],
                }
            )
    ids = [str(row["id"]) for row in routes]
    if len(ids) != len(set(ids)):
        return routes, {"exit_code": 1, "stderr_sha256": "", "diagnostics": ["duplicate listed route"]}
    return sorted(routes, key=lambda row: str(row["id"])), None


def verify_private_visibility(root: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    manifest = tomllib.loads((root / MANIFEST).read_text())
    feature_names = sorted(str(name) for name in manifest.get("features", {}))
    with tempfile.TemporaryDirectory(prefix="fireweed-private-surface-") as directory:
        fixture_root = Path(directory)
        (fixture_root / "src").mkdir()
        features = json.dumps(feature_names)
        dependency_path = json.dumps(str((root / "crates/fireweed").resolve()))
        (fixture_root / "Cargo.toml").write_text(
            "[package]\nname='fireweed-private-surface'\nversion='0.0.0'\nedition='2024'\npublish=false\n"
            "[workspace]\n[dependencies]\n"
            f"fireweed={{path={dependency_path},default-features=false,features={features}}}\n"
        )
        chunks = [rows[index : index + 32] for index in range(0, len(rows), 32)]
        for chunk in chunks:
            line_to_row: dict[int, dict[str, object]] = {}
            source_lines = []
            for row in chunk:
                source_lines.append(
                    f"use fireweed::{row['item']} as PrivateSurface{len(source_lines)};"
                )
                line_to_row[len(source_lines)] = row
            source_lines.append("fn main() {}")
            (fixture_root / "src/main.rs").write_text("\n".join(source_lines) + "\n")
            completed = run(
                [
                    "rustup", "run", "1.92.0", "cargo", "check", "--offline",
                    "--manifest-path", str(fixture_root / "Cargo.toml"),
                    "--target-dir", str(root / "target/public-crate-boundary/p2a"),
                    "--message-format=json",
                ],
                cwd=root,
            )
            for line in completed.stdout.splitlines():
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                diagnostic = message.get("message", {})
                if message.get("reason") != "compiler-message":
                    continue
                code = (diagnostic.get("code") or {}).get("code")
                if code != "E0603":
                    continue
                for span in diagnostic.get("spans", []):
                    if not span.get("is_primary"):
                        continue
                    row = line_to_row.get(int(span.get("line_start", 0)))
                    if row is not None:
                        row["observed_diagnostic"] = "E0603"
                        row["observed_rejected"] = True


def boundary_owners(test_id: str, private_refs: list[str]) -> list[str]:
    haystack = test_id.lower()
    owners = {owner for owner, pattern in OWNER_PATTERNS.items() if pattern.search(haystack)}
    if PRIVATE_CONFIGURATION_ITEMS.intersection(private_refs):
        owners.add("P3b")
    if not owners:
        owners.add("P7")
    return sorted(owners)


def bind_routes(
    sources: list[dict[str, object]],
    routes: list[dict[str, object]],
    baseline: list[tuple[str, str]],
    successors: dict[tuple[str, str], dict[str, object]],
) -> tuple[list[dict[str, object]], list[str]]:
    observed: dict[tuple[str, str], list[tuple[dict[str, object], dict[str, object]]]] = {}
    errors: list[str] = []
    for source in sources:
        matches: list[dict[str, object]] = []
        for placement in source["placements"]:
            if placement["kind"] == "external":
                matches.extend(row for row in routes if row["target"] == placement["target"])
            else:
                prefix = str(placement["module"]) + "::"
                matches.extend(
                    row for row in routes
                    if row["target"] == "fireweed" and str(row["test_id"]).startswith(prefix)
                )
        unique_matches = {str(row["id"]): row for row in matches}
        source["listed_test_ids"] = sorted(unique_matches)
        source["listed_test_count"] = len(unique_matches)
        if source["placement_count"] == 1 and source["listed_test_count"] == 0:
            errors.append(f"zero-test source: {source['source']}")
        for row in unique_matches.values():
            placement = source["placements"][0] if source["placement_count"] == 1 else {"kind": "invalid"}
            if placement["kind"] == "internal":
                prefix = str(placement["module"]) + "::"
                logical_test = str(row["test_id"])[len(prefix) :]
            else:
                logical_test = str(row["test_id"])
            key = (str(source["logical_source"]), logical_test)
            observed.setdefault(key, []).append((source, row))

    baseline_set = set(baseline)
    # Successor rows are part of the governed binding set too: a row whose
    # assertion vanished must fail as stale rather than disappear from output.
    keys = sorted(baseline_set | set(observed) | set(successors))
    bindings = []
    for key in keys:
        candidates = observed.get(key, [])
        source_name, test_id = key
        old = route_id(Path(source_name).stem, test_id)
        successor = successors.get(key)
        final = str(successor["final_id"]) if successor is not None else old
        observed_ids = sorted(str(row["id"]) for _, row in candidates)
        source = candidates[0][0] if len(candidates) == 1 else None
        private_refs = list(source["private_refs"]) if source is not None else []
        placement = str(source["placement"]) if source is not None else "missing"
        if key in baseline_set and not candidates and final not in {str(row["id"]) for row in routes}:
            errors.append(f"lost baseline assertion: {source_name}::{test_id}")
        if len(candidates) > 1:
            errors.append(f"duplicate assertion placement: {source_name}::{test_id}")
        if placement == "internal" and successor is None:
            errors.append(f"unclassified internal boundary assertion: {source_name}::{test_id}")
        if placement != "internal" and successor is not None:
            errors.append(f"stale successor binding for external assertion: {source_name}::{test_id}")
        bindings.append(
            {
                "key": f"{source_name}::{test_id}",
                "source": source_name,
                "test_id": test_id,
                "baseline": key in baseline_set,
                "old_id": old,
                "temporary_id": observed_ids[0] if placement == "internal" and len(observed_ids) == 1 else old,
                "final_id": final,
                "boundary_kind": (
                    "public_external" if placement == "external"
                    else "crate_private_white_box" if private_refs
                    else "temporary_internal_public_intent" if placement == "internal"
                    else "missing"
                ),
                "private_refs": private_refs,
                "boundary_owners": (
                    list(successor["boundary_owners"])
                    if successor is not None
                    else boundary_owners(test_id, private_refs)
                ),
                "observed_ids": observed_ids,
                "observed_count": len(observed_ids),
            }
        )
    return bindings, errors


def generate(root: Path, *, with_cargo: bool, prior: dict[str, object] | None = None) -> dict[str, object]:
    sources = discover_placements(root)
    baseline = parse_baseline(root)
    baseline_sources = {source for source, _ in baseline}
    private_surface = discover_private_surface(root, sources)
    digest = input_digest(root, sources)
    discover_private_refs(root, sources, {str(row["item"]) for row in private_surface})
    errors = []
    for source in sources:
        if not source["exists"]:
            errors.append(f"registered test source is missing: {source['source']}")
        if source["placement_count"] == 0:
            errors.append(f"unregistered test source: {source['source']}")
        elif source["placement_count"] > 1:
            errors.append(f"dual/duplicate source placement: {source['source']}")
        source_path = Path(str(source["source"]))
        if (
            source_path.parent.as_posix() == TEST_ROOT
            and source["placement"] != "external"
        ):
            errors.append(
                f"top-level test source lacks adjacent Cargo [[test]] target: {source['source']}"
            )
        if (
            source_path.parent.as_posix() != TEST_ROOT
            and str(source["logical_source"]) not in baseline_sources
        ):
            errors.append(
                f"new nested test source must extend an existing registered target/module: {source['source']}"
            )
        if source["placement"] == "external" and source["private_refs"]:
            errors.append(
                f"external source references crate-private items: {source['source']}: "
                + ",".join(source["private_refs"])
            )

    routes: list[dict[str, object]] = []
    cargo_failure = None
    if with_cargo:
        routes, cargo_failure = cargo_fireweed_routes(root)
        verify_private_visibility(root, private_surface)
        cargo_status = "listed" if cargo_failure is None else "compile_failure"
    elif prior is not None and prior.get("input_sha256") == digest:
        routes = list(prior.get("routes", []))
        previous_private = {row["id"]: row for row in prior.get("private_surface", [])}
        for row in private_surface:
            previous = previous_private.get(row["id"])
            if previous is not None:
                row["observed_diagnostic"] = previous.get("observed_diagnostic", "not_run")
                row["observed_rejected"] = previous.get("observed_rejected", False)
        cargo_status = str(prior.get("cargo_status", "stale"))
        cargo_failure = prior.get("cargo_failure")
    else:
        cargo_status = "stale"
        cargo_failure = {"diagnostics": ["placement inputs changed; regenerate with --with-cargo"]}
    if cargo_failure is not None:
        errors.append("Fireweed all-features compile/list failed or is stale")
    for row in private_surface:
        if not row["observed_rejected"]:
            errors.append(f"private surface visibility was not rejected: {row['id']}")

    bindings, binding_errors = bind_routes(
        sources, routes, baseline, parse_successors(root)
    )
    errors.extend(binding_errors)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_by": "scripts/ci/fireweed_test_placement.py",
        "baseline": BASELINE,
        "input_sha256": digest,
        "cargo_status": cargo_status,
        "cargo_failure": cargo_failure,
        "private_surface": private_surface,
        "sources": sources,
        "routes": routes,
        "assertion_bindings": bindings,
        "errors": sorted(set(errors)),
    }


def validate(document: object) -> None:
    require(isinstance(document, dict), "Fireweed placement inventory must be an object")
    required = {
        "schema_version", "generated_by", "baseline", "input_sha256", "cargo_status",
        "cargo_failure", "private_surface", "sources", "routes", "assertion_bindings", "errors",
    }
    require(set(document) == required, "Fireweed placement inventory schema drift")
    require(document["schema_version"] == SCHEMA_VERSION, "Fireweed placement schema version")
    require(document["cargo_status"] == "listed", "Fireweed all-features routes were not listed")
    require(document["cargo_failure"] is None, "Fireweed all-features listing failure")
    require(not document["errors"], "Fireweed placement errors: " + "; ".join(document["errors"]))
    route_ids = [row["id"] for row in document["routes"]]
    require(len(route_ids) == len(set(route_ids)), "duplicate Fireweed listed route")
    for source in document["sources"]:
        require(source["exists"], f"missing source {source['source']}")
        require(source["placement_count"] == 1, f"source placement cardinality {source['source']}")
        require(source["listed_test_count"] > 0, f"zero-test source {source['source']}")
    for row in document["private_surface"]:
        require(row["observed_rejected"], f"private item became visible: {row['id']}")
        require(row["observed_diagnostic"] == "E0603", f"unexpected private diagnostic: {row['id']}")
    for binding in document["assertion_bindings"]:
        require(binding["observed_count"] == 1, f"lost/duplicate assertion {binding['key']}")
        require(binding["boundary_owners"], f"unowned boundary assertion {binding['key']}")
        require(binding["old_id"] and binding["temporary_id"] and binding["final_id"], f"incomplete binding {binding['key']}")


def self_test(document: dict[str, object]) -> None:
    validate(document)
    mutations = []
    missing = copy.deepcopy(document)
    missing["sources"][0]["placement_count"] = 0
    mutations.append(("missing placement", missing))
    dual = copy.deepcopy(document)
    dual["sources"][0]["placement_count"] = 2
    mutations.append(("dual placement", dual))
    zero = copy.deepcopy(document)
    zero["sources"][0]["listed_test_count"] = 0
    mutations.append(("zero-test source", zero))
    lost = copy.deepcopy(document)
    lost["assertion_bindings"][0]["observed_count"] = 0
    mutations.append(("lost assertion", lost))
    visible = copy.deepcopy(document)
    visible["private_surface"][0]["observed_rejected"] = False
    mutations.append(("private visibility", visible))
    duplicate = copy.deepcopy(document)
    duplicate["routes"].append(copy.deepcopy(duplicate["routes"][0]))
    mutations.append(("duplicate route", duplicate))
    for name, malformed in mutations:
        try:
            validate(malformed)
        except PlacementError:
            continue
        raise PlacementError(f"{name} negative fixture passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit", action="store_true")
    parser.add_argument("--with-cargo", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    document = generate(root, with_cargo=args.with_cargo)
    try:
        if args.self_test:
            self_test(document)
            print("Fireweed test placement self-test passed")
        elif args.emit:
            print(json.dumps(document, indent=2, sort_keys=True))
        else:
            validate(document)
            print("Fireweed test placement valid")
        return 0
    except PlacementError as error:
        print(f"Fireweed test placement failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
