#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "scripts/ci/storage-remediation-inventory.json"
AUTHORITY = ROOT / "docs/helix/04-build/storage-authority-manifest.json"
WORKSPACES = [
    "Cargo.toml",
    "crates/fireweed-bench/Cargo.toml",
    "tools/fireweed-turso-compat-probe/Cargo.toml",
]
SOURCE_SUFFIXES = {".rs", ".sh", ".py", ".toml", ".yml", ".yaml", ".lock"}
EXCLUDED_DIGEST_PATHS = {
    "scripts/ci/storage-remediation-inventory.json",
}
META_POLICY_SOURCES = {
    "scripts/ci/inventory-storage-remediation.py",
    "scripts/ci/storage-remediation-policy.py",
}

PRODUCT_WORKFLOW_REQUIREMENTS = [
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
    "operator_validation_tests",
]


def run(command: list[str], *, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )


def tracked_files() -> list[str]:
    raw = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
    )
    return sorted(path for path in raw.decode().split("\0") if path)


def source_files() -> list[str]:
    return [
        path
        for path in tracked_files()
        if Path(path).suffix in SOURCE_SUFFIXES
        and path not in EXCLUDED_DIGEST_PATHS
        and not path.startswith(".ddx/")
    ]


def source_digest(paths: list[str]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.encode())
        digest.update(b"\0")
        digest.update((ROOT / path).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def owner_for(path: str, *, performance: bool = False, e3: bool = False) -> tuple[str, list[str]]:
    if e3 and path.startswith("crates/fireweed-conformance/"):
        return "P0f", ["P0f"]
    if path.startswith("crates/fireweed-bench/") or performance:
        return "P15" if performance else "P10pb", ["P15" if performance else "P10pb", "P2f"]
    if path.startswith("crates/fireweed-postgres/"):
        return "P10pp", ["P10pp", "P2f"]
    if path.startswith("crates/fireweed-server/"):
        return "P10ps", ["P10ps", "P2f"]
    if path.startswith("crates/fireweed-conformance/"):
        return "P10pc", ["P10pc", "P2f"]
    if path.startswith("crates/fireweed/"):
        return "P10pf", ["P10pf", "P2f"]
    if path.startswith(".github/workflows/"):
        exclusive = []
        lower = path.lower()
        if "turso" in lower:
            exclusive.append("P13t")
        if "nightly" in lower:
            exclusive.append("P13a")
        if "release" in lower:
            exclusive.append("P17r")
        return "P10w", exclusive + ["P10w", "P2f"]
    if path.startswith("docs/site/"):
        return "P19", ["P19", "P17a", "P2f"]
    if path.startswith("scripts/perf/") or "performance" in path.lower():
        return "P15", ["P15", "P2f"]
    if path.startswith("scripts/") or path.startswith("examples/"):
        return "P10px", ["P10px", "P2f"]
    return "P3-P9/P15", ["P3-P9/P15", "P2f"]


def debt_row(
    category: str,
    path: str,
    line: int,
    identity: str,
    *,
    performance: bool = False,
    e3: bool = False,
    status: str = "debt",
    detail: str = "",
) -> dict[str, object]:
    owner, chain = owner_for(path, performance=performance, e3=e3)
    raw_id = f"{category}:{path}:{line}:{identity}"
    return {
        "id": hashlib.sha256(raw_id.encode()).hexdigest()[:20],
        "category": category,
        "path": path,
        "line": line,
        "identity": identity,
        "status": status,
        "owner": owner,
        "dependency_chain": chain,
        "detail": detail,
    }


def cargo_target_argument(kind: list[str], name: str) -> list[str]:
    if "test" in kind:
        return ["--test", name]
    if "example" in kind:
        return ["--example", name]
    if "bin" in kind:
        return ["--bin", name]
    return ["--lib"]


def cargo_routes(
    manifest: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object] | None]:
    def failure_for(error: subprocess.CalledProcessError) -> dict[str, object]:
        diagnostic_lines = [
            line.strip()
            for line in error.stderr.splitlines()
            if "error" in line.lower() or "lock file" in line.lower()
        ][:20]
        return {
            "manifest": manifest,
            "exit_code": error.returncode,
            "stderr_sha256": hashlib.sha256(error.stderr.encode()).hexdigest(),
            "diagnostics": diagnostic_lines,
        }

    try:
        metadata_result = run(
            [
                "rustup",
                "run",
                "1.92.0",
                "cargo",
                "metadata",
                "--manifest-path",
                manifest,
                "--format-version",
                "1",
                "--no-deps",
                "--locked",
            ]
        )
    except subprocess.CalledProcessError as error:
        return [], [], failure_for(error)
    metadata = json.loads(metadata_result.stdout)
    packages = {package["id"]: package for package in metadata["packages"]}
    try:
        artifacts = run(
            [
                "rustup",
                "run",
                "1.92.0",
                "cargo",
                "test",
                "--manifest-path",
                manifest,
                "--workspace",
                "--locked",
                "--no-run",
                "--message-format=json",
            ]
        )
    except subprocess.CalledProcessError as error:
        return [], [], failure_for(error)
    routes: list[dict[str, object]] = []
    seen_executables: set[str] = set()
    for line in artifacts.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("reason") != "compiler-artifact" or not message.get("executable"):
            continue
        if not message.get("profile", {}).get("test"):
            continue
        executable = message["executable"]
        if executable in seen_executables:
            continue
        seen_executables.add(executable)
        package = packages.get(message["package_id"])
        if package is None:
            continue
        target = message["target"]
        listing = run([executable, "--list", "--format", "terse"])
        for listed in listing.stdout.splitlines():
            if ": " not in listed:
                continue
            test_id, listed_kind = listed.rsplit(": ", 1)
            if listed_kind not in {"test", "benchmark"}:
                continue
            selector = cargo_target_argument(target["kind"], target["name"])
            invocation = [
                "rustup",
                "run",
                "1.92.0",
                "cargo",
                "test",
                "--manifest-path",
                manifest,
                "--locked",
                "-p",
                package["name"],
                *selector,
                test_id,
                "--",
                "--exact",
            ]
            route_id = "::".join(
                [manifest, package["name"], target["name"], listed_kind, test_id]
            )
            routes.append(
                {
                    "id": route_id,
                    "workspace": manifest,
                    "package": package["name"],
                    "target": target["name"],
                    "target_kind": target["kind"],
                    "harness_id": test_id,
                    "listed_kind": listed_kind,
                    "exact_invocation": invocation,
                    "expected_ran": 1,
                }
            )

    doc_routes: list[dict[str, object]] = []
    for package in packages.values():
        if not any("lib" in target["kind"] or "proc-macro" in target["kind"] for target in package["targets"]):
            continue
        command = [
            "rustup",
            "run",
            "1.92.0",
            "cargo",
            "test",
            "--manifest-path",
            manifest,
            "--locked",
            "-p",
            package["name"],
            "--doc",
            "--",
            "--list",
            "--format",
            "terse",
        ]
        try:
            listing = run(command)
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"rustdoc listing failed for {package['name']}: {error.stderr}"
            ) from error
        for listed in listing.stdout.splitlines():
            if not listed.endswith(": test"):
                continue
            test_id = listed[: -len(": test")]
            match = re.match(r"(.+?) - (?:(.+?) )?\(line (\d+)\)$", test_id)
            source = ""
            owner_item = ""
            line_number = 0
            block_digest = ""
            behavior = "run"
            if match:
                root_candidate = ROOT / match.group(1)
                package_candidate = Path(package["manifest_path"]).parent / match.group(1)
                source_path = root_candidate if root_candidate.is_file() else package_candidate
                try:
                    source = source_path.resolve().relative_to(ROOT).as_posix()
                except ValueError:
                    source = source_path.as_posix()
                owner_item = match.group(2) or "<crate>"
                line_number = int(match.group(3))
                if source_path.is_file():
                    source_lines = source_path.read_text(errors="replace").splitlines()
                    window = "\n".join(source_lines[max(0, line_number - 8) : line_number + 24])
                    normalized = "\n".join(line.strip() for line in window.splitlines())
                    block_digest = hashlib.sha256(normalized.encode()).hexdigest()
                    if "compile_fail" in window:
                        behavior = "expected_compiler_rejection"
                    elif "```ignore" in window:
                        behavior = "ignored_unlisted_debt"
                    elif "```no_run" in window:
                        behavior = "compile_only_debt"
            invocation = [
                "rustup",
                "run",
                "1.92.0",
                "cargo",
                "test",
                "--manifest-path",
                manifest,
                "--locked",
                "-p",
                package["name"],
                "--doc",
                test_id,
                "--",
                "--exact",
            ]
            execution = run(invocation)
            execution_output = execution.stdout + "\n" + execution.stderr
            pass_counts = [int(value) for value in re.findall(r"(\d+) passed", execution_output)]
            observed_ran = max(pass_counts, default=0)
            doc_routes.append(
                {
                    "id": "::".join([manifest, package["name"], "rustdoc", test_id]),
                    "workspace": manifest,
                    "package": package["name"],
                    "harness_id": test_id,
                    "source": source,
                    "line": line_number,
                    "owner_item": owner_item,
                    "normalized_block_sha256": block_digest,
                    "behavioral_pass": behavior,
                    "exact_invocation": invocation,
                    "expected_ran": 1,
                    "observed_ran": observed_ran,
                }
            )
    return routes, doc_routes, None


def fireweed_registration_debt() -> list[dict[str, object]]:
    manifest_path = ROOT / "crates/fireweed/Cargo.toml"
    manifest = tomllib.loads(manifest_path.read_text())
    registered = {entry["path"] for entry in manifest.get("test", [])}
    rows = []
    for source in sorted((ROOT / "crates/fireweed/tests").glob("*.rs")):
        relative = source.relative_to(ROOT / "crates/fireweed").as_posix()
        if relative not in registered:
            rows.append(
                debt_row(
                    "source_registration",
                    source.relative_to(ROOT).as_posix(),
                    1,
                    relative,
                    detail="top-level source lacks adjacent [[test]] registration while autotests=false",
                )
            )
    return rows


def cargo_machete_exception_debt(paths: list[str]) -> list[dict[str, object]]:
    rows = []
    explicit_owners = {
        ("crates/fireweed-server/Cargo.toml", "fireweed-turso"): (
            "P12a",
            ["P12a", "P2f"],
        ),
    }
    for path in paths:
        if not path.endswith("Cargo.toml"):
            continue
        manifest_text = (ROOT / path).read_text()
        manifest = tomllib.loads(manifest_text)
        ignored = (
            manifest.get("package", {})
            .get("metadata", {})
            .get("cargo-machete", {})
            .get("ignored", [])
        )
        for dependency in ignored:
            line = next(
                (
                    index
                    for index, line_text in enumerate(manifest_text.splitlines(), start=1)
                    if dependency in line_text and "ignored" in line_text
                ),
                1,
            )
            row = debt_row(
                "cargo_machete_exception",
                path,
                line,
                dependency,
                detail="ignored unused dependency must have a removal owner and cannot survive closure",
            )
            explicit_owner = explicit_owners.get((path, dependency))
            if explicit_owner is not None:
                row["owner"], row["dependency_chain"] = explicit_owner
            rows.append(row)
    return rows


def scan_source_debt(paths: list[str]) -> dict[str, list[dict[str, object]]]:
    debt: dict[str, list[dict[str, object]]] = {
        "source_registration": fireweed_registration_debt(),
        "cargo_machete_exceptions": cargo_machete_exception_debt(paths),
        "ignored_tests": [],
        "harness_skips": [],
        "quarantine": [],
        "opt_ins": [],
        "loud_skips": [],
        "no_ops": [],
        "source_guards": [],
        "workflow_inline": [],
        "release_repeat_contract": [],
        "rustdoc_unlisted_or_compile_only": [],
        "workspace_listing_failures": [],
        "public_release_gate_failures": [],
    }
    skip_pattern = re.compile(
        r"SKIPPED|skipped|skip(?:ping)?[ :] |not[_ -]configured|not configured|"
        r"missing[^\n]{0,50}(?:URL|endpoint|fixture|service|binary)|unavailable|"
        r"(?:none|no\s+[^\n]{0,40})(?:are\s+)?registered|no\s+targets|scaffold passes",
        re.IGNORECASE,
    )
    early_pattern = re.compile(r"\breturn\s*(?:;|Ok\s*\(\s*\)\s*;)|\bexit\s+0\b|\bcontinue\s*;", re.DOTALL)
    source_guard_pattern = re.compile(
        r"source_revision|source[_-]root|expected[_-]source|expected[_-]revision|"
        r"git\s+rev-parse|git_commit|producing[_-]root",
        re.IGNORECASE,
    )
    for path in paths:
        # These two files contain the registry vocabulary and discovery regexes as data. Scanning their
        # string literals as product behavior recursively manufactures false debt; their executable
        # behavior is covered by policy self-tests and shape fixtures instead.
        if path in META_POLICY_SOURCES:
            continue
        resolved = ROOT / path
        text = resolved.read_text(errors="replace")
        lines = text.splitlines()
        performance = path.startswith("scripts/perf/") or "performance" in path.lower()
        e3 = "e3" in path.lower()
        if path.endswith(".rs"):
            for match in re.finditer(r"#\s*\[\s*ignore(?:\s*=\s*[^\]]+)?\]", text):
                line = text.count("\n", 0, match.start()) + 1
                debt["ignored_tests"].append(
                    debt_row("ignored_test", path, line, match.group(0), performance=performance, e3=e3)
                )
            for match in re.finditer(r"^\s*//[!/]\s*```([^\n]*)$", text, re.MULTILINE):
                fence_info = match.group(1).strip()
                if "ignore" in fence_info or "no_run" in fence_info:
                    line = text.count("\n", 0, match.start()) + 1
                    debt["rustdoc_unlisted_or_compile_only"].append(
                        debt_row(
                            "rustdoc_compile_only" if "no_run" in fence_info else "rustdoc_unlisted",
                            path,
                            line,
                            fence_info or "rust",
                            detail="rustdoc fence must have a listed exact behavioral route; compile-only/ignored is debt",
                        )
                    )
        for index, line_text in enumerate(lines, start=1):
            if "quarantine" in line_text.lower():
                debt["quarantine"].append(
                    debt_row("quarantine", path, index, line_text.strip()[:120], performance=performance, e3=e3)
                )
            if source_guard_pattern.search(line_text):
                row = debt_row(
                    "source_guard",
                    path,
                    index,
                    line_text.strip()[:120],
                    performance=performance,
                    e3=e3,
                )
                # P0f activates the conformance E3 producer and removes its early-success path.
                # P15 owns the later performance/source-binding policy for that same producer.
                if e3 and path.startswith("crates/fireweed-conformance/"):
                    row["owner"] = "P15"
                    row["dependency_chain"] = ["P15", "P2f"]
                debt["source_guards"].append(row)
            if skip_pattern.search(line_text):
                context = "\n".join(lines[index - 1 : min(len(lines), index + 24)])
                lowered = context.lower()
                discovery_negative = any(
                    marker in lowered
                    for marker in (".skip(", "skip point", "skip_point", "fault", "chaos")
                )
                early = early_pattern.search(context) is not None and not discovery_negative
                row = debt_row(
                    "loud_skip",
                    path,
                    index,
                    line_text.strip()[:120],
                    performance=performance,
                    e3=e3,
                    status="discovery_negative" if discovery_negative else "debt",
                    detail=("early_success=true" if early else "early_success=false"),
                )
                debt["loud_skips"].append(row)
                if early:
                    debt["harness_skips"].append(copy_row(row, "harness_skip"))
                if "ignore" in lowered or "env::" in lowered or "std::env" in lowered:
                    debt["opt_ins"].append(copy_row(row, "opt_in"))
            if path.startswith(".github/workflows/") and re.search(
                r"continue-on-error:|\|\|\s*true|exit\s+0|if:.*false|skip",
                line_text,
                re.IGNORECASE,
            ):
                debt["workflow_inline"].append(
                    debt_row("workflow_inline", path, index, line_text.strip()[:120])
                )
        if path.endswith(".sh"):
            effective = [
                line.strip()
                for line in lines
                if line.strip()
                and not line.lstrip().startswith("#")
                and not line.startswith("#!")
                and not line.startswith("set ")
            ]
            if effective and all(
                re.fullmatch(r"(?:true|:|exit\s+0|echo\s+.*)", line) for line in effective
            ):
                debt["no_ops"].append(
                    debt_row("no_op", path, 1, "shell_success_only", performance=performance)
                )
    return debt


def public_release_gate_failures() -> list[dict[str, object]]:
    completed = subprocess.run(
        ["bash", "scripts/verify-public-identity.sh"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if completed.returncode == 0:
        return []
    rows = []
    for output_line in completed.stdout.splitlines():
        match = re.match(r"^([^:]+):(\d+):\s*(.+)$", output_line)
        if match is None:
            continue
        path, line, detail = match.groups()
        rows.append(
            debt_row(
                "public_identity_gate_failure",
                path,
                int(line),
                detail,
                detail="pre-existing public-release functional gate failure; repair in the path owner's bead",
            )
        )
    if not rows:
        raise RuntimeError(
            "public identity gate failed without path:line diagnostics: "
            + completed.stdout[-1000:]
        )
    return rows


def copy_row(row: dict[str, object], category: str) -> dict[str, object]:
    copied = dict(row)
    copied["category"] = category
    copied["id"] = hashlib.sha256(f"{category}:{row['id']}".encode()).hexdigest()[:20]
    return copied


def add_release_repeat_debt(debt: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    suite_path = ROOT / "scripts/ci/release-repeat-suites.toml"
    data = tomllib.loads(suite_path.read_text())
    suites = data.get("suites", [])
    legacy_rows = []
    for index, suite in enumerate(suites):
        name = suite.get("name", "")
        command = suite.get("command", [])
        legacy_rows.append(
            {
                "name": name,
                "command": command,
                "kind": suite.get("kind"),
                "executable": suite.get("executable"),
            }
        )
        debt["no_ops"].append(
            debt_row(
                "legacy_false_green",
                "scripts/ci/release-repeat-suites.toml",
                index + 1,
                name,
                detail="hand-maintained row points at a success-only fixture and is non-executable debt",
            )
        )
    for name in PRODUCT_WORKFLOW_REQUIREMENTS:
        debt["release_repeat_contract"].append(
            debt_row(
                "release_repeat_contract",
                "scripts/ci/release-repeat-suites.toml",
                1,
                name,
                detail="P2r must generate one real campaign-aware binding",
            )
        )
    caller_matches = []
    for path in tracked_files():
        if Path(path).suffix not in SOURCE_SUFFIXES:
            continue
        text = (ROOT / path).read_text(errors="replace")
        for index, line in enumerate(text.splitlines(), start=1):
            if "release-repeat-suites.toml" in line or "verify-product-workflow-names.sh" in line:
                caller_matches.append({"path": path, "line": index, "text": line.strip()})
    return {
        "legacy_rows": legacy_rows,
        "required_contract_debts": PRODUCT_WORKFLOW_REQUIREMENTS,
        "current_verifier_semantics": "missing_only_required_minus_names",
        "required_jobs_executed_or_counted": False,
        "callers": caller_matches,
    }


def inventory(with_cargo: bool) -> dict[str, object]:
    paths = source_files()
    harness_routes: list[dict[str, object]] = []
    rustdoc_routes: list[dict[str, object]] = []
    workspace_rows = []
    workspace_failures: list[dict[str, object]] = []
    for manifest in WORKSPACES:
        require_path = ROOT / manifest
        if not require_path.is_file():
            raise RuntimeError(f"workspace manifest missing: {manifest}")
        workspace_row = {
            "manifest": manifest,
            "routing": "root" if manifest == "Cargo.toml" else "independent",
            "listing_status": "not_run",
            "listing_error_sha256": "",
        }
        workspace_rows.append(workspace_row)
        if with_cargo:
            harness, rustdoc, failure = cargo_routes(manifest)
            harness_routes.extend(harness)
            rustdoc_routes.extend(rustdoc)
            if failure is None:
                workspace_row["listing_status"] = "listed"
            else:
                workspace_row["listing_status"] = "compile_failure_debt"
                workspace_row["listing_error_sha256"] = failure["stderr_sha256"]
                workspace_failures.append(failure)
    debt = scan_source_debt(paths)
    if with_cargo:
        debt["public_release_gate_failures"] = public_release_gate_failures()
    for route in rustdoc_routes:
        if route["observed_ran"] != 1:
            route_path = str(route["source"] or route["workspace"])
            debt["rustdoc_unlisted_or_compile_only"].append(
                debt_row(
                    "rustdoc_exact_execution_failure",
                    route_path,
                    int(route["line"]),
                    str(route["harness_id"]),
                    detail="listed ID was invoked with --exact but did not report exactly one passing behavioral test",
                )
            )
    for failure in workspace_failures:
        manifest = str(failure["manifest"])
        debt["workspace_listing_failures"].append(
            debt_row(
                "workspace_listing_failure",
                manifest,
                1,
                str(failure["stderr_sha256"]),
                performance=manifest == "crates/fireweed-bench/Cargo.toml",
                detail="; ".join(str(item) for item in failure["diagnostics"]),
            )
        )
    repeat_contract = add_release_repeat_debt(debt)
    if not with_cargo and OUTPUT.is_file():
        prior = json.loads(OUTPUT.read_text())
        harness_routes = prior.get("harness_routes", [])
        rustdoc_routes = prior.get("rustdoc_routes", [])
        prior_workspaces = {
            row["manifest"]: row for row in prior.get("workspaces", [])
        }
        for row in workspace_rows:
            previous = prior_workspaces.get(row["manifest"])
            if previous is not None:
                row["listing_status"] = previous.get("listing_status", "not_run")
                row["listing_error_sha256"] = previous.get("listing_error_sha256", "")
        for previous in prior.get("debt_registries", {}).get(
            "workspace_listing_failures", []
        ):
            debt["workspace_listing_failures"].append(previous)
        for previous in prior.get("debt_registries", {}).get(
            "rustdoc_unlisted_or_compile_only", []
        ):
            if previous.get("category") == "rustdoc_exact_execution_failure":
                debt["rustdoc_unlisted_or_compile_only"].append(previous)
        debt["public_release_gate_failures"] = prior.get("debt_registries", {}).get(
            "public_release_gate_failures", []
        )
    return {
        "schema_version": 1,
        "generated_by": "scripts/ci/inventory-storage-remediation.py",
        "authority_manifest_sha256": hashlib.sha256(AUTHORITY.read_bytes()).hexdigest(),
        "source_inventory_sha256": source_digest(paths),
        "workspaces": workspace_rows,
        "harness_routes": sorted(harness_routes, key=lambda row: row["id"]),
        "rustdoc_routes": sorted(rustdoc_routes, key=lambda row: row["id"]),
        "debt_registries": debt,
        "release_repeat_quarantine": repeat_contract,
        "discovery_negatives": [
            "Iterator::skip and iterator .skip(...) calls",
            "fault-injection skip operations",
            "SQLite chaos skip-point vocabulary",
            "P2 inventory/policy source vocabulary (covered by policy self-tests and shape fixtures)",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--emit", action="store_true")
    parser.add_argument("--with-cargo", action="store_true")
    args = parser.parse_args()
    if not args.write and not args.emit:
        parser.error("one of --write or --emit is required")
    document = inventory(with_cargo=args.with_cargo)
    rendered = json.dumps(document, indent=2, sort_keys=True) + "\n"
    if args.write:
        OUTPUT.write_text(rendered)
    if args.emit:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
