#!/usr/bin/env python3
"""Check real Cargo production edges; exercise manifest and compiler controls."""
from __future__ import annotations
import argparse
import glob
import os
import re
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
PURE = {
    "fireweed-core": {"bytes", "sha2", "postcard", "serde", "serde_json", "axon-esf"},
    "fireweed-engine": {"async-trait", "fireweed-core", "axon-esf", "bytes", "base64", "sha2", "getrandom", "serde", "serde_json", "postcard", "serde_path_to_error", "futures"},
    "fireweed-conformance": {"fireweed-core", "fireweed-engine", "bytes", "tokio", "serde_json", "futures"},
    "fireweed-queue-spool": {"fireweed-core", "fireweed-engine", "tempfile", "postcard", "sha2", "serde", "tokio"},
}
ADAPTERS = {
    "fireweed-postgres": {"async-trait", "tokio-postgres", "tokio", "fireweed-core", "fireweed-engine", "fireweed-queue-spool", "fireweed-projection", "fireweed-objectlog", "axon-esf", "postgres", "native-tls", "postgres-native-tls", "serde", "serde_json", "postcard", "bytes", "futures", "sha2"},
    "fireweed-turso": {"fireweed-projection", "fireweed-core", "fireweed-engine", "fireweed-queue-spool", "fireweed-relational", "bytes", "serde_json", "tokio", "turso", "turso_core"},
    "fireweed-objectlog": {"async-trait", "futures", "fireweed-core", "fireweed-engine", "fireweed-projection", "fireweed-queue-spool", "object-log", "aws-sdk-s3", "tokio", "serde", "serde_json", "bytes"},
}
# One exact existing dependency, not an adapter-wide exemption. Remove when the
# named legacy composition retires; new source locations are not permitted.
LEGACY = {
    ("fireweed-postgres", "fireweed-objectlog"): {
        "file": "crates/fireweed-postgres/src/async_objectlog_postgres.rs",
        "owner": "PostgreSQL adapter maintainer",
        "removal": "retire AsyncObjectLogPostgresBackend during API-007 cutover",
    },
}


def production_dependencies(document: dict, workspace: dict) -> set[str]:
    result = set()
    sections = [document.get("dependencies", {}), document.get("build-dependencies", {})]
    for target in document.get("target", {}).values():
        sections.extend([target.get("dependencies", {}), target.get("build-dependencies", {})])
    for section in sections:
        for alias, entry in section.items():
            if isinstance(entry, dict) and entry.get("workspace"):
                entry = workspace.get("dependencies", {}).get(alias, entry)
            result.add(entry.get("package", alias) if isinstance(entry, dict) else alias)
    return result


def load_graph(root: Path) -> dict[str, set[str]]:
    document = tomllib.loads((root / "Cargo.toml").read_text())
    workspace = document.get("workspace", {})
    excluded = {Path(path).resolve() for pattern in workspace.get("exclude", []) for path in glob.glob(str(root / pattern))}
    paths = []
    if "package" in document:
        paths.append(root / "Cargo.toml")
    for member in workspace.get("members", []):
        paths.extend(Path(path) / "Cargo.toml" for path in glob.glob(str(root / member)) if Path(path).is_dir() and (not glob.has_magic(member) or Path(path).resolve() not in excluded))
    pending = sorted(set(paths))
    seen = set()
    graph = {}
    while pending:
        path = pending.pop()
        path = path.resolve()
        if path in seen:
            continue
        seen.add(path)
        package = tomllib.loads(path.read_text())
        name = package["package"]["name"]
        if name in graph:
            raise ValueError(f"duplicate package {name}")
        graph[name] = production_dependencies(package, workspace)
        # Cargo automatically makes in-workspace path dependencies members,
        # including dev dependencies. Discover membership independently of which
        # edges are production edges; workspace.exclude remains authoritative.
        sections = [package.get(kind, {}) for kind in ("dependencies", "build-dependencies", "dev-dependencies")]
        for target in package.get("target", {}).values():
            sections.extend(target.get(kind, {}) for kind in ("dependencies", "build-dependencies", "dev-dependencies"))
        for section in sections:
            for alias, entry in section.items():
                origin = path.parent
                if isinstance(entry, dict) and entry.get("workspace"):
                    entry = workspace.get("dependencies", {}).get(alias, entry)
                    origin = root
                if not isinstance(entry, dict) or "path" not in entry:
                    continue
                folder = (origin / entry["path"]).resolve()
                if folder.is_relative_to(root.resolve()) and folder not in excluded:
                    pending.append(folder / "Cargo.toml")
    return graph


def violations(graph: dict[str, set[str]]) -> list[str]:
    errors = []
    for source, dependencies in sorted(graph.items()):
        for target in sorted(dependencies):
            if source in PURE and target not in PURE[source]:
                errors.append(f"forbidden pure edge: {source} -> {target}")
            if source in ADAPTERS and target not in ADAPTERS[source]:
                errors.append(f"forbidden adapter edge: {source} -> {target}")
            if source == "fireweed-postgres" and target == "fireweed-objectlog" and (source, target) not in LEGACY:
                errors.append(f"unrecorded legacy edge: {source} -> {target}")
    # Iterative traversal keeps dependency graph checking independent of stack depth.
    complete = set()
    for start in sorted(graph):
        if start in complete:
            continue
        active = set()
        stack = [(start, False)]
        while stack:
            node, exiting = stack.pop()
            if exiting:
                active.remove(node)
                complete.add(node)
                continue
            if node in active:
                errors.append(f"production dependency cycle at {node}")
                break
            if node in complete:
                continue
            active.add(node)
            stack.append((node, True))
            stack.extend((child, False) for child in sorted(graph[node] & graph.keys(), reverse=True))
    return errors


def dependency_tokens(root: Path, package: str, protected: set[str]) -> set[str]:
    tokens = {name.replace("-", "_") for name in protected}
    manifest = root / "crates" / package / "Cargo.toml"
    if not manifest.is_file():
        return tokens
    workspace_manifest = root / "Cargo.toml"
    workspace = tomllib.loads(workspace_manifest.read_text()).get("workspace", {}) if workspace_manifest.is_file() else {}
    doc = tomllib.loads(manifest.read_text())
    sections = [doc.get("dependencies", {}), doc.get("build-dependencies", {})]
    for target in doc.get("target", {}).values():
        sections.extend([target.get("dependencies", {}), target.get("build-dependencies", {})])
    for section in sections:
        for alias, entry in section.items():
            if isinstance(entry, dict) and entry.get("workspace"):
                entry = workspace.get("dependencies", {}).get(alias, entry)
            name = entry.get("package", alias) if isinstance(entry, dict) else alias
            if name in protected or name.startswith("aws-sdk-"):
                tokens.add(alias.replace("-", "_"))
    return tokens


def source_controls(root: Path) -> list[str]:
    errors = []
    for (source, target), exception in LEGACY.items():
        folder = root / "crates" / source / "src"
        tokens = dependency_tokens(root, source, {target})
        for path in folder.rglob("*.rs"):
            if any(re.search(r"(?<![A-Za-z0-9_])" + re.escape(token) + r"\b", path.read_text()) for token in tokens) and str(path.relative_to(root)) != exception["file"]:
                errors.append(f"legacy edge expanded to {path.relative_to(root)}")
    queue = root / "crates/fireweed/src/queue.rs"
    if queue.is_file():
        text = queue.read_text()
        for sdk in sorted(dependency_tokens(root, "fireweed", {"tokio-postgres", "turso_core", "turso", "postgres", "aws-sdk-s3"})) + ["aws_sdk_[A-Za-z0-9_]*"]:
            if re.search(r"(?<![A-Za-z0-9_])" + sdk + r"\s*::", text) or re.search(r"\b(?:use|extern\s+crate)\s+(?:\{[^;]*?)?\b" + sdk + r"\b", text):
                errors.append(f"vendor SDK in concrete queue facade: {sdk}")
    return errors


def manifest_controls(scratch: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="boundary-manifest-", dir=scratch) as name:
        root = Path(name)
        (root / "Cargo.toml").write_text('[workspace]\nmembers=["core","engine"]\n')
        for path, package, dependency in [("core", "fireweed-core", "serde=\"1\""), ("engine", "fireweed-engine", 'fireweed-core={path="../core"}')]:
            (root / path).mkdir()
            (root / path / "Cargo.toml").write_text(f'[package]\nname="{package}"\nversion="0.1.0"\n[dependencies]\n{dependency}\n')
        if violations(load_graph(root)):
            raise ValueError("allowed real-manifest edge rejected")
        core = root / "core/Cargo.toml"
        core.write_text(core.read_text() + '\n[target.\'cfg(unix)\'.dependencies]\nfireweed-engine={path="../engine"}\n')
        errors = violations(load_graph(root))
        if not any("forbidden pure edge" in error for error in errors) or not any("cycle" in error for error in errors):
            raise ValueError("forbidden target edge/cycle control was not detected")
        core.write_text('[package]\nname="fireweed-core"\nversion="0.1.0"\n[dependencies]\nrenamed_sdk={package="tokio-postgres",version="0.7"}\n')
        if not any("tokio-postgres" in error for error in violations(load_graph(root))):
            raise ValueError("renamed SDK dependency escaped checker")

        # Real Cargo path-member discovery, not only literal workspace.members.
        (root / "Cargo.toml").write_text('[workspace]\nmembers=["engine"]\nexclude=["excluded"]\n')
        (root / "engine/Cargo.toml").write_text('[package]\nname="fireweed-engine"\nversion="0.1.0"\n[dependencies]\nfireweed-core={path="../core"}\n')
        (root / "core/Cargo.toml").write_text('[package]\nname="fireweed-core"\nversion="0.1.0"\n[dependencies]\nfireweed-engine={path="../engine"}\n')
        discovered = load_graph(root)
        if set(discovered) != {"fireweed-core", "fireweed-engine"} or not any("cycle" in e for e in violations(discovered)):
            raise ValueError("automatic path-member forbidden cycle escaped")
        (root / "excluded").mkdir()
        (root / "excluded/Cargo.toml").write_text('[package]\nname="excluded"\nversion="0.1.0"\n')
        (root / "engine/Cargo.toml").write_text((root / "engine/Cargo.toml").read_text() + 'excluded={path="../excluded"}\n')
        if "excluded" in load_graph(root):
            raise ValueError("workspace.exclude ignored")
        (root / "Cargo.toml").write_text('[workspace]\nmembers=["*"]\nexclude=["excluded"]\n')
        if "excluded" in load_graph(root):
            raise ValueError("glob member workspace.exclude ignored")
        (root / "Cargo.toml").write_text('[workspace]\nmembers=["engine","excluded"]\nexclude=["excluded"]\n')
        if "excluded" not in load_graph(root):
            raise ValueError("explicit member incorrectly excluded")

    # Backend-neutral future combinators stay with the conformance workload owner.
    with tempfile.TemporaryDirectory(prefix="boundary-conformance-", dir=scratch) as name:
        root = Path(name)
        (root / "Cargo.toml").write_text('[workspace]\nmembers=["conformance","core"]\n')
        for folder, package in [("conformance", "fireweed-conformance"), ("core", "fireweed-core")]:
            (root / folder).mkdir()
            (root / folder / "Cargo.toml").write_text(f'[package]\nname="{package}"\nversion="0.1.0"\n[dependencies]\n')
        manifest = root / "conformance/Cargo.toml"
        base = manifest.read_text()
        manifest.write_text(base + 'futures="0.3"\n')
        if violations(load_graph(root)):
            raise ValueError("backend-neutral conformance future combinators rejected")
        for dependency in ['backend={package="fireweed-turso",version="1"}', 'sdk={package="aws-sdk-s3",version="1"}']:
            manifest.write_text(base + dependency + '\n')
            if not any("forbidden pure edge: fireweed-conformance" in error for error in violations(load_graph(root))):
                raise ValueError("conformance backend/SDK edge escaped")
        manifest.write_text(base)
        core = root / "core/Cargo.toml"
        core.write_text(core.read_text() + 'futures="0.3"\n')
        if not any("forbidden pure edge: fireweed-core -> futures" in error for error in violations(load_graph(root))):
            raise ValueError("core future dependency escaped")



def adapter_source_controls(scratch: Path) -> None:
    for source, target in [("fireweed-postgres", "object-log"), ("fireweed-turso", "fireweed-objectlog"), ("fireweed-objectlog", "sqlx")]:
        if not violations({source: {target}}):
            raise ValueError(f"adapter dependency control escaped: {source}->{target}")
    with tempfile.TemporaryDirectory(prefix="boundary-source-", dir=scratch) as name:
        root = Path(name)
        pg = root / "crates/fireweed-postgres/src/new.rs"
        pg.parent.mkdir(parents=True)
        pg.write_text("use fireweed_objectlog as log; use log::ObjectLogEngineStore;")
        if not any("legacy edge expanded" in e for e in source_controls(root)):
            raise ValueError("aliased legacy object-log source escaped")
        pg.unlink()
        queue = root / "crates/fireweed/src/queue.rs"
        queue.parent.mkdir(parents=True)
        for text in ("use tokio_postgres as pg; use pg::Client;", "use turso_core::Database;", "use {tokio_postgres as pg};"):
            queue.write_text(text)
            if not any("vendor SDK" in e for e in source_controls(root)):
                raise ValueError("aliased/root vendor source escaped: " + text)

        pg_manifest = root / "crates/fireweed-postgres/Cargo.toml"
        pg_manifest.write_text('[package]\nname="fireweed-postgres"\nversion="0.1.0"\n[dependencies]\nlog_authority={package="fireweed-objectlog",version="1"}\n')
        pg.write_text("use log_authority::ObjectLogEngineStore;")
        if not any("legacy edge expanded" in e for e in source_controls(root)):
            raise ValueError("Cargo-renamed legacy source escaped")
        facade_manifest = root / "crates/fireweed/Cargo.toml"
        facade_manifest.write_text('[package]\nname="fireweed"\nversion="0.1.0"\n[target.\'cfg(unix)\'.dependencies]\nsql_vendor={package="tokio-postgres",version="0.7"}\n')
        queue.write_text("use sql_vendor::Client;")
        if not any("vendor SDK" in e for e in source_controls(root)):
            raise ValueError("Cargo-renamed conditional SDK source escaped")


def compiler_controls(root: Path, scratch: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="boundary-rust-", dir=scratch) as name:
        fixture = Path(name)
        (fixture / "src").mkdir()
        dependency = str(root / "crates/fireweed").replace("\\", "\\\\").replace('"', '\\"')
        (fixture / "Cargo.toml").write_text(f'[package]\nname="queue-boundary-control"\nversion="0.0.0"\nedition="2024"\n[workspace]\n[dependencies]\nfireweed={{path="{dependency}",default-features=false}}\n')
        shutil.copyfile(root / "Cargo.lock", fixture / "Cargo.lock")
        env = dict(os.environ, CARGO_TARGET_DIR=str(root / "target"))
        source = fixture / "src/main.rs"
        command = ["cargo", "check", "--quiet", "--manifest-path", str(fixture / "Cargo.toml")]
        source.write_text('fn main(){let queue:Option<fireweed::queue::Queue>=None; let _=queue.map(|q|q.clone());}\n')
        allowed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=120)
        if allowed.returncode:
            raise ValueError("public consumer control failed: " + allowed.stderr[-4000:])
        source.write_text('fn main(){let queue:Option<fireweed::queue::Queue>=None; let _=queue.unwrap().store;}\n')
        forbidden = subprocess.run(command, env=env, capture_output=True, text=True, timeout=120)
        if forbidden.returncode == 0 or "E0616" not in forbidden.stderr:
            raise ValueError("private store access did not fail with the compiler privacy error")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--controls", action="store_true")
    arguments = parser.parse_args()
    try:
        errors = violations(load_graph(ROOT)) + source_controls(ROOT)
        if errors:
            raise ValueError("\n".join(errors))
        if arguments.controls:
            scratch = ROOT / "target/queue-boundary-controls"
            scratch.mkdir(parents=True, exist_ok=True)
            manifest_controls(scratch)
            adapter_source_controls(scratch)
            compiler_controls(ROOT, scratch)
        print("queue module boundaries: PASS" + (" (manifest, cycle, alias and compiler controls)" if arguments.controls else ""))
        return 0
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"queue module boundaries: FAIL: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
