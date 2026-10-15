"""Offline source-topology controls for the reviewed crates.io Turso dependency."""
from __future__ import annotations

import copy
from pathlib import Path
import re
import subprocess
import tomllib

VERSION = "0.8.2"
PACKAGES = {"turso", "turso_core", "turso_sdk_kit", "turso_sync_engine", "turso_sync_sdk_kit"}
SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
MANIFESTS = ("Cargo.toml", "crates/fireweed-bench/Cargo.toml", "scripts/ci/fixtures/public-crate-boundary/Cargo.toml")
LOCKS = tuple(str(Path(path).with_name("Cargo.lock")) for path in MANIFESTS)
VENDOR_FILES = {"vendor/.gitattributes", "vendor/README.md", "vendor/FIREWEED_VENDOR.json"}


class StockTursoError(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise StockTursoError(message)


def validate(manifests: dict, adapter: dict, locks: dict, vendor: set[str]) -> None:
    require(set(manifests) == set(MANIFESTS), "missing stock Turso manifest")
    require(vendor == VENDOR_FILES, "vendor must contain only retired-fork provenance and metadata")
    for name in ("turso", "turso_core"):
        dependency = adapter.get("dependencies", {}).get(name)
        require(isinstance(dependency, dict), f"missing stock {name} dependency")
        require(dependency.get("version") == VERSION, f"unreviewed {name} version")
        require(not {"path", "git", "registry", "registry-index", "workspace", "package"}.intersection(dependency), f"non-stock {name} dependency")
    for path, manifest in manifests.items():
        tables = list(manifest.get("patch", {}).values()) + [manifest.get("replace", {})]
        for table in tables:
            require(isinstance(table, dict), f"invalid dependency override table: {path}")
            for alias, specification in table.items():
                package = specification.get("package", alias) if isinstance(specification, dict) else alias
                require(not any(str(value).split(":")[0].startswith("turso") for value in (alias, package)), f"Turso dependency override in {path}")
    checksums: dict[str, str] = {}
    require(set(locks) == set(LOCKS), "missing stock Turso lockfile")
    for path, lock in locks.items():
        for name in PACKAGES:
            entries = [entry for entry in lock.get("package", []) if entry.get("name") == name]
            require(len(entries) == 1, f"missing or duplicate {name} in {path}")
            entry = entries[0]
            require(entry.get("version") == VERSION and entry.get("source") == SOURCE, f"non-stock {name} lock entry in {path}")
            checksum = entry.get("checksum", "")
            require(isinstance(checksum, str) and re.fullmatch(r"[0-9a-f]{64}", checksum) is not None, f"invalid {name} checksum in {path}")
            require(checksums.setdefault(name, checksum) == checksum, f"inconsistent {name} checksums")


def read_and_validate(root: Path) -> None:
    def read(path: str) -> dict:
        return tomllib.loads((root / path).read_text())
    tracked = set(filter(None, subprocess.check_output(["git", "ls-files", "-z", "--", "vendor/"], cwd=root, text=True).split("\0")))
    # Include untracked, nonignored files so an uncommitted fork cannot pass.
    tracked.update(filter(None, subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z", "--", "vendor/"], cwd=root, text=True).split("\0")))
    for path in tracked:
        require((root / path).is_file() and not (root / path).is_symlink(), f"vendor metadata must be regular files: {path}")
    validate({path: read(path) for path in MANIFESTS}, read("crates/fireweed-turso/Cargo.toml"), {path: read(path) for path in LOCKS}, tracked)


def self_test() -> None:
    manifests = {path: {} for path in MANIFESTS}
    adapter = {"dependencies": {name: {"version": VERSION} for name in ("turso", "turso_core")}}
    locks = {path: {"package": [{"name": name, "version": VERSION, "source": SOURCE, "checksum": "a" * 64} for name in sorted(PACKAGES)]} for path in LOCKS}
    validate(manifests, adapter, locks, VENDOR_FILES)
    cases = []
    for key, value in (("path", "../fork"), ("git", "https://example.invalid/fork"), ("registry", "custom"), ("version", "0.7.2")):
        changed = copy.deepcopy(adapter)
        changed["dependencies"]["turso"][key] = value
        cases.append((manifests, changed, locks, VENDOR_FILES))
    for section in ("patch", "replace"):
        changed = copy.deepcopy(manifests)
        override = {"innocent_alias": {"package": "turso_core", "path": "../fork"}}
        changed[MANIFESTS[0]][section] = {"crates-io": override} if section == "patch" else override
        cases.append((changed, adapter, locks, VENDOR_FILES))
    for mutation in ("missing", "duplicate", "version", "source", "checksum", "mismatch"):
        changed = copy.deepcopy(locks)
        entries = changed[LOCKS[0]]["package"]
        if mutation == "missing": entries.pop()
        elif mutation == "duplicate": entries.append(copy.deepcopy(entries[0]))
        else:
            field, value = {"version": ("version", "0.7.2"), "source": ("source", "git+https://example.invalid/fork"), "checksum": ("checksum", "bogus"), "mismatch": ("checksum", "b" * 64)}[mutation]
            entries[0][field] = value
        cases.append((manifests, adapter, changed, VENDOR_FILES))
    cases.append((manifests, adapter, locks, VENDOR_FILES | {"vendor/turso_core/lib.rs"}))
    for number, case in enumerate(cases):
        try:
            validate(*case)
        except StockTursoError:
            continue
        raise StockTursoError(f"stock Turso negative control passed: {number}")
