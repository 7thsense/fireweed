#!/usr/bin/env python3
"""Check the microsite's example documents and the pages that show them.

Every example is a set of YAML documents under docs/site/examples/src/,
listed in docs/site/_meta/example-manifest.yaml. This check fails when:

- a listed file is missing, or a file under examples/src/ is not listed;
- a document doesn't parse, or breaks API-008's grammar: unknown keys,
  unknown statements, objects or reads the shared schema doesn't declare,
  a write without request_id and issued_at, names that aren't valid;
- a committed page differs from what render_site.py renders from the
  sources, so every page shows each document unchanged.

It doesn't run documents against PostgreSQL. The executor's own tests do
that for the statements these examples use.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "docs/site"
MANIFEST = SITE / "_meta/example-manifest.yaml"
EX_SRC = SITE / "examples/src"

NAME = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
TYPES = {"text", "int", "float", "bool", "timestamp", "json", "text_set"}

SCHEMA_KEYS = {"version", "request_retention", "queues", "tables", "reads"}
QUEUE_KEYS = {"max_attempts", "retention", "priority", "fields"}
TABLE_KEYS = {"key", "sequenced", "expire_after_write", "fields"}
FIELD_KEYS = {"type", "index", "merge"}
READ_KEYS = {"name", "version", "params", "select"}
PROGRAM_KEYS = {"request_id", "issued_at", "durability", "params", "statements"}
EXPECT_KEYS = {"on_unmet", "min", "max"}

# Statement name -> (allowed keys, key naming its object, object kind).
STATEMENTS: dict[str, tuple[set[str], str | None, str | None]] = {
    "insert": ({"into", "rows", "on_conflict"}, "into", None),
    "claim": ({"from", "limit", "lease", "worker", "where", "eligible_at"}, "from", "queue"),
    "settle": (
        {"queue", "outcome", "items", "after", "set", "body", "require_all"},
        "queue",
        "queue",
    ),
    "renew": ({"queue", "items", "lease", "require_all"}, "queue", "queue"),
    "update": (
        {
            "target", "where", "ids", "keys", "set", "body", "leased",
            "limit", "on_limit", "expect", "override_merge",
        },
        "target",
        None,
    ),
    "delete": ({"target", "where", "ids", "keys", "limit", "expect"}, "target", None),
    "assert": ({"target", "where", "ids", "keys", "expect"}, "target", None),
    "select": (
        {"from", "where", "fields", "aggregate", "group_by", "order_by", "limit"},
        "from",
        None,
    ),
    "read": ({"name", "version", "params"}, None, None),
    "pause": ({"queue"}, "queue", "queue"),
    "resume": ({"queue"}, "queue", "queue"),
}
WRITES = {"insert", "claim", "settle", "renew", "update", "delete", "pause", "resume"}
REQUIRED = {
    "insert": {"into", "rows"},
    "claim": {"from", "limit", "lease", "worker"},
    "settle": {"queue", "outcome", "items"},
    "renew": {"queue", "items", "lease"},
    "update": {"target"},
    "delete": {"target"},
    "assert": {"target", "expect"},
    "select": {"from", "limit"},
    "read": {"name", "version"},
    "pause": {"queue"},
    "resume": {"queue"},
}
OUTCOMES = {"complete", "fail", "retry", "release"}


def load_manifest() -> dict:
    return yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))


def listed_files(manifest: dict) -> list[str]:
    files = [manifest["schema"], *manifest.get("snippets", [])]
    for example in manifest["examples"]:
        files.extend(doc["path"] for doc in example["documents"])
    return files


def unknown(where: str, keys, allowed: set[str]) -> list[str]:
    return [f"{where}: unknown key {k!r}" for k in keys if k not in allowed]


def check_schema(path: str, s: dict) -> tuple[list[str], dict[str, str], set[tuple[str, int]]]:
    errors = unknown(path, s, SCHEMA_KEYS)
    if not isinstance(s.get("version"), int):
        errors.append(f"{path}: version must be an integer")
    objects: dict[str, str] = {}
    for kind, decls, allowed in (
        ("queue", s.get("queues") or {}, QUEUE_KEYS),
        ("table", s.get("tables") or {}, TABLE_KEYS),
    ):
        for name, decl in decls.items():
            where = f"{path}: {kind} {name}"
            if not NAME.match(name):
                errors.append(f"{where}: invalid name")
            if name in objects:
                errors.append(f"{where}: declared twice")
            objects[name] = kind
            errors += unknown(where, decl, allowed)
            fields = decl.get("fields") or {}
            for field, fd in fields.items():
                if not NAME.match(field):
                    errors.append(f"{where}: invalid field name {field!r}")
                errors += unknown(f"{where}.{field}", fd, FIELD_KEYS)
                if fd.get("type") not in TYPES:
                    errors.append(f"{where}.{field}: unknown type {fd.get('type')!r}")
            for key in decl.get("key", []):
                if key not in fields:
                    errors.append(f"{where}: key field {key!r} is not declared")
    reads: set[tuple[str, int]] = set()
    for read in s.get("reads") or []:
        where = f"{path}: read {read.get('name')}"
        errors += unknown(where, read, READ_KEYS)
        reads.add((read.get("name"), read.get("version")))
        errors += check_statement(where, {"select": read.get("select")}, objects, reads)
    return errors, objects, reads


def check_statement(where: str, st, objects: dict[str, str], reads) -> list[str]:
    if not isinstance(st, dict) or len(st) != 1:
        return [f"{where}: a statement is a map with one key"]
    (name, body), = st.items()
    if name not in STATEMENTS:
        return [f"{where}: unknown statement {name!r}"]
    allowed, obj_key, kind = STATEMENTS[name]
    if not isinstance(body, dict):
        return [f"{where}: {name} takes a map"]
    errors = unknown(f"{where}: {name}", body, allowed)
    missing = REQUIRED[name] - set(body)
    if missing:
        errors.append(f"{where}: {name} is missing {sorted(missing)}")
    if obj_key:
        obj = body.get(obj_key)
        if obj not in objects:
            errors.append(f"{where}: {name} names undeclared object {obj!r}")
        elif kind and objects[obj] != kind:
            errors.append(f"{where}: {name} needs a {kind}, {obj!r} is a {objects[obj]}")
    if name == "read" and (body.get("name"), body.get("version")) not in reads:
        errors.append(f"{where}: read {body.get('name')} v{body.get('version')} is not declared")
    if name == "settle" and body.get("outcome") not in OUTCOMES:
        errors.append(f"{where}: unknown settle outcome {body.get('outcome')!r}")
    if "expect" in body:
        errors += unknown(f"{where}: {name}.expect", body["expect"], EXPECT_KEYS)
    if name in {"update", "delete", "assert"} and not ({"where", "ids", "keys"} & set(body)):
        errors.append(f"{where}: {name} needs where, ids or keys")
    return errors


def check_program(path: str, p: dict, objects: dict[str, str], reads) -> list[str]:
    errors = unknown(path, p, PROGRAM_KEYS)
    statements = p.get("statements")
    if not isinstance(statements, list) or not statements:
        return errors + [f"{path}: statements must be a non-empty list"]
    writes = False
    for i, st in enumerate(statements):
        errors += check_statement(f"{path}: statement {i}", st, objects, reads)
        if isinstance(st, dict) and set(st) & WRITES:
            writes = True
    if writes and "request_id" not in p:
        errors.append(f"{path}: a program that writes needs request_id")
    if "request_id" in p and "issued_at" not in p:
        errors.append(f"{path}: request_id needs issued_at")
    return errors


def main() -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import render_site  # noqa: E402

    manifest = load_manifest()
    errors: list[str] = []

    listed = listed_files(manifest)
    for rel in listed:
        if not (EX_SRC / rel).is_file():
            errors.append(f"missing example source: docs/site/examples/src/{rel}")
    on_disk = {p.relative_to(EX_SRC).as_posix() for p in EX_SRC.rglob("*") if p.is_file()}
    for rel in sorted(on_disk - set(listed)):
        errors.append(f"unlisted example source: docs/site/examples/src/{rel}")
    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        return 1

    schema_path = manifest["schema"]
    schema = yaml.safe_load((EX_SRC / schema_path).read_text(encoding="utf-8"))
    schema_errors, objects, reads = check_schema(schema_path, schema)
    errors += schema_errors
    documents = 1
    for example in manifest["examples"]:
        for doc in example["documents"]:
            documents += 1
            program = yaml.safe_load((EX_SRC / doc["path"]).read_text(encoding="utf-8"))
            errors += check_program(doc["path"], program, objects, reads)

    for page, content in render_site.render_all().items():
        path = SITE / page
        if not path.is_file():
            errors.append(f"missing page: docs/site/{page} (run python3 scripts/site/render_site.py)")
        elif path.read_text(encoding="utf-8") != content:
            errors.append(f"stale page: docs/site/{page} (run python3 scripts/site/render_site.py)")
    rendered = set(render_site.render_all())
    for path in sorted(SITE.rglob("*.html")):
        rel = path.relative_to(SITE).as_posix()
        if rel not in rendered:
            errors.append(f"page not produced by render_site.py: docs/site/{rel}")

    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        print(f"{len(errors)} example check(s) failed", file=sys.stderr)
        return 1
    print(
        f"examples ok: {len(manifest['examples'])} example(s), {documents} document(s), "
        f"{len(rendered)} page(s) match their sources"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
