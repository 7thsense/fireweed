#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <concurrency-registry.toml>" >&2
  exit 2
fi

registry_path="$1"

python3 - "$registry_path" <<'PY'
import sys
import tomllib

path = sys.argv[1]

try:
    with open(path, "rb") as handle:
        data = tomllib.load(handle)
except Exception as exc:
    print(f"failed to read TOML registry: {exc}", file=sys.stderr)
    sys.exit(1)

def fail(message):
    print(message, file=sys.stderr)
    sys.exit(1)

if data.get("schema_version") != 1:
    fail("schema_version must be 1")
for field in ("reviewer", "reviewed_at", "workspace_scope"):
    if not str(data.get(field, "")).strip():
        fail(f"{field} is required")

audits = data.get("audits")
if not isinstance(audits, list) or not audits:
    fail("at least one [[audits]] row is required")

for audit in audits:
    crate = str(audit.get("crate", "")).strip()
    if not crate:
        fail("audits.crate is required")
    source_globs = audit.get("source_globs_checked")
    if not isinstance(source_globs, list) or not source_globs:
        fail(f"{crate} must list source_globs_checked")
    custom = audit.get("custom_structures", [])
    loom_tests = audit.get("loom_tests", [])
    no_custom = bool(audit.get("no_custom_concurrency", False))
    if no_custom and custom:
        fail(f"{crate} cannot combine no_custom_concurrency with custom_structures")
    if not no_custom and not custom:
        fail(f"{crate} must either set no_custom_concurrency or list custom_structures")
    if len(loom_tests) < len(custom):
        fail(f"{crate} must list a loom_tests entry for every custom structure")

print(f"validated concurrency registry: {path}")
PY
