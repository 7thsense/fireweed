#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
document="${1:-${repo_root}/docs/helix/02-design/public-artifact-topology.md}"

python3 - "$repo_root" "$document" <<'PY'
import collections
import json
import pathlib
import subprocess
import sys

repo_root = pathlib.Path(sys.argv[1])
document = pathlib.Path(sys.argv[2])
start_marker = "<!-- workspace-package-inventory:start -->"
end_marker = "<!-- workspace-package-inventory:end -->"

text = document.read_text(encoding="utf-8")
if text.count(start_marker) != 1 or text.count(end_marker) != 1:
    raise SystemExit("topology inventory must contain exactly one start and end marker")

inventory = text.split(start_marker, 1)[1].split(end_marker, 1)[0]
rows = []
for line in inventory.splitlines():
    line = line.strip()
    if not line.startswith("|"):
        continue
    cells = [cell.strip() for cell in line.strip("|").split("|")]
    if cells[0] == "Current package" or set(cells[0]) <= {"-", ":"}:
        continue
    if len(cells) != 7:
        raise SystemExit(f"topology inventory row must have 7 columns: {line}")
    rows.append(cells)

if not rows:
    raise SystemExit("topology inventory has no package rows")

current_names = [row[0] for row in rows]
target_names = [row[1] for row in rows]

def duplicates(values):
    counts = collections.Counter(values)
    return sorted(value for value, count in counts.items() if count > 1)

duplicate_current = duplicates(current_names)
duplicate_target = duplicates(target_names)
if duplicate_current:
    raise SystemExit("duplicate workspace package classification: " + ", ".join(duplicate_current))
if duplicate_target:
    raise SystemExit("duplicate target package name: " + ", ".join(duplicate_target))

metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--no-deps", "--format-version", "1"],
    cwd=repo_root,
    text=True,
))
workspace_ids = set(metadata["workspace_members"])
workspace_names = {
    package["name"] for package in metadata["packages"]
    if package["id"] in workspace_ids
}
documented_names = set(current_names)
omitted = sorted(workspace_names - documented_names)
unknown = sorted(documented_names - workspace_names)
if omitted:
    raise SystemExit("workspace packages omitted from topology: " + ", ".join(omitted))
if unknown:
    raise SystemExit("topology entries are not root workspace packages: " + ", ".join(unknown))

allowed_classes = {"publishable", "repository-only", "experimental", "private"}
orders = []
publishable = []
for current, target, package_class, registry, order, features, _ in rows:
    if package_class not in allowed_classes:
        raise SystemExit(f"invalid class for {current}: {package_class}")
    if not target.startswith("fireweed"):
        raise SystemExit(f"target package does not use Fireweed namespace: {target}")
    if package_class == "publishable":
        publishable.append(current)
        if registry == "-" or order == "-" or not features:
            raise SystemExit(f"publishable package lacks registry, order, or feature policy: {current}")
        try:
            orders.append(int(order))
        except ValueError as error:
            raise SystemExit(f"invalid publish order for {current}: {order}") from error
    elif order != "-":
        raise SystemExit(f"non-publishable package has a publish order: {current}")

if publishable != ["pqueue"]:
    raise SystemExit("ADR-009 requires pqueue to be the sole publishable current package")
if sorted(orders) != list(range(1, len(orders) + 1)):
    raise SystemExit("publish order must be unique and contiguous from 1")

print(f"public artifact topology valid: {len(rows)} workspace packages classified exactly once")
PY
