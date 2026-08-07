#!/usr/bin/env bash
# verify-build-closure.sh — candidate-safe build-closure check + operator tracker audit.
#
# Modes
# -----
#   candidate (default for release/CI callers)
#     Never reads `.ddx/**`. Verifies a checked-in or supplied fixture of required
#     bead statuses. Safe for PR/release gates that must not depend on live tracker
#     state.
#
#   operator
#     Live tracker audit against `.ddx/beads.jsonl`. Operator-only; not for required
#     CI/release paths. Release/CI callers must use candidate mode only.
#
# Usage:
#   bash scripts/ci/verify-build-closure.sh --mode candidate --fixture scripts/ci/fixtures/closure/all-closed.json
#   bash scripts/ci/verify-build-closure.sh --mode candidate --fixture path.json --aggregate NAME
#   bash scripts/ci/verify-build-closure.sh --mode operator --aggregate pqueue-131eadfa
#   bash scripts/ci/verify-build-closure.sh --fixture path.json            # legacy → candidate
#   bash scripts/ci/verify-build-closure.sh --aggregate pqueue-131eadfa    # legacy → operator
#
# Exit codes:
#   0  closure verified
#   1  open required items / missing evidence
#   2  usage / mode error
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

MODE=""
FIXTURE=""
AGGREGATE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --fixture)
            FIXTURE="$2"
            shift 2
            ;;
        --aggregate)
            AGGREGATE="$2"
            shift 2
            ;;
        -h|--help)
            sed -n '2,30p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

# Legacy flag combinations → mode selection.
if [[ -z "$MODE" ]]; then
    if [[ -n "$FIXTURE" ]]; then
        MODE="candidate"
    elif [[ -n "$AGGREGATE" ]]; then
        MODE="operator"
    else
        echo "verify-build-closure: require --mode candidate|operator (or legacy --fixture / --aggregate)" >&2
        exit 2
    fi
fi

case "$MODE" in
    candidate|operator) ;;
    *)
        echo "verify-build-closure: unknown mode '$MODE' (expected candidate|operator)" >&2
        exit 2
        ;;
esac

# ---------------------------------------------------------------------------
# candidate — fixture only; hard fail if any path would touch .ddx/**
# ---------------------------------------------------------------------------
if [[ "$MODE" == "candidate" ]]; then
    if [[ -z "$FIXTURE" ]]; then
        echo "verify-build-closure: candidate mode requires --fixture <path>" >&2
        exit 2
    fi
    if [[ ! -f "$FIXTURE" ]]; then
        echo "verify-build-closure: fixture not found: $FIXTURE" >&2
        exit 2
    fi

    # Refuse any candidate invocation that names a .ddx path as the fixture.
    case "$FIXTURE" in
        *.ddx/*|.ddx/*|*/.ddx/*)
            echo "verify-build-closure: candidate mode must never read .ddx/** (fixture=$FIXTURE)" >&2
            exit 2
            ;;
    esac

    python3 - "$FIXTURE" "$AGGREGATE" <<'PY'
import json, sys
from pathlib import Path

fixture_path = Path(sys.argv[1])
aggregate_filter = sys.argv[2] or None

# Candidate mode must not open .ddx for any reason.
if ".ddx" in fixture_path.parts:
    print(f"candidate mode refused .ddx path: {fixture_path}", file=sys.stderr)
    sys.exit(2)

data = json.loads(fixture_path.read_text())
if not isinstance(data, dict):
    print("fixture must be a JSON object", file=sys.stderr)
    sys.exit(2)

required = data.get("required")
if not isinstance(required, list):
    print("fixture.required must be a list", file=sys.stderr)
    sys.exit(2)

if aggregate_filter is not None:
    fixture_agg = data.get("aggregate")
    if fixture_agg is not None and fixture_agg != aggregate_filter:
        print(
            f"fixture aggregate {fixture_agg!r} does not match --aggregate {aggregate_filter!r}",
            file=sys.stderr,
        )
        sys.exit(1)

open_items = [
    item.get("id", "<missing-id>")
    for item in required
    if not isinstance(item, dict) or item.get("status") != "closed"
]
if open_items:
    print("open required beads: " + ", ".join(open_items), file=sys.stderr)
    sys.exit(1)

print("fixture closure verified")
PY
    exit 0
fi

# ---------------------------------------------------------------------------
# operator — live tracker audit (reads .ddx/beads.jsonl only in this mode)
# ---------------------------------------------------------------------------
if [[ -z "$AGGREGATE" ]]; then
    echo "verify-build-closure: operator mode requires --aggregate <id>" >&2
    exit 2
fi

if [[ -n "$FIXTURE" ]]; then
    echo "verify-build-closure: operator mode does not accept --fixture (use candidate mode)" >&2
    exit 2
fi

python3 - "$AGGREGATE" <<'PY'
import json, pathlib, sys

aggregate = sys.argv[1]
tracker = pathlib.Path(".ddx/beads.jsonl")
if not tracker.is_file():
    print(f"operator mode: tracker not found: {tracker}", file=sys.stderr)
    sys.exit(1)

items = []
for line in tracker.read_text().splitlines():
    if line.strip():
        items.append(json.loads(line))
by_id = {i["id"]: i for i in items}
agg = by_id.get(aggregate)
if not agg:
    print(f"{aggregate}: not found", file=sys.stderr)
    sys.exit(1)
if agg.get("status") != "closed":
    print(f"{aggregate}: aggregate is not closed", file=sys.stderr)
    sys.exit(1)
open_deps = []
missing_evidence = []
for dep in agg.get("dependencies", []):
    dep_id = dep["depends_on_id"]
    item = by_id.get(dep_id)
    if not item or item.get("status") != "closed":
        open_deps.append(dep_id)
    elif not item.get("closing_commit_sha"):
        missing_evidence.append(dep_id)
if open_deps:
    print("open required dependency beads: " + ", ".join(open_deps), file=sys.stderr)
    sys.exit(1)
if missing_evidence:
    print(
        "closed dependency beads without closing_commit_sha: " + ", ".join(missing_evidence),
        file=sys.stderr,
    )
    sys.exit(1)
if not agg.get("closing_commit_sha"):
    print(f"{aggregate}: aggregate lacks closing_commit_sha", file=sys.stderr)
    sys.exit(1)
print(f"{aggregate}: live closure verified")
PY
