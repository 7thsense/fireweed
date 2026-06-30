#!/usr/bin/env bash
set -euo pipefail

LCOV=""
CRATE=""
MIN_LINES=""
MIN_BRANCHES=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fixture|--lcov) LCOV="$2"; shift 2 ;;
        --crate) CRATE="$2"; shift 2 ;;
        --min-lines) MIN_LINES="$2"; shift 2 ;;
        --min-branches) MIN_BRANCHES="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$LCOV" || -z "$CRATE" || -z "$MIN_LINES" ]]; then
    echo "usage: check-lcov-coverage.py --fixture FILE --crate CRATE --min-lines N [--min-branches N]" >&2
    exit 2
fi

python3 - "$LCOV" "$CRATE" "$MIN_LINES" "$MIN_BRANCHES" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
crate = sys.argv[2]
min_lines = float(sys.argv[3])
min_branches = float(sys.argv[4]) if sys.argv[4] else None

def record_matches_crate(record):
    sf = next((line.split(":", 1)[1] for line in record if line.startswith("SF:")), "")
    if not sf:
        return False
    parts = Path(sf).parts
    for idx, part in enumerate(parts[:-1]):
        if part == "crates" and idx + 1 < len(parts) and parts[idx + 1] == crate:
            return True
    return f"crates/{crate}/" in sf or f"crates\\{crate}\\" in sf

records = []
current = []
for raw in path.read_text(encoding="utf-8").splitlines():
    current.append(raw)
    if raw == "end_of_record":
        records.append(current)
        current = []
if current:
    records.append(current)

found = hit = branches_found = branches_hit = 0
matched_records = 0
for record in records:
    if not record_matches_crate(record):
        continue
    matched_records += 1
    for raw in record:
        if raw.startswith("LF:"):
            found += int(raw.split(":", 1)[1])
        elif raw.startswith("LH:"):
            hit += int(raw.split(":", 1)[1])
        elif raw.startswith("BRF:"):
            branches_found += int(raw.split(":", 1)[1])
        elif raw.startswith("BRH:"):
            branches_hit += int(raw.split(":", 1)[1])

if matched_records == 0:
    print(f"{crate}: no LCOV records found for crate", file=sys.stderr)
    sys.exit(1)

line_pct = 100.0 if found == 0 else hit * 100.0 / found
print(f"{crate}: lines {line_pct:.2f}% ({hit}/{found})")
if line_pct < min_lines:
    print(f"{crate}: line coverage {line_pct:.2f}% < {min_lines:.2f}%", file=sys.stderr)
    sys.exit(1)

if min_branches is not None:
    if branches_found == 0:
        print(f"{crate}: missing branch coverage records", file=sys.stderr)
        sys.exit(1)
    branch_pct = branches_hit * 100.0 / branches_found
    print(f"{crate}: branches {branch_pct:.2f}% ({branches_hit}/{branches_found})")
    if branch_pct < min_branches:
        print(f"{crate}: branch coverage {branch_pct:.2f}% < {min_branches:.2f}%", file=sys.stderr)
        sys.exit(1)
PY
