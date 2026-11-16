#!/usr/bin/env bash
# SPIKE-002 measurement driver. Runs every (layout, shape) combo plus the two
# no-index baselines, one process per combo (fresh db file each time), and
# appends each JSON result line to results/results.jsonl.
#
# Usage: ROWS=1000000 ./run.sh   (defaults: ROWS=100000)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${SPIKE_BIN:-/home/erik/.cache/fleet-tmp/claude-1000/-home-erik-Projects-fireweed--claude-worktrees-external-interface-contract-5748d2/876e6944-8069-4a77-9c2c-34921ace2936/scratchpad/design/spike-target/release/spike}"
DB_DIR="${SPIKE_DB_DIR:-/home/erik/.cache/fleet-tmp/claude-1000/-home-erik-Projects-fireweed--claude-worktrees-external-interface-contract-5748d2/876e6944-8069-4a77-9c2c-34921ace2936/scratchpad/design/dbs}"
OUT="$HERE/results/results.jsonl"
ROWS="${ROWS:-100000}"

mkdir -p "$DB_DIR" "$HERE/results"
: > "$OUT"

run() {
    local layout="$1" shape="$2"
    echo "== layout=$layout shape=$shape rows=$ROWS ==" >&2
    "$BIN" "$layout" "$shape" "$DB_DIR/combo.db" "$ROWS" >> "$OUT"
}

for shape in int uuid email str128 str256; do
    run a "$shape"
done
for shape in int uuid email str128 str256; do
    run b "$shape"
done
for shape in int uuid email str128 str256; do
    run c "$shape"
done
for shape in int uuid email str128 str256; do
    run d "$shape"
done
run e int

echo "== baseline text/256 rows=$ROWS ==" >&2
"$BIN" baseline text 256 "$DB_DIR/combo.db" "$ROWS" >> "$OUT"
echo "== baseline int rows=$ROWS ==" >&2
"$BIN" baseline int 0 "$DB_DIR/combo.db" "$ROWS" >> "$OUT"

echo "done -> $OUT" >&2
