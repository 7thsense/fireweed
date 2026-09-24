#!/usr/bin/env bash
# SPIKE-004 measurement driver. Runs the full (layout x scale) matrix plus the
# claim-order-index size baselines, one process per run (fresh db file each
# time), stdout -> results/<name>.jsonl, stderr (progress log) -> results/<name>.log.
#
# Usage: ./run.sh            (full matrix: l1/l2 x 1m/10m + baselines)
#        ./run.sh l1-1m      (just one named run)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${SPIKE_BIN:-/home/erik/.cache/fleet-tmp/claude-1000/-home-erik-Projects-fireweed--claude-worktrees-external-interface-contract-5748d2/876e6944-8069-4a77-9c2c-34921ace2936/scratchpad/design/spike-target/release/spike}"
DB_DIR="${SPIKE_DB_DIR:-/home/erik/.cache/fleet-tmp/claude-1000/-home-erik-Projects-fireweed--claude-worktrees-external-interface-contract-5748d2/876e6944-8069-4a77-9c2c-34921ace2936/scratchpad/design/dbs}"
OUT="$HERE/results"
mkdir -p "$DB_DIR" "$OUT"

run_one() {
    local name="$1" layout="$2" rows_completed="$3" rows_initial="$4"
    echo "== run $name: layout=$layout completed=$rows_completed initial=$rows_initial ==" >&2
    "$BIN" run "$layout" "$DB_DIR/$name.db" "$rows_completed" "$rows_initial" \
        > "$OUT/$name.jsonl" 2> "$OUT/$name.log"
    echo "== $name done ==" >&2
}

run_baseline() {
    local name="$1" layout="$2" rows_completed="$3" rows_initial="$4"
    echo "== size_baseline $name: layout=$layout completed=$rows_completed initial=$rows_initial ==" >&2
    "$BIN" size_baseline "$layout" "$DB_DIR/$name.db" "$rows_completed" "$rows_initial" \
        > "$OUT/$name.jsonl" 2> "$OUT/$name.log"
    echo "== $name done ==" >&2
}

# Finding 8 (owner target shape): N queues sharing one Turso file. Build is
# separate from the timed matrix — "fastest bulk load", not an ingest
# benchmark (task instruction) — large per-queue transactions via
# generate_series-driven INSERT..SELECT, indexes built after the load.
run_bulk_build() {
    local name="$1" layout="$2" n_queues="$3" rows_completed="$4" rows_initial="$5"
    echo "== bulk_build $name: layout=$layout n_queues=$n_queues completed=$rows_completed initial=$rows_initial ==" >&2
    "$BIN" bulk_build "$layout" "$DB_DIR/$name.db" "$n_queues" "$rows_completed" "$rows_initial" \
        > "$OUT/$name-build.jsonl" 2> "$OUT/$name-build.log"
    echo "== $name build done ==" >&2
}

# Single-writer claim/finish/extend/sweep round-robin over n_selected queues
# of an already-built multi-queue db (same trial shape as run(), Finding 1-6,
# so the numbers are directly comparable).
run_finding8() {
    local name="$1" layout="$2" db_name="$3" n_queues_total="$4" n_selected="$5"
    echo "== finding8 $name: layout=$layout db=$db_name n_queues_total=$n_queues_total n_selected=$n_selected ==" >&2
    "$BIN" finding8 "$layout" "$DB_DIR/$db_name.db" "$n_queues_total" "$n_selected" \
        > "$OUT/$name.jsonl" 2> "$OUT/$name.log"
    echo "== $name done ==" >&2
}

# N-concurrent-claimers throughput/latency, shared (production's own
# writer: Mutex<Connection> pattern) vs separate (N raw connections relying
# on Turso's own file-lock + busy_timeout) — see doc Finding 8.
run_multi_claim() {
    local name="$1" layout="$2" db_name="$3" n_queues_total="$4" n_selected="$5" n_workers="$6" ops_per_worker="$7" mode="$8"
    echo "== multi_claim $name: layout=$layout n_workers=$n_workers mode=$mode ==" >&2
    "$BIN" multi_claim "$layout" "$DB_DIR/$db_name.db" "$n_queues_total" "$n_selected" "$n_workers" "$ops_per_worker" "$mode" \
        >> "$OUT/$name.jsonl" 2>> "$OUT/$name.log"
    echo "== $name ($mode, n_workers=$n_workers) done ==" >&2
}

target="${1:-all}"

# At 10M rows the stage-blind query (no stage predicate) forces Turso to sort
# the whole matching set (~10M rows) rather than seek — measured to run for
# minutes per rep (see the results doc's stage-blind finding). Cap it to 1
# rep at that scale; 1M and below keep the default 3.
run_10m() {
    FW_SPIKE_BLIND_REPS="${FW_SPIKE_BLIND_REPS:-1}" run_one "$@"
}

case "$target" in
    l1-1m)  run_one l1-1m l1 950000 50000 ;;
    l2-1m)  run_one l2-1m l2 950000 50000 ;;
    l1-10m) run_10m l1-10m l1 9500000 500000 ;;
    l2-10m) run_10m l2-10m l2 9500000 500000 ;;
    baseline-l1-1m) run_baseline baseline-l1-1m l1 950000 50000 ;;
    baseline-l2-1m) run_baseline baseline-l2-1m l2 950000 50000 ;;
    multi100-l2-build) run_bulk_build multi100-l2 l2 100 950000 50000 ;;
    multi100-l1-build) run_bulk_build multi100-l1 l1 100 950000 50000 ;;
    finding8-l2) run_finding8 finding8-l2 l2 multi100-l2 100 20 ;;
    finding8-l1) run_finding8 finding8-l1 l1 multi100-l1 100 20 ;;
    all)
        run_one l1-1m l1 950000 50000
        run_one l2-1m l2 950000 50000
        run_10m l1-10m l1 9500000 500000
        run_10m l2-10m l2 9500000 500000
        run_baseline baseline-l1-1m l1 950000 50000
        run_baseline baseline-l2-1m l2 950000 50000
        ;;
    *)
        echo "unknown target $target" >&2
        exit 2
        ;;
esac

echo "done" >&2
