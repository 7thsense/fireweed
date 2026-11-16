#!/usr/bin/env bash
# SPIKE-006 measurement driver. One process per run (fresh db file for every
# ingest), stdout -> results/<name>.jsonl, stderr (progress + [load] lines)
# -> results/<name>.log. Mirrors spikes/claim-scale/run.sh's shape.
#
# Usage: ./run.sh primary        (ingest+bench, all 4 layouts, primary scale)
#        ./run.sh large-c        (large multi-queue file, layout C)
#        ./run.sh large-d        (large multi-queue file, layout D)
#        ./run.sh cache-ablation (re-bench the large-C file at a bigger cache_size)
#        ./run.sh per-queue-tables
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${SPIKE_BIN:-/home/erik/.cache/fleet-tmp/claude-1000/-home-erik-Projects-fireweed--claude-worktrees-external-interface-contract-5748d2/876e6944-8069-4a77-9c2c-34921ace2936/scratchpad/design/spike-target-declared-fields/release/spike}"
DB_DIR="${SPIKE_DB_DIR:-/home/erik/.cache/fleet-tmp/claude-1000/-home-erik-Projects-fireweed--claude-worktrees-external-interface-contract-5748d2/876e6944-8069-4a77-9c2c-34921ace2936/scratchpad/design/dbs-declared-fields}"
OUT="$HERE/results"
mkdir -p "$DB_DIR" "$OUT"

PRIMARY_QUEUES=3
PRIMARY_ROWS=200000
CLAIM_REPS=20
OP_REPS=300

run_ingest() {
    local name="$1" layout="$2" n_queues="$3" rows="$4"
    echo "== ingest $name: layout=$layout n_queues=$n_queues rows_per_queue=$rows ==" >&2
    "$BIN" ingest "$layout" "$DB_DIR/$name.db" "$n_queues" "$rows" \
        > "$OUT/$name-ingest.jsonl" 2> "$OUT/$name-ingest.log"
    echo "== ingest $name done ==" >&2
}

run_bench() {
    local name="$1" layout="$2" touch_q="$3" rows="$4" trials="$5" claim_reps="$6" op_reps="$7"
    echo "== bench $name: layout=$layout touch_queue=$touch_q ==" >&2
    "$BIN" bench "$layout" "$DB_DIR/$name.db" "$touch_q" "$rows" "$trials" "$claim_reps" "$op_reps" \
        > "$OUT/$name-bench.jsonl" 2> "$OUT/$name-bench.log"
    echo "== bench $name done ==" >&2
}

target="${1:-primary}"

case "$target" in
    primary)
        for L in a b c d; do
            run_ingest "primary-$L" "$L" "$PRIMARY_QUEUES" "$PRIMARY_ROWS"
            run_bench "primary-$L" "$L" 1 "$PRIMARY_ROWS" 3 "$CLAIM_REPS" "$OP_REPS"
        done
        ;;
    ingest-a) run_ingest "primary-a" a "$PRIMARY_QUEUES" "$PRIMARY_ROWS" ;;
    ingest-b) run_ingest "primary-b" b "$PRIMARY_QUEUES" "$PRIMARY_ROWS" ;;
    ingest-c) run_ingest "primary-c" c "$PRIMARY_QUEUES" "$PRIMARY_ROWS" ;;
    ingest-d) run_ingest "primary-d" d "$PRIMARY_QUEUES" "$PRIMARY_ROWS" ;;
    bench-a) run_bench "primary-a" a 1 "$PRIMARY_ROWS" 3 "$CLAIM_REPS" "$OP_REPS" ;;
    bench-b) run_bench "primary-b" b 1 "$PRIMARY_ROWS" 3 "$CLAIM_REPS" "$OP_REPS" ;;
    bench-c) run_bench "primary-c" c 1 "$PRIMARY_ROWS" 3 "$CLAIM_REPS" "$OP_REPS" ;;
    bench-d) run_bench "primary-d" d 1 "$PRIMARY_ROWS" 3 "$CLAIM_REPS" "$OP_REPS" ;;
    large-c)
        run_ingest "large-c" c 15 200000
        run_bench "large-c" c 1 200000 3 "$CLAIM_REPS" "$OP_REPS"
        run_bench "large-c" c 15 200000 3 "$CLAIM_REPS" "$OP_REPS"
        ;;
    large-d)
        run_ingest "large-d" d 25 200000
        run_bench "large-d" d 1 200000 3 "$CLAIM_REPS" "$OP_REPS"
        run_bench "large-d" d 25 200000 3 "$CLAIM_REPS" "$OP_REPS"
        ;;
    cache-ablation)
        echo "== cache ablation: large-c @ 512 MiB cache_size ==" >&2
        FW_SPIKE_CACHE_KIB=-524288 "$BIN" bench c "$DB_DIR/large-c.db" 1 200000 3 "$CLAIM_REPS" "$OP_REPS" \
            > "$OUT/cache-ablation-512mib.jsonl" 2> "$OUT/cache-ablation-512mib.log"
        echo "== cache ablation done ==" >&2
        ;;
    per-queue-tables)
        for n in 100 1000 10000; do
            echo "== per_queue_tables n=$n ==" >&2
            "$BIN" per_queue_tables "$DB_DIR/pqt-$n.db" "$n" 20 \
                > "$OUT/pqt-$n.jsonl" 2> "$OUT/pqt-$n.log"
        done
        ;;
    *)
        echo "unknown target $target" >&2
        exit 2
        ;;
esac
echo "done" >&2
