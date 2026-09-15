#!/usr/bin/env bash
# Repeat representative campaign and primitive qualification without overlap.
# OBJECT_LOG_FLUSH_RUNTIME_THREADS, when supplied, is inherited by all four runs
# and recorded by workflow-capacity.py. Do not change it between attempts.
set -euo pipefail

if [[ $# != 1 ]]; then
  echo "Usage: $0 NEW_OUTPUT_DIRECTORY" >&2
  exit 2
fi

# Refuse to overwrite earlier evidence. Resolve before changing directories.
mkdir -- "$1"
qualification_output=$(cd -- "$1" && pwd)
qualification_repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd -- "$qualification_repo"
cargo build --locked --release -p fireweed-workload

qualification_failed=0
for attempt in 1 2; do
  printf 'Campaign qualification %s/2 (12.5k recipients/sec)\n' "$attempt" >&2
  if ! python3 scripts/perf/workflow-capacity.py \
    --profile campaign --campaign-metadata --campaign-timestamp-priority \
    --items 1000000 --batch 1000 --purge-batch 8000 \
    --shards 64 --workers 2 --load-workers 2 --recycle --cycles 8 \
    --deadline-seconds 1800 --stretch \
    > "$qualification_output/campaign-$attempt.json"; then
    qualification_failed=1
  fi
  printf 'Primitive qualification %s/2\n' "$attempt" >&2
  if ! python3 scripts/perf/workflow-capacity.py \
    --profile primitives --primitive-varied-payload \
    --items 1000000 --batch 1000 --shards 32 \
    --deadline-seconds 900 --qualify \
    > "$qualification_output/primitives-$attempt.json"; then
    qualification_failed=1
  fi
done

printf 'Evidence: %s\n' "$qualification_output" >&2
exit "$qualification_failed"
