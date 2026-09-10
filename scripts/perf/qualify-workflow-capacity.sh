#!/usr/bin/env bash
# Repeat the qualified public-API workloads without overlapping capacity runs.
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
cargo build --release -p fireweed-workload

qualification_failed=0
for attempt in 1 2; do
  printf 'Workflow qualification %s/2\n' "$attempt" >&2
  if ! python3 scripts/perf/workflow-capacity.py \
    --profile mutable --items 500000 --batch 1000 --purge-batch 8000 \
    --shards 16 --workers 8 --load-workers 4 --recycle --cycles 6 \
    --deadline-seconds 1200 --qualify \
    > "$qualification_output/workflow-$attempt.json"; then
    qualification_failed=1
  fi
  printf 'Primitive qualification %s/2\n' "$attempt" >&2
  if ! python3 scripts/perf/workflow-capacity.py \
    --profile primitives --items 1000000 --batch 1000 --shards 16 \
    --workers 8 --deadline-seconds 900 --qualify \
    > "$qualification_output/primitives-$attempt.json"; then
    qualification_failed=1
  fi
done

printf 'Evidence: %s\n' "$qualification_output" >&2
exit "$qualification_failed"
