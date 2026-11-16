#!/usr/bin/env bash
# Explicit migration error for the retired E3 producer; never starts remote work.
set -euo pipefail

echo "E3 live qualification is retired: the old producer did not measure current Turso workflows or trustworthy seal counters." >&2
echo "Historical schema-v1 evidence remains readable; these commands cannot produce new E3 qualification." >&2
echo "For current correctness, run the queue_recovery_pipeline and queue::s3_turso tests in the fireweed crate with qualified S3 fixtures." >&2
exit 2
