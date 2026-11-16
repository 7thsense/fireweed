#!/usr/bin/env bash
# Focused PR job test runs for .github/workflows/turso.yml. Large-scale tests live in
# `large` submodules and run in the separate large tier (scripts/ci/large-tier-gate.sh,
# run daily by nightly.yml), which does not run on pull requests.
#
# Usage: focused-queue-tests.sh turso|queue
#   turso  fireweed-turso local embedded WAL suite (no services)
#   queue  fireweed lib suite: Queue S3/Turso pipeline and recovery tests
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

case "${1:-}" in
    turso)
        # --nocapture preserves the first panic if a later test aborts the process.
        cargo +1.99.0 test -p fireweed-turso --features local -- --skip ::large:: --nocapture
        ;;
    queue)
        cargo +1.99.0 test -p fireweed --all-features --lib -- --skip ::large::
        ;;
    *)
        echo "usage: focused-queue-tests.sh turso|queue" >&2
        exit 2
        ;;
esac
