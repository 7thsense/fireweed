#!/usr/bin/env bash
set -euo pipefail

bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3 \
    --tp002-e0e1-source pqueue-7e2b3132 \
    --tp002-e2-source pqueue-9afd88cc,pqueue-76d92a33 \
    --tp002-e3-source pqueue-b1abd895,pqueue-472a09d4
bash scripts/ci/check-concurrency-verification.sh crates/pqueue-storage/concurrency_registry.toml
bash scripts/ci/lint-deferrals.sh docs/helix/04-build/BUILD-001-implementation-plan.md
echo "nightly gate passed"
