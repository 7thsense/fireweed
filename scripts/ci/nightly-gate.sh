#!/usr/bin/env bash
set -euo pipefail

bash scripts/ci/release-gate.sh --require-tp002-evidence E0,E1,E2,E3
bash scripts/ci/check-concurrency-verification.sh crates/pqueue-storage/concurrency_registry.toml
bash scripts/ci/lint-deferrals.sh docs/helix/04-build/BUILD-001-implementation-plan.md
echo "nightly gate passed"
