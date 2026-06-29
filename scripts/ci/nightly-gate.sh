#!/usr/bin/env bash
# nightly-gate.sh — wraps the release gate (smoke lane) plus deferral linting.
#
# The pre-migration nightly also ran a concurrency-registry check over
# crates/pqueue-storage/concurrency_registry.toml; that crate AND its registry
# were deleted in the Phase-6 hexagonal migration, so the check is dropped (the
# per-queue ownership model under ADR-008 has no intra-queue shard concurrency
# registry to verify).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "${SCRIPT_DIR}/release-gate.sh"
bash "${SCRIPT_DIR}/lint-deferrals.sh" docs/helix/04-build/BUILD-001-implementation-plan.md
echo "nightly gate passed"
