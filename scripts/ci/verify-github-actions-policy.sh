#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workflow_root="${repo_root}/.github/workflows"

forbidden='scripts/perf/|fireweed-bench|cargo[[:space:]]+bench|performance_[A-Za-z0-9_]*|queue_density|fireweed-matrix|fireweed-loadgen'
if rg -n -i --glob '*.yml' --glob '*.yaml' "${forbidden}" "${workflow_root}"; then
    echo "GitHub Actions must not execute performance tooling or benchmark suites" >&2
    exit 1
fi

if rg -n --glob '*.yml' --glob '*.yaml' '(^|[[:space:]/])release-gate\.sh([[:space:]]|$)' "${workflow_root}" |
    rg -v -- '--governed-performance-only([[:space:]]|$)'; then
    echo "GitHub release jobs must use preverified governed performance evidence" >&2
    exit 1
fi

ci="${workflow_root}/ci.yml"
grep -Fq 'timeout-minutes: 3' "${ci}"
if rg -n 'services:|matrix:|cargo install|rustup toolchain install nightly|docker run|kind-helm|(^|[[:space:]/])release-gate\.sh([[:space:]]|$)|(^|[[:space:]/])nightly-gate\.sh([[:space:]]|$)|cargo test --workspace' "${ci}"; then
    echo "default CI contains an unbounded or duplicated heavy lane" >&2
    exit 1
fi

release_gate="${repo_root}/scripts/ci/release-gate.sh"
grep -Fq -- '--governed-performance-only' "${release_gate}"
grep -Fq 'RUN_LOCAL_PERFORMANCE=false' "${release_gate}"

echo "GitHub Actions policy valid: default CI is bounded and Actions contain no performance execution"

# Focused Turso lane (public default projection): must remain path-filtered and non-manual-only.
turso="${workflow_root}/turso.yml"
if [[ ! -f "${turso}" ]]; then
    echo "missing governed focused lane: .github/workflows/turso.yml" >&2
    exit 1
fi
grep -Fq 'pull_request:' "${turso}"
grep -Fq 'paths:' "${turso}"
grep -Fq '1.92.0' "${turso}"
grep -Fq 'turso_projection_is_the_public_env_default' "${turso}"
grep -Fq 'objectlog_turso_profile_rebuilds_deleted_projection_from_authoritative_log' "${turso}"
grep -Fq 'storage_matrix_t0_t2_all_twenty_cells' "${turso}"
# Retired-symbol forbids are enforced by crates/fireweed-server/tests/turso_workflow_shape.rs
# (keeps forbidden identifiers out of this script and the YAML).
echo "GitHub Actions policy valid: turso.yml is a governed focused public-default lane"
