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
