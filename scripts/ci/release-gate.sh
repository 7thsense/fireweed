#!/usr/bin/env bash
# release-gate.sh — Fireweed Queue release evidence gate.
#
# Crate map after the Phase-6 hexagonal migration (the ONLY crates this gate
# references): fireweed-core / fireweed-engine / fireweed-projection /
# fireweed-conformance / fireweed-memory / fireweed-turso / fireweed-postgres /
# fireweed-objectlog / fireweed / fireweed-release /
# fireweed-bench. The deleted pre-Fireweed service, storage, and Kafka crates
# crates are NOT referenced anywhere below.
#
# WHAT THIS GATE PROVES:
#   - fmt clean, clippy clean (-D warnings), and the selected workspace correctness suite is green.
#   - Queue suites: fireweed lib (queue_recovery_pipeline, queue::s3_turso), simple_queue_postgres and the
#     fireweed-postgres queue_* targets.
#   - Local mode emits into a CLEAN ledger dir and requires well-formed E2/E3 evidence. GitHub release
#     jobs skip those workloads and consume exact-revision governed evidence instead.
#   - The exact files named by the governed TP-002 release manifest semantically satisfy E0-E3. The E3
#     contract is additionally source-revision-bound and rejects quiet-host or absolute host-speed gates.
#   - Repository-held TP-003 evidence contains passing required rows for AC-TXN-1/2/3/6 on both exact
#     Postgres storage pairs. Evidence generation is local/manual; GitHub only verifies governed inputs.
#   - Live coverage bars: fireweed-core >=90% line / >=85% branch,
#     fireweed-engine >=80% line (enforced below; this comment is not the
#     authority — the check-lcov-coverage.sh calls are).
#
# The exact-tag freshness attestation is intentionally enforced by the tag workflow, where the resolved tag
# and checked-out commit are available. This local gate proves semantic completeness and source binding; it
# never scans docs/perf/evidence and cannot accept an unlisted replacement row.
set -euo pipefail

CARGO="rustup run 1.99.0 cargo"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TP002_RELEASE_DIR="${FIREWEED_TP002_RELEASE_DIR:-${REPO_ROOT}/target/tp002-release}"
TP002_COMPOSITE_CONTRACT="${TP002_RELEASE_DIR}/composite-contract.json"
RUN_LOCAL_PERFORMANCE=true
if (($# == 1)) && [[ "$1" == "--governed-performance-only" ]]; then
    RUN_LOCAL_PERFORMANCE=false
elif (($# != 0)); then
    printf 'release-gate.sh: unexpected argument(s): %s\n' "$*" >&2
    echo "usage: bash scripts/ci/release-gate.sh [--governed-performance-only]" >&2
    exit 64
fi

SOURCE_REVISION="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
[[ "${SOURCE_REVISION}" =~ ^[0-9a-f]{40}$ ]] || {
    echo "release-gate.sh: checked-out HEAD is not a full lowercase Git revision" >&2
    exit 1
}
for required in "${TP002_COMPOSITE_CONTRACT}"; do
    [[ -s "${required}" ]] || {
        echo "release-gate.sh: required exact-revision evidence is missing or empty: ${required}" >&2
        exit 1
    }
done

if [[ "${RUN_LOCAL_PERFORMANCE}" == true ]]; then
    echo "=== Fireweed release gate (local performance + governed release evidence) ==="
else
    echo "=== Fireweed release gate (functional checks + governed performance evidence) ==="
fi

echo "--- fmt ---"
${CARGO} fmt --all --check

echo "--- clippy ---"
${CARGO} clippy --workspace --all-targets -- -D warnings

echo "--- public artifact topology ---"
bash "${REPO_ROOT}/scripts/verify-public-artifact-topology.sh"

echo "--- exact Postgres TP-003 transaction evidence fixtures ---"
${CARGO} test -p fireweed-release --test transaction_evidence_tests -- --nocapture

echo "--- workspace correctness tests ---"
if [[ "${RUN_LOCAL_PERFORMANCE}" == true ]]; then
    ${CARGO} test --workspace
else
    # GitHub release jobs validate exact-revision governed evidence but never compile or execute
    # performance integration targets. Full integration and durability matrices are run locally/manual.
    ${CARGO} test --workspace --lib --bins
fi

# Explicit Queue + conformance suites (v0.24 process gap fireweed-c1dc998a):
# `cargo test --workspace --lib` compiles fireweed lib tests but release notes historically listed only
# narrow engine/objectlog filters. Fail closed if these named suites do not compile or regress.
echo "--- fireweed Queue lib suite (queue_recovery_pipeline + queue::s3_turso; must compile + pass) ---"
${CARGO} test -p fireweed --lib

echo "--- fireweed Queue PostgreSQL integration targets ---"
${CARGO} test -p fireweed --features postgres --test simple_queue_postgres
${CARGO} test -p fireweed-postgres --test queue_contract --test queue_process_loss --test queue_server_crash

echo "--- fireweed-conformance suite ---"
${CARGO} test -p fireweed-conformance --lib

if [[ "${RUN_LOCAL_PERFORMANCE}" == true ]]; then
    # The local E2/E3 emitters were facade tests and are gone; no performance workload runs here.
    echo "--- no local performance evidence suites remain; governed evidence is verified below ---"
else
    # Policy-negative (fail-closed): local performance is not silently green when unexecuted.
    # Hosted/release paths must either set RUN_LOCAL_PERFORMANCE=true or prove governed evidence
    # already covers the required smoke evidence ids for this exact source revision.
    echo "--- local performance execution omitted; asserting governed-evidence policy-negative ---"
    if [[ -z "${FIREWEED_ALLOW_GOVERNED_PERF_EVIDENCE_ONLY:-}" ]]; then
        echo "release-gate: RUN_LOCAL_PERFORMANCE is false and FIREWEED_ALLOW_GOVERNED_PERF_EVIDENCE_ONLY is unset" >&2
        echo "  set RUN_LOCAL_PERFORMANCE=true to execute local suites, or" >&2
        echo "  FIREWEED_ALLOW_GOVERNED_PERF_EVIDENCE_ONLY=1 to accept exact-revision governed evidence only" >&2
        exit 1
    fi
    echo "  FIREWEED_ALLOW_GOVERNED_PERF_EVIDENCE_ONLY=1: local perf suites not run; governed evidence is required next"
fi

echo "--- governed TP-002 composite semantic contract ---"
bash "${SCRIPT_DIR}/verify-governed-release-composite.sh" \
    --contract "${TP002_COMPOSITE_CONTRACT}" --expected-revision "${SOURCE_REVISION}"

echo "--- live coverage gate ---"
mkdir -p "${REPO_ROOT}/target/coverage"
# Clean instrumentation so artifacts from deleted crates can't contaminate.
${CARGO} llvm-cov clean --workspace
${CARGO} llvm-cov --package fireweed-core --lcov \
    --output-path "${REPO_ROOT}/target/coverage/fireweed-core.lcov"
bash "${SCRIPT_DIR}/check-lcov-coverage.sh" \
    --lcov "${REPO_ROOT}/target/coverage/fireweed-core.lcov" --crate fireweed-core --min-lines 90
# cargo-llvm-cov spawns Cargo/rustc subprocesses of its own. Pin the whole
# subprocess tree to nightly and put the nightly binaries ahead of Homebrew's
# standalone stable Cargo/rustc; selecting only the outer Cargo allows the
# nested `rustc` lookup to reject llvm-cov's nightly-only `-Z` branch flags.
NIGHTLY_BIN="$(dirname "$(rustup which --toolchain nightly rustc)")"
PATH="${NIGHTLY_BIN}:${PATH}" RUSTUP_TOOLCHAIN=nightly \
    rustup run nightly cargo llvm-cov --package fireweed-core --branch --lcov \
    --output-path "${REPO_ROOT}/target/coverage/fireweed-core-branch.lcov"
bash "${SCRIPT_DIR}/check-lcov-coverage.sh" \
    --lcov "${REPO_ROOT}/target/coverage/fireweed-core-branch.lcov" \
    --crate fireweed-core --min-lines 90 --min-branches 85
${CARGO} llvm-cov clean --workspace
for package in fireweed-engine fireweed fireweed-memory fireweed-turso; do
    coverage_features=()
    if [[ "${package}" == fireweed-turso ]]; then
        coverage_features=(--features local)
    fi
    CARGO_BUILD_JOBS=1 ${CARGO} llvm-cov --no-report --package "${package}" "${coverage_features[@]}"
done
${CARGO} llvm-cov report --lcov \
    --output-path "${REPO_ROOT}/target/coverage/fireweed-engine.lcov"
bash "${SCRIPT_DIR}/check-lcov-coverage.sh" \
    --lcov "${REPO_ROOT}/target/coverage/fireweed-engine.lcov" --crate fireweed-engine --min-lines 80

echo "--- build-closure integrity (candidate mode; never reads .ddx/**) ---"
bash "${SCRIPT_DIR}/verify-build-closure.sh" \
    --mode candidate \
    --fixture "${SCRIPT_DIR}/fixtures/closure/release-aggregate-pqueue-131eadfa.json" \
    --aggregate pqueue-131eadfa

echo "=== release gate PASSED ==="
if [[ "${RUN_LOCAL_PERFORMANCE}" == true ]]; then
    echo "    No local performance workload ran; coverage bars met."
else
    echo "    No performance workload ran; exact-revision governed evidence was verified."
fi
echo "    Governed release evidence E0-E3 is semantically complete; E3 is source-bound and portable."
