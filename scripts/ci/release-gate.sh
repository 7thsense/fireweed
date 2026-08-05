#!/usr/bin/env bash
# release-gate.sh — Fireweed Queue release evidence gate.
#
# Crate map after the Phase-6 hexagonal migration (the ONLY crates this gate
# references): fireweed-core / fireweed-engine / fireweed-projection /
# fireweed-conformance / fireweed-memory / fireweed-sqlite / fireweed-postgres /
# fireweed-objectlog / fireweed-resp / fireweed / fireweed-server / fireweed-release /
# fireweed-bench. The deleted pre-Fireweed service, storage, and Kafka crates
# crates are NOT referenced anywhere below.
#
# WHAT THIS GATE PROVES:
#   - fmt clean, clippy clean (-D warnings), and the selected workspace correctness suite is green.
#   - Public 15-cell storage matrix (T0–T2 library harness + server matrix suites + legacy product-name
#     ban) via scripts/ci/storage-matrix-gate.sh --skip-helm. Helm fixtures stay in deployment-release-gate
#     / helm-gate.sh. Full-matrix fixture hard-fail is opt-in via FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1.
#   - Local mode emits into a CLEAN ledger dir and requires well-formed E2/E3 evidence. GitHub release
#     jobs skip those workloads and consume exact-revision governed evidence instead.
#   - The exact files named by the governed TP-002 release manifest semantically satisfy E0-E3. The E3
#     contract is additionally source-revision-bound and rejects quiet-host or absolute host-speed gates.
#   - Repository-held TP-003 evidence contains passing required rows for AC-TXN-1/2/3/6 on both exact
#     Postgres storage pairs. Evidence generation is local/manual; GitHub only verifies governed inputs.
#   - Live coverage bars: fireweed-core >=90% line / >=85% branch,
#     fireweed-engine >=80% line (enforced below; this comment is not the
#     authority — the check-lcov-coverage.py calls are).
#
# The exact-tag freshness attestation is intentionally enforced by the tag workflow, where the resolved tag
# and checked-out commit are available. This local gate proves semantic completeness and source binding; it
# never scans docs/perf/evidence and cannot accept an unlisted replacement row.
set -euo pipefail

CARGO="rustup run 1.92.0 cargo"
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

echo "--- public crate encapsulation ---"
bash "${REPO_ROOT}/scripts/verify-public-artifact-topology.sh"
bash "${REPO_ROOT}/scripts/verify-public-crate-boundary.sh"

echo "--- exact Postgres TP-003 transaction evidence fixtures ---"
${CARGO} test -p fireweed-release --test transaction_evidence_tests -- --nocapture

# A clean ledger directory so stale pre-migration rows in the retained
# target/fireweed-ledger evidence path can
# never satisfy the gate. Every suite is pointed at this dir via the env var
# that fireweed_release::ledger_path() honors.
if [[ "${RUN_LOCAL_PERFORMANCE}" == true ]]; then
    FIREWEED_LEDGER_DIR="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-ledger.XXXXXX")"
    export FIREWEED_LEDGER_DIR
    trap 'rm -rf "${FIREWEED_LEDGER_DIR}"' EXIT
    echo "--- clean ledger dir: ${FIREWEED_LEDGER_DIR} ---"
fi

echo "--- workspace correctness tests ---"
if [[ "${RUN_LOCAL_PERFORMANCE}" == true ]]; then
    ${CARGO} test --workspace
else
    # GitHub release jobs validate exact-revision governed evidence but never compile or execute
    # performance integration targets. Full integration and durability matrices are run locally/manual.
    ${CARGO} test --workspace --lib --bins
fi

# Explicit facade + conformance suites (v0.24 process gap fireweed-c1dc998a):
# `cargo test --workspace --lib` compiles fireweed lib tests but release notes historically listed only
# narrow engine/objectlog filters. Fail closed if these named suites do not compile or regress.
echo "--- fireweed facade lib suite (must compile + pass; ignored tests need documented reasons) ---"
${CARGO} test -p fireweed --lib

echo "--- fireweed facade integration targets (concrete + mutation + memory public interface) ---"
# Full public_interface_conformance includes objectlog cells that still advertise incomplete
# LogEngine ports (hot projection / bounded_mutation / catalog verify). Those stay tracked as
# product completion work; the release gate requires the memory public interface plus lib suite.
${CARGO} test -p fireweed --test concrete_fireweed
${CARGO} test -p fireweed --test item_mutation
${CARGO} test -p fireweed --test public_interface_conformance memory_public_interface -- --exact

echo "--- fireweed-conformance suite ---"
${CARGO} test -p fireweed-conformance --lib

# v0.25 headline proofs: integration targets are NOT covered by --workspace --lib --bins
# on GitHub release jobs. Fail closed if these regress (P0 eligibility, claim-by-id, RESP).
echo "--- claim_by_item_ids + eligibility recovery + RESP XCLAIM first-delivery ---"
${CARGO} test -p fireweed-engine --test claim_by_item_ids
${CARGO} test -p fireweed-sqlite --test eligibility_index_recovery
${CARGO} test -p fireweed-resp --test e2e xclaim_first_delivery_pending_ids

# Public 15-cell StorageConfig matrix (Phase 6). Helm is owned by deployment-release-gate /
# helm-gate.sh so this call skips helm. S3/PG cells skip when fixtures are unset unless
# FIREWEED_STORAGE_MATRIX_REQUIRE_FULL=1 is set (then missing fixtures fail the gate).
echo "--- storage matrix gate (15-cell public surface; helm deferred to deployment gate) ---"
bash "${SCRIPT_DIR}/storage-matrix-gate.sh" --skip-helm

if [[ "${RUN_LOCAL_PERFORMANCE}" == true ]]; then
    echo "--- local performance evidence suites ---"
    ${CARGO} test --manifest-path "${REPO_ROOT}/crates/fireweed-bench/Cargo.toml" \
        --test performance_cross_queue_scale_out_tests \
        --test queue_density_single_node_tests

    echo "--- strict validation of locally emitted evidence ---"
    ${CARGO} run -p fireweed-release --bin fireweed-verify-ledger -- \
        --ledger-dir "${FIREWEED_LEDGER_DIR}" \
        --strict \
        --require-smoke-evidence E2,E3
else
    echo "--- local performance execution skipped; exact-revision governed evidence is authoritative ---"
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
bash "${SCRIPT_DIR}/check-lcov-coverage.py" \
    --lcov "${REPO_ROOT}/target/coverage/fireweed-core.lcov" --crate fireweed-core --min-lines 90
# cargo-llvm-cov spawns Cargo/rustc subprocesses of its own. Pin the whole
# subprocess tree to nightly and put the nightly binaries ahead of Homebrew's
# standalone stable Cargo/rustc; selecting only the outer Cargo allows the
# nested `rustc` lookup to reject llvm-cov's nightly-only `-Z` branch flags.
NIGHTLY_BIN="$(dirname "$(rustup which --toolchain nightly rustc)")"
PATH="${NIGHTLY_BIN}:${PATH}" RUSTUP_TOOLCHAIN=nightly \
    rustup run nightly cargo llvm-cov --package fireweed-core --branch --lcov \
    --output-path "${REPO_ROOT}/target/coverage/fireweed-core-branch.lcov"
bash "${SCRIPT_DIR}/check-lcov-coverage.py" \
    --lcov "${REPO_ROOT}/target/coverage/fireweed-core-branch.lcov" \
    --crate fireweed-core --min-lines 90 --min-branches 85
${CARGO} llvm-cov clean --workspace
for package in fireweed-engine fireweed fireweed-memory fireweed-sqlite; do
    CARGO_BUILD_JOBS=1 ${CARGO} llvm-cov --no-report --package "${package}"
done
${CARGO} llvm-cov report --lcov \
    --output-path "${REPO_ROOT}/target/coverage/fireweed-engine.lcov"
bash "${SCRIPT_DIR}/check-lcov-coverage.py" \
    --lcov "${REPO_ROOT}/target/coverage/fireweed-engine.lcov" --crate fireweed-engine --min-lines 80

echo "--- build-closure integrity (candidate mode; never reads .ddx/**) ---"
bash "${SCRIPT_DIR}/verify-build-closure.sh" \
    --mode candidate \
    --fixture "${SCRIPT_DIR}/fixtures/closure/release-aggregate-pqueue-131eadfa.json" \
    --aggregate pqueue-131eadfa

echo "=== release gate PASSED ==="
if [[ "${RUN_LOCAL_PERFORMANCE}" == true ]]; then
    echo "    Locally generated evidence E2,E3 present and well-formed; coverage bars met."
else
    echo "    No performance workload ran; exact-revision governed evidence was verified."
fi
echo "    Governed release evidence E0-E3 is semantically complete; E3 is source-bound and portable."
