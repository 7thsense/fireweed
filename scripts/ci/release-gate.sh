#!/usr/bin/env bash
# release-gate.sh — pqueue release evidence gate (HONEST smoke lane).
#
# Crate map after the Phase-6 hexagonal migration (the ONLY crates this gate
# references): pqueue-core / pqueue-engine / pqueue-projection /
# pqueue-conformance / pqueue-memory / pqueue-sqlite / pqueue-postgres /
# pqueue-objectlog / pqueue-resp / pqueue / pqueue-server / pqueue-release /
# pqueue-bench. The deleted pqueue-service / pqueue-storage / pqueue-kafka
# crates are NOT referenced anywhere below.
#
# WHAT THIS GATE PROVES (green-local, SMOKE tier):
#   - fmt clean, clippy clean (-D warnings), `cargo test --workspace` green.
#   - The TP-002 evidence-emitting suites run into a CLEAN ledger dir, every
#     emitted row is well-formed + strict-valid, and the SMOKE-tier headline
#     ids E2 (cross-queue scale-out + queue density) and E3 (object-log
#     cost/ack + recovery) are present.
#   - Live coverage bars: pqueue-core >=90% line / >=85% branch,
#     pqueue-engine >=80% line (enforced below; this comment is not the
#     authority — the check-lcov-coverage.py calls are).
#
# WHAT THIS GATE DOES *NOT* PROVE (RELEASE tier — never faked):
#   This script creates a clean temporary ledger and validates only the
#   newly-generated SMOKE-tier E2/E3 rows. Repository-held RELEASE-tier E0-E3
#   evidence under docs/perf/evidence is not ingested or asserted here, so a
#   green smoke lane is not a release-tier evidence verdict. Integrating the
#   governed release evidence into the tag gate is tracked by pqueue-bf46289d.
set -euo pipefail

CARGO="rustup run 1.92.0 cargo"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if (($# > 0)); then
    printf 'release-gate.sh: unexpected argument(s): %s\n' "$*" >&2
    echo "usage: bash scripts/ci/release-gate.sh" >&2
    exit 64
fi

echo "=== pqueue release gate (SMOKE lane) ==="
echo "    This gate validates newly generated SMOKE-tier E2,E3 evidence only."
echo "    It does not assert repository-held RELEASE-tier E0-E3 evidence (pqueue-bf46289d)."

echo "--- fmt ---"
${CARGO} fmt --all --check

echo "--- clippy ---"
${CARGO} clippy --workspace --all-targets -- -D warnings

# A CLEAN ledger dir so stale pre-migration rows in target/pqueue-ledger can
# never satisfy the gate. Every suite is pointed at this dir via the env var
# that pqueue_release::ledger_path() honors.
PQUEUE_LEDGER_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pqueue-ledger.XXXXXX")"
export PQUEUE_LEDGER_DIR
trap 'rm -rf "${PQUEUE_LEDGER_DIR}"' EXIT
echo "--- clean ledger dir: ${PQUEUE_LEDGER_DIR} ---"

echo "--- workspace tests (emits product_validation + E3 smoke rows) ---"
${CARGO} test --workspace

# pqueue-bench is a SELF-CONTAINED workspace (not a root member), so the E2
# evidence suites must be driven via --manifest-path; they emit into the same
# PQUEUE_LEDGER_DIR exported above.
echo "--- bench evidence suites (separate workspace; emits E2 smoke rows) ---"
${CARGO} test --manifest-path "${REPO_ROOT}/crates/pqueue-bench/Cargo.toml" \
    --test performance_cross_queue_scale_out_tests \
    --test queue_density_single_node_tests

echo "--- tier-aware ledger verification (smoke lane: E2,E3 present + well-formed) ---"
${CARGO} run -p pqueue-release --bin pqueue-verify-ledger -- \
    --ledger-dir "${PQUEUE_LEDGER_DIR}" \
    --strict \
    --require-smoke-evidence E2,E3

echo "--- live coverage gate ---"
mkdir -p "${REPO_ROOT}/target/coverage"
# Clean instrumentation so artifacts from deleted crates can't contaminate.
${CARGO} llvm-cov clean --workspace
${CARGO} llvm-cov --package pqueue-core --lcov \
    --output-path "${REPO_ROOT}/target/coverage/pqueue-core.lcov"
bash "${SCRIPT_DIR}/check-lcov-coverage.py" \
    --lcov "${REPO_ROOT}/target/coverage/pqueue-core.lcov" --crate pqueue-core --min-lines 90
# cargo-llvm-cov spawns Cargo/rustc subprocesses of its own. Pin the whole
# subprocess tree to nightly and put the nightly binaries ahead of Homebrew's
# standalone stable Cargo/rustc; selecting only the outer Cargo allows the
# nested `rustc` lookup to reject llvm-cov's nightly-only `-Z` branch flags.
NIGHTLY_BIN="$(dirname "$(rustup which --toolchain nightly rustc)")"
PATH="${NIGHTLY_BIN}:${PATH}" RUSTUP_TOOLCHAIN=nightly \
    rustup run nightly cargo llvm-cov --package pqueue-core --branch --lcov \
    --output-path "${REPO_ROOT}/target/coverage/pqueue-core-branch.lcov"
bash "${SCRIPT_DIR}/check-lcov-coverage.py" \
    --lcov "${REPO_ROOT}/target/coverage/pqueue-core-branch.lcov" \
    --crate pqueue-core --min-lines 90 --min-branches 85
${CARGO} llvm-cov clean --workspace
for package in pqueue-engine pqueue pqueue-memory pqueue-sqlite; do
    CARGO_BUILD_JOBS=1 ${CARGO} llvm-cov --no-report --package "${package}"
done
${CARGO} llvm-cov report --lcov \
    --output-path "${REPO_ROOT}/target/coverage/pqueue-engine.lcov"
bash "${SCRIPT_DIR}/check-lcov-coverage.py" \
    --lcov "${REPO_ROOT}/target/coverage/pqueue-engine.lcov" --crate pqueue-engine --min-lines 80

echo "--- build-closure integrity ---"
bash "${SCRIPT_DIR}/verify-build-closure.sh" --aggregate pqueue-131eadfa

echo "=== release gate (SMOKE lane) PASSED ==="
echo "    Smoke evidence E2,E3 present + well-formed; coverage bars met."
echo "    Repository-held RELEASE-tier E0-E3 evidence was not asserted by this lane."
