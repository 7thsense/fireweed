#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tier=smoke
for ((i = 1; i <= $#; i++)); do
  if [[ "${!i}" == "--tier" ]]; then
    next=$((i + 1))
    tier=${!next}
  fi
done

if [[ "$tier" == "full" ]]; then
  if [[ -n "${CI:-}${GITHUB_ACTIONS:-}${GITLAB_CI:-}${BUILDKITE:-}${TF_BUILD:-}" ]]; then
    echo "authoritative full performance runs are forbidden in CI" >&2
    exit 64
  fi
  git fetch origin main
  test "$(git rev-parse HEAD)" = "$(git rev-parse refs/remotes/origin/main)" || {
    echo "full matrix requires HEAD to equal fetched origin/main" >&2
    exit 65
  }
  test -z "$(git diff --name-only)$(git diff --cached --name-only)" || {
    echo "full matrix requires a clean tracked worktree" >&2
    exit 65
  }
  untracked_source=$(git ls-files --others --exclude-standard -- docs crates scripts .cargo Cargo.toml Cargo.lock rust-toolchain.toml)
  test -z "$untracked_source" || {
    echo "full matrix refuses untracked source or governing documents" >&2
    exit 65
  }
  preflight_dir=target/perf-matrix-preflight
  mkdir -p "$preflight_dir"
  FIREWEED_PG_TEST_URL="$FIREWEED_PERF_POSTGRES_URL" \
    PQUEUE_PG_TEST_URL="$FIREWEED_PERF_POSTGRES_URL" \
    cargo test -p fireweed-conformance --all-features \
    >"$preflight_dir/conformance.log" 2>&1
  FIREWEED_PERF_CONFORMANCE_SHA256=$(sha256sum "$preflight_dir/conformance.log" | awk '{print $1}')
  export FIREWEED_PERF_CONFORMANCE_SHA256
  echo "same-commit conformance suite passed"
fi

cargo test --manifest-path crates/fireweed-bench/Cargo.toml --locked --lib \
  >target/perf-matrix-benchmark-tests.log 2>&1
FIREWEED_PERF_BENCH_TEST_SHA256=$(sha256sum target/perf-matrix-benchmark-tests.log | awk '{print $1}')
export FIREWEED_PERF_BENCH_TEST_SHA256
echo "locked benchmark library suite passed"
cargo build --manifest-path crates/fireweed-bench/Cargo.toml --release --locked --bin fireweed-performance-matrix

runner=crates/fireweed-bench/target/release/fireweed-performance-matrix
if command -v timeout >/dev/null 2>&1; then
  timeout --signal=TERM --kill-after=15s 4h "$runner" "$@"
else
  "$runner" "$@"
fi
