#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="${repo_root}/scripts/ci/fixtures/public-crate-boundary/Cargo.toml"
target_dir="${repo_root}/target/public-crate-boundary"

cargo check --quiet --offline --manifest-path "${fixture}" --target-dir "${target_dir}" --bin supported

expect_rejected() {
    local bin="$1"
    local diagnostic="$2"
    local output
    output="$(mktemp "${TMPDIR:-/tmp}/fireweed-public-boundary.XXXXXX")"
    if cargo check --offline --manifest-path "${fixture}" --target-dir "${target_dir}" --bin "${bin}" \
        >"${output}" 2>&1; then
        echo "public crate boundary unexpectedly compiled forbidden fixture: ${bin}" >&2
        rm -f "${output}"
        return 1
    fi
    if ! grep -Fq -- "${diagnostic}" "${output}"; then
        echo "forbidden fixture ${bin} failed for an unexpected reason:" >&2
        sed -n '1,160p' "${output}" >&2
        rm -f "${output}"
        return 1
    fi
    rm -f "${output}"
}

expect_rejected raw-port 'trait `PushPort` is private'
expect_rejected internal-crate 'use of unresolved module or unlinked crate `fireweed_engine`'
expect_rejected backend-accessor 'no method named `backend`'

echo "public crate boundary valid: facade compiles; ports, internal crates, and backend access are unreachable"
