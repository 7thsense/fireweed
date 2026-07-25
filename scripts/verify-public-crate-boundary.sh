#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="${repo_root}/scripts/ci/fixtures/public-crate-boundary/Cargo.toml"
target_dir="${repo_root}/target/public-crate-boundary"
method_inventory="${repo_root}/scripts/ci/fixtures/public-crate-boundary/fireweed-methods.txt"

cargo check --quiet --offline --manifest-path "${fixture}" --target-dir "${target_dir}" --bin supported

actual_methods="$(
    rg -o '^    pub (async )?fn [a-zA-Z0-9_]+' "${repo_root}/crates/fireweed/src/facade.rs" |
        sed -E 's/^    pub (async )?fn //' |
        sort -u
)"
while IFS= read -r method; do
    [[ -z "${method}" ]] && continue
    if ! grep -Fxq "${method}" <<<"${actual_methods}"; then
        echo "supported Fireweed method is missing from the concrete facade: ${method}" >&2
        exit 1
    fi
done <"${method_inventory}"

expect_rejected() {
    local bin="$1"
    shift
    local output
    output="$(mktemp "${TMPDIR:-/tmp}/fireweed-public-boundary.XXXXXX")"
    if cargo check --offline --manifest-path "${fixture}" --target-dir "${target_dir}" --bin "${bin}" \
        >"${output}" 2>&1; then
        echo "public crate boundary unexpectedly compiled forbidden fixture: ${bin}" >&2
        rm -f "${output}"
        return 1
    fi
    local matched=false
    local diagnostic
    for diagnostic in "$@"; do
        if grep -Fq -- "${diagnostic}" "${output}"; then
            matched=true
            break
        fi
    done
    if [[ "${matched}" != true ]]; then
        echo "forbidden fixture ${bin} failed for an unexpected reason:" >&2
        sed -n '1,160p' "${output}" >&2
        rm -f "${output}"
        return 1
    fi
    rm -f "${output}"
}

expect_rejected_all() {
    local bin="$1"
    shift
    local output
    output="$(mktemp "${TMPDIR:-/tmp}/fireweed-public-boundary.XXXXXX")"
    if cargo check --offline --manifest-path "${fixture}" --target-dir "${target_dir}" --bin "${bin}" \
        >"${output}" 2>&1; then
        echo "public crate boundary unexpectedly compiled forbidden fixture: ${bin}" >&2
        rm -f "${output}"
        return 1
    fi
    local diagnostic
    for diagnostic in "$@"; do
        if ! grep -Fq -- "${diagnostic}" "${output}"; then
            echo "forbidden fixture ${bin} did not reject expected symbol ${diagnostic}:" >&2
            sed -n '1,200p' "${output}" >&2
            rm -f "${output}"
            return 1
        fi
    done
    rm -f "${output}"
}

expect_rejected raw-port 'trait `PushPort` is private'
expect_rejected internal-crate 'use of unresolved module or unlinked crate `fireweed_engine`'
expect_rejected backend-accessor 'no method named `backend`'
expect_rejected rejected-retired-generic-facade 'no `Pqueue` in the root' 'struct `Pqueue` is private'
expect_rejected rejected-retired-embedded-facade 'no `EmbeddedPqueue` in the root' 'struct `EmbeddedPqueue` is private'
expect_rejected rejected-retired-lib-backend 'no `LibBackend` in the root' 'trait `LibBackend` is private'
expect_rejected rejected-retired-embedded-handle 'no `EmbeddedHandle` in the root' 'struct `EmbeddedHandle` is private'
expect_rejected rejected-retired-generic-constructor 'no `Pqueue` in the root' 'struct `Pqueue` is private'
expect_rejected_all rejected-retired-embedded-config \
    'EmbeddedDurabilityConfig' \
    'EmbeddedObjectLogConfig' \
    'EmbeddedProjectionConfig' \
    'EmbeddedRecoveryAction' \
    'EmbeddedRecoveryPolicy' \
    'EmbeddedResponseBarrier' \
    'EmbeddedSecret' \
    'EmbeddedSegmentConfig' \
    'open_embedded' \
    'open_embedded_async' \
    'open_embedded_sqlite'

echo "public crate boundary valid: facade compiles; retired facade names, ports, internal crates, and backend access are unreachable"
