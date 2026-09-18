#!/usr/bin/env bash
# Tests of projection verify/delete/rebuild MUST call Fireweed::projection_control().
# Unlinking Turso/SQLite projection files and reopening is not a ProjectionLifecycle
# implementation. Crash-loss / RecoveryAction-on-open belongs in server or workload
# recovery suites, not as a stand-in for the live facade API.
set -euo pipefail

ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
TESTS="${TESTS:-${ROOT}/crates/fireweed/tests}"

# Fixture teardown may unlink files after Drop. Everything else in the public
# facade tests must use projection_control() for delete/rebuild.
ALLOWLIST='support/ss_capacity.rs'

if [[ "${1:-}" == "--self-test" ]]; then
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    mkdir -p "$tmp/crates/fireweed/tests"
    cat >"$tmp/crates/fireweed/tests/fake.rs" <<'EOF'
fn fake() {
    std::fs::remove_file(path.join("projection.turso")).unwrap();
}
EOF
    if ROOT="$tmp" TESTS="$tmp/crates/fireweed/tests" bash "$0"; then
        echo "forbid-fake-projection-lifecycle: self-test expected a failure" >&2
        exit 1
    fi
    echo "forbid-fake-projection-lifecycle: self-test OK"
    exit 0
fi

hits="$(
    grep -RInE 'remove_file' --include='*.rs' "$TESTS" 2>/dev/null \
        | grep -iE 'projection(\.(turso|db|sqlite))?|-wal"|-shm"|proj_path|projection_path' \
        | grep -vE "${ALLOWLIST}" \
        || true
)"

if [[ -n "$hits" ]]; then
    echo "forbid-fake-projection-lifecycle: facade tests must not unlink projection files as a rebuild stand-in." >&2
    echo "Use Fireweed::projection_control().delete/rebuild instead." >&2
    echo "$hits" >&2
    exit 1
fi

echo "forbid-fake-projection-lifecycle: OK"
