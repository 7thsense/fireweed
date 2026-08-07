#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

write_file_list() {
    local list_file="$1"
    shift
    : >"$list_file"
    local path
    for path in "$@"; do
        printf '%s\n' "$path" >>"$list_file"
    done
}

expect_rejects() {
    local fixture="$1"
    local label="$2"
    local list_file="$tmp_dir/files-$label.txt"
    write_file_list "$list_file" "$fixture"
    if "$script_dir/verify-public-identity.sh" --root "$tmp_dir" --files-from "$list_file" >"$tmp_dir/$label.out" 2>"$tmp_dir/$label.err"; then
        echo "expected verifier to reject $label fixture" >&2
        cat "$tmp_dir/$label.out" >&2
        cat "$tmp_dir/$label.err" >&2
        exit 1
    fi
}

expect_accepts() {
    local fixture="$1"
    local label="$2"
    local list_file="$tmp_dir/files-$label.txt"
    write_file_list "$list_file" "$fixture"
    if ! "$script_dir/verify-public-identity.sh" --root "$tmp_dir" --files-from "$list_file" >"$tmp_dir/$label.out" 2>"$tmp_dir/$label.err"; then
        echo "expected verifier to accept $label fixture" >&2
        cat "$tmp_dir/$label.out" >&2
        cat "$tmp_dir/$label.err" >&2
        exit 1
    fi
}

expect_allowlist_rejects() {
    local allowlist="$1"
    local label="$2"
    local list_file="$tmp_dir/files-$label.txt"
    write_file_list "$list_file" "src/fireweed.md"
    if "$script_dir/verify-public-identity.sh" \
        --root "$tmp_dir" \
        --files-from "$list_file" \
        --allowlist "$allowlist" \
        >"$tmp_dir/$label.out" 2>"$tmp_dir/$label.err"; then
        echo "expected verifier to reject $label allowlist" >&2
        cat "$tmp_dir/$label.out" >&2
        cat "$tmp_dir/$label.err" >&2
        exit 1
    fi
}

mkdir -p "$tmp_dir/src" \
    "$tmp_dir/docs/helix/00-discover" \
    "$tmp_dir/docs/helix/02-design/adr" \
    "$tmp_dir/crates/fireweed-core/src" \
    "$tmp_dir/scripts/ci/fixtures/public-crate-boundary/src"

printf '%s\n' 'The pqueue CLI is public.' >"$tmp_dir/src/lowercase.md"
printf '%s\n' 'fn construct_pqueue_runtime() {}' >"$tmp_dir/src/snake-case.md"
printf '%s\n' 'The Pqueue type is public.' >"$tmp_dir/src/camelcase.md"
printf '%s\n' 'The EmbeddedPqueue type is public.' >"$tmp_dir/src/embedded-camelcase.md"
printf '%s\n' 'Set PQUEUE_PG_URL before running.' >"$tmp_dir/src/uppercase.md"
printf '%s\n' 'Queueyard is the public product name.' >"$tmp_dir/src/queueyard.md"
printf '%s\n' 'Clone https://github.com/telepathdata/7thsense-pqueue.git.' >"$tmp_dir/src/repository.md"
printf '%s\n' 'Send PQ.CLAIM over RESP.' >"$tmp_dir/src/resp-uppercase.md"
printf '%s\n' 'No PQ* extension commands are required.' >"$tmp_dir/src/resp-prefix.md"
printf '%s\n' "ERR wrong number of arguments for 'pq.mget'" >"$tmp_dir/src/resp-lowercase.md"
printf '%s\n' 'Set the pq-tenant-id header.' >"$tmp_dir/src/short-prefix.md"
printf '%s\n' 'use pqueue_core::QueueId;' >"$tmp_dir/crates/fireweed-core/src/lib.rs"
printf '%s\n' '[package]' 'name = "pqueue-core"' >"$tmp_dir/crates/fireweed-core/Cargo.toml"
printf '%s\n' 'Current identity is Fireweed.' >"$tmp_dir/src/fireweed.md"
printf '%s\n' 'Audit item pqueue-a997391c is immutable.' >"$tmp_dir/src/bead-id.md"
printf '%s\n' 'This longer pqueue-a997391c-alias is not an immutable bead ID.' >"$tmp_dir/src/bead-prefix.md"
printf '%s\n' 'No content residue.' >"$tmp_dir/src/old-pqueue-path.md"

expect_rejects "src/lowercase.md" "lowercase"
expect_rejects "src/snake-case.md" "snake-case"
expect_rejects "src/camelcase.md" "camelcase"
expect_rejects "src/embedded-camelcase.md" "embedded-camelcase"
expect_rejects "src/uppercase.md" "uppercase"
expect_rejects "src/queueyard.md" "queueyard"
expect_rejects "src/repository.md" "repository"
expect_rejects "src/resp-uppercase.md" "resp-uppercase"
expect_rejects "src/resp-prefix.md" "resp-prefix"
expect_rejects "src/resp-lowercase.md" "resp-lowercase"
expect_rejects "src/short-prefix.md" "short-prefix"
expect_rejects "crates/fireweed-core/src/lib.rs" "rust-namespace"
expect_rejects "crates/fireweed-core/Cargo.toml" "cargo-namespace"
expect_rejects "src/bead-prefix.md" "bead-prefix"
expect_rejects "src/old-pqueue-path.md" "path-residue"
expect_accepts "src/fireweed.md" "fireweed"
expect_accepts "src/bead-id.md" "immutable-bead-id"

# Exact historical paths may retain only the token classes declared for them.
printf '%s\n' 'ADR-020 records the old pqueue namespace.' \
    >"$tmp_dir/docs/helix/02-design/adr/ADR-020-public-namespace-and-compatibility.md"
expect_accepts \
    "docs/helix/02-design/adr/ADR-020-public-namespace-and-compatibility.md" \
    "exact-historical-path"

printf '%s\n' 'An unrelated document may not claim the pqueue exception.' \
    >"$tmp_dir/docs/helix/02-design/adr/unapproved-history.md"
expect_rejects \
    "docs/helix/02-design/adr/unapproved-history.md" \
    "adjacent-history-path"

# The negative compile fixtures are exact exceptions; copying one elsewhere is not.
printf '%s\n' 'use fireweed::Pqueue;' \
    >"$tmp_dir/scripts/ci/fixtures/public-crate-boundary/src/rejected_retired_generic_facade.rs"
expect_accepts \
    "scripts/ci/fixtures/public-crate-boundary/src/rejected_retired_generic_facade.rs" \
    "exact-negative-fixture"

printf '%s\n' 'use fireweed::Pqueue;' >"$tmp_dir/src/copied-negative.rs"
expect_rejects "src/copied-negative.rs" "copied-negative-fixture"

# Schema v2 cannot express a broad subtree exception or a compatibility class.
printf '%s\n' \
    '{"schema":"fireweed-public-identity-allowlist-v2","adr":"ADR-023","entries":[{"id":"broad","class":"historical/audit","reason":"test","owner_surface":"test","removal_condition":"test","paths":["docs/*"],"match_pattern":"pqueue"}]}' \
    >"$tmp_dir/broad-allowlist.json"
expect_allowlist_rejects "$tmp_dir/broad-allowlist.json" "broad-path"

printf '%s\n' \
    '{"schema":"fireweed-public-identity-allowlist-v2","adr":"ADR-023","entries":[{"id":"compat","class":"temporary compatibility","reason":"test","owner_surface":"test","removal_condition":"test","paths":["src/fireweed.md"],"match_pattern":"pqueue"}]}' \
    >"$tmp_dir/compatibility-allowlist.json"
expect_allowlist_rejects "$tmp_dir/compatibility-allowlist.json" "compatibility-class"

# P17a: new markdown hyperlinks into .ddx/** fail structural scan; inert
# historical v0.14.0 targets are classified when present under that exact path.
mkdir -p "$tmp_dir/docs/releases"
printf '%s\n' 'See [.ddx/executions/new/note.md](../../.ddx/executions/new/note.md).' \
    >"$tmp_dir/docs/releases/v0.99.0.md"
expect_rejects "docs/releases/v0.99.0.md" "new-ddx-hyperlink"

printf '%s\n' \
    'Authoritative evidence: [.ddx/executions/20260715T043214-936c36b0/release-evidence-correction.md](../../.ddx/executions/20260715T043214-936c36b0/release-evidence-correction.md).' \
    >"$tmp_dir/docs/releases/v0.14.0.md"
# v0.14.0 inert hyperlink alone (no retired identity token) must accept.
list_file="$tmp_dir/files-inert-ddx.txt"
printf '%s\n' 'docs/releases/v0.14.0.md' >"$list_file"
if ! "$script_dir/verify-public-identity.sh" --root "$tmp_dir" --files-from "$list_file" \
    >"$tmp_dir/inert-ddx.out" 2>"$tmp_dir/inert-ddx.err"; then
    echo "inert historical .ddx hyperlink unexpectedly rejected" >&2
    cat "$tmp_dir/inert-ddx.err" >&2
    exit 1
fi
grep -Fq 'inert .ddx hyperlinks classified=' "$tmp_dir/inert-ddx.out" || {
    echo "structural scan did not classify inert .ddx hyperlinks" >&2
    cat "$tmp_dir/inert-ddx.out" >&2
    exit 1
}

echo "public identity residue verifier focused tests passed"
