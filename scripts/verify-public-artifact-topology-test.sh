#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
validator="${repo_root}/scripts/verify-public-artifact-topology.sh"
source_document="${repo_root}/docs/helix/02-design/public-artifact-topology.md"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT

"$validator" "$source_document"

omitted_document="${test_dir}/omitted.md"
awk '!/^\| pqueue-core \|/' "$source_document" >"$omitted_document"
if "$validator" "$omitted_document" >"${test_dir}/omitted.out" 2>"${test_dir}/omitted.err"; then
    echo "validator accepted an omitted workspace package" >&2
    exit 1
fi
grep -q 'workspace packages omitted from topology: pqueue-core' "${test_dir}/omitted.err"

duplicate_document="${test_dir}/duplicate.md"
awk '{ print; if (/^\| pqueue-core \|/) print }' "$source_document" >"$duplicate_document"
if "$validator" "$duplicate_document" >"${test_dir}/duplicate.out" 2>"${test_dir}/duplicate.err"; then
    echo "validator accepted a duplicate workspace package" >&2
    exit 1
fi
grep -q 'duplicate workspace package classification: pqueue-core' "${test_dir}/duplicate.err"

echo "public artifact topology validator self-test passed"
