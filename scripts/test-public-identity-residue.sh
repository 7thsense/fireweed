#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
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

mkdir -p "$tmp_dir/src" "$tmp_dir/docs/helix/history" "$tmp_dir/docs/deployment"

cat >"$tmp_dir/src/lowercase.md" <<'EOF'
The pqueue CLI is the public command.
EOF
cat >"$tmp_dir/src/uppercase.md" <<'EOF'
Set PQUEUE_PG_URL before running.
EOF
cat >"$tmp_dir/src/queueyard.md" <<'EOF'
Queueyard is the public product name.
EOF
cat >"$tmp_dir/src/repository.md" <<'EOF'
Clone https://github.com/telepathdata/7thsense-pqueue.git for the public repo.
EOF

expect_rejects "src/lowercase.md" "lowercase"
expect_rejects "src/uppercase.md" "uppercase"
expect_rejects "src/queueyard.md" "queueyard"
expect_rejects "src/repository.md" "repository"

cat >"$tmp_dir/docs/helix/history/queueyard.md" <<'EOF'
Queueyard remains in naming-analysis history for audit traceability.
EOF
cat >"$tmp_dir/docs/deployment/persistence.md" <<'EOF'
The persisted path /var/lib/pqueue/object-log and compatibility variable PQUEUE_OBJECT_LOG_DIR remain documented.
EOF

approved_list="$tmp_dir/approved-files.txt"
write_file_list "$approved_list" \
    "docs/helix/history/queueyard.md" \
    "docs/deployment/persistence.md"
"$script_dir/verify-public-identity.sh" --root "$tmp_dir" --files-from "$approved_list" >/dev/null

"$script_dir/verify-public-identity.sh" >/dev/null
git -C "$repo_root" diff --check

echo "public identity residue verifier tests passed"
