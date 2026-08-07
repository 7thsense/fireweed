#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
case_root="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-source-preview-test.XXXXXX")"; trap 'rm -rf "$case_root"' EXIT
repo="$case_root/repo"; mkdir -p "$repo/src"; git -C "$case_root" init -q repo
git -C "$repo" config user.name Test; git -C "$repo" config user.email test@example.invalid
git -C "$repo" remote add origin "https://example.invalid/fireweed.git"
# Minimal tracked ignore so the shared predicate's P1 root coverage can be stubbed
# for this isolated fixture by pointing authority at a fixture manifest.
printf '%s\n' '[package]' 'name = "fireweed-fixture"' 'version = "0.1.0"' 'edition = "2024"' 'license = "MIT OR Apache-2.0"' >"$repo/Cargo.toml"
printf '%s\n' 'pub fn fixture() {}' >"$repo/src/lib.rs"
# Fixture authority with no declared roots so the isolated repo need not mirror product .gitignore.
cat >"$case_root/authority.json" <<'JSON'
{
  "tracked_ignore_policy": {
    "authority": "tracked_gitignore_only",
    "local_or_global_excludes_have_policy_authority": false,
    "forbidden_in_repository_paths": [".env.garage-e3"],
    "classes": {
      "administrative": {"roots": [], "required_proofs": []},
      "build_dependency_cache": {"roots": [], "required_proofs": []}
    }
  }
}
JSON
# Override predicate authority via symlink into scripts path? Instead, patch by
# wrapping: call build script after installing a thin predicate shim is too heavy.
# Build script always uses product authority; isolated fixture must satisfy it.
# Copy product .gitignore subset and create empty roots.
printf '%s\n' 'target/' '.ddx/agent-logs/' 'node_modules/' '__pycache__/' 'examples/python-resp/.venv/' >"$repo/.gitignore"
git -C "$repo" add .
GIT_AUTHOR_DATE='2026-01-01T00:00:00Z' GIT_COMMITTER_DATE='2026-01-01T00:00:00Z' git -C "$repo" commit -qm fixture
revision="$(git -C "$repo" rev-parse HEAD)"

# The shared predicate reads the product authority manifest paths relative to
# source-root declarations; product roots must be covered by fixture .gitignore
# (done above). Product authority still lists those roots — coverage check uses
# source-root's .gitignore.

for run in first second; do
  bash "$SCRIPT_DIR/build-source-preview-artifacts.sh" \
    --repo "$repo" \
    --out "$case_root/$run" \
    --version 0.1.0 \
    --revision "$revision" \
    --builder test-builder \
    --expected-source "$revision" \
    --expected-remote origin \
    --expected-ref HEAD
done
diff -qr "$case_root/first" "$case_root/second"
bash "$SCRIPT_DIR/verify-source-preview-artifacts.sh" --dist "$case_root/first" --version 0.1.0 --revision "$revision"
printf 'tamper\n' >>"$case_root/first/fireweed-0.1.0-source.tar.gz"
if bash "$SCRIPT_DIR/verify-source-preview-artifacts.sh" --dist "$case_root/first" --version 0.1.0 --revision "$revision" >/dev/null 2>&1; then
  echo "tampered source archive was accepted" >&2
  exit 1
fi

# Ambient-SHA path rejected.
if bash "$SCRIPT_DIR/build-source-preview-artifacts.sh" \
  --repo "$repo" --out "$case_root/no-expected" --version 0.1.0 --revision "$revision" --builder test-builder \
  >/dev/null 2>&1; then
  echo "missing expected-source flags were accepted" >&2
  exit 1
fi

# In-repo output rejected.
if bash "$SCRIPT_DIR/build-source-preview-artifacts.sh" \
  --repo "$repo" --out "$repo/out-inside" --version 0.1.0 --revision "$revision" --builder test-builder \
  --expected-source "$revision" --expected-remote origin --expected-ref HEAD \
  >/dev/null 2>&1; then
  echo "in-repo output was accepted" >&2
  exit 1
fi

grep -Fq 'build-source-preview-artifacts.sh' "$SCRIPT_DIR/../../.github/workflows/release.yml"
grep -Fq 'verify-source-preview-artifacts.sh' "$SCRIPT_DIR/../../.github/workflows/release.yml"
grep -Fq -- '--expected-source' "$SCRIPT_DIR/../../.github/workflows/release.yml"
grep -Fq -- '--expected-remote' "$SCRIPT_DIR/../../.github/workflows/release.yml"
grep -Fq -- '--expected-ref' "$SCRIPT_DIR/../../.github/workflows/release.yml"
if grep -Eq -- '--revision[[:space:]]+"?\$\{?GITHUB_SHA' "$SCRIPT_DIR/../../.github/workflows/release.yml"; then
  echo "release workflow still binds source-preview --revision to ambient GITHUB_SHA" >&2
  exit 1
fi
echo "source-preview-artifacts-test: PASS"
