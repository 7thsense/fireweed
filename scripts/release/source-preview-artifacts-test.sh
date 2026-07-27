#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
case_root="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-source-preview-test.XXXXXX")"; trap 'rm -rf "$case_root"' EXIT
repo="$case_root/repo"; mkdir -p "$repo/src"; git -C "$case_root" init -q repo
git -C "$repo" config user.name Test; git -C "$repo" config user.email test@example.invalid
printf '%s\n' '[package]' 'name = "fireweed-fixture"' 'version = "0.1.0"' 'edition = "2024"' 'license = "MIT OR Apache-2.0"' >"$repo/Cargo.toml"
printf '%s\n' 'pub fn fixture() {}' >"$repo/src/lib.rs"; git -C "$repo" add .
GIT_AUTHOR_DATE='2026-01-01T00:00:00Z' GIT_COMMITTER_DATE='2026-01-01T00:00:00Z' git -C "$repo" commit -qm fixture
revision="$(git -C "$repo" rev-parse HEAD)"
for run in first second; do bash "$SCRIPT_DIR/build-source-preview-artifacts.sh" --repo "$repo" --out "$case_root/$run" --version 0.1.0 --revision "$revision" --builder test-builder; done
diff -qr "$case_root/first" "$case_root/second"
bash "$SCRIPT_DIR/verify-source-preview-artifacts.sh" --dist "$case_root/first" --version 0.1.0 --revision "$revision"
printf 'tamper\n' >>"$case_root/first/fireweed-0.1.0-source.tar.gz"
if bash "$SCRIPT_DIR/verify-source-preview-artifacts.sh" --dist "$case_root/first" --version 0.1.0 --revision "$revision" >/dev/null 2>&1; then echo "tampered source archive was accepted" >&2; exit 1; fi
grep -Fq 'build-source-preview-artifacts.sh' "$SCRIPT_DIR/../../.github/workflows/release.yml"
grep -Fq 'verify-source-preview-artifacts.sh' "$SCRIPT_DIR/../../.github/workflows/release.yml"
echo "source-preview-artifacts-test: PASS"
