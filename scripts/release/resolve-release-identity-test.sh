#!/usr/bin/env bash
# Behavioral contract for P17r release identity resolution (tag → E → S).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOLVER="${SCRIPT_DIR}/resolve-release-identity.sh"
CASE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-resolve-release-id.XXXXXX")"
trap 'rm -rf "$CASE_ROOT"' EXIT

fail() {
  echo "resolve-release-identity-test: $*" >&2
  exit 1
}

expect_failure() {
  local label="$1"
  shift
  if "$@" >"$CASE_ROOT/${label}.out" 2>"$CASE_ROOT/${label}.err"; then
    fail "${label} unexpectedly passed"
  fi
}

# --- fixture: origin bare + clone with S then promoted E ---
ORIGIN="$CASE_ROOT/origin.git"
WORK="$CASE_ROOT/work"
git init --bare -q "$ORIGIN"
git clone -q "$ORIGIN" "$WORK"
git -C "$WORK" config user.name Test
git -C "$WORK" config user.email test@example.invalid

mkdir -p "$WORK/src" "$WORK/target/tp002-release/e3" "$WORK/docs/perf/evidence"
cat >"$WORK/Cargo.toml" <<'TOML'
[workspace]
members = []
resolver = "2"

[workspace.package]
version = "0.30.0"
edition = "2024"
license = "MIT OR Apache-2.0"
TOML
printf 'fn main() {}\n' >"$WORK/src/lib.rs"
printf '# note\n' >"$WORK/docs/perf/evidence/current-note.md"
git -C "$WORK" add .
GIT_AUTHOR_DATE='2026-01-01T00:00:00Z' GIT_COMMITTER_DATE='2026-01-01T00:00:00Z' \
  git -C "$WORK" commit -qm 'source S'
S="$(git -C "$WORK" rev-parse HEAD)"
git -C "$WORK" branch -M main
git -C "$WORK" push -q origin main
git -C "$WORK" update-ref "refs/heads/release-source/v0.30.0" "$S"

# Build promoted evidence tree at E (parent = S) with promotion message metadata.
PROMO="$CASE_ROOT/promo"
git -C "$WORK" worktree add --detach -q "$PROMO" "$S"
mkdir -p "$PROMO/target/tp002-release/e3"
printf '{"schema_version":1,"source_revision":"%s","authorities":{"e0":"e0.jsonl"}}\n' "$S" \
  >"$PROMO/target/tp002-release/composite-contract.json"
printf '{"schema_version":1,"policy":"exact-tag-rerun","scope":"tp002-release-v1","source":{"tag":"v0.30.0","commit":"%s"},"producing_command":"test","produced_at":"2026-01-01T00:00:00Z","reviewed_at":"2026-01-01T00:00:00Z","evidence":[{"path":"target/tp002-release/composite-contract.json","sha256":"%s"}],"inputs":[{"kind":"product_code","path":"Cargo.toml","sha256":"%s"},{"kind":"harness","path":"src/lib.rs","sha256":"%s"},{"kind":"config","path":"Cargo.toml","sha256":"%s"},{"kind":"dependency_lock","path":"Cargo.toml","sha256":"%s"}]}\n' \
  "$S" \
  "$(sha256sum "$PROMO/target/tp002-release/composite-contract.json" | awk '{print $1}')" \
  "$(sha256sum "$PROMO/Cargo.toml" | awk '{print $1}')" \
  "$(sha256sum "$PROMO/src/lib.rs" | awk '{print $1}')" \
  "$(sha256sum "$PROMO/Cargo.toml" | awk '{print $1}')" \
  "$(sha256sum "$PROMO/Cargo.toml" | awk '{print $1}')" \
  >"$PROMO/target/tp002-release/attestation.json"
printf '{}\n' >"$PROMO/target/tp002-release/e0.jsonl"
git -C "$PROMO" add target/tp002-release
GIT_AUTHOR_NAME='Fireweed Evidence Promoter' GIT_AUTHOR_EMAIL='evidence@invalid' \
GIT_COMMITTER_NAME='Fireweed Evidence Promoter' GIT_COMMITTER_EMAIL='evidence@invalid' \
GIT_AUTHOR_DATE='2026-01-02T00:00:00Z' GIT_COMMITTER_DATE='2026-01-02T00:00:00Z' \
  git -C "$PROMO" commit -qm "$(printf 'chore(evidence): promote governed bundle\n\nMeasured-source: %s\nSource-ref: refs/heads/release-source/v0.30.0\nCampaign: storage-closure\n' "$S")"
E="$(git -C "$PROMO" rev-parse HEAD)"
git -C "$PROMO" tag -a "v0.30.0" -m "release v0.30.0" "$E"
git -C "$PROMO" push -q origin "refs/tags/v0.30.0"
git -C "$WORK" fetch -q origin tag v0.30.0

echo "--- happy path: annotated tag resolve ---"
out="$(bash "$RESOLVER" resolve --repo "$WORK" --tag v0.30.0 --remote origin)"
grep -Fq "tag=v0.30.0" <<<"$out" || fail "missing tag"
grep -Fq "version=0.30.0" <<<"$out" || fail "missing version"
grep -Fq "evidence_commit=${E}" <<<"$out" || fail "evidence_commit mismatch"
grep -Fq "measured_source=${S}" <<<"$out" || fail "measured_source mismatch"
grep -Fq "source_ref=refs/heads/release-source/v0.30.0" <<<"$out" || fail "source_ref mismatch"
grep -Fq "tag_object_type=annotated" <<<"$out" || fail "expected annotated"

echo "--- parse-e from evidence checkout ---"
out="$(bash "$RESOLVER" parse-e --evidence-root "$PROMO")"
grep -Fq "evidence_commit=${E}" <<<"$out" || fail "parse-e E mismatch"
grep -Fq "measured_source=${S}" <<<"$out" || fail "parse-e S mismatch"

echo "--- dual-checkout path isolation (distinct roots) ---"
EVIDENCE_CO="$CASE_ROOT/evidence-co"
SOURCE_CO="$CASE_ROOT/source-co"
git clone -q --no-checkout "$ORIGIN" "$EVIDENCE_CO"
git -C "$EVIDENCE_CO" checkout -q "$E"
git clone -q --no-checkout "$ORIGIN" "$SOURCE_CO"
git -C "$SOURCE_CO" checkout -q "$S"
[[ "$(cd "$EVIDENCE_CO" && pwd -P)" != "$(cd "$SOURCE_CO" && pwd -P)" ]] ||
  fail "evidence and source checkouts must be physically distinct"
[[ "$(git -C "$EVIDENCE_CO" rev-parse HEAD)" == "$E" ]] || fail "evidence HEAD not E"
[[ "$(git -C "$SOURCE_CO" rev-parse HEAD)" == "$S" ]] || fail "source HEAD not S"
# Contaminating source tools must not be taken from E when S is the producer root.
test -f "$EVIDENCE_CO/target/tp002-release/composite-contract.json"
test ! -f "$SOURCE_CO/target/tp002-release/composite-contract.json"

echo "--- missing tag ---"
expect_failure missing_tag bash "$RESOLVER" resolve --repo "$WORK" --tag v9.9.9 --remote origin
grep -Eiq 'missing|could not be resolved' "$CASE_ROOT/missing_tag.err" ||
  fail "missing tag diagnostic"

echo "--- wrong tag version vs V ---"
# Create annotated tag pointing at E but named for a different version.
git -C "$PROMO" tag -a "v0.29.0" -m "wrong" "$E"
git -C "$PROMO" push -q origin "refs/tags/v0.29.0"
git -C "$WORK" fetch -q origin tag v0.29.0
expect_failure wrong_version bash "$RESOLVER" resolve --repo "$WORK" --tag v0.29.0 --remote origin
grep -Fq 'does not equal' "$CASE_ROOT/wrong_version.err" || fail "wrong version diagnostic"

echo "--- lightweight tag rejected ---"
# Fresh bare remote containing only a lightweight v0.30.0 → E (no annotation object).
LIGHT_ORIGIN="$CASE_ROOT/light-origin.git"
LIGHT="$CASE_ROOT/light"
git init --bare -q "$LIGHT_ORIGIN"
git clone -q "$ORIGIN" "$LIGHT"
git -C "$LIGHT" config user.name Test
git -C "$LIGHT" config user.email test@example.invalid
git -C "$LIGHT" fetch -q origin "+refs/heads/*:refs/heads/*" "+refs/tags/*:refs/tags/*" 2>/dev/null || true
# Ensure main and S/E objects exist, then publish only a lightweight tag to light-origin.
git -C "$LIGHT" checkout -q -B main "$S"
git -C "$LIGHT" remote remove origin
git -C "$LIGHT" remote add origin "$LIGHT_ORIGIN"
git -C "$LIGHT" push -q origin "refs/heads/main:refs/heads/main"
git -C "$LIGHT" push -q origin "${E}:refs/heads/evidence"
git -C "$LIGHT" tag -d v0.30.0 >/dev/null 2>&1 || true
git -C "$LIGHT" tag v0.30.0 "$E" # lightweight
git -C "$LIGHT" push -q origin "refs/tags/v0.30.0"
# Fresh clone from light-origin so fetch cannot revive the annotated tag object.
LIGHT2="$CASE_ROOT/light2"
git clone -q "$LIGHT_ORIGIN" "$LIGHT2"
expect_failure lightweight_remote bash "$RESOLVER" resolve --repo "$LIGHT2" --tag v0.30.0 --remote origin
grep -Eiq 'lightweight' "$CASE_ROOT/lightweight_remote.err" ||
  fail "lightweight tag diagnostic"

echo "--- S-ref mismatch (Source-ref does not resolve to S) ---"
BAD_ORIGIN="$CASE_ROOT/bad-origin.git"
BAD="$CASE_ROOT/bad-ref"
git init --bare -q "$BAD_ORIGIN"
git clone -q "$ORIGIN" "$BAD"
git -C "$BAD" config user.name Test
git -C "$BAD" config user.email test@example.invalid
git -C "$BAD" checkout -q -B main "$S"
mkdir -p "$BAD/target/tp002-release"
printf 'x\n' >"$BAD/target/tp002-release/note.txt"
git -C "$BAD" add target/tp002-release/note.txt
GIT_AUTHOR_NAME='x' GIT_AUTHOR_EMAIL='x@y' GIT_COMMITTER_NAME='x' GIT_COMMITTER_EMAIL='x@y' \
  git -C "$BAD" commit -qm "$(printf 'chore(evidence): bad\n\nMeasured-source: %s\nSource-ref: refs/heads/does-not-exist\nCampaign: t\n' "$S")"
BAD_E="$(git -C "$BAD" rev-parse HEAD)"
git -C "$BAD" tag -d v0.30.0 >/dev/null 2>&1 || true
git -C "$BAD" tag -a "v0.30.0" -m bad "$BAD_E"
git -C "$BAD" remote remove origin
git -C "$BAD" remote add origin "$BAD_ORIGIN"
git -C "$BAD" push -q origin "refs/heads/main:refs/heads/main"
git -C "$BAD" push -q origin "refs/tags/v0.30.0"
expect_failure s_ref_resolve bash "$RESOLVER" resolve --repo "$BAD" --tag v0.30.0 --remote origin
grep -Eiq 'Source-ref|resolves' "$CASE_ROOT/s_ref_resolve.err" ||
  fail "S-ref mismatch diagnostic"

echo "--- E-source contamination (Measured-source != parent) ---"
CONTAM="$CASE_ROOT/contam"
git -C "$WORK" worktree add --detach -q "$CONTAM" "$S"
# orphan-like: commit with wrong Measured-source
printf 'contam\n' >"$CONTAM/extra.txt"
git -C "$CONTAM" add extra.txt
# Create a second parentless... can't easily. Amend message with wrong S:
FALSE_S="0123456789abcdef0123456789abcdef01234567"
GIT_AUTHOR_NAME='x' GIT_AUTHOR_EMAIL='x@y' GIT_COMMITTER_NAME='x' GIT_COMMITTER_EMAIL='x@y' \
  git -C "$CONTAM" commit -qm "$(printf 'chore(evidence): contam\n\nMeasured-source: %s\nSource-ref: refs/heads/main\nCampaign: t\n' "$FALSE_S")"
CONTAM_E="$(git -C "$CONTAM" rev-parse HEAD)"
# For parse-e, parent is S but Measured-source is FALSE_S
git -C "$CONTAM" checkout -q "$CONTAM_E"
expect_failure e_contam bash "$RESOLVER" parse-e --evidence-root "$CONTAM"
grep -Eiq 'parent|Measured-source|mismatch|contamination' "$CASE_ROOT/e_contam.err" ||
  fail "E contamination diagnostic"

echo "--- composite source_revision mismatch ---"
MIS="$CASE_ROOT/mis-contract"
git clone -q --no-checkout "$ORIGIN" "$MIS"
git -C "$MIS" checkout -q "$E"
python3 - "$MIS/target/tp002-release/composite-contract.json" <<'PY'
import json,sys
p=sys.argv[1]
d=json.load(open(p,encoding="utf-8"))
d["source_revision"]="ffffffffffffffffffffffffffffffffffffffff"
json.dump(d, open(p,"w"), indent=2)
PY
expect_failure contract_mismatch bash "$RESOLVER" parse-e --evidence-root "$MIS"
grep -Eiq 'composite-contract|source_revision' "$CASE_ROOT/contract_mismatch.err" ||
  fail "contract mismatch diagnostic"

echo "--- ambient GITHUB_SHA must not appear as an executable reference ---"
# Comments may name the forbidden ambient variable; code must not expand it.
if grep -nE '\$\{?GITHUB_SHA\}?|GITHUB_SHA[[:space:]]*=' "$RESOLVER"; then
  fail "resolver must not read or assign GITHUB_SHA"
fi

echo "resolve-release-identity-test: PASS"
