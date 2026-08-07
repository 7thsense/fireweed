#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# Isolated clean source root so product WIP cannot break the shared predicate.
SOURCE_WT="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-governed-source.XXXXXX")"
git -C "$REPO_ROOT" worktree add --detach "$SOURCE_WT" HEAD >/dev/null
REVISION="$(git -C "$SOURCE_WT" rev-parse HEAD)"
EXPECTED_REMOTE="$(git -C "$SOURCE_WT" remote get-url origin 2>/dev/null || echo origin)"
if git -C "$SOURCE_WT" remote get-url origin >/dev/null 2>&1; then
  EXPECTED_REMOTE_ARG="origin"
else
  git -C "$SOURCE_WT" remote add origin "https://example.invalid/fireweed.git"
  EXPECTED_REMOTE_ARG="origin"
fi
TAG="v0.0.0-archive-test"
PRODUCED_AT="2026-07-20T00:00:00Z"
REVIEWED_AT="2026-07-20T00:05:00Z"
CASE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/fireweed-governed-archive-test.XXXXXX")"
cleanup() {
  rm -rf "$CASE_ROOT"
  git -C "$REPO_ROOT" worktree remove --force "$SOURCE_WT" >/dev/null 2>&1 || rm -rf "$SOURCE_WT"
}
trap cleanup EXIT

fail() {
  echo "build-governed-evidence-bundle-test: $*" >&2
  exit 1
}

expect_failure() {
  local label="$1"
  shift
  if "$@" >"$CASE_ROOT/$label.out" 2>&1; then
    fail "$label unexpectedly passed"
  fi
}

make_inputs() {
  local case_dir="$1"
  mkdir -p "$case_dir/source" "$case_dir/e3" "$case_dir/bin"
  for name in e0 e1 e2-scale; do
    printf '{"source_revision":"%s","authority":"%s"}\n' "$REVISION" "$name" >"$case_dir/source/$name.jsonl"
  done
  printf '{"measurements":{"revision":"%s"}}\n' "$REVISION" >"$case_dir/source/e2-density.jsonl"
  printf '{"source_revision":"%s"}\n' "$REVISION" >"$case_dir/source/e2-failover.json"
  printf '{"source_revision":"%s","authority":"e3"}\n' "$REVISION" >"$case_dir/e3/e3.jsonl"
  printf '{"source_revision":"%s","authority":"tp003"}\n' "$REVISION" >"$case_dir/e3/tp003.jsonl"
  printf '{"source_revision":"%s","authority":"fencing"}\n' "$REVISION" >"$case_dir/e3/fencing.json"
  printf 'must not be archived\n' >"$case_dir/source/stale-unlisted.jsonl"
  printf 'must not be archived\n' >"$case_dir/e3/stale-unlisted.jsonl"
  apply_fake_rustup "$case_dir/bin/rustup"
}

apply_fake_rustup() {
  local path="$1"
  # Literal runtime expressions belong to the generated command shim.
  # shellcheck disable=SC2016
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'args=("$@")' \
    'value_after() { local wanted="$1"; shift; while (($#)); do if [[ "$1" == "$wanted" ]]; then printf "%s" "$2"; return; fi; shift; done; exit 2; }' \
    'case " $* " in' \
    '  *" --bin fireweed-build-e3-contract "*) out="$(value_after --out "${args[@]}")"; printf "{\"schema_version\":1}\n" >"$out" ;;' \
    '  *" --bin fireweed-build-evidence-attestation "*) out="$(value_after --out "${args[@]}")"; commit="$(value_after --commit "${args[@]}")"; printf "{\"source\":{\"commit\":\"%s\"}}\n" "$commit" >"$out" ;;' \
    'esac' >"$path"
  chmod +x "$path"
}

common_args() {
  printf '%s\n' \
    --source-root "$SOURCE_WT" \
    --expected-source "$REVISION" \
    --expected-remote "$EXPECTED_REMOTE_ARG" \
    --expected-ref HEAD
}

run_stage() {
  local case_dir="$1"
  # shellcheck disable=SC2046
  PATH="$case_dir/bin:$PATH" bash "$SCRIPT_DIR/build-governed-evidence-bundle.sh" \
    --source-dir "$case_dir/source" \
    --e3-source-dir "$case_dir/e3" \
    --out "$case_dir/tp002-release" \
    --revision "$REVISION" \
    $(common_args) \
    --tag "$TAG" \
    --produced-at "$PRODUCED_AT" \
    --reviewed-at "$REVIEWED_AT"
}

for run in first second; do
  make_inputs "$CASE_ROOT/$run"
  run_stage "$CASE_ROOT/$run"
  archive="$CASE_ROOT/$run/$REVISION.tar.gz"
  sidecar="$archive.sha256"
  [[ -s "$archive" && -s "$sidecar" ]] || fail "$run did not produce archive and sidecar"
  (cd "$CASE_ROOT/$run" && sha256sum -c "$(basename "$sidecar")")
  tar -tzf "$archive" >"$CASE_ROOT/$run.contents"
  if grep -Eq '(^/|(^|/)\.\.(/|$))' "$CASE_ROOT/$run.contents"; then
    fail "$run archive contains an unsafe path"
  fi
  printf '%s\n' \
    tp002-release/ \
    tp002-release/attestation.json \
    tp002-release/composite-contract.json \
    tp002-release/e0.jsonl \
    tp002-release/e1.jsonl \
    tp002-release/e2-density.jsonl \
    tp002-release/e2-failover.json \
    tp002-release/e2-scale.jsonl \
    tp002-release/e3/ \
    tp002-release/e3/e3-contract.json \
    tp002-release/e3/e3.jsonl \
    tp002-release/e3/fencing.json \
    tp002-release/e3/tp003.jsonl >"$CASE_ROOT/$run.expected-contents"
  diff -u "$CASE_ROOT/$run.expected-contents" "$CASE_ROOT/$run.contents" ||
    fail "$run archive content set is not exact"
  for required in \
    tp002-release/composite-contract.json \
    tp002-release/e0.jsonl tp002-release/e1.jsonl \
    tp002-release/e2-scale.jsonl tp002-release/e2-density.jsonl \
    tp002-release/e2-failover.json tp002-release/attestation.json \
    tp002-release/e3/e3.jsonl tp002-release/e3/tp003.jsonl \
    tp002-release/e3/fencing.json tp002-release/e3/e3-contract.json; do
    grep -Fxq "$required" "$CASE_ROOT/$run.contents" || fail "$run archive missing $required"
  done
  if grep -Fq stale-unlisted "$CASE_ROOT/$run.contents"; then
    fail "$run archive admitted an unlisted stale input"
  fi
  tar -xOf "$archive" tp002-release/attestation.json |
    grep -Fq "\"commit\":\"$REVISION\"" || fail "$run attestation is not revision-bound"
done

cmp "$CASE_ROOT/first/$REVISION.tar.gz" "$CASE_ROOT/second/$REVISION.tar.gz" ||
  fail "identical governed inputs did not produce a deterministic archive"

wrong_revision=0000000000000000000000000000000000000000
make_inputs "$CASE_ROOT/wrong-revision"
expect_failure wrong_revision env PATH="$CASE_ROOT/wrong-revision/bin:$PATH" \
  bash "$SCRIPT_DIR/build-governed-evidence-bundle.sh" \
  --source-dir "$CASE_ROOT/wrong-revision/source" --e3-source-dir "$CASE_ROOT/wrong-revision/e3" \
  --out "$CASE_ROOT/wrong-revision/tp002-release" --revision "$wrong_revision" \
  --source-root "$SOURCE_WT" --expected-source "$wrong_revision" \
  --expected-remote "$EXPECTED_REMOTE_ARG" --expected-ref HEAD

# Missing expected-source flags (ambient SHA path) must fail.
make_inputs "$CASE_ROOT/missing-expected"
expect_failure missing_expected env PATH="$CASE_ROOT/missing-expected/bin:$PATH" \
  bash "$SCRIPT_DIR/build-governed-evidence-bundle.sh" \
  --source-dir "$CASE_ROOT/missing-expected/source" --e3-source-dir "$CASE_ROOT/missing-expected/e3" \
  --out "$CASE_ROOT/missing-expected/tp002-release" --revision "$REVISION"

make_inputs "$CASE_ROOT/inside"
inside="$(mktemp -d "$REPO_ROOT/target/governed-archive-inside.XXXXXX")"
trap 'rm -rf "$CASE_ROOT" "$inside"; git -C "$REPO_ROOT" worktree remove --force "$SOURCE_WT" >/dev/null 2>&1 || true' EXIT
expect_failure inside_repo env PATH="$CASE_ROOT/inside/bin:$PATH" \
  bash "$SCRIPT_DIR/build-governed-evidence-bundle.sh" \
  --source-dir "$CASE_ROOT/inside/source" --e3-source-dir "$CASE_ROOT/inside/e3" \
  --out "$inside/tp002-release" --revision "$REVISION" \
  --source-root "$SOURCE_WT" --expected-source "$REVISION" \
  --expected-remote "$EXPECTED_REMOTE_ARG" --expected-ref HEAD

make_inputs "$CASE_ROOT/stale-archive"
touch "$CASE_ROOT/stale-archive/$REVISION.tar.gz"
expect_failure stale_archive env PATH="$CASE_ROOT/stale-archive/bin:$PATH" \
  bash "$SCRIPT_DIR/build-governed-evidence-bundle.sh" \
  --source-dir "$CASE_ROOT/stale-archive/source" --e3-source-dir "$CASE_ROOT/stale-archive/e3" \
  --out "$CASE_ROOT/stale-archive/tp002-release" --revision "$REVISION" \
  --source-root "$SOURCE_WT" --expected-source "$REVISION" \
  --expected-remote "$EXPECTED_REMOTE_ARG" --expected-ref HEAD \
  --tag "$TAG" --produced-at "$PRODUCED_AT" --reviewed-at "$REVIEWED_AT"
[[ ! -e "$CASE_ROOT/stale-archive/tp002-release" ]] ||
  fail "stale archive rejection still staged a substitute bundle"

# Dirty product path outside .ddx must fail the shared predicate.
make_inputs "$CASE_ROOT/dirty-source"
printf 'dirty\n' >>"$SOURCE_WT/Cargo.toml"
expect_failure dirty_source env PATH="$CASE_ROOT/dirty-source/bin:$PATH" \
  bash "$SCRIPT_DIR/build-governed-evidence-bundle.sh" \
  --source-dir "$CASE_ROOT/dirty-source/source" --e3-source-dir "$CASE_ROOT/dirty-source/e3" \
  --out "$CASE_ROOT/dirty-source/tp002-release" --revision "$REVISION" \
  --source-root "$SOURCE_WT" --expected-source "$REVISION" \
  --expected-remote "$EXPECTED_REMOTE_ARG" --expected-ref HEAD
git -C "$SOURCE_WT" checkout -- Cargo.toml

echo "build-governed-evidence-bundle-test: PASS"
