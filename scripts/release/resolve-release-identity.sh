#!/usr/bin/env bash
# P17r: resolve release tag → evidence commit E and measured source S.
#
# Never treats ambient GITHUB_SHA (or any workflow input SHA) as measured source.
# S and its immutable source-ref come only from E's promotion metadata.
#
# Modes:
#   resolve  — given a local/remote-accessible repo + tag, print identity
#   parse-e  — given an evidence checkout at E, parse promotion metadata
#
# Outputs (stdout, one KEY=value per line, stable order):
#   tag=...
#   version=...
#   evidence_commit=...   # E (peeled tag target)
#   measured_source=...   # S
#   source_ref=...        # immutable source ref recorded at promotion
#   tag_object_type=annotated
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MODE=""
REPO=""
TAG=""
EVIDENCE_ROOT=""
REMOTE="origin"
REQUIRE_ANNOTATED=1
EXPECTED_VERSION=""

usage() {
  cat <<'EOF' >&2
usage:
  resolve-release-identity.sh resolve \
    --repo <git-dir> --tag <vX.Y.Z> [--remote <name>] [--expected-version V] \
    [--allow-lightweight]
  resolve-release-identity.sh parse-e \
    --evidence-root <E-checkout> [--expected-version V]
EOF
  exit 64
}

fail() {
  echo "resolve-release-identity: $*" >&2
  exit 1
}

while (($#)); do
  case "$1" in
    resolve|parse-e) MODE="$1"; shift ;;
    --repo) REPO="${2:-}"; shift 2 ;;
    --tag) TAG="${2:-}"; shift 2 ;;
    --evidence-root) EVIDENCE_ROOT="${2:-}"; shift 2 ;;
    --remote) REMOTE="${2:-}"; shift 2 ;;
    --expected-version) EXPECTED_VERSION="${2:-}"; shift 2 ;;
    --allow-lightweight) REQUIRE_ANNOTATED=0; shift ;;
    -h|--help) usage ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[[ -n "$MODE" ]] || usage

is_full_sha() {
  [[ "${1:-}" =~ ^[0-9a-f]{40}$ ]]
}

emit() {
  local tag="$1" version="$2" e="$3" s="$4" ref="$5" obj_type="$6"
  printf 'tag=%s\n' "$tag"
  printf 'version=%s\n' "$version"
  printf 'evidence_commit=%s\n' "$e"
  printf 'measured_source=%s\n' "$s"
  printf 'source_ref=%s\n' "$ref"
  printf 'tag_object_type=%s\n' "$obj_type"
}

parse_promotion_message() {
  local repo="$1" e="$2"
  local body measured source_ref
  body="$(git -C "$repo" log -1 --format=%B "$e")"
  measured="$(printf '%s\n' "$body" | sed -n 's/^Measured-source:[[:space:]]*//p' | head -n1)"
  source_ref="$(printf '%s\n' "$body" | sed -n 's/^Source-ref:[[:space:]]*//p' | head -n1)"
  is_full_sha "$measured" || fail "E ${e} missing Measured-source 40-hex in promotion commit message"
  [[ -n "$source_ref" ]] || fail "E ${e} missing Source-ref in promotion commit message"
  printf '%s\n%s\n' "$measured" "$source_ref"
}

cross_check_promoted_contracts() {
  local evidence_root="$1" s="$2"
  local composite attestation
  composite="${evidence_root}/target/tp002-release/composite-contract.json"
  attestation="${evidence_root}/target/tp002-release/attestation.json"
  if [[ -f "$composite" ]]; then
    local contract_rev
    contract_rev="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1],encoding="utf-8"))["source_revision"])' "$composite")"
    [[ "$contract_rev" == "$s" ]] ||
      fail "composite-contract source_revision ${contract_rev} != measured source ${s}"
  fi
  if [[ -f "$attestation" ]]; then
    local att_commit
    att_commit="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1],encoding="utf-8"))["source"]["commit"])' "$attestation")"
    [[ "$att_commit" == "$s" ]] ||
      fail "attestation source.commit ${att_commit} != measured source ${s}"
  fi
}

workspace_version_at() {
  local repo="$1" rev="$2"
  git -C "$repo" show "${rev}:Cargo.toml" | awk '
    /^\[workspace\.package\]$/ { in_ws=1; next }
    in_ws && /^\[/ { exit }
    in_ws && /^version[[:space:]]*=/ {
      value=$0
      sub(/^[^=]*=[[:space:]]*"/, "", value)
      sub(/".*/, "", value)
      print value
      exit
    }
  '
}

resolve_tag() {
  local repo="$1" tag="$2" remote="$3"
  [[ -d "${repo}/.git" || -f "${repo}/.git" ]] || fail "--repo is not a git directory: ${repo}"
  [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "tag must match vMAJOR.MINOR.PATCH (got ${tag})"

  # Prefer remote resolution so manual dispatch never trusts a stale local ref alone.
  if git -C "$repo" remote get-url "$remote" >/dev/null 2>&1; then
    git -C "$repo" fetch --no-tags "$remote" "refs/tags/${tag}:refs/tags/${tag}" 2>/dev/null ||
      git -C "$repo" fetch "$remote" "refs/tags/${tag}:refs/tags/${tag}" ||
      fail "tag ${tag} could not be resolved through remote ${remote}"
  fi

  git -C "$repo" rev-parse -q --verify "refs/tags/${tag}" >/dev/null ||
    fail "tag ${tag} is missing"

  local obj_type peeled parent
  obj_type="$(git -C "$repo" cat-file -t "refs/tags/${tag}")"
  case "$obj_type" in
    tag)
      obj_type="annotated"
      ;;
    commit)
      obj_type="lightweight"
      if [[ "$REQUIRE_ANNOTATED" -eq 1 ]]; then
        fail "tag ${tag} is lightweight; release requires an annotated tag created by product-ready P20pr"
      fi
      ;;
    *)
      fail "tag ${tag} has unsupported object type ${obj_type}"
      ;;
  esac

  peeled="$(git -C "$repo" rev-parse "refs/tags/${tag}^{commit}")"
  is_full_sha "$peeled" || fail "could not peel tag ${tag} to a commit"

  local measured source_ref
  {
    read -r measured
    read -r source_ref
  } < <(parse_promotion_message "$repo" "$peeled")

  parent="$(git -C "$repo" rev-parse "${peeled}^" 2>/dev/null || true)"
  [[ "$parent" == "$measured" ]] ||
    fail "E parent ${parent:-missing} != Measured-source ${measured} (S/ref mismatch or E-source contamination)"

  # Source-ref must resolve to measured S in this repository.
  local resolved_ref
  resolved_ref="$(git -C "$repo" rev-parse -q --verify "${source_ref}^{commit}" 2>/dev/null || true)"
  [[ "$resolved_ref" == "$measured" ]] ||
    fail "Source-ref ${source_ref} resolves to '${resolved_ref:-missing}', not measured source ${measured}"

  local version
  version="$(workspace_version_at "$repo" "$measured")"
  [[ -n "$version" ]] || fail "unable to read workspace.package.version at S=${measured}"
  [[ "$tag" == "v${version}" ]] ||
    fail "tag ${tag} does not equal v\${V} (workspace V=${version} at S)"

  if [[ -n "$EXPECTED_VERSION" ]]; then
    [[ "$version" == "$EXPECTED_VERSION" ]] ||
      fail "expected-version ${EXPECTED_VERSION} != workspace V ${version}"
  fi

  emit "$tag" "$version" "$peeled" "$measured" "$source_ref" "$obj_type"
}

parse_e() {
  local evidence_root="$1"
  [[ -d "$evidence_root" ]] || fail "--evidence-root must be a directory"
  [[ -d "${evidence_root}/.git" || -f "${evidence_root}/.git" ]] ||
    fail "--evidence-root must be a git checkout"

  local e
  e="$(git -C "$evidence_root" rev-parse HEAD)"
  is_full_sha "$e" || fail "evidence HEAD is not a full commit"

  local measured source_ref
  {
    read -r measured
    read -r source_ref
  } < <(parse_promotion_message "$evidence_root" "$e")

  local parent
  parent="$(git -C "$evidence_root" rev-parse "${e}^" 2>/dev/null || true)"
  [[ "$parent" == "$measured" ]] ||
    fail "E parent ${parent:-missing} != Measured-source ${measured}"

  cross_check_promoted_contracts "$evidence_root" "$measured"

  local version tag
  version="$(workspace_version_at "$evidence_root" "$measured")"
  [[ -n "$version" ]] || fail "unable to read workspace version at S"
  tag="v${version}"
  if [[ -n "$EXPECTED_VERSION" ]]; then
    [[ "$version" == "$EXPECTED_VERSION" ]] ||
      fail "expected-version ${EXPECTED_VERSION} != workspace V ${version}"
  fi

  # Tag object type is unknown when only E is checked out; report as derived-from-E.
  emit "$tag" "$version" "$e" "$measured" "$source_ref" "derived-from-e"
}

case "$MODE" in
  resolve)
    [[ -n "$REPO" && -n "$TAG" ]] || usage
    resolve_tag "$REPO" "$TAG" "$REMOTE"
    ;;
  parse-e)
    [[ -n "$EVIDENCE_ROOT" ]] || usage
    parse_e "$EVIDENCE_ROOT"
    ;;
  *)
    usage
    ;;
esac
