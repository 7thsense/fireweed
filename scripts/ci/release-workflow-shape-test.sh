#!/usr/bin/env bash
# P17r static shape contract for .github/workflows/release.yml.
#
# Pairs with scripts/release/resolve-release-identity-test.sh (behavioral
# tag/E/S cases). Together these are the two governed-release shape tests.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WF="${ROOT}/.github/workflows/release.yml"
cd "${ROOT}"

fail() {
  echo "release-workflow-shape-test: $*" >&2
  exit 1
}

[[ -f "${WF}" ]] || fail "missing ${WF}"
text="$(cat "${WF}")"

echo "--- triggers: push.tags v* + workflow_dispatch rerun ---"
grep -Fq 'push:' "${WF}" || fail "missing push trigger"
grep -Fq 'tags:' "${WF}" || fail "missing push.tags"
grep -Fq '"v*"' "${WF}" || grep -Fq "'v*'" "${WF}" || fail "missing v* tag filter"
grep -Fq 'workflow_dispatch:' "${WF}" || fail "missing workflow_dispatch"

echo "--- dual checkout paths ---"
grep -Fq 'path: fireweed-evidence' "${WF}" || fail "missing evidence checkout path"
grep -Fq 'path: fireweed-source' "${WF}" || fail "missing source checkout path"
grep -Fq 'path: fireweed-bootstrap' "${WF}" || fail "missing bootstrap checkout path"
grep -Fq 'fireweed-evidence' "${WF}" && grep -Fq 'fireweed-source' "${WF}" ||
  fail "dual-checkout names missing"

echo "--- identity resolution (no ambient GITHUB_SHA as S) ---"
grep -Fq 'resolve-release-identity.sh' "${WF}" || fail "missing identity resolver"
grep -Fq 'measured_source' "${WF}" || fail "missing measured_source wiring"
grep -Fq 'evidence_commit' "${WF}" || fail "missing evidence_commit wiring"
grep -Fq 'source_ref' "${WF}" || fail "missing source_ref wiring"
# Forbidden ambient SHA bindings for source producers / evidence revision.
if grep -nE -- '--expected-revision[[:space:]]+"\$GITHUB_SHA"|--expected-revision[[:space:]]+\$GITHUB_SHA' "${WF}"; then
  fail "composite still binds expected-revision to GITHUB_SHA"
fi
if grep -nE -- '--revision[[:space:]]+"\$\{GITHUB_SHA\}"|--revision[[:space:]]+"\$GITHUB_SHA"|--revision \$\{GITHUB_SHA\}|--revision \$GITHUB_SHA' "${WF}"; then
  fail "source-preview still binds --revision to GITHUB_SHA"
fi
if grep -nE -- '--commit[[:space:]]+"\$\{GITHUB_SHA\}"|--commit[[:space:]]+"\$GITHUB_SHA"|--commit \$\{?GITHUB_SHA\}?' "${WF}"; then
  fail "attestation/image evidence still binds --commit to GITHUB_SHA"
fi
if grep -nE 'sha-\$\{GITHUB_SHA\}|sha-\$GITHUB_SHA|sha_tag=.*GITHUB_SHA' "${WF}"; then
  fail "Docker sha_tag still uses GITHUB_SHA instead of measured S"
fi
if grep -nE 'origin/main' "${WF}"; then
  fail "release workflow must not use origin/main cleanliness as release identity"
fi
# Evidence URL by ambient SHA is retired; promoted evidence comes from E checkout.
if grep -nE 'FIREWEED_GOVERNED_EVIDENCE_BASE_URL|\$\{GITHUB_SHA\}\.tar\.gz' "${WF}"; then
  fail "release must consume promoted evidence from E checkout, not ambient-SHA URL"
fi
# Positive: immutable image tag is sha-${S} / measured_source.
grep -Fq 'sha-${s}' "${WF}" || grep -Fq 'sha-${{ steps.identity.outputs.measured_source }}' "${WF}" ||
  grep -Fq 'sha_tag=${image}:sha-' "${WF}" ||
  fail "missing sha-\${S} image tag wiring"

echo "--- source producers receive expected-source/remote/ref ---"
grep -Fq 'build-source-preview-artifacts.sh' "${WF}" || fail "missing source-preview builder"
grep -Fq -- '--expected-source' "${WF}" || fail "missing --expected-source"
grep -Fq -- '--expected-remote' "${WF}" || fail "missing --expected-remote"
grep -Fq -- '--expected-ref' "${WF}" || fail "missing --expected-ref"
grep -Fq 'verify-source-predicate.sh' "${WF}" || fail "missing Constraint 11 predicate"
grep -Fq -- '--mode e' "${WF}" || fail "missing dual-root E-mode predicate invocation"

echo "--- external run-owned root ---"
grep -Fq 'fireweed-release-run' "${WF}" || fail "missing external run-owned root"
grep -Fq 'RUNNER_TEMP' "${WF}" || fail "run root must be under RUNNER_TEMP"

echo "--- no services / no kind; Docker publication exception retained ---"
if grep -nE '^[[:space:]]*services:' "${WF}"; then
  fail "release.yml must not declare GitHub Actions services (move to governed-product)"
fi
if grep -nE 'postgres:[[:space:]]*$|image:[[:space:]]*postgres:' "${WF}"; then
  fail "release.yml must not declare postgres service"
fi
if grep -nE 'kindest/node|install kind|KIND_NODE_IMAGE|setup-kind' "${WF}"; then
  fail "release.yml must not install or pin kind (governed-product owns kind)"
fi
grep -Fq 'docker/build-push-action' "${WF}" || fail "Docker publication exception missing"
grep -Fq 'docker/login-action' "${WF}" || fail "GHCR login missing"

echo "--- governed composite + attestation bind measured S ---"
grep -Fq 'verify-governed-release-composite.sh' "${WF}" || fail "missing composite verifier"
grep -Fq 'fireweed-verify-evidence-attestation' "${WF}" || fail "missing attestation verifier"
grep -Fq 'steps.identity.outputs.measured_source' "${WF}" || fail "measured_source outputs unused"
grep -Fq 'FIREWEED_ALLOW_GOVERNED_PERF_EVIDENCE_ONLY' "${WF}" ||
  fail "governed-performance-only path requires FIREWEED_ALLOW_GOVERNED_PERF_EVIDENCE_ONLY"
# Order: composite/release-gate before exact-tag attestation bind.
composite_line="$(grep -nF 'verify-governed-release-composite.sh' "${WF}" | head -n1 | cut -d: -f1)"
attest_line="$(grep -nF 'fireweed-verify-evidence-attestation' "${WF}" | head -n1 | cut -d: -f1)"
[[ -n "${composite_line}" && -n "${attest_line}" && "${composite_line}" -lt "${attest_line}" ]] ||
  fail "composite/release gate must precede attestation bind"

echo "--- no required tracker callers ---"
if grep -nE 'verify-build-closure\.sh.*operator|--mode[[:space:]]+operator' "${WF}"; then
  fail "release.yml must not invoke operator tracker audit"
fi
if grep -nE '\.ddx/beads' "${WF}"; then
  fail "release.yml must not read .ddx tracker"
fi

echo "--- Hybrid product selectors absent ---"
if grep -nEiq 'hybrid-strict|hybrid-async|FIREWEED_PROJECTION_BACKEND=hybrid|projection_backend:[[:space:]]*hybrid' "${WF}"; then
  fail "Hybrid product residue in release.yml"
fi

echo "--- pair behavioral shape test exists ---"
test -x "${ROOT}/scripts/release/resolve-release-identity-test.sh" ||
  test -f "${ROOT}/scripts/release/resolve-release-identity-test.sh" ||
  fail "missing resolve-release-identity-test.sh"

echo "release-workflow-shape-test: PASS"
