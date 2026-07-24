#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELPER="${SCRIPT_DIR}/release-failure-notification.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

summary_path="${tmp_dir}/summary.md"
stdout_path="${tmp_dir}/stdout.txt"

GITHUB_SERVER_URL="https://github.com" \
GITHUB_REPOSITORY="telepathdata/fireweed" \
GITHUB_RUN_ID="28641569553" \
GITHUB_RUN_ATTEMPT="2" \
GITHUB_WORKFLOW="release" \
GITHUB_JOB="release" \
GITHUB_STEP_SUMMARY="${summary_path}" \
bash "${HELPER}" >"${stdout_path}"

grep -Fq "::error title=Release workflow failed::" "${stdout_path}"
grep -Fq "actions/runs/28641569553/attempts/2" "${stdout_path}"
grep -Fq "## Release workflow failure" "${summary_path}"
grep -Fq "Publication is not healthy until this failure is resolved" "${summary_path}"

echo "release-failure-notification test passed"
