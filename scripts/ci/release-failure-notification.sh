#!/usr/bin/env bash
# Emit a visible release-failure alert for GitHub Actions and summarize the failure in the job summary.
#
# The workflow should call this from a failure-only step so a red release run
# leaves both a log annotation and a run summary entry.
set -euo pipefail

err() {
    echo "release-failure-notification: $*" >&2
}

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        err "missing required environment variable: ${name}"
        exit 1
    fi
}

require_env GITHUB_SERVER_URL
require_env GITHUB_REPOSITORY
require_env GITHUB_RUN_ID
require_env GITHUB_RUN_ATTEMPT
require_env GITHUB_WORKFLOW
require_env GITHUB_JOB

summary_path="${GITHUB_STEP_SUMMARY:-}"
if [[ -z "${summary_path}" ]]; then
    err "missing required environment variable: GITHUB_STEP_SUMMARY"
    exit 1
fi

run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
attempt_url="${run_url}/attempts/${GITHUB_RUN_ATTEMPT}"

cat <<EOF
::error title=Release workflow failed::Release workflow failed in job '${GITHUB_JOB}' for '${GITHUB_WORKFLOW}'. Review the run summary and logs: ${attempt_url}
EOF

{
    printf '## Release workflow failure\n\n'
    printf 'The release workflow failed in job `%s` for `%s`.\n\n' "${GITHUB_JOB}" "${GITHUB_WORKFLOW}"
    printf -- '- Run: %s\n' "${run_url}"
    printf -- '- Attempt: %s\n' "${attempt_url}"
    printf -- '- Repository: %s\n\n' "${GITHUB_REPOSITORY}"
    printf 'Publication is not healthy until this failure is resolved and the release job completes successfully.\n'
} >>"${summary_path}"
