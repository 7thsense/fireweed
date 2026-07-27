#!/usr/bin/env bash
# Publish the staged microsite to the gh-pages branch (legacy GitHub Pages source).
# Used when GitHub Actions is unavailable at the org, or for a manual cut.
#
# Prefer .github/workflows/pages.yml once org Actions is enabled (workflow deploys
# via actions/deploy-pages). This script remains a supported fallback.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${ROOT}/target/site-pages"
REMOTE="${REMOTE:-origin}"
BRANCH="${BRANCH:-gh-pages}"

python3 "${ROOT}/scripts/site/stage_pages.py" "${OUT}"
python3 "${ROOT}/scripts/site/check_links.py"

WORKDIR="$(mktemp -d)"
cleanup() { rm -rf "${WORKDIR}"; }
trap cleanup EXIT

cp -a "${OUT}/." "${WORKDIR}/"
cd "${WORKDIR}"
git init -b "${BRANCH}" >/dev/null
git config user.email "${GIT_AUTHOR_EMAIL:-41898282+github-actions[bot]@users.noreply.github.com}"
git config user.name "${GIT_AUTHOR_NAME:-github-actions[bot]}"
git add -A
git commit -m "Deploy Fireweed microsite to GitHub Pages" >/dev/null
git remote add origin "$(git -C "${ROOT}" remote get-url "${REMOTE}")"
git push -u origin "HEAD:${BRANCH}" --force
echo "Published ${BRANCH} → GitHub Pages (https://telepathdata.github.io/fireweed/)"
