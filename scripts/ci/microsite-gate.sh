#!/usr/bin/env bash
# Microsite gate: link check + example provenance + stage validation.
#
# Invoked by:
#   - .github/workflows/pages.yml (build job)
#   - scripts/ci/pr-gate.sh (bootstrap)
#   - scripts/ci/deployment-release-gate.sh (validate_docs_microsite)
#   - agent harness pre-commit / pre-push expectations (see AGENTS.md)
#
# Fail closed on broken local hrefs so GitHub Pages cannot stay stale behind a
# red pages workflow.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

echo "--- microsite: check links ---"
python3 scripts/site/check_links.py

echo "--- microsite: example provenance ---"
python3 scripts/site/check_example_provenance.py

echo "--- microsite: stage + validate Pages tree ---"
STAGE_DIR="${MICROSITE_STAGE_DIR:-${ROOT}/target/site-pages}"
python3 scripts/site/stage_pages.py "${STAGE_DIR}"

echo "=== microsite-gate PASSED ==="
