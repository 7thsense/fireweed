#!/usr/bin/env bash
# Microsite gate: link check + example documents + stage validation.
#
# Invoked by .github/workflows/pages.yml (build job) and by agents before
# committing site changes (see AGENTS.md). Needs python3 with PyYAML.
#
# Fail closed on broken local hrefs so GitHub Pages cannot stay stale behind a
# red pages workflow.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

echo "--- microsite: check links ---"
python3 scripts/site/check_links.py

echo "--- microsite: example documents and rendered pages ---"
python3 scripts/site/check_example_provenance.py

echo "--- microsite: stage + validate Pages tree ---"
STAGE_DIR="${MICROSITE_STAGE_DIR:-${ROOT}/target/site-pages}"
python3 scripts/site/stage_pages.py "${STAGE_DIR}"

echo "=== microsite-gate PASSED ==="
