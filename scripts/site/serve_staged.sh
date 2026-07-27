#!/usr/bin/env bash
# Stage the Pages tree and serve it locally for Playwright / manual checks.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${1:-${ROOT}/target/site-pages}"
PORT="${PORT:-4173}"

python3 "${ROOT}/scripts/site/stage_pages.py" "${OUT}"
cd "${OUT}"
echo "Serving ${OUT} at http://127.0.0.1:${PORT}/ (site at /site/)"
exec python3 -m http.server "${PORT}" --bind 127.0.0.1
