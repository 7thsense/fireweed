#!/usr/bin/env bash
# Local deployment release gate for pqueue.
#
# This gate composes the existing source/release checks with deployment checks.
# The only tolerated local skip is the disposable kind backend matrix when the
# local Docker/kind toolchain is unavailable or Docker cannot be used.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CHART_DIR="${REPO_ROOT}/charts/pqueue"
PACKAGE_DIR="${REPO_ROOT}/target/deployment-release-gate/release-dist"

BACKENDS=(postgres_native object_log_sqlite_projection)

err() { echo "deployment-release-gate: $*" >&2; }

run_cmd() {
    printf '+++'
    printf ' %q' "$@"
    printf '\n'
    "$@"
}

chart_version() {
    awk -F': *' '$1 == "version" { print $2; exit }' "${CHART_DIR}/Chart.yaml"
}

validate_docs_microsite() {
    echo "+++ validate docs/microsite"
    python3 - <<'PY'
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlparse
import sys

root = Path.cwd()
required_docs = [
    Path("docs/helix/04-build/DEPLOYMENT-READINESS.md"),
    Path("docs/deployment/helm-static-validation.md"),
    Path("docs/deployment/kind-helm-integration.md"),
    Path("docs/deployment/operator-guide.md"),
]
index = Path("docs/operator/index.html")

for path in required_docs + [index]:
    if not (root / path).is_file():
        print(f"missing docs/microsite file: {path}", file=sys.stderr)
        sys.exit(1)

required_phrases = {
    Path("docs/deployment/helm-static-validation.md"): [
        "bash scripts/ci/helm-gate.sh",
        "object_log_sqlite_projection",
    ],
    Path("docs/deployment/kind-helm-integration.md"): [
        "bash scripts/ci/kind-helm-test.sh --backend postgres_native",
        "bash scripts/ci/kind-helm-test.sh --backend object_log_sqlite_projection",
    ],
}
for path, phrases in required_phrases.items():
    text = (root / path).read_text(encoding="utf-8")
    for phrase in phrases:
        if phrase not in text:
            print(f"{path} missing documented command or profile: {phrase}", file=sys.stderr)
            sys.exit(1)

class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        for name, value in attrs:
            if name == "href" and value:
                self.hrefs.append(value)

parser = LinkParser()
parser.feed((root / index).read_text(encoding="utf-8"))

checked_links = 0
for href in parser.hrefs:
    parsed = urlparse(href)
    if parsed.scheme or parsed.netloc or href.startswith(("#", "mailto:")):
        continue
    local = unquote(parsed.path)
    if not local:
        continue
    target = (root / index).parent / local
    if not target.is_file():
        print(f"{index} has broken local link: {href}", file=sys.stderr)
        sys.exit(1)
    checked_links += 1

print(f"validated {len(required_docs)} deployment docs and {checked_links} microsite local link(s)")
PY
}

kind_unavailable_reasons() {
    local reasons=()
    local tool
    for tool in docker kind kubectl helm; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            reasons+=("missing tool: ${tool}")
        fi
    done

    if command -v docker >/dev/null 2>&1; then
        if ! docker info >/dev/null 2>&1; then
            reasons+=("docker daemon not usable: docker info failed")
        fi
    fi

    if [[ "${#reasons[@]}" -gt 0 ]]; then
        printf '%s\n' "${reasons[@]}"
    fi
}

run_non_cluster_gates() {
    echo "=== deployment release gate: non-cluster checks ==="
    run_cmd bash scripts/ci/release-gate.sh
    run_cmd bash scripts/ci/helm-gate.sh

    local version
    version="$(chart_version)"
    if [[ -z "${version}" ]]; then
        err "could not read chart version from charts/pqueue/Chart.yaml"
        exit 1
    fi
    run_cmd bash scripts/release/package-helm-chart.sh \
        --version "${version}" \
        --destination "${PACKAGE_DIR}" \
        --chart-dir charts/pqueue

    validate_docs_microsite
}

run_kind_matrix() {
    echo "=== deployment release gate: kind backend matrix ==="
    local reasons
    reasons="$(kind_unavailable_reasons)"
    if [[ -n "${reasons}" ]]; then
        echo "=== deployment release gate: SKIPPED kind backend matrix ==="
        echo "skip scope: kind backend matrix only (${BACKENDS[*]})"
        echo "missing local capability:"
        while IFS= read -r reason; do
            [[ -n "${reason}" ]] && echo "  - ${reason}"
        done <<<"${reasons}"
        echo "non-cluster deployment release checks passed before this kind-only skip"
        return 0
    fi

    local backend
    for backend in "${BACKENDS[@]}"; do
        run_cmd bash scripts/ci/kind-helm-test.sh --backend "${backend}"
    done
}

main() {
    cd "${REPO_ROOT}"
    run_non_cluster_gates
    run_kind_matrix
    echo "=== deployment release gate PASSED ==="
}

main "$@"
