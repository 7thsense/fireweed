#!/usr/bin/env bash
# P17v: fail-closed validation of frozen package identity V and independent
# coordinates. Proves reservation (tag absent), synchronized sources, pre-S
# note presence, and that independent coordinates were not rewritten to V.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$REPO_ROOT"

VERSION=""
while (($#)); do
    case "$1" in
        --version)
            VERSION="${2:-}"
            shift 2
            ;;
        -h|--help)
            echo "usage: $0 --version MAJOR.MINOR.PATCH" >&2
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

if [[ -z "$VERSION" || ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "usage: $0 --version MAJOR.MINOR.PATCH" >&2
    exit 2
fi

TAG="v${VERSION}"
NOTE="docs/releases/${TAG}.md"
fail() {
    echo "verify-release-identity: $*" >&2
    exit 1
}

workspace_version="$({
    awk '
        /^\[workspace\.package\]$/ { in_workspace_package = 1; next }
        in_workspace_package && /^\[/ { exit }
        in_workspace_package && /^version[[:space:]]*=/ {
            value = $0
            sub(/^[^=]*=[[:space:]]*"/, "", value)
            sub(/".*/, "", value)
            print value
            exit
        }
    ' Cargo.toml
})"
[[ "$workspace_version" == "$VERSION" ]] ||
    fail "Cargo.toml workspace.package.version=${workspace_version} != ${VERSION}"

# Every workspace member package version in the lockfile must match V.
python3 - "$VERSION" <<'PY'
import re
import sys
from pathlib import Path

version = sys.argv[1]
text = Path("Cargo.lock").read_text(encoding="utf-8")
mismatched = []
for match in re.finditer(
    r'name = "(fireweed(?:-[a-z0-9-]+)?)"\nversion = "([^"]+)"', text
):
    name, pkg_version = match.group(1), match.group(2)
    if pkg_version != version:
        mismatched.append(f"{name}={pkg_version}")
if mismatched:
    raise SystemExit(
        "Cargo.lock fireweed* package versions drift from "
        f"{version}: {', '.join(mismatched)}"
    )
print(f"Cargo.lock: all fireweed* package versions == {version}")
PY

[[ -f "$NOTE" ]] || fail "missing pre-S note: ${NOTE}"
# Pre-S note must declare reservation policy without claiming an executed pass
# or elevating .ddx to product authority.
python3 - "$NOTE" "$VERSION" <<'PY'
import re
import sys
from pathlib import Path

note_path, version = Path(sys.argv[1]), sys.argv[2]
text = note_path.read_text(encoding="utf-8")
required = [
    re.compile(r"pre-S|pre-s|reserved identity", re.I),
    re.compile(re.escape(version)),
    re.compile(r"list-public-version-sources\.sh"),
    re.compile(r"verify-release-identity\.sh"),
    re.compile(r"P20pr|product-ready", re.I),
    re.compile(r"independent", re.I),
]
for pattern in required:
    if not pattern.search(text):
        raise SystemExit(f"{note_path}: missing required pre-S policy marker: {pattern.pattern}")

# Affirmative unexecuted-pass or authority elevation only (negated policy prose is allowed).
forbidden = [
    re.compile(r"\bars_met\s*=\s*true\b", re.I),
    re.compile(r"(?i)\b(all|every)\s+gates?\s+(passed|green)\b"),
    re.compile(r"(?i)\bthe live `\./?\.ddx` tracker is product authority\b"),
    re.compile(r"(?i)\b\.ddx/\*\* is product(?:/release)? authority\b"),
]
for pattern in forbidden:
    if pattern.search(text):
        raise SystemExit(f"{note_path}: forbidden unexecuted-pass or .ddx-authority claim: {pattern.pattern}")
print(f"pre-S note ok: {note_path}")
PY

if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null 2>&1; then
    fail "tag ${TAG} must remain absent (reserved until product-ready P20pr); found local tag"
fi
# Remote reservation check: when origin is reachable, the tag must still be absent.
if git remote get-url origin >/dev/null 2>&1; then
    if remote_tags="$(git ls-remote --tags origin "refs/tags/${TAG}" 2>/dev/null)"; then
        if [[ -n "${remote_tags}" ]]; then
            fail "tag ${TAG} must remain absent on origin (reserved until product-ready P20pr)"
        fi
    else
        echo "verify-release-identity: warning: could not query origin for ${TAG}; local absence still enforced" >&2
    fi
fi

chart_version="$(sed -n 's/^version:[[:space:]]*//p' charts/fireweed-queue/Chart.yaml | head -n 1)"
chart_app_version="$(sed -n 's/^appVersion:[[:space:]]*"\{0,1\}\([^"[:space:]]*\)"\{0,1\}[[:space:]]*$/\1/p' charts/fireweed-queue/Chart.yaml | head -n 1)"
[[ -n "$chart_version" && -n "$chart_app_version" ]] || fail "unable to read chart versions"
[[ "$chart_version" != "$VERSION" ]] ||
    fail "Helm chart version was synchronized to package V=${VERSION}; must remain independent source default"
[[ "$chart_app_version" != "$VERSION" ]] ||
    fail "Helm chart appVersion was synchronized to package V=${VERSION}; must remain independent source default"

bench_version="$({
    awk '
        /^\[package\]$/ { in_package = 1; next }
        in_package && /^\[/ { exit }
        in_package && /^version[[:space:]]*=/ {
            value = $0
            sub(/^[^=]*=[[:space:]]*"/, "", value)
            sub(/".*/, "", value)
            print value
            exit
        }
    ' crates/fireweed-bench/Cargo.toml
})"
[[ -n "$bench_version" ]] || fail "unable to read fireweed-bench version"
[[ "$bench_version" != "$VERSION" ]] ||
    fail "fireweed-bench version was synchronized to package V=${VERSION}; independent tool coordinate"
[[ "$bench_version" == "0.3.1" ]] ||
    fail "fireweed-bench version=${bench_version}; expected preserved independent tool coordinate 0.3.1"

gates_version="$(python3 -c 'import json; print(json.load(open("scripts/ci/public-release-gates.json"))["version"])')"
gates_ci_version="$(python3 -c 'import json; print(json.load(open("scripts/ci/public-release-gates-ci.json"))["version"])')"
[[ "$gates_version" != "$VERSION" && "$gates_version" != "v${VERSION}" ]] ||
    fail "public-release-gates.json /version was rewritten to package V"
[[ "$gates_ci_version" != "$VERSION" && "$gates_ci_version" != "v${VERSION}" ]] ||
    fail "public-release-gates-ci.json /version was rewritten to package V"
[[ "$gates_version" != "$gates_ci_version" ]] ||
    fail "gate-set /version identities must remain independent of each other"

# Inventory must classify each coordinate class (dynamic, not hard-coded prose only).
inventory="$(bash "${SCRIPT_DIR}/list-public-version-sources.sh" "${TAG}")"
printf '%s\n' "$inventory" | grep -Fq "treatment=release-synchronized; target=${VERSION}" ||
    fail "inventory missing synchronized workspace classification for ${VERSION}"
printf '%s\n' "$inventory" | grep -Fq "treatment=independent source defaults" ||
    fail "inventory missing independent Helm classification"
printf '%s\n' "$inventory" | grep -Fq "treatment=independent tool coordinate" ||
    fail "inventory missing independent fireweed-bench classification"
printf '%s\n' "$inventory" | grep -Fq "scripts/ci/public-release-gates.json" ||
    fail "inventory missing full gate-set identity"
printf '%s\n' "$inventory" | grep -Fq "scripts/ci/public-release-gates-ci.json" ||
    fail "inventory missing ci gate-set identity"
printf '%s\n' "$inventory" | grep -Fq "target_note=${TAG}.md" ||
    fail "inventory missing target pre-S note"
printf '%s\n' "$inventory" | grep -Eq "git tag ${TAG}: state=absent" ||
    fail "inventory must report reserved tag ${TAG} as absent"

echo "verify-release-identity: ok V=${VERSION} tag=${TAG} reserved absent; independent coordinates preserved"
