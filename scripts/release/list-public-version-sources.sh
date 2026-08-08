#!/usr/bin/env bash
set -euo pipefail

# ADR-020 fixes the first Fireweed-branded release at v0.20.0. This command
# inventories public version coordinates for the requested release target;
# it does not rewrite compatibility names or historical release notes.
#
# Coordinate treatments (P17v):
#   release-synchronized  — must equal the requested package SemVer V
#   independent source defaults — Helm chart source version/appVersion
#   independent tool coordinate — unpublished fireweed-bench package version
#   independent gate-set identity — P13a /version fields (not package SemVer)
#   reserved tag — vV must remain absent until product-ready P20pr
#   historical notes immutable — prior docs/releases/v*.md; target note is pre-S
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$REPO_ROOT"

target_release="${1:-v0.30.1}"
if [[ ! "$target_release" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "usage: $0 [vMAJOR.MINOR.PATCH]" >&2
    exit 2
fi
target_version="${target_release#v}"

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

chart_name="$(sed -n 's/^name:[[:space:]]*//p' charts/fireweed-queue/Chart.yaml | head -n 1)"
chart_version="$(sed -n 's/^version:[[:space:]]*//p' charts/fireweed-queue/Chart.yaml | head -n 1)"
chart_app_version="$(sed -n 's/^appVersion:[[:space:]]*"\{0,1\}\([^"[:space:]]*\)"\{0,1\}[[:space:]]*$/\1/p' charts/fireweed-queue/Chart.yaml | head -n 1)"

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

gates_version="$(python3 -c 'import json; print(json.load(open("scripts/ci/public-release-gates.json"))["version"])')"
gates_ci_version="$(python3 -c 'import json; print(json.load(open("scripts/ci/public-release-gates-ci.json"))["version"])')"

require_value() {
    local source="$1"
    local value="$2"
    if [[ -z "$value" ]]; then
        echo "unable to read public version source: ${source}" >&2
        exit 1
    fi
}

require_value "Cargo.toml workspace.package.version" "$workspace_version"
require_value "charts/fireweed-queue/Chart.yaml name" "$chart_name"
require_value "charts/fireweed-queue/Chart.yaml version" "$chart_version"
require_value "charts/fireweed-queue/Chart.yaml appVersion" "$chart_app_version"
require_value "crates/fireweed-bench/Cargo.toml version" "$bench_version"
require_value "scripts/ci/public-release-gates.json /version" "$gates_version"
require_value "scripts/ci/public-release-gates-ci.json /version" "$gates_ci_version"

printf 'public version sources (ADR-020 target=%s)\n' "$target_release"
printf 'Cargo.toml: workspace.package.version=%s; treatment=release-synchronized; target=%s\n' \
    "$workspace_version" "$target_version"

while IFS= read -r coordinate; do
    coordinate="${coordinate#"${coordinate%%[![:space:]]*}"}"
    coordinate="${coordinate%;}"
    printf 'README.md: %s; treatment=release-synchronized; target=%s\n' \
        "$coordinate" "$target_release"
done < <(sed -n '/^- container image /p; /ghcr\.io.*sha-<commit>/p; /^- Helm chart package /p; /^- binary archives /p' README.md)

printf 'charts/fireweed-queue/Chart.yaml: name=%s version=%s appVersion=%s; treatment=independent source defaults; packaged release overrides version and appVersion to %s\n' \
    "$chart_name" "$chart_version" "$chart_app_version" "$target_version"

printf 'scripts/release/package-helm-chart.sh: input=--version; outputs=version,app_version,release_tag,release_asset_coordinate; evidence=fireweed-queue-helm-chart.txt; treatment=release-synchronized; target=%s\n' \
    "$target_release"

printf 'crates/fireweed-bench/Cargo.toml: package.version=%s; treatment=independent tool coordinate; never synchronize to package SemVer V by pattern\n' \
    "$bench_version"

printf 'scripts/ci/public-release-gates.json: /version=%s; treatment=independent gate-set identity; owner=P13a; never rewrite to package SemVer V\n' \
    "$gates_version"

printf 'scripts/ci/public-release-gates-ci.json: /version=%s; treatment=independent gate-set identity; owner=P13a; never rewrite to package SemVer V\n' \
    "$gates_ci_version"

mapfile -t release_notes < <(find docs/releases -maxdepth 1 -type f -name 'v*.md' -printf '%f\n' | sort -V)
if [[ "${#release_notes[@]}" -eq 0 ]]; then
    echo "unable to read public version source: docs/releases" >&2
    exit 1
fi
printf 'docs/releases: notes=%s; treatment=historical notes immutable; target_note=%s.md\n' \
    "$(IFS=,; echo "${release_notes[*]}")" "$target_release"

if git rev-parse -q --verify "refs/tags/${target_release}" >/dev/null 2>&1; then
    tag_state="present"
else
    tag_state="absent"
fi
printf 'git tag %s: state=%s; treatment=reserved until product-ready P20pr; storage campaign must not create\n' \
    "$target_release" "$tag_state"
