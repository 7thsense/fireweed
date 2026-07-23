#!/usr/bin/env bash
# Install the release gate's pinned gitleaks build after verifying the official
# release archive digest. The release workflow runs on Linux x86-64.
set -euo pipefail

readonly GITLEAKS_VERSION="8.30.1"
readonly GITLEAKS_ARCHIVE="gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz"
readonly GITLEAKS_ARCHIVE_SHA256="551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
readonly GITLEAKS_RELEASE_URL="https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/${GITLEAKS_ARCHIVE}"

if (($# != 1)); then
    echo "usage: bash scripts/ci/install-gitleaks.sh <destination-directory>" >&2
    exit 64
fi

destination="$1"
temporary_directory="$(mktemp -d "${TMPDIR:-/tmp}/pqueue-gitleaks.XXXXXX")"
trap 'rm -rf "${temporary_directory}"' EXIT

curl --fail --location --proto '=https' --tlsv1.2 \
    "${GITLEAKS_RELEASE_URL}" -o "${temporary_directory}/${GITLEAKS_ARCHIVE}"
printf '%s  %s\n' \
    "${GITLEAKS_ARCHIVE_SHA256}" \
    "${temporary_directory}/${GITLEAKS_ARCHIVE}" | sha256sum --check --strict
tar -xzf "${temporary_directory}/${GITLEAKS_ARCHIVE}" \
    -C "${temporary_directory}" gitleaks
mkdir -p "${destination}"
install -m 0755 "${temporary_directory}/gitleaks" "${destination}/gitleaks"
"${destination}/gitleaks" version | grep -Fx "${GITLEAKS_VERSION}"
