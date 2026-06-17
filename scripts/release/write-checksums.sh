#!/usr/bin/env bash
set -euo pipefail

DIST_DIR="${1:-target/release-dist}"
CHECKSUM_FILE="SHA256SUMS"

if [[ ! -d "$DIST_DIR" ]]; then
    echo "release distribution directory does not exist: $DIST_DIR" >&2
    exit 1
fi

cd "$DIST_DIR"

files=()
for artifact in *; do
    [[ -f "$artifact" ]] || continue
    [[ "$artifact" == "$CHECKSUM_FILE" ]] && continue
    files+=("$artifact")
done

if [[ "${#files[@]}" -eq 0 ]]; then
    echo "no release artifacts found in $DIST_DIR" >&2
    exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${files[@]}" > "$CHECKSUM_FILE"
else
    shasum -a 256 "${files[@]}" > "$CHECKSUM_FILE"
fi
