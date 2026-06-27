#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
    VERSION="$(cargo metadata --no-deps --format-version 1 | jq -r '.workspace_members[0] as $root | .packages[] | select(.id == $root) | .version')"
fi

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH="x86_64" ;;
    arm64|aarch64) ARCH="aarch64" ;;
    *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

TARGET_TRIPLE="${ARCH}-${OS}"
DIST_DIR="target/release-dist"
STAGE_DIR="target/release-package/pqueue-${VERSION}-${TARGET_TRIPLE}"
ARCHIVE="${DIST_DIR}/pqueue-${VERSION}-${TARGET_TRIPLE}.tar.gz"

rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR" "$DIST_DIR"

cargo +1.92.0 build --release --bin pqueue-service --bin pqueue-verify-ledger

cp "target/release/pqueue-service" "$STAGE_DIR/"
cp "target/release/pqueue-verify-ledger" "$STAGE_DIR/"
cat > "$STAGE_DIR/MANIFEST.txt" <<EOF
pqueue ${VERSION}
target=${TARGET_TRIPLE}

Binaries:
- pqueue-service: RESP service runtime and container entrypoint.
- pqueue-verify-ledger: validates pqueue verification ledger JSONL files.

Build command:
cargo +1.92.0 build --release --bin pqueue-service --bin pqueue-verify-ledger
EOF

tar -C "$(dirname "$STAGE_DIR")" -czf "$ARCHIVE" "$(basename "$STAGE_DIR")"

bash scripts/release/write-checksums.sh "$DIST_DIR"

echo "$ARCHIVE"
echo "${DIST_DIR}/SHA256SUMS"
