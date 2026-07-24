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
STAGE_DIR="target/release-package/fireweed-${VERSION}-${TARGET_TRIPLE}"
ARCHIVE="${DIST_DIR}/fireweed-${VERSION}-${TARGET_TRIPLE}.tar.gz"

rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR" "$DIST_DIR"

# Optional cargo features for the service binary (e.g. FIREWEED_FEATURES=tls for the Lakebase /
# cloud-postgres native-tls runtime). Empty by default — the stock release ships no extra features.
FIREWEED_FEATURES="${FIREWEED_FEATURES:-}"
SERVICE_FEATURE_ARGS=()
if [[ -n "$FIREWEED_FEATURES" ]]; then
    SERVICE_FEATURE_ARGS=(-p fireweed-server --features "$FIREWEED_FEATURES")
fi

rustup run 1.92.0 cargo build --release --bin fireweed-verify-ledger
rustup run 1.92.0 cargo build --release --bin fireweed-service "${SERVICE_FEATURE_ARGS[@]}"

cp "target/release/fireweed-service" "$STAGE_DIR/"
cp "target/release/fireweed-verify-ledger" "$STAGE_DIR/"
cat > "$STAGE_DIR/MANIFEST.txt" <<EOF
Fireweed ${VERSION}
target=${TARGET_TRIPLE}

Binaries:
- fireweed-service: RESP service runtime and container entrypoint.
- fireweed-verify-ledger: validates Fireweed verification ledger JSONL files.

Build command:
rustup run 1.92.0 cargo build --release --bin fireweed-service --bin fireweed-verify-ledger
  (set FIREWEED_FEATURES=tls for the Lakebase / cloud-postgres native-tls service build)
EOF

tar -C "$(dirname "$STAGE_DIR")" -czf "$ARCHIVE" "$(basename "$STAGE_DIR")"

bash scripts/release/write-checksums.sh "$DIST_DIR"

echo "$ARCHIVE"
echo "${DIST_DIR}/SHA256SUMS"
