#!/usr/bin/env bash
# Disposable TLS-postgres harness for the `postgres_tls_connection_succeeds` proof.
#
# Spins up a self-signed `postgres:16 -c ssl=on` in docker, points the env-gated live TLS test at it
# (`FIREWEED_PG_TLS_TEST_URL` with sslmode=require), runs the test under `--features tls`, and tears the
# container + image down on exit. This is the reproducible companion to
# crates/fireweed-postgres/tests/tls_connection.rs — it proves the native-tls transport end-to-end without a
# managed cloud database.
#
# Usage: bash scripts/dev/pg-tls-smoke.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CONTAINER="fireweed-pgtls-smoke"
IMAGE="fireweed-pgtls-smoke:local"
HOST_PORT="${FIREWEED_PG_TLS_PORT:-55433}"
WORKDIR="$(mktemp -d)"

CARGO="${CARGO:-cargo}"

cleanup() {
    docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
    docker image rm -f "${IMAGE}" >/dev/null 2>&1 || true
    rm -rf "${WORKDIR}"
}
trap cleanup EXIT

echo "=== generating self-signed server certificate ==="
openssl req -new -x509 -days 2 -nodes -text \
    -subj "/CN=localhost" \
    -out "${WORKDIR}/server.crt" -keyout "${WORKDIR}/server.key" >/dev/null 2>&1
chmod 644 "${WORKDIR}/server.crt"
chmod 600 "${WORKDIR}/server.key"

echo "=== building TLS postgres image (cert/key owned by the db user) ==="
cat >"${WORKDIR}/Dockerfile" <<'EOF'
FROM postgres:16
COPY server.crt /etc/pg/server.crt
COPY server.key /etc/pg/server.key
RUN chown postgres:postgres /etc/pg/server.crt /etc/pg/server.key \
 && chmod 600 /etc/pg/server.key && chmod 644 /etc/pg/server.crt
EOF
docker build -t "${IMAGE}" "${WORKDIR}" >/dev/null

echo "=== starting ${CONTAINER} on 127.0.0.1:${HOST_PORT} (ssl=on) ==="
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER}" \
    -e POSTGRES_USER=fireweed -e POSTGRES_PASSWORD=fireweed -e POSTGRES_DB=fireweed \
    -p "127.0.0.1:${HOST_PORT}:5432" \
    "${IMAGE}" \
    -c ssl=on -c ssl_cert_file=/etc/pg/server.crt -c ssl_key_file=/etc/pg/server.key >/dev/null

echo "=== waiting for the database to accept connections ==="
for _ in {1..30}; do
    if docker exec "${CONTAINER}" pg_isready -U fireweed -d fireweed >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

export FIREWEED_PG_TLS_TEST_URL="postgres://fireweed:fireweed@127.0.0.1:${HOST_PORT}/fireweed?sslmode=require"
echo "=== running the live TLS round-trip proof ==="
echo "    FIREWEED_PG_TLS_TEST_URL=${FIREWEED_PG_TLS_TEST_URL}"
( cd "${REPO_ROOT}" && "${CARGO}" test -p fireweed-postgres --features tls --test tls_connection -- --nocapture )

echo "=== TLS smoke PASSED ==="
