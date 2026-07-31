# syntax=docker/dockerfile:1

# Reproducible container image for the Fireweed API-001 service. The image runs the
# production service binary directly and does not rely on local source mounts.
#
# Toolchain is pinned to the ADR-003 / workspace `rust-version` (1.92). See
# docs/deployment/container-runtime-contract.md for the runtime config contract.

# ---- builder ----
FROM rust:1.92-bookworm AS builder

WORKDIR /build

# Copy the full workspace. The build context is trimmed by .dockerignore so
# target/, VCS, and execution evidence stay out.
COPY . .

# Optional extra cargo features. The default service build already includes the full public
# log×projection matrix (including postgres) via fireweed-server defaults — cell selection is
# runtime env only. Pass `tls` for Lakebase / cloud-postgres native-tls:
#   docker build --build-arg CARGO_FEATURES=tls -t fireweed-service:tls .
ARG CARGO_FEATURES=""

RUN cargo build --release -p fireweed-release --bin fireweed-verify-ledger \
 && cargo build --release -p fireweed-server --bin fireweed-service \
        ${CARGO_FEATURES:+--features "$CARGO_FEATURES"}

# ---- runtime ----
FROM debian:bookworm-slim AS runtime

# Runtime libraries: `ca-certificates` + `libssl3` are required when the service is built `--features tls`
# (the native-tls / OpenSSL connector dynamically links libssl and verifies the Lakebase / cloud-postgres
# server certificate against the system trust store). They are harmless for the default (no-tls) image.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3 \
 && rm -rf /var/lib/apt/lists/*

# Run as a non-root system user.
RUN useradd --system --uid 10001 --user-group --no-create-home fireweed

COPY --from=builder /build/target/release/fireweed-service /usr/local/bin/fireweed-service
COPY --from=builder /build/target/release/fireweed-verify-ledger /usr/local/bin/fireweed-verify-ledger

USER fireweed

# Default runtime configuration (public axes); override per deployment via Helm values.
ENV FIREWEED_LISTEN_ADDR=0.0.0.0:8080 \
    FIREWEED_LOG_BACKEND=filesystem \
    FIREWEED_PROJECTION_BACKEND=memory

# RESP service listens here.
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/fireweed-service"]
