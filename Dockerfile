# syntax=docker/dockerfile:1

# Reproducible container image for the pqueue API-001 service. The image runs the
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

# `pqueue-objectlog` (object-log backend) and `pqueue-kafka` (designed P1 backend)
# link external sibling workspaces (`fjord`, `heimq`) via relative paths outside
# this build context. The runtime service binary uses neither: `pqueue-objectlog`
# is only a dev-dependency exercised by tests, and `pqueue-kafka` is not a service
# dependency at all. They are detached from the workspace so the image builds
# self-contained, without local source mounts. `pqueue-sqlite` has no external
# dependency and stays in place.
RUN sed -i '\#"crates/pqueue-objectlog",#d;\#"crates/pqueue-kafka",#d' Cargo.toml \
    && sed -i '/^pqueue-objectlog = /d' crates/pqueue-service/Cargo.toml \
    && cargo build --release --bin pqueue-service --bin pqueue-verify-ledger

# ---- runtime ----
FROM debian:bookworm-slim AS runtime

# Run as a non-root system user.
RUN useradd --system --uid 10001 --user-group --no-create-home pqueue

COPY --from=builder /build/target/release/pqueue-service /usr/local/bin/pqueue-service
COPY --from=builder /build/target/release/pqueue-verify-ledger /usr/local/bin/pqueue-verify-ledger

USER pqueue

# Default runtime configuration; override per deployment via Helm values.
ENV PQUEUE_LISTEN_ADDR=0.0.0.0:8080 \
    PQUEUE_BACKEND_PROFILE=postgres_native

# HTTP service + health probes (/healthz, /readyz) listen here.
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/pqueue-service"]
