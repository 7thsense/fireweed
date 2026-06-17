# pqueue-611b6645 — Container image and runtime config contract

## Summary

Added a reproducible container image build for the pqueue service plus a narrow
runtime entrypoint (`pqueue-service`) and documented the runtime configuration
contract consumed by Helm. The image runs the production service binary directly
with no local source mounts.

Before this bead the `pqueue-service` crate was library-only: it exposed an
axum `Router` (`app_with_state`) but had no binary that bound a socket, served
the app, or exposed health probes. The only workspace binary was
`pqueue-verify-ledger`. The bead scope explicitly allows
`crates/pqueue-service/**` for "narrow runtime config/help output needed by
container deployment", so a small entrypoint was added rather than guessing.

## Changes

- `crates/pqueue-service/src/runtime.rs` (new): `RuntimeConfig` (env-driven,
  testable via `from_getter`), `BackendProfile`, `health_router`
  (`/healthz`, `/readyz`), `service_router`, and `help_text` documenting the
  config contract. Inline unit tests cover defaults, overrides, blank-value
  fallback, and rejection of invalid listen addr / unsupported backend profile.
- `crates/pqueue-service/src/bin/pqueue-service.rs` (new): the container
  entrypoint. `--help`/`-h` and `--version`/`-V` exit 0; otherwise it reads env
  config, binds `PQUEUE_LISTEN_ADDR`, serves the API-001 app + health probes,
  and shuts down gracefully on SIGINT.
- `crates/pqueue-service/src/lib.rs`: `pub mod runtime;`.
- `crates/pqueue-service/Cargo.toml`: `tokio` promoted to a runtime dependency
  (rt-multi-thread, macros, net, signal).
- `crates/pqueue-service/tests/container_runtime_contract_tests.rs` (new):
  health-probe and merged-router integration tests.
- `Dockerfile` (new): multi-stage build pinned to Rust 1.92 builder,
  `debian:bookworm-slim` runtime, non-root user, `EXPOSE 8080`, entrypoint
  `pqueue-service`. The `pqueue-objectlog` (links external `fjord`) and
  `pqueue-kafka` (links external `heimq`) crates are detached from the workspace
  in-image so the build is self-contained; neither is used by the runtime binary
  (objectlog is a dev-dependency only, kafka is not a service dependency).
- `.dockerignore` (new): trims build context (target/, VCS, `.ddx/`, tooling).
- `docs/deployment/container-runtime-contract.md` (new): entrypoint, env/config
  keys consumed today, health endpoint/port, and the reserved backend-profile
  settings Helm must supply for `postgres_native` and
  `object_log_sqlite_projection`.
- `README.md`: link to the contract doc and a container build/smoke snippet.

## Acceptance evidence

1. `docker build -t pqueue:dev .` — PASS (image
   `sha256:5d048b3e7f48…`).
2. `docker run --rm pqueue:dev --help` — exits 0 and prints the runtime config
   contract (the documented help command proving the entrypoint). The health
   endpoint was also confirmed live: a running container returned
   `GET /healthz -> 200 ok` and `GET /readyz -> 200 ready` (via container IP;
   host published-port forwarding is flaky in this sandbox, container-internal
   serving is correct).
3. `cargo +1.92.0 build --release --workspace` — PASS.
4. `docs/deployment/container-runtime-contract.md` (linked from `README.md`)
   states the image entrypoint, required environment/config keys, health
   endpoint/port (`8080`, `/healthz`, `/readyz`), and the backend-profile
   settings needed by Helm.

Tests: `cargo +1.92.0 test -p pqueue-service --lib runtime` (6 pass) and
`--test container_runtime_contract_tests` (3 pass).

## Environment note (not committed)

The workspace links sibling repos `fjord` and `heimq` via relative paths
(`../../../fjord`, `../../../heimq`). The orchestrator normally symlinks these
into `.ddx-exec-wt/`; `heimq` was present but `fjord` was missing in this
worktree, so a `fjord` symlink to the canonical checkout was created to run the
full-workspace build (AC3). This is environment provisioning only — no repo file
records it, and `Cargo.lock` churn from fjord becoming resolvable (a one-line
`object-log` git-source correction) was reverted to keep the commit scoped.
