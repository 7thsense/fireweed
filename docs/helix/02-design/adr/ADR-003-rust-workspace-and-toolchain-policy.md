---
ddx:
  id: adr-rust-workspace-and-toolchain-policy
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
    - td-storage-architecture-backend-contracts
---

# ADR-003: Rust Workspace and Toolchain Policy

## Context

pqueue is a high-performance, reliability-sensitive queue engine with durable
storage, bounded memory requirements, and concurrency-heavy claim/lease paths.
The concerns document selects Rust, but implementation needs explicit authority
for workspace boundaries, toolchain policy, async runtime, dependency posture,
unsafe usage, and verification gates.

## Decision Drivers

- Keep queue semantics testable without running the HTTP service.
- Keep storage backends pluggable without making core depend on Postgres.
- Prefer stable, common Rust tooling over novel framework choices.
- Deny unsafe code unless a later design proves it is necessary.
- Make concurrency, dependency, and performance checks part of the first
  implementation standard.

## Decision

pqueue will be implemented as a Rust Cargo workspace using the latest stable
Rust toolchain at project creation time, pinned in `rust-toolchain.toml`.

The initial workspace crates are:

| Crate | Purpose |
|-------|---------|
| `pqueue-core` | API-001 domain types, validation, priority encoding, lifecycle state transitions, idempotency semantics, and errors. |
| `pqueue-storage` | TD-001 storage traits, command envelopes, command positions, durability profiles, and backend conformance harness. |
| `pqueue-postgres` | Postgres `ControlPlaneStore`, `LogStore`, and `ProjectionStore` implementations for `postgres_native`. |
| `pqueue-service` | HTTP/JSON service binding, auth context, route handlers, telemetry, and backend wiring. |
| `pqueue-client` | Rust client facade over embedded core or HTTP transport. |

The crate graph must flow inward:

```text
pqueue-service  -> pqueue-client, pqueue-core, pqueue-storage, pqueue-postgres
pqueue-client   -> pqueue-core
pqueue-postgres -> pqueue-core, pqueue-storage
pqueue-storage  -> pqueue-core
pqueue-core     -> no pqueue crate dependencies
```

## Toolchain

- Rust edition: 2024 unless initial tooling proves a blocker; otherwise edition
  2021 with a recorded follow-up.
- MSRV: latest stable Rust at initial workspace creation. Any published crate
  release must record its MSRV in package metadata and release notes.
- Formatting: `cargo fmt --all --check`.
- Lints: `cargo clippy --workspace --all-targets -- -D warnings`.
- Tests: `cargo test --workspace`.
- Dependency audit: `cargo deny check` once dependencies exist.
- Unused dependency check: `cargo machete` once dependencies exist.
- Unsafe policy: `#![forbid(unsafe_code)]` in all initial crates.

Any exception to unsafe denial requires a later ADR or TD section with safety
invariants, tests, and code ownership.

## Async Runtime and Service Stack

- Async runtime: Tokio.
- HTTP service: Axum or another Tower-compatible HTTP stack.
- Serialization: Serde for JSON request/response and internal command payloads.
- Database access: SQLx preferred for Postgres because compile-time checked
  queries can be introduced as the schema stabilizes. If SQLx offline metadata
  becomes too heavy early, `tokio-postgres` may be used behind repository
  traits, but the choice must be recorded in the implementation plan.
- Tracing: `tracing` and OpenTelemetry-compatible layers.

The core crate must not require Tokio. Runtime-specific behavior belongs in
service or backend crates.

## Error and Resource Policy

- Use explicit typed errors at crate boundaries.
- Preserve API-001 error codes at service boundaries.
- Avoid unbounded channels, unbounded task spawning, and unbounded in-memory
  buffers in data-plane paths.
- Batch sizes, request body sizes, lease durations, and metadata/payload sizes
  must be configurable and bounded.
- Use cancellation-safe async patterns in storage calls and background workers.

## Testing Policy

The first implementation must include:

- Unit tests for `pqueue-core` validation, priority encoding, lifecycle
  transitions, idempotency, and retry rules.
- Shared conformance tests in `pqueue-storage`.
- Postgres integration tests for TD-002 scenarios.
- Concurrency stress tests for duplicate claim prevention and stale lease
  handling.
- Loom or equivalent model tests for any custom concurrent data structure.
- Benchmark harnesses for batch push, update, claim, renew, finalize, and 10M
  item projection/query fixtures.

No implementation bead should be closed on formatting or unit tests alone when
its acceptance criteria touch storage, concurrency, or API semantics.

## Consequences

Positive:

- The core queue engine remains reusable as a library.
- Backend crates can evolve independently behind conformance tests.
- Service/auth concerns do not leak into pure queue semantics.
- Tooling choices are familiar and enforceable in CI.

Negative:

- More crates and test harnesses increase setup cost.
- SQLx compile-time checks may add migration and CI friction.
- Denying unsafe may rule out some specialized lock-free structures until a
  later design explicitly justifies them.

## Status

Accepted for initial implementation planning.
