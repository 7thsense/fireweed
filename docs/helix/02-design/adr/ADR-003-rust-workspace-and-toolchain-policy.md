---
ddx:
  id: adr-rust-workspace-and-toolchain-policy
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
    - td-storage-architecture-backend-contracts
  review:
    self_hash: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
    deps:
      api-native-client-interface: 852a753af558d8b8a21e4a86e87915b14c030fefcb4a27473bcbb08cfe044580
      concerns: 7e3b81e376f75f71691f55ac1ca4d9599eddcfe6eefe70f614c366c132e07992
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
      td-storage-architecture-backend-contracts: 430d0dc1f83fa62aeb19948efd2a84f5c31df7d15195e51c8296c93c711919f5
    reviewed_at: "2026-07-06T14:59:49Z"
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

The workspace crates (as amended by ADR-007's hexagonal cutover, ADR-009's
encapsulated surface, and ADR-012's composition; the original table named
`pqueue-storage`/`pqueue-service`/`pqueue-client`, all since dissolved or
deleted) are:

| Crate | Purpose |
|-------|---------|
| `pqueue-core` | API-001 domain types, validation, priority encoding, lifecycle state transitions, idempotency semantics, and errors. |
| `pqueue-engine` | Ports, command envelopes/positions, ownership + fencing, and the generic `ComposedBackend` orchestration (ADR-012). |
| `pqueue-projection` | Shared in-memory projection state machine (`ProjectionData`). |
| `pqueue-conformance` | Backend-parameterized conformance harness (the behavioral contract). |
| `pqueue-memory` / `pqueue-sqlite` / `pqueue-postgres` / `pqueue-objectlog` | Driven adapters per backend substrate. |
| `pqueue-resp` | RESP wire driving adapter (TD-006). |
| `pqueue` | The library facade — the only published crate (ADR-009). |
| `pqueue-server` | Composition root binary (DI, ReclaimDriver ticker, ownership renewal, health probe). |

The crate graph must flow inward (adapters → projection → engine → core;
enforced by a dependency-direction test):

```text
pqueue-server  -> all adapters, pqueue, pqueue-engine, pqueue-core
pqueue / pqueue-resp -> pqueue-engine, pqueue-core (+ feature-gated adapters for pqueue)
adapters (memory/sqlite/postgres/objectlog) -> pqueue-projection, pqueue-engine, pqueue-core
pqueue-projection -> pqueue-engine, pqueue-core
pqueue-engine   -> pqueue-core
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
- Per-queue background work (lease-expiry sweeps, progress-bound aggregation,
  summary recompute, recurring rearm, idempotency/retention GC) MUST be
  multiplexed onto bounded, shared per-node resources (worker pools, connection
  pools, batched sweepers), never one task, loop, or connection per queue. This is
  a hard requirement for the
  PRD queue-density target (>=1000 concurrently active queues per node): adding
  the 1000th active queue must cost only bounded incremental resource. Per-shard
  projection state (e.g. SQLite databases) must be opened lazily and bounded by
  an LRU or equivalent cap rather than held open per shard indefinitely.

## Testing Policy

The first implementation must include:

- Unit tests for `pqueue-core` validation, priority encoding, lifecycle
  transitions, idempotency, and retry rules.
- Shared conformance tests in `pqueue-conformance`.
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

Accepted for initial implementation planning; crate layout amended 2026-07-05 to
record the realized post-ADR-007/ADR-012 workspace.
