---
ddx:
  id: adr-rust-workspace-and-toolchain-policy
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
    - td-storage-architecture-backend-contracts
  review:
    self_hash: 1f0c7eb647424e5ff2875cf5726f5de88b88276fabd7f203424ace231c1f6ab2
    deps:
      api-native-client-interface: 6b76e5c4c37c91d40e8d5229d9eeae516f71385aa06e856fb41a4a19ee5856e8
      concerns: 122b700fbf6049b7fa177b99efa27c5fce011775767d682458a0e2872981fb54
      prd: 382115039de93226b051a09e719c7e1c50f12563d96c1ba85ef142c0ae5d0ce0
      td-storage-architecture-backend-contracts: 5980a5612e178fc0828f567f21efaafd9d49cf7e62b2d8655bf7b9ef32e97d8d
    reviewed_at: "2026-06-20T19:01:18Z"
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
- Per-queue and per-`(queue,shard)` background work (lease-expiry sweeps,
  cross-shard progress aggregation, summary recompute, recurring rearm,
  idempotency/retention GC) MUST be multiplexed onto bounded, shared per-node
  resources (worker pools, connection pools, batched sweepers), never one task,
  loop, or connection per queue or per shard. This is a hard requirement for the
  PRD queue-density target (>=1000 concurrently active queues per node): adding
  the 1000th active queue must cost only bounded incremental resource. Per-shard
  projection state (e.g. SQLite databases) must be opened lazily and bounded by
  an LRU or equivalent cap rather than held open per shard indefinitely.

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
