---
ddx:
  id: adr-rust-workspace-and-toolchain-policy
  depends_on:
    - prd
    - concerns
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
  status: accepted
  review:
    self_hash: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
    deps:
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
    reviewed_at: "2026-07-20T00:01:25Z"
---

# ADR-003: Rust workspace and toolchain policy

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-007, ADR-009, ADR-011, ADR-015, ADR-025 | High |

## Context

Fireweed is a reliability-sensitive queue engine with durable storage, bounded memory and concurrency-heavy claim and lease paths. Its external interface is a contract crate that any implementation must satisfy (ADR-025), so the workspace layout has to keep that contract free of implementation code, and the toolchain and verification rules have to hold for every crate.

## Decision

Fireweed is a Rust Cargo workspace on the latest stable Rust toolchain, pinned in `rust-toolchain.toml`, with edition 2024. The workspace `rust-version` records the MSRV, and a published crate's release notes repeat it.

### Crate layers

| Layer | Crates | Rule |
|-------|--------|------|
| Contract | `fireweed-contract` | I/O-free. It holds the `Store` and `Operator` traits, the DTOs, errors, `Clock`, the contract constants, validation, `fingerprint`, the canonical encoder and the ESF index-key encoder (API-006 §1). It depends on no other Fireweed crate. |
| Conformance | `fireweed-contract-suite` | The black-box suite (API-006 §9). It depends only on `fireweed-contract`. |
| Public facade | `fireweed` | `Fireweed` over `Arc<dyn Store>`, and one constructor per implementation that returns the `Fireweed` handle and the implementation's admin handle (ADR-009). |
| Implementation | the S3×Turso crates: `fireweed-engine`, `fireweed-objectlog`, `fireweed-relational`, `fireweed-turso` and the control-plane store | They implement `Store` and depend on `fireweed-contract`, never on `fireweed` or the suite. Inside the implementation, adapters depend on the engine's ports and never the reverse (ADR-007). None is published. |
| Binaries and harnesses | `fireweed-server`, the workload, loadgen and bench crates | Consumers of the layers above. The workload and loadgen crates use only `Store` (OP4). |

A dependency-direction test fails the build on any edge that breaks these rules. A second implementation depends only on `fireweed-contract` (ADR-007).

### Toolchain gates

- Formatting: `cargo fmt --all --check`.
- Lints: `cargo clippy --workspace --all-targets -- -D warnings`.
- Tests: `cargo test --workspace`.
- Dependency audit: `cargo deny check`.
- Unused dependencies: `cargo machete`.
- Unsafe code: `#![forbid(unsafe_code)]` in every crate. An exception needs an ADR or a TD section that states its safety invariants, tests and owner.

### Runtime and libraries

- Async runtime: Tokio, in the implementation crates and binaries only. `fireweed-contract` and the engine's ports are runtime-neutral (ADR-015).
- Serialization: Serde for internal command and document encoding. Byte limits and request fingerprints use the canonical encoder in `fireweed-contract`, never an implementation's storage size (API-006 LIM1).
- Index keys: `axon-esf`, git-pinned to an axon release tag, never a path dependency (ADR-011).
- Tracing: `tracing`, with OpenTelemetry-compatible layers.

### Errors and resources

- Every crate boundary uses typed errors. At the `Store` boundary the only errors are API-006's `Error` and per-unit `Rejection`; no backend-shaped error crosses it.
- Data paths use no unbounded channel, unbounded task spawning or unbounded in-memory buffer. Every size a caller controls is bounded by the contract constants (API-006 §6.8).
- Storage calls and background workers are cancellation-safe (ADR-015, ADR-017).
- Per-queue background work runs on bounded, shared per-node pools, never one task, loop or connection per queue (ADR-002). A node opens the Turso file of a partition it owns lazily, and the number of open files is bounded.

### Testing policy

- Unit tests in `fireweed-contract` for validation, ordering, `fingerprint`, the canonical encoder and ESF round-trips.
- The contract suite against every implementation, each scenario run directly and through the torn-read adversary (API-006 CS1–CS3), plus the separate operator suite (CS5).
- Concurrency stress tests for claim exclusivity, group exclusivity and stale tokens.
- Loom or an equivalent model test for any custom concurrent data structure.
- Performance gates for the cost rules the suite does not check: C9, the S2 cost bound and F4's projection read rule, at 10M records in one queue and at 100 queues × 1M records in one Turso file (TP-003).

No bead closes on formatting or unit tests alone when its acceptance criteria touch storage, concurrency or contract semantics.

## Consequences

| Type | Impact |
|------|--------|
| Positive | The contract compiles and tests without any implementation, so a second implementation needs no engine code. |
| Positive | Service, transport and deployment concerns cannot leak into the contract crate. |
| Positive | The toolchain gates are ordinary Rust tools and run the same way locally and in CI. |
| Negative | More crates and harnesses raise setup cost. |
| Negative | Forbidding unsafe code rules out some specialized lock-free structures until a design justifies them. |

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-007-hexagonal-architecture-and-two-interfaces.md`
- `docs/helix/02-design/adr/ADR-009-engine-enforced-coordination-and-encapsulated-library-surface.md`
- `docs/helix/02-design/adr/ADR-015-full-async-storage-boundaries.md`
