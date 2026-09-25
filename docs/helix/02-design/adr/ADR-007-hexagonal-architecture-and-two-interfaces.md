---
ddx:
  id: adr-hexagonal-architecture-and-two-interfaces
  depends_on:
    - adr-cqrs-log-projection-storage-model
    - adr-kernel-records-contract
    - api-fireweed-kernel-contract
  status: accepted
  review:
    self_hash: 02e04b32110f57e05ea80a7b6ce642cba655866e14302db6a8b0d1de0f62d012
    deps:
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-embedded-engine-integration-and-public-surface: e06dc6a96cdcd7293b5ba67e9c17d387cd2bd51c14daef13287bdf62a9e3951e
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      api-operator-repair-contract: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
    reviewed_at: "2026-07-20T00:01:27Z"
---

# ADR-007: Hexagonal architecture; implementations plug in at the kernel contract

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-001, ADR-003, ADR-009, ADR-024, ADR-025 | High |

## Context

- The engine ports (`LibBackend`, a 21-trait bound that includes log and snapshot traits) served as the seam for storage backends. When a PostgreSQL backend was plugged in there, the seam absorbed it: lowest-common-denominator defaults, capability flags exported as API, and dialect forks (ADR-025).
- A driving adapter that binds those ports directly, as a RESP front did, offers its own subset of the semantics. Two driving interfaces then mean two contracts.
- The external contract is API-006: eight operations on an object-safe `Store` trait, one black-box conformance suite, and no capability flags (ADR-025). The architecture has to make that contract the only place an implementation plugs in.

## Decision

1. **The seam is the kernel contract.** `fireweed-contract` defines the object-safe `Store` and `Operator` traits and everything they exchange (API-006 §1). An implementation plugs in by implementing `Store`, providing an admin handle that carries its `Operator`, and passing the conformance suite (API-006 §9). Nothing else is a plug-in point.
2. **There is one driving interface, in-process.** `fireweed::Fireweed` wraps `Arc<dyn Store>` and delegates 1:1 to the eight ops. A CLI or a service is a consumer of `Fireweed`, not a second interface. A network front, if one is built, is a pure transport over `Store` under its own contract; it adds no semantics and exposes nothing the contract does not.
3. **Each implementation is hexagonal inside.** The S3×Turso implementation (ADR-024) has an engine that defines ports for the object log, the Turso projection and the control plane, and adapters that implement them. The engine owns the claim path, validation, request-id replay, fencing (ADR-009) and lease reclaim. Only the implementation's constructor names concrete adapter types.
4. **Internal ports are never a seam.** The engine's ports belong to S3×Turso. They are not public, no other implementation is built on them, and changing them needs no contract change. A second implementation, such as an isolated PostgreSQL unit, implements `Store` directly and depends only on `fireweed-contract` (ADR-025 commits to none).
5. **Dependencies point inward.** `fireweed-contract` depends on no Fireweed crate; implementation crates depend on it; inside an implementation, adapters depend on the engine's ports. A dependency-direction test enforces this (ADR-003).

## Alternatives

| Option | Evaluation |
|--------|------------|
| Keep the engine ports as the seam for every backend | Rejected. A shared mid-level abstraction grows to fit every design plugged into it, and its capability differences leak into the API. |
| Two driving interfaces: a RESP subset and the Rust library | Rejected. Two interfaces with different semantics are two contracts to specify and test. A later transport carries `Store` unchanged. |
| An HTTP/REST interface with hand-written SDKs | Rejected. It is a third surface with no consumer that needs it now. |
| One generic `Fireweed<B>` over a backend type parameter | Rejected. The backend type leaks into every consumer's types; `Store` is object-safe, so `Arc<dyn Store>` hides it. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | One contract, one suite and one seam: an implementation is certified without sharing engine code. |
| Positive | The engine's ports can change for performance without touching consumers. |
| Positive | Domain logic lives in the engine, not in a transport crate. |
| Negative | A second implementation reimplements the whole contract; it cannot reuse the S3×Turso engine. |
| Negative | Consumers that need a network front, such as Seventh Sense (Scala), wait for a transport over `Store`. |

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-025-kernel-records-contract.md`
- `docs/helix/02-design/adr/ADR-009-engine-enforced-coordination-and-encapsulated-library-surface.md`
- `docs/helix/02-design/adr/ADR-024-s3-turso-public-cell.md`
