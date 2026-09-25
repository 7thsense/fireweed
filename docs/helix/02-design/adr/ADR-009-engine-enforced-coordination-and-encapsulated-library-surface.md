---
ddx:
  id: adr-engine-enforced-coordination-and-encapsulated-library-surface
  depends_on:
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-queue-as-shard-unit-and-projection-families
    - adr-auth-tenancy-and-storage-isolation
    - adr-kernel-records-contract
    - api-fireweed-kernel-contract
  status: accepted
  review:
    self_hash: 36c73add90f1c464172040dd7c926608f49c5a263b2bf03d9dd03103d8a5b6c2
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-embedded-engine-integration-and-public-surface: e06dc6a96cdcd7293b5ba67e9c17d387cd2bd51c14daef13287bdf62a9e3951e
      adr-hexagonal-architecture-and-two-interfaces: 02e04b32110f57e05ea80a7b6ce642cba655866e14302db6a8b0d1de0f62d012
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      td-sharding-and-shard-ownership: b98590bc7a51f8e904052d64aaa6ab4d8a9c9729d155d17ee0823ffcf6b64a0d
    reviewed_at: "2026-07-20T00:01:28Z"
---

# ADR-009: The engine enforces coordination below `Store`; the contract is the only external surface

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-002, ADR-003, ADR-007, ADR-008, ADR-013, ADR-025, TD-003, TD-018 | High |

## Context

- Ownership is per partition: one control-plane lease and one epoch per partition, and the log rejects a commit made under a stale epoch (ADR-008, ADR-013). That model is safe only if no consumer path can write around it.
- The current public surface exposes about 100 entry points, including engine ports, backend constructors, ownership calls, log positions and capability probes (ADR-025). A consumer that can reach a port or a backend type can bypass fencing and couples itself to one implementation.
- The contract is object-safe (`Arc<dyn Store>`), so a single public handle can hide the implementation completely.

## Decision

1. **Coordination lives inside the implementation, below `Store`.** Routing a queue to its partition, holding and renewing partition leases, and fencing commits by epoch are the S3×Turso engine's job. `Store` exposes no owner, epoch, redirect or placement. A fenced or superseded owner surfaces to the caller as `Unavailable`, an unknown outcome that the caller resolves by resending with its request id or by `read` (API-006 E2, RQ2, RQ4).
2. **The fence is checked at commit.** An owner captures its epoch when it acquires the partition lease and carries that value, never a re-read of the current epoch, to the log's conditional publication. One durable epoch, stored in the partition head, serves both the handoff and the append fence (TD-018). So lease loss fails closed on the data path: an owner that stalled while a peer took its partition cannot commit.
3. **Lease renewal is bounded per node.** One renewal driver per node renews every partition the node owns. After a renewal or acquire timeout the node re-reads the lease state; it never blindly retries an acquire.
4. **The external surface is the contract.** A consumer reaches Fireweed only through:
   - `fireweed-contract`: the `Store` and `Operator` traits, the DTOs, errors, `Clock`, the contract constants and the encoders (API-006 §1);
   - `fireweed`: `Fireweed` over `Arc<dyn Store>`, delegating 1:1, and one constructor per implementation that returns the `Fireweed` handle together with the implementation's admin handle.

   No engine port, backend type, log or projection position, capability flag, storage selector or alias is reachable from either crate. The implementation crates are `publish = false`. Guard tests check the publish topology from the manifests and the public API of both crates.
5. **The admin handle is separate from the data plane.** For S3×Turso it is `TursoAdmin`, which carries the `Operator`, projection control and queue placement. No `Store` value converts into it (OP1), and CI bans it in Snorri, the workload and loadgen crates and the contract suite (OP4).
6. **Authorization stays out of the library.** The library performs no authentication or authorization; the embedding host authorizes before it calls (ADR-002).
7. **Identity comes from the caller and the log.** A record's identity is the caller's key (API-006 §3). The engine mints no public item id. Internal identities (the insertion sequence, versions, claim ids) are derived from the log, so a rebuild or a handoff reproduces them (ADR-013).

## Alternatives

| Option | Evaluation |
|--------|------------|
| Coordination in each driving adapter | Rejected. Every adapter would have to repeat it, and one that forgot would be an unfenced path. |
| A generic `Fireweed<B>` with public ports behind a documented escape hatch | Rejected. The backend type leaks into consumers, and the escape hatch becomes a second surface. |
| Ownership, epochs and redirects on the public surface | Rejected. They are deployment plumbing; the contract states semantics only (API-006 §8). |
| Documenting the boundary as a convention | Rejected. A boundary that is not structural erodes; the publish topology and guard tests make it structural. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | No consumer path can skip the epoch fence. |
| Positive | Consumers depend on the contract, never on an implementation's types, so implementations can change internally without breaking them. |
| Positive | Operator force-clear and projection control cannot leak into data paths. |
| Negative | A fenced owner looks like an ordinary `Unavailable` to the caller, who must resolve it by request id or `read`. |
| Negative | A deliberate embedder can still path-depend on an unpublished crate; the guard tests catch it only inside this workspace. |

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-007-hexagonal-architecture-and-two-interfaces.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/02-design/technical-designs/TD-003-sharding-and-shard-ownership.md`
- `docs/helix/02-design/technical-designs/TD-018-partitioned-object-log.md`
