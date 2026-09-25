---
ddx:
  id: adr-log-single-source-of-truth
  depends_on:
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - api-fireweed-kernel-contract
  status: accepted
  review:
    self_hash: 35052eb1b94371aa8abb8e8b348a21b459522c7d5feaba04b7146745a04bda62
    deps:
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-orthogonal-log-projection-composition: 778fdbadeadce6b52e101bda39921f88b193c5737ea96d4b8ae8e8a424a4e743
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
    reviewed_at: "2026-07-20T00:01:23Z"
---

# ADR-013: The durable command log is the single source of truth

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-001, ADR-008, ADR-016, ADR-024, TD-010, TD-017, TD-018 | High |

## Context

- ADR-001 makes the object log the durable record and the Turso projection a derived view. A projection that mutates in place with no authoritative log cannot be rebuilt, cannot be verified against anything, and ties durability to one node's disk.
- A Turso file is lost when its node is lost, and partitions move between nodes (ADR-008). A new owner must be able to rebuild exactly what the old owner served, including every version and every live token a client holds.

## Decision

1. **The object log is the single system of record for every queue.** The Turso projection is a rebuildable view derived only from committed log commands. It holds no acknowledged state that replaying the log cannot reconstruct.
2. **Everything a client can observe comes from the log.** The insertion sequence, record versions, claim ids and the hash of each claim's token (never the token) are derived from log contents, so a rebuild or a handoff reproduces them and a live token stays valid across both. File-local surrogates, such as the per-queue integer and group ids, never leave the file, and a rebuild need not reproduce them (TD-017 §1).
3. **Commit order.** A command is durable in the log before any op that reports it returns `Ok` (API-006 DUR1). The projection applies commands in log order, and it records its applied position in the same Turso transaction as the applied state, so the position never passes the state. On open, the projection replays the log after its recorded position.
4. **Rebuild goes through projection control.** Projection control on the admin handle verifies, deletes and rebuilds a partition's projection by replaying the log (TD-010). A lost Turso file is recovered the same way. A rebuilt projection serves exactly what the live one served.
5. **The log is the fence authority.** A partition's epoch lives in its log head, and the log's conditional publication rejects a commit under a stale epoch. The control plane grants leases; it never decides whether a commit is accepted (ADR-008).
6. **There is no projection-only mode.** Every production composition appends to the durable object log. Test builds may publish the log to a local filesystem and open the projection on Turso `:memory:` (ADR-024); neither is a production configuration.
7. **The projection never authorizes log retention.** Until snapshots exist, rebuild replays from genesis and the log keeps every command. A snapshot at a committed log position would become a second rebuild source only if TD-018's proposal P1 is measured and accepted.

## Alternatives

| Option | Evaluation |
|--------|------------|
| The projection as the authority, with the log as an audit trail | Rejected. The projection could not be rebuilt or verified, and losing a node's disk would lose acknowledged writes. |
| A projection-only mode for embedded or single-node use | Rejected. It would be a second durability contract, and API-006 has one (DUR1). |
| Node-local identities (item ids, claim ids) minted outside the log | Rejected. A rebuild or handoff could not reproduce them, and live tokens would break on handoff. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Losing a Turso file costs a rebuild, never data. |
| Positive | A projection can be verified at any time against a rebuild from the log. |
| Positive | Handoff between nodes needs no data copy: the new owner reads the log. |
| Negative | Cold recovery pays replay time, from genesis until snapshots are accepted. |
| Negative | Every state change pays a log append before it is acknowledged. |

## References

- `docs/helix/02-design/adr/ADR-001-cqrs-log-projection-storage-model.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
- `docs/helix/02-design/technical-designs/TD-018-partitioned-object-log.md`
