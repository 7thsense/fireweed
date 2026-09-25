---
ddx:
  id: adr-queue-as-shard-unit-and-projection-families
  depends_on:
    - prd
    - adr-cqrs-log-projection-storage-model
    - adr-kernel-records-contract
  status: accepted
  review:
    self_hash: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
    deps:
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
    reviewed_at: "2026-07-20T00:01:20Z"
---

# ADR-008: The queue is the unit of atomicity; each queue is assigned one partition

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | PRD, API-006, ADR-001, ADR-013, ADR-025, TD-003, TD-017, TD-018 | High for the placement model; the partition cell's commit path is measurement-gated (TD-018) |

## Context

- A queue split into shards needs every claim to probe each shard and merge the candidates. That costs a scatter-gather per claim, stalls on the slowest shard, and weakens rank order exactly where scale matters. The owner's rule: "Queue is the unit of sharding. If you want more sharding, create more queues."
- Queues are cheap and one per job type (ADR-025 §2), so a deployment has many of them. With one log index document per queue, commit cost grows with the number of active queues, and every commit rewrites a document that grows without bound (TD-018, Findings 4 and 5).
- Placement is not part of the contract: `define_queue` has no placement input, and API-006 §8 excludes storage layout and partition placement.

## Decision

1. **The queue is the unit of atomicity.** Every op addresses one queue, every atomic unit stays inside it, and nothing is atomic across queues (API-006 A1–A4, ISO1). Claim order and progress are defined per queue and per stage (C4, C5a) and are computed on the queue's one owner; nothing is merged across owners.
2. **A queue is never split.** There are no partitions or shards inside a queue. A producer that needs more than one queue's throughput spreads its work over several queues.
3. **The control plane assigns each queue one partition, by mapping.** The assignment is made when the queue is first defined and never changes. It fixes the queue's node and its object-log residency. It is a control-plane decision (a default policy with an admin override on `TursoAdmin`), not a hash of the queue's name, and it is invisible through `Store`.
4. **Tenants are independent of partitions.** A partition holds queues of many tenants, and one tenant's queues may span many partitions.
5. **Many queues share a partition.** A partition has one log, one head document and one Turso projection file (TD-017 §1). One head update publishes every queue a commit carries, instead of one index update per queue, and an idle queue costs a directory entry and a few rows, with no per-queue document on the commit path.
6. **Queues are never rebalanced.** A queue never moves between partitions. A hot partition is relieved by moving the whole partition to another node. A single queue's ceiling is one partition's committer and one Turso writer.
7. **Ownership is per partition: one writer, fenced by an epoch.** Each partition has one control-plane lease. Its owner's epoch lives in the partition's head, and the log rejects a commit made under a stale epoch (ADR-013). A handoff follows TD-003's sequence per partition: drain, release or expire the old lease, acquire the next epoch, read the head fresh, fence, hydrate the Turso file, then serve. How partitions are assigned to nodes is the control plane's choice.
8. **The control plane is pluggable.** A single-node deployment runs it in-process; a multi-node deployment uses an external store (ADR-001). It holds assignments and leases and is never on the per-command path.
9. **Measure first.** TD-018 designs the partition cell. Its commit path is built only after TD-018's commit-cost measurements, starting with today's per-queue commit cost on MinIO at 1, 10, 100 and 1,000 queues. Compaction, snapshots and truncation, splitting a log by partition range, and full-delete garbage collection are proposals that need their own measurement and owner decision.

## Alternatives

| Option | Evaluation |
|--------|------------|
| Shards inside a queue, with fan-out claims | Rejected. Scatter-gather per claim, stalls, and weaker rank order, for single-queue scale that more queues already provide. |
| Hashed placement (HRW over queue names) | Rejected. The control plane could not pack or separate queues by measured load, and adding partitions would move queues. |
| One log index document per queue | Rejected. Commit cost grows with active queues, and the documents grow without bound (TD-018). |
| Rebalancing queues between partitions | Rejected. A move would copy a queue's log and projection state and re-fence it; whole-partition moves and placement at creation cover the need. |
| A placement input on `define_queue` | Rejected. Placement is deployment control on the admin handle, not a contract matter (API-006 §7, §8). |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Claims are single-hop index seeks on one owner, with no merge across owners. |
| Positive | Commit cost follows commits per partition, not active queues, so queues stay cheap. |
| Positive | One lease and one epoch cover every queue of a partition. |
| Negative | A single queue cannot exceed one partition's committer and one Turso writer. |
| Negative | A poor placement is permanent for that queue; a hot partition moves as a whole or not at all. |
| Negative | A handoff moves every queue of the partition at once. |

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-025-kernel-records-contract.md`
- `docs/helix/02-design/technical-designs/TD-003-sharding-and-shard-ownership.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
- `docs/helix/02-design/technical-designs/TD-018-partitioned-object-log.md`
