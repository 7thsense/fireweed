---
ddx:
  id: td-sharding-and-shard-ownership
  depends_on:
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - adr-log-single-source-of-truth
    - prd
    - concerns
  status: accepted
  review:
    self_hash: b98590bc7a51f8e904052d64aaa6ab4d8a9c9729d155d17ee0823ffcf6b64a0d
    deps:
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
    reviewed_at: "2026-07-20T00:01:25Z"
---

# Technical Design: TD-003 Queue placement, ownership and fencing

**Status**: Accepted
**Contract**: API-006 | **ADR**: ADR-001, ADR-008, ADR-009, ADR-013, ADR-024, ADR-025 | **TD**: TD-010, TD-017, TD-018
**Scope**: how the S3×Turso implementation places each queue on one partition and each partition on one node, gives each partition one writer, and fences a writer that has lost ownership

## Scope

S3 object log × Turso is the one production implementation of API-006's `Store` (ADR-024, ADR-025). This design covers how it spreads queues over nodes without splitting any queue.

In scope:
- the placement model: queue to partition, and partition to node;
- single-writer ownership: the partition lease, the epoch and the fencing rule;
- the lease lifecycle, handoff and recovery;
- the in-process owner runtime (ADR-009);
- the control plane's role and its failure behavior;
- per-node bounds on ownership work, and the ownership tests.

Out of scope:
- The partition cell's storage: its log, its head document, its commit path and the measurements that gate them. TD-018 designs them.
- The Turso projection adapter and its replay (TD-010), and the Turso layout (TD-017).
- The external contract. API-006 says nothing about placement or ownership (its §8). A consumer sees a handoff only as `Busy` or `Unavailable` on some operations (API-006 E3).

## Placement

- **The queue is never split** (ADR-008). Every record, claim, gate and metadata entry of a queue lives in one partition, so atomic units (API-006 A1) and group exclusivity (C6a) need no cross-node coordination.
- **Queue to partition, by mapping.** On a queue's first `define_queue`, the control plane assigns the queue one partition. The assignment is a mapping that the control plane records, not a hash of the queue's name, and it never changes.
- **Partition to node.** The partition fixes the queue's node and where its object-log data lives. A partition moves to another node only on node failure, on a drain, or when the cluster grows, and all of its queues move with it.
- **No rebalancing.** A queue never moves between partitions. A hot partition is relieved by moving the whole partition to a node with spare capacity.
- **Tenants are independent of partitions.** One partition holds queues of many tenants, and one tenant's queues may span many partitions.
- **One writer is the ceiling of one queue.** A queue is served by its partition's one object-log committer and one Turso writer (TD-017 §12). A workload that needs more throughput uses more queues, as one queue per job type already does.

Choosing a partition for a new queue, and a node for a partition, is control-plane policy: a default policy with an admin override on `TursoAdmin` (ADR-008). TD-018 proposes the default, the least-loaded open partition, and leaves open whether a partition's node is assigned explicitly or chosen by highest-random-weight (HRW) hashing over partition ids. Placement is never visible through `Store`.

## Ownership

- **One writer per partition.** A partition has at most one owner at a time: the node it is placed on. Only the owner commits the partition's commands to the object log and applies them to the partition's Turso file, for every queue in the partition.
- **Lease and epoch.** The owner holds the partition's lease, recorded in the control plane. The lease carries an epoch that strictly increases whenever a different node takes the lease, and never repeats or decreases. The epoch lives in the partition's log head (ADR-013), so one durable value serves both the handoff and the commit fence (ADR-009).
- **The lease governs liveness; the epoch governs safety.** An expired lease lets the next owner take over. Only the epoch decides whether a commit lands, so clock error or a paused process can delay a handoff but never admit two writers.

### The fencing rule

The epoch is the one fencing authority, and the log's conditional publication enforces it. The control plane grants leases; it never decides whether a commit is accepted (ADR-013). TD-018 designs the head document and its conditional write.

1. **Fence before serving.** After it acquires epoch E+1, the new owner writes E+1 into the partition head with a conditional write before it serves any operation or commits any data. From that write on, the head's epoch is E+1, even though no E+1 data exists yet.
2. **Reject a non-current epoch.** Every commit carries the writer's epoch, and the head's conditional write fails unless that epoch equals the head's epoch. A writer at E is rejected as soon as E+1 is fenced.
3. **Carry the cached epoch.** The owner caches the epoch it acquired and fenced, and every commit carries that cached value. It never re-reads the current epoch when it commits, because a superseded owner would then read E+1 and pass (ADR-009).
4. **Check at commit.** The check is part of the commit's conditional write, so an owner that is superseded between admitting an operation and committing it is rejected, and nothing it planned takes effect.
5. **Read the head fresh.** The new owner reads the partition head after it has won the lease, and fences against that head. It never serves from head or index state that it loaded before, and a conflict on a conditional write always re-reads the head.

A rejected commit acknowledges nothing. The caller sees `Unavailable`, an unknown outcome that it resolves by resending with the same `request_id` or by `read` (API-006 E2, RQ2, RQ4; ADR-009).

**Claims survive a handoff.** Each claim's log entry carries its claim id and the hash of its token (TD-017 §3). The new owner's Turso file restores them, so a holder keeps settling, extending and releasing with the same token after a handoff, and lease expiry keeps its API-006 meaning (V4). A handoff cancels no claim.

## Lease lifecycle

| State | Meaning | Next states |
|---|---|---|
| `Unassigned` | No live lease. The node that the partition is placed on may acquire it. | `PendingFence`, on acquire |
| `PendingFence` | A node has acquired epoch E+1 and has not yet fenced it in the head. It serves nothing, and no other node may acquire. | `Assigned`, once the fence is durable; `Unassigned`, on release or expiry |
| `Assigned` | The owner has fenced its epoch and serves the partition. It renews before the lease expires. | `Assigned`, on renew; `Draining`, when the partition moves; `Unassigned`, on release or expiry |
| `Draining` | The owner admits no new operations and finishes the ones it has admitted. | `Unassigned`, on release or expiry |

Operations, each one transaction in the control plane:
- **Acquire.** It is rejected while another node holds a live lease. Otherwise it allocates E+1 and records the lease as `PendingFence`, with the caller as owner. An owner that reacquires its own lease, while no other node has taken it, keeps its epoch.
- **Confirm fence.** Only the owner at E+1, while the lease is live, moves it from `PendingFence` to `Assigned`, and only after the fence is durable in the head.
- **Renew.** It extends the lease at the same epoch and never shortens it. A renewal whose owner or epoch does not match, or whose lease has expired, fails; the node stops committing for that partition and re-reads the lease state.
- **Begin drain.** It moves a live `Assigned` lease to `Draining` and records the new target.
- **Release.** The owner at the current epoch returns the lease to `Unassigned`. The epoch is kept, so the next acquire allocates a strictly greater one.

Liveness:
- Node liveness is a heartbeat. A node whose heartbeat has expired leaves the live set, but its leases are reclaimed only when they expire.
- The lease TTL is at least the heartbeat TTL, and well above the renewal interval and the longest expected pause, so a healthy owner is not reclaimed. A short TTL costs extra handoffs, never correctness.
- Only the node that a partition is placed on acquires its lease. A node never contends for a live lease held elsewhere; moving a live partition is a drain. After a timed-out acquire or renewal, a node re-reads the lease state instead of retrying the acquire (ADR-009).

## Handoff

The sequence is the same for a planned move, a node failure and cluster growth (ADR-008):

1. **Place.** The control plane places the partition on the target node.
2. **Drain** (planned moves only). The current owner moves the lease to `Draining`, stops admitting operations for the partition's queues, lets admitted operations commit or fail, and releases the lease. A caller whose operation was not admitted gets `Busy` or `Unavailable` and retries.
3. **Release or expire.** On node failure the lease expires instead.
4. **Acquire.** The target acquires the lease at a strictly greater epoch.
5. **Read the head fresh and fence.** The target reads the partition head and fences the new epoch against it (rules 1 and 5).
6. **Hydrate.** The target brings the partition's Turso file to the log head: a tail replay from the file's apply cursor, or a rebuild from the log when the file is missing (TD-010).
7. **Serve.** The target confirms the fence, which makes the lease `Assigned`, and serves. Claims whose `until` passed during the handoff are reclaimed within Δ (API-006 V4).

A handoff that stops part-way is safe. A node that stalls or dies at any step leaves the partition unserved until its lease expires and the target acquires a greater epoch. Delivery stays at-least-once (API-006 L6): a record whose claim expires during the handoff becomes Free and may be claimed again.

Nothing is copied between nodes. The log is already in S3, and the Turso file is derived from it (ADR-013). Hydration time is the cost of a handoff: a tail replay when the target holds a current file, and a replay from genesis when it holds none. TD-018 proposes partition snapshots (its P1) to bound the second, subject to measurement.

## In-process owner runtime

The `fireweed` library is an owner runtime (ADR-009). `Fireweed` wraps `Arc<dyn Store>` (API-006 §1), and the S3×Turso `Store` routes each queue to its partition, holds the partition leases and fences commits below the `Store` trait, so no consumer path can skip any of these steps.
- A single-node deployment is the degenerate case: the node owns every partition, and its epochs do not change while it runs.
- A node commits for a partition only after it has acquired and fenced that partition's lease. Routing a call to the owning node is a transport concern, and a transport is outside API-006 (§8).
- A node that loses a lease while it is paused has its next commit rejected by rule 2, whether or not its renewal loop has run. The cached lease is advisory for liveness; the commit-time check is the safety authority.

## Control plane

- **Role.** The control plane owns placement (the queue-to-partition assignment and the partition-to-node placement), node registration and heartbeats, and the partition leases. Where the queue-to-partition assignment is stored, as create-only objects beside the log or as control-plane rows, is an open question in TD-018.
- **Low rate, no consensus.** The control plane is never on the per-command path. Nodes do not discover each other, elect a leader or run consensus; they read assignments and leases from the control plane, and the log's conditional publication enforces the fence.
- **Pluggable.** A single-node deployment runs the control plane in-process, and a multi-node deployment uses an external store (ADR-008). Any provider must uphold these invariants:

| Invariant | Requirement |
|---|---|
| Single live lease | At most one live lease per partition. Concurrent acquires linearize, and at most one succeeds against a given prior epoch. |
| Monotonic epoch | Epochs strictly increase per partition and never repeat, across acquire races and reclaims. |
| Fence before serving | An acquired epoch is binding in the head before its owner serves (rule 1). |
| Safe staleness | A stale read of a lease or an assignment may name an old owner, but acting on it can only fail at commit. It never produces two writers. |
| Fail closed | While the control plane is unreachable, an owner serves only under a lease it has confirmed live, acquisitions and renewals fail with a retryable error, and no commit proceeds on an unconfirmed epoch. |

## Security

- Lease operations are control-plane operations, and only the service principal may perform them.
- Holding a partition's lease lets a node write that partition's queues and nothing else. Every `Store` operation still acts on one queue only (API-006 ISO1), and the embedding host authorizes callers before it calls (ADR-009).
- **Zombie writer.** An owner that pauses past its lease and resumes is rejected at commit (rules 2 to 4).
- **Owner spoofing.** Acquire fails while another node holds a live lease. The epoch is the only commit credential, which is enough for service-internal nodes. An unguessable lease token alongside the epoch is a possible hardening for less trusted deployments; it is not part of this design.
- **Split brain.** Two nodes may briefly both believe they own a partition. Only the current epoch's commits land.

## Performance

- **No per-operation control-plane call.** Commits carry the cached epoch. The fence costs one conditional write per acquire.
- **Renewal is batched per node.** One renewal driver per node renews every partition lease the node holds, in bounded control-plane writes on the renewal interval, so the control-plane rate is O(owned partitions / renewal interval). A node learns its assignments in one bounded query.
- **Bounded per-node work.** Ownership state and background work are bounded per node. There is no task, loop or connection per queue, and a node that serves 1,000 or more active queues keeps every queue progressing.
- **Handoff cost** is the fence plus hydration (see Handoff).

## Testing

| Scenario | Required evidence |
|---|---|
| Stale epoch | A writer at E commits after E+1 is fenced. The commit fails and changes nothing. |
| Fence before data | E+1 is fenced and no E+1 data exists. A commit by the E writer is rejected. |
| Cached epoch at commit | An owner with cached epoch E keeps writing after a peer fences E+1. Each commit fails at commit with no effect, and the caller sees `Unavailable`. A sole owner is never fenced spuriously. |
| Stalled owner | An owner stalls past its lease and a peer takes the partition. The stalled owner's next commit is rejected whether or not its renewal loop ran. |
| Contention | Two nodes briefly both believe they own a partition. Only current-epoch commits land, and no command is lost or duplicated. |
| Placement target only | Only the node that the partition is placed on acquires its lease. The epoch does not alternate between nodes. |
| Planned handoff | Drain, release, acquire, fence, hydrate, serve. The new owner reproduces every acknowledged write of every queue in the partition, and a live claim's token keeps working after the handoff. |
| Node failure | The owner is killed. After lease expiry the target acquires a greater epoch, fences it, rebuilds the Turso file from the log, and reproduces the acknowledged state exactly. |
| Interrupted drain | The owner dies mid-drain. Expiry and the fence keep one writer, and claims that expire become Free (API-006 L6). |
| Fresh head | A target whose cached head predates a peer's commits re-reads the head, fences and serves no stale state. |
| Density | One node serves at least 1,000 active queues. Renewal stays batched, background work stays bounded, and every queue completes work while one queue is hot. |

## Risks

| Risk | Prob | Impact | Mitigation |
|---|---|---|---|
| A stale writer commits between an epoch advance and the new owner's first data | M | H | Fence before serving, and reject any non-current epoch at commit (rules 1 and 2). |
| An unavailable control plane blocks handoffs | M | M | Existing owners serve only under confirmed live leases; acquisitions fail closed; no commit uses an unconfirmed epoch. |
| A hot partition cannot shed a queue, because there is no rebalancing | M | H | Place new queues by measured load, move whole partitions, and use TD-018's P2 if measurement shows it is needed. |
| A poor placement is permanent | M | M | The admin override at creation, and placement inputs taken from measured load. |
| A handoff moves every queue of a partition at once | M | M | Bounded queues and records per partition; hydration from a current local file where possible. |
| Hydration from genesis grows with the log | H | M | TD-018's P1 (snapshot plus tail), gated on measurement. |
| A handoff serves head or index state loaded before the fence | M | H | Rule 5 and the fresh-head test. |
| One queue is taken for unbounded scale | M | M | One writer per queue is the ceiling; more throughput means more queues. |
