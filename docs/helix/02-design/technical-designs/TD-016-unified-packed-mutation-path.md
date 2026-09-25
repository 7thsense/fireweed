---
ddx:
  id: td-unified-packed-mutation-path
  type: technical-design
  depends_on:
    - prd
    - concerns
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - adr-log-single-source-of-truth
    - adr-async-commit-strategy-and-dispatch
    - td-object-log-turso-projection
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: concerns}
    - {kind: informed_by, to: api-fireweed-kernel-contract}
    - {kind: informed_by, to: adr-kernel-records-contract}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: td-object-log-turso-projection}
  status: accepted
---

# Technical Design: TD-016 Unified packed mutation path

**Status**: Accepted
**Contract**: API-006 | **ADR**: ADR-013, ADR-017, ADR-025 | **TD**: TD-010, TD-017

## Scope

This design covers the one ordinary write path of the S3×Turso implementation: how concurrent mutations of one queue share a sequencer, an in-process overlay, one packed log append and one set-based Turso apply.

In scope: the sequencer and its generation kinds, the overlay, log-first claim planning, the packed append and apply, the bounds, and the evidence.

Out of scope:
- the Turso schema and statement forms (TD-017);
- the projection adapter, its connections, recovery and poisoning (TD-010);
- the object log and its commit cost (TD-018);
- ownership and fencing (TD-003).

## Technical approach

**Strategy.** Every ordinary mutation inserts, rewrites or removes rows. Mutations of one queue that do the same kind of work join one generation. The generation is planned once, appended to the object log as one packed object, and applied in one Turso writer transaction. Each request keeps its own command envelope, outcome and response inside the packed object, so replay and request-id resolution stay per request (ADR-013).

**Pipeline.** This is the only ordinary write path:

```
admit(kind) → linger → start generation
  → selection fence
  → serving-reader snapshot ∪ unpublished overlay
  → realize in FIFO order (insert: allocate | rewrite: plan against overlay versions | claim: select | remove: seal)
  → release the reader slot and connection
  → packed append: one log commit for the generation
  → apply the packed vector in one Turso writer transaction, in log order
  → fold the generation into the overlay
  → return the retained responses
```

- A generation does not wait for the apply of an earlier generation. Claim selection waits for an earlier apply only when it must see rows that an earlier insert generation added.
- Empty claims append nothing.
- The writer applies the vector intact and never splits it into per-request transactions. Adjacent envelopes of the same kind may coalesce into set-based SQL. A claim followed by a settle of the same records keeps log order.

**Generation kinds** (`MutationGenerationKind`, `crates/fireweed-engine/src/claim_batch.rs`):

| Kind | Work | Row effect |
|---|---|---|
| `Push` | inserts of new keys | add rows |
| `Rewrite` | batched content or order changes of Free records; later selection waits for their apply | rewrite rows |
| `Addressed` | mutations addressed by key, planned under the queue's writer fence | rewrite or remove rows |
| `Update` | claims, and settles that take a record out of the claimable set | lease or remove rows |
| Singleton | every other mutation, including any transition that returns a leased record to Free, gate and pause changes, queue definition and lease expiry | one request per generation |

A request joins its queue's newest generation when that generation has not started, is of the same kind and has room. Otherwise the request starts a new generation, and a singleton always does. Generations of one queue run in FIFO order.

**Bounds** (`crates/fireweed-engine/src/claim_batch.rs`, `crates/fireweed/src/turso_compose.rs`):
- A generation holds at most 8 requests, 8,192 records and 32 MiB of retained response. A full generation starts at once; otherwise it lingers for 10 ms.
- A queue holds at most 4 queued generations and 16 queued requests.
- A request over the per-generation record or byte bound, or one that arrives when its queue is full, is refused at admission with a capacity error.

**Overlay.** The overlay is the in-process record of effects that are appended but not yet visible to the serving reader: unpublished keys, leased record ids and removed record ids.
- A generation plans against the reader snapshot plus the overlay, so a stale reader never re-selects a record that an unpublished generation has leased.
- A claim excludes only the ids of in-flight generations, never the history of settled ones, so its exclusion list stays bounded by the generations in flight.
- Once the apply that covers an id is visible to the serving reader, the overlay drops it.

**Log-first claims.** Under one committed reader snapshot, the generation driver selects candidates and builds each claim's full response: records, projected fields and token. It closes the snapshot before any object-log I/O, appends the claim envelopes, and retains the responses until the apply covers their positions. After the append, a response is never re-rendered from Turso and never borrows a projection connection. A packed claim apply checks that every named record moved; a mismatch poisons the apply instead of leasing a different set (TD-010).

**Grouped claims** take a separate exclusive path, which also retains its responses and never leases in SQL before the append.

**Trade-offs.**
- Gain: one code path to profile and to bound. Every phase of a workload measures this path, and none has its own protocol.
- Lose: two generations of one queue cannot plan concurrently without the overlay, and a stale serving reader must be corrected in process instead of by waiting for apply.
- Rejected: a separate protocol per operation (a claim coordinator, a settle bypass, leasing in SQL before the append).

## Design gap

How API-006's `write` and `claim` map onto these generation kinds, without terminal stages and at API-006's limits, is not yet designed.

The kinds assume that a settle takes a record out of the claimable set, so the overlay can fold its id into the removed set. API-006 has no terminal stage. A settle is a `Release(t)` with a stage move or a new rank, and the record stays Free and claimable in its new stage (API-006 W5); on this path, every transition that returns a leased record to Free is a singleton. A `write` unit also combines inserts, patches, stage moves, releases, deletes, extends, gate ops and a metadata op in one atomic unit (API-006 W2), while each kind covers one kind of row work. The design must place these at API-006's limits (1,000 record writes and 16 MiB per request, and 1,000 records and a 16 MiB response per claim) without breaking the overlay's exclusion rule or per-request replay. Until it exists, TD-017 assumes only that each packed vector applies in one transaction.

## Evidence

- MEASURED (evidence `1788659385`, `docs/perf/evidence/ss-phased/1788659385/summary.json`): `ss_phased_capacity_smoke` at N = 10,000 in batches of 100 with 8 in flight, on one hot queue, settled 14,493 records/s on ingest (P1), 18,843 and 13,367 records/s on the two update phases (P2, P3), and 17,086 records/s on claim then settle (P4), with exact final counts.
- The run used filesystem log publication and a workload shaped for the earlier API, not S3×Turso and API-006 operations. It is a regression reference for this path, not a product bar (TD-017 §13).

## Components

| Component | Responsibility | Files |
|---|---|---|
| Sequencer and classifier | Assign each request a generation kind or a singleton; hold the bounds | `crates/fireweed-engine/src/claim_batch.rs`, `crates/fireweed-engine/src/command.rs`, `crates/fireweed-engine/src/async_composed.rs` |
| Generation driver | Linger, snapshot plus overlay, realize, packed append, fold | `crates/fireweed/src/turso_compose.rs` |
| Snapshot, selection and apply | Serving-reader snapshot, claim selection with the in-flight exclusion, set-based packed apply | `crates/fireweed-turso/src/local.rs`, `crates/fireweed-turso/src/projection.rs`, `crates/fireweed-relational/src/apply.rs` |

## Security

- The overlay holds record ids and keys in process, plus responses up to the generation byte bound. It holds no other record data.
- A double claim under a stale reader is prevented by the overlay's leased ids, not by an exclusive fence.
- Tenant and queue scope are unchanged: one generation serves one queue.

## Testing

- **Classifier.** Every command maps to exactly one kind or to singleton, with no wildcard arm.
- **Overlay.** A claim excludes in-flight leased and removed ids; a removal folds its id; a duplicate key in a concurrent insert is rejected.
- **Packed apply.** A packed vector applied in one writer transaction produces the same state as applying each request alone, in log order.
- **Ordering.** A claim followed by a settle of the same records in one vector keeps log order.
- **Exactly once.** Eight concurrent insert requests are each applied exactly once.
- **Claim then settle.** A claim followed by a settle completes without a moved-row mismatch.
- **Capacity.** `ss_phased_capacity_smoke` at N = 10,000 reports settled rates and exact final counts. It is reported, not gated.

## Risks

| Risk | Prob | Impact | Mitigation |
|---|---|---|---|
| A stale serving reader re-selects an unpublished claim | H | H | The overlay's in-flight exclusion; never an exclusion list that grows with history. |
| Set-based apply reorders a claim and a settle of the same records | M | H | Coalesce only adjacent runs of one kind, and keep envelope order across kinds. |
| Settles take the singleton path until the design gap is closed | H | M | Close the design gap before measuring API-006 settle throughput on this path. |
| The overlay or the retained responses outgrow memory | L | M | The per-generation and per-queue bounds, and admission refusal beyond them. |
