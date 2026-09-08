---
ddx:
  id: td-unified-packed-mutation-path
  type: technical-design
  depends_on:
    - prd
    - concerns
    - api-native-client-interface
    - adr-log-single-source-of-truth
    - adr-async-commit-strategy-and-dispatch
    - td-object-log-turso-projection
    - td-batch-shape-and-critical-section-audit
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: concerns}
    - {kind: informed_by, to: api-native-client-interface}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: td-batch-shape-and-critical-section-audit}
    - {kind: informed_by, to: ss-objectlog-turso-memory-goal}
  status: accepted
---

# Technical Design: TD-016 Unified packed add / update / delete

**Contract**: API-001 | **ADR**: ADR-013, ADR-017 | **TD**: TD-010, TD-011

## Scope

One write path for the public queue. Seventh Sense phases (ingest, enrich,
schedule, deliver) are measurements of that path, not designs.

In scope: filesystem--turso (and the same object-log × Turso composition)
ordinary item mutations: Push, UpdateFieldsBatch, item Claim, Complete, Fail,
Purge.

Out of scope: grouped/cohort Claim (exclusive selection), Retry/Release/Rearm
until overlay can represent unlease, postgres S9, public API changes, larger
generation bounds, SQL-first Claim, `SKIP LOCKED`.

## Technical Approach

**Strategy**: Every ordinary mutation is bulk add, bulk update, or bulk delete
of item rows. Those three operations share one sequencer, one overlay, one
packed append, and one set-based apply. Command envelopes stay distinct for
replay (API-001). Serving protocols do not.

**Key Decisions**:

- Two sequencer kinds, not four protocols. `Push` = add. `Update` = rewrite or
  remove live rows (field-update, claim, complete, fail, purge). Same-kind
  requests overlay FIFO in one generation (fill 8, 800 items, 4 MiB, 20 ms
  linger, two generations / sixteen requests per queue).
- Overlay replaces exclusive item-claim fence. Unpublished `client_keys`,
  `leased_ids`, and `terminal_ids` are the in-process candidate set. The next
  generation merges that overlay instead of waiting its own apply. Claim SELECT
  excludes only in-flight overlay ids, never the cumulative completed set.
- Realize after snapshot, before append. Add allocates ids/blobs. Claim SELECTs
  pending minus overlay. Field-update plans against overlay versions. Complete /
  Fail / Purge seal terminal or purge envelopes and fold `terminal_ids`. Empty
  claims append nothing.
- One packed append (`SelectionRequired`) per generation. Shared selection
  fence. Slot/connection dropped before append. Responses are retained from
  realize; no post-publication render.
- One Turso writer transaction applies the packed vector. Adjacent same-kind
  envelopes may coalesce into set-based SQL; mixed Claim-then-Complete of the
  same ids keeps log order. Apply remains log-authoritative (ADR-013).
- Grouped/cohort Claim stays exclusive. Retry/Release/Rearm stay singleton
  until overlay can put a leased id back to pending without a second protocol.

**Trade-offs**:

- Gain: one code path to profile; P1–P4 settled ≥10k items/s on this path
  (evidence `1788659385`).
- Lose: cannot overlap two Update generations' SELECTs without overlay; stale
  serving-reader rows must be excluded in-process, not by waiting coverage.
- Rejected: per-phase protocols (ClaimCoordinator, Complete Bypass
  `KeyedQueueGate`, SQL-first lease-before-append). Divergent stage rates were
  the symptom of those forks.

### Pipeline (the only ordinary write path)

```
admit(kind) → linger → start_generation
  → shared fence
  → serving-reader snapshot ∪ unpublished overlay
  → FIFO realize (add allocate | update plan | claim select | delete seal)
  → drop slot
  → packed append
  → set-based apply of that vector
  → fold overlay; remember unpublished leased ids only
  → return retained responses
```

Do not wait `last_claim` / `last_candidate_mutation` apply before return.
Wait produce catch-up only when Claim SELECT must see a prior **Add**
generation's pending rows.

### Mapping

| Public op | Kind | Realize | Live-set effect |
|---|---|---|---|
| Push | Add | identity overlay + allocate | insert pending |
| UpdateFieldsBatch | Update | plan against overlay versions | rewrite fields/schedule |
| Claim (item) | Update | SELECT pending − overlay leased/terminal | pending → leased |
| Complete / Fail | Update | seal terminal envelopes | leased → terminal (delete from live set) |
| Purge | Update | seal purge envelopes | remove rows |
| Retry / Release / Rearm | Singleton | existing planner | leased → pending |
| Grouped / cohort Claim | Exclusive | existing retained path | pending → leased |

Seventh Sense P1 is Add. P2/P3 are Update (fields/schedule). P4 is Update
(claim) then Update (complete). If a phase is slow, instrument this pipeline's
snapshot / realize / append / apply stages. Do not add a lane.

### Overlay exclude bound

`select_item_claim_ids_on` bind count for `NOT IN` is at most in-flight overlay
ids: `GENERATION_MAX_ITEMS * MUTATION_MAX_GENERATIONS_PER_QUEUE` (800 × 2).
Completed ids drop from `leased_ids` into `terminal_ids` for overlay correctness
but must not accumulate into SQL exclude after apply has made them non-Pending.
`remember_leases` tracks unpublished claims only; Complete may forget after the
id is in `terminal_ids` **and** either apply has covered or SELECT uses
`lifecycle_state='Pending'` plus in-flight leased ids.

## Component Changes

### Modified: mutation sequencer and classifier

- **Current**: `MutationGenerationKind::{Push, Update}`; Claim and Complete
  already classify as `Compatible(Update)` in `claim_batch.rs` / `command.rs`.
- **Changes**: Keep two kinds. No ClaimCoordinator for item Claim. No Bypass
  Complete. Classifier tests name Add vs Update, not SS phases.
- **Files**: `crates/fireweed-engine/src/claim_batch.rs`,
  `crates/fireweed-engine/src/command.rs`,
  `crates/fireweed-engine/src/async_composed.rs`

### Modified: derived Turso compose driver

- **Current**: Push and BatchUpdate drive `drive_candidate_mutation`. Item Claim
  and Complete were forked; working tree admits them as Update work.
- **Changes**: `MutationGenerationWork::{Push, BatchUpdate, Claim, Finalize,
  Singleton}` only. `drive_started_generation` is the single driver. Overlay
  merge is union, not clobber. Claim realize uses serving reader.
- **Files**: `crates/fireweed/src/turso_compose.rs`

### Modified: Turso snapshot, SELECT, apply

- **Current**: serving-reader mutation snapshot; Claim SELECT `NOT IN` can grow
  with remembered history; apply still tends to per-envelope or broad-row
  rewrite.
- **Changes**: snapshot carries `leased_ids` / `terminal_ids`. SELECT exclude is
  in-flight only. Packed apply is set-based per adjacent kind run in log order.
- **Files**: `crates/fireweed-turso/src/local.rs`,
  `crates/fireweed-turso/src/projection.rs`,
  `crates/fireweed-relational/src/apply.rs`

## API/Interface Design

No public surface change. API-001 batch shapes stay.

| Surface | Governing Contract | Usage |
|---|---|---|
| Push / BatchUpdate / Claim / Finalize / Purge | API-001 | Same requests; composition packs compatible calls |
| Log envelope | ADR-013 | One command per accepted public request in the packed object |
| Commit strategy | ADR-017 | `SeparateReplayCommit` + `SelectionRequired` for this path |

## Data Model Changes

None. Overlay is process-local. `fireweed_items` lifecycle_state remains
Pending / Leased / Complete / Failed. No reservation table, no claim outbox
writes on the live path.

## Integration Points

| From | To | Method | Data |
|---|---|---|---|
| Facade | Sequencer | admit | generation kind + work |
| Driver | Serving reader | snapshot + claim SELECT | identities, pending rows |
| Driver | Object log | packed_append | command vector |
| Apply | Turso writer | one IMMEDIATE | same vector in log order |

### External Dependencies

- **Turso 0.7 ordinary WAL**: single writer. Fallback: none; this cell is the
  product default (TD-010).

## Security

- **Authentication / authorization**: unchanged facade/tenant scope (ADR-002).
- **Data protection**: overlay holds item ids and client keys in process, not
  payloads beyond generation response-byte cap (4 MiB).
- **Threats**: double-claim under stale reader — mitigated by overlay leased
  ids, not by exclusive fence. Authority-first Claim apply still requires
  moved-row count before bearer side effects.

## Performance

- **Expected load**: N=10k public batches of 100, inflight 8, one hot queue.
- **Response target**: each of P1/P2/P3/P4 settled items/s ≥ 10,000 at N=10k
  on filesystem--turso; T3 exact (`pending=0`, `leased=0`, `complete=10000`
  after P4). Ack-only rates are not the gate.
- **Pinned evidence**: `docs/perf/evidence/ss-phased/1788659385/summary.json`
  — P1 14493, P2 18843, P3 13367, P4 settled 17086 (T3 exact). Fused Complete
  keeps `rowid BETWEEN` only when the named ids occupy that slice; otherwise a
  PK VALUES join. Overlay prune drops applied generations so Claim exclude
  stays near in-flight (measured plateau 3200 at N=10k, not historical N).
  Goal-doc N=100k T1/T2 remain after this N=10k floor.
- **Optimizations**: pack compatible requests; set-based apply; O(in-flight)
  claim exclude; no apply-wait on the return path.

## Testing

- **Unit**: classifier exhaustiveness (Push=Add, Claim/Complete/Fail/Purge=
  Update); overlay FIFO (claim exclude, complete folds terminal, push key
  conflict); SELECT bind cap.
- **Integration**: `grouped_and_item_claim_use_retained_carrier_without_sql_first_lease`;
  eight inflight pushes exactly-once; item claim then complete without
  `moved 0 of N` poison.
- **Contract**: API-001 per-request outcomes and `request_id` replay unchanged.
- **Performance**: `ss_phased_capacity_smoke` N=10k filesystem--turso, settled
  rates and T3 exact.
- **Security**: authority-first moved-row poison still fails the packed Claim
  apply when named rows did not move.

## Migration & Rollback

- **Backward Compatibility**: log envelopes unchanged; replay of mixed
  UpdateFields / Claim / Finalize histories remains valid.
- **Data Migration**: none. Legacy claim outbox stays recovery-only.
- **Feature Toggle**: none. Derived object-log × Turso is the live path.
- **Rollback**: revert the compose driver to the previous generation; do not
  reintroduce SQL-first serving.

## Implementation Sequence

1. Freeze two-kind dispatch and overlay — Files:
   `claim_batch.rs`, `command.rs`, `async_composed.rs`, `turso_compose.rs` —
   Tests: classifier + overlay unit + compose source audits.
2. Bound Claim SELECT exclude and overlay lifetime — Files:
   `turso_compose.rs`, `projection.rs`, `local.rs` — Tests: bind-cap audit +
   item claim integration (no poison).
3. Set-based packed apply for Add and Update vectors — Files: `apply.rs`,
   `projection.rs` — Tests: packed Claim/Complete/UpdateFields apply matches
   solo model in one writer transaction.
4. Qualify N=10k all-phase ≥10k settled items/s — Files: evidence +
   `ss-objectlog-turso-memory-goal.md` — Tests: `ss_phased_capacity_smoke`.

**Prerequisites**: TD-010 Turso projection; ADR-013 log authority; working-tree
classifier already treats Claim/Complete as Update.

## Risks

| Risk | Prob | Impact | Mitigation |
|---|---|---|---|
| Stale reader re-selects unpublished claims | H | Double-claim / moved-0 poison | In-flight overlay exclude; never O(N) history NOT IN |
| Set-based apply reorders Claim then Complete of same ids | M | Wrong lifecycle | Adjacent-run coalesce only; preserve envelope order across kinds |
| Schedule-index UPDATE stays O(seconds) | M | P3 < 10k/s | Profile this path's apply stage; do not fork a schedule protocol |
| Uncommitted compose landing | L | Beads describe a path not on HEAD | Slice 1 lands dispatch+overlay before apply work |

## Review Checklist

- [x] Ordinary writes are add, update, or delete on one path
- [x] SS phases are measurements, not protocols
- [x] Key decisions have rationale
- [x] Trade-offs explicit
- [x] Files named
- [x] API-001 referenced, not redefined
- [x] No schema migration
- [x] Numeric 10k/s target and evidence pin
- [x] Tests and rollback named
- [x] Implementation sequence is the bead DAG
