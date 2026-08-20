---
ddx:
  id: build-later-phase-speed
  type: implementation-plan
  links:
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: adr-cqrs-log-projection-storage-model}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: build-one-projection-cleanup}
  review:
    round: 1
    claude: BLOCK
    codex: BLOCK
    folded: docs/helix/04-build/reviews/later-phase-speed-round1/
    disposition: "claim is take-next-batch; live apply of already-leased Claim must not re-lease"
---

# Claim is take-next-batch

Deliver is `BatchClaim` + `complete`. Claim is not a second protocol. It is: next N due items, exclusive to this worker, bodies included. Then record that on the log.

`one-projection-cleanup.md` already specified this (Class S). The live path does not follow it.

Latest `1787259713`, `filesystem--turso`, N=10k: ingest 31692/s, enrich 33465, schedule 51361, deliver **382** (claim p50 256 ms).

Target: deliver ≥ ingest. Payloads and groups stay. `apply_start_delay_ms.max(300)` stays (produce only). No planner map, `SKIP LOCKED`, or reservation table. Default `open()` stays Strict. Sqlite-log Class A stays one sqlite txn.

## Contract (keep)

```
BEGIN IMMEDIATE
  SELECT next due rows + payloads
  UPDATE those rows to Leased
  INSERT Claim envelope into fireweed_claim_outbox
COMMIT
drop the writer
append that envelope to the object log
DELETE outbox after PUT is durable (not a third apply of the lease)
return the rows from the SELECT
```

- Exclusivity is the committed SQL lease.
- Apply of `Claim` is idempotent: already this token → no-op. Rebuild (still `Pending`) still leases in apply.
- Complete is Class L. Wait for unapplied Push/Update (`catch_up_produce`) before the next lease, not for completes.
- Crash before COMMIT: nothing. After COMMIT before PUT: drain outbox on reopen.

## Cuts

### 1. Live Claim apply does not touch items or groups

Already-leased (Class S just committed): cursor + delete outbox in the apply txn. No item `UPDATE`, bearers, `load_grouped_items`, or group relect.

`append_class_s_claim` returns after PUT + enqueue. Caller does not `delete_claim_outbox_row`.

Pending rows (rebuild): today's lease + group-remove.

### 2. The lease txn is the deliver op

Thin SELECT (payloads stay; no `fields`/`metadata` unless schema/indexes; gate anti-join only if blocked). Unfiltered item claim does not maintain group summaries. Group-aware claims repair heads on read (`rep` no longer pending).

### 3. Pipeline eight claims

After 1–2. Waves of 8 claims; one empty is not done. Writer serializes leases; appends pack.

## Out of scope

Planner map. Process HashMap. `SKIP LOCKED` on Turso. Reservation table. Dropping produce apply-start delay. Changing harness payloads or groups. Postgres dialect. Collapsing sqlite-log into object-log.

## Measure

```sh
cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture
```

| cut | done when |
|---|---|
| 1 | already-leased Claim apply does not scan groups; caller does not `writer.lock()` after enqueue |
| 2 | one IMMEDIATE; thin SELECT; claim p50 ≪ 256 ms |
| 3 | inflight=8 P4; deliver ≥ ingest at N=10k |
