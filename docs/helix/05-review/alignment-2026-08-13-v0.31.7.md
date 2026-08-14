# Alignment — 2026-08-13 (v0.31.7 pin)

**Scope:** PRD / product-vision vs shipped `v0.31.7` (`4dc843c7` peel, notes on `927ed368`).  
**Mode:** high alignment (gap → destination). Not a 74-doc hash refresh.

## Session brief

- Close the cell-uniform claim-echo hole on sqlite (compact-log `index_fields`).
- Confirm performance ratchets on a quiet host.
- Next product cuts are **+10% durable tps** on the sqlite×sqlite snorri cycle probe (w1),
  not a new +1k ladder.

## Summary

| Area | Status | Notes |
|------|--------|-------|
| Transaction contract (new FWC1 byte maps) | **ALIGNED** | Native serialize matches native deserialize |
| Claim exclusivity / no lost work | **ALIGNED** | Sealer + admit still serialize apply in log order |
| Same contract, all 20 cells (P0-15) | **STRAINED** | Memory + sqlite claim echo from `index_fields`; Turso/Postgres still entity-column only |
| Log remains SoT (ADR-013) | **ALIGNED** | Echo is a derived view of durable `index_fields` |
| Seventh Sense / Snorri compact-log echo | **PARTIAL** | Memory apply + sqlite `render_claimed`; not Turso (default) or Postgres |
| sqlite×sqlite durable tps (quiet host) | **ALIGNED to 13k cell** | w1 **13,141** / w4 **13,422** / w8 **14,092**; w8/w1 = 1.07 |
| +10% vs v0.31.6 w1 (12,221 → 13,443) | **OPEN** | 13,141 is +7.5%; next cut waits for ≥13,443 |
| Class A / Class B durability claims | **ALIGNED** | No new log-rebuild overclaim |
| P20pr governed tag evidence (E ≠ S) | **INCOMPLETE** | Same as v0.31.5/6; product pin is annotated tag + GitHub release |
| P0-11 / P0-14 scale (10M, 1000 queues) | **INCOMPLETE** | Evidence packaging / live E2–E3; not this cut |

## Findings

### 1. ALIGNED — FWC1 byte-map symmetry

- **Evidence:** `crates/fireweed-engine/src/wire_bytes.rs` human vs native split.
- **Governing:** vision transaction contract; PRD P0-15 visibility of accepted mutations.

### 2. ALIGNED — concurrency ratchet (w8 ≱ collapse)

- **Evidence:** sealer in `claim_finalize_push_cycle`; quiet-host probe w8 **14,092** > w1 **13,141**.
- **Governing:** one queue cannot be 8×13k; workers must not serialize into a slower seal.

### 3. PARTIAL — claim entity echo

- Memory: `rehydrate_entity_document` → `echo_entity_document`.
- SQLite: `render_claimed` now SELECTs `index_fields` and synthesizes when `entity_document` is NULL.
- Turso / Postgres: no `index_fields` column; typed keys still from `entity_document` only.
- **Destination:** every public projection cell echoes the same JSON object Snorri/API-005 expect.

### 4. OPEN — +10% performance increment

| Baseline | Value | +10% gate | Quiet-host now |
|----------|------:|----------:|---------------:|
| v0.31.6 sqlite×sqlite w1 | 12,221 | **13,443** | 13,141 |
| This session sqlite×sqlite w1 | 13,141 | **14,455** (next after gate) | — |

Do **not** cut a performance increment until w1 ≥ 13,443 on the in-tree probe (best-of-2).

### 5. INCOMPLETE — P20pr / 10M / 1000-queue evidence

Unchanged from the 2026-08-01 alignment. Not closed here.

## Residual queue

1. Land sqlite claim echo (this session).
2. Persist `index_fields` + echo on Turso and Postgres (owns `v0.31.8` if not taken by the +10% tps cut).
3. Hit sqlite×sqlite w1 ≥ 13,443, then cut.
4. P20pr promotion metadata when an evidence archive exists.

## Verdict

v0.31.7 is a valid **product pin** for FWC1 consumability, Rust 1.97.1, and
w8≈w1. It does **not** finish P0-15 on Turso/Postgres. Performance ratchets
≥10k / ≥13k (sqlite×memory historically) and sqlite×sqlite ~13k on a quiet host
are met; the new **+10% increment** from v0.31.6 w1 is not yet met.
