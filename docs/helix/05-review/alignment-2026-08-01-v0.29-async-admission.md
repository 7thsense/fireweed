# Alignment — 2026-08-01 (v0.29 async admission + authority)

**Scope:** Snorri-facing correctness after async cutover; object-log authority;
post-v0.15 E3 evidence bind.  
**Catalog:** in-tree HELIX under `docs/helix/`; graph from package floor when
project `workflows/graph.yml` is absent.  
**Mode:** align (gap → destination, not full 74-doc stale refresh).

## Summary

| Area | Status | Notes |
|------|--------|-------|
| Queue-local admit then apply (leases/fences) | **ALIGNED** | `finalize_outcomes` / `renew_item_ids` / claim_by_item_ids under `KeyedQueueGate`; regressions green |
| Instance-fence concurrent commits | **ALIGNED** | `c3bae4c9` / fireweed-5497780d |
| NativeConditionalWrite / Garage | **ALIGNED** | Matrix + fail-closed open wording; no second authority |
| LogEngine TP-003 E3 48-row live emit | **INCOMPLETE** | Emitter + script wire present; full AC-TXN-1..7 at 4 bounds needs operator env run |
| TP-002 tag-gate E0–E3 packaging | **INCOMPLETE** | `pqueue-bf46289d` blocked on density + E3 residual |
| Doc review-hash staleness (74 docs) | **STALE_PLAN** | Dependency hash drift; not a product-behavior divergence |

## Findings

### 1. ALIGNED — validate-before-apply race family (Snorri)

- **Evidence:** `d757af3e` — `AsyncComposedBackend::finalize_outcomes` /
  `renew_item_ids`; objectlog memory/sqlite/hybrid + postgres objectlog;
  `claim_by_item_ids` prepare under `submit_operation`.
- **Tests:** `claim_then_immediate_commit_transition_succeeds`,
  `competing_workers_claim_finalize_never_illegal_lifecycle` (objectlog lib).
- **Governing:** TD-007 pre-validate-before-append; ADR-017 async commit
  strategy; same pattern as fence TOCTOU fix.

### 2. ALIGNED — object-log authority

- **Evidence:** `docs/operator/object-log-authority-compatibility.md`; Garage
  v2.2.0 unsupported; `open_s3` error names endpoint + precondition.
- **Governing:** API-005 `ObjectLogAuthority::NativeConditionalWrite` only.
- **Non-goal:** no alternate authority for non-enforcing endpoints without a
  new ADR.

### 3. INCOMPLETE — E3 four-bound TP-003 bind (pqueue-802be88f)

| Field | Content |
|-------|---------|
| Destination | TP-002 / TP-003 evidence + `scripts/perf/tp002-e3-s3.sh` |
| Deliverable | Live 48-row `FIREWEED_E3_TRANSACTION_EVIDENCE_OUT` + fence row + E3 ledger → `fireweed-build-e3-contract` green |
| Next mode | Runtime / operator live MinIO run (not doc evolve) |
| Evidence | `e3_contract` 40 unit tests pass; emitter in `e3_governed_transaction_evidence_matrix.rs`; script prefers that test |

Focused **semantic** ACs (reject missing AC / unjustified N/A / force-sealed vs
latency-window) are already green in `fireweed-release` tests.

### 4. STALE_PLAN — mass `ddx doc stale`

74 docs report missing/changed dependency review hashes. Treat as **hygiene**,
not as “async cutover broke contracts.” Refresh hashes via normal review
pass when editing each doc; do not mass-regenerate product specs for this
release.

## Residual queue (after this alignment)

1. `pqueue-802be88f` — complete live 48-row emission on release host  
2. `pqueue-bf46289d` — tag gate (blocked on density + E3)  
3. `pqueue-c4e5f691` — epic until children close  

## Verdict

Product **code path** for Snorri blockers is aligned and verified in-tree.
Performance evidence packaging remains incomplete by design of the post-v0.15
epic; it does not block a correctness-focused v0.29.0 source release.
