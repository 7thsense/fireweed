---
ddx:
  id: alignment-2026-07-29-v0.23.3
  type: alignment-report
  flow: helix
  status: draft
  links:
    - kind: informed_by
      to: storage-matrix-completion-brief
    - kind: informed_by
      to: public-preview-boundary
    - kind: informed_by
      to: production-deployment-readiness
---

# Alignment report — v0.23.3 post-matrix release

**Date**: 2026-07-29  
**Catalog**: source-checkout `workflows/graph.yml` (HELIX plugin)  
**Scope**: product surface after `v0.23.3` tag (`211376db`); public 5×3 storage matrix + release path.  
**Authority**: vision/PRD unchanged; governing matrix brief + completion brief + API-005 + ADR-013 Class B.

## Summary

| Classification | Count | Notes |
|----------------|------:|-------|
| ALIGNED | 4 | Matrix brief/completion intent; public-preview-boundary Class B voice; Cargo/tag/`docs/releases/v0.23.3.md` identity; storage-matrix gate wiring in release scripts |
| STALE_PLAN | 2 | Public preview checklist still v0.21.0; DEPLOYMENT-READINESS still carries legacy `objectlog`/`hybrid` product tables alongside the 15-cell gate |
| INCOMPLETE | 2 | E3 exact 10M recovery note still PREPARED; density live evidence blocked on durable runner |
| BLOCKED | 2 | `fireweed-3aaa3ebc` (in progress — live MinIO recovery run); `pqueue-c989bc20` (durable Kind runner) |

## Findings

### ALIGNED

1. **Product matrix model** — `orthogonal-storage-matrix-brief` and `storage-matrix-completion-brief` match shipped `StorageConfig` open paths and Class A/B semantics. Evidence: `docs/helix/02-design/orthogonal-storage-matrix-brief.md`, `docs/helix/04-build/storage-matrix-completion-brief.md`, `crates/fireweed/src/lib.rs` `ObjectLogAuthority::NativeConditionalWrite` only.
2. **Release identity** — workspace version, annotated tag, and notes agree: `v0.23.3` / `211376db` / <https://github.com/7thsense/fireweed/releases/tag/v0.23.3>.
3. **Matrix CI binding** — `scripts/ci/storage-matrix-gate.sh` is wired into release/deployment release-gate paths (commit `212915ce` and successors on `main`).
4. **Legacy authority demotion** — public tests no longer reference `ObjectLogAuthority::Postgres`; clippy `-D warnings` restored (`d16b97bc`).

### STALE_PLAN

1. **Public preview checklist** (`docs/helix/05-deploy/public-preview-checklist.md`)  
   - **Classification**: STALE_PLAN  
   - **Evidence**: title and tables still pin `v0.21.0` / 2026-07-26 while latest public release is `v0.23.3`.  
   - **Destination**: deployment-checklist (same path).  
   - **Deliverable**: retarget checklist to `v0.23.3` with current tag, matrix gate, and publication boundary.  
   - **Next mode**: evolve (this pass).

2. **DEPLOYMENT-READINESS legacy SKU tables** (`docs/helix/04-build/DEPLOYMENT-READINESS.md`)  
   - **Classification**: STALE_PLAN  
   - **Evidence**: lines ~117–142 still describe `objectlog`×`hybrid*` product rows while §storage matrix (lines ~50–80, ~256+) already states the 15-cell public surface.  
   - **Destination**: deployment readiness doc.  
   - **Deliverable**: demote or delete legacy SKU tables; single public axis table only.  
   - **Next mode**: evolve (bead).

### INCOMPLETE

1. **E3 10M recovery PASS stamp** — `docs/perf/tp002-e3-objectlog-s3-release.md` remains **PREPARED**. Live `TestE3RecoveryExact*` is the AC for `fireweed-3aaa3ebc` (claimed; release-profile run on MinIO).  
   - **Next**: close bead when both exact tests + reject tests + fmt/clippy pass; then flip status PREPARED→PASS with evidence.

2. **1,000-queue density release evidence** — `pqueue-c989bc20` blocked on durable runner after external parent kill mid Kind job.  
   - **Next**: operator durable runner, then re-run density harness.

### BLOCKED (external)

- Density durable-runner requirement (see bead `pqueue-c989bc20`).
- Full native multi-region S3 campaign remains outside MinIO local proof (parent epic `pqueue-c4e5f691` children).

## Handoffs filed

| Gap | Bead / action |
|-----|----------------|
| Preview checklist retarget | Evolve in this session |
| DEPLOYMENT-READINESS legacy tables | Bead (if not closed by same edit) |
| E3 10M PASS | Existing `fireweed-3aaa3ebc` |
| Density | Existing `pqueue-c989bc20` (operator) |

## Verdict

Product matrix authority and v0.23.3 publication are **aligned**. Deploy checklist and residual DEPLOYMENT-READINESS SKU language are **stale relative to the released matrix**. Scale evidence (10M recovery, density) remains **incomplete/blocked**, not a matrix-product gap.
