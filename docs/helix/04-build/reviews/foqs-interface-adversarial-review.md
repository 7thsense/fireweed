---
ddx:
  id: review-foqs-interface-and-boundary-plan
  depends_on:
    - build-foqs-inspired-interface-and-boundary
  links:
    - {kind: reviews, to: build-foqs-inspired-interface-and-boundary}
  status: complete
---

# FOQS-Inspired Interface and Boundary Plan: Adversarial Review

## Outcome

Codex and Claude independently converged on **no blocking findings** after iterative review of the build
plan, API-001, ADR-004, ADR-008, the PRD, and the current Rust/backend surfaces.

## Material Corrections

- Split mandatory template semantics (ADR-018) from optional orchestration semantics (ADR-019).
- Added backend-family atomic-create prerequisites, including durable SQLite catalog arbitration and a
  mandatory live PostgreSQL race gate, before `ensure_queue` may claim exact create-or-read behavior.
- Defined façade-local ensure success/conflict types carrying stored and desired definitions, created state,
  and non-durable template diagnostics without changing engine or wire errors.
- Constrained dispersion to a stamped, single-queue, group-granularity discovery result with an advisory
  urgency predicate, stable framing, and no mutation of discovery order.
- Added relational discovery repair for ungrouped work and pure time-based eligibility crossings using a
  read-only live query rather than mutating summaries during discovery.
- Defined multi-queue claim structural preflight, deterministic ownership acquisition, non-short-circuit
  result collection, durable-executor cancellation effects, explicit ceilings, and non-atomic semantics.
- Distinguished downstream/callee pacing from pqueue's own P1 deployment and tenant capacity controls.
- Split the integration example from unconditional full-workspace gates so mandatory work remains closable.

## Convergence Evidence

- Final Codex blocker-only pass: `NO BLOCKING FINDINGS`.
- Final Claude blocker-only pass: `NO BLOCKING FINDINGS`.
- `ddx doc validate`, `ddx doc audit`, and `git diff --check` passed on the converged plan before bead filing.
