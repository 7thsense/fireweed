---
ddx:
  id: tp-formal-analysis
  type: test-plan
  depends_on: [contract-formal-queue-semantics, contract-model-trace-replay, architecture]
  status: accepted
---

# TP-006: Bounded formal analysis and implementation correspondence

## Scope and rigor

CONTRACT-006 defines backend-neutral API-007 safety/progress properties; CONTRACT-007 defines independent trace correspondence and backend refinements. Use precise reviewed specifications during implementation and executable bounded analysis before critical protocol/readiness claims. Formal-methods scope is durable ACK/freshness, publication authority, atomic state/work/new-work groups, request/lease replay, clocks, version ABA and admission ownership. Routine unrelated code does not require a formal model.

Finite exhaustive results apply only to declared domains and assumptions. They are not unbounded Rust refinement, provider durability, production liveness or numeric performance proof. Every report declares assumptions, bounds, transitions, invariant IDs, model/source/corpus hashes, explored state/edge counts, exclusions and implementation mappings. Exhausted search/resource/time caps, zero cases, malformed schemas or unmatched traces fail qualification.

## Required analysis families

| Properties | Model and production obligation |
| --- | --- |
| F-ACK/PREFIX/FENCE | Abstract committed history plus PostgreSQL atomic-transaction and S3 conditional-publication refinements; fresh public observations and exact original-genesis recovery |
| F-GROUP/KEY/ABA | Atomic claimed updates, state CAS, new work and receipt; full-key uniqueness, collision separation, stable nonrepeating versions |
| F-LEASE/REPLAY/CURRENT-REPLAY/CLOCK | Current ownership, expiration, renewal, whole-claim validity, permanent expired identities and fresh operational-clock sampling |
| F-BATCH | Serial witness, original request/group identity, outcome conservation and acknowledged coverage |
| F-BUDGET | Owned bytes/slots/scratch through cancellation and physical terminal drain; no early release or double release |
| F-EVENTUAL/DEADLINE | Explicit fairness and timed arrival/service/recovery envelope; production numeric progress independently tested by TP-002/005 |

Every safety property has a positive reachable witness and an intentional negative control producing a reproducible counterexample. Liveness controls require a fairness-qualified nonprogress cycle, not arbitrary provider outages. Timed deadline fixtures state finite workload/service bounds; eventual application is insufficient evidence for numeric progress.

## Implementation correspondence

Execute independently predicted transitions through the identical public facade on both backends. Compare exact work/state/receipt identities, membership/order, tokens, current versions/expiry, attempts, rejection effects, new work and successful read coverage. Include concurrency, lost replies, expiry, renewal, cancellation, clock delay, collision/ABA, original-genesis corruption, S3 stale head/epoch, commit crash/reopen and interrupted projection apply. Record intended and actually matched edges separately. Primitive/internal diagnostics cannot substitute for successful public-path observations.

Real PostgreSQL process/server faults, live S3 capability/fault qualification and stock Turso reopen/rebuild tests remain separate mandatory evidence. A modelled lost reply is not packet loss; graceful reopen is not process death. Do not delete projection files where the public recovery interface owns rebuilding.

## Legacy model disposition

The existing scripts/formal/check.py and unittest corpus may continue as regression controls. Earlier recovery/batching/replay models permitted stale empty observations or expired request reuse and the old trace corpus used paused apply, ephemeral control-plane sessions and leased-tail exclusion. Their green results do not qualify API-007. Update affected models and correspondence before readiness claims, preserving explicit positive/negative controls and bounds rather than carrying historical counts into the new contract.

Run model and replay tools into run-owned target directories or declared evidence storage, outside active Helix documents. Documentation contains property/assumption/acceptance requirements; runtime graphs, trace captures and review journals remain run artifacts.
