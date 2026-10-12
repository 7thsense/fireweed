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

## API-007 publication gate

Run `python3 scripts/formal/check_api007.py`. Its independent finite specification
covers original resolution, a distinct-identity background publisher, and
unavailable-authority refusal with interrupted projection apply/reopen. Attempts
become publishable only after dispatch. Accepted stale CAS operations still
reject at the publication frontier. Equal, changed and expired replay requests
pass through a fresh witnessed cut; a separate history-based oracle checks their
classification, including missing cached originals and changed-before-expired
precedence.

Qualification requires complete graphs, completed reachable outcome witnesses,
and shortest negative traces replayed against the exact transition relation.
The runner fails incomplete searches, missing witnesses, damaged transitions,
source changes and output-path escapes. Reports bind configuration, graphs,
traces, source revision/dirty state and Python version. Outputs remain under
`target/api007-formal/`. The reported frontier count is actual coverage; zero
reachable frontiers does not establish frontier execution.

This family does not model catalog/process/cache evidence arbitration,
independent concurrent same-identity callers, lease/clock/ABA/key collisions,
budget/progress/deadlines, FP4 administration or known-batch cached integrity.
Its production mapping is documentary and unexecuted. Identical public backend
trace correspondence and the other required families remain mandatory.

## Implementation correspondence

Execute independently predicted transitions through the identical public facade on both backends. Compare exact work/state/receipt identities, membership/order, tokens, current versions/expiry, attempts, rejection effects, new work and successful read coverage. Include concurrency, lost replies, expiry, renewal, cancellation, clock delay, collision/ABA, original-genesis corruption, S3 stale head/epoch, commit crash/reopen and interrupted projection apply. Record intended and actually matched edges separately. Primitive/internal diagnostics cannot substitute for successful public-path observations.

Real PostgreSQL process/server faults, live S3 capability/fault qualification and stock Turso reopen/rebuild tests remain separate mandatory evidence. A modelled lost reply is not packet loss; graceful reopen is not process death. Do not delete projection files where the public recovery interface owns rebuilding.

## Legacy model disposition

The existing scripts/formal/check.py and unittest corpus may continue as regression controls. Earlier recovery/batching/replay models permitted stale empty observations or expired request reuse and the old trace corpus used paused apply, ephemeral control-plane sessions and leased-tail exclusion. Their green results do not qualify API-007. Update affected models and correspondence before readiness claims, preserving explicit positive/negative controls and bounds rather than carrying historical counts into the new contract.

Run model and replay tools into run-owned target directories or declared evidence storage, outside active Helix documents. Documentation contains property/assumption/acceptance requirements; runtime graphs, trace captures and review journals remain run artifacts.

## Legacy scheduled regression tooling

The legacy interpreter supports `schedule-smoke` and `exhaustive` profiles.
Selection is regenerated solely from its independent finite model. Scheduled
cases retain every oracle alternative and compatible predecessor; first polling
is a stimulus, and actual backend polls/Pending/completion must demonstrate
overlap. Per-case observed paths must realize the requested schedule. Reconcile
case/base/schedule identities and per-step witnesses; require witnessed-edge set
equality with the independently reconstructed legacy graph for its exhaustive
profile. Counts or intended targets alone cannot qualify even that legacy model.

The local fixture wrapper accepts `--fixture-parent` on a filesystem with sufficient
inode and byte headroom. It preflights conservative capacity, uses one private
root with sibling S3/world directories and child TMPDIR, retains S3 worlds through
qualification, and verifies owned cleanup. These operational guards do not change
the oracle or substitute for current API-007 public-path obligations.

Historical source-bound execution, closure indices and reviews are preserved under
[legacy replay artifacts](../../../artifacts/formal/legacy-replay/edge-closure-evidence.md).
Those results apply to their recorded revisions and legacy backend fixture; merging
the tooling does not qualify the current API-007 implementation. Run new outputs
into run-owned target/evidence directories, outside active Helix documents.
