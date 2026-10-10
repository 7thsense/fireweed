---
ddx:
  id: adr-postgres-baseline-queue-contract
  depends_on:
    - adr-s3-turso-public-cell
  status: accepted
---

# ADR-026: Simple claim/commit interface with PostgreSQL baseline

Date: 2026-10-10. Decision: project owner, explicit instruction in this session.

The useful queue boundary is insert batch, filtered priority claim batch, commit
claimed updates, and filtered stats. Commit includes atomic workflow state and
new-work inserts; no separate transaction API is needed. Typed state reads and
indexed queries supply Snorri recovery and await lookup without a side-record API.
API-007 specifies the target contract against current Snorri requirements.

## Decision and precedence

- Supersede the unmerged API-006/ADR-025 kernel redesign as governing direction.
  Preserve research/spikes as historical measured evidence, not implementation
  requirements. Do not merge the preserved branch's wholesale contract deletions.
- Maintain PostgreSQL relational storage as the reference implementation and
  supported baseline construction alongside S3 object-log/Turso. This supersedes
  ADR-024's prohibition of PostgreSQL construction for this interface. It does not
  restore a twelve-cell matrix or arbitrary log/projection combinations.
- Both implement one interface, one Snorri adapter, and one mandatory public
  conformance suite. No broad-claim/client-filter fallback is conformant.
- API-007 supersedes current async claim visibility semantics where they permit
  omission of acknowledged eligible work. Internal asynchronous projection is
  allowed; consumer-visible semantics must satisfy the same baseline guarantees.
- Preserve durable-log authority, genesis rebuild, owner fencing, stock-Turso
  safety and formal reliability tests. Partitioning/snapshot/truncation proposals
  remain separately measurement-gated and are not authorized by this decision.

## Delivery status

This decision accepts a target, not completed implementation. Current public
construction still rejects PostgreSQL; filtered-claim fallbacks and existing
adapter APIs require implementation. API-001/API-005 remain descriptions of the
current binding until the new binding and migration pass API-007 acceptance.
Shared DTO/validation/receipt/conformance code must be reused; storage algorithms
need not be identical. No Snorri state/output limit is silently reduced.

Next implementation work: finalize concrete DTO/limit bindings; restore baseline
construction; implement mandatory filtered claims and atomic commit groups;
migrate the single Snorri adapter; run both qualification lanes and comparative
performance evidence. This documentation change files no execution beads and
claims no performance or qualification result.
