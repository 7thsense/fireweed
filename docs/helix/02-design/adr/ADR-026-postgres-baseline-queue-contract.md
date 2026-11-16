---
ddx:
  id: adr-postgres-baseline-queue-contract
  depends_on:
    - prd
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

## Module and configuration ownership

Architecture owns the module map, integration translation and construction root.
Core/planners retain shared semantic policy; adapters own native persistence.
The construction Contract defines caller-owned typed configuration and operational
clock with validation before I/O. The diagnostics Contract defines caller-owned
instrumentation and separately bounded best-effort logging. Library code does
not discover environment, exporters or service topology in its hot path.

HELIX v0.15.4 modularity and encapsulation applies to handwritten source.
Enforce production dependency/visibility controls before dependent feature work;
record exact legacy exceptions and remove them at cutover. Formal assurance is
scoped to reviewed properties, assumptions, models and correspondence. Results
that permit stale empty claims or reused expired request identities cannot
qualify this contract. The build plan owns remaining execution and evidence.
