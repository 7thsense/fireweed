---
ddx:
  id: product-vision
---

# Product Vision

## Mission Statement

Fireweed is the data plane for Snorri and Cayce. It holds their
high-volume, fast-changing workflow data, many workflows with many runs,
in queues and tables they declare, and lets them claim, change and query
that data in groups through a small typed DSL that runs as plain SQL.

## Positioning

For teams building workflow and campaign engines, Fireweed is a storage
layer whose unit of work is a transaction over groups of rows: claim a
filtered batch in due-time order, settle it, update workflow state and
insert next work, atomically. Unlike a fixed queue API, the DSL covers
bulk changes, guards and live counts without new methods. Unlike raw SQL,
every program is checked against the schema and bounded by indexes before
it runs.

## Vision

When Fireweed succeeds, Snorri and Cayce keep no tables of their own and no
unbounded state in memory. Every piece of durable workflow data they need
goes through Fireweed. The control plane holds small, static state; logs
and other exhaust go to Niflheim; tenant-wide counters stay in Dragonfly.

**North Star**: every program's work scales with the rows it touches,
through indexes, never with the size of a queue, and every acknowledged
program is durable and visible to the next.

## Product boundary

[ADR-027](../02-design/adr/ADR-027-data-plane-interface-is-the-implementation-boundary.md)
and [API-008](../02-design/contracts/API-008-data-plane-dsl.md) define the
interface:
- a **schema** declares a space's queues, tables, typed fields, indexes and
  named reads, and the executor creates real tables and indexes from it;
- a **program** is a list of typed statements that run as one transaction;
- request ids replay their recorded response, and a request issued before
  the space's horizon is refused rather than run twice;
- leases are fenced by tokens, and the database's clock decides what is
  due.

PostgreSQL 18 is the first executor. A local engine (Turso on NVMe with an
S3 log, and SQLite) follows and runs the same conformance documents.
Consumers never write SQL or manage storage transactions.

## Users

| Attribute | Description |
|-----------|-------------|
| Who | Snorri (workflow execution) and Cayce (campaign execution and its UI) |
| Pain | Workflow state rewritten as one JSON blob per commit, idempotency and seen-event sets held in process memory, and fixed queue methods that can't express bulk changes |
| Why Fireweed | Typed queues and tables per workflow, atomic programs over them, and indexed reads, on PostgreSQL first |

## Success Definition

| Criterion | Definition |
|-----------|------------|
| No hidden state | No executor loads an object whole or holds request state in memory. |
| Index-bounded work | Statements read about as many index entries as they return; no statement scans a whole table. |
| Consumer fit | Snorri runs with no tables of its own; Cayce's UI uses declared reads only. |
| One contract | Every executor passes the conformance suite for what it declares. |
