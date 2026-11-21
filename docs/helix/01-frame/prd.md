---
ddx:
  id: prd
  depends_on:
    - product-vision
---

# Product Requirements: Fireweed data plane

## Problem

Snorri and Cayce need durable storage for workflow data that is high in
volume and rate of change: work items, workflow state, per-recipient state,
admission chunks, events and timelines. Without it they rewrite workflow
state as one JSON blob per commit and keep idempotency and seen-event sets
in process memory. A fixed queue API can't express the bulk changes,
guards and live counts they need.

## Goals

1. One interface, the data-plane DSL (API-008), for every shape of
   workflow data: queues, keyed tables and sequenced tables.
2. Programs that claim, settle, update, insert and check guards atomically.
3. Work that scales with the rows a program touches, never with the size of
   a queue.
4. PostgreSQL 18 as the first, production-supported executor; a local
   engine later, passing the same conformance documents.

## Non-goals

- A tenant data plane. Entity resolution, profiles and enrichment are
  Niflheim's; account-level state is the control plane's (ADR-027 D53,
  D57).
- Tenant-wide counters and cross-workflow policies. They stay in Dragonfly,
  with values from the control plane (D45, D49, D58).
- SQL from consumers, joins, or ad-hoc queries without an index.
- Large payloads; a field holds a reference (D16).

## Functional requirements

1. A consumer creates a space from a schema and changes it only by adding
   objects, fields and reads (API-008 §Changing a schema).
2. A program is a list of typed statements that run in order as one
   transaction; a rejection rolls all of them back and is recorded.
3. A program with a request id carries `issued_at`. A replay returns the
   recorded response; a different program with the same id fails; a
   request issued before the space's horizon is refused (D61).
4. Claims take due items in due-time, then priority, order, skip items
   other transactions hold, and grant a lease fenced by a token. A stale
   token never settles or renews (D6).
5. Lease expiry and attempt exhaustion follow from stored times and the
   database clock; no caller drives a clock (D1, D2).
6. Updates and deletes can guard with expectations, step through large
   matches, and choose how to treat leased items.
7. Counts and other aggregates run on the database, read from an index,
   with no row cap (D63).
8. Many independent programs can run in one batch with one commit (D62).
9. Versions never repeat for a key, even after a delete and a reinsert
   (D61).

## Non-functional requirements

| Requirement | Target |
|-------------|--------|
| No in-process state | The executor keeps nothing between calls: no caches, loaded copies or locks. |
| Index-bounded statements | Every selection has an index path; no statement scans a whole table. |
| Bounded statements | API-008 §Limits caps statements, rows and bytes per program; every statement runs under a timeout. |
| Concurrency | Many processes can claim from one queue at once without leasing an item twice. |
| Durability | A `durable` program is committed before its response; `fire_and_forget` skips the WAL flush (API-008 §Durability). |

## Success metrics

ADR-027 §Validation lists the metrics and the triggers that reopen the
design. The executor's conformance tests (API-008 §Conformance) are the
acceptance suite.

## Open questions

ADR-027 §Open questions for the owner.
