---
ddx:
  id: architecture
  type: architecture
  authoring:
    home: repo
  links:
    - kind: informed_by
      to: prd
    - kind: informs
      to: api-data-plane-dsl
---

# Fireweed architecture

## Scope

ADR-027 and API-008 govern. Consumers declare queues and tables in a schema
document and change them with programs of typed statements, one transaction
each. An executor creates real tables and indexes and runs each statement as
one SQL statement. PostgreSQL 18 is the first executor; the local engine
follows.

## System context

| Element | Role | Talks to Fireweed through |
|---------|------|---------------------------|
| Snorri | Workflow execution: instance heads, await keys, effects, idempotency | Programs over its workflow's space |
| Cayce | Campaign execution and its UI: recipients, admission chunks, suppression copies, timelines | Programs and declared reads |
| Control plane | Tenants, definitions, configuration, suppression masters | Not Fireweed; consumers read it themselves |
| Dragonfly | Tenant-wide counters and cross-workflow policies | Not Fireweed (ADR-027 D45, D58) |
| Niflheim | Exhaust, entity resolution, profiles | Not Fireweed (D57) |

## Modules

| Crate | Responsibility | Public API | Depends on | Must not depend on |
|-------|----------------|------------|------------|--------------------|
| `fireweed-dsl` | Schema and program documents, the checker that resolves a program against a schema, limits and fingerprints | `Schema`, `Program`, `Statement`, `Response`, `check`, `from_yaml` | serde, YAML and JSON parsing, SHA-256 | Any database driver or executor |
| `fireweed-dsl-postgres` | The PostgreSQL executor: DDL from a schema, one SQL statement per DSL statement, request records, sweep | `apply_schema`, `get_schema`, `execute`, `execute_batch`, `sweep` | `fireweed-dsl`, `tokio-postgres` | In-process state between calls |

## Running a program

1. One pipelined flight sets the transaction's settings, opens the space
   (its schema row and request record) and sets savepoints.
2. The program is checked against the space's schema in Rust.
3. Each statement runs as one SQL statement.
4. The response is recorded with the request id, and the transaction
   commits.

API-008 §The PostgreSQL executor gives the layout (one PostgreSQL schema per
space, one table per queue and table) and each statement's SQL.

## Quality attributes

| Attribute | How it is met | How it is checked |
|-----------|---------------|-------------------|
| Work bounded by indexes | The index rule; sequential scans off in program transactions | Tests assert no sequential scan and few index reads over thousands of items |
| No in-process state | Every call reads the schema and request record from the database | Code review; the crates hold no statics or caches |
| Exact replay | Request records written in the program's transaction; horizon refusal | Replay, conflict and expiry tests |
| Concurrency | Row locks, `SKIP LOCKED` claims, deadlock retries | Several connections claiming one queue without leasing an item twice |

## Decisions

ADR-027 records the decision and the owner's rulings. Its open questions
(PostgreSQL layout, cohorts, UMF schema documents, a narrow lease table, run
retirement) are the remaining design work.
