---
ddx:
  id: spike-aurora-dsql-compatibility
  depends_on:
    - prd
    - adr-queue-as-shard-unit-and-projection-families
    - adr-log-single-source-of-truth
    - td-postgres-native-reference-mode
  links:
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: adr-queue-as-shard-unit-and-projection-families}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: td-postgres-native-reference-mode}
  status: completed
---

# Technical Spike: Aurora DSQL Compatibility With `postgres_native`

**Spike ID**: SPIKE-001 | **Lead**: DDx queue steward | **Time Budget**: 4 hours | **Status**: Completed

## Objective

**Technical question**: Can Aurora DSQL execute the exact PostgreSQL SQL and
transaction semantics required by fireweed's `postgres_native` push, update,
claim, finalize, control-plane, metrics, and rebuild paths without changing
ADR-008 or ADR-013?

Goals:

- inventory exact fireweed SQL shapes against current official Aurora DSQL support;
- identify syntax, transaction, concurrency, and quota incompatibilities;
- return one verdict: `supported`, `requires-new-claim-algorithm`, or `rejected`.

Success criteria: every design-defining claim cites current official AWS
documentation or exact repository evidence; unsupported claims remain explicit.

Out of scope: a DSQL adapter, credentials, infrastructure, schema changes,
performance claims, or an architecture commitment.

## Scope

- **As of**: 2026-07-27.
- **Sources**: current fireweed SQL/design plus four official AWS Aurora DSQL
  documentation pages. PostgreSQL-compatible syntax was not treated as proof of
  DSQL behavior unless AWS documents it.
- **Live probe**: not run. No approved Aurora DSQL endpoint or credentials were
  present or read. Documentation is sufficient for the rejection verdict.
- **Search limit**: primary sources only. The web connector failed authentication,
  so official AWS Markdown pages were retrieved directly over HTTPS.

## Hypothesis

**HYPOTHESIS: PARTIALLY CONFIRMED** — ordinary DML and keyed transactions may
port, but `postgres_native`'s ordered concurrent claim, database trigger logic,
and advisory-lock seams will not.

Expected outcome: Aurora DSQL requires a distinct claim/concurrency design and
cannot be relabeled as the existing PostgreSQL-native backend.

## Approach

Method: bounded literature review and exact-SQL static compatibility analysis.

1. Extract SQL used by `fireweed-postgres` for item claims, group/cohort claims,
   updates/finalizes, control-plane serialization, manifest pointers, triggers,
   and projection rebuild.
2. Compare each shape with AWS's supported SQL, migration guidance, OCC model,
   and hard database limits.
3. Evaluate consequences against PRD P0-5..15, ADR-008, ADR-013, and TD-002.
4. Stop when one incompatibility invalidates the current claim algorithm and
   independent evidence invalidates the current schema/trigger path.

## Findings

### FINDING: Ordered `SKIP LOCKED` claim is incompatible

Aurora DSQL documents `SELECT ... FOR UPDATE` only for a single-table query with
equality predicates on every primary-key column. Range, `IN`, `OR`, joins, or
other non-equality predicates produce an error. Its supported-clause table does
not list `SKIP LOCKED`.

Source: [AWS — Supported SQL for Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-sql-features.html).
Confidence: high; AWS directly controls the behavior.

Fireweed's item claim selects an ordered eligible range, limits it, locks it
with `FOR UPDATE SKIP LOCKED`, and updates the candidates in one CTE
(`crates/fireweed-postgres/src/relational.rs`, `CLAIM_CTE`). Group and cohort
claim use joins/lateral subqueries, ordering, limits, and `FOR UPDATE OF ...
SKIP LOCKED`. These are outside DSQL's documented locking subset.

Implication: DSQL cannot run TD-002's claim algorithm. Removing `SKIP LOCKED`
would not be a syntax-only port; it changes contention, selection, progress,
whole-group/cohort atomicity, and failure behavior.

### FINDING: DSQL concurrency is optimistic, not PostgreSQL row-lock scheduling

Aurora DSQL uses lock-free optimistic concurrency control. Conflicting row
updates are detected at commit and return SQLSTATE `40001` (`OC000`); AWS tells
applications to use idempotent transaction retries and avoid hot keys.

Source: [AWS — Concurrency control in Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html).
Confidence: high.

Fireweed uses row locks as part of its selection algorithm: concurrent claimers
skip locked candidate sets, group summary locks prevent group splitting, cohort
locks preserve all-or-none membership, and queue/control rows serialize
mutations. OCC can preserve atomic commit after retry, but it cannot preserve
the same nonblocking selection algorithm by merely retrying the same query.

Implication: a DSQL backend needs a new claim-reservation/CAS algorithm and new
progress-under-contention evidence. It cannot inherit TD-002 conformance.

### FINDING: Trigger and PL/pgSQL schema paths are unsupported

AWS documents SQL-language functions, not PL/pgSQL, and its migration guide
directs trigger-like behavior into application/event-driven logic.

Sources:

- [AWS — Supported SQL for Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-sql-features.html)
- [AWS — Migrating PostgreSQL to Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html)

Confidence: high for the documented support surface and migration direction.

`fireweed-postgres` creates PL/pgSQL functions and row triggers for typed-index
components, lifecycle metrics, and group-due summary maintenance
(`crates/fireweed-postgres/src/relational.rs`, schema string). Those artifacts
cannot be installed as written.

Implication: DSQL requires application-owned set-based maintenance in the same
transaction, with independent proofs for exact metrics/index/group-summary
behavior. That is a new adapter design, not a provider substitution.

### FINDING: Advisory-lock and index DDL seams differ

The manifest-pointer schema initializer calls
`SELECT pg_advisory_xact_lock($1)`
(`crates/fireweed-postgres/src/manifest_pointer.rs`). DSQL describes a lock-free
architecture and does not list PostgreSQL advisory-lock functions in its
supported SQL surface. DSQL also requires `CREATE INDEX ASYNC`; fireweed
migrations use `CREATE INDEX CONCURRENTLY`.

Sources:

- [AWS — Concurrency control in Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html)
- [AWS — Migrating PostgreSQL to Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html)

Confidence: high that current fireweed statements are not documented as
supported; a live negative probe was not required for the verdict.

Implication: schema initialization and manifest-pointer serialization need
DSQL-native coordination and DDL lifecycle. The existing pointer adapter cannot
be reused unchanged.

### FINDING: Hard transaction limits constrain valid fireweed batches

Aurora DSQL hard-limits a write transaction to 3,000 mutated rows, 10 MiB of
modified data, and five minutes; the protocol message limit is 10 MiB. The row
limit counts all DML statements, not just submitted queue items.

Source: [AWS — Aurora DSQL quotas and database limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html).
Confidence: high.

Fireweed accepts logical batches up to 1,000 items, but one push/update/finalize
can also write command-log, idempotency, item, gate, cohort, typed-index,
summary, metrics, and replay rows. A 1,000-item request can exceed 3,000 mutated
rows even before variable gate/index fanout. Payload and replay outcomes can
also approach the 10 MiB limits.

Implication: a DSQL design needs preflight row/byte accounting and a lower
contract/profile batch envelope. Internal multi-transaction chunking would
violate API-001's one-request durability and unknown-outcome semantics unless a
new coordinator is designed.

### FINDING: Fixed Repeatable Read and retry behavior are compatible only in part

Aurora DSQL fixes transaction isolation at PostgreSQL `Repeatable Read`; it
supports `BEGIN`/`START TRANSACTION` with that isolation and standard commit/
rollback.

Source: [AWS — Migrating PostgreSQL to Aurora DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html).
Confidence: high.

This aligns with fireweed's explicit repeatable-read transaction intent, and
ordinary `INSERT`/`UPDATE`/`DELETE`, CTE, join, and `ON CONFLICT` shapes are
documented. Those compatible pieces do not compensate for the claim, trigger,
lock, and transaction-budget failures.

## Exact SQL Compatibility Matrix

`Expected` is based on official documentation; `probe` is safe read/DDL/DML to
run only in a disposable approved environment.

| Fireweed surface | Exact SQL shape / probe | Expected | Semantic consequence |
|---|---|---|---|
| item claim | `SELECT item_id ... ORDER BY priority_sort,created_seq LIMIT $4 FOR UPDATE SKIP LOCKED` | rejected | Core TD-002 concurrent ordered claim unavailable. |
| grouped claim | `... JOIN LATERAL (...) ... FOR UPDATE OF s SKIP LOCKED` | rejected | Joins and non-key predicates violate DSQL locking subset; group non-splitting proof lost. |
| cohort claim | `... NOT EXISTS (...) ORDER BY ... LIMIT 1 FOR UPDATE OF c SKIP LOCKED` | rejected | Whole-cohort selection needs a new CAS/reservation design. |
| keyed control row | `SELECT ... FROM fireweed_queue_owner WHERE tenant=$1 AND queue=$2 FOR UPDATE` | syntax candidate; OCC semantics | Equality on full key fits documented syntax, but conflicts surface at commit and require idempotent retry. |
| batch update/finalize lock | `... WHERE item_id=ANY($3) OR client_item_key=ANY($4) FOR UPDATE` | rejected | `ANY`/`OR` is outside full-key equality locking; mutation planning must change. |
| advisory lock | `SELECT pg_advisory_xact_lock($1)` | unsupported by documented surface | Schema/pointer serialization must be redesigned. |
| trigger function | `CREATE FUNCTION ... RETURNS trigger ... LANGUAGE plpgsql` | rejected | PL/pgSQL and trigger-maintained metrics/index summaries cannot install. |
| row trigger | `CREATE TRIGGER ... AFTER INSERT OR DELETE OR UPDATE ...` | rejected by documented migration model | Maintenance must move into explicit DML within transaction budgets. |
| index migration | `CREATE INDEX CONCURRENTLY ...` | rejected; use `CREATE INDEX ASYNC` | Migration lifecycle and readiness checks differ. |
| push DML | `INSERT ... VALUES ... ON CONFLICT ...` | syntax supported within limits | Still blocked by trigger/schema dependencies and 3,000-row/10 MiB transaction caps. |
| command/projection commit | multiple DML statements in one `REPEATABLE READ` transaction | supported in principle within limits; conflicts return `40001` | Requires complete idempotent retry and hot-key contention evidence. |
| rebuild chunk | repeated set-based `INSERT`/`UPDATE` in one transaction | supported only below limits | Rebuild must stage bounded transactions while withholding serving authority until exact completion. |

Suggested negative/positive probes:

```sql
-- Negative: ordered/range lock required by item claim.
SELECT item_id FROM fireweed_items
 WHERE tenant_id = 't' AND queue_id = 'q' AND lifecycle_state = 'Pending'
 ORDER BY priority_sort, created_seq LIMIT 10 FOR UPDATE SKIP LOCKED;

-- Positive syntax candidate: exact full-key equality.
SELECT assignment_epoch FROM fireweed_queue_owner
 WHERE tenant = 't' AND queue = 'q' FOR UPDATE;

-- Negative: current schema coordination/function surfaces.
SELECT pg_advisory_xact_lock(1);
CREATE FUNCTION fireweed_probe() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;

-- Limit probes in a disposable table.
-- Execute transactions at 2,999/3,000/3,001 mutated rows and just below/at/
-- above 10 MiB; record SQLSTATE and verify rejection has no durable effect.
```

No live probe is necessary to claim support. A future live run may confirm
negative SQLSTATEs and OCC retry behavior, but it cannot upgrade the verdict
without a new governed claim design.

## PRD and Architecture Consequences

| Authority | Consequence |
|---|---|
| PRD P0-5..8 | Batch mutation can use ordinary DML only after DSQL-specific row/byte admission and explicit maintenance replace triggers. |
| PRD P0-9..10 | Queue-global progress and read-after-success remain mandatory; DSQL cannot weaken them to accommodate OCC. |
| PRD P0-11..14 | ADR-008 whole-queue ownership remains unchanged; DSQL automatic partitioning does not become fireweed queue ownership. |
| PRD P0-15 | Retry storms on hot keys and fixed transaction limits require bounded backoff, admission, and same-run capacity evidence. |
| ADR-008 | No intra-queue split or scatter-gather claim is authorized. A DSQL claim redesign must remain owner-local and queue-whole. |
| ADR-013 | Durable log authority and log → serving projection → success ordering remain non-negotiable. Multi-transaction chunking cannot expose partial success. |
| TD-002 | The exact `postgres_native` claim, trigger, advisory-lock, and DDL design is not portable to DSQL. |

## Analysis

**Hypothesis**: confirmed at the backend-verdict level.

| Option | Evidence fit | Advantages | Risks | Confidence |
|---|---|---|---|---|
| Treat DSQL as `postgres_native` | conflicts with documented locking/function/trigger surface | minimal naming/config change | incorrect claim semantics; schema install failure; transaction-limit failures | high rejection |
| Create DSQL-specific backend with new claim algorithm | OCC and ordinary DML provide a possible substrate | may gain managed distributed scale | large design/conformance effort; hot-key conflicts; new admission/rebuild logic | medium feasibility |
| Reject DSQL for now | fully preserves accepted architecture and evidence | zero semantic dilution or premature adapter cost | forfeits DSQL deployment option | high |

Primary risks of a future DSQL backend:

- a CAS/reservation claim algorithm can satisfy safety but fail FR-9/FR-12
  progress under contention;
- transaction retries can duplicate external observation unless every mutation
  and response is replay-safe by `request_id`;
- explicit replacement of trigger work can exceed 3,000 rows or 10 MiB;
- automatic physical partitioning can be mistaken for ADR-008 logical ownership;
- AWS support and limits may change, requiring a dated recheck.

## Conclusions

**Primary conclusion**: `rejected` as the existing `postgres_native` backend.

Aurora DSQL cannot execute fireweed's accepted ordered `FOR UPDATE SKIP LOCKED`
claim algorithms, PL/pgSQL trigger schema, advisory-lock coordination, or index
DDL as written. Its OCC and hard transaction caps also require new retry,
admission, and contention proofs.

**Confidence**: high for rejection of direct compatibility; medium on eventual
feasibility of a distinct DSQL backend.

Limitations:

- no approved live DSQL environment was available;
- AWS documents supported subsets rather than an exhaustive unsupported-function
  catalog, so advisory-lock/trigger negative SQLSTATEs were not observed;
- no DSQL-specific alternative claim algorithm was designed or benchmarked.

## Recommendation

**RECOMMENDATION: reject Aurora DSQL as a `postgres_native` profile and make no
implementation or architecture change.**

Rationale: the incompatibilities hit the core concurrency and transaction
contract, not peripheral SQL syntax.

If DSQL is reconsidered, require a new proposed ADR and technical design that:

1. defines an OCC-native owner-local claim reservation/CAS algorithm preserving
   FR-9..12 and FR-23..35;
2. replaces trigger/advisory-lock behavior with explicit bounded transaction
   steps and proves 3,000-row/10 MiB compliance;
3. preserves ADR-008 queue-whole ownership and ADR-013 response ordering;
4. runs exact live probes plus full API-001 conformance and same-run contention
   evidence before any selectable backend profile exists.

This spike does not create that ADR, design, adapter, credentials,
infrastructure, or implementation work.

## Source Ledger

| Source | Date/version | Claim used | Confidence | Limitation |
|---|---|---|---|---|
| [AWS Supported SQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-sql-features.html) | retrieved 2026-07-27 | locking subset, SQL/TCL/DDL/DML support | high | list says non-exhaustive |
| [AWS Migration Guide](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html) | retrieved 2026-07-27 | fixed isolation, transaction rows, trigger/app-logic guidance, index syntax | high | migration guidance, not error catalog |
| [AWS Concurrency Control](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html) | retrieved 2026-07-27 | lock-free OCC, SQLSTATE 40001, retry guidance | high | performance depends on workload |
| [AWS Quotas and Limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html) | retrieved 2026-07-27 | 3,000 rows, 10 MiB write/message, 5-minute transaction | high | quotas may evolve |
| `crates/fireweed-postgres/src/relational.rs` | repository HEAD | exact claim, trigger, mutation, rebuild SQL | high | local implementation snapshot |
| `crates/fireweed-postgres/src/manifest_pointer.rs` | repository HEAD | advisory-lock schema initialization | high | local implementation snapshot |
| `docs/helix/02-design/technical-designs/TD-002-postgres-native-reference-mode.md` | repository HEAD | accepted backend semantics | high | DSQL is outside its scope |

## Artifacts

- This spike and its exact SQL probe matrix.
- No live resources, credentials, schemas, adapters, or generated implementation
  work.
