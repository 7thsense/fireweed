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
      to: api-queue-claim-commit-contract
---

This is the target design and its acceptance contract. The public PostgreSQL path is implemented; S3/Turso assembly and the single Snorri adapter cutover remain pending. PostgreSQL construction validates limits, pool size, parsed URLs and checked admission/scratch multipliers before connection or DDL, with credential-redaction and no-I/O negative tests. Typed custom capacities reach PostgreSQL admission and scratch execution. S3/Turso construction remains an implementation gate. Existing code is not qualified merely by this specification.


# Fireweed architecture

## Scope

One queue interface supports PostgreSQL as the durable baseline and S3 object-log
with stock Turso as the derived projection. API-007 governs all operations.
Workflow state and new work commit with claimed updates. Snorri has one adapter;
storage choice affects construction, durability mechanisms and measured cost.

## Level 1: System Context

| Element | Purpose | Protocol |
|---|---|---|
| Snorri and application hosts | Dispatch work; maintain workflow state, audit and follow-up work | One typed asynchronous queue interface |
| PostgreSQL | Atomic reference persistence and native indexed selection | Native PostgreSQL driver |
| S3 object-log | Authoritative ordered publication and writer fencing | Conditional object publication |
| Turso | Rebuildable indexed work/state/receipt projection | Stock embedded database |
| Host instrumentation | Safe diagnostic capture and export | Diagnostics Contract |

## Level 2: Container Diagram

| Container | Technology | Responsibility | Communication |
|---|---|---|---|
| Application/worker process | Rust 1.99, Tokio | Own queue construction, business workflow and bounded execution | Queue facade and caller-owned instrumentation |
| PostgreSQL baseline | PostgreSQL 16 | Atomically persist work, state, receipts, leases and versions | PostgreSQL SQL transactions |
| S3 persistence | AWS S3 or a qualified compatible endpoint; object-log | Durable checksummed publication and conditional authority | Object-log adapter |
| Derived projection | Stock Turso 0.8.2, ordinary WAL | Native filtering, exact committed progress, atomic apply and rebuild | Turso adapter |

## Module Boundaries

**Source Applicability**: source; handwritten Rust protocol, adapters and Python gates.

| Module | Responsibility / Owned Types | Public API | Allowed Dependencies | Forbidden Dependencies |
|---|---|---|---|---|
| `fireweed-core` | Queue values, schemas, limits, validation and fingerprints | API-007 DTOs | Serialization, bytes, hashing, pure schema libraries | Backend crates, SQL/object-store SDKs, facade and conformance |
| `fireweed-engine` | Pure planners, resolved deltas, wire codecs, admission invariants and internal ports | Backend-neutral internal queue port | Core and pure computational/protocol libraries | Facade, PostgreSQL/Turso/S3 drivers and persistence adapters |
| `fireweed-queue-spool` | Bounded scratch storage and whole-worker ownership | Internal admitted spool/worker primitives | Core, engine and local execution/filesystem primitives | Consumer business rules and backend SDKs |
| `fireweed-postgres` | Native baseline SQL, filtered locking, atomic persistence and recovery | Internal mandatory queue adapter | Core, engine, spool and PostgreSQL driver | Turso/S3 runtime and duplicated queue policy |
| `fireweed-turso` | Native projection, indexed selection, snapshot reads and atomic apply | Internal projection adapter | Core, engine, spool, relational translation and Turso SDK | Consumer workflow policy and object-log authority redefinition |
| `fireweed-objectlog` | Full-address namespace, bounded record transport, authoritative head/fence and publication | Internal log adapter | Core, engine, object-log/S3 integration and admitted workers | PostgreSQL/Turso SQL policy and consumer workflow rules |
| `fireweed` queue facade | Concrete construction and delegation; shared clone/shutdown ownership | API-007 and construction Contract | Internal mandatory port and selected adapters | Public vendor/session handles, optional ordinary-operation guarantees and direct database work in `queue.rs` |
| `fireweed-conformance` | Shared scenarios, workload generation and outcome oracles | Mandatory tests for both constructors | Core, engine and backend-neutral future combinators for owned workload cleanup; adapters only in test fixtures | Backend-dependent skipped semantics or weaker baseline assertions |
| Snorri adapter | Workflow mapping, definition pins, transition/await state versions | Snorri integration Contract | Concrete queue facade and Snorri domain | Storage SDKs, broad claim/client filter and consumer side-record APIs |

**Integration Owners**: PostgreSQL → `fireweed-postgres`; Turso → `fireweed-turso`;
S3/object-log → `fireweed-objectlog`; telemetry exporters → application host.
Drivers translate vendor representations and errors at those owning boundaries.

**Construction Policy**: `fireweed/src/queue.rs` wires a private mandatory port
behind one concrete handle. Hosts supply typed configuration, operational clock
and diagnostics. Core/planners do not discover environment or construct services.

**Boundary Check**: `python3 scripts/ci/check_queue_boundaries.py --controls`.
Local, pre-commit and CI use the same command. Production dependency cycles and
forbidden edges fail; supported allowed/forbidden controls exercise the actual
checker. Rust visibility protects private ownership. Semantic review additionally
checks invariant bypass, DTO/SDK leakage and policy duplication.

Existing legacy facade, alternate-backend and migration-only modules require
exact source/target exceptions with an owner and removal trigger. Exceptions
cover only recorded debt; new violations and expanded baselines fail. Test-only
adapter→facade edges may exercise public conformance; they do not permit a
production cycle. Remove legacy consumer exports after the single-adapter cutover.

## Deployment

| Component | Infrastructure | Instances / scaling | Backup / recovery |
|---|---|---|---|
| Baseline qualification | Isolated PostgreSQL 16 and Rust workers | One database; controlled concurrent worker count | Database WAL, physical process/server loss and reopen |
| Log/projection qualification | Qualified S3 endpoint and stock Turso files | One fenced writer per queue; bounded shared workers and connections | Exact original genesis and contiguous authoritative log replay |
| Production hosts | Host-selected process/container infrastructure | Scale queue owners under the measured capacity envelope | Durable authority survives worker loss; local projection is rebuildable |

Production availability/capacity claims require the actual endpoint, topology
and resource limits in the qualification record. No benchmark result transfers
from an unqualified provider or custom Turso fork.

## Data Flow

Insert durably publishes work and its request identity. Claim selects only the
matching eligible prefix in declared order and durably publishes those leases.
Commit validates current credentials/versions, atomically updates workflow/audit
state and new work, and publishes its immutable result. Reads and claims cover
acknowledged authority before returning. Projection lag is internal; application
code does not poll it to obtain the promised result.

## Quality Attributes

| Attribute | Target | Strategy | Verification |
|---|---|---|---|
| Durability | Zero loss of acknowledged work/state/receipts within the declared provider failure model | Atomic PostgreSQL persistence; conditional durable log publication | Real process loss, ambiguous replies, reopen and rebuild |
| Lease safety | Zero simultaneous valid leases per item; stale credentials have zero effects | Current token/expiry/version and writer-fence checks | Same mandatory suite plus adversarial interleavings |
| Atomicity | Zero partial effects in a rejected group | One atomic work/state/new-work/result boundary | Late failure and restart checks on both stores |
| Scale | 10M resident hot work; 1000 cold queues plus hot queue | Native indexed selection, bounded shared resources and bounded log metadata | Settled matched release workloads and density/progress cases |
| Performance | Published p50/p95/p99, useful settled rate, memory, drain debt and cost for the same workload | Comparable durable semantics and workload generator | PostgreSQL baseline versus S3/Turso; correctness gates rates |
| Diagnostic privacy | Zero secrets, tokens or raw work/state bodies in any sink | Allowlist before emission/capture/export | Traced/untraced receiver, redaction and outage controls |

## Decisions and Tradeoffs

ADR-026 establishes the simple atomic interface and PostgreSQL baseline.
A shared semantic planner avoids duplicated business rules; native SQL and log
mechanisms remain backend-owned. A derived Turso projection is replaceable cache
state, while S3 publication is authority. Both constructors must satisfy the same
observable guarantees. Formal analysis checks named properties under explicit
bounds and assumptions; it does not prove provider durability or Rust refinement.

### Scoped existing dependency exception

`fireweed-postgres` currently depends on `fireweed-objectlog` solely for
`src/async_objectlog_postgres.rs` and the legacy AsyncObjectLogPostgresBackend.
Owner: PostgreSQL adapter maintainer. Removal trigger: retire that composed
backend during API-007 cutover. The checker forbids this import in any new
PostgreSQL source file; the native queue store must remain independent of its
runtime. This exception does not permit PostgreSQL→Turso or direct S3 SDK edges.
