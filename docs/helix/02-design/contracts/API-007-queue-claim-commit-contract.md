---
ddx:
  id: api-queue-claim-commit-contract
  depends_on:
    - adr-postgres-baseline-queue-contract
  status: accepted
---

# API-007: Batch queue and state contract

Accepted by the project owner on 2026-10-10. This is the target interface,
not a claim that current adapters already conform. ADR-026 governs adoption.
It supersedes the unmerged API-006 kernel redesign and ADR-025 as interface
direction. There is no public begin/commit/rollback transaction object.

## 1. Interface and scope

One concrete asynchronous Fireweed handle, selected at construction, provides:

| Operation | Request | Result |
|---|---|---|
| `insert` | Queue, request id, `issued_at`, keyed work batch; insert-only or upsert mode | Input-correlated ids, versions and outcomes |
| `claim` | Queue, request id, `issued_at`, filter, priority order, eligibility time, lease duration, maximum count/bytes | Ordered work records and lease credentials |
| `commit` | Queue, batch of independently identified commit groups, each with `issued_at` | Input-correlated committed/rejected group receipts |
| `stats` | Queue, filter, requested grouping | Pending, leased, completed, failed counts, oldest eligible age and operational as_of |
| `read_state` | Queue, typed collection and keys | Input-correlated state values and versions |
| `query` | Queue, declared work/state index, filter, order, opaque cursor, limits | Bounded page of records and next cursor |
| `read_commit` | Queue and group request ids, each with the `issued_at` it was first issued with | Recorded outcomes, or explicit absent/expired outcomes. An id older than the retention horizon is `Expired` with no lookup |
| `preview` | Queue and administrative intent, including evaluated_at | Non-durable planned outcomes from the current covered snapshot; no identity retention |

Queue provisioning declares work fields, an explicit mandatory-indexed additional
field/property-bag policy where needed, state collections, indexes and the
retention policy (below) once.
Definitions retain declared unique indexes (including Snorri run/target identity).
Commit validates complete resulting unique tuples atomically, including rekeys;
missing components follow the existing NULLS DISTINCT rule. Definition drift is a deployment error. Construction and maintenance are not
queue hot-path operations. PostgreSQL and S3/Turso implement this same contract;
Snorri uses one adapter and never names a storage backend in its operation code.
No capability flag makes an operation in this table optional.

A claim group means work claimed for processing, not API-006 WholeGroups or a
new group-lock protocol. Lease credentials may be per item; callers treat them
as opaque. A commit group may consume several claims from the same queue.

### Retention policy and `issued_at`

Provisioning declares `retention` with no default:

- `request_id_nanos` is the replay window for insert, claim and commit request
  ids, measured from the request's `issued_at`. It must be greater than 0.
- `deleted_keys` is `Reusable` or `Retired { window_nanos }`. `Reusable` lets a
  deleted work key be inserted again at once, with a higher version. `Retired`
  returns `Rejection::AlreadyExists` from `insert` (both modes) and `work_inserts`
  for a key deleted within `window_nanos`; after that the key may be used again,
  with a higher version. `window_nanos` must be greater than 0 and at least
  `request_id_nanos` plus 5 minutes. Provisioning refuses anything less with
  `Invalid`.

Every insert, claim and commit group carries `issued_at`, UTC nanoseconds in the
unit of `eligible_at`, and it is covered by the request fingerprint.

- Retry rule: the caller takes `issued_at` once, when it creates a request id,
  stores it with that id, and resends both unchanged on every retry. The same id
  with a different `issued_at` is `RequestConflict` while its receipt exists. A
  caller that regenerates `issued_at` for a retry breaks this rule; the queue
  cannot detect it once the receipt is gone.
- Future skew: an `issued_at` more than 5 minutes ahead of the queue's clock is
  refused with `Invalid` before any lookup or effect, and the same id may be
  retried once the clock has caught up. The skew is fixed, not configurable. The
  queue's clock is the owner's clock on S3/Turso and the store clock on
  PostgreSQL. `preview` applies the same check as `commit`.
- Deletion time: a deleted work key's time is the `issued_at` of the commit group
  that deleted it, retained with the deleted row so a rebuild from the log
  reaches the same state. Under `Retired`, whether a reinsert falls inside the
  window is decided against the queue's clock when the inserting request is
  planned. The decision is logged as an ordinary outcome, so a rebuild never reads
  a clock.

Cleanup bounds what the policy retains. A declared policy is the authorization:
there is no cleanup method and no new error. The queue's owner (S3/Turso) and the
store (PostgreSQL) offer a pass in the background on a bounded schedule, at most
one a minute of the queue's clock, and a pass that fails costs no request.

- Horizon: a pass at time `at` advances the queue's horizon `H` to
  `at - request_id_nanos`, held back so that it never passes a claim whose lease,
  plus the 5 minute skew, has not ended. `H` never decreases. On S3/Turso `at` is
  carried by a logged `Retention` record, and applying it reads no clock, so every
  rebuild of the log reaches the same horizon and the same rows. On PostgreSQL
  `at` is the store clock inside the cleanup transaction, and `H` is stored with
  the queue.
- Requests older than the horizon: a request with `issued_at < H` returns
  `RequestExpired` with no lookup and no effect, for insert, claim and commit
  groups, and `read_commit` returns `Expired` for its id. An id at or after `H`
  still replays its recorded outcome, or conflicts if its body changed.

  | Case | Result |
  |---|---|
  | `issued_at` older than `H` | `RequestExpired`, with no lookup |
  | `issued_at` beyond `now + skew` | `Invalid`, and the same id may be retried later |
  | Same id, different `issued_at`, row present | `RequestConflict` |
  | Row removed | Only once `issued_at < H`, so nothing slips between removal and refusal |

- Rows removed: requests, receipts and claim records with `issued_at < H` are
  removed, and under `Retired` a deleted work row is removed once
  `deleted_at < at - window_nanos`. One pass removes at most 1000 rows, and a pass
  that removes that many is offered again as soon as the clock moves, until
  nothing is left. Under
  `Reusable` a deleted row is kept. Terminal work is never removed automatically;
  deleting it is a commit with `Delete`. The log itself is not truncated.
- Versions: each queue keeps a `version_floor`, the highest version of any row
  cleanup removed. A key with no row starts above it, so versions never repeat for
  a key even after its deleted row is gone.

## 2. Records and selection

Work has a stable caller key, version, priority, eligibility time, lifecycle,
declared and admitted additional typed fields, and opaque body. State has a collection, stable key, version,
declared query fields, and opaque value. Versions never repeat for a key, including across deletion and reinsertion
and after its deleted row is removed, using the queue version floor,
so stale CAS cannot match a replacement record (ABA). State is never claimable. Both are
first-class records with documented schemas, keyed reads, and indexed queries;
there is no consumer-facing side-record API or backend position.

Hot-path filters provide typed equality, membership, ranges and presence/absence
on declared or explicitly admitted indexed additional fields, plus key selection.
Administrative selection also supports All/Any/Not and intrinsic lifecycle,
attempt, lease-active-at-evaluated_at and due-time predicates. Full namespace/name
and typed values disambiguate every hash/prefix; no digest-only or schema-ordinal
identity limits arbitrary Snorri payload keys. Missing differs from present JSON
null, and no cross-type coercion is permitted. Snorri requires work-kind,
scheduled time, item/idempotency key, instance, tenant/run, artifact/version,
and lifecycle selectors. Order is declared priority or scheduled time with a
stable insertion-order tie break; direction is explicit. Queue provisioning
retains progress_bound_ms: the existing progress guard precedes nominal priority
within the matching eligible set when necessary to meet the numeric queue-global
bound. It never selects outside the filter. Preserve existing eligible-age anchors
and reclaim/retry rules; ordinary updates cannot reset age to evade the bound.
Stats oldest-eligible age uses those same eligibility and progress definitions. The same predicate
vocabulary serves claim, stats and query. Unsupported shapes fail validation
before effects; they never become client filtering or broad claims.

Claim atomically selects eligible matching unleased work and leases only that
selection. No unrelated work is leased and subsequently released. Concurrent
claims cannot grant two live leases on one item. Time eligibility is separate
from priority: future work is not selected merely because it ranks first.
On a quiescent queue, return the leading matching eligible records up to the
count/byte limit, subject to that explicit progress-guard precedence. Short results under contention are permitted; projection lag
alone is not permission to omit previously acknowledged eligible work.

## 3. Commit groups: the only atomic mutation boundary

Conceptual binding (DTO names may be refined without weakening semantics):

```text
CommitRequest { queue, groups: [CommitGroup] }
CommitGroup {
  request_id,
  claimed_updates: [{ key, lease, expected_version, patch, disposition }],
  state_updates: [{ collection, key, expect: Absent | Version(v), change: Put | Delete | Check }],
  work_inserts: [KeyedWork],
  work_updates: [{ key, expect: Version(v), change: Patch | Replace | Delete | Administrative }],
  administration: optional Addressed(targets) | SelectFirst(ordered_clauses),
  issued_at
}
```

Disposition is complete, fail, release, retry-at, or retain/renew the lease.
`Check` is also permitted: it verifies the claimed record's current version and
live credential at the operation's fresh lease time, with an empty patch and no
renewal. State `Check` verifies `Absent` or `Version(v)` without changing the row.
Checks share the group's atomic publication guard, count toward request and record
limits, and cannot overlap another operation on the same record. They produce no
version receipt, payload update, unique-index change or insertion sequence advance.
A checks-only group still publishes its immutable replay receipt; replay uses that
original outcome even after checked records or time have changed. A rejected check
does not by itself identify which predicate of a compound application condition failed.
An update may change permitted fields/body/priority/eligibility. Delete of active-leased
work requires its live credential. Patch/Delete reject an active lease; fresh Replace only accepts Pending work.
Administrative changes carry lifecycle intent Keep/Pending/Completed/Failed and
an explicit RejectActive/RequireActive/InvalidateActive lease guard. They are
version-fenced and atomic with state/audit changes, never transfer authority.
Terminal Complete/Fail intents reject Terminal; Pending explicitly rearms while
preserving attempts/insertion sequence. Administrative no-change preserves the
version. A transition to Pending or changed schedule resets its eligibility age
to max(now,due). These ordinary CAS updates cover version-fenced individual controls. The distinct
administrative intent below preserves actual whole-request control semantics.
Fresh Replace resets attempts/insertion/eligibility age and increments the
durable per-key version. It rejects terminal and stored-leased work; an ordinary
upsert cannot resurrect terminal work.
State collection schemas determine values and query fields; Fireweed does not
interpret workflow programs. Snorri validates its transition before calling.

Every group is all-or-nothing: state CAS, all accepted changes, new work and its
replay result publish together. For ordinary claimed/addressed CAS work, a failed precondition
changes no work or state in that group; its rejected outcome is durably retained
under the group identity for replay. Groups are independent, return positional
outcomes, and may partially succeed across the batch. There is no cross-queue
atomicity. An empty claimed_updates list is legal: insert, await consumption,
migration and recovery can use the same atomic group without manufacturing a
claim. `insert` is the convenient batch insertion binding to these semantics.

An optional server-resolved administrative intent has Addressed and ordered
SelectFirst forms. Addressed retains input order, missing targets and optional
expected versions. SelectFirst freezes first-clause ownership before preconditions;
a failed first clause never falls through. Per-row business no-op, missing, invalid,
version conflict, terminal, lease and predicate outcomes do not automatically reject
successful administrative sibling rows. They belong to one compact committed
whole-operation result with exact histograms and per-clause matched counts.
External state CAS and ordinary group failures still reject the whole group.
Zero targets/matches are valid intents with retained real-execution identities.
Resolution uses the group initial covered snapshot after prior successful groups;
own inserts cannot become targets. Overlap with explicit work/claimed changes or
inserts rejects the group before effects. Disjoint state/audit/new-work changes may
publish atomically with administration. Addressed controls can return compact
before lifecycle/lease/schedule/overlay metadata; opaque bodies need not hydrate.
Preview uses that same planner but bypasses receipt lookup, appends nothing and
retains no identity; it is explicitly not a durable CommitReceipt.

Envelope validation precedes all effects. Duplicate writes to one key within a
group, malformed credentials/values and excessive limits reject the envelope.
Groups may target the same work or state key. First execution evaluates overlapping groups
in request order against prior successful groups; rejected groups advance nothing.
Disjoint groups may execute concurrently. Replay resolves each existing identity
from its original receipt before evaluating any new identity, so regrouping and
reordering retried groups cannot change successful or rejected outcomes. Duplicate
group identities in one envelope are invalid. Independence means no batch-wide
rollback, not permission to reorder conflicting first executions.
The implementation performs genuinely batched storage work, not one round trip
per item. Packing/logging groups together never changes their independence.

## 4. Durability, visibility, replay and clocks

- Success means every reported effect and its replay receipt survives process
  loss. Acknowledged state and work must be recovered together, never partly.
- An operation issued after success observes those effects, including claim,
  read_state, query and stats. Stats totals, groups and bins share one covered
  per-queue snapshot; no cross-queue global snapshot is implied. Acknowledged
  prior writes cannot be hidden by apply debt.
  S3/Turso may apply asynchronously internally; it must wait for coverage or use
  an equally proven committed read path. Snorri never polls projection positions.
- Known rejection means definitely not applied. Infrastructure failure after
  admission may mean outcome unknown, explicitly distinguished from rejection.
  Retry the same request identity or read its receipt; never infer rollback.
  A native backend may internally prepare that same identity again only after
  a strictly newer durable fence and a projection caught up to that fence
  prove that the earlier identity is absent and cannot still publish. Receipt
  absence in an untrusted projection is insufficient. Duplicate authoritative
  identities or cache/history disagreement return outcome unknown. Conditional
  writes use one transport attempt; a lost write reply cannot become a known
  rejection through an SDK retry.
- Commit identity is per group, scoped to queue, fingerprinted over the canonical
  full body. Replay returns the original outcome without rechecking expired
  leases or duplicating effects. Rebatching unchanged groups preserves replay.
  Different body with the same id is a conflict.
- Receipts remain durable until the provisioned retention window passes. Cleanup
  advances a horizon; an id older than it is rejected as expired, with no retained
  marker, and never silently treated as new. Callers retry with the original
  `issued_at`. There is no implicit loss of migration/recovery dedup inside the
  window.
- Claim identity is durable and scoped to queue, with a canonical fingerprint
  covering filter, order, eligibility time, duration, count/byte limits and fields.
  Same id with any changed body is a conflict. Empty selection is retained and
  replays empty. A nonempty selection preserves its original selected keys and lease identity
  but renders their current committed records, versions and expiry, only while
  every selected item remains attached to a live original lease;
  partial settlement/release or expiry returns ClaimExpired, never new work.
  Renewal preserves identity and reports the current durable expiry on replay.
  A request issued before the claim horizon cannot issue another claim. The claim
  horizon never passes a live lease. Renewal is explicit and bounded.
- Dropping a future does not roll back admitted work. Storage owns completion
  and its resource charge until a terminal outcome; shutdown drains or safely
  resolves owned operations. Post-publication faults cannot become known rejection.
- Eligibility time is caller-supplied. Lease time advances independently of
  replay execution timestamps; ownership fencing uses an independent operational
  clock. Manual test clocks wake expiry processing. Stale holders fail immediately
  at expiry even if background reclamation has not yet materialized.

## 5. Bounds and performance

Requests and responses have explicit count and canonical-byte admission limits,
identical across implementations. Wire/envelope overflow rejects before any group begins. Data-dependent resolved
administrative publication overflow is a positional durable rejected TooLarge
outcome for that group, with no work/state effect in it and successful siblings
retained; never report a known envelope TooLarge after a sibling publishes. Preview
returns the equivalent non-durable rejection without partial successful counts.
Oversized groups cannot be split silently: atomicity belongs to the whole group. Large independent
batches may be chunked with stable per-group ids.

Do not impose API-006's 64 KiB field cap on Snorri's state/audit. The supported
Snorri envelope includes its current 1 MiB transition output and up to 512 effects
and 512 awaits, plus input settlement, state and audit bookkeeping. Current
instance projections may exceed one transition's output; their separate bound
must be measured and explicitly documented before activation, not inferred as
1 MiB or silently truncated. Exact request/response/group bounds are a required
implementation binding decision tested at and beyond each boundary.

Broad administrative selectors have no silent 4096-match cap. Their internal
planning/publication memory and bytes are bounded explicitly, with above-4096
coverage and actual consumer-size qualification before cutover. Stats numeric
bucket dimensions preserve request order, zero bins, missing/null bins and
out-of-range omission in one covered per-queue snapshot.

Claims use storage-side indexed selection and bounded memory. Reads and queries
are paged; no whole-queue hydration is required. Validation, DTOs, fingerprinting,
Snorri mapping and test scenarios are shared. SQL row locking/transactions and
object-log publication/apply/recovery remain implementation-specific.

Each S3/Turso queue has one owner at a time, by a lease held in the control
plane. The owner retains its queue position and fence for the tenure of that
lease, and keeps the queue's projection in a database of its own, one file per
owned queue in a directory used by that process alone. The owner opens that
database empty when its tenure begins and deletes it when the tenure ends; a
file left by an earlier process is deleted, never read. Because no two queues
share a projection writer, the batches of different queues proceed in parallel.
Mutations plan against the local projection inside one projection transaction
per owner iteration, so a later mutation sees the earlier ones. The iteration
publishes one record carrying every mutation's entry, conditionally on that
fence and the exact expected offset, and commits that transaction only once the
record is stored; mutations read no log records. Reads are served from the
projection and read no log records. The owner tracks a local lease deadline,
measured from before each lease request was sent, and stops serving at it
without contacting anything. A new owner serves nothing until the previous
lease's full duration plus a skew allowance has passed. After a passed deadline
or any publication that does not confirm success, the owner drops its projection
and leaves owner state. It may re-enter only through a new lease and a strictly
newer fence, rebuilding from the log, so an original that did publish is found
by its identity. Other handles return Busy without effects. PostgreSQL has no
owner limit, and callers treat Busy as retryable on both backends.

Throughput is settled useful work, not acknowledgments accumulating apply debt.
Compare PostgreSQL and S3/Turso on the same source, host, workload, sizes and
concurrency, reporting latency, throughput, memory, storage and object-request
cost. S3/Turso must demonstrate improvement before claiming an advantage;
correctness is mandatory independently of benchmark results. Existing 10M-item,
queue-density and bounded-progress obligations are not waived by this contract.

## 6. Mapping to actual Snorri requirements

| Snorri requirement | Contract mapping |
|---|---|
| Create/enroll work | Keyed insert/upsert batch with deterministic keys |
| Transition/effect/timer/facade selection | Server-side work-kind/key filters; scheduled/priority order and eligibility |
| TransitionCommitBatch | One commit group: settle input(s), CAS instance state, persist next state/audit/digest/input acknowledgment, insert effects/awaits/continuation |
| TransitionCommitBatchSet | Vector of independent groups retaining transition_request_id across rebatching |
| Instance concurrency | State CAS; retain expected_state_version and the adapter's separate projection revision where needed; no compulsory group-claim redesign |
| Await/external result consumption | CAS await state, update required instance fence/state version, insert continuation and result atomically |
| Dispatch and timeout | CAS dispatch/await bookkeeping and insert retry/continuation together, without falsely advancing workflow state_version |
| Timer firing | Settle timer, update await state and enqueue continuation atomically |
| Migration/recovery | Claimless CAS group with durable audit and replay identity |
| Instance/audit/await recovery | Typed keyed state reads and declared indexed queries; verify audit digest |
| Shard manifest bootstrap | Absent/version CAS on a named configuration state record |
| Counts/live controls | One-snapshot bucket/filtered stats; whole-operation Addressed/SelectFirst administration with per-row outcomes, exact replay and true preview; ordinary version-fenced CAS updates remain separate |

This preserves RuntimeStateStorePort's actual rule: committed transitions and
await consumption advance state_version; ordinary dispatch/timeouts/timer
bookkeeping do not independently do so. State and audit remain non-work data.
Snorri owns business semantics; Fireweed owns atomic persistence and leasing.
The [actual consumer integration binding](CONTRACT-snorri-integration.md)
is normative for arbitrary indexed payload equality, membership/eligibility and
precedence, administration/preview clocks, compact results/overflow, bucket stats
and versioned collision-free object-log address migration. Its source inventory
and reviewed sequence distinguish target requirements from currently implemented
Rust DTOs and adapters.

## 7. Persisted-data adoption

Fireweed is pre-release. There is no compatibility with data or formats written
by earlier builds: no legacy readers, shims, fallbacks or migration paths.
Persisted data in a superseded format is refused or unreadable, and any change
to a persisted format replaces the old one outright. Definition drift is never
silently accepted. Converting old opaque state storage to named collections is
not supported; use a new schema/queue.

## 8. One mandatory acceptance suite

Run these exact black-box scenarios through the public handle against both
PostgreSQL and real S3-compatible object storage/Turso. Missing fixtures fail the
qualification lane; skip, unsupported result or client-filter fallback is failure.
Separate backend fault hooks may differ; assertions and consumer calls do not.

| Named scenario | Required proof |
|---|---|
| InsertBatchIdentity | Stable keys, insert/upsert outcomes, duplicate/conflicting request ids |
| FilteredPriorityClaim | Mixed kinds/priorities/future schedules; only matching due items leased, exact order |
| ClaimIdentityReplay | Changed request body conflicts; empty selection, partial settle, renewal, expiry, process restart and cleanup preserve identity |
| VersionNeverReused | Delete/reinsert rejects stale state/work CAS and stale holder |
| OldFormatRefusal | Data in a superseded persisted format is refused with a typed error; no reader, migration or fallback exists |
| ConcurrentClaims | Concurrent claims through one owner are disjoint. A second handle receives Busy with no effects, or disjoint claims where the backend permits, and continues with disjoint work after handoff |
| ProgressUnderSkew | Sustained higher-priority arrivals and near-bound old work preserve numeric progress and age within filter; stats agrees |
| LeaseExpiryAndRenewal | Immediate stale-token rejection, bounded reclaim, explicit renewal, clock separation |
| CommitStateAndNextWork | Input settlement, state/audit, acknowledgment and emitted work all commit or all reject |
| CommitConflictNoEffects | Stale lease/state CAS/duplicate new key produces no group effects |
| IndependentGroupReplay | Mixed outcomes, lost response, regrouped replay, no duplicate effects or state advance |
| ReadAfterCommit | Paused/delayed applier cannot return stale successful reads or silently omit eligible claims |
| ProcessLossRecovery | Kill after durable commit before response; fresh process recovers exact state/work/receipt |
| StateIsNotWork | State/config/audit never appears in any work claim |
| AwaitRace | Concurrent deliveries consume once and insert one continuation; preserve Snorri state-version rules |
| AuditIntegrity | Tampered audit fails recovery rather than returning unverified state |
| CancellationShutdown | Lost caller, publication ambiguity, shutdown and ownership handoff preserve outcomes and bounded resource ownership |
| StatsAndQueryParity | Identical filters/counts/grouping/page order; bounded indexed reads |
| EnvelopeBoundaries | Current Snorri output/fanout/projection sizes; at/beyond byte and count limits; no partial oversized group |
| DurableReplayRetention | Old migration/recovery retry safe inside the window; past the horizon it returns expired, never reexecutes |
| RetentionHorizon | Cleanup advances the horizon: an older insert, claim or commit group is `RequestExpired` with no lookup and no effect; an id at the horizon replays; `read_commit` gives `Expired` for an older id and `Absent` for a newer one; the horizon never passes a live claim, and an expired claim yields no work; rebuild reaches the same horizon and rows; one pass removes at most its budget and retained rows stay flat under churn; a removed retired key returns above its old versions |

Run Snorri's shared StateStore conformance unchanged in business assertions on
both implementations, including scalar/batch transitions, external deliveries,
timers, effects, migrations, recovery and native durable process restart. Keep
main's formal/fault regressions, ownership fencing, authoritative rebuild and
stock-Turso schema/transaction serialization gates. New contract tests supplement
those protections; old implementation proof is not proof of a new storage path.

## Construction, module ownership and diagnostics

[Architecture](../architecture.md) owns modules and integration boundaries.
[Construction configuration](CONTRACT-construction-config.md) owns typed settings,
defaults, precedence, validation and secret redaction.
[Diagnostics](CONTRACT-diagnostics.md) owns safe structured events, OpenTelemetry
mapping, caller-owned export and bounded capture/retrieval. Instrumentation must
not alter durability, replay, selection, admission or atomic state semantics.
The [formal contract](CONTRACT-006-formal-queue-semantics.md) defines named
properties and the bounds on any executable assurance.

### Stats classification time

QueueStats.as_of is required signed Unix nanoseconds from the operational clock
sampled when the result is read from the owner's projection. It is the same sample used for lease
expiry classification across totals, groups and bins, independent of eligible_at.
It is not a projection watermark or durable sequence. The injected/system clock
may move backward; as_of records its actual sample rather than imposing monotonicity.
The complete live response budget includes this field. Consumer conversion and
negative-epoch UTC-hour grouping remain adapter cutover obligations.
