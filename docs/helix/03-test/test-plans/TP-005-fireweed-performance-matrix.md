---
ddx:
  id: tp-fireweed-performance-matrix
  depends_on:
    - prd
    - api-fireweed-rust-facade
    - tp-fireweed-facade-and-snorri-acceptance
    - adr-cqrs-log-projection-storage-model
    - adr-orthogonal-log-projection-composition
    - td-storage-architecture-backend-contracts
    - tp-scale-substantiation
  status: accepted
---

# TP-005: Fireweed performance matrix

## Testing strategy

**Goals**: Produce a reproducible, revision-bound comparative performance
profile of every supported Fireweed embedding storage composition on one
declared client/service topology;
measure the public facade rather than internal storage ports; preserve raw
samples and correctness evidence; distinguish comparable common-path results
from configuration-specific recovery and maintenance results.

**Out of scope**: Cloud-provider certification;
multi-node scale-out, which remains governed by TP-002 E2; extrapolation from
this host to another host; GitHub-hosted performance execution.

Performance qualification has a hard functional prerequisite. A matrix cell
MUST first pass TP-004's complete public-operation conformance suite and its
documented durability verification with zero skips and zero
construction-dependent `EngineError::Unavailable` results. An incomplete cell
is release-blocking and ineligible for performance execution; the runner MUST
NOT spend benchmark time characterizing it.

**Traceability source**: PRD scale substantiation; ADR-001 storage profiles;
ADR-012 orthogonal log/projection composition; API-005 construction boundary;
TD-001 backend contract; TP-002 capacity and scale evidence.

Performance qualification runs on controlled operator-selected hardware.
GitHub Actions may compile the runner, execute deterministic verifier tests, and
verify a checked-in evidence artifact. It MUST NOT produce authoritative timing
evidence. The runner rejects an authoritative run when a conventional CI
environment variable is present unless the operator selects a non-authoritative
smoke tier.

TP-005 evidence is never TP-002 E0, E1, E2, or E3 evidence. The schema rejects
`tp002_evidence_ids`, and TP-005 artifacts MUST NOT be included in a TP-002
governed bundle. TP-005 measures a single-caller embedding baseline at bounded
workload size; it does not satisfy TP-002 resident-scale, scale-out, cost,
recovery, or exact-tag release requirements.

### Evidence classes and claim boundary

| Class | Purpose | Comparable across cells | Release claim |
| --- | --- | --- | --- |
| `common` | Public facade append, claim, and finalize | Only within one response-barrier class and one run | Host/topology-bound embedding profile |
| `recovery` | Close, reopen, rebuild, and verify durable state | Only among cells with the same recovery contract | Host-bound recovery profile |
| `maintenance` | Verify, delete, and rebuild disposable projections | Only among cells exposing `projection_control()` | Host-bound maintenance profile |
| `smoke` | Fast runner and schema validation | No | None |
| `million-cycle-v1` | Insert 1M, modify 500K, read and verify 1M through every supported constructor | No; each constructor is gated independently | P0 per-constructor functionality and host-bound performance gate |

### Targeted million-item lifecycle gate

`million-cycle-v1` is a fail-closed gate, not a comparative benchmark. For
every supported matrix cell it performs the same public-facade lifecycle:

1. Create one queue with a maximum push batch of 1,000.
2. Insert exactly 1,000,000 deterministic items with unique
   `ClientItemKey`s, in batches of 1,000. The timed insert phase MUST complete
   in at most 9 seconds.
3. Modify exactly the first 500,000 items using API-001 `BatchUpdate`, in
   batches of 1,000. Every outcome MUST be `Updated`; the timed modify phase
   MUST complete in at most 9 seconds.
4. Read exactly 1,000,000 items using `live_items` in key-order batches of
   1,000. The timed read phase includes value/version verification and MUST
   complete in at most 9 seconds.
5. Verify an exact deterministic digest: 500,000 updated rows at version 2 and
   500,000 untouched rows at version 1. Missing, duplicate, reordered,
   incorrectly updated, or extra rows fail.
6. For every durable constructor, close and reopen after modification, then
   repeat the exact read/digest verification outside the timed phase. Volatile
   memory records `durability=volatile`; every other cell must record and prove
   its documented durability boundary.

Queue creation, one bounded 10,000-item warmup, close/reopen, service setup,
and cleanup are outside phase timings and recorded separately. Item/request
construction remains inside each timed phase because the gate represents the
caller-observed public operation. `EngineError::Unavailable`, an omitted cell,
a skip, or a reduced item count is a P0 failure. The authoritative gate runs on
controlled local hardware and is forbidden in CI.

### Matrix

Each cell has a stable identifier. `required` means failure or omission makes a
full local run fail. `conditional` means the cell is required when its declared
service configuration is supplied; otherwise the result is `not_configured`,
never `pass` or a silent skip. These requirement states describe infrastructure
availability only; they never excuse missing Fireweed functionality.

| Cell | Construction | Authority | Projection / barrier | Requirement |
| --- | --- | --- | --- | --- |
| `memory` | `open_memory` | memory | in-memory / atomic | required |
| `sqlite-log` | `open_sqlite` | SQLite command log | in-memory replay | required |
| `sqlite-relational` | `open_sqlite_relational` | SQLite relational | relational / atomic | required |
| `postgres-log` | `open_postgres_runtime(LogReplay)` | PostgreSQL command log | in-memory replay | conditional on PostgreSQL |
| `postgres-relational` | `open_postgres_runtime(Relational)` | PostgreSQL relational | relational / atomic | conditional on PostgreSQL |
| `objectlog-local-direct` | `open_objectlog` | local filesystem object log | durable object-log state visible | required direct object-log baseline |
| `objectlog-local-sqlite-strict` | `open_objectlog_sqlite` | local filesystem object log | SQLite / strict | required |
| `objectlog-local-sqlite-async` | `open_objectlog_sqlite` | local filesystem object log | SQLite / authority + hot view visible; SQLite deferred | required diagnostic; not release-promotable |
| `objectlog-local-postgres-strict` | `open_objectlog_postgres` | local filesystem object log | PostgreSQL / strict | conditional on PostgreSQL |
| `objectlog-s3-sqlite-strict` | `open_objectlog_sqlite` | S3-compatible object log | SQLite / strict | conditional on S3 |
| `objectlog-s3-sqlite-async` | `open_objectlog_sqlite` | S3-compatible object log | SQLite / authority + hot view visible; SQLite deferred | conditional diagnostic; not release-promotable |
| `objectlog-s3-postgres-strict` | `open_objectlog_postgres` | S3-compatible object log | PostgreSQL / strict | conditional on S3 and PostgreSQL |
| `objectlog-s3-memory` | none | S3-compatible object log | in-memory | unsupported by API-005 |
| `objectlog-*-postgres-async` | none | object log | PostgreSQL / async projection | unsupported by API-005 |

The matrix records unsupported rows in its manifest but never fabricates timing
samples for them. Composed local and S3-compatible object-log cells use the
same segmented-log target and maximum-latency settings. API-005 specifies the
direct `open_objectlog` constructor separately; it exposes no segment settings,
is reported as its own public-API baseline, and is not compared with composed
segmented object-log cells.

### Response-barrier classes

The runner assigns every result to exactly one class. It prints values from all
classes together but computes comparative verdicts only inside a class.

| Class | Cells | Success boundary |
| --- | --- | --- |
| `volatile-visible` | `memory` | Volatile state visible |
| `local-durable-visible` | `sqlite-log`, `sqlite-relational` | Local durable authority and serving view visible |
| `postgres-durable-visible` | `postgres-log`, `postgres-relational` | PostgreSQL durable authority and serving view visible |
| `objectlog-durable-visible` | `objectlog-local-direct` | Direct local object-log commit visible |
| `objectlog-hot-visible` | `objectlog-*-sqlite-async` | Segmented object-log authority and synchronous hot view visible; disposable SQLite may lag |
| `objectlog-projection-visible` | `objectlog-*-sqlite-strict`, `objectlog-*-postgres-strict` | Object-log authority and durable serving projection visible |

Cross-class ratios are descriptive only and carry `comparison_status =
"different_success_boundary"`. Async rows report the timed response boundary
and a separate untimed catch-up duration; the two values are never added and
never presented as a strict-barrier equivalent. Async rows remain diagnostic
until ADR-012's release blockers are independently closed.

### Exact construction contract

The runner uses `futures::executor::block_on` on one caller thread and the
synchronous API-005 constructors. It MUST NOT create a Tokio runtime. Async-safe
constructor variants are equivalent construction-entry alternatives, not
storage cells; TP-004 owns their compile/semantic coverage. Coordinated and
multi-node construction changes deployment topology and remains under TP-002,
not this single-owner embedding matrix.

All `PostgresRuntimeConfig` cells set `schema` to a unique run-owned value,
`node_id = None`, and `coordination = None`. Mode is exactly `LogReplay` or
`Relational` as named by the cell.

All composed object-log cells except `objectlog-local-direct` use:

```text
SegmentConfig::new(262144, 20)
RecoveryPolicy {
  incompatible_projection: RecoveryAction::RebuildProjection,
  verify_checksums: true,
  max_tail_commands: 1000000,
}
```

`ObjectLogStorage`, `ProjectionConfig`, and `ResponseBarrier` match the matrix
row exactly. The direct object-log cell records the fixed behavior of the
specified `open_objectlog` constructor; it has no synthetic `SegmentConfig` and
no pairwise comparator. Every local root, SQLite projection path, PostgreSQL schema, and
object-log namespace is derived from the run ID, cell ID, shape ID, and
repetition. The full resolved non-secret configuration is serialized with the
row. A mismatch between the resolved config and the cell definition fails
before warm-up.

### Test levels

| Level | Coverage target | Priority |
| --- | --- | --- |
| Contract | 100% of matrix cell IDs, evidence fields, and failure semantics | P0 |
| Integration | Every configured cell constructs through API-005 and reconciles exact state | P0 |
| Performance | Five measured repetitions per common-path workload after warm-up | P0 |
| Targeted lifecycle | Exact 1M/500K/1M cycle and three 9-second phase ceilings on every supported constructor | P0 |
| Recovery | Every durable configured cell reopens and reconciles exact state | P0 |
| Maintenance | Every configured disposable-projection cell verifies and rebuilds | P0 |
| Smoke | One small repetition over local cells for developer feedback | P1 |

### Frameworks

| Type | Framework | Reason |
| --- | --- | --- |
| Contract | Rust unit tests plus JSON Schema-style semantic verifier | Rejects missing, duplicate, malformed, or contradictory evidence |
| Integration | `fireweed-bench` independent Cargo workspace | Isolates timing from the root workspace and exercises the public facade |
| Performance | Monotonic `Instant` samples emitted as JSON | Retains auditable raw measurements without a hidden statistical framework |
| External services | Operator-provided PostgreSQL and S3-compatible endpoints | Measures real configured services on controlled hardware |

## Test data

All generated items are deterministic from the recorded seed. No customer data
or production identifiers are used.

| Shape | Payload | Fields | Grouping | Priority | Items | Batch | Requests/op/rep |
| --- | ---: | --- | --- | --- | ---: | ---: | ---: |
| `minimal` | 0 B | none | none | sequential | 12,800 | 128 | 100 |
| `record-1k` | 1 KiB | 16 × 64 B | none | deterministic uniform | 12,800 | 128 | 100 |
| `group-keyed-256` | 256 B | 4 × 32 B | 64 group keys | deterministic uniform | 12,800 | 128 | 100 |
| `large-16k` | 16 KiB | none | none | deterministic uniform | 1,600 | 16 | 100 |

The authoritative full tier uses concurrency 1, one unmeasured warm-up
repetition, and five measured repetitions. Each operation therefore retains
100 request samples per repetition and 500 pooled samples per cell/shape.
This is a deliberate embedding baseline: it measures one caller driving one
queue without claiming saturation throughput. A later saturation profile may
vary concurrency, but it must use a different profile ID and may not replace
this baseline.

Each repetition uses a fresh queue and fresh isolated storage namespace. The
execution schedule is fixed: warm-up visits shapes in table order and cells in
stable cell-ID order; measured rounds are outermost, shapes remain in table
order, and for shape index `s` and round `r` the stable cell list rotates left
by `(r + s) mod cell_count`, then reverses when `r` is odd. Warm-up storage is
cleaned and verified before measured round zero. The exact schedule is recorded.
The runner records every request duration in nanoseconds, not only precomputed
percentiles.

## Coverage requirements

| Metric | Target | Minimum | Enforcement |
| --- | --- | --- | --- |
| Full-tier matrix cells completed | Every supported cell whose services are required by the tier | 100% | Runner exits non-zero |
| Common operations per cell/shape | append, claim, finalize | 100% | Semantic verifier |
| Accepted/claimed/finalized reconciliation | exact | exact | Runner and verifier |
| Measured repetitions | 5 | 5 | Semantic verifier |
| Raw request samples | exactly 100 per common operation/repetition | 100 | Semantic verifier |
| Pooled raw request samples | exactly 500 per common cell/shape/operation | 500 | Semantic verifier |
| Cross-repetition throughput CV | population CV reported | finite, mean > 0 | Semantic verifier |
| Environment provenance | complete required fields | 100% | Semantic verifier |
| Source provenance | clean pushed commit | exact | Launch wrapper and verifier |
| Secret leakage | zero credential values | zero | Redaction test and evidence scan |
| Million-cycle functionality | insert + `batch_update` + `live_items` on every supported cell | 100%, zero `Unavailable` | Runner exits non-zero |
| Million-cycle phase ceilings | insert ≤9 s; modify ≤9 s; read+verify ≤9 s | all three per cell | Runner and semantic verifier |
| Million-cycle durable reopen | exact final digest after close/reopen for every durable cell | 100% | Runner and semantic verifier |

### Common-path protocol

For every configured cell, shape, and repetition:

1. Construct a fresh `Fireweed` through the public API-005 constructor.
2. Create one queue using the versioned `matrix_queue_definition_v1` constant:
   integer ascending priority, created-sequence tie break, strict ordering,
   `max_rank_error=0`, default eligibility and recurrence, no cohort, 60,000 ms
   progress/request-id/client-key/terminal retention, 3,600,000 ms maximum
   lease, 1,000,000 retry attempts, 1,000,000 push/claim batch maxima, no group
   maximum, no indexes or entity schema, and change-record emission disabled.
   Read the persisted definition back and require exact equality.
3. Append the shape's exact population using
   `push_batch_with_request_id`, one deterministic unique request ID per batch,
   and retain every request duration.
4. Claim using `claim_response_with(max=batch, lease_ms=3_600_000,
   ClaimCompatibility::default())`; finalize the returned item IDs using
   `ack`. Claim and finalize durations are recorded separately.
5. Assert every claim before exhaustion returns exactly `batch` items (all
   authoritative counts divide exactly by batch), accepted = claimed =
   finalized, accepted and claimed item IDs are unique and equal,
   claim order matches priority then created-sequence order, lease tokens are
   non-empty, and final queue metrics show zero pending and zero leased items.
6. Record throughput per operation from total items divided by the enclosing
   monotonic operation interval. The interval contains only the 100 sequential
   public method calls; post-operation assertions are outside it.

Setup, schema creation, warm-up, recovery, cleanup, and correctness scans are
outside common-operation timing. Allocation and serialization performed by the
public call path remain inside request timing.

Before timing, each cell runs an untimed conformance preflight: persisted queue
definition equality; replay of one `push_batch_with_request_id` returns the
same item IDs without duplicate effects; conflicting reuse of that request ID
is rejected; when `commit_capabilities().lease_validation` is true, one claimed
item rejects a fabricated stale lease token through `commit`; otherwise that
check is explicitly `not_applicable`; read-after-success sees the accepted
state. A required preflight failure blocks every timing row for that cell.

### Repetition and summary policy

- One warm-up repetition runs first for each cell and shape and is discarded.
- Five measured repetitions run with stable seed-derived identifiers.
- The artifact retains per-request samples and per-repetition totals.
- Nearest-rank percentile `p` sorts `N` integer nanosecond samples ascending and
  selects one-indexed rank `ceil(p*N)`, clamped to `1..N`; ties require no
  special handling.
- The summary pools all 500 request samples and reports pooled p50/p95/p99.
  It also reports median/minimum/maximum repetition throughput.
- Population CV is `sqrt(sum((x-mean)^2)/N)/mean` over five successful,
  positive repetition-throughput values. Zero, non-finite, or missing values
  invalidate the row; failures are never included in the denominator.
- No samples are trimmed. Failed repetitions remain recorded as failures and
  make the run incomplete.
- Comparisons are valid only between rows sharing run ID, host fingerprint,
  response-barrier class, profile, shape, item count, batch size, operation, and
  repetition schedule.
- Pairwise comparison status is `material` only when at least four of five
  same-round throughput ratios point in the same direction, the median ratio is
  at least 1.10 in that direction, and both rows have CV <= 0.15. Otherwise it
  is `inconclusive`; this deterministic label is not a significance claim.
- Results establish capacity on the declared topology. They do not become a
  portable pass bar merely because one cell is faster than another.

### Recovery, async catch-up, and maintenance protocol

Every durable cell receives three recovery repetitions for both `minimal` and
`record-1k`, using the shape's authoritative item count. The runner closes the
handle after append, reopens the same authority/projection coordinates, times
the constructor call through its successful return (the public ready boundary),
and verifies exact pending identity and a complete duplicate-free drain.
Recovery results report all three raw durations plus median/min/max; no p95 or
p99 is claimed from three samples. Recovery includes payload, fields, priority,
and item identity for `record-1k`, not counts alone.

For an async cell, recovery setup first reaches and verifies exact public
projection catch-up before closing; reopening therefore measures a known
durable checkpoint rather than unbounded pre-close projection debt.

After each async common repetition, the runner polls public projection
`verify()` with a 60-second monotonic bound until `compatible=true` and
`projection_sequence == authoritative_sequence`. It records response-end
sequence, catch-up duration, final sequences, poll count, and typed
backpressure/errors. Timeout or non-convergence fails the repetition. This is a
catch-up observation, not ADR-012 debt/lineage release evidence.

For each `objectlog-{local,s3}-{sqlite,postgres}-*` cell, the runner additionally
obtains `projection_control()`, records `capabilities()`, and invokes only
operations whose capability bit is true. Three maintenance repetitions each
begin with a fresh 12,800-item `record-1k` population and exact pending identity
verification, delete only the run-owned disposable projection, rebuild from the
authoritative log, record raw durations, verify the same complete pending
identity afterward, and drain it exactly. A
capability required by the cell contract but absent is a cell failure; an
operation not promised for that cell is `not_applicable`. Projection maintenance
is never included in common-path latency.

Memory records `not_applicable` for recovery. DB-resident relational profiles
record reopen rather than replay. Unsupported maintenance capabilities record
`not_applicable`, not zero-duration success.

### External-service isolation and tier semantics

`smoke` runs only memory, SQLite, and local object-log cells, only the `minimal`
shape, 512 items, batch 64, one warm-up, and one measured repetition. Its
verifier expects exactly eight samples per operation and never applies the full
tier's repetition or sample counts. It is always non-authoritative. `full`
requires both PostgreSQL and S3 configuration and every supported matrix cell;
missing configuration exits 64 before creating storage. A supplied but
unreachable service is `failed`, never `not_configured`. A future `local` tier
may omit external cells but cannot call itself full.

The full run executes the client and local filesystems on this machine, an
isolated PostgreSQL database reachable from this machine, and Garage on
`eldir`. This differs from TP-004's acceptance-test placement and makes no
claim about performance on the Garage host itself.

PostgreSQL configuration is accepted only when
`FIREWEED_PERF_POSTGRES_DATABASE_ACK` exactly equals the parsed database name,
the database name begins `fireweed`, and it is not `postgres`, `template0`, or
`template1`. Plain PostgreSQL cells use enumerated
`fireweed_perf_<run-id>_*` schemas. Object-log/PostgreSQL cells use the facade's
exact derived schema: `pq_` plus the first 60 lowercase hex characters of
SHA-256 over the logical object-log namespace. Before construction the runner
places both kinds in an immutable cleanup allowlist; cleanup accepts exact
allowlist membership only, never a wildcard or caller-supplied physical schema.
A PostgreSQL advisory lock derived from `fireweed-performance-matrix-v1`, held
by one dedicated connection for the entire orchestration, prevents concurrent
matrix runs. Normal completion, error, panic, SIGINT, and SIGTERM attempt schema
cleanup and lock release; failed cleanup makes the artifact `failed`. SIGKILL
cannot be cleaned automatically and is reported by the next lock/prefix audit.

Every logical S3 namespace is below
`fireweed-perf/v1/<commit12>/<run-id>/<cell>/<shape>/<phase>/rNN`. The facade's
physical prefix is lowercase hex of the logical namespace's UTF-8 bytes followed
by `/`. Cleanup accepts only a logical namespace from the immutable run
allowlist, derives the physical prefix internally, and refuses an empty prefix,
`/`, a logical namespace outside `fireweed-perf/v1/`, or a bucket
name not exactly acknowledged by `FIREWEED_PERF_S3_BUCKET_ACK`. Cleanup lists
and deletes only the exact run-owned prefix, relists it, and requires zero
remaining keys. Bucket-root list/delete and deletion of a sibling run prefix are
impossible through the cleanup type. A harness-owned raw-S3 create-if-absent
lock object at
`fireweed-perf/v1/_locks/matrix.lock` prevents concurrent runs; its payload
contains the run ID, commit, and start time. A live lock blocks. Stale-lock
removal is a separate explicit operator action and is not implemented by the
benchmark.

All local paths live below a newly created run root. Canonicalization must prove
each cleanup target remains below that root before deletion. Run-owned cleanup
guards execute on ordinary errors and unwinding. The evidence artifact records
cleanup of every namespace; any incomplete cleanup makes the full run fail.

## Evidence contract

The runner writes one canonical JSON document and a SHA-256 sidecar. The
document contains:

- schema version, run ID, tier, status, start/end UTC timestamps, and command;
- full 40-character Git commit, branch, sanitized remote URL, fetched remote
  ref and commit, submodule state, and source-affecting-clean assertion;
- OS, kernel, architecture, hashed host fingerprint, CPU model/count, total
  memory, virtualization/container signal, filesystem type/mount, free space,
  CPU governor/turbo data when available, load average, Rust and Cargo versions,
  exact enabled features/rustflags, build profile, and benchmark lockfile hash;
- profile parameters, seed, cell order per repetition, shape definitions, and
  redacted service topology;
- every supported, configured, not-configured, unsupported, passed, and failed
  cell with a reason;
- raw request durations, per-repetition totals, derived summaries, exact
  reconciliation counts, recovery results, and maintenance results;
- PostgreSQL server version and durability settings when configured;
- S3 endpoint scheme, hashed endpoint host and bucket, region, provider label,
  and preflight RTT samples when configured; access and secret keys are omitted;
- cleanup status for every run-owned local path, database schema, and object
  namespace.

The verifier recomputes percentiles, summaries, CVs, comparison labels,
reconciliation, schedule, matrix completeness, and the sidecar digest. Unknown
schema fields fail closed within a schema version; historical artifacts remain
verifiable by their versioned verifier. `tp002_evidence_ids` is forbidden.
Credential-value fields and configured credential values are rejected anywhere
in serialized evidence; documented environment variable names may appear only
in the specification, never in recorded command arguments. Before printing or serializing, errors pass
through a redactor seeded with the PostgreSQL URL, S3 access key, and S3 secret;
the wrapper never enables shell tracing. The invoking command records variable
names, never values. Remote URLs remove user information and query strings.

Before an authoritative run, the wrapper executes `git fetch origin main`,
requires `HEAD == refs/remotes/origin/main`, requires no staged or tracked
changes, and rejects untracked files under `docs/`, `crates/`, `scripts/`,
`.cargo/`, or any Cargo/toolchain manifest path. Other untracked paths are
represented only by content hashes in provenance; they cannot alter the
benchmark or its governing specification. Submodules, when present, must be initialized,
clean, and at recorded commits. The runner records the fetched ref and remote
commit after these checks. The evidence file itself is added after execution
and may be committed separately. Re-running the same commit creates a new run
ID; evidence is append-only and is never silently overwritten.

The full orchestrator has a four-hour run timeout and a 15-minute timeout per
cell/shape/round fragment. It writes an atomic checkpoint after every fragment.
`--resume` accepts a fragment only when its digest, source commit, run ID,
resolved non-secret configuration, schedule, and verifier result all match;
otherwise it cleans that fragment's exact allowlisted namespace and reruns it.
A public call that reaches its configured progress bound or returns typed
backpressure fails the fragment and remains in evidence. The progress bound is
not represented as a retention setting. A normal full run is budgeted for
30--120 minutes on the declared topology; exceeding that estimate is diagnostic,
while exceeding a hard timeout fails the run.

Before any authoritative timing, the same pushed commit must pass
`cargo test -p fireweed-conformance --all-features` and the benchmark crate's
locked test suite. Commands, exit status, duration, and SHA-256 of captured
output are recorded; raw output is retained separately after value redaction.

## Acceptance criteria layer allocation

| Requirement source | Primary layer | Blocking evidence |
| --- | --- | --- |
| API-005 opaque facade | Contract/integration | All cells are constructed publicly and timed through `Fireweed` |
| ADR-001 selectable profiles | Performance matrix | Every supported composition is classified and configured cells execute |
| ADR-012 orthogonal composition | Matrix completeness | Public embedding log, projection, and barrier choices are explicit; control-plane/topology variants remain TP-002 scope |
| TD-001 shared semantics | Common protocol | Exact accepted/claimed/finalized reconciliation for every row |
| TP-002 evidence honesty | Evidence verifier | Host-bound claims, exact revision, raw samples, and no silent skips |
| TP-004 Garage boundary | External integration | Garage cells run when S3 and PostgreSQL configuration is supplied |

## Implementation order

1. Define serializable evidence types and a verifier with malformed-fixture
   tests.
2. Add a dedicated matrix binary/module without changing the TP-002 benchmark
   suites or their evidence format. The timed SUT module may import only the
   public `fireweed` crate; service cleanup/provenance code is separate and
   excluded from timed intervals.
3. Implement stable cell construction, fresh namespace allocation, service
   locks, signal-aware cleanup guards, and cleanup audits.
4. Implement warm-up, rotated repetition scheduling, common operations,
   recovery, and maintenance.
5. Add the clean-pushed-revision launch wrapper and secret-safe environment
   mapping.
6. Run smoke locally, then the full configured matrix on the pushed revision.

## Infrastructure

| Requirement | Specification |
| --- | --- |
| Execution host | This operator-selected client machine plus the declared Garage service host; never a GitHub-hosted runner |
| Rust | Repository-pinned toolchain; `cargo run --release --locked`; exact features recorded |
| PostgreSQL | PostgreSQL 16+ dedicated database acknowledged by `FIREWEED_PERF_POSTGRES_DATABASE_ACK`; URL supplied by `FIREWEED_PERF_POSTGRES_URL` |
| Object storage | Garage on `eldir`; endpoint/bucket/region and credentials supplied by `FIREWEED_PERF_S3_*`; bucket separately acknowledged |
| Local storage | Run-owned directory under an explicit work root on the measured filesystem |
| Output | `docs/perf/matrix-results/<commit>-<run-id>.json` plus `.sha256` |
| CI | Compile and verifier tests only; timing evidence from CI is non-authoritative |

## Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Thermal or background-load drift | Comparisons biased by order | Warm-up, five repetitions, rotated cell order, raw samples, CV reporting |
| Remote Garage network variance | S3 tails obscure implementation changes | Record hashed topology and RTT; retain raw samples; compare only within run/class |
| PostgreSQL residue or cross-run contention | Later cells become incomparable | Unique schemas, isolated database, explicit cleanup status |
| Object namespace residue | Cost and list performance drift | Unique prefix per repetition and verified prefix cleanup |
| Async projection reports success before catch-up | False reconciliation | Report response timing separately; bounded public verification catch-up blocks correctness |
| Generic harness hides incomplete functionality | Misleading timing evidence | TP-004 functional and durability gates block the cell before any timed work |
| Secrets reach evidence or logs | Credential disclosure | Redacted configuration types, field-name denylist, value scan before write |
| Benchmark becomes a release bottleneck | Tests are avoided or run on CI | Separate smoke/full tiers; controlled-host execution; verifier remains fast |

**Known gaps**: This baseline is single-caller and does not establish saturation
capacity, multi-client fairness, multi-node scaling, or cloud-provider durability.
Those require distinct profile IDs or remain governed by TP-002. The current
public facade does not offer S3 object log plus in-memory projection or async
PostgreSQL projection; the manifest records those boundaries.

## Build handoff

**Commands**:

```sh
cargo test --manifest-path crates/fireweed-bench/Cargo.toml --locked
scripts/perf/fireweed-matrix.sh --tier smoke
set -a
source <operator-managed-performance-env-file>
set +a
scripts/perf/fireweed-matrix.sh --tier full \
  --output docs/perf/matrix-results
scripts/perf/verify-fireweed-matrix.sh \
  docs/perf/matrix-results/<commit>-<run-id>.json
```

**Priority**: Evidence contract and verifier first; public-facade common path;
local cells; PostgreSQL cells; Garage cells; recovery and maintenance.

**Blocking gate**: The full matrix refuses missing PostgreSQL or S3
configuration; every supported cell executes; exact reconciliation, async
catch-up, recovery, maintenance, and cleanup pass; the verifier independently
recomputes the artifact and comparison labels; the sidecar matches; and no
credential value is present.

## Review checklist

- [ ] Every API-005-supported storage/projection/barrier composition is present.
- [ ] Unsupported and not-configured cells cannot masquerade as passing.
- [ ] Common-path measurements use identical operations and workload parameters.
- [ ] Comparisons never cross response-barrier classes.
- [ ] Setup, recovery, maintenance, and cleanup are excluded from common timing.
- [ ] Raw samples, repetition summaries, environment, and exact source revision are retained.
- [ ] Statistical derivations are specified and independently recomputed.
- [ ] Correctness reconciliation blocks every timing claim.
- [ ] PostgreSQL schemas and Garage prefixes are run-owned, locked, and fail-closed on cleanup.
- [ ] Async rows report bounded catch-up separately and remain diagnostic.
- [ ] Service credentials are never serialized or printed.
- [ ] Authoritative execution fetches and matches the remote revision and rejects source-affecting dirt.
- [ ] GitHub Actions is limited to compilation and deterministic verifier tests.
- [ ] Results are explicitly host-bound and do not imply portable SLAs.
- [ ] TP-005 evidence cannot be accepted as TP-002 E0-E3 evidence.
