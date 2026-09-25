---
ddx:
  id: tp-fireweed-performance-matrix
  depends_on:
    - prd
    - api-fireweed-kernel-contract
    - adr-s3-turso-public-cell
    - td-kernel-projection-storage-layout
    - tp-scale-substantiation
    - tp-verification-acceptance-criteria
  status: accepted
  review:
    self_hash: af91841838128efbd8aec308c20b127aef0fbc4f50e328b6a210674b9ba73080
    deps:
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-orthogonal-log-projection-composition: 5e35283d3ad0cc38c61d57aac7a63ce7c5fc8028bc8ff5f51a2bb4c28a1f13e6
      api-fireweed-rust-facade: 26104ab47a5ecfa0f2fea739303d599d3a414461770f73e48a87a14dd48cba37
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
      tp-fireweed-facade-and-snorri-acceptance: b0646721bd9f5ec8bdcdf1698ff07fa03fe167f0bcd8991d57c35fcbb79493cc
      tp-scale-substantiation: 23f20e8dab88330e4ddd165a0d2230151b7ef0f99ca16c016671558ed5719686
    reviewed_at: "2026-08-04T04:50:53Z"
---

# TP-005: Fireweed performance matrix

## Testing strategy

**Goals**: a reproducible, revision-bound capacity record of the S3×Turso
implementation of API-006 on one declared host and service topology; every
timed call goes through `Fireweed` over `Arc<dyn Store>`; raw samples and
correctness evidence are kept; common-path results are kept apart from
recovery and maintenance results.

**Out of scope**: cloud-provider certification; multi-node scale-out, which
TP-002 E2 governs; extrapolation from this host to another; performance runs
on GitHub-hosted runners.

**Traceability source**: PRD scale substantiation; API-006; TD-017 §13, which
names the capacity runs this plan owns; TP-002 capacity and scale evidence.

This plan owns the host-bound capacity matrix. Its results are capacity
observations for the declared host and topology, and they never substitute
for:

- the TP-003 §3.12 claim-path and storage gates (AC-PERF-1 to AC-PERF-8),
  which are structural or same-run and rest on the spikes (SPIKE-002 to
  SPIKE-006);
- the TP-002 E0–E3 records, which alone may set governed wall-clock
  thresholds.

TP-005 evidence is never TP-002 evidence. The schema rejects
`tp002_evidence_ids`, and TP-005 artifacts are never part of a TP-002 governed
bundle. TP-005 measures a single-caller embedding baseline at bounded workload
size; it does not meet TP-002's resident-scale, scale-out, cost, recovery or
exact-tag requirements.

**Functional prerequisite.** A revision is eligible for timing only after
`fireweed-contract-suite` passes on it with zero skips (TP-003 AC-KC-1 to
AC-KC-3). A revision that has not passed is release-blocking and ineligible;
the runner spends no benchmark time characterizing it.

**No backend knobs.** Every run measures the one implementation through the
same eight ops, and every run makes the same assertions (API-006 CS4). Runs
differ only in workload shape.

Performance qualification runs on controlled, operator-selected hardware.
GitHub Actions may compile the runner, execute deterministic verifier tests
and verify a supplied evidence artifact; it never produces authoritative
timing evidence. The runner rejects an authoritative run when a conventional
CI environment variable is present, unless the operator selects the
non-authoritative smoke tier.

### Evidence classes and claim boundary

| Class | Purpose | Comparable | Release claim |
| --- | --- | --- | --- |
| `common` | Timed insert, claim and settle through `Fireweed` | Only within one run | Host- and topology-bound capacity record |
| `recovery` | Restart over the same durable state and verify it | Only within one run | Host-bound recovery record |
| `maintenance` | Verify, delete and rebuild the Turso projection through projection control on the admin handle | Only within one run | Host-bound maintenance record |
| `million-cycle-v1` | Insert 1M records, modify 500K, read and verify 1M | No | Host-bound timing only; not a 10M-resident result |
| `phased` | The TD-017 §13 capacity runs: P1–P4, the Snorri-shaped P2p–P4r, 32 queues per file, and the Snorri transition bench | Only within one run | Host-bound regression reference; never a gate |
| `smoke` | Fast runner and schema validation | No | None |

### Targeted million-record lifecycle

`million-cycle-v1` is a fail-closed functional gate at scale, not a
comparative benchmark:

1. Define one queue with the `record-1k` shape (see "Test data").
2. Insert exactly 1,000,000 records with unique keys, 1,000 writes per
   request, and time the insert phase.
3. Modify exactly the first 500,000 records by key with a sparse field patch,
   1,000 writes per request. Every outcome is `Put { created: false }`. Time
   the modify phase.
4. Read exactly 1,000,000 records by key, 1,000 keys per `read`, verifying
   every field value. Time the read phase, verification included.
5. Verify an exact deterministic digest: each modified record's version is
   greater than the version its insert returned, and each untouched record
   still has its insert version. A missing, duplicate, wrongly modified or
   extra record fails.
6. Restart over the same durable state, then repeat the read and digest
   verification outside the timed phases.

Queue definition, one bounded 10,000-record warm-up, restart, setup and
cleanup are outside the phase timings and recorded separately. Request
construction stays inside each timed phase, because the gate measures the
caller-observed operation. An error, a skip or a reduced record count is a P0
failure. The authoritative run is on controlled local hardware and never in
CI. This plan sets no portable wall-clock ceiling.

### Phased capacity runs

TD-017 §13 names these runs and records their reference values. Each phase
starts only after the previous phase's writes are acknowledged, so a phase's
rate (records divided by the phase's wall time) is attributable. The rollup
rate is N divided by the sum of the phase times.

- **P1–P4**, `ss_phased_capacity`, N = 10,000 by default, one queue with
  ordinary stages and declared fields:
  - P1 inserts N records, 100 per request;
  - P2 writes a 1,024 B profile field to each record by key, 100 per request;
  - P3 writes each record's scheduled time as its rank by key, 100 per
    request;
  - P4 claims 100 records at a time with `Next` and settles each claim by
    moving its records to the next stage with `Release(t)`.
- **P2p, P3p, P4p and P4r**, on a Snorri-shaped queue with 22 declared
  indexes, two of them keyed on `Stage`, and Snorri's large state and audit
  values as declared fields, so the ones over 4 KiB spill:
  - P2p mixes lease-free stage moves between ordinary stages into the updates;
  - P3p re-ranks each record by key, as P3 does;
  - P4p claims, then settles with `Release(t)`, a move to another stage and a
    field patch, rekeying the `Stage`-keyed indexes;
  - P4r claims, then settles with `Release(t)` and a new rank;
  - claims run both with `fields: None` and with a projection that leaves out
    the large fields (API-006 F4).
- **32 queues per file**: the 32-shard workflow qualification, run as 32
  queues in one partition file.
- **Snorri transition bench**: `WholeGroups` claims on the transition stage,
  the hydrating instance read, the instance record's `Expect::Version` write,
  and settles that move stages with history in fields, sized with Snorri's
  measured audit and state bytes.

These runs never gate. TP-003 decides whether any of them becomes a same-run
gate.

### Construction

The runner builds a fresh S3×Turso instance through the `fireweed`
constructor, which returns the `Fireweed` handle and `TursoAdmin`. Timed calls use `Fireweed` only; the admin handle is used only for projection
control in the recovery and maintenance classes. Every S3 namespace and local
path is derived from the run id, shape id, phase and repetition. The full
resolved non-secret constructor configuration is serialized with each row, and
every row of a run uses the same values; a mismatch between the resolved and
declared configuration fails before warm-up.

### Test levels

| Level | Coverage target | Priority |
| --- | --- | --- |
| Contract | 100% of workload ids, evidence fields and failure semantics | P0 |
| Integration | Every run constructs through the public constructor and reconciles exact state | P0 |
| Performance | Five measured repetitions per common-path workload after warm-up | P0 |
| Targeted lifecycle | `million-cycle-v1` | P0 when run |
| Recovery | Restart reconciles exact state | P0 |
| Maintenance | Projection verify, delete and rebuild reconcile exact state | P0 |
| Phased | The TD-017 §13 capacity runs complete with exact final state | P1 |
| Smoke | One small repetition for developer feedback | P1 |

### Frameworks

| Type | Framework | Reason |
| --- | --- | --- |
| Contract | Rust unit tests plus a JSON Schema-style semantic verifier | Rejects missing, duplicate, malformed or contradictory evidence |
| Integration | The `fireweed-bench` Cargo workspace | Isolates timing from the root workspace and calls only public Fireweed APIs |
| Performance | Monotonic `Instant` samples emitted as JSON | Keeps auditable raw measurements without a hidden statistical framework |
| External services | An operator-provided S3-compatible endpoint | Measures a real configured service on controlled hardware |

## Test data

Every generated record is deterministic from the recorded seed. No customer
data or production identifiers are used. Each shape defines one queue with the
default stages `[INITIAL, COMPLETED]`, the shape's declared fields and no
indexes.

| Shape | Fields | Groups | Rank | Records | Batch | Requests/op/rep |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `minimal` | none | none | default (insertion time) | 12,800 | 128 | 100 |
| `record-1k` | 16 `Str` fields of 64 B | none | deterministic uniform | 12,800 | 128 | 100 |
| `group-keyed-256` | 4 `Str` fields of 64 B | 64 | deterministic uniform | 12,800 | 128 | 100 |
| `large-16k` | one `Bytes` field of 16 KiB, which spills | none | deterministic uniform | 1,600 | 16 | 100 |

`large-16k` runs the common path twice in each repetition, each time on a
fresh queue: once claiming with `fields: None` and once with
`fields: Some([])` (API-006 F4). The two are reported as separate rows.

The authoritative full tier uses concurrency 1, one unmeasured warm-up
repetition and five measured repetitions. Each operation therefore keeps 100
request samples per repetition and 500 pooled samples per shape. This is a
deliberate embedding baseline: one caller driving one queue, with no claim of
saturation throughput. A saturation workload may vary concurrency under a
different workload id, and never replaces this baseline.

Each repetition uses a fresh queue and a fresh isolated storage namespace. The
schedule is fixed: warm-up visits shapes in table order; measured rounds are
outermost, and for round `r` the shape order rotates left by `r mod
shape_count` and reverses when `r` is odd. Warm-up storage is cleaned and
verified before measured round zero, and the exact schedule is recorded. The
runner records every request duration in nanoseconds, not only precomputed
percentiles.

## Coverage requirements

| Metric | Target | Minimum | Enforcement |
| --- | --- | --- | --- |
| Shapes completed | all four | all four; no silent substitution | Runner exits non-zero |
| Common operations per shape | insert, claim, settle | 100% | Semantic verifier |
| Inserted, claimed and settled reconciliation | exact | exact | Runner and verifier |
| Measured repetitions | 5 | 5 | Semantic verifier |
| Raw request samples | exactly 100 per operation and repetition | 100 | Semantic verifier |
| Pooled raw request samples | exactly 500 per shape and operation | 500 | Semantic verifier |
| Cross-repetition throughput CV | population CV reported | finite, mean > 0 | Semantic verifier |
| Environment provenance | complete required fields | 100% | Semantic verifier |
| Source provenance | clean pushed commit | exact | Launch wrapper and verifier |
| Secret leakage | zero credential values | zero | Redaction test and evidence scan |
| Million-cycle phases | insert, modify, and read with verification | recorded for all three; no TP-005 ceiling | Semantic verifier |
| Million-cycle restart | exact final digest after restart | 100% | Runner and semantic verifier |

### Common-path protocol

For every shape and repetition:

1. Construct a fresh instance (see "Construction").
2. Define the shape's queue, read it back with `read{queue: true}`, and
   require the normalized definition.
3. Insert the shape's exact population: each request is one unit of `batch`
   writes with `Expect::Absent` and a deterministic unique `request_id`. Keep
   every request duration.
4. Claim with `Next` on `INITIAL`, `max_records = batch` and a 1 h lease, and
   settle each claim with one write: `Release(t)` and a move to `COMPLETED`
   for every claimed record. Record claim and settle durations separately.
5. Assert: every claim before exhaustion returns exactly `batch` records;
   inserted = claimed = settled; every key is claimed exactly once; claim
   results are in `(rank, insertion sequence)` order; each claim returns one
   token; and on the quiet queue `stats` shows `INITIAL` with no Free or
   Leased record and `COMPLETED` with every record Free. A short or empty
   claim is a legal contract outcome (C5), but in this single-caller shape it
   is a capacity anomaly: the repetition is recorded as failed, with the
   short claim in its evidence.
6. Throughput per operation is the records handled divided by the monotonic
   interval of that operation's sequential calls. Assertions run outside the
   interval.

Setup, definition, warm-up, recovery, cleanup and correctness scans are
outside common-operation timing. Allocation and serialization on the call
path are inside request timing.

Before timing, each run makes an untimed preflight: the definition reads back
exactly; a resend of one insert request with its `request_id` returns
`replayed: true` with no second effect; the same `request_id` with a different
body gets `RequestIdConflict`; a settle with a fabricated token gets
`StaleLease`; and a `read` after a write observes it. A failed preflight
blocks every timing row of the run.

### Repetition and summary policy

- One warm-up repetition runs first for each shape and is discarded.
- Five measured repetitions run with stable seed-derived identifiers.
- The artifact keeps per-request samples and per-repetition totals.
- Nearest-rank percentile `p` sorts `N` integer nanosecond samples ascending
  and selects the one-indexed rank `ceil(p*N)`, clamped to `1..N`.
- The summary pools all 500 request samples and reports pooled p50, p95 and
  p99, and the median, minimum and maximum repetition throughput.
- Population CV is `sqrt(sum((x-mean)^2)/N)/mean` over five successful,
  positive repetition throughputs. A zero, non-finite or missing value
  invalidates the row; failures never enter the denominator.
- No samples are trimmed. A failed repetition stays recorded as a failure and
  makes the run incomplete.
- Comparisons are valid only between rows that share run id, host
  fingerprint, workload shape, record count, batch size, operation and
  repetition schedule.
- A pairwise comparison is `material` only when at least four of five
  same-round throughput ratios point the same way, the median ratio is at
  least 1.10 in that direction, and both rows have CV ≤ 0.15. Otherwise it is
  `inconclusive`. The label is deterministic, not a significance claim.
- Results establish capacity on the declared topology. They never become a
  portable pass bar because one run is faster than another.

### Recovery and maintenance protocol

**Recovery.** Three restart repetitions for `minimal` and `record-1k`, each at
the shape's full record count: drop the instance, construct a new one over the
same S3 namespace and Turso file, time construction to its successful return,
and verify the exact state (keys, stages, ranks and fields, not counts alone),
then drain it exactly. Results report the three raw durations with median,
minimum and maximum; three samples support no p95 or p99.

**Pre-restart verify.** Before each restart the runner calls projection
control's verify on the admin handle and requires the file's apply cursor to
equal the log head; a mismatch fails correctness.

**Maintenance.** Three repetitions, each on a fresh 12,800-record `record-1k`
population: verify the exact state, delete the run-owned Turso projection
through projection control, rebuild it from the object log, record the raw
durations, verify the same exact state, and drain it. Maintenance never
enters common-path latency.

### Tiers and isolation

`smoke` is a non-authoritative developer loop. `full` requires an attested S3
service. Missing configuration or attestation exits before storage is
created. A supplied but unreachable service is `failed`, never
`not_configured`. Absolute rates from either tier are capacity observations,
not portable pass bars.

The full run executes on a provisioned qualification runner with isolated
local paths and a reachable S3-compatible service. Before any instance opens,
the runner attests its identity and topology and the service's non-secret
capabilities: provider, version, region, native atomic conditional create and
update, consistency contract, TLS mode and bucket ownership. Provider brand
and host name are observations, not requirements. A missing required
capability fails closed before timing.

Every logical S3 namespace is below
`fireweed-perf/v1/<commit12>/<run-id>/<shape>/<phase>/rNN`. The physical
prefix is the lowercase hex of the logical namespace's UTF-8 bytes followed by
`/`. Cleanup accepts only a logical namespace from the run's immutable
allowlist, derives the physical prefix itself, and refuses an empty prefix,
`/`, a namespace outside `fireweed-perf/v1/`, or a bucket not exactly
acknowledged by `FIREWEED_PERF_S3_BUCKET_ACK`. Cleanup lists and deletes only
the exact run-owned prefix, lists it again, and requires zero remaining keys;
the cleanup type cannot list or delete the bucket root or a sibling run's
prefix. A create-if-absent lock object at
`fireweed-perf/v1/_locks/matrix.lock`, holding the run id, commit and start
time, prevents concurrent runs. A live lock blocks; removing a stale lock is a
separate explicit operator action that the benchmark does not implement.

All local paths live below a newly created run root. Canonicalization proves
each cleanup target is below that root before deletion. Cleanup guards run on
ordinary errors and unwinding. The artifact records the cleanup of every
namespace, and an incomplete cleanup fails the full run.

## Evidence contract

The runner writes one canonical JSON document and a SHA-256 sidecar. The
document contains:

- schema version, run id, tier, status, start and end UTC timestamps, and
  command;
- the full 40-character Git commit, branch, sanitized remote URL, fetched
  remote ref and commit, submodule state, and a source-clean assertion;
- OS, kernel, architecture, hashed host fingerprint, CPU model and count,
  total memory, virtualization or container signal, filesystem type and
  mount, free space, CPU governor and turbo data when available, load
  average, Rust and Cargo versions, enabled features and rustflags, build
  profile, and the benchmark lockfile hash;
- workload parameters, seed, the shape order of every repetition, shape
  definitions, and the redacted service topology;
- the resolved non-secret constructor configuration;
- raw request durations, per-repetition totals, derived summaries, exact
  reconciliation counts, and the recovery, maintenance and phased results;
- the S3 endpoint scheme, hashed endpoint host and bucket, region, provider
  and version, conditional-write and consistency capabilities, TLS mode,
  bucket acknowledgement and preflight round-trip samples; access and secret
  keys are omitted;
- the cleanup status of every run-owned local path and object namespace.

The verifier recomputes percentiles, summaries, CVs, comparison labels,
reconciliation, schedule, shape completeness and the sidecar digest. Unknown
schema fields fail closed within a schema version; each artifact stays
verifiable by the verifier of its schema version. `tp002_evidence_ids` is
forbidden. Credential fields and configured credential values are rejected
anywhere in serialized evidence; documented environment variable names appear
only in this specification, never in recorded command arguments. Errors pass
through a redactor seeded with the S3 access key and secret before they are
printed or serialized, and the wrapper never enables shell tracing. The
recorded command names variables, never their values, and remote URLs lose
their user information and query strings.

Before an authoritative run, the wrapper executes `git fetch origin main`,
requires `HEAD == refs/remotes/origin/main`, requires no staged or tracked
changes, and rejects untracked files under `docs/`, `crates/`, `scripts/`,
`.cargo/` or any Cargo or toolchain manifest path. Other untracked paths enter
provenance only as content hashes. Submodules, when present, are initialized,
clean and at recorded commits. The evidence file is archived with its
attestation. Running the same commit again creates a new run id; evidence is
append-only and never overwritten.

The full orchestrator has a four-hour run timeout and a 15-minute timeout per
shape, round and phase fragment, and writes an atomic checkpoint after every
fragment. `--resume` accepts a fragment only when its digest, source commit,
run id, resolved configuration, schedule and verifier result all match;
otherwise it cleans that fragment's allowlisted namespace and reruns it. A
call that returns `Busy` or `Unavailable` fails its fragment and stays in the
evidence. A normal full run is budgeted at 30 to 120 minutes on the declared
topology; exceeding the estimate is diagnostic, and exceeding a hard timeout
fails the run.

Before any authoritative timing, the same pushed commit passes the contract
suite (TP-003 AC-KC-1 to AC-KC-3) and the benchmark crate's locked test suite.
Commands, exit status, duration and the SHA-256 of captured output are
recorded; raw output is kept separately after redaction.

## Acceptance criteria layer allocation

| Requirement source | Primary layer | Blocking evidence |
| --- | --- | --- |
| API-006 conformance | Functional prerequisite | The contract suite passes on the revision before any timing |
| API-006 CS4, no backend knobs | Protocol | Every run makes the same calls and assertions |
| ADR-024 S3×Turso | Construction | Every run constructs the one implementation through its public constructor |
| TD-017 §13 capacity runs | Phased class | P1–P4, P2p–P4r, 32 queues per file and the Snorri transition bench complete with exact final state |
| TP-002 evidence honesty | Evidence verifier | Host-bound claims, exact revision, raw samples and no silent skips |
| TP-003 §3.12 | Separation | No TP-005 result is cited as an AC-PERF pass |

## Implementation order

1. Define serializable evidence types and a verifier with malformed-fixture
   tests.
2. Add a dedicated runner module without changing the TP-002 benchmark suites
   or their evidence format. The timed module imports only public Fireweed
   crates; cleanup and provenance code is separate and outside timed
   intervals.
3. Implement construction, fresh namespace allocation, the S3 lock, signal-aware
   cleanup guards and cleanup audits.
4. Implement warm-up, rotated scheduling, the common operations, recovery
   and maintenance.
5. Implement `million-cycle-v1` and the phased runs.
6. Add the clean-pushed-revision launch wrapper and secret-safe environment
   mapping.
7. Run smoke locally, then the full tier on the pushed revision.

## Infrastructure

| Requirement | Specification |
| --- | --- |
| Execution host | A provisioned, attested, operator-selected runner with a declared service topology; never a GitHub-hosted runner |
| Rust | Repository-pinned toolchain; `cargo run --release --locked`; exact features recorded |
| Object storage | A live S3-compatible provider attesting native conditional writes; endpoint, bucket, region and credentials from `FIREWEED_PERF_S3_*`; the bucket separately acknowledged |
| Local storage | A run-owned directory under an explicit work root on the measured filesystem |
| Output | Run-owned `target/fireweed-perf/<commit>-<run-id>.json` plus `.sha256`; an external evidence archive may keep the attested pair |
| CI | Compile and verifier tests only; timing evidence from CI is non-authoritative |

## Risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Thermal or background-load drift | Comparisons biased by order | Warm-up, five repetitions, rotated shape order, raw samples, CV reporting |
| Remote provider network variance | S3 tails hide implementation changes | Record the hashed topology and round trips; keep raw samples; compare only within a run |
| Object namespace residue | Cost and list performance drift | A unique prefix per repetition and verified prefix cleanup |
| An incomplete implementation is timed | Misleading capacity evidence | The contract-suite prerequisite blocks timing |
| Secrets reach evidence or logs | Credential disclosure | Redacted configuration types, a field-name denylist, and a value scan before write |
| The benchmark becomes a release bottleneck | Tests get skipped or moved to CI | Separate smoke and full tiers; controlled-host execution; a fast verifier |

**Known boundaries**: this baseline is single-caller. It does not establish
saturation capacity, multi-client fairness, multi-node scaling or the
certification of any S3 provider; those need distinct workload ids or stay
with TP-002. Absolute rates are not portable pass bars.

## Build handoff

**Commands**:

```sh
cargo test --manifest-path crates/fireweed-bench/Cargo.toml --locked
scripts/perf/fireweed-matrix.sh --tier smoke
set -a
source <operator-managed-performance-env-file>
set +a
scripts/perf/fireweed-matrix.sh --tier full \
  --output target/fireweed-perf
scripts/perf/verify-fireweed-matrix.sh \
  target/fireweed-perf/<commit>-<run-id>.json
```

**Priority**: evidence contract and verifier; common path; recovery and
maintenance; `million-cycle-v1`; the phased runs.

**Blocking gate**: the full tier refuses a missing S3 configuration; every
shape runs with no silent substitution; exact reconciliation, recovery,
maintenance and cleanup pass; the verifier independently recomputes
the artifact and comparison labels; the sidecar matches; and no credential
value is present.

## Review checklist

- [ ] The contract suite passed on the revision before any timing.
- [ ] Every run constructs S3×Turso through its public constructor; a missing
      S3 service fails before timing.
- [ ] Every timed call goes through `Fireweed`; the admin handle serves only
      projection control.
- [ ] Common-path measurements use identical operations and workload
      parameters.
- [ ] Setup, recovery, maintenance and cleanup are outside common timing.
- [ ] Raw samples, repetition summaries, environment and exact source revision
      are kept.
- [ ] Statistical derivations are specified and independently recomputed.
- [ ] Correctness reconciliation blocks every timing claim.
- [ ] S3 prefixes and local paths are run-owned, locked and fail closed on
      cleanup.
- [ ] Service credentials are never serialized or printed.
- [ ] Authoritative execution fetches and matches the remote revision and
      rejects source-affecting changes.
- [ ] GitHub Actions is limited to compilation and deterministic verifier
      tests.
- [ ] Results are explicitly host-bound and imply no portable SLA.
- [ ] TP-005 evidence cannot be accepted as TP-002 E0–E3 evidence or as a
      TP-003 AC-PERF pass.
