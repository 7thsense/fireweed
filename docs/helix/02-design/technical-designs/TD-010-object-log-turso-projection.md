---
ddx:
  id: td-object-log-turso-projection
  depends_on:
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - adr-log-single-source-of-truth
    - adr-turso-derived-projection
    - adr-s3-turso-public-cell
    - adr-full-async-storage-boundaries
    - adr-async-commit-strategy-and-dispatch
    - concerns
  links:
    - {kind: informed_by, to: api-fireweed-kernel-contract}
    - {kind: informed_by, to: adr-kernel-records-contract}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: adr-s3-turso-public-cell}
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: concerns}
  status: accepted
  review:
    self_hash: d05ba185fd6e0be01855a97f39abbce04a486adf4a6f2c69f14e236befecc25f
    deps:
      adr-async-commit-strategy-and-dispatch: 6daa55d01fce58248b5b607c3015ed0600d23ff123912e2bc1fd63a484a8ab49
      adr-full-async-storage-boundaries: 0543121229a415143387307275263908017b43697ddac970d54d6d30a2c7ccaa
      adr-log-single-source-of-truth: c88063a069f43bd90f31e4875ad8b35fca9876de5b52cb777908d314d46abd1b
      adr-turso-derived-projection: b93a1a9c4ba242940b86878551dddd35f9aa4e399357417c620e66f5ab2a7b67
      api-native-client-interface: b99403ef55afffd134ac3ef1a71065c497558c94de379c2b257b119000a0f488
      concerns: d00e29334f99ed2fe3c9151bacb107255a3d7add89606949e409eb6614382d6c
      td-s3-object-log-sqlite-projection-mode: 7770bb133f4ace189bfc715e3be6472f894f7c62d52adfc051540fea97c6a4b2
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
    reviewed_at: "2026-08-04T04:50:53Z"
---

# Technical Design: TD-010 Turso projection over the object log

**Status**: Accepted
**Contract**: API-006 | **ADR**: ADR-013, ADR-015, ADR-016, ADR-017, ADR-024, ADR-025 | **TD**: TD-003, TD-016, TD-017, TD-018
**Scope**: the Turso projection of the S3×Turso implementation: how it applies the object log, serves reads, recovers and rebuilds

## Scope

S3 object log × Turso is the one production implementation of API-006's `Store` (ADR-024, ADR-025). The object log is the authority. Each Turso file is a projection derived from it, and it serves claims and reads.

In scope:
- the `fireweed-turso` adapter, and which Turso modes it runs;
- the authority split, and the order of log commit, apply and response;
- the apply cursor, replay and poisoning;
- connection roles;
- the async boundary and cancellation;
- open, recovery, and rebuild through projection control;
- resource bounds, tests and risks.

Out of scope:
- The projection schema, the statement forms and the connection settings. TD-017 fixes them.
- The write pipeline: generations, the overlay, log-first claim planning and packing. TD-016 owns it.
- The object-log layout, partitions, snapshots and compaction. TD-018 designs them.
- Ownership, fencing and handoff. TD-003 owns them.

## Supported Turso boundary

- `turso = "=0.7.2"`, with `default-features = false`, embedded and local, in ordinary WAL mode (ADR-016).
- Not supported: remote databases, embedded replicas, sync, experimental MVCC, FTS and allocator features.
- Initialization sets each pragma on its own, consumes the result and reads the value back. It never sends the pragma batch through `execute_batch`: the Turso 0.7 probe showed that call can report failure after it has already switched the journal mode.
- The connection settings are TD-017 §12's: the 4 KiB page, WAL, `synchronous = NORMAL` on log-backed writers, no autocheckpoint, the Passive checkpoint cap, the cache sizes and the busy timeout.
- A non-durable projection is Turso on `:memory:`, with the same schema, SQL and apply path. Tests of queue semantics and non-durable benches use it. Tests of checkpoint, reopen and rebuild use a temporary file (ADR-024).
- The pin moves only after the Turso compatibility probe and the plan tests (TD-017 §11, rule 8) pass on the new version.

## Authority and commit order

- **The log is the authority** (ADR-013). A Turso file holds nothing that a replay of the log cannot rebuild. Every value a client observes or sends back comes from the log (TD-017 §1).
- **Commit order.** Every mutating operation (`define_queue`, `write`, `claim`, `delete_queue` and `force_clear`) runs in four steps:
  1. The owner plans the operation against its serving snapshot and the in-flight overlay (TD-016).
  2. It commits the operation's command to the object log, carrying its fenced epoch (TD-003).
  3. The writer applies the committed command to the Turso file in one transaction.
  4. The response returns.

  So `Ok` means durable (API-006 DUR1), and any claim, `read` or `scan` issued after it observes its effects (W12, V2, V3). `stats` and `aggregate` read the same committed state and may be torn (V3).
- **Claims are log-first.** A claim's selection, its leases and its token hash are in its log entry. The claim's response is built from the planning snapshot before the append and is never re-rendered from Turso after it (TD-016).
- **Unknown outcomes.** A crash between steps 2 and 4 leaves the command in the log and the caller without an answer. A resend with the same `request_id`, or a `read` of what the request touched, resolves it (API-006 E2, RQ2, RQ4). The log decides.

## Apply

- **One transaction per packed vector.** The writer applies each committed vector in log order in one `BEGIN IMMEDIATE` transaction. Record rows, claims, fields, index entries, counters, request records and the apply cursor commit together (TD-017 §11, rule 6).
- **Apply cursor.** For each queue, the file records the next log offset it expects to apply. A command below the cursor is already applied and is skipped, so an overlapping replay is a no-op. A command above the cursor is a gap, and the apply fails.
- **Poison.** An apply that fails after its log commit acknowledges nothing and poisons the projection for the affected queues: new operations on them fail closed with `Unavailable` until the owner recovers them by replay from the cursor or by a rebuild.
- **Background batches.** The reclaim sweep, the `delete_queue` purge and the request-record sweep run on the writer connection, each batch in its own transaction under the checkpoint cap (TD-017 §3, §8, §9).

## Connections

- **One writer per file**, behind a `Mutex` (TD-017 §12). Every apply and every background batch goes through it.
- **Serving readers** take the claim-planning snapshots and serve `read`, `scan`, `stats` and `aggregate`. They read committed state only. A read issued after an acknowledgement observes it, because the apply committed before the acknowledgement.
- **Pooled readers**, with a small cache, serve generation drivers and committed-outcome reads.
- **No per-queue resources.** No connection, task or loop exists per queue. Connections and background work are shared per file and per node.

## Async boundary and cancellation

These rules come from ADR-015 and ADR-017.

- Every storage call returns a runtime-neutral `Send` future, and no Tokio type crosses into the contract or the engine ports. No blocking driver call runs on a reactor thread, and no standard mutex guard is held across an `.await`.
- A mutation hands owned request data and an owned connection capability to a bounded commit task before its transaction starts. The caller awaits only a result channel, so dropping the caller's future cannot cancel a started commit.
- Dropping the caller's future before the mutation reaches its commit task leaves no effect. After that point the commit task carries the mutation to a determinate outcome, through the log commit and the apply, and a caller that dropped its future resolves the outcome as an unknown outcome (API-006 E2).
- Shutdown stops admission, drains the started commit tasks and closes the writer.

## Open, recovery and rebuild

- **Open.** The adapter verifies the file's schema version, then replays each owned queue's log from its apply cursor to the head before it serves (TD-003, handoff step 6). It refuses to serve a file whose schema version it does not know.
- **Missing or unusable file.** A new file is rebuilt from the log. Without snapshots, a rebuild replays from genesis; TD-018 proposes partition snapshots (its P1), subject to measurement.
- **Corrupt state never becomes authority.** A file that fails verification cannot advance the trusted log position and cannot justify deleting any log object.
- **Projection control.** The admin handle (`TursoAdmin`, API-006 §7) carries projection control: verify a file against the log, delete it, and rebuild it. It is deployment control, not a consumer path. Tests of projection verify, delete and rebuild call projection control; deleting the file and reopening is not a substitute.
- **Tokens survive.** A rebuild restores every claim from its log entry, with its id and token hash, so live tokens stay valid across a rebuild (TD-017 §3).

## Resource bounds

- Responses retained between planning and apply are bounded by TD-016's generation bounds, separately from the page cache.
- Each connection's page cache is bounded by TD-017 §12.
- A single-thread heartbeat keeps advancing throughout database work.

## Testing

- **Conformance.** `fireweed-contract-suite` runs against S3×Turso through `run_all` (API-006 CS1), with `reopen` over the same object-log namespace and Turso file, and with the post-commit fault hook.
- **Settings.** Each pragma is read back after it is set, and TD-017's plan tests pass.
- **Atomicity.** Record rows, claims, index entries, counters, the request record and the apply cursor commit together, and an injected failure rolls all of them back.
- **Replay.** An overlapping prefix is a no-op, a gap fails, a reopen resumes from the cursor, and a rebuild from the log reproduces every observable value, live tokens included.
- **Cancellation.** Drop the caller's future before hand-off to the commit task, during the log commit, after it and before the apply, and while waiting for the writer. Each case ends with no effect or with one determinate outcome.
- **Poison.** An apply failure after the log commit fails the affected queues closed until recovery, and no acknowledged write is lost or duplicated.
- **Runtime.** A single-thread heartbeat advances during Turso work.
- **Parity.** The same operation sequence gives the same observable state on `:memory:` and on a file.
- **Where it runs.** Qualification runs on an S3-compatible store. The object log's filesystem publication adapter tests the log protocol without one, and a filesystem result is not evidence about S3×Turso (ADR-024).

## Risks

| Risk | Prob | Impact | Mitigation |
|---|---|---|---|
| Turso is pre-1.0, and an upgrade changes SQL or planner behavior | M | H | The exact pin; the probe and the plan tests on every upgrade (TD-017 §11). |
| Rebuild time grows with the log | H | M | TD-018's P1 (snapshot plus tail), gated on measurement. |
| Cancellation strands a transaction or a waiter | M | H | Owned commit tasks, and cancellation tests at each cut. |
| A corrupt local file is served | L | H | Verification at open, rebuild from the log, and corrupt state can never advance the trusted log position. |
| An apply failure leaves a queue unserved | L | M | Poison fails the queue closed instead of serving stale state; replay or rebuild restores it. |
