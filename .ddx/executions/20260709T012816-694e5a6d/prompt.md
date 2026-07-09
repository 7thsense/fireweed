<bead-review>
  <bead id="pqueue-a352dbcc" iter=1>
    <title>Hybrid-strict/async fault seams + AC-TXN-5/5A poison matrix</title>
    <description>
PROBLEM: AC-TXN-5 and AC-TXN-5A (TP-003 §3.10 rows 210-211) require injecting failures in the objectlog/hybrid-strict and hybrid-async projection paths — after manifest commit, after SQLite commit before memory apply, during memory apply, before response delivery (strict); and before manifest, after manifest before memory apply, during async SQLite apply, after SQLite lag recovery, while async apply debt exceeds budget, after backpressure admission trips (async) — none of which the public seam reaches, so AC-TXN-5/5A are unrecorded/deferred. ROOT CAUSE: the hybrid deferred-apply path (crates/pqueue-server object_log_sqlite hybrid + crates/pqueue-sqlite projection apply) has no test-injectable failure hook. PROPOSED FIX: add test-only fault hooks at the hybrid-strict and hybrid-async cut points; extend the matrix suite to cover AC-TXN-5 (SQLite-commit/memory-fail poisons the store fail-closed until restart; restart hydrates memory from SQLite ProjectionImage; same-body retry returns original, conflicting body -&gt; request-id-conflict) and AC-TXN-5A (success only after manifest+synchronous memory apply; ordered batching applies sealed batches in batch_sequence order exactly once; sqlite_high_water advances only after complete batch apply; crash-before-memory-apply resolves unknown-outcome by request_id; backpressure/debt fails closed). NON-SCOPE: object-log segment/manifest cut points (AC-TXN-4, separate bead).
    </description>
    <acceptance>
1. `ac_txn_5_hybrid_strict_poison_replay` passes (fail-closed poison + restart hydration + request-id semantics).
2. `ac_txn_5a_hybrid_async_success_barrier` passes (success barrier + ordered exactly-once batch apply + unknown-outcome-by-request-id + backpressure fail-closed).
3. `rustup run 1.92.0 cargo test -p pqueue-conformance -p pqueue-sqlite -p pqueue-server` passes.
4. `rg -n 'AC-TXN-5' docs/perf/evidence/tp003-ac-txn-matrix.jsonl` shows the hybrid rows as pass.
    </acceptance>
    <labels>kind:test, area:pqueue-sqlite, area:pqueue-server, area:pqueue-conformance, gap-closure, phase-5, tp-003</labels>
  </bead>

  <changed-files>
    <file>.ddx/executions/20260709T010529-2bd2df14/manifest.json</file>
    <file>crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs</file>
    <file>crates/pqueue-sqlite/src/lib.rs</file>
    <file>crates/pqueue-sqlite/src/relational.rs</file>
    <file>docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl</file>
    <file>docs/perf/evidence/tp003-ac-txn-matrix.jsonl</file>
  </changed-files>

  <governing>
    <note>No governing documents found. Evaluate the diff against the acceptance criteria alone.</note>
  </governing>

  <diff rev="81e029e943780850c84fc6dc3ad3ab0b803fb303">
<untrusted-data>
diff --git a/docs/perf/evidence/tp003-ac-txn-matrix.jsonl b/docs/perf/evidence/tp003-ac-txn-matrix.jsonl
index 7ed102af..7ac74224 100644
--- a/docs/perf/evidence/tp003-ac-txn-matrix.jsonl
+++ b/docs/perf/evidence/tp003-ac-txn-matrix.jsonl
@@ -1,15 +1,17 @@
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"memory","result":"n/a","detail":"non-durable in-memory dev profile: kill/restart durability is not applicable","assertions":[],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"memory","result":"partial","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","restart-replay clause N/A (non-durable in-memory dev profile)"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"memory","result":"partial","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply + AfterApplyBeforeResponse restart clauses N/A (non-durable in-memory dev profile)"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"sqlite_log","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"sqlite_log","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"sqlite_log","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"objectlog","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"objectlog","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"objectlog","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-4","backend":"objectlog","result":"pass","detail":"","assertions":["BeforeSegmentWrite: 0 durable commands (0 lost accepted items)","AfterSegmentWriteBeforeManifest: orphan segments durably written but ignored by replay (0 lost, 0 duplicated)","AfterManifestBeforeAck: committed push AND claim commands replay exactly once on recovery (0 duplicate active leases)","DuringOwnerReassignment: epoch-fence commit survives a lost ack; stale-epoch commits rejected, current-epoch commits succeed","DuringSnapshotWrite: a lost snapshot write leaves the command log fully intact (0 lost items)"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-6","backend":"sqlite_log|object_log_sqlite","result":"pass","detail":"","assertions":["identical final visible QueueMetrics (incl. complete/failed terminal counts), select_eligible order, and pending/active-lease set (item_id:attempt) across combinations; NOT compared: per-request_id idempotency records or per-item terminal-outcome records (metrics=QueueMetrics { pending: 3, leased: 0, complete: 1, failed: 0, resident_terminal_count: 1 }, eligible=[\"4\", \"3\", \"1\"], pending=[])"],"recorded_at":"epoch:1783558940"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-7","backend":"objectlog(force-seal|group-commit)","result":"partial","detail":"AC-TXN-3 invariance across commit-latency-bound settings","assertions":["force-seal AC-TXN-3 assertions == group-commit AC-TXN-3 assertions: true","BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"memory","result":"n/a","detail":"non-durable in-memory dev profile: kill/restart durability is not applicable","assertions":[],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"memory","result":"partial","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","restart-replay clause N/A (non-durable in-memory dev profile)"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"memory","result":"partial","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply + AfterApplyBeforeResponse restart clauses N/A (non-durable in-memory dev profile)"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"sqlite_log","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"sqlite_log","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"sqlite_log","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"objectlog","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"objectlog","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"objectlog","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-4","backend":"objectlog","result":"pass","detail":"","assertions":["BeforeSegmentWrite: 0 durable commands (0 lost accepted items)","AfterSegmentWriteBeforeManifest: orphan segments durably written but ignored by replay (0 lost, 0 duplicated)","AfterManifestBeforeAck: committed push AND claim commands replay exactly once on recovery (0 duplicate active leases)","DuringOwnerReassignment: epoch-fence commit survives a lost ack; stale-epoch commits rejected, current-epoch commits succeed","DuringSnapshotWrite: a lost snapshot write leaves the command log fully intact (0 lost items)"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-5","backend":"object_log_sqlite(hybrid-strict)","result":"pass","detail":"","assertions":["AfterSqliteCommitBeforeMemoryApply: a durable SQLite commit whose memory apply never runs poisons the store; every subsequent read fails closed before restart","restart hydrates memory from the durable SQLite ProjectionImage; a same-(position,body) retry replays the original result without a second append","request-id semantics on the objectlog/hybrid substrate: same-body retry replays the original result; conflicting body returns request-id-conflict"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-5A","backend":"object_log_sqlite(hybrid-async)","result":"pass","detail":"","assertions":["success barrier: a memory-apply failure withholds success even though the preceding manifest commit is durable","ordered batching: 3 live-applied commands drain in one flush, the SQLite high-water advances through the whole batch exactly once, and a no-op flush does not re-advance it","unknown-outcome-by-request_id (delegated to AC-TXN-3 on the objectlog/hybrid substrate): 3 assertions held","backpressure fail-closed: async apply debt over the hard budget rejects new mutation admission and withholds the lagging high-water"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-6","backend":"sqlite_log|object_log_sqlite","result":"pass","detail":"","assertions":["identical final visible QueueMetrics (incl. complete/failed terminal counts), select_eligible order, and pending/active-lease set (item_id:attempt) across combinations; NOT compared: per-request_id idempotency records or per-item terminal-outcome records (metrics=QueueMetrics { pending: 3, leased: 0, complete: 1, failed: 0, resident_terminal_count: 1 }, eligible=[\"4\", \"3\", \"1\"], pending=[])"],"recorded_at":"epoch:1783560329"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-7","backend":"objectlog(force-seal|group-commit)","result":"partial","detail":"AC-TXN-3 invariance across commit-latency-bound settings","assertions":["force-seal AC-TXN-3 assertions == group-commit AC-TXN-3 assertions: true","BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783560329"}
diff --git a/.ddx/executions/20260709T010529-2bd2df14/manifest.json b/.ddx/executions/20260709T010529-2bd2df14/manifest.json
new file mode 100644
index 00000000..cf70e4dd
--- /dev/null
+++ b/.ddx/executions/20260709T010529-2bd2df14/manifest.json
@@ -0,0 +1,36 @@
+{
+  "attempt_id": "20260709T010529-2bd2df14",
+  "bead_id": "pqueue-a352dbcc",
+  "base_rev": "da56fcdd359c8508afcb6c84732e0fc5cf45f764",
+  "created_at": "2026-07-09T01:05:30.830320631Z",
+  "requested": {
+    "harness": "claude",
+    "model": "claude-sonnet-5",
+    "prompt": "synthesized"
+  },
+  "bead": {
+    "id": "pqueue-a352dbcc",
+    "title": "Hybrid-strict/async fault seams + AC-TXN-5/5A poison matrix",
+    "description": "PROBLEM: AC-TXN-5 and AC-TXN-5A (TP-003 §3.10 rows 210-211) require injecting failures in the objectlog/hybrid-strict and hybrid-async projection paths — after manifest commit, after SQLite commit before memory apply, during memory apply, before response delivery (strict); and before manifest, after manifest before memory apply, during async SQLite apply, after SQLite lag recovery, while async apply debt exceeds budget, after backpressure admission trips (async) — none of which the public seam reaches, so AC-TXN-5/5A are unrecorded/deferred. ROOT CAUSE: the hybrid deferred-apply path (crates/pqueue-server object_log_sqlite hybrid + crates/pqueue-sqlite projection apply) has no test-injectable failure hook. PROPOSED FIX: add test-only fault hooks at the hybrid-strict and hybrid-async cut points; extend the matrix suite to cover AC-TXN-5 (SQLite-commit/memory-fail poisons the store fail-closed until restart; restart hydrates memory from SQLite ProjectionImage; same-body retry returns original, conflicting body -\u003e request-id-conflict) and AC-TXN-5A (success only after manifest+synchronous memory apply; ordered batching applies sealed batches in batch_sequence order exactly once; sqlite_high_water advances only after complete batch apply; crash-before-memory-apply resolves unknown-outcome by request_id; backpressure/debt fails closed). NON-SCOPE: object-log segment/manifest cut points (AC-TXN-4, separate bead).",
+    "acceptance": "1. `ac_txn_5_hybrid_strict_poison_replay` passes (fail-closed poison + restart hydration + request-id semantics).\n2. `ac_txn_5a_hybrid_async_success_barrier` passes (success barrier + ordered exactly-once batch apply + unknown-outcome-by-request-id + backpressure fail-closed).\n3. `rustup run 1.92.0 cargo test -p pqueue-conformance -p pqueue-sqlite -p pqueue-server` passes.\n4. `rg -n 'AC-TXN-5' docs/perf/evidence/tp003-ac-txn-matrix.jsonl` shows the hybrid rows as pass.",
+    "labels": [
+      "kind:test",
+      "area:pqueue-sqlite",
+      "area:pqueue-server",
+      "area:pqueue-conformance",
+      "gap-closure",
+      "phase-5",
+      "tp-003"
+    ]
+  },
+  "paths": {
+    "dir": ".ddx/executions/20260709T010529-2bd2df14",
+    "prompt": ".ddx/executions/20260709T010529-2bd2df14/prompt.md",
+    "manifest": ".ddx/executions/20260709T010529-2bd2df14/manifest.json",
+    "result": ".ddx/executions/20260709T010529-2bd2df14/result.json",
+    "checks": ".ddx/executions/20260709T010529-2bd2df14/checks.json",
+    "usage": ".ddx/executions/20260709T010529-2bd2df14/usage.json",
+    "worktree": "home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-a352dbcc-20260709T010529-2bd2df14"
+  },
+  "prompt_sha": "b10822021ea9f1ad657675e7701481369b9a08599b4a9e1a6aa59254e7e1af94"
+}
\ No newline at end of file
diff --git a/crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs b/crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs
index 819e2a2e..5da2c8a9 100644
--- a/crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs
+++ b/crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs
@@ -15,6 +15,8 @@
 //! | AC-TXN-2 rejection no-effect | partial (in-proc) | ✓ | ✓ | ✓ | ✓ |
 //! | AC-TXN-3 unknown-outcome replay | partial (in-proc) | ✓ full | ✓ full | ✓ full | ✓ full |
 //! | AC-TXN-4 objectlog crash-point matrix | — | — | ✓ (5 internal cut points)* | — | — |
+//! | AC-TXN-5 hybrid-strict poison + replay | — | — | | ✓ (projection cut points)† | — |
+//! | AC-TXN-5A hybrid-async success barrier | — | — | | ✓ (projection cut points)† | — |
 //! | AC-TXN-6 cross-combination parity | — | ✓ (sqlite-log vs objectlog+sqlite) | | | — |
 //! | AC-TXN-7 latency-bound invariance | — | — | partial (force-seal vs group-commit) | | — |
 //!
@@ -32,17 +34,36 @@
 //! lives in a distinct architectural layer (`pqueue-engine`'s `ComposedBackend`, which applies a batch only
 //! after `LogStore::append` already returned `Ok`) and is not internal to this crate; it stays a documented
 //! follow-up rather than a fake pass here.
+//!
+//! `†` AC-TXN-5/5A (see `ac_txn_5_hybrid_strict_poison_replay_scenario` /
+//! `ac_txn_5a_hybrid_async_success_barrier_scenario` below) add the analogous seam on the PROJECTION side —
+//! [`pqueue_sqlite::HybridFaultHook`] on `HybridProjectionStore` — for the instants the public seam cannot
+//! isolate (a fault strictly between the durable SQLite commit and the in-memory apply, and one strictly
+//! inside the deferred async SQLite checkpoint), driving `HybridProjectionStore` DIRECTLY via
+//! `ProjectionStore` for those clauses. Where a cut point genuinely IS reachable through the public seam (a
+//! memory-apply failure struck from inside `apply_live`, or a crash in the commit→apply window covered by
+//! AC-TXN-3), these scenarios drive it through the real `ComposedBackend<ObjectLog, HybridProjectionStore,
+//! InProcessControlPlane>` instead. Backpressure fail-closed (AC-TXN-5A) is proven directly against
+//! [`pqueue_sqlite::HybridAsyncMonitor`], the component that implements TD-004's admission-gating contract.
 
 use std::sync::Arc;
 use std::sync::atomic::{AtomicU64, Ordering};
 
 use pqueue_conformance::fault::{
     AcEvidence, AcOutcome, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
-    ac_txn_3_unknown_outcome_replay, ac_txn_6_parity, write_evidence,
+    ac_txn_3_unknown_outcome_replay, ac_txn_6_parity, durable_command_count, write_evidence,
+};
+use pqueue_core::RequestId;
+use pqueue_engine::{
+    ClaimCommand, CommandPosition, ComposedBackend, ControlPlaneStore, EngineError,
+    InProcessControlPlane, LogStore, ProjectionSnapshot, ProjectionStore, PushPort, QueueCommand,
+    PushCommand,
 };
-use pqueue_engine::{ClaimCommand, ComposedBackend, EngineError, InProcessControlPlane, LogStore, ProjectionSnapshot};
 use pqueue_objectlog::{FaultCutPoint, FaultHook, ObjectLog, SegmentConfig};
-use pqueue_sqlite::HybridProjectionStore;
+use pqueue_sqlite::{
+    HybridAsyncDebt, HybridAsyncMonitor, HybridAsyncThresholds, HybridFaultCutPoint,
+    HybridFaultHook, HybridProjectionStore,
+};
 
 static COUNTER: AtomicU64 = AtomicU64::new(0);
 
@@ -419,6 +440,347 @@ async fn ac_txn_4_objectlog_crash_point_matrix() {
     );
 }
 
+// ---------------------------------------------------------------------------
+// AC-TXN-5 / AC-TXN-5A: hybrid-strict / hybrid-async projection-apply fault seams (TP-003 §3.10 rows
+// 210-211)
+// ---------------------------------------------------------------------------
+//
+// Mirrors AC-TXN-4's honesty split: the object-log side already has its own internal `FaultHook`
+// (`pqueue_objectlog::segmented`, AC-TXN-4); this section adds the analogous seam on the PROJECTION side —
+// `pqueue_sqlite::HybridFaultHook` — striking instants strictly INSIDE `HybridProjectionStore`'s own apply
+// pipeline (between the durable SQLite commit and the in-memory apply, and inside the deferred async SQLite
+// checkpoint apply) that neither `Backend::write` nor `PushPort::push_with_request_id` can isolate. Where a
+// cut point genuinely IS reachable through the public seam (a memory-apply failure struck from inside
+// `apply_live`, or a crash in the commit→apply window), these scenarios drive it through the real
+// `ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>` instead, same as the rest of
+// this suite.
+
+/// Crashes (`Err`) every time the hybrid projection's apply pipeline reaches `target`; a no-op at every
+/// other cut point. The `HybridProjectionStore` analogue of `CrashAt` above (AC-TXN-4).
+struct HybridCrashAt(HybridFaultCutPoint);
+
+impl HybridFaultHook for HybridCrashAt {
+    fn fault_point(&self, cut: HybridFaultCutPoint) -> pqueue_engine::EngineResult<()> {
+        if cut == self.0 {
+            Err(EngineError::Storage(format!(
+                "fault-injection: crash at {cut:?}"
+            )))
+        } else {
+            Ok(())
+        }
+    }
+}
+
+/// Assemble a fresh `objectlog/hybrid` composed backend at `root` with `hook` installed on its
+/// `HybridProjectionStore` BEFORE the first command lands, so the fault strikes the very first apply.
+fn objectlog_hybrid_with_fault_hook(root: &std::path::Path, hook: Arc<dyn HybridFaultHook>) -> HybridBackend {
+    std::fs::create_dir_all(root).ok();
+    let sqlite_path = root.join("projection.sqlite");
+    let log =
+        ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("open object log");
+    let hybrid =
+        HybridProjectionStore::open(sqlite_path.to_str().unwrap()).expect("open hybrid projection");
+    hybrid.set_fault_hook(Some(hook));
+    ComposedBackend::new(log, hybrid, InProcessControlPlane::new())
+        .with_group_commit(true)
+        .recover()
+        .expect("recover objectlog/hybrid with fault hook installed")
+}
+
+fn hybrid_push_env(id: &str, key: &str) -> pqueue_engine::CommandEnvelope {
+    pqueue_conformance::envelope(
+        QueueCommand::Push(PushCommand {
+            items: vec![pqueue_conformance::item(id, key, 5)],
+        }),
+        vec![],
+    )
+}
+
+/// **AC-TXN-5** (TP-003 §3.10 row 210, `objectlog/hybrid-strict`): a fault struck strictly BETWEEN the
+/// durable SQLite commit and the in-memory apply — the ordering `HybridProjectionStore::apply` uses — must
+/// poison the store fail-closed until restart, and a restart must hydrate memory from the (already
+/// consistent) durable SQLite `ProjectionImage` without re-appending anything. Drives `HybridProjectionStore`
+/// DIRECTLY via `ProjectionStore` (bypassing `ComposedBackend`) for the poison/restart-hydration clauses,
+/// the same honest bypass AC-TXN-4 uses on the object-log side, because the public seam cannot isolate this
+/// exact instant; request-id semantics are then proven end-to-end through the real composed backend.
+async fn ac_txn_5_hybrid_strict_poison_replay_scenario() -> AcOutcome {
+    let mut asserts = Vec::new();
+    let shard = pqueue_conformance::shard();
+    let base = base_dir("hybrid-strict-poison");
+    std::fs::create_dir_all(&base).ok();
+    let path = base.join("projection.sqlite");
+    let pos0 = CommandPosition::new(shard.clone(), 0, 0);
+    let env0 = hybrid_push_env("1", "kx");
+
+    // --- AfterSqliteCommitBeforeMemoryApply: SQLite durably committed; the fault fires before memory
+    // observes it. The store must poison and fail every subsequent op closed, before any restart. ---
+    {
+        let mut hybrid =
+            HybridProjectionStore::open(path.to_str().unwrap()).expect("open hybrid projection");
+        ProjectionStore::ensure_shard(&mut hybrid, &pqueue_conformance::qdef())
+            .map_err(|e| format!("ensure_shard: {e:?}"))?;
+        hybrid.set_fault_hook(Some(Arc::new(HybridCrashAt(
+            HybridFaultCutPoint::AfterSqliteCommitBeforeMemoryApply,
+        ))));
+        let err = ProjectionStore::apply(
+            &mut hybrid,
+            std::slice::from_ref(&pos0),
+            std::slice::from_ref(&env0),
+        );
+        ensure!(
+            err.is_err(),
+            "a SQLite-commit-success/memory-apply-fail must not return success"
+        );
+        let after = ProjectionStore::metrics(&hybrid, &shard);
+        ensure!(
+            after.is_err(),
+            "the store must fail closed (reads included) after the poison, before restart; got {after:?}"
+        );
+    }
+    asserts.push(
+        "AfterSqliteCommitBeforeMemoryApply: a durable SQLite commit whose memory apply never runs poisons the store; every subsequent read fails closed before restart".into(),
+    );
+
+    // --- restart: reopen from the SAME durable SQLite file (no fault hook) and confirm memory hydrates
+    // from the SQLite ProjectionImage exactly, then that a same-(position,body) retry replays the original
+    // result without a second append. ---
+    {
+        let mut hybrid =
+            HybridProjectionStore::open(path.to_str().unwrap()).expect("reopen hybrid projection");
+        ProjectionStore::ensure_shard(&mut hybrid, &pqueue_conformance::qdef())
+            .map_err(|e| format!("ensure_shard after restart: {e:?}"))?;
+        let m = ProjectionStore::metrics(&hybrid, &shard)
+            .map_err(|e| format!("metrics after restart: {e:?}"))?;
+        ensure!(
+            m.pending == 1,
+            "restart must hydrate memory from the durable SQLite ProjectionImage (pending=1); got {m:?}"
+        );
+
+        ProjectionStore::apply(
+            &mut hybrid,
+            std::slice::from_ref(&pos0),
+            std::slice::from_ref(&env0),
+        )
+        .map_err(|e| format!("same-body retry apply: {e:?}"))?;
+        let m2 = ProjectionStore::metrics(&hybrid, &shard)
+            .map_err(|e| format!("metrics after retry: {e:?}"))?;
+        ensure!(
+            m2.pending == 1,
+            "same-body retry must replay the original result without a second append; got pending={}",
+            m2.pending
+        );
+    }
+    asserts.push(
+        "restart hydrates memory from the durable SQLite ProjectionImage; a same-(position,body) retry replays the original result without a second append".into(),
+    );
+
+    // --- request-id conflict semantics: proven end-to-end through a FRESH, healthy instance of the exact
+    // same objectlog/hybrid backend combination (the layer that owns request_id idempotency/conflict
+    // detection), independent of the poisoned instance above. ---
+    {
+        let make = objectlog_sqlite_factory();
+        let backend = make("rid-conflict");
+        backend
+            .create_queue(pqueue_conformance::qdef())
+            .await
+            .map_err(|e| format!("create_queue: {e:?}"))?;
+        let rid = RequestId::new("ac-txn-5-rid").unwrap();
+        let body = vec![pqueue_conformance::fault::spec("ac-txn-5-a", 1)];
+        let first = backend
+            .push_with_request_id(&shard, rid.clone(), body.clone(), pqueue_conformance::ts(1), None)
+            .await
+            .map_err(|e| format!("first request-id push: {e:?}"))?;
+        let replay = backend
+            .push_with_request_id(&shard, rid.clone(), body, pqueue_conformance::ts(2), None)
+            .await
+            .map_err(|e| format!("same-body retry: {e:?}"))?;
+        ensure!(
+            replay == first,
+            "same-body retry under the same request_id must replay the original result"
+        );
+        let conflict = backend
+            .push_with_request_id(
+                &shard,
+                rid,
+                vec![pqueue_conformance::fault::spec("ac-txn-5-different", 2)],
+                pqueue_conformance::ts(3),
+                None,
+            )
+            .await;
+        ensure!(
+            matches!(conflict, Err(EngineError::RequestIdConflict)),
+            "a conflicting body under the same request_id must return request-id-conflict; got {conflict:?}"
+        );
+    }
+    asserts.push(
+        "request-id semantics on the objectlog/hybrid substrate: same-body retry replays the original result; conflicting body returns request-id-conflict".into(),
+    );
+
+    Ok(asserts)
+}
+
+#[tokio::test]
+async fn ac_txn_5_hybrid_strict_poison_replay() {
+    let outcome = ac_txn_5_hybrid_strict_poison_replay_scenario().await;
+    assert!(
+        outcome.is_ok(),
+        "AC-TXN-5 hybrid-strict poison/replay failed: {:?}",
+        outcome.err()
+    );
+}
+
+/// **AC-TXN-5A** (TP-003 §3.10 row 211, `objectlog/hybrid-async`): the success barrier is manifest commit
+/// PLUS a completed synchronous memory apply — a memory-apply failure must not return success even though
+/// the manifest commit is durable; the deferred SQLite checkpoint applies a whole ordered batch exactly
+/// once; a crash before memory apply resolves as unknown-outcome by `request_id` (delegated to the generic
+/// AC-TXN-3 harness on this exact substrate); and async apply debt over budget fails new admission closed.
+async fn ac_txn_5a_hybrid_async_success_barrier_scenario() -> AcOutcome {
+    let mut asserts = Vec::new();
+    let shard = pqueue_conformance::shard();
+
+    // --- (a) success barrier: a memory-apply failure must not return success, even though the object-log
+    // manifest commit that preceded it IS durable. ---
+    {
+        let base = base_dir("hybrid-async-success-barrier");
+        let backend = objectlog_hybrid_with_fault_hook(
+            &base.join("run"),
+            Arc::new(HybridCrashAt(HybridFaultCutPoint::DuringMemoryApply)),
+        );
+        backend
+            .create_queue(pqueue_conformance::qdef())
+            .await
+            .map_err(|e| format!("create_queue: {e:?}"))?;
+        let rid = RequestId::new("ac-txn-5a-barrier").unwrap();
+        let body = vec![pqueue_conformance::fault::spec("ac-txn-5a-barrier-item", 5)];
+        let err = backend
+            .push_with_request_id(&shard, rid, body, pqueue_conformance::ts(1), None)
+            .await;
+        ensure!(
+            err.is_err(),
+            "manifest commit ALONE, without a completed synchronous memory apply, must not return success"
+        );
+        let durable = durable_command_count(&backend).await?;
+        ensure!(
+            durable == 1,
+            "the manifest commit is durable on the object log even though the success barrier withheld success; got {durable} durable commands"
+        );
+    }
+    asserts.push(
+        "success barrier: a memory-apply failure withholds success even though the preceding manifest commit is durable".into(),
+    );
+
+    // --- (b) ordered exactly-once batch apply: several live-applied deferred commands drain in ONE flush,
+    // in order, and the SQLite logical high-water advances through the whole batch exactly once. ---
+    {
+        let base = base_dir("hybrid-async-ordered-batch");
+        std::fs::create_dir_all(&base).ok();
+        let path = base.join("projection.sqlite");
+        let mut hybrid =
+            HybridProjectionStore::open(path.to_str().unwrap()).expect("open hybrid projection");
+        ProjectionStore::ensure_shard(&mut hybrid, &pqueue_conformance::qdef())
+            .map_err(|e| format!("ensure_shard: {e:?}"))?;
+
+        let batch: Vec<(CommandPosition, pqueue_engine::CommandEnvelope)> = (0..3)
+            .map(|i: u64| {
+                let id = (i + 1).to_string();
+                (
+                    CommandPosition::new(shard.clone(), 0, i),
+                    hybrid_push_env(&id, &format!("k{id}")),
+                )
+            })
+            .collect();
+        for (pos, env) in &batch {
+            ProjectionStore::apply_live(
+                &mut hybrid,
+                std::slice::from_ref(pos),
+                std::slice::from_ref(env),
+            )
+            .map_err(|e| format!("apply_live: {e:?}"))?;
+        }
+        ensure!(
+            hybrid.deferred_command_count() == 3,
+            "all 3 live-applied commands must be queued for deferred SQLite apply before any flush; got {}",
+            hybrid.deferred_command_count()
+        );
+        ProjectionStore::flush_deferred(&mut hybrid).map_err(|e| format!("flush_deferred: {e:?}"))?;
+        ensure!(
+            hybrid.deferred_command_count() == 0,
+            "one flush must drain the whole ordered batch exactly once; {} left deferred",
+            hybrid.deferred_command_count()
+        );
+        let hw = ProjectionStore::recovery_high_water(&hybrid, &shard)
+            .map_err(|e| format!("recovery_high_water: {e:?}"))?;
+        ensure!(
+            hw == Some(CommandPosition::new(shard.clone(), 0, 2)),
+            "the SQLite logical high-water must advance through the whole ordered batch (0,1,2) exactly once; got {hw:?}"
+        );
+        // A second flush with nothing pending is a true no-op — no duplicate SQLite work, no re-advance.
+        ProjectionStore::flush_deferred(&mut hybrid).map_err(|e| format!("second flush_deferred: {e:?}"))?;
+        let hw2 = ProjectionStore::recovery_high_water(&hybrid, &shard)
+            .map_err(|e| format!("recovery_high_water after no-op flush: {e:?}"))?;
+        ensure!(hw2 == hw, "a no-op flush must not move the high-water");
+    }
+    asserts.push(
+        "ordered batching: 3 live-applied commands drain in one flush, the SQLite high-water advances through the whole batch exactly once, and a no-op flush does not re-advance it".into(),
+    );
+
+    // --- (c) unknown-outcome-by-request_id: delegated to the generic AC-TXN-3 harness run against this
+    // exact objectlog/hybrid substrate — a crash after the durable append but before the response is
+    // observed resolves the request_id replay to the ONE committed result after restart. ---
+    let txn3 = ac_txn_3_unknown_outcome_replay(objectlog_sqlite_factory(), DURABLE).await?;
+    asserts.push(format!(
+        "unknown-outcome-by-request_id (delegated to AC-TXN-3 on the objectlog/hybrid substrate): {} assertions held",
+        txn3.len()
+    ));
+
+    // --- (d) backpressure fail-closed: once async apply debt trips Hard backpressure, new mutations are
+    // rejected retryably and the lagging high-water is withheld until the backlog drains below budget. ---
+    {
+        let thresholds =
+            HybridAsyncThresholds::new(100, 1_000_000, 100, 60_000, 3).expect("valid thresholds");
+        let mut monitor = HybridAsyncMonitor::new(thresholds);
+        let hw = CommandPosition::new(shard.clone(), 0, 41);
+        monitor.observe(
+            HybridAsyncDebt {
+                apply_lag_commands: 10,
+                ..Default::default()
+            },
+            0,
+        );
+        ensure!(monitor.admit_mutation().is_ok(), "clear debt must admit mutations");
+        monitor.observe(
+            HybridAsyncDebt {
+                apply_lag_commands: 100,
+                ..Default::default()
+            },
+            1,
+        );
+        ensure!(
+            matches!(monitor.admit_mutation(), Err(EngineError::Unavailable)),
+            "debt over the hard budget must reject new admission with a retryable error"
+        );
+        ensure!(
+            monitor.recovery_high_water_safe(Some(hw)).is_none(),
+            "the lagging high-water must not be advertised while debt is over budget"
+        );
+    }
+    asserts.push(
+        "backpressure fail-closed: async apply debt over the hard budget rejects new mutation admission and withholds the lagging high-water".into(),
+    );
+
+    Ok(asserts)
+}
+
+#[tokio::test]
+async fn ac_txn_5a_hybrid_async_success_barrier() {
+    let outcome = ac_txn_5a_hybrid_async_success_barrier_scenario().await;
+    assert!(
+        outcome.is_ok(),
+        "AC-TXN-5A hybrid-async success barrier failed: {:?}",
+        outcome.err()
+    );
+}
+
 #[tokio::test]
 async fn ac_txn_contract_matrix() {
     let mut records: Vec<AcEvidence> = Vec::new();
@@ -479,6 +841,24 @@ async fn ac_txn_contract_matrix() {
         ac_txn_4_crash_point_matrix().await,
     );
 
+    // --- AC-TXN-5 hybrid-strict poison + restart hydration + request-id semantics (see module doc). ---
+    record(
+        &mut records,
+        &mut failures,
+        "AC-TXN-5",
+        "object_log_sqlite(hybrid-strict)",
+        ac_txn_5_hybrid_strict_poison_replay_scenario().await,
+    );
+
+    // --- AC-TXN-5A hybrid-async success barrier + ordered batching + backpressure (see module doc). ---
+    record(
+        &mut records,
+        &mut failures,
+        "AC-TXN-5A",
+        "object_log_sqlite(hybrid-async)",
+        ac_txn_5a_hybrid_async_success_barrier_scenario().await,
+    );
+
     // --- AC-TXN-6 cross-combination parity (sqlite-log[atomic] vs object_log_sqlite[eventual]) ---
     record(&mut records, &mut failures, "AC-TXN-6", "sqlite_log|object_log_sqlite",
         ac_txn_6_parity(sqlite_log_factory(), objectlog_sqlite_factory()).await);
diff --git a/crates/pqueue-sqlite/src/lib.rs b/crates/pqueue-sqlite/src/lib.rs
index 91f429bc..9b57ec0a 100644
--- a/crates/pqueue-sqlite/src/lib.rs
+++ b/crates/pqueue-sqlite/src/lib.rs
@@ -14,9 +14,9 @@ pub use compose_log::SqliteLog;
 pub use relational::{
     BackpressureLevel, CheckpointLineage, CheckpointProgress, ComposedSqliteRelationalBackend,
     DEFAULT_DEFERRED_FLUSH_CHUNK, HybridAsyncDebt, HybridAsyncMetrics, HybridAsyncMonitor,
-    HybridAsyncThresholds, HybridProjectionStore, SqliteCheckpointStore, SqliteProjectionStore,
-    SqliteRelational, SqliteRelationalBackend, WalCheckpointStats, composed_sqlite_relational,
-    composed_sqlite_relational_in_memory,
+    HybridAsyncThresholds, HybridFaultCutPoint, HybridFaultHook, HybridProjectionStore,
+    SqliteCheckpointStore, SqliteProjectionStore, SqliteRelational, SqliteRelationalBackend,
+    WalCheckpointStats, composed_sqlite_relational, composed_sqlite_relational_in_memory,
 };
 
 use pqueue_engine::{ComposedBackend, EngineResult, InProcessControlPlane};
diff --git a/crates/pqueue-sqlite/src/relational.rs b/crates/pqueue-sqlite/src/relational.rs
index 9d3179b5..59c41441 100644
--- a/crates/pqueue-sqlite/src/relational.rs
+++ b/crates/pqueue-sqlite/src/relational.rs
@@ -4215,6 +4215,38 @@ pub struct SqliteProjectionStore {
 /// this bead) — chunk size was ruled out, not confirmed, as the lever for that gate.
 pub const DEFAULT_DEFERRED_FLUSH_CHUNK: usize = 250;
 
+// ---------------------------------------------------------------------------
+// Internal fault-injection seam (TP-003 §3.10 AC-TXN-5/5A)
+// ---------------------------------------------------------------------------
+//
+// The public `ProjectionStore` seam (`apply`/`apply_live`/`flush_deferred`) does not let a caller strike a
+// fault strictly BETWEEN the durable SQLite commit and the in-memory apply, or strictly inside the deferred
+// async SQLite checkpoint apply — those instants are internal to [`HybridProjectionStore`]'s own commit
+// pipeline. This test-only hook lets a test strike a "process died right here" fault at each of those named
+// instants and observe the durable/poison contract, mirroring `pqueue_objectlog::segmented`'s `FaultHook`
+// (AC-TXN-4) for the projection-apply side of the hybrid substrate.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum HybridFaultCutPoint {
+    /// The SQLite checkpoint for this batch committed durably, but the in-memory apply that makes it
+    /// client-visible has not run yet (the "hybrid-strict" `apply` ordering: SQLite, then memory).
+    AfterSqliteCommitBeforeMemoryApply,
+    /// Struck at the top of the in-memory apply step, shared by every apply path (`apply`, `apply_live`,
+    /// `apply_live_owned`, `apply_recovery`) — the success barrier every hybrid profile applies before
+    /// returning to the caller.
+    DuringMemoryApply,
+    /// Struck immediately before the deferred queue's batched SQLite checkpoint transaction (the
+    /// "hybrid-async" background apply that catches SQLite up to the in-memory high-water).
+    DuringAsyncSqliteApply,
+}
+
+/// A test-only fault hook for [`HybridProjectionStore`] (TP-003 §3.10 AC-TXN-5/5A). Returning `Err` aborts
+/// the pipeline at that instant; `Ok(())` (the default no-op behavior of not installing a hook at all) lets
+/// the pipeline run normally. Never invoked from any production call site — only `set_fault_hook` installs
+/// one, and nothing in this crate calls it outside tests.
+pub trait HybridFaultHook: Send + Sync {
+    fn fault_point(&self, cut: HybridFaultCutPoint) -> EngineResult<()>;
+}
+
 pub struct HybridProjectionStore {
     sqlite: SqliteProjectionStore,
     memory: InMemoryProjection,
@@ -4224,6 +4256,8 @@ pub struct HybridProjectionStore {
     deferred_commands: usize,
     deferred_flush_chunk: usize,
     poisoned: Option<String>,
+    /// Test-only fault-injection hook (TP-003 §3.10 AC-TXN-5/5A). `None` in every production path.
+    fault_hook: Mutex<Option<Arc<dyn HybridFaultHook>>>,
 }
 
 impl HybridProjectionStore {
@@ -4245,6 +4279,7 @@ impl HybridProjectionStore {
             deferred_commands: 0,
             deferred_flush_chunk: DEFAULT_DEFERRED_FLUSH_CHUNK,
             poisoned: None,
+            fault_hook: Mutex::new(None),
         }
     }
 
@@ -4259,6 +4294,26 @@ impl HybridProjectionStore {
             deferred_commands: 0,
             deferred_flush_chunk: DEFAULT_DEFERRED_FLUSH_CHUNK,
             poisoned: None,
+            fault_hook: Mutex::new(None),
+        }
+    }
+
+    /// Install (or clear, with `None`) a test-only fault hook (TP-003 §3.10 AC-TXN-5/5A). Never called from
+    /// any production call site.
+    pub fn set_fault_hook(&self, hook: Option<Arc<dyn HybridFaultHook>>) {
+        *self.fault_hook.lock().expect("hybrid fault hook poisoned") = hook;
+    }
+
+    /// Invoke the installed fault hook (if any) at `cut`. `Ok(())` when no hook is installed.
+    fn fault(&self, cut: HybridFaultCutPoint) -> EngineResult<()> {
+        let hook = self
+            .fault_hook
+            .lock()
+            .expect("hybrid fault hook poisoned")
+            .clone();
+        match hook {
+            Some(h) => h.fault_point(cut),
+            None => Ok(()),
         }
     }
 
@@ -4364,6 +4419,7 @@ impl HybridProjectionStore {
     ) -> EngineResult<()> {
         let mut advanced: HashMap<QueueKey, u64> = HashMap::new();
         let apply_result: EngineResult<()> = (|| {
+            self.fault(HybridFaultCutPoint::DuringMemoryApply)?;
             for (pos, env) in positions.iter().zip(commands.iter()) {
                 let next_seq = self.memory_next_seq.get(&pos.queue).copied().unwrap_or(0);
                 if pos.sequence >= next_seq {
@@ -4396,6 +4452,14 @@ impl HybridProjectionStore {
     ) -> EngineResult<()> {
         self.check_healthy()?;
         self.sqlite.apply_committed_batch(positions, commands)?;
+        if let Err(e) = self.fault(HybridFaultCutPoint::AfterSqliteCommitBeforeMemoryApply) {
+            // The SQLite checkpoint already committed durably; a memory apply that never runs would leave
+            // memory silently behind the durable image. Poison so every subsequent read/write fails closed
+            // until a restart re-hydrates memory from the (already-consistent) SQLite ProjectionImage.
+            return self.poison(format!(
+                "memory apply skipped after durable SQLite commit (fault injected): {e}"
+            ));
+        }
         self.apply_memory(positions, commands)
     }
 
@@ -9191,6 +9255,12 @@ impl ProjectionStore for HybridProjectionStore {
             .take(take)
             .map(|(_, env)| env.clone())
             .collect();
+        if let Err(e) = self.fault(HybridFaultCutPoint::DuringAsyncSqliteApply) {
+            // The deferred batch is untouched (still queued for the next flush attempt) but the async
+            // apply pipeline is no longer trustworthy: poison so it fails closed instead of silently
+            // retrying forever against a possibly-corrupt SQLite image.
+            return self.poison(format!("async SQLite checkpoint apply faulted: {e}"));
+        }
         self.sqlite.apply_committed_batch(&positions, &commands)?;
         self.deferred.drain(..take);
         self.deferred_commands = self.deferred.len();
diff --git a/docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl b/docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl
index bd3b747e..eb6eb453 100644
--- a/docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl
+++ b/docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl
@@ -1,3 +1,3 @@
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"postgres","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783558938"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"postgres","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783558938"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"postgres","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558938"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"postgres","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783560326"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"postgres","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783560326"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"postgres","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783560326"}
</untrusted-data>
  </diff>

  <strictness-mode mode="strict">strict — each AC must be anchored to a named Test* function or a diff-touched symbol; file-only evidence is insufficient.</strictness-mode>

  <instructions>
You are reviewing a bead implementation against its acceptance criteria.

## AC-Check Ratification

When an &lt;ac-check&gt; section is present, ratify the mechanical results rather
than re-verifying them independently from the diff:

- result="pass": confirm the evidence is credible. Override to fail only if
  the evidence is fabricated — include judgment_override_reason and a diff
  citation (file:line) in the per_ac evidence string.
- result="fail": mechanically verified failure. Grade as fail and BLOCK unless
  the commit message contains an explicit AC-Waive trailer for this AC.
- result="needs_judgment": adjudicate from the diff. If you cannot determine
  pass/fail without additional bead context from the operator, use
  REQUEST_CLARIFICATION for that AC item.
- result="error": treat as needs_judgment.

Overriding a mechanical grade (pass→fail or fail→pass) requires an explicit
judgment_override_reason note and a concrete diff citation in the evidence.

## Strictness Mode

The &lt;strictness-mode&gt; tag specifies per-bead evidence requirements:

- strict (kind:fix, kind:feat): each AC must be anchored to a named Test*
  function or a diff-touched symbol; file-only evidence is insufficient.
- behavior-light (kind:refactor, kind:chore): build green plus file/symbol
  evidence suffices; test-name match required only when an AC explicitly
  names a Test* function.
- mechanical (kind:doc, kind:mechanical): file presence, renames, or symbol
  evidence only; no test-name or runtime evidence required.

## Verdicts

For each acceptance-criteria (AC) item, decide whether it is implemented
correctly, then assign one overall verdict:

- APPROVE — every AC item is fully and correctly implemented.
- REQUEST_CHANGES — some AC items are partial or have fixable minor issues.
- BLOCK — at least one AC item is not implemented or incorrectly implemented;
  or the diff is insufficient to evaluate.
- REQUEST_CLARIFICATION — you cannot adjudicate one or more needs_judgment AC
  items without operator clarification. Use this ONLY when the item is
  ambiguous even given the full diff. This verdict does NOT block the queue;
  it routes to the operator lane for input.

## Required output format (schema_version: 1)

Respond with EXACTLY one JSON object as your final response, fenced as a single ```json … ``` code block. Do not include any prose outside the fenced block. The JSON must match this schema:

```json
{
  "schema_version": 1,
  "verdict": "APPROVE",
  "summary": "≤300 char human-readable verdict justification",
  "per_ac": [
    { "number": 1, "item": "acceptance criterion text", "grade": "pass", "evidence": "file:line or test evidence" }
  ],
  "findings": [
    { "severity": "info", "summary": "what is wrong or notable", "location": "path/to/file.go:42" }
  ]
}
```

Rules:
- "verdict" must be exactly one of "APPROVE", "REQUEST_CHANGES", "BLOCK", "REQUEST_CLARIFICATION".
- "severity" must be exactly one of "info", "warn", "block".
- Output the JSON object inside ONE fenced ```json … ``` block. No additional prose, no extra fences, no markdown headings.
- Do not echo this template back. Do not write the verdict value anywhere except as the JSON value of the verdict field.
  </instructions>
</bead-review>
