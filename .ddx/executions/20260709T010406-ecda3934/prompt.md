<bead-review>
  <bead id="pqueue-3b981b92" iter=1>
    <title>Object-log internal fault seam + AC-TXN-4 crash-point matrix</title>
    <description>
PROBLEM: AC-TXN-4 (TP-003 §3.10 row 209) requires injecting failures at object-log INTERNAL cut points — before segment write, after segment write before manifest, after manifest before projection apply, during projection apply, during snapshot write, during owner reassignment, and during manifest CAS/fallback commit — but the B3.1 fault harness only drives the PUBLIC Backend::write seam (append/apply), so AC-TXN-4 is currently recorded as partial (deeper cut points unreachable). ROOT CAUSE: the object-log segment/manifest/snapshot code has no test-injectable failure hook. PROPOSED FIX: add a test-only fault-injection seam (a cfg(test)/feature-gated failpoint hook or an injected FaultHook trait) at the named object-log cut points in crates/pqueue-objectlog (segment write, manifest commit/CAS, snapshot write, owner reassignment); extend external_transaction_contract_matrix_tests so AC-TXN-4 exercises each reachable cut point and asserts the TP-003 outcomes (0 lost accepted items, 0 duplicate active leases, committed commands replay exactly once, orphan segments ignored/reconciled, stale-epoch commits rejected); record any cut point still genuinely unreachable as an honest gap, not a fake pass. NON-SCOPE: hybrid-strict/async poison (AC-TXN-5/5A, separate bead); postgres wiring.
    </description>
    <acceptance>
1. `fault_injection_harness_tests` exercises object-log internal cut points (segment/manifest/snapshot) and passes.
2. `ac_txn_4_objectlog_crash_point_matrix` passes, asserting exactly-once replay + no lost items + stale-epoch reject across the reachable cut points.
3. `rustup run 1.92.0 cargo test -p pqueue-conformance -p pqueue-objectlog` passes.
4. `rg -n 'AC-TXN-4' docs/perf/evidence/tp003-ac-txn-matrix.jsonl` shows pass (not partial) for the covered cut points.
    </acceptance>
    <labels>kind:test, area:pqueue-objectlog, area:pqueue-conformance, gap-closure, phase-5, tp-003</labels>
  </bead>

  <changed-files>
    <file>crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs</file>
    <file>crates/pqueue-conformance/tests/fault_injection_harness_tests.rs</file>
    <file>crates/pqueue-objectlog/src/compose_log.rs</file>
    <file>crates/pqueue-objectlog/src/lib.rs</file>
    <file>crates/pqueue-objectlog/src/segmented.rs</file>
    <file>docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl</file>
    <file>docs/perf/evidence/tp003-ac-txn-matrix.jsonl</file>
  </changed-files>

  <governing>
    <note>No governing documents found. Evaluate the diff against the acceptance criteria alone.</note>
  </governing>

  <diff rev="d893a22238d2813b611590e5671c9357c2619290">
<untrusted-data>
diff --git a/docs/perf/evidence/tp003-ac-txn-matrix.jsonl b/docs/perf/evidence/tp003-ac-txn-matrix.jsonl
index 91b0e59a..7ed102af 100644
--- a/docs/perf/evidence/tp003-ac-txn-matrix.jsonl
+++ b/docs/perf/evidence/tp003-ac-txn-matrix.jsonl
@@ -1,15 +1,15 @@
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"memory","result":"n/a","detail":"non-durable in-memory dev profile: kill/restart durability is not applicable","assertions":[],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"memory","result":"partial","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","restart-replay clause N/A (non-durable in-memory dev profile)"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"memory","result":"partial","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply + AfterApplyBeforeResponse restart clauses N/A (non-durable in-memory dev profile)"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"sqlite_log","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"sqlite_log","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"sqlite_log","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"objectlog","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"objectlog","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"objectlog","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-4","backend":"objectlog","result":"partial","detail":"public-seam cut point only","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)","DOCUMENTED GAP: before-segment-write / after-segment-before-manifest / manifest-CAS / snapshot-write / owner-reassignment cut points need an objectlog-internal fault seam not exposed by the public commit API"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-6","backend":"sqlite_log|object_log_sqlite","result":"pass","detail":"","assertions":["identical final visible QueueMetrics (incl. complete/failed terminal counts), select_eligible order, and pending/active-lease set (item_id:attempt) across combinations; NOT compared: per-request_id idempotency records or per-item terminal-outcome records (metrics=QueueMetrics { pending: 3, leased: 0, complete: 1, failed: 0, resident_terminal_count: 1 }, eligible=[\"4\", \"3\", \"1\"], pending=[])"],"recorded_at":"epoch:1783550637"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-7","backend":"objectlog(force-seal|group-commit)","result":"partial","detail":"AC-TXN-3 invariance across commit-latency-bound settings","assertions":["force-seal AC-TXN-3 assertions == group-commit AC-TXN-3 assertions: true","BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783550637"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"memory","result":"n/a","detail":"non-durable in-memory dev profile: kill/restart durability is not applicable","assertions":[],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"memory","result":"partial","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","restart-replay clause N/A (non-durable in-memory dev profile)"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"memory","result":"partial","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply + AfterApplyBeforeResponse restart clauses N/A (non-durable in-memory dev profile)"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"sqlite_log","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"sqlite_log","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"sqlite_log","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"objectlog","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"objectlog","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"objectlog","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"object_log_sqlite","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-4","backend":"objectlog","result":"pass","detail":"","assertions":["BeforeSegmentWrite: 0 durable commands (0 lost accepted items)","AfterSegmentWriteBeforeManifest: orphan segments durably written but ignored by replay (0 lost, 0 duplicated)","AfterManifestBeforeAck: committed push AND claim commands replay exactly once on recovery (0 duplicate active leases)","DuringOwnerReassignment: epoch-fence commit survives a lost ack; stale-epoch commits rejected, current-epoch commits succeed","DuringSnapshotWrite: a lost snapshot write leaves the command log fully intact (0 lost items)"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-6","backend":"sqlite_log|object_log_sqlite","result":"pass","detail":"","assertions":["identical final visible QueueMetrics (incl. complete/failed terminal counts), select_eligible order, and pending/active-lease set (item_id:attempt) across combinations; NOT compared: per-request_id idempotency records or per-item terminal-outcome records (metrics=QueueMetrics { pending: 3, leased: 0, complete: 1, failed: 0, resident_terminal_count: 1 }, eligible=[\"4\", \"3\", \"1\"], pending=[])"],"recorded_at":"epoch:1783558940"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-7","backend":"objectlog(force-seal|group-commit)","result":"partial","detail":"AC-TXN-3 invariance across commit-latency-bound settings","assertions":["force-seal AC-TXN-3 assertions == group-commit AC-TXN-3 assertions: true","BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558940"}
diff --git a/crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs b/crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs
index c33b6b9f..819e2a2e 100644
--- a/crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs
+++ b/crates/pqueue-conformance/tests/external_transaction_contract_matrix_tests.rs
@@ -14,25 +14,34 @@
 //! | AC-TXN-1 durable+visible (per-op reopen) | n/a (non-durable) | ✓ | ✓ | ✓ | ✓ |
 //! | AC-TXN-2 rejection no-effect | partial (in-proc) | ✓ | ✓ | ✓ | ✓ |
 //! | AC-TXN-3 unknown-outcome replay | partial (in-proc) | ✓ full | ✓ full | ✓ full | ✓ full |
-//! | AC-TXN-4 objectlog crash-point matrix | — | — | partial (+gap*) | partial (+gap*) | — |
+//! | AC-TXN-4 objectlog crash-point matrix | — | — | ✓ (5 internal cut points)* | — | — |
 //! | AC-TXN-6 cross-combination parity | — | ✓ (sqlite-log vs objectlog+sqlite) | | | — |
 //! | AC-TXN-7 latency-bound invariance | — | — | partial (force-seal vs group-commit) | | — |
 //!
 //! AC-TXN-3's request_id-replay-across-restart is a REAL assertion on EVERY durable profile (atomic AND
 //! eventual-apply): `ComposedBackend` recovery rebuilds the push-idempotency map from the durable log for
 //! both durability classes (this suite's B3.1 run closed the earlier atomic-composed-log gap in
-//! `crates/pqueue-engine/src/compose.rs`). `*` the deeper objectlog cut points (segment-write,
-//! manifest-CAS, snapshot write, owner reassignment) are not reachable through the public commit seam and
-//! stay documented gaps that need an objectlog-internal fault seam.
+//! `crates/pqueue-engine/src/compose.rs`).
+//!
+//! `*` AC-TXN-4 drives [`pqueue_objectlog::ObjectLog`]'s `LogStore` impl DIRECTLY (bypassing
+//! `ComposedBackend`) with the [`pqueue_objectlog::FaultHook`] seam added for this row, striking 5 instants
+//! strictly INSIDE the segmented substrate's own commit pipeline that the public `Backend::write` seam
+//! cannot reach: `BeforeSegmentWrite`, `AfterSegmentWriteBeforeManifest`, `AfterManifestBeforeAck`,
+//! `DuringOwnerReassignment`, `DuringSnapshotWrite` (see `ac_txn_4_crash_point_matrix` below). One instant
+//! named in TP-003 §3.10 row 209 — a crash strictly DURING the composed backend's projection-apply step —
+//! lives in a distinct architectural layer (`pqueue-engine`'s `ComposedBackend`, which applies a batch only
+//! after `LogStore::append` already returned `Ok`) and is not internal to this crate; it stays a documented
+//! follow-up rather than a fake pass here.
 
+use std::sync::Arc;
 use std::sync::atomic::{AtomicU64, Ordering};
 
 use pqueue_conformance::fault::{
-    AcEvidence, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
+    AcEvidence, AcOutcome, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
     ac_txn_3_unknown_outcome_replay, ac_txn_6_parity, write_evidence,
 };
-use pqueue_engine::{ComposedBackend, InProcessControlPlane};
-use pqueue_objectlog::{ObjectLog, SegmentConfig};
+use pqueue_engine::{ClaimCommand, ComposedBackend, EngineError, InProcessControlPlane, LogStore, ProjectionSnapshot};
+use pqueue_objectlog::{FaultCutPoint, FaultHook, ObjectLog, SegmentConfig};
 use pqueue_sqlite::HybridProjectionStore;
 
 static COUNTER: AtomicU64 = AtomicU64::new(0);
@@ -141,6 +150,275 @@ fn record_na(records: &mut Vec<AcEvidence>, ac: &'static str, backend: &str, det
     });
 }
 
+// ---------------------------------------------------------------------------
+// AC-TXN-4: object-log-internal crash-point matrix (TP-003 §3.10 row 209)
+// ---------------------------------------------------------------------------
+//
+// `pqueue_conformance` deliberately depends only on the domain (engine + core) — adapters depend on IT,
+// not the reverse (see the crate doc) — so this scenario cannot live in `pqueue_conformance::fault`
+// alongside the AC-TXN-1..3/6 scenarios; it lives here, in the objectlog-specific test binary, which
+// already carries `pqueue_objectlog` as a dev-dependency. It drives `ObjectLog`'s `LogStore` impl DIRECTLY
+// (bypassing `ComposedBackend` entirely) so the [`FaultHook`] strikes instants strictly INSIDE the
+// segmented substrate's own commit pipeline that the public `Backend::write` seam cannot reach.
+
+/// Crashes (`Err`) every time the pipeline reaches `target`; a no-op at every other cut point.
+struct CrashAt(FaultCutPoint);
+
+impl FaultHook for CrashAt {
+    fn fault_point(&self, cut: FaultCutPoint) -> pqueue_engine::EngineResult<()> {
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
+fn objectlog_direct(base: &std::path::Path, tag: &str) -> (std::path::PathBuf, ObjectLog) {
+    let root = base.join(tag);
+    let log = ObjectLog::open(root.clone()).expect("open object log");
+    (root, log)
+}
+
+fn ac_txn_4_push_env(id: &str, key: &str) -> pqueue_engine::CommandEnvelope {
+    pqueue_conformance::envelope(
+        pqueue_engine::QueueCommand::Push(pqueue_engine::PushCommand {
+            items: vec![pqueue_conformance::item(id, key, 5)],
+        }),
+        vec![],
+    )
+}
+
+macro_rules! ensure {
+    ($cond:expr, $($arg:tt)*) => {
+        if !($cond) {
+            return Err(format!($($arg)*));
+        }
+    };
+}
+
+/// AC-TXN-4: strike 5 object-log-internal cut points and assert TP-003 §3.10's outcomes hold at each:
+/// 0 lost accepted items, 0 duplicate active leases, committed commands replay exactly once, orphan
+/// segments are ignored by replay, and stale-epoch commits are rejected.
+async fn ac_txn_4_crash_point_matrix() -> AcOutcome {
+    let mut asserts = Vec::new();
+    let shard = pqueue_conformance::shard();
+    let base = base_dir("objectlog-direct");
+
+    // --- BeforeSegmentWrite: nothing durable — 0 lost accepted items (nothing was ever accepted). ---
+    {
+        let (_root, mut log) = objectlog_direct(&base, "before-seg");
+        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
+        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::BeforeSegmentWrite))));
+        let err = log.append(&shard, &[ac_txn_4_push_env("1", "kx")], 0);
+        ensure!(err.is_err(), "BeforeSegmentWrite must abort the append");
+        let entries = log
+            .read_from(&shard, None, 100)
+            .map_err(|e| format!("read_from: {e:?}"))?
+            .entries;
+        ensure!(
+            entries.is_empty(),
+            "BeforeSegmentWrite left {} durable commands, expected 0",
+            entries.len()
+        );
+    }
+    asserts.push("BeforeSegmentWrite: 0 durable commands (0 lost accepted items)".into());
+
+    // --- AfterSegmentWriteBeforeManifest: a durable orphan segment must be ignored by replay, and a clean
+    // retry afterward must not be confused by it (0 lost, 0 duplicated).
+    {
+        let (_root, mut log) = objectlog_direct(&base, "after-seg-before-manifest");
+        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
+        let before = log.counters().objects_put;
+        log.set_fault_hook(Some(Arc::new(CrashAt(
+            FaultCutPoint::AfterSegmentWriteBeforeManifest,
+        ))));
+        let err = log.append(&shard, &[ac_txn_4_push_env("1", "orphan")], 0);
+        ensure!(
+            err.is_err(),
+            "AfterSegmentWriteBeforeManifest must abort the append"
+        );
+        ensure!(
+            log.counters().objects_put > before,
+            "the segment object was not genuinely durably written before the fault struck"
+        );
+        let entries = log
+            .read_from(&shard, None, 100)
+            .map_err(|e| format!("read_from: {e:?}"))?
+            .entries;
+        ensure!(
+            entries.is_empty(),
+            "the orphan segment surfaced on replay ({} entries)",
+            entries.len()
+        );
+        log.set_fault_hook(None);
+        log.append(&shard, &[ac_txn_4_push_env("2", "real")], 0)
+            .map_err(|e| format!("retry append: {e:?}"))?;
+        let entries = log
+            .read_from(&shard, None, 100)
+            .map_err(|e| format!("read_from: {e:?}"))?
+            .entries;
+        ensure!(
+            entries.len() == 1,
+            "orphan segment not cleanly ignored on retry; got {} entries",
+            entries.len()
+        );
+    }
+    asserts.push(
+        "AfterSegmentWriteBeforeManifest: orphan segments durably written but ignored by replay (0 lost, 0 duplicated)"
+            .into(),
+    );
+
+    // --- AfterManifestBeforeAck: committed commands replay exactly once. A Claim command crashed the SAME
+    // way must not resurrect as two leases (0 duplicate active leases).
+    {
+        let (root, mut log) = objectlog_direct(&base, "after-manifest-before-ack");
+        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
+        log.set_fault_hook(Some(Arc::new(CrashAt(
+            FaultCutPoint::AfterManifestBeforeAck,
+        ))));
+        let err = log.append(&shard, &[ac_txn_4_push_env("1", "committed-unacked")], 0);
+        ensure!(err.is_err(), "AfterManifestBeforeAck must abort the append");
+        drop(log);
+
+        let mut log2 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
+        log2.ensure_shard(&shard)
+            .map_err(|e| format!("ensure_shard: {e:?}"))?;
+        let entries = log2
+            .read_from(&shard, None, 100)
+            .map_err(|e| format!("read_from: {e:?}"))?
+            .entries;
+        ensure!(
+            entries.len() == 1,
+            "committed-but-unacked command did not replay exactly once; got {} entries",
+            entries.len()
+        );
+
+        log2.set_fault_hook(Some(Arc::new(CrashAt(
+            FaultCutPoint::AfterManifestBeforeAck,
+        ))));
+        let claim_env = pqueue_conformance::envelope(
+            pqueue_engine::QueueCommand::Claim(ClaimCommand {
+                item_ids: vec![pqueue_core::ItemId::new("1").unwrap()],
+                lease_token: pqueue_core::LeaseToken::new("lease-1").unwrap(),
+                lease_expires_at: pqueue_conformance::ts(500),
+            }),
+            vec![pqueue_core::ItemId::new("1").unwrap()],
+        );
+        let claim_err = log2.append(&shard, &[claim_env], 0);
+        ensure!(
+            claim_err.is_err(),
+            "the claim's AfterManifestBeforeAck fault must abort the append"
+        );
+        drop(log2);
+
+        let mut log3 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
+        log3.ensure_shard(&shard)
+            .map_err(|e| format!("ensure_shard: {e:?}"))?;
+        let claim_entries = log3
+            .read_from(&shard, None, 100)
+            .map_err(|e| format!("read_from: {e:?}"))?
+            .entries
+            .into_iter()
+            .filter(|(_, env)| matches!(env.command, pqueue_engine::QueueCommand::Claim(_)))
+            .count();
+        ensure!(
+            claim_entries == 1,
+            "0 duplicate active leases: expected exactly 1 committed claim command, got {}",
+            claim_entries
+        );
+    }
+    asserts.push(
+        "AfterManifestBeforeAck: committed push AND claim commands replay exactly once on recovery (0 duplicate active leases)"
+            .into(),
+    );
+
+    // --- DuringOwnerReassignment: the epoch-fence commit survives a lost ack; stale-epoch commits reject.
+    {
+        let (root, mut log) = objectlog_direct(&base, "owner-reassignment");
+        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
+        log.set_fault_hook(Some(Arc::new(CrashAt(
+            FaultCutPoint::DuringOwnerReassignment,
+        ))));
+        let err = log.acquire_epoch(&shard);
+        ensure!(err.is_err(), "DuringOwnerReassignment must abort acquire_epoch");
+        drop(log);
+
+        let mut log2 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
+        log2.ensure_shard(&shard)
+            .map_err(|e| format!("ensure_shard: {e:?}"))?;
+        let epoch = log2
+            .current_epoch(&shard)
+            .map_err(|e| format!("current_epoch: {e:?}"))?;
+        ensure!(
+            epoch == 1,
+            "the fence entry must durably commit even though the acquirer's ack was lost; got epoch {epoch}"
+        );
+        let stale = log2.append(&shard, &[ac_txn_4_push_env("1", "stale")], 0);
+        ensure!(
+            matches!(stale, Err(EngineError::EpochFenced)),
+            "a write at the superseded epoch must be fenced; got {stale:?}"
+        );
+        log2.append(&shard, &[ac_txn_4_push_env("2", "current")], 1)
+            .map_err(|e| format!("write at current epoch: {e:?}"))?;
+    }
+    asserts.push(
+        "DuringOwnerReassignment: epoch-fence commit survives a lost ack; stale-epoch commits rejected, current-epoch commits succeed"
+            .into(),
+    );
+
+    // --- DuringSnapshotWrite: a lost snapshot write must not lose or corrupt the command log.
+    {
+        let (_root, mut log) = objectlog_direct(&base, "snapshot-write");
+        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
+        let positions = log
+            .append(&shard, &[ac_txn_4_push_env("1", "before-snapshot")], 0)
+            .map_err(|e| format!("append: {e:?}"))?;
+        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::DuringSnapshotWrite))));
+        let err = log.write_snapshot(
+            &shard,
+            positions[0].clone(),
+            ProjectionSnapshot { payload: vec![9] },
+        );
+        ensure!(err.is_err(), "DuringSnapshotWrite must abort the snapshot write");
+        let latest = log
+            .latest_snapshot(&shard)
+            .map_err(|e| format!("latest_snapshot: {e:?}"))?;
+        ensure!(
+            latest.is_none(),
+            "a failed snapshot write left a committed snapshot ref"
+        );
+        let entries = log
+            .read_from(&shard, None, 100)
+            .map_err(|e| format!("read_from: {e:?}"))?
+            .entries;
+        ensure!(
+            entries.len() == 1,
+            "a lost snapshot write must not lose the command log; got {} entries",
+            entries.len()
+        );
+    }
+    asserts.push(
+        "DuringSnapshotWrite: a lost snapshot write leaves the command log fully intact (0 lost items)".into(),
+    );
+
+    Ok(asserts)
+}
+
+/// AC-TXN-4 as its own dedicated test (bead pqueue-3b981b92): the object-log-internal crash-point matrix
+/// must pass standalone, independent of the aggregate `ac_txn_contract_matrix` evidence run.
+#[tokio::test]
+async fn ac_txn_4_objectlog_crash_point_matrix() {
+    let outcome = ac_txn_4_crash_point_matrix().await;
+    assert!(
+        outcome.is_ok(),
+        "AC-TXN-4 crash-point matrix failed: {:?}",
+        outcome.err()
+    );
+}
+
 #[tokio::test]
 async fn ac_txn_contract_matrix() {
     let mut records: Vec<AcEvidence> = Vec::new();
@@ -192,17 +470,14 @@ async fn ac_txn_contract_matrix() {
     record(&mut records, &mut failures, "AC-TXN-3", "object_log_sqlite",
         ac_txn_3_unknown_outcome_replay(objectlog_sqlite_factory(), DURABLE).await);
 
-    // --- AC-TXN-4 object-log crash-point matrix (partial: only the public-seam cut point) ---
-    // The reachable objectlog cut point (append-durable-before-apply == "after segment write before
-    // projection apply") is proven exactly-once by AC-TXN-3 above on both objectlog profiles. The deeper
-    // internal cut points are documented gaps.
-    match ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await {
-        Ok(mut a) => {
-            a.push("DOCUMENTED GAP: before-segment-write / after-segment-before-manifest / manifest-CAS / snapshot-write / owner-reassignment cut points need an objectlog-internal fault seam not exposed by the public commit API".into());
-            records.push(AcEvidence { ac: "AC-TXN-4", backend: "objectlog".into(), result: "partial", detail: "public-seam cut point only".into(), assertions: a });
-        }
-        Err(e) => record(&mut records, &mut failures, "AC-TXN-4", "objectlog", Err(e)),
-    }
+    // --- AC-TXN-4 object-log-internal crash-point matrix (5 reachable cut points; see module doc). ---
+    record(
+        &mut records,
+        &mut failures,
+        "AC-TXN-4",
+        "objectlog",
+        ac_txn_4_crash_point_matrix().await,
+    );
 
     // --- AC-TXN-6 cross-combination parity (sqlite-log[atomic] vs object_log_sqlite[eventual]) ---
     record(&mut records, &mut failures, "AC-TXN-6", "sqlite_log|object_log_sqlite",
diff --git a/crates/pqueue-conformance/tests/fault_injection_harness_tests.rs b/crates/pqueue-conformance/tests/fault_injection_harness_tests.rs
index 967d58e2..9aaad68e 100644
--- a/crates/pqueue-conformance/tests/fault_injection_harness_tests.rs
+++ b/crates/pqueue-conformance/tests/fault_injection_harness_tests.rs
@@ -14,11 +14,12 @@ use std::sync::atomic::{AtomicU64, Ordering};
 use pqueue_conformance::fault::{CutPoint, durable_command_count, inject_commit, spec};
 use pqueue_conformance::{envelope, item, qdef, qkey, shard, ts};
 use pqueue_engine::{
-    ComposedBackend, ControlPlaneStore, InProcessControlPlane, ProjectionRead, PushCommand,
-    QueueCommand,
+    ComposedBackend, ControlPlaneStore, EngineError, EngineResult, InProcessControlPlane, LogStore,
+    ProjectionRead, ProjectionSnapshot, PushCommand, QueueCommand,
 };
-use pqueue_objectlog::{ObjectLog, SegmentConfig};
+use pqueue_objectlog::{FaultCutPoint, FaultHook, ObjectLog, SegmentConfig};
 use pqueue_sqlite::HybridProjectionStore;
+use std::sync::Arc;
 
 static COUNTER: AtomicU64 = AtomicU64::new(0);
 
@@ -246,3 +247,216 @@ async fn lost_response_replays_once_objectlog_sqlite() {
 async fn lost_response_replays_once_sqlite_log() {
     assert_lost_response_replays_once(&sqlite_log_factory()).await;
 }
+
+// ---------------------------------------------------------------------------
+// Object-log INTERNAL cut points (TP-003 §3.10 AC-TXN-4): segment write, manifest CAS/ack, owner
+// reassignment (epoch-fence CAS), and snapshot write. These are instants strictly INSIDE
+// `SegmentedObjectLog`'s own commit pipeline that the public `Backend::write` seam above cannot reach —
+// that seam can only crash before or after a whole `append`/`acquire_epoch`/`write_snapshot` call, not at
+// the durable sub-steps inside one. Driven directly against `ObjectLog`'s `LogStore` impl (bypassing
+// `ComposedBackend`) so the fault strikes the pipeline itself.
+// ---------------------------------------------------------------------------
+
+/// Crashes (`Err`) every time the pipeline reaches `target`; a no-op at every other cut point.
+struct CrashAt(FaultCutPoint);
+
+impl FaultHook for CrashAt {
+    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
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
+fn objectlog_direct(tag: &str) -> (std::path::PathBuf, ObjectLog) {
+    let root = unique_dir(tag);
+    let log = ObjectLog::open(root.clone()).expect("open object log");
+    (root, log)
+}
+
+fn push_env(id: &str, key: &str) -> pqueue_engine::CommandEnvelope {
+    envelope(
+        QueueCommand::Push(PushCommand {
+            items: vec![item(id, key, 5)],
+        }),
+        vec![],
+    )
+}
+
+#[test]
+fn objectlog_internal_before_segment_write_is_inert() {
+    let (_root, mut log) = objectlog_direct("cut-before-seg");
+    log.ensure_shard(&shard()).unwrap();
+    log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::BeforeSegmentWrite))));
+    let err = log.append(&shard(), &[push_env("1", "kx")], 0);
+    assert!(err.is_err(), "BeforeSegmentWrite must abort the append");
+    assert_eq!(
+        log.read_from(&shard(), None, 100).unwrap().entries.len(),
+        0,
+        "BeforeSegmentWrite must leave 0 durable commands (0 lost accepted items)"
+    );
+}
+
+#[test]
+fn objectlog_internal_after_segment_write_before_manifest_orphans_the_segment() {
+    let (_root, mut log) = objectlog_direct("cut-after-seg-before-manifest");
+    log.ensure_shard(&shard()).unwrap();
+    let before = log.counters().objects_put;
+    log.set_fault_hook(Some(Arc::new(CrashAt(
+        FaultCutPoint::AfterSegmentWriteBeforeManifest,
+    ))));
+    let err = log.append(&shard(), &[push_env("1", "orphan")], 0);
+    assert!(
+        err.is_err(),
+        "AfterSegmentWriteBeforeManifest must abort the append"
+    );
+    assert!(
+        log.counters().objects_put > before,
+        "the segment object was genuinely durably written before the fault struck"
+    );
+    assert_eq!(
+        log.read_from(&shard(), None, 100).unwrap().entries.len(),
+        0,
+        "the orphan segment (durable but unnamed by any manifest entry) must never surface on replay"
+    );
+    // Retry cleanly: recovery/replay must not be confused by the orphan segment left behind.
+    log.set_fault_hook(None);
+    let positions = log
+        .append(&shard(), &[push_env("2", "real")], 0)
+        .expect("retry after the orphaned segment succeeds");
+    assert_eq!(positions.len(), 1);
+    assert_eq!(
+        log.read_from(&shard(), None, 100).unwrap().entries.len(),
+        1,
+        "exactly the retried command is visible; the orphan never resurfaces"
+    );
+}
+
+#[test]
+fn objectlog_internal_after_manifest_before_ack_replays_exactly_once() {
+    let (root, mut log) = objectlog_direct("cut-after-manifest-before-ack");
+    log.ensure_shard(&shard()).unwrap();
+    log.set_fault_hook(Some(Arc::new(CrashAt(
+        FaultCutPoint::AfterManifestBeforeAck,
+    ))));
+    let err = log.append(&shard(), &[push_env("1", "committed-unacked")], 0);
+    assert!(
+        err.is_err(),
+        "the caller must observe the fault (the ack — and any downstream projection apply — was lost)"
+    );
+    drop(log);
+
+    // Reopen: recovery re-derives all state from the durable manifest tail, not from stale in-memory
+    // bookkeeping (the crash struck strictly after the manifest CAS had already committed).
+    let mut log2 = ObjectLog::open(root.clone()).expect("reopen object log");
+    log2.ensure_shard(&shard()).unwrap();
+    let entries = log2.read_from(&shard(), None, 100).unwrap().entries;
+    assert_eq!(
+        entries.len(),
+        1,
+        "committed commands replay exactly once on recovery, even though the ack was lost"
+    );
+
+    // A fresh append after recovery must not collide with or duplicate the recovered command.
+    let positions = log2
+        .append(&shard(), &[push_env("2", "after-recovery")], 0)
+        .expect("append after recovery");
+    assert_eq!(positions.len(), 1);
+    let entries2 = log2.read_from(&shard(), None, 100).unwrap().entries;
+    assert_eq!(
+        entries2.len(),
+        2,
+        "exactly-once: the recovered command plus the new one, 0 duplicate state transitions"
+    );
+}
+
+#[test]
+fn objectlog_internal_owner_reassignment_fence_survives_lost_ack_and_fences_stale_epoch() {
+    let (root, mut log) = objectlog_direct("cut-owner-reassignment");
+    log.ensure_shard(&shard()).unwrap();
+    assert_eq!(log.current_epoch(&shard()).unwrap(), 0);
+
+    log.set_fault_hook(Some(Arc::new(CrashAt(
+        FaultCutPoint::DuringOwnerReassignment,
+    ))));
+    let err = log.acquire_epoch(&shard());
+    assert!(
+        err.is_err(),
+        "the acquirer must observe the fault (the epoch-fence ack was lost)"
+    );
+    drop(log);
+
+    let mut log2 = ObjectLog::open(root.clone()).expect("reopen object log");
+    log2.ensure_shard(&shard()).unwrap();
+    assert_eq!(
+        log2.current_epoch(&shard()).unwrap(),
+        1,
+        "the fence entry durably committed even though the acquirer's own ack was lost"
+    );
+
+    // Stale-epoch commits are rejected from the durable manifest tail, not from in-memory state.
+    let stale = log2.append(&shard(), &[push_env("1", "stale-epoch")], 0);
+    assert!(
+        matches!(stale, Err(EngineError::EpochFenced)),
+        "a write at the superseded epoch must be fenced; got {stale:?}"
+    );
+    assert_eq!(
+        log2.read_from(&shard(), None, 100).unwrap().entries.len(),
+        0,
+        "a fenced write must append nothing"
+    );
+
+    // The new epoch is fully usable.
+    let ok = log2.append(&shard(), &[push_env("2", "current-epoch")], 1);
+    assert!(
+        ok.is_ok(),
+        "a write at the new current epoch must succeed: {ok:?}"
+    );
+}
+
+#[test]
+fn objectlog_internal_snapshot_write_failure_leaves_the_log_intact() {
+    let (_root, mut log) = objectlog_direct("cut-snapshot-write");
+    log.ensure_shard(&shard()).unwrap();
+    let positions = log
+        .append(&shard(), &[push_env("1", "before-snapshot")], 0)
+        .expect("append before snapshot");
+
+    log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::DuringSnapshotWrite))));
+    let err = log.write_snapshot(
+        &shard(),
+        positions[0].clone(),
+        ProjectionSnapshot {
+            payload: vec![1, 2, 3],
+        },
+    );
+    assert!(
+        err.is_err(),
+        "DuringSnapshotWrite must abort the snapshot write"
+    );
+    assert!(
+        log.latest_snapshot(&shard()).unwrap().is_none(),
+        "a failed snapshot write must leave no committed snapshot ref"
+    );
+    assert_eq!(
+        log.read_from(&shard(), None, 100).unwrap().entries.len(),
+        1,
+        "the command log itself is untouched by a lost snapshot write (0 lost items)"
+    );
+
+    // Retry cleanly.
+    log.set_fault_hook(None);
+    log.write_snapshot(
+        &shard(),
+        positions[0].clone(),
+        ProjectionSnapshot {
+            payload: vec![1, 2, 3],
+        },
+    )
+    .expect("retry succeeds");
+    assert!(log.latest_snapshot(&shard()).unwrap().is_some());
+}
diff --git a/crates/pqueue-objectlog/src/compose_log.rs b/crates/pqueue-objectlog/src/compose_log.rs
index 3e443a8c..54b99499 100644
--- a/crates/pqueue-objectlog/src/compose_log.rs
+++ b/crates/pqueue-objectlog/src/compose_log.rs
@@ -78,6 +78,12 @@ impl ObjectLog {
     pub fn counters(&self) -> crate::segmented::SegmentCounters {
         self.log.counters()
     }
+
+    /// Install (or clear, with `None`) a test-only fault hook on the underlying segmented substrate
+    /// (TP-003 §3.10 AC-TXN-4). See [`crate::segmented::FaultHook`] / [`crate::segmented::FaultCutPoint`].
+    pub fn set_fault_hook(&self, hook: Option<std::sync::Arc<dyn crate::segmented::FaultHook>>) {
+        self.log.set_fault_hook(hook);
+    }
 }
 
 impl LogStore for ObjectLog {
diff --git a/crates/pqueue-objectlog/src/lib.rs b/crates/pqueue-objectlog/src/lib.rs
index 467ccf22..c1a75979 100644
--- a/crates/pqueue-objectlog/src/lib.rs
+++ b/crates/pqueue-objectlog/src/lib.rs
@@ -23,7 +23,7 @@ pub use compose_log::{
     ComposedObjectLogBackend, ObjectLog, composed_objectlog_backend,
     composed_objectlog_backend_group_commit,
 };
-pub use segmented::SegmentConfig;
+pub use segmented::{FaultCutPoint, FaultHook, SegmentConfig};
 
 use std::collections::BTreeMap;
 use std::collections::HashMap;
diff --git a/crates/pqueue-objectlog/src/segmented.rs b/crates/pqueue-objectlog/src/segmented.rs
index ae378fdd..608de9db 100644
--- a/crates/pqueue-objectlog/src/segmented.rs
+++ b/crates/pqueue-objectlog/src/segmented.rs
@@ -35,6 +35,7 @@ use std::fs::OpenOptions;
 use std::io::{ErrorKind, Read, Write as _};
 use std::net::TcpStream;
 use std::path::{Path, PathBuf};
+use std::sync::Arc;
 use std::sync::Mutex;
 use std::sync::atomic::{AtomicU64, Ordering};
 
@@ -636,6 +637,53 @@ fn branch_metadata_key(branch: &QueueKey) -> String {
     format!("{}branch.json", shard_prefix(branch))
 }
 
+// ---------------------------------------------------------------------------
+// Internal fault-injection seam (TP-003 §3.10 AC-TXN-4)
+// ---------------------------------------------------------------------------
+//
+// The only commit-pipeline seam the engine exposes to a driver is `Backend::write` (append/apply as one
+// unit), which cannot strike the instants INSIDE this substrate's own group-commit pipeline: durable
+// segment write, durable manifest CAS commit, durable epoch-fence commit (owner reassignment), and durable
+// snapshot write are all internal to `SegmentedObjectLog::seal` / `acquire_epoch` / `write_snapshot`. This
+// seam is a test-only hook (never driven in production — no caller outside a test sets one) that lets a
+// test strike a "process died right here" fault at each of those named instants and observe the durable
+// footprint the crash leaves behind, so recovery/replay correctness can be asserted for real instead of
+// documented as an unreachable gap.
+
+/// The object-log-internal commit-pipeline instants a test can strike (TP-003 §3.10 AC-TXN-4). Each
+/// variant names a point strictly INSIDE the durable pipeline that the public `Backend::write` seam cannot
+/// reach because it treats a whole `append` (or `acquire_epoch`/`write_snapshot`) as one opaque call.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum FaultCutPoint {
+    /// Kill before the sealed segment object is durably written. Nothing durable exists yet — equivalent
+    /// in spirit to the public-seam `BeforeAppend`, but internal to this substrate's own pipeline.
+    BeforeSegmentWrite,
+    /// Kill after the segment object is durably written but before the manifest CAS is attempted. The
+    /// segment is now an ORPHAN: durable on the store but named by no committed manifest entry, so replay
+    /// (which only trusts the manifest) must never surface it.
+    AfterSegmentWriteBeforeManifest,
+    /// Kill after the manifest CAS durably commits (the TD-004 ack boundary — the manifest entry names the
+    /// segment and is now the durable source of truth) but before the caller receives the acked positions.
+    /// This is strictly before the composed backend's projection apply, since `ComposedBackend` only
+    /// applies a batch after its `LogStore::append` call returns `Ok`; recovery must replay the
+    /// manifest-committed segment exactly once even though the ack (and therefore the apply) was lost.
+    AfterManifestBeforeAck,
+    /// Kill after an epoch-fence entry durably commits to the manifest (owner reassignment /
+    /// `acquire_epoch`) but before the acquirer's local bookkeeping observes the new epoch. A stale-epoch
+    /// writer's next commit must still be rejected from the durable manifest tail, not from in-memory state.
+    DuringOwnerReassignment,
+    /// Kill before a projection snapshot blob is durably written. The command log remains the sole source
+    /// of truth, so a lost snapshot write must not lose or corrupt any committed command.
+    DuringSnapshotWrite,
+}
+
+/// A test-only fault hook: called at each [`FaultCutPoint`] the pipeline passes through. Returning `Err`
+/// simulates a process death at that instant (the in-flight operation aborts there); returning `Ok(())`
+/// (the default no-op behavior of not installing a hook at all) lets the pipeline run normally.
+pub trait FaultHook: Send + Sync {
+    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()>;
+}
+
 // ---------------------------------------------------------------------------
 // The segmented object log
 // ---------------------------------------------------------------------------
@@ -676,6 +724,8 @@ pub struct SegmentedObjectLog<S: BlobStore> {
     store: S,
     config: SegmentConfig,
     inner: Mutex<Inner>,
+    /// Test-only fault-injection hook (TP-003 §3.10 AC-TXN-4). `None` in every production path.
+    fault_hook: Mutex<Option<Arc<dyn FaultHook>>>,
 }
 
 impl<S: BlobStore> SegmentedObjectLog<S> {
@@ -689,6 +739,22 @@ impl<S: BlobStore> SegmentedObjectLog<S> {
                 counters: SegmentCounters::default(),
                 object_sizes: BTreeMap::new(),
             }),
+            fault_hook: Mutex::new(None),
+        }
+    }
+
+    /// Install (or clear, with `None`) a test-only fault hook (TP-003 §3.10 AC-TXN-4). Never called from
+    /// production code paths.
+    pub fn set_fault_hook(&self, hook: Option<Arc<dyn FaultHook>>) {
+        *self.fault_hook.lock().expect("fault hook poisoned") = hook;
+    }
+
+    /// Invoke the installed fault hook (if any) at `cut`. `Ok(())` when no hook is installed.
+    fn fault(&self, cut: FaultCutPoint) -> EngineResult<()> {
+        let hook = self.fault_hook.lock().expect("fault hook poisoned").clone();
+        match hook {
+            Some(h) => h.fault_point(cut),
+            None => Ok(()),
         }
     }
 
@@ -882,6 +948,9 @@ impl<S: BlobStore> SegmentedObjectLog<S> {
             };
             let key = format!("{prefix}manifest/{next_index:020}.json");
             if self.store_put_if_absent(&key, &to_json(&entry)?, true)? {
+                // The fence entry just won its CAS: the epoch handoff is now durably committed to the
+                // manifest, even though this acquirer's own in-memory bookkeeping has not yet observed it.
+                self.fault(FaultCutPoint::DuringOwnerReassignment)?;
                 let mut g = self.inner.lock().expect("segmented log poisoned");
                 if let Some(buf) = g.shards.get_mut(shard) {
                     buf.committed_epoch = new_epoch;
@@ -998,6 +1067,8 @@ impl<S: BlobStore> SegmentedObjectLog<S> {
             (buf.next_seq, buf.next_manifest_index, buf.committed_epoch)
         };
 
+        self.fault(FaultCutPoint::BeforeSegmentWrite)?;
+
         // 3. Write the immutable, checksummed segment object (idempotent at its first-seq key). The segment
         //    is the framed concatenation of the per-command bytes serialized once at buffer time — no
         //    re-serialize on seal (Fix A). The checksum covers the records-blob region.
@@ -1007,6 +1078,8 @@ impl<S: BlobStore> SegmentedObjectLog<S> {
         let seg_key = format!("{prefix}seg/{first_seq:020}.seg");
         self.store_put_segment(&seg_key, &seg_bytes)?;
 
+        self.fault(FaultCutPoint::AfterSegmentWriteBeforeManifest)?;
+
         // 4. Commit the manifest entry via the create-only CAS at the next index.
         let entry = ManifestEntry {
             index: cur_index,
@@ -1036,6 +1109,12 @@ impl<S: BlobStore> SegmentedObjectLog<S> {
             return Err(EngineError::Conflict);
         }
 
+        // The manifest CAS just won: the segment is now named by a durably committed manifest entry (the
+        // TD-004 ack boundary). A fault struck here models a crash after that durable commit but before the
+        // ack (and therefore before any projection apply, which only ever runs after this call returns
+        // `Ok`) reaches the caller.
+        self.fault(FaultCutPoint::AfterManifestBeforeAck)?;
+
         // 5. Ack: the manifest entry is durable. Advance state + counters, then return positions.
         let mut positions = Vec::with_capacity(n);
         for i in 0..n {
@@ -1434,6 +1513,7 @@ impl<S: BlobStore> SegmentedObjectLog<S> {
             payload: payload.to_vec(),
         };
         let key = format!("{prefix}{ref_id}.json");
+        self.fault(FaultCutPoint::DuringSnapshotWrite)?;
         self.store_put(&key, &to_json(&blob)?, false)?;
         Ok(ref_id)
     }
diff --git a/docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl b/docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl
index 3b36216f..bd3b747e 100644
--- a/docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl
+++ b/docs/perf/evidence/tp003-ac-txn-matrix-postgres.jsonl
@@ -1,3 +1,3 @@
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"postgres","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783550635"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"postgres","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783550635"}
-{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"postgres","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783550635"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-1","backend":"postgres","result":"pass","detail":"","assertions":["BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)","BatchClaim lease durable after kill/reopen","BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)","BatchFinalize terminal state durable after kill/reopen; sibling still claimable"],"recorded_at":"epoch:1783558938"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-2","backend":"postgres","result":"pass","detail":"","assertions":["rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect","accepted siblings survive restart with 0 phantom commits from rejects"],"recorded_at":"epoch:1783558938"}
+{"suite":"external_transaction_contract_matrix_tests","spec":"TP-003 §3.10","ac":"AC-TXN-3","backend":"postgres","result":"pass","detail":"","assertions":["BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once","AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)","AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)"],"recorded_at":"epoch:1783558938"}
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
