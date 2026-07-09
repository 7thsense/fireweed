//! `external_transaction_contract_matrix_tests` (TP-001 lines 148/199, TP-002 line 222) — the
//! API-001 external-transaction contract (TP-003 §3.10, AC-TXN-1..7) run across backend PROFILES under
//! fault injection, using the reusable [`pqueue_conformance::fault`] harness.
//!
//! Each row is a REAL run: the scenario functions drive live backends through the [`Backend::write`]
//! commit seam and the [`PushPort::push_with_request_id`] idempotency path, simulate process kills by
//! drop+reopen of durable state, and assert the invariant. Every row (pass / n-a / documented-gap) is
//! written to `docs/perf/evidence/tp003-ac-txn-matrix.jsonl` so the evidence reflects exactly this run.
//!
//! # Honest coverage map (see per-row `assertions`/`detail` in the evidence JSONL)
//!
//! | AC | memory | sqlite-log | objectlog | objectlog+sqlite | postgres (env) |
//! |----|--------|-----------|-----------|------------------|----------------|
//! | AC-TXN-1 durable+visible (per-op reopen) | n/a (non-durable) | ✓ | ✓ | ✓ | ✓ |
//! | AC-TXN-2 rejection no-effect | partial (in-proc) | ✓ | ✓ | ✓ | ✓ |
//! | AC-TXN-3 unknown-outcome replay | partial (in-proc) | ✓ full | ✓ full | ✓ full | ✓ full |
//! | AC-TXN-4 objectlog crash-point matrix | — | — | ✓ (5 internal cut points)* | — | — |
//! | AC-TXN-6 cross-combination parity | — | ✓ (sqlite-log vs objectlog+sqlite) | | | — |
//! | AC-TXN-7 latency-bound invariance | — | — | partial (force-seal vs group-commit) | | — |
//!
//! AC-TXN-3's request_id-replay-across-restart is a REAL assertion on EVERY durable profile (atomic AND
//! eventual-apply): `ComposedBackend` recovery rebuilds the push-idempotency map from the durable log for
//! both durability classes (this suite's B3.1 run closed the earlier atomic-composed-log gap in
//! `crates/pqueue-engine/src/compose.rs`).
//!
//! `*` AC-TXN-4 drives [`pqueue_objectlog::ObjectLog`]'s `LogStore` impl DIRECTLY (bypassing
//! `ComposedBackend`) with the [`pqueue_objectlog::FaultHook`] seam added for this row, striking 5 instants
//! strictly INSIDE the segmented substrate's own commit pipeline that the public `Backend::write` seam
//! cannot reach: `BeforeSegmentWrite`, `AfterSegmentWriteBeforeManifest`, `AfterManifestBeforeAck`,
//! `DuringOwnerReassignment`, `DuringSnapshotWrite` (see `ac_txn_4_crash_point_matrix` below). One instant
//! named in TP-003 §3.10 row 209 — a crash strictly DURING the composed backend's projection-apply step —
//! lives in a distinct architectural layer (`pqueue-engine`'s `ComposedBackend`, which applies a batch only
//! after `LogStore::append` already returned `Ok`) and is not internal to this crate; it stays a documented
//! follow-up rather than a fake pass here.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::fault::{
    AcEvidence, AcOutcome, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
    ac_txn_3_unknown_outcome_replay, ac_txn_6_parity, write_evidence,
};
use pqueue_engine::{ClaimCommand, ComposedBackend, EngineError, InProcessControlPlane, LogStore, ProjectionSnapshot};
use pqueue_objectlog::{FaultCutPoint, FaultHook, ObjectLog, SegmentConfig};
use pqueue_sqlite::HybridProjectionStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A base directory unique to THIS process+profile-instance. The tag-keyed factories below join the
/// per-scenario `tag` onto this base, so reopening with the same tag recovers the same durable store
/// while different tags (independent AC-TXN cut-point phases) get isolated stores.
fn base_dir(profile: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("pqueue-ac-txn-{profile}-{}-{n}", std::process::id()))
}

fn sqlite_log_factory() -> impl Fn(&str) -> pqueue_sqlite::ComposedSqliteBackend {
    let base = base_dir("sqlite");
    move |tag: &str| {
        let path = base.join(format!("{tag}.db"));
        std::fs::create_dir_all(&base).ok();
        pqueue_sqlite::composed_sqlite_backend(path.to_str().unwrap()).expect("open composed sqlite-log")
    }
}

fn objectlog_factory() -> impl Fn(&str) -> pqueue_objectlog::ComposedObjectLogBackend {
    let base = base_dir("objectlog");
    move |tag: &str| {
        pqueue_objectlog::composed_objectlog_backend(base.join(tag)).expect("open composed objectlog")
    }
}

fn objectlog_group_commit_factory() -> impl Fn(&str) -> pqueue_objectlog::ComposedObjectLogBackend {
    let base = base_dir("objectlog-gc");
    move |tag: &str| {
        pqueue_objectlog::composed_objectlog_backend_group_commit(
            base.join(tag),
            SegmentConfig::new(1, 1).unwrap(),
        )
        .expect("open composed objectlog group-commit")
    }
}

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

fn objectlog_sqlite_factory() -> impl Fn(&str) -> HybridBackend {
    let base = base_dir("objectlog-sqlite");
    move |tag: &str| {
        let root = base.join(tag);
        std::fs::create_dir_all(&root).ok();
        let sqlite = root.join("projection.sqlite");
        ComposedBackend::new(
            ObjectLog::open_group_commit(&root, SegmentConfig::new(1, 1).unwrap())
                .expect("open object log"),
            HybridProjectionStore::open(sqlite.to_str().unwrap()).expect("open hybrid projection"),
            InProcessControlPlane::new(),
        )
        .with_group_commit(true)
        .recover()
        .expect("recover objectlog/hybrid")
    }
}

const DURABLE: TxnCaps = TxnCaps { durable_reopen: true };
const NON_DURABLE: TxnCaps = TxnCaps { durable_reopen: false };

/// Record one AC-TXN outcome into the evidence buffer, tracking failures for the final assertion. A
/// passing row whose assertions include an explicit "N/A" (a clause inapplicable to the profile) or a
/// "GAP" note is recorded as `partial`, never `pass`, so status never overclaims coverage.
fn record(
    records: &mut Vec<AcEvidence>,
    failures: &mut Vec<String>,
    ac: &'static str,
    backend: &str,
    outcome: Result<Vec<String>, String>,
) {
    match outcome {
        Ok(assertions) => {
            let partial = assertions
                .iter()
                .any(|a| a.contains("N/A") || a.contains("GAP"));
            records.push(AcEvidence {
                ac,
                backend: backend.to_string(),
                result: if partial { "partial" } else { "pass" },
                detail: String::new(),
                assertions,
            });
        }
        Err(reason) => {
            failures.push(format!("{ac} [{backend}]: {reason}"));
            records.push(AcEvidence {
                ac,
                backend: backend.to_string(),
                result: "fail",
                detail: reason,
                assertions: vec![],
            });
        }
    }
}

fn record_na(records: &mut Vec<AcEvidence>, ac: &'static str, backend: &str, detail: &str) {
    records.push(AcEvidence {
        ac,
        backend: backend.to_string(),
        result: "n/a",
        detail: detail.to_string(),
        assertions: vec![],
    });
}

// ---------------------------------------------------------------------------
// AC-TXN-4: object-log-internal crash-point matrix (TP-003 §3.10 row 209)
// ---------------------------------------------------------------------------
//
// `pqueue_conformance` deliberately depends only on the domain (engine + core) — adapters depend on IT,
// not the reverse (see the crate doc) — so this scenario cannot live in `pqueue_conformance::fault`
// alongside the AC-TXN-1..3/6 scenarios; it lives here, in the objectlog-specific test binary, which
// already carries `pqueue_objectlog` as a dev-dependency. It drives `ObjectLog`'s `LogStore` impl DIRECTLY
// (bypassing `ComposedBackend` entirely) so the [`FaultHook`] strikes instants strictly INSIDE the
// segmented substrate's own commit pipeline that the public `Backend::write` seam cannot reach.

/// Crashes (`Err`) every time the pipeline reaches `target`; a no-op at every other cut point.
struct CrashAt(FaultCutPoint);

impl FaultHook for CrashAt {
    fn fault_point(&self, cut: FaultCutPoint) -> pqueue_engine::EngineResult<()> {
        if cut == self.0 {
            Err(EngineError::Storage(format!(
                "fault-injection: crash at {cut:?}"
            )))
        } else {
            Ok(())
        }
    }
}

fn objectlog_direct(base: &std::path::Path, tag: &str) -> (std::path::PathBuf, ObjectLog) {
    let root = base.join(tag);
    let log = ObjectLog::open(root.clone()).expect("open object log");
    (root, log)
}

fn ac_txn_4_push_env(id: &str, key: &str) -> pqueue_engine::CommandEnvelope {
    pqueue_conformance::envelope(
        pqueue_engine::QueueCommand::Push(pqueue_engine::PushCommand {
            items: vec![pqueue_conformance::item(id, key, 5)],
        }),
        vec![],
    )
}

macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            return Err(format!($($arg)*));
        }
    };
}

/// AC-TXN-4: strike 5 object-log-internal cut points and assert TP-003 §3.10's outcomes hold at each:
/// 0 lost accepted items, 0 duplicate active leases, committed commands replay exactly once, orphan
/// segments are ignored by replay, and stale-epoch commits are rejected.
async fn ac_txn_4_crash_point_matrix() -> AcOutcome {
    let mut asserts = Vec::new();
    let shard = pqueue_conformance::shard();
    let base = base_dir("objectlog-direct");

    // --- BeforeSegmentWrite: nothing durable — 0 lost accepted items (nothing was ever accepted). ---
    {
        let (_root, mut log) = objectlog_direct(&base, "before-seg");
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::BeforeSegmentWrite))));
        let err = log.append(&shard, &[ac_txn_4_push_env("1", "kx")], 0);
        ensure!(err.is_err(), "BeforeSegmentWrite must abort the append");
        let entries = log
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.is_empty(),
            "BeforeSegmentWrite left {} durable commands, expected 0",
            entries.len()
        );
    }
    asserts.push("BeforeSegmentWrite: 0 durable commands (0 lost accepted items)".into());

    // --- AfterSegmentWriteBeforeManifest: a durable orphan segment must be ignored by replay, and a clean
    // retry afterward must not be confused by it (0 lost, 0 duplicated).
    {
        let (_root, mut log) = objectlog_direct(&base, "after-seg-before-manifest");
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
        let before = log.counters().objects_put;
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::AfterSegmentWriteBeforeManifest,
        ))));
        let err = log.append(&shard, &[ac_txn_4_push_env("1", "orphan")], 0);
        ensure!(
            err.is_err(),
            "AfterSegmentWriteBeforeManifest must abort the append"
        );
        ensure!(
            log.counters().objects_put > before,
            "the segment object was not genuinely durably written before the fault struck"
        );
        let entries = log
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.is_empty(),
            "the orphan segment surfaced on replay ({} entries)",
            entries.len()
        );
        log.set_fault_hook(None);
        log.append(&shard, &[ac_txn_4_push_env("2", "real")], 0)
            .map_err(|e| format!("retry append: {e:?}"))?;
        let entries = log
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.len() == 1,
            "orphan segment not cleanly ignored on retry; got {} entries",
            entries.len()
        );
    }
    asserts.push(
        "AfterSegmentWriteBeforeManifest: orphan segments durably written but ignored by replay (0 lost, 0 duplicated)"
            .into(),
    );

    // --- AfterManifestBeforeAck: committed commands replay exactly once. A Claim command crashed the SAME
    // way must not resurrect as two leases (0 duplicate active leases).
    {
        let (root, mut log) = objectlog_direct(&base, "after-manifest-before-ack");
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::AfterManifestBeforeAck,
        ))));
        let err = log.append(&shard, &[ac_txn_4_push_env("1", "committed-unacked")], 0);
        ensure!(err.is_err(), "AfterManifestBeforeAck must abort the append");
        drop(log);

        let mut log2 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
        log2.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let entries = log2
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.len() == 1,
            "committed-but-unacked command did not replay exactly once; got {} entries",
            entries.len()
        );

        log2.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::AfterManifestBeforeAck,
        ))));
        let claim_env = pqueue_conformance::envelope(
            pqueue_engine::QueueCommand::Claim(ClaimCommand {
                item_ids: vec![pqueue_core::ItemId::new("1").unwrap()],
                lease_token: pqueue_core::LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: pqueue_conformance::ts(500),
            }),
            vec![pqueue_core::ItemId::new("1").unwrap()],
        );
        let claim_err = log2.append(&shard, &[claim_env], 0);
        ensure!(
            claim_err.is_err(),
            "the claim's AfterManifestBeforeAck fault must abort the append"
        );
        drop(log2);

        let mut log3 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
        log3.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let claim_entries = log3
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries
            .into_iter()
            .filter(|(_, env)| matches!(env.command, pqueue_engine::QueueCommand::Claim(_)))
            .count();
        ensure!(
            claim_entries == 1,
            "0 duplicate active leases: expected exactly 1 committed claim command, got {}",
            claim_entries
        );
    }
    asserts.push(
        "AfterManifestBeforeAck: committed push AND claim commands replay exactly once on recovery (0 duplicate active leases)"
            .into(),
    );

    // --- DuringOwnerReassignment: the epoch-fence commit survives a lost ack; stale-epoch commits reject.
    {
        let (root, mut log) = objectlog_direct(&base, "owner-reassignment");
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(
            FaultCutPoint::DuringOwnerReassignment,
        ))));
        let err = log.acquire_epoch(&shard);
        ensure!(err.is_err(), "DuringOwnerReassignment must abort acquire_epoch");
        drop(log);

        let mut log2 = ObjectLog::open(root.clone()).map_err(|e| format!("reopen: {e:?}"))?;
        log2.ensure_shard(&shard)
            .map_err(|e| format!("ensure_shard: {e:?}"))?;
        let epoch = log2
            .current_epoch(&shard)
            .map_err(|e| format!("current_epoch: {e:?}"))?;
        ensure!(
            epoch == 1,
            "the fence entry must durably commit even though the acquirer's ack was lost; got epoch {epoch}"
        );
        let stale = log2.append(&shard, &[ac_txn_4_push_env("1", "stale")], 0);
        ensure!(
            matches!(stale, Err(EngineError::EpochFenced)),
            "a write at the superseded epoch must be fenced; got {stale:?}"
        );
        log2.append(&shard, &[ac_txn_4_push_env("2", "current")], 1)
            .map_err(|e| format!("write at current epoch: {e:?}"))?;
    }
    asserts.push(
        "DuringOwnerReassignment: epoch-fence commit survives a lost ack; stale-epoch commits rejected, current-epoch commits succeed"
            .into(),
    );

    // --- DuringSnapshotWrite: a lost snapshot write must not lose or corrupt the command log.
    {
        let (_root, mut log) = objectlog_direct(&base, "snapshot-write");
        log.ensure_shard(&shard).map_err(|e| format!("ensure_shard: {e:?}"))?;
        let positions = log
            .append(&shard, &[ac_txn_4_push_env("1", "before-snapshot")], 0)
            .map_err(|e| format!("append: {e:?}"))?;
        log.set_fault_hook(Some(Arc::new(CrashAt(FaultCutPoint::DuringSnapshotWrite))));
        let err = log.write_snapshot(
            &shard,
            positions[0].clone(),
            ProjectionSnapshot { payload: vec![9] },
        );
        ensure!(err.is_err(), "DuringSnapshotWrite must abort the snapshot write");
        let latest = log
            .latest_snapshot(&shard)
            .map_err(|e| format!("latest_snapshot: {e:?}"))?;
        ensure!(
            latest.is_none(),
            "a failed snapshot write left a committed snapshot ref"
        );
        let entries = log
            .read_from(&shard, None, 100)
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.len() == 1,
            "a lost snapshot write must not lose the command log; got {} entries",
            entries.len()
        );
    }
    asserts.push(
        "DuringSnapshotWrite: a lost snapshot write leaves the command log fully intact (0 lost items)".into(),
    );

    Ok(asserts)
}

/// AC-TXN-4 as its own dedicated test (bead pqueue-3b981b92): the object-log-internal crash-point matrix
/// must pass standalone, independent of the aggregate `ac_txn_contract_matrix` evidence run.
#[tokio::test]
async fn ac_txn_4_objectlog_crash_point_matrix() {
    let outcome = ac_txn_4_crash_point_matrix().await;
    assert!(
        outcome.is_ok(),
        "AC-TXN-4 crash-point matrix failed: {:?}",
        outcome.err()
    );
}

#[tokio::test]
async fn ac_txn_contract_matrix() {
    let mut records: Vec<AcEvidence> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    // --- memory (composed in-memory, atomic, NON-durable dev profile) ---
    record_na(
        &mut records,
        "AC-TXN-1",
        "memory",
        "non-durable in-memory dev profile: kill/restart durability is not applicable",
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-2",
        "memory",
        ac_txn_2_rejection_no_effect(|_: &str| pqueue_memory::composed_memory_backend(), NON_DURABLE).await,
    );
    record(
        &mut records,
        &mut failures,
        "AC-TXN-3",
        "memory",
        ac_txn_3_unknown_outcome_replay(|_: &str| pqueue_memory::composed_memory_backend(), NON_DURABLE).await,
    );

    // --- sqlite-log (composed SqliteLog + in-memory projection, atomic, durable) ---
    record(&mut records, &mut failures, "AC-TXN-1", "sqlite_log",
        ac_txn_1_success_durable_visible(sqlite_log_factory()).await);
    record(&mut records, &mut failures, "AC-TXN-2", "sqlite_log",
        ac_txn_2_rejection_no_effect(sqlite_log_factory(), DURABLE).await);
    record(&mut records, &mut failures, "AC-TXN-3", "sqlite_log",
        ac_txn_3_unknown_outcome_replay(sqlite_log_factory(), DURABLE).await);

    // --- objectlog (composed ObjectLog + in-memory projection, eventual-apply, durable) ---
    record(&mut records, &mut failures, "AC-TXN-1", "objectlog",
        ac_txn_1_success_durable_visible(objectlog_factory()).await);
    record(&mut records, &mut failures, "AC-TXN-2", "objectlog",
        ac_txn_2_rejection_no_effect(objectlog_factory(), DURABLE).await);
    record(&mut records, &mut failures, "AC-TXN-3", "objectlog",
        ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await);

    // --- object_log_sqlite (hybrid, eventual-apply, durable) — a COMMITTED profile ---
    record(&mut records, &mut failures, "AC-TXN-1", "object_log_sqlite",
        ac_txn_1_success_durable_visible(objectlog_sqlite_factory()).await);
    record(&mut records, &mut failures, "AC-TXN-2", "object_log_sqlite",
        ac_txn_2_rejection_no_effect(objectlog_sqlite_factory(), DURABLE).await);
    record(&mut records, &mut failures, "AC-TXN-3", "object_log_sqlite",
        ac_txn_3_unknown_outcome_replay(objectlog_sqlite_factory(), DURABLE).await);

    // --- AC-TXN-4 object-log-internal crash-point matrix (5 reachable cut points; see module doc). ---
    record(
        &mut records,
        &mut failures,
        "AC-TXN-4",
        "objectlog",
        ac_txn_4_crash_point_matrix().await,
    );

    // --- AC-TXN-6 cross-combination parity (sqlite-log[atomic] vs object_log_sqlite[eventual]) ---
    record(&mut records, &mut failures, "AC-TXN-6", "sqlite_log|object_log_sqlite",
        ac_txn_6_parity(sqlite_log_factory(), objectlog_sqlite_factory()).await);

    // --- AC-TXN-7 latency-bound is not a correctness knob (objectlog force-seal vs group-commit) ---
    // Repeat AC-TXN-3 under both commit-latency-bound settings; the invariants must be identical.
    let force_seal = ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await;
    let group_commit = ac_txn_3_unknown_outcome_replay(objectlog_group_commit_factory(), DURABLE).await;
    match (force_seal, group_commit) {
        (Ok(a), Ok(b)) => {
            let same = a == b;
            let mut assertions = vec![format!("force-seal AC-TXN-3 assertions == group-commit AC-TXN-3 assertions: {same}")];
            assertions.extend(a);
            if !same {
                failures.push("AC-TXN-7 [objectlog]: latency-bound setting changed AC-TXN-3 invariants".into());
            }
            records.push(AcEvidence { ac: "AC-TXN-7", backend: "objectlog(force-seal|group-commit)".into(), result: if same {"partial"} else {"fail"}, detail: "AC-TXN-3 invariance across commit-latency-bound settings".into(), assertions });
        }
        (fs, gc) => {
            if let Err(e) = fs { record(&mut records, &mut failures, "AC-TXN-7", "objectlog(force-seal)", Err(e)); }
            if let Err(e) = gc { record(&mut records, &mut failures, "AC-TXN-7", "objectlog(group-commit)", Err(e)); }
        }
    }

    let path = write_evidence("tp003-ac-txn-matrix.jsonl", &records).expect("write evidence jsonl");
    eprintln!("AC-TXN evidence written to {}", path.display());
    for r in &records {
        eprintln!("  [{}] {} => {} ({} assertions)", r.result, r.ac, r.backend, r.assertions.len());
    }
    assert!(failures.is_empty(), "AC-TXN matrix failures:\n{}", failures.join("\n"));
}

/// Postgres rows run in a SEPARATE non-tokio test because the sync postgres client panics under a tokio
/// runtime (see `pqueue-postgres/tests/conformance.rs`). Env-gated on `PQUEUE_PG_TEST_URL`; LOUD-skips
/// when absent. Postgres composed-log is atomic + in-memory projection, so it carries the same
/// documented request_id-across-restart gap as sqlite-log.
#[test]
fn ac_txn_contract_matrix_postgres() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "AC-TXN POSTGRES SKIPPED (external_transaction_contract_matrix_tests) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };

    // A tag-keyed durable reopen factory: the per-AC prefix + the per-phase `tag` name one postgres
    // schema, so reopening with the same tag recovers the same durable rows while different phases get
    // isolated schemas. Schema identifiers are sanitized (postgres identifiers dislike hyphens).
    let pg_factory = |prefix: String, url: String| {
        let run = COUNTER.fetch_add(1, Ordering::SeqCst);
        move |tag: &str| {
            let sch = format!(
                "pq_actxn_{prefix}_{}_{run}_{}",
                std::process::id(),
                tag.replace('-', "_")
            );
            pqueue_postgres::composed_postgres_backend_in_schema(&url, &sch).expect("connect postgres")
        }
    };

    let mut records: Vec<AcEvidence> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    record(&mut records, &mut failures, "AC-TXN-1", "postgres",
        futures::executor::block_on(ac_txn_1_success_durable_visible(pg_factory("txn1".into(), url.clone()))));
    record(&mut records, &mut failures, "AC-TXN-2", "postgres",
        futures::executor::block_on(ac_txn_2_rejection_no_effect(pg_factory("txn2".into(), url.clone()), DURABLE)));
    record(&mut records, &mut failures, "AC-TXN-3", "postgres",
        futures::executor::block_on(ac_txn_3_unknown_outcome_replay(pg_factory("txn3".into(), url.clone()), DURABLE)));

    let path = write_evidence("tp003-ac-txn-matrix-postgres.jsonl", &records).expect("write pg evidence");
    eprintln!("AC-TXN postgres evidence written to {}", path.display());
    for r in &records {
        eprintln!("  [{}] {} => {} ({} assertions)", r.result, r.ac, r.backend, r.assertions.len());
    }
    assert!(failures.is_empty(), "AC-TXN postgres failures:\n{}", failures.join("\n"));
}
