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
//! | AC-TXN-4 objectlog crash-point matrix | — | — | partial (+gap*) | partial (+gap*) | — |
//! | AC-TXN-6 cross-combination parity | — | ✓ (sqlite-log vs objectlog+sqlite) | | | — |
//! | AC-TXN-7 latency-bound invariance | — | — | partial (force-seal vs group-commit) | | — |
//!
//! AC-TXN-3's request_id-replay-across-restart is a REAL assertion on EVERY durable profile (atomic AND
//! eventual-apply): `ComposedBackend` recovery rebuilds the push-idempotency map from the durable log for
//! both durability classes (this suite's B3.1 run closed the earlier atomic-composed-log gap in
//! `crates/pqueue-engine/src/compose.rs`). `*` the deeper objectlog cut points (segment-write,
//! manifest-CAS, snapshot write, owner reassignment) are not reachable through the public commit seam and
//! stay documented gaps that need an objectlog-internal fault seam.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::fault::{
    AcEvidence, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
    ac_txn_3_unknown_outcome_replay, ac_txn_6_parity, write_evidence,
};
use pqueue_engine::{ComposedBackend, InProcessControlPlane};
use pqueue_objectlog::{ObjectLog, SegmentConfig};
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

    // --- AC-TXN-4 object-log crash-point matrix (partial: only the public-seam cut point) ---
    // The reachable objectlog cut point (append-durable-before-apply == "after segment write before
    // projection apply") is proven exactly-once by AC-TXN-3 above on both objectlog profiles. The deeper
    // internal cut points are documented gaps.
    match ac_txn_3_unknown_outcome_replay(objectlog_factory(), DURABLE).await {
        Ok(mut a) => {
            a.push("DOCUMENTED GAP: before-segment-write / after-segment-before-manifest / manifest-CAS / snapshot-write / owner-reassignment cut points need an objectlog-internal fault seam not exposed by the public commit API".into());
            records.push(AcEvidence { ac: "AC-TXN-4", backend: "objectlog".into(), result: "partial", detail: "public-seam cut point only".into(), assertions: a });
        }
        Err(e) => record(&mut records, &mut failures, "AC-TXN-4", "objectlog", Err(e)),
    }

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
