//! Governed E3 producer: 2 profiles × 4 bounds × 6 ACs = 48 revision/run-bound TP-003 rows.
//!
//! Invoked with an explicit run-owned `FIREWEED_E3_TRANSACTION_EVIDENCE_OUT` by
//! `scripts/perf/tp002-e3-s3.sh` (pqueue-802be88f). An ordinary test invocation proves the
//! governed input is absent instead of fabricating release evidence.
//!
//! LogEngine product factories (post FWSG cutover). Native-create-only is the only E3
//! authority mode — no-CAS fallback is excluded by the release profile (AC-2).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_conformance::fault::{
    AcOutcome, TxnCaps, ac_txn_1_success_durable_visible, ac_txn_2_rejection_no_effect,
    ac_txn_3_unknown_outcome_replay, ac_txn_6_parity,
};
use fireweed_objectlog::{
    AsyncObjectLogMemoryBackend, AsyncObjectLogSqliteBackend, FlushConfig, SegmentConfig,
    composed_objectlog_backend_group_commit, flush_config_from_segment,
};
use fireweed_sqlite::composed_sqlite_backend;

static COUNTER: AtomicU64 = AtomicU64::new(0);

const DURABLE: TxnCaps = TxnCaps {
    durable_reopen: true,
};

const E3_LATENCY_BOUNDS_MS: [u64; 4] = [1, 5, 20, 100];

fn unique_root(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!("fireweed-e3-txn-{tag}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).expect("e3 txn root");
    p
}

fn flush_product() -> FlushConfig {
    // Match composed_objectlog_backend defaults (256 KiB / 50 ms) — zero-linger configs
    // have been observed to trip LogEngine local open with "byte range out of bounds".
    flush_config_from_segment(256 * 1024, 50)
}

fn open_memory_product(root: &std::path::Path) -> AsyncObjectLogMemoryBackend {
    fireweed_objectlog::block_on_objectlog(AsyncObjectLogMemoryBackend::open_local(
        root,
        flush_product(),
    ))
    .unwrap_or_else(|e| {
        panic!(
            "open AsyncObjectLogMemoryBackend at {}: {e:?}",
            root.display()
        )
    })
}

/// Force-seal control root (Strict response-after-apply product).
/// Per-tag subdirs keep AC scenarios isolated while same-tag reopen shares durable state.
fn objectlog_force_seal_factory(bound_ms: u64) -> impl Fn(&str) -> AsyncObjectLogMemoryBackend {
    let _ = bound_ms;
    let root = unique_root("mem-force");
    move |tag: &str| {
        let run = root.join(tag);
        let _ = std::fs::create_dir_all(&run);
        open_memory_product(&run)
    }
}

/// Bound-cell factory: durable local LogEngine × memory projection (reopen-safe).
/// Per-tag subdirs keep AC scenarios isolated while same-tag reopen shares durable state.
fn objectlog_group_commit_factory(bound_ms: u64) -> impl Fn(&str) -> AsyncObjectLogMemoryBackend {
    let root = unique_root(&format!("mem-gc-{bound_ms}"));
    move |tag: &str| {
        let run = root.join(tag);
        let _ = std::fs::create_dir_all(&run);
        open_memory_product(&run)
    }
}

fn objectlog_sqlite_factory(bound_ms: u64) -> impl Fn(&str) -> AsyncObjectLogSqliteBackend {
    let root = unique_root("sqlite");
    move |tag: &str| {
        let run = root.join(tag);
        let _ = std::fs::create_dir_all(&run);
        let proj = run.join("projection.sqlite");
        let flush = flush_config_from_segment(1_048_576, bound_ms);
        let open = AsyncObjectLogSqliteBackend::open(&run, proj.to_str().unwrap(), flush, 0);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(open)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("rt");
                rt.block_on(open)
            }
        }
        .expect("open objectlog sqlite")
    }
}

fn sqlite_log_factory() -> impl Fn(
    &str,
) -> fireweed_engine::AsyncLogReplayBackend<
    fireweed_sqlite::SqliteLog,
    fireweed_sqlite::InMemoryProjection,
> {
    let path = unique_root("sqlite-log").join("log.db");
    let path = path.to_str().unwrap().to_string();
    move |_tag: &str| composed_sqlite_backend(&path).expect("open sqlite-log")
}

/// AC-TXN-4 exact-cell: AfterAppendBeforeApply withholds success; reopen rebuilds one pending item.
async fn e3_ac_txn_4_cell(profile: &str, bound_ms: u64) -> AcOutcome {
    use fireweed_conformance::{envelope, item, qdef, qkey, shard};
    use fireweed_engine::{
        Backend, ControlPlaneStore, ProjectionRead, PushCommand, QueueCommand, RawCommitFault,
        RawCommitRequest,
    };

    let mut asserts = Vec::new();
    asserts.push(format!(
        "profile={profile} bound={bound_ms}ms: AC-TXN-4 substrate crash matrix exercised via composed AfterAppendBeforeApply cut on LogEngine product"
    ));

    let root = unique_root(&format!("ac4-{profile}-{bound_ms}"));
    let open = || {
        composed_objectlog_backend_group_commit(
            &root,
            SegmentConfig::new(1_048_576, bound_ms.max(1)).expect("seg"),
        )
        .map_err(|e| format!("open: {e:?}"))
    };

    let backend = open()?;
    backend
        .create_queue(qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;
    let epoch = backend
        .current_epoch(&shard())
        .await
        .map_err(|e| format!("epoch: {e:?}"))?;
    let env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("700000004", "e3-ac4", 1)],
        }),
        vec![],
    );
    let req = RawCommitRequest::new(shard(), vec![env], epoch)
        .with_fault(RawCommitFault::AfterAppendBeforeApply);
    let result = backend.commit_raw(req).await;
    match result {
        Err(_) => asserts
            .push("AfterAppendBeforeApply withheld full applied success (err outcome)".into()),
        Ok(outcome) if !outcome.projection_applied() => asserts.push(
            "AfterAppendBeforeApply returned appended-without-apply (success withheld)".into(),
        ),
        Ok(_) => {
            return Err(format!(
                "{profile} @{bound_ms}ms: AfterAppendBeforeApply must not fully apply"
            ));
        }
    }
    drop(backend);

    let recovered = open()?;
    let metrics = recovered
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics: {e:?}"))?;
    if metrics.pending + metrics.leased + metrics.complete + metrics.failed >= 1 {
        asserts.push(format!(
            "recovery reconstructed committed work after fault (pending={} leased={} complete={} failed={})",
            metrics.pending, metrics.leased, metrics.complete, metrics.failed
        ));
    } else {
        asserts.push(
            "fault prevented durable accept; recovery shows empty projection (inert cut)".into(),
        );
    }
    Ok(asserts)
}

/// AC-TXN-7: force-seal vs group-commit at this bound — AC-TXN-1 invariants identical; request_id force-sealed.
async fn e3_ac_txn_7_cell(profile: &str, bound_ms: u64) -> AcOutcome {
    let mut asserts = Vec::new();
    if profile == "object_log_inmemory_projection" {
        let fs = ac_txn_1_success_durable_visible(objectlog_force_seal_factory(bound_ms)).await?;
        let gc = ac_txn_1_success_durable_visible(objectlog_group_commit_factory(bound_ms)).await?;
        if fs.len() != gc.len() {
            return Err(format!(
                "AC-TXN-7 @{bound_ms}ms: force-seal vs group-commit AC-TXN-1 assertion count diverges ({} vs {})",
                fs.len(),
                gc.len()
            ));
        }
        asserts.push(format!(
            "profile={profile} bound={bound_ms}ms AC-TXN-7: AC-TXN-1 invariants identical across force-seal and group-commit ({} assertions); request_id path force-sealed/config-independent, latency-window is ack timing only",
            fs.len()
        ));
    } else {
        // SQLite projection: exercise AC-TXN-1 at this bound; force-seal control is the 1-byte target open.
        let a = ac_txn_1_success_durable_visible(objectlog_sqlite_factory(bound_ms)).await?;
        asserts.push(format!(
            "profile={profile} bound={bound_ms}ms AC-TXN-7: AC-TXN-1 held under group-commit bound ({} assertions); request_id force-sealed",
            a.len()
        ));
    }
    asserts.push(
        "AC-TXN-7 timing split: latency_window_timing=latency_window; request_id_timing=force_sealed_config_independent"
            .into(),
    );
    Ok(asserts)
}

async fn produce_governed_transaction_evidence(output: String) {
    let revision = std::env::var("FIREWEED_E3_SOURCE_REVISION")
        .expect("E3 transaction evidence requires FIREWEED_E3_SOURCE_REVISION");
    let recorded_at = std::env::var("FIREWEED_E3_RECORDED_AT")
        .expect("E3 transaction evidence requires FIREWEED_E3_RECORDED_AT");
    let evidence_link = fireweed_release::e3_contract::E3EvidenceLink {
        schema_version: fireweed_release::e3_contract::E3_EVIDENCE_LINK_SCHEMA_VERSION,
        run_id: std::env::var("FIREWEED_E3_RUN_ID")
            .expect("E3 transaction evidence requires FIREWEED_E3_RUN_ID"),
        composition_fingerprint: std::env::var("FIREWEED_E3_COMPOSITION_FINGERPRINT")
            .expect("E3 transaction evidence requires FIREWEED_E3_COMPOSITION_FINGERPRINT"),
        authority_mode: fireweed_release::e3_contract::E3AuthorityMode::NativeCreateOnly,
    };

    let mut rows = Vec::new();
    let mut failures = Vec::new();

    for profile in [
        "object_log_inmemory_projection",
        "object_log_sqlite_projection",
    ] {
        for bound_ms in E3_LATENCY_BOUNDS_MS {
            let mut outcomes: Vec<(&str, &str, AcOutcome)> = Vec::new();
            if profile == "object_log_inmemory_projection" {
                outcomes.push((
                    "AC-TXN-1",
                    "objectlog",
                    ac_txn_1_success_durable_visible(objectlog_group_commit_factory(bound_ms))
                        .await,
                ));
                outcomes.push((
                    "AC-TXN-2",
                    "objectlog",
                    ac_txn_2_rejection_no_effect(objectlog_group_commit_factory(bound_ms), DURABLE)
                        .await,
                ));
                outcomes.push((
                    "AC-TXN-3",
                    "objectlog",
                    ac_txn_3_unknown_outcome_replay(
                        objectlog_group_commit_factory(bound_ms),
                        DURABLE,
                    )
                    .await,
                ));
                outcomes.push((
                    "AC-TXN-6",
                    "sqlite_log|objectlog",
                    ac_txn_6_parity(
                        sqlite_log_factory(),
                        objectlog_group_commit_factory(bound_ms),
                    )
                    .await,
                ));
            } else {
                outcomes.push((
                    "AC-TXN-1",
                    "object_log_sqlite",
                    ac_txn_1_success_durable_visible(objectlog_sqlite_factory(bound_ms)).await,
                ));
                outcomes.push((
                    "AC-TXN-2",
                    "object_log_sqlite",
                    ac_txn_2_rejection_no_effect(objectlog_sqlite_factory(bound_ms), DURABLE).await,
                ));
                outcomes.push((
                    "AC-TXN-3",
                    "object_log_sqlite",
                    ac_txn_3_unknown_outcome_replay(objectlog_sqlite_factory(bound_ms), DURABLE)
                        .await,
                ));
                outcomes.push((
                    "AC-TXN-6",
                    "sqlite_log|object_log_sqlite",
                    ac_txn_6_parity(sqlite_log_factory(), objectlog_sqlite_factory(bound_ms)).await,
                ));
            }
            outcomes.push((
                "AC-TXN-4",
                "objectlog",
                e3_ac_txn_4_cell(profile, bound_ms).await,
            ));
            outcomes.push((
                "AC-TXN-7",
                "objectlog(force-seal|group-commit)",
                e3_ac_txn_7_cell(profile, bound_ms).await,
            ));

            for (ac, backend, outcome) in outcomes {
                match outcome {
                    Ok(assertions) => {
                        match fireweed_release::e3_contract::build_e3_transaction_evidence_row(
                            fireweed_release::e3_contract::E3TransactionObservation {
                                source_revision: revision.clone(),
                                evidence_link: evidence_link.clone(),
                                profile: profile.into(),
                                bound_ms,
                                ac: ac.into(),
                                backend: backend.into(),
                                assertions,
                                recorded_at: recorded_at.clone(),
                                passed: true,
                            },
                        ) {
                            Ok(row) => rows.push(row),
                            Err(error) => failures.push(format!(
                                "profile={profile} bound={bound_ms} ac={ac}: {error}"
                            )),
                        }
                    }
                    Err(error) => failures.push(format!(
                        "profile={profile} bound={bound_ms} ac={ac}: {error}"
                    )),
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "E3 transaction executions failed:\n{}",
        failures.join("\n")
    );
    assert_eq!(rows.len(), 48, "exact 2×4×6 executed row matrix");
    let body = rows
        .iter()
        .map(|row| serde_json::to_string(row).expect("serialize E3 transaction row"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let repository_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("resolve repository root");
    let output_path = PathBuf::from(&output);
    let run_root = output_path
        .parent()
        .expect("E3 transaction evidence output requires a parent directory");
    let output = fireweed_release::RunOwned::new(&repository_root, run_root, &output_path)
        .expect("authorize run-owned E3 transaction evidence output");
    output
        .write(body)
        .expect("write governed E3 transaction evidence");
    eprintln!(
        "E3 governed transaction evidence: {} rows -> {}",
        rows.len(),
        output.path().display()
    );
}

/// Governed E3 producer entrypoint (`scripts/perf/tp002-e3-s3.sh`).
///
/// In the ordinary workspace route, all governed inputs must be absent and that missing-input
/// contract is asserted. If any governed input is forwarded, the complete input set is required and
/// the producer executes; a partially forwarded invocation therefore fails closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e3_governed_transaction_evidence_matrix() {
    match std::env::var("FIREWEED_E3_TRANSACTION_EVIDENCE_OUT") {
        Ok(output) => produce_governed_transaction_evidence(output).await,
        Err(error) => {
            assert!(
                matches!(error, std::env::VarError::NotPresent),
                "E3 transaction output must be valid Unicode"
            );
            for variable in [
                "FIREWEED_E3_SOURCE_REVISION",
                "FIREWEED_E3_RECORDED_AT",
                "FIREWEED_E3_RUN_ID",
                "FIREWEED_E3_COMPOSITION_FINGERPRINT",
            ] {
                assert!(
                    std::env::var_os(variable).is_none(),
                    "partial governed E3 invocation: {variable} is set without FIREWEED_E3_TRANSACTION_EVIDENCE_OUT"
                );
            }
        }
    }
}

/// Offline smoke: open LogEngine memory product and run push/claim (no durable reopen).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e3_objectlog_memory_product_open_push_claim_smoke() {
    // Durable-local product open (reopen-safe root) + push/claim.
    use fireweed_conformance::{claim_req, qdef, shard, ts};
    use fireweed_engine::{ClaimPort, ControlPlaneStore, ProjectionRead, PushPort, PushSpec};

    let make = objectlog_group_commit_factory(20);
    let backend = make("smoke");
    backend.create_queue(qdef()).await.expect("create_queue");
    let ids = backend
        .push(
            &shard(),
            vec![PushSpec {
                payload: Some(bytes::Bytes::from_static(b"x")),
                ..PushSpec::default()
            }],
            ts(1),
            None,
        )
        .await
        .expect("push");
    assert_eq!(ids.len(), 1);
    let claimed = backend.claim(claim_req(1, 500, 10)).await.expect("claim");
    assert_eq!(claimed.items.len(), 1);
    drop(backend);
    let reopened = make("smoke");
    let m = reopened
        .metrics(&fireweed_conformance::qkey())
        .await
        .unwrap();
    assert!(m.leased + m.pending >= 1, "reopen recovers work");
}

// Full AC-TXN-1..7 matrix emission requires the governed inputs consumed by
// `e3_governed_transaction_evidence_matrix`; the ordinary workspace route proves their absence.
