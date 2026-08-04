//! `fault_injection_harness_tests` (TP-001 line 148) — the reusable fault-injection CAPABILITY and its
//! own tests. This suite proves the harness itself: that [`fireweed_conformance::fault::inject_commit`]
//! genuinely drives a backend's commit pipeline through the typed [`Backend::commit_raw`] seam and
//! injects a crash at the AC-TXN-3 cut points, producing the durable footprint each cut point claims.
//!
//! The harness expresses each cut point as owned request data and simulates a process kill by dropping the handle and
//! reopening the SAME durable state. These tests exercise the capability against a non-durable profile
//! (memory — in-process invariants only) and two durable profiles (composed sqlite-log, composed
//! object-log) so the cut-point mechanics are validated on the recovery path they exist to model.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fireweed_conformance::fault::{CutPoint, durable_command_count, inject_commit, spec};
use fireweed_conformance::{envelope, item, qdef, qkey, shard, ts};
use fireweed_engine::{
    AsyncLogReplayBackend, Backend, CommandPosition, ControlPlaneStore, DurabilityClass,
    EngineError, EngineResult, ProjectionRead, PushCommand, QueueCommand, RawCommitFault,
    RawCommitOutcome, RawCommitRequest,
};
use fireweed_objectlog::{
    LegacyObjectLogSqliteBackend, LegacyObjectLogSqliteConfig, flush_config_from_segment,
};
use tokio::sync::{Notify, oneshot};

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct ControlledCommitState {
    started: Notify,
    release: Notify,
    resolved: AtomicBool,
    replay_outcome: Mutex<Option<RawCommitOutcome>>,
}

struct ControlledOwnedCommitBackend {
    state: Arc<ControlledCommitState>,
}

impl Backend for ControlledOwnedCommitBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn commit_raw(
        &self,
        request: RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<RawCommitOutcome>> + Send {
        let state = Arc::clone(&self.state);
        async move {
            let (result_tx, result_rx) = oneshot::channel();
            tokio::spawn(async move {
                // Transfer is complete before this signal. The worker owns the request and remains alive if
                // the caller awaiting `result_rx` is cancelled.
                state.started.notify_waiters();
                state.release.notified().await;

                let result = if request.fault() == RawCommitFault::BeforeAppend {
                    Err(EngineError::Invalid("fault-injection: kill before append"))
                } else {
                    let positions = request
                        .commands()
                        .iter()
                        .enumerate()
                        .map(|(index, _)| {
                            CommandPosition::new(
                                request.shard().clone(),
                                request.expected_epoch(),
                                index as u64 + 1,
                            )
                        })
                        .collect();
                    let outcome = if request.fault() == RawCommitFault::AfterAppendBeforeApply {
                        RawCommitOutcome::appended(positions)
                    } else {
                        RawCommitOutcome::applied(positions)
                    };
                    *state
                        .replay_outcome
                        .lock()
                        .expect("replay outcome poisoned") = Some(outcome.clone());
                    Ok(outcome)
                };
                state.resolved.store(true, Ordering::Release);
                let _ = result_tx.send(result);
            });

            result_rx.await.map_err(|_| {
                EngineError::Storage("owned commit worker dropped its result".to_string())
            })?
        }
    }
}

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "fireweed-fault-harness-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

// --- durable factories (stable location => drop+reopen recovers the same state) ---

fn sqlite_log_factory()
-> impl Fn() -> AsyncLogReplayBackend<fireweed_sqlite::SqliteLog, fireweed_sqlite::InMemoryProjection>
{
    let path = unique_dir("sqlite").with_extension("db");
    let path = path.to_str().unwrap().to_string();
    move || fireweed_sqlite::composed_sqlite_backend(&path).expect("open composed sqlite-log")
}

fn objectlog_factory() -> impl Fn() -> fireweed_objectlog::ComposedObjectLogBackend {
    let root = unique_dir("objectlog");
    move || {
        fireweed_objectlog::composed_objectlog_backend(root.clone())
            .expect("open composed objectlog")
    }
}

type LegacySqliteBackend = LegacyObjectLogSqliteBackend;

fn open_legacy_sqlite_sync(root: &std::path::Path) -> LegacySqliteBackend {
    let sqlite = root.join("projection.sqlite");
    let path = sqlite.to_str().unwrap();
    let config = LegacyObjectLogSqliteConfig {
        deferred_flush_chunk: 1,
        strict: false,
        async_monitor: None,
    };
    let flush = flush_config_from_segment(1, 1);
    let open = LegacyObjectLogSqliteBackend::open(root, path, flush, 0, config);
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(open)),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(open)
        }
    }
    .expect("recover objectlog/legacy-sqlite compatibility product")
}

fn objectlog_sqlite_factory() -> impl Fn() -> LegacySqliteBackend {
    let root = unique_dir("objectlog-sqlite");
    move || open_legacy_sqlite_sync(&root)
}

// ---------------------------------------------------------------------------
// Capability tests: BeforeAppend produces zero durable effect on every backend.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dropping_unpolled_owned_commit_leaves_no_effect() {
    let backend = fireweed_memory::composed_memory_backend();
    backend.create_queue(qdef()).await.unwrap();
    let epoch = backend.current_epoch(&shard()).await.unwrap();
    let request = RawCommitRequest::new(
        shard(),
        vec![envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("700001", "unpolled", 1)],
            }),
            vec![],
        )],
        epoch,
    );

    // Constructing the future transfers owned request data to the backend seam. Cancellation before its
    // first poll is still the queued phase: no append or apply may have started.
    let commit = backend.commit_raw(request);
    drop(commit);

    assert_eq!(durable_command_count(&backend).await.unwrap(), 0);
    assert_eq!(backend.metrics(&qkey()).await.unwrap().pending, 0);
}

#[tokio::test]
async fn dropping_started_caller_does_not_cancel_owned_commit() {
    let state = Arc::new(ControlledCommitState::default());
    let backend = Arc::new(ControlledOwnedCommitBackend {
        state: Arc::clone(&state),
    });
    let started = state.started.notified();
    let caller_backend = Arc::clone(&backend);
    let caller = tokio::spawn(async move {
        caller_backend
            .commit_raw(RawCommitRequest::new(
                shard(),
                vec![envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item("700003", "started", 1)],
                    }),
                    vec![],
                )],
                7,
            ))
            .await
    });

    // The backend-owned worker has the request and is deliberately suspended during commit. Cancelling the
    // caller now drops only its response awaiter.
    started.await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    state.release.notify_one();

    tokio::time::timeout(Duration::from_secs(1), async {
        while !state.resolved.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned commit must resolve after caller cancellation");

    let replay = state
        .replay_outcome
        .lock()
        .expect("replay outcome poisoned")
        .clone()
        .expect("resolved outcome remains replayable after lost response");
    assert!(replay.projection_applied());
    assert_eq!(replay.positions().len(), 1);
    assert_eq!(replay.positions()[0].backend_epoch, 7);
}

async fn assert_before_append_is_inert<B>(make: &impl Fn() -> B)
where
    B: fireweed_conformance::ConformanceCore + fireweed_engine::LogRead,
{
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    let env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "kx", 5)],
        }),
        vec![],
    );
    let killed = inject_commit(&a, env, CutPoint::BeforeAppend).await;
    assert!(killed.is_err(), "BeforeAppend must not commit the write");
    assert_eq!(
        durable_command_count(&a).await.unwrap(),
        0,
        "BeforeAppend must leave zero durable commands"
    );
    assert_eq!(
        a.metrics(&qkey()).await.unwrap().pending,
        0,
        "BeforeAppend must leave zero visible state"
    );
}

#[tokio::test]
async fn before_append_is_inert_memory() {
    assert_before_append_is_inert(&fireweed_memory::composed_memory_backend).await;
}

#[tokio::test]
async fn before_append_is_inert_sqlite_log() {
    assert_before_append_is_inert(&sqlite_log_factory()).await;
}

#[tokio::test]
async fn before_append_is_inert_objectlog() {
    assert_before_append_is_inert(&objectlog_factory()).await;
}

// ---------------------------------------------------------------------------
// Capability tests: AfterAppendBeforeApply durably appends, leaves the in-process
// projection unapplied, and REPLAYS EXACTLY ONCE when the durable backend reopens.
// ---------------------------------------------------------------------------

async fn assert_after_append_replays_once<B>(make: &impl Fn() -> B)
where
    B: fireweed_conformance::ConformanceCore + fireweed_engine::LogRead,
{
    // Session 1: append durably but skip apply (kill in the commit->apply window).
    {
        let a = make();
        a.create_queue(qdef()).await.unwrap();
        let env = envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "ky", 8)],
            }),
            vec![],
        );
        let pos = inject_commit(&a, env, CutPoint::AfterAppendBeforeApply)
            .await
            .expect("append half of the commit succeeds");
        assert!(!pos.is_empty(), "append returned a durable position");
        assert_eq!(
            durable_command_count(&a).await.unwrap(),
            1,
            "the command is durable on the log after the commit->apply kill"
        );
        // The in-process projection was deliberately NOT applied (models the lost apply/response).
        assert_eq!(
            a.metrics(&qkey()).await.unwrap().pending,
            0,
            "apply was skipped, so the in-process projection has not yet applied the command"
        );
    }
    // Session 2: reopen the same durable state; recovery replays the tail exactly once.
    let b = make();
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "recovery replayed the durable-but-unapplied command exactly once"
    );
    assert_eq!(
        b.select_eligible(&shard(), ts(100), 10)
            .await
            .unwrap()
            .len(),
        1,
        "no duplicate state transition after recovery replay"
    );
}

#[tokio::test]
async fn after_append_before_apply_replays_once_sqlite_log() {
    assert_after_append_replays_once(&sqlite_log_factory()).await;
}

#[tokio::test]
async fn after_append_before_apply_replays_once_objectlog() {
    assert_after_append_replays_once(&objectlog_factory()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_append_before_apply_replays_once_objectlog_sqlite() {
    assert_after_append_replays_once(&objectlog_sqlite_factory()).await;
}

// ---------------------------------------------------------------------------
// Capability test: the full commit (append+apply+Ok) is visible in-process.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_commit_is_visible_in_process_memory() {
    let a = fireweed_memory::composed_memory_backend();
    a.create_queue(qdef()).await.unwrap();
    let env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "kz", 3)],
        }),
        vec![],
    );
    let pos = inject_commit(&a, env, CutPoint::AfterResponse)
        .await
        .expect("full commit succeeds");
    assert!(!pos.is_empty());
    assert_eq!(
        a.metrics(&qkey()).await.unwrap().pending,
        1,
        "a full commit is visible in-process before the response returns"
    );
    assert_eq!(durable_command_count(&a).await.unwrap(), 1);
}

#[tokio::test]
async fn typed_commit_reports_resolved_apply_boundary() {
    let backend = fireweed_memory::composed_memory_backend();
    backend.create_queue(qdef()).await.unwrap();
    let epoch = backend.current_epoch(&shard()).await.unwrap();
    let outcome = backend
        .commit_raw(RawCommitRequest::new(
            shard(),
            vec![envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item("700002", "resolved", 1)],
                }),
                vec![],
            )],
            epoch,
        ))
        .await
        .unwrap();

    assert!(outcome.projection_applied());
    assert_eq!(outcome.positions().len(), 1);
    assert_eq!(backend.metrics(&qkey()).await.unwrap().pending, 1);
}

// ---------------------------------------------------------------------------
// The harness drives the real request_id idempotency path too (AfterApplyBeforeResponse
// / AfterResponse): a committed-then-lost response replays exactly once on reopen.
// ---------------------------------------------------------------------------

/// The lost-response `request_id` replay: a committed-then-lost response must replay the ONE committed
/// result on reopen. `ComposedBackend` recovery rebuilds the push-idempotency map from the durable log
/// for BOTH durability classes — the ATOMIC composed-log profile (`sqlite_log`, exercised here) as well
/// as the EVENTUAL-APPLY profile (`objectlog_sqlite`). Before B3.1, atomic composed-log recovery dropped
/// the idempotency map (the rebuild was gated on `DurabilityClass::EventualApply` in
/// `crates/fireweed-engine/src/compose.rs`), so the `sqlite_log` arm below re-executed and returned a fresh
/// id — an INV-14 violation this bead's engine fix closed.
async fn assert_lost_response_replays_once<B>(make: &impl Fn() -> B)
where
    B: fireweed_conformance::ConformanceCore,
{
    let rid = fireweed_core::RequestId::new("harness-lost").unwrap();
    let body = vec![spec("harness-lost", 4)];
    let committed = {
        let a = make();
        a.create_queue(qdef()).await.unwrap();
        a.push_with_request_id(&shard(), rid.clone(), body.clone(), ts(1), None)
            .await
            .unwrap()
    };
    let b = make();
    let replay = b
        .push_with_request_id(&shard(), rid, body, ts(2), None)
        .await
        .unwrap();
    assert!(committed.is_fresh(), "the original request commits freshly");
    assert!(replay.is_replayed(), "the retry is identified as a replay");
    assert_eq!(
        replay.item_ids, committed.item_ids,
        "lost-response retry replays the committed ids"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "no duplicate committed result after a lost-response retry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_response_replays_once_objectlog_sqlite() {
    assert_lost_response_replays_once(&objectlog_sqlite_factory()).await;
}

/// Regression guard for the B3.1 engine fix: atomic composed-log recovery must rebuild push-idempotency.
#[tokio::test]
async fn lost_response_replays_once_sqlite_log() {
    assert_lost_response_replays_once(&sqlite_log_factory()).await;
}

// Object-log INTERNAL cut points (segment write / manifest CAS / owner reassignment)
// lived on the retired in-tree segmented ObjectLog substrate and were deleted with it.
// LogEngine native fault injection is tracked separately under program A.
