//! Fault-injection harness (TP-001 `fault_injection_harness_tests`) and the reusable AC-TXN scenario
//! functions the `external_transaction_contract_matrix_tests` suite runs across backend profiles
//! (TP-003 §3.10, AC-TXN-1..7).
//!
//! # What this exercises for real
//!
//! The only commit-pipeline seam the engine exposes to a driver is [`Backend::write`]: a synchronous
//! unit-of-work closure handed a [`pqueue_engine::LogWriter`] and a
//! [`pqueue_engine::ProjectionWriter`]. [`inject_commit`] drives that seam and injects
//! a crash at the AC-TXN-3 cut points that ARE reachable through it:
//!
//! * `BeforeAppend` — return before `append`: zero durable effect on every backend.
//! * `AfterAppendBeforeApply` — `append` durably, then return **without** `apply`: models a kill in the
//!   commit→apply window. The command is on the durable log for recovery to replay; the in-process
//!   projection is deliberately left unapplied, so the caller drops+reopens a durable backend to recover.
//!
//! A "process kill/restart" is simulated by dropping the backend handle and rebuilding it from the SAME
//! durable state (the `make` factory reopens the same file/dir/schema) — the identical mechanism the
//! `durable_reconnect_suite!` uses. In-memory profiles cannot reopen durable state, so the restart-bound
//! rows are only run against durable profiles (documented in the matrix suite).
//!
//! # Honest cut-point coverage
//!
//! The mid-pipeline cut points (`BeforeAppend`, `AfterAppendBeforeApply`) are driven with a raw
//! [`CommandEnvelope`] carrying `request_id: None`, so they prove **item-level** exactly-once replay
//! (INV-14) but not `request_id` idempotency dedup at that exact instant — constructing a
//! request-id-bearing `Push` envelope at the raw seam would require engine-internal helpers
//! (`build_push_items`, `push_body_hash`, counter reservation) that are not exported. The response-window
//! cut points (`AfterApplyBeforeResponse`, `AfterResponse`) are driven through the real
//! [`pqueue_engine::PushPort::push_with_request_id`] idempotency path, so they prove `request_id` replay exactly-once.
//! This split is recorded per row in the evidence JSONL rather than papered over.

use std::path::PathBuf;

use bytes::Bytes;
use pqueue_core::{ClientItemKey, PriorityValue, RequestId};
use pqueue_engine::{
    Backend, CommandEnvelope, CommandPosition, ControlPlaneStore, EngineError, FinalizeKind,
    FinalizeOutcome, LogRead, PushCommand, PushSpec, QueueCommand,
};

use crate::{ConformanceCore, claim_req, commit, envelope, item, qdef, qkey, shard, ts};

/// The AC-TXN-3 commit-pipeline cut points (TP-003 §3.10 row AC-TXN-3): the ordered instants at which a
/// process kill or a dropped response can strike a single mutating commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutPoint {
    /// Kill before anything is durably appended — no original commit exists.
    BeforeAppend,
    /// Kill after the durable append but before the projection apply / commit barrier. The command is
    /// durable on the log; recovery must replay it exactly once. The in-process projection is left
    /// unapplied on purpose (the caller reopens a durable backend to recover).
    AfterAppendBeforeApply,
    /// Kill after apply commits but before the success response reaches the client. The command is
    /// durable AND visible; a retry by `request_id` must replay the one committed result.
    AfterApplyBeforeResponse,
    /// Response delivered; a later duplicate retry must still replay exactly once.
    AfterResponse,
}

/// Outcome of one AC-TXN scenario: `Ok(assertions exercised)` or `Err(reason it failed)`. Scenarios
/// return a value instead of panicking so the matrix orchestrator can record faithful evidence for every
/// row (pass or fail) before asserting the suite is green.
pub type AcOutcome = Result<Vec<String>, String>;

/// The transaction-recovery capabilities of a backend profile, so each AC-TXN scenario runs exactly the
/// rows the profile genuinely supports instead of faking coverage.
#[derive(Debug, Clone, Copy)]
pub struct TxnCaps {
    /// The `make` factory reopens the SAME durable state (an in-memory dev profile cannot, so its
    /// restart-bound rows are skipped and recorded as N/A rather than asserted). Every durable
    /// composed-log backend rebuilds the `request_id` push-idempotency map from the durable log on
    /// recovery (`ComposedBackend::rebuild_push_idempotency_from_log`), so a `request_id` replay across a
    /// restart resolves to exactly one committed result on both atomic and eventual-apply profiles.
    pub durable_reopen: bool,
}

macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            return Err(format!($($arg)*));
        }
    };
}

/// Build a single push spec keyed by `key` at Int64 `priority`, carrying an opaque payload so
/// after-restart visibility asserts the FULL record survived, not just the id.
pub fn spec(key: &str, priority: i64) -> PushSpec {
    PushSpec {
        client_item_key: Some(ClientItemKey::new(key).unwrap()),
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(Bytes::copy_from_slice(key.as_bytes())),
        ..Default::default()
    }
}

/// Drive one raw commit through the backend's [`Backend::write`] unit-of-work seam, injecting a crash at
/// `cut`. Returns the appended positions when the append reached the durable log, or the injected error.
///
/// This is the reusable fault-injection primitive: the whole capability is expressing each AC-TXN-3 cut
/// point as a decision INSIDE the one commit closure the engine exposes.
pub async fn inject_commit<B: Backend + ControlPlaneStore>(
    backend: &B,
    env: CommandEnvelope,
    cut: CutPoint,
) -> Result<Vec<CommandPosition>, EngineError> {
    let epoch = backend.current_epoch(&shard()).await?;
    backend
        .write(move |lw, pw| {
            if cut == CutPoint::BeforeAppend {
                return Err(EngineError::Invalid("fault-injection: kill before append"));
            }
            let pos = lw.append(&shard(), std::slice::from_ref(&env), epoch)?;
            if cut == CutPoint::AfterAppendBeforeApply {
                // Durable append committed; skip apply to model a kill in the commit→apply window. The
                // caller drops+reopens the durable backend so recovery replays this tail entry.
                return Ok(pos);
            }
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(pos)
        })
        .await
}

/// The number of durably-appended commands for the fixture shard (recovery/no-divergence probe).
pub async fn durable_command_count<B: LogRead>(backend: &B) -> Result<usize, String> {
    Ok(backend
        .read_from(&shard(), None, 100_000)
        .await
        .map_err(|e| format!("read_from: {e:?}"))?
        .entries
        .len())
}

// ---------------------------------------------------------------------------
// AC-TXN scenario functions (generic over the durable backend factory)
// ---------------------------------------------------------------------------

/// **AC-TXN-1** success durable + visible after kill/restart (INV-10, INV-12). Each mutating operation is
/// committed, then the durable backend is IMMEDIATELY killed + reopened and the effect of THAT operation
/// is asserted from recovered state before the next operation runs — so no later op can mask an earlier
/// op's lost effect (in particular a lease renewal is verified before the finalize that would erase it).
///
/// Coverage: `BatchPush` (via the acknowledged `request_id` path), `BatchClaim`, `BatchRenewLeases`, and
/// `BatchFinalize`. It does NOT exercise `CreateQueue`-alone, `BatchUpdate`, `SetGates`, or `PurgeItems`;
/// the recorded assertion names only the four operations actually covered.
pub async fn ac_txn_1_success_durable_visible<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
) -> AcOutcome {
    let mut asserts = Vec::new();
    let key_b = ClientItemKey::new("txn1-b").unwrap();
    let rid = RequestId::new("ac-txn-1-push").unwrap();

    // --- BatchPush (acknowledged via request_id) then kill/reopen and assert both items durable+visible.
    let acked = {
        let a = make("txn1");
        a.create_queue(qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        a.push_with_request_id(
            &shard(),
            rid.clone(),
            vec![spec("txn1-a", 5), spec("txn1-b", 9)],
            ts(1),
            None,
        )
        .await
        .map_err(|e| format!("push_with_request_id: {e:?}"))?
    };
    ensure!(acked.len() == 2, "expected 2 acked push ids, got {}", acked.len());
    {
        let b = make("txn1");
        let m = b.metrics(&qkey()).await.map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            (m.pending, m.leased, m.complete) == (2, 0, 0),
            "BatchPush not durable after kill/reopen; got pending={} leased={} complete={}",
            m.pending, m.leased, m.complete
        );
        let live = b
            .live_items(&shard(), std::slice::from_ref(&key_b))
            .await
            .map_err(|e| format!("live_items: {e:?}"))?
            .into_iter().next().flatten()
            .ok_or_else(|| "pushed item not visible by client_item_key after reopen".to_string())?;
        ensure!(live.item_id == acked[1], "pushed item id mismatch after reopen");
        ensure!(live.payload.as_deref() == Some(&b"txn1-b"[..]), "pushed payload lost across reopen");
    }
    asserts.push("BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)".into());

    // --- BatchClaim then kill/reopen and assert the item is durably Leased.
    let leased_id = {
        let b = make("txn1");
        let claimed = b.claim(claim_req(1, 500, 10)).await.map_err(|e| format!("claim: {e:?}"))?;
        ensure!(claimed.items.len() == 1, "claim leased one item");
        claimed.items[0].item_id
    };
    {
        let b = make("txn1");
        let m = b.metrics(&qkey()).await.map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            (m.pending, m.leased, m.complete) == (1, 1, 0),
            "BatchClaim lease not durable after kill/reopen; got pending={} leased={} complete={}",
            m.pending, m.leased, m.complete
        );
    }
    asserts.push("BatchClaim lease durable after kill/reopen".into());

    // --- BatchRenewLeases (extend the deadline 500 -> 900) then kill/reopen and prove the RENEWED
    // deadline survived: a reclaim tick at 600 (past the original 500, before the renewed 900) must NOT
    // reclaim the lease. This checkpoint runs BEFORE the finalize, so the renew effect cannot be masked.
    {
        let b = make("txn1");
        b.renew(&shard(), vec![leased_id], ts(900), ts(20), None)
            .await
            .map_err(|e| format!("renew: {e:?}"))?;
    }
    {
        let b = make("txn1");
        b.tick(ts(600)).await.map_err(|e| format!("tick: {e:?}"))?;
        let m = b.metrics(&qkey()).await.map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            (m.pending, m.leased) == (1, 1),
            "BatchRenewLeases deadline lost after kill/reopen: a tick at 600 reclaimed the lease that renew extended to 900; got pending={} leased={}",
            m.pending, m.leased
        );
    }
    asserts.push("BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)".into());

    // --- BatchFinalize(Complete) then kill/reopen and assert the terminal state survived.
    {
        let b = make("txn1");
        b.finalize(
            &shard(),
            vec![FinalizeOutcome::new(leased_id, FinalizeKind::Complete)],
            ts(650),
            None,
        )
        .await
        .map_err(|e| format!("finalize: {e:?}"))?;
    }
    {
        let b = make("txn1");
        let m = b.metrics(&qkey()).await.map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            (m.pending, m.leased, m.complete) == (1, 0, 1),
            "BatchFinalize terminal state not durable after kill/reopen; got pending={} leased={} complete={}",
            m.pending, m.leased, m.complete
        );
        // The surviving pending sibling is still claimable (0 read-after-success gaps).
        let claimed = b.claim(claim_req(1, 1500, 700)).await.map_err(|e| format!("claim: {e:?}"))?;
        ensure!(
            claimed.items.first().map(|i| i.item_id) == Some(acked[1]),
            "surviving pending item not claimable after finalize+reopen"
        );
    }
    asserts.push("BatchFinalize terminal state durable after kill/reopen; sibling still claimable".into());

    Ok(asserts)
}

/// **AC-TXN-2** rejection has no durable effect (INV-13). Structurally-rejected envelopes and per-item
/// invalid/conflict cases append nothing and leave 0 durable effect, while an accepted sibling committed
/// in the same session retains normal success semantics. On a durable profile the no-effect is
/// re-verified after a restart+replay from durable state; on a non-durable dev profile it is verified
/// in-process (the restart clause is recorded N/A).
pub async fn ac_txn_2_rejection_no_effect<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
    caps: TxnCaps,
) -> AcOutcome {
    let mut asserts = Vec::new();
    let accepted_key = ClientItemKey::new("txn2-accepted").unwrap();

    let a = make("txn2");
    a.create_queue(qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;
    // Accepted sibling: a normal push that MUST survive.
    let ids = a
        .push(&shard(), vec![spec("txn2-accepted", 5)], ts(0), None)
        .await
        .map_err(|e| format!("accepted push: {e:?}"))?;
    ensure!(ids.len() == 1, "accepted push landed one item");
    let accepted_id = ids[0];

    let before = durable_command_count(&a).await?;

    // (a) per-item invalid: finalize a pending (non-leased) item → Invalid, appends nothing.
    ensure!(
        matches!(
            a.finalize(
                &shard(),
                vec![FinalizeOutcome::new(accepted_id, FinalizeKind::Complete)],
                ts(10),
                None,
            )
            .await,
            Err(EngineError::Invalid(_))
        ),
        "finalize of a pending item must be a structured rejection"
    );
    // (b) unknown-id renew → NotFound, appends nothing.
    ensure!(
        a.renew(&shard(), vec![pqueue_core::ItemId::new("404").unwrap()], ts(500), ts(10), None)
            .await
            .is_err(),
        "renew of an unknown id must be rejected"
    );
    // Immediately prove the two structural rejections above appended 0 durable commands and produced 0
    // visible effect — independent of the request-id-conflict retry checked below.
    let after_rejects = durable_command_count(&a).await?;
    ensure!(
        after_rejects == before,
        "rejected finalize/renew appended durable commands ({before} -> {after_rejects})"
    );
    let m_rejects = a.metrics(&qkey()).await.map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        (m_rejects.pending, m_rejects.leased, m_rejects.complete, m_rejects.failed) == (1, 0, 0, 0),
        "rejected finalize/renew changed visible state; got pending={} leased={} complete={} failed={}",
        m_rejects.pending, m_rejects.leased, m_rejects.complete, m_rejects.failed
    );
    // (c) request-id conflict: reuse a request_id with a different body → RequestIdConflict.
    let rid = RequestId::new("ac-txn-2-rid").unwrap();
    a.push_with_request_id(&shard(), rid.clone(), vec![spec("txn2-rid", 1)], ts(11), None)
        .await
        .map_err(|e| format!("first request-id push: {e:?}"))?;
    let after_rid = durable_command_count(&a).await?;
    ensure!(
        matches!(
            a.push_with_request_id(&shard(), rid, vec![spec("txn2-rid-different", 2)], ts(12), None)
                .await,
            Err(EngineError::RequestIdConflict)
        ),
        "reused request_id with a different body must conflict"
    );
    let after = durable_command_count(&a).await?;
    ensure!(
        after == after_rid,
        "request-id conflict must not append (durable {before}->{after_rid}->{after})"
    );
    // In-process: only the two accepted commands are visible; the rejects had no effect.
    let m = a.metrics(&qkey()).await.map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete, m.failed) == (2, 0, 0, 0),
        "rejected mutations must leave 0 visible effect; got pending={} leased={} complete={} failed={}",
        m.pending, m.leased, m.complete, m.failed
    );
    asserts.push(
        "rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect".into(),
    );

    if !caps.durable_reopen {
        asserts.push("restart-replay clause N/A (non-durable in-memory dev profile)".into());
        return Ok(asserts);
    }

    // Restart and replay from durable state: only the accepted effects survive; rejects left no phantom.
    drop(a);
    let b = make("txn2");
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics after restart: {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete, m.failed) == (2, 0, 0, 0),
        "only accepted siblings survive restart; got pending={} leased={} complete={} failed={}",
        m.pending, m.leased, m.complete, m.failed
    );
    let live = b
        .live_items(&shard(), std::slice::from_ref(&accepted_key))
        .await
        .map_err(|e| format!("live_items: {e:?}"))?
        .into_iter()
        .next()
        .flatten();
    ensure!(
        live.is_some(),
        "accepted sibling must retain normal success semantics after restart"
    );
    asserts.push("accepted siblings survive restart with 0 phantom commits from rejects".into());

    Ok(asserts)
}

/// **AC-TXN-3** unknown-outcome replay across the commit cut points (INV-5, INV-14). Each cut point kills
/// the commit at a different instant; the retry must resolve to exactly one committed result (or a fresh
/// execution when no original commit exists), with 0 duplicate state transitions.
///
/// Coverage is capability-gated so nothing is faked:
/// * BeforeAppend + in-process `request_id` replay run on EVERY profile.
/// * AfterAppendBeforeApply durable-replay runs on durable profiles (item-level exactly-once via the raw
///   [`inject_commit`] seam + reopen; the raw envelope carries no `request_id`, so this proves item-level
///   INV-14 at that exact instant — `push_with_request_id` is atomic from the caller and cannot be
///   interrupted between its internal append and apply through the public API).
/// * AfterApplyBeforeResponse lost-response-across-restart `request_id` replay is a REAL assertion on
///   EVERY durable profile: recovery rebuilds the push-idempotency map from the durable log for both
///   atomic and eventual-apply composed-log backends.
pub async fn ac_txn_3_unknown_outcome_replay<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
    caps: TxnCaps,
) -> AcOutcome {
    let mut asserts = Vec::new();

    // --- BeforeAppend (in-process) + AfterResponse in-process request_id replay: EVERY profile. ---
    {
        let a = make("txn3-before");
        a.create_queue(qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        let env = envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("101", "kb", 5)],
            }),
            vec![],
        );
        let killed = inject_commit(&a, env, CutPoint::BeforeAppend).await;
        ensure!(killed.is_err(), "BeforeAppend must not commit");
        ensure!(
            durable_command_count(&a).await? == 0,
            "BeforeAppend left a durable command"
        );
        // No original commit exists → a fresh request_id push executes exactly once (INV-14).
        let rid = RequestId::new("ac-txn-3-fresh").unwrap();
        let body = vec![spec("txn3-fresh", 5)];
        let ids = a
            .push_with_request_id(&shard(), rid.clone(), body.clone(), ts(1), None)
            .await
            .map_err(|e| format!("fresh push after BeforeAppend: {e:?}"))?;
        ensure!(ids.len() == 1, "fresh execution created exactly one item");
        // AfterResponse: a duplicate retry replays the one committed result (0 duplicate transitions).
        let replay = a
            .push_with_request_id(&shard(), rid, body, ts(2), None)
            .await
            .map_err(|e| format!("after-response replay: {e:?}"))?;
        ensure!(replay == ids, "after-response retry must replay the same result");
        ensure!(
            a.metrics(&qkey()).await.unwrap().pending == 1,
            "BeforeAppend + fresh + replay yields exactly one item"
        );
    }
    asserts.push(
        "BeforeAppend: no original commit -> fresh execution; AfterResponse: request_id replays exactly once".into(),
    );

    if !caps.durable_reopen {
        asserts.push(
            "AfterAppendBeforeApply + AfterApplyBeforeResponse restart clauses N/A (non-durable in-memory dev profile)".into(),
        );
        return Ok(asserts);
    }

    // --- AfterAppendBeforeApply: durable-but-unapplied -> recovery replays exactly once (item-level). ---
    {
        let a = make("txn3-append");
        a.create_queue(qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        let env = envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("102", "ka", 7)],
            }),
            vec![],
        );
        let pos = inject_commit(&a, env, CutPoint::AfterAppendBeforeApply)
            .await
            .map_err(|e| format!("AfterAppendBeforeApply append: {e:?}"))?;
        ensure!(!pos.is_empty(), "append returned a durable position");
        ensure!(
            durable_command_count(&a).await? == 1,
            "the command is durable on the log after the commit->apply kill"
        );
        ensure!(
            a.metrics(&qkey()).await.unwrap().pending == 0,
            "apply was skipped, so the in-process projection has not applied the command"
        );
        drop(a);
        // reopen: recovery replays the durable tail and applies it exactly once.
        let b = make("txn3-append");
        ensure!(
            b.metrics(&qkey()).await.map_err(|e| format!("metrics: {e:?}"))?.pending == 1,
            "committed-but-unapplied command must replay exactly once on recovery"
        );
        ensure!(
            b.select_eligible(&shard(), ts(100), 10).await.map_err(|e| format!("{e:?}"))?.len() == 1,
            "recovery applied the replayed command exactly once (0 duplicate state transitions)"
        );
    }
    asserts.push(
        "AfterAppendBeforeApply: durable command replays exactly once on recovery (0 duplicate transitions, item-level INV-14)".into(),
    );

    // --- AfterApplyBeforeResponse: committed+applied, RESPONSE LOST -> request_id replay after restart.
    // This is a REAL assertion on every durable profile (atomic + eventual): recovery rebuilds the
    // push-idempotency map from the durable log, so the retry of the already-committed request_id must
    // resolve to the ONE committed result with 0 duplicate state transitions.
    {
        let rid = RequestId::new("ac-txn-3-lost-response").unwrap();
        let body = vec![spec("txn3-lost", 3)];
        let committed_ids = {
            let a = make("txn3-lost");
            a.create_queue(qdef())
                .await
                .map_err(|e| format!("create_queue: {e:?}"))?;
            a.push_with_request_id(&shard(), rid.clone(), body.clone(), ts(1), None)
                .await
                .map_err(|e| format!("push before lost response: {e:?}"))?
            // The client never observes this success (the response is "lost"); we drop the handle.
        };
        // Kill + restart, then retry the same request_id (the client re-sends after the timeout).
        let b = make("txn3-lost");
        let replay = b
            .push_with_request_id(&shard(), rid, body, ts(2), None)
            .await
            .map_err(|e| format!("replay after lost response: {e:?}"))?;
        ensure!(
            replay == committed_ids,
            "same request_id after a lost response must replay the ONE committed result (got {replay:?} vs {committed_ids:?})"
        );
        let m = b.metrics(&qkey()).await.map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            m.pending == 1,
            "lost-response replay created a duplicate committed result (pending={})",
            m.pending
        );
        ensure!(
            b.select_eligible(&shard(), ts(100), 10).await.map_err(|e| format!("{e:?}"))?.len() == 1,
            "lost-response replay produced a duplicate state transition"
        );
    }
    asserts.push(
        "AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)".into(),
    );

    Ok(asserts)
}

/// **AC-TXN-6** cross-combination parity. Run the SAME operation history and the SAME failure schedule on
/// two backend profiles, then compare — after a restart of both — the final visible `QueueMetrics`
/// (including complete/failed terminal COUNTS), the `select_eligible` order, and the pending/active-lease
/// set. Uses explicit item ids (server-minted ids differ per backend by construction) so the compared
/// state is backend-independent. It does NOT compare per-`request_id` idempotency records or per-item
/// terminal-outcome records; the recorded assertion names exactly what is compared.
pub async fn ac_txn_6_parity<A: ConformanceCore + LogRead, B: ConformanceCore + LogRead>(
    make_a: impl Fn(&str) -> A,
    make_b: impl Fn(&str) -> B,
) -> AcOutcome {
    // Drive the identical op history + failure schedule against one backend, returning its post-restart
    // observable state as a comparable tuple.
    async fn run<X: ConformanceCore + LogRead>(
        make: &impl Fn(&str) -> X,
    ) -> Result<(pqueue_engine::QueueMetrics, Vec<String>, Vec<String>), String> {
        {
            let x = make("txn6");
            x.create_queue(qdef())
                .await
                .map_err(|e| format!("create_queue: {e:?}"))?;
            // Op history: three explicit-id pushes at distinct priorities.
            commit(
                &x,
                envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![
                            item("1", "k1", 30),
                            item("2", "k2", 10),
                            item("3", "k3", 20),
                        ],
                    }),
                    vec![],
                ),
            )
            .await;
            // Failure schedule step 1: BeforeAppend kill — must leave no effect on either backend.
            let killed = inject_commit(
                &x,
                envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item("99", "kg", 1)],
                    }),
                    vec![],
                ),
                CutPoint::BeforeAppend,
            )
            .await;
            if killed.is_ok() {
                return Err("BeforeAppend fault unexpectedly committed".into());
            }
            // Op history: claim + finalize the highest-priority item (p2 @ prio 10) -> terminal.
            let claimed = x
                .claim(claim_req(1, 500, 10))
                .await
                .map_err(|e| format!("claim: {e:?}"))?;
            let leased = claimed.items[0].item_id;
            x.finalize(
                &shard(),
                vec![FinalizeOutcome::new(leased, FinalizeKind::Complete)],
                ts(20),
                None,
            )
            .await
            .map_err(|e| format!("finalize: {e:?}"))?;
            // Failure schedule step 2: AfterAppendBeforeApply — durable-but-unapplied push of "p4".
            inject_commit(
                &x,
                envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item("4", "k4", 15)],
                    }),
                    vec![],
                ),
                CutPoint::AfterAppendBeforeApply,
            )
            .await
            .map_err(|e| format!("AfterAppendBeforeApply: {e:?}"))?;
        }
        // Restart both: recovery replays the durable-but-unapplied p4 exactly once.
        let x = make("txn6");
        let metrics = x
            .metrics(&qkey())
            .await
            .map_err(|e| format!("metrics: {e:?}"))?;
        let eligible: Vec<String> = x
            .select_eligible(&shard(), ts(100), 100)
            .await
            .map_err(|e| format!("select_eligible: {e:?}"))?
            .into_iter()
            .map(|i| i.to_string())
            .collect();
        let mut pending: Vec<String> = x
            .pending(&shard())
            .await
            .map_err(|e| format!("pending: {e:?}"))?
            .into_iter()
            .map(|l| format!("{}:{}", l.item_id, l.attempt_count))
            .collect();
        pending.sort();
        Ok((metrics, eligible, pending))
    }

    let a = run(&make_a).await?;
    let b = run(&make_b).await?;
    ensure!(
        a.0 == b.0,
        "final visible metrics diverge across combinations: {:?} vs {:?}",
        a.0,
        b.0
    );
    ensure!(
        a.1 == b.1,
        "final visible eligibility order diverges across combinations: {:?} vs {:?}",
        a.1,
        b.1
    );
    ensure!(
        a.2 == b.2,
        "active-lease / pending set diverges across combinations: {:?} vs {:?}",
        a.2,
        b.2
    );
    Ok(vec![format!(
        "identical final visible QueueMetrics (incl. complete/failed terminal counts), select_eligible order, and pending/active-lease set (item_id:attempt) across combinations; NOT compared: per-request_id idempotency records or per-item terminal-outcome records (metrics={:?}, eligible={:?}, pending={:?})",
        a.0, a.1, a.2
    )])
}

// ---------------------------------------------------------------------------
// Evidence JSONL
// ---------------------------------------------------------------------------

/// One evidence record for TP-003 §3.10 (`docs/perf/evidence/tp003-ac-txn-matrix.jsonl`).
#[derive(Debug)]
pub struct AcEvidence {
    pub ac: &'static str,
    pub backend: String,
    pub result: &'static str,
    pub detail: String,
    pub assertions: Vec<String>,
}

impl AcEvidence {
    fn to_json_line(&self, ts_rfc3339: &str) -> String {
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        let assertions = self
            .assertions
            .iter()
            .map(|a| format!("\"{}\"", esc(a)))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"suite\":\"external_transaction_contract_matrix_tests\",\"spec\":\"TP-003 §3.10\",\"ac\":\"{}\",\"backend\":\"{}\",\"result\":\"{}\",\"detail\":\"{}\",\"assertions\":[{}],\"recorded_at\":\"{}\"}}",
            self.ac,
            esc(&self.backend),
            self.result,
            esc(&self.detail),
            assertions,
            ts_rfc3339,
        )
    }
}

/// Resolve `docs/perf/evidence/` at the workspace root from this crate's manifest dir.
pub fn evidence_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/perf/evidence")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/perf/evidence"))
}

/// Write the evidence file, overwriting any prior run so the JSONL reflects exactly THIS run.
pub fn write_evidence(file_name: &str, records: &[AcEvidence]) -> std::io::Result<PathBuf> {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(file_name);
    // A coarse recorded_at without pulling a time crate: seconds since the epoch, ISO-ish.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let stamp = format!("epoch:{secs}");
    let body = records
        .iter()
        .map(|r| r.to_json_line(&stamp))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&path, format!("{body}\n"))?;
    Ok(path)
}
