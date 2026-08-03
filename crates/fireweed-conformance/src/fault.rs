//! Fault-injection harness (TP-001 `fault_injection_harness_tests`) and the reusable AC-TXN scenario
//! functions the `external_transaction_contract_matrix_tests` suite runs across backend profiles
//! (TP-003 §3.10, AC-TXN-1..7).
//!
//! # What this exercises for real
//!
//! The direct commit-pipeline seam the engine exposes to a driver is [`Backend::commit_raw`]: an async
//! operation accepting an owned [`fireweed_engine::RawCommitRequest`]. [`inject_commit`] drives that seam and injects
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
//! # Honest cut-point coverage (AC-TXN-3, TP-003 §3.10 row 208)
//!
//! `BeforeAppend` and `AfterResponse` are driven in-process through the real
//! [`fireweed_engine::PushPort::push_with_request_id`] idempotency path. The mid-pipeline
//! `AfterAppendBeforeApply` cut is now ALSO `request_id`-bearing: [`RequestIdReplayProbe`] builds the exact
//! durable `request_id`-bearing push envelope `push_with_request_id` would append (same request_id + body
//! fingerprint + `RequestOutcome` + minted ids), which [`inject_commit`] appends durably then leaves
//! unapplied; on reopen, recovery rebuilds the push-idempotency map from that durable envelope, so a retry by
//! `request_id` replays the one committed result (0 duplicate transitions). `AfterApplyBeforeResponse` proves
//! the same across a full restart. So PUSH `request_id` exactly-once replay is proven at ALL FOUR cut points.
//!
//! Only PUSH and `commit_transition` carry a `request_id` in this engine. `commit_transition` (the
//! authoritative claimed-work commit) is covered per its reachable cut points: in-process replay on atomic
//! backends; Unavailable → capability-N/A on eventual-apply; and on DURABLE atomic backends an ALL-COMMITTED
//! commit's cross-restart replay is PROVEN at both `AfterApplyBeforeResponse` (commit fully, kill, reopen) and
//! `AfterAppendBeforeApply` (append the request_id-bearing commit envelope via
//! [`RequestIdReplayProbe::build_request_id_commit_envelope`], kill before apply, reopen) — recovery rebuilds
//! `commit_idempotency` from the durable log (`rebuild_commit_idempotency_from_log`, the symmetric twin of the
//! push rebuild), so a same-body retry Replays the exact per-entry outcome, a different body → RequestIdConflict,
//! and the input is finalized exactly once. A MIXED committed+rejected commit is ALSO now replayed
//! BYTE-IDENTICALLY across restart at both cut points (bead pqueue-db60657d, closed): `commit_transition`
//! stamps the whole per-entry vec (committed AND rejected, each rejection's structured error projected via
//! `CommitRejection`) onto a terminal `RequestOutcome::CommitTransition` marker, and recovery reconstructs the
//! full `Vec<EntryRecovery>` from it — so a `[valid→Committed, stale→Rejected(StaleLease)]` retry replays the
//! exact vec (Rejected carrying the same StaleLease), `explain_commit` returns the identical full vec, and the
//! committed input is finalized exactly once (0 duplicate). The `AfterAppendBeforeApply` mixed cut is struck by
//! appending the mixed commit's durable envelopes (via
//! [`RequestIdReplayProbe::build_request_id_commit_envelopes`]) unapplied, then reopening. No residual `GAP`
//! remains for AC-TXN-3. The classic ports
//! (claim/renew/finalize/update_fields/purge/replace_if_pending) carry NO `request_id` and record
//! capability-N/A. This split is recorded per row in the evidence JSONL rather than papered over.

use std::collections::BTreeMap;
use std::path::PathBuf;

use bytes::Bytes;
use fireweed_core::{ClientItemKey, GateKeyPolicy, GroupKey, LeaseToken, PriorityValue, RequestId};
use fireweed_engine::{
    Backend, ClaimCompatibility, ClaimRef, ClaimedItem, CommandEnvelope, CommandPosition,
    CommitEntryOutcome, CommitEntryStatus, CommitRecovery, CommitTransition, CommitTransitionEntry,
    CommitTransitionPort, ControlPlaneStore, EngineError, EntryRecovery, FenceLeaseCommand,
    FinalizeKind, FinalizeOutcome, GroupBatching, LogRead, PayloadUpdate, PushCommand, PushSpec,
    QueueCommand, RawCommitFault, RawCommitRequest, RecoveryReadPort, RequestIdReplayProbe,
    SetGatesCommand, SetGatesPort, UnfenceLeaseCommand,
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

/// The transaction fixture used only by scenarios that exercise gate-bearing items and gate mutation.
/// The shared [`qdef`] intentionally keeps gates disabled so unrelated transaction scenarios do not gain
/// capabilities they never use.
fn gate_qdef() -> fireweed_core::QueueDefinition {
    let mut definition = qdef();
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition
}

/// Drive one raw commit through the backend's typed [`Backend::commit_raw`] seam, injecting a crash at
/// `cut`. Returns the appended positions when the append reached the durable log, or the injected error.
///
/// This is the reusable fault-injection primitive: every AC-TXN-3 cut is an owned request value rather than
/// arbitrary caller code running inside the backend transaction.
pub async fn inject_commit<B: Backend + ControlPlaneStore>(
    backend: &B,
    env: CommandEnvelope,
    cut: CutPoint,
) -> Result<Vec<CommandPosition>, EngineError> {
    let epoch = backend.current_epoch(&shard()).await?;
    let fault = match cut {
        CutPoint::BeforeAppend => RawCommitFault::BeforeAppend,
        CutPoint::AfterAppendBeforeApply => RawCommitFault::AfterAppendBeforeApply,
        CutPoint::AfterApplyBeforeResponse | CutPoint::AfterResponse => RawCommitFault::None,
    };
    backend
        .commit_raw(RawCommitRequest::new(shard(), vec![env], epoch).with_fault(fault))
        .await
        .map(|outcome| outcome.into_positions())
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
/// Coverage: EVERY mutating operation TP-003 §3.10 row 206 names — `CreateQueue`, `BatchPush` (via the
/// acknowledged `request_id` path), `BatchUpdate`, `SetGates`, `BatchClaim`, `BatchRenewLeases`,
/// `BatchFinalize`, and `PurgeItems`. The four core lifecycle ops (Push/Claim/RenewLeases/Finalize) are
/// checkpointed inline; the remaining four (`CreateQueue`-alone, `BatchUpdate`, `SetGates`, `PurgeItems`)
/// each get their own kill-after-success checkpoint on an isolated tag via the `ac_txn_1_kill_after_*`
/// helpers below, so no later op can mask an earlier op's assertion. Two ops are capability-gated and record
/// no capability N/A for the current profile matrix: every configured projection supports both
/// `BatchUpdate` and `SetGates`, including durable replay after reopen.
pub async fn ac_txn_1_success_durable_visible<B: ConformanceCore + LogRead + SetGatesPort>(
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
    ensure!(
        acked.len() == 2,
        "expected 2 acked push ids, got {}",
        acked.len()
    );
    {
        let b = make("txn1");
        let m = b
            .metrics(&qkey())
            .await
            .map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            (m.pending, m.leased, m.complete) == (2, 0, 0),
            "BatchPush not durable after kill/reopen; got pending={} leased={} complete={}",
            m.pending,
            m.leased,
            m.complete
        );
        let live = b
            .live_items(&shard(), std::slice::from_ref(&key_b))
            .await
            .map_err(|e| format!("live_items: {e:?}"))?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| "pushed item not visible by client_item_key after reopen".to_string())?;
        ensure!(
            live.item_id == acked[1],
            "pushed item id mismatch after reopen"
        );
        ensure!(
            live.payload.as_deref() == Some(&b"txn1-b"[..]),
            "pushed payload lost across reopen"
        );
    }
    asserts.push(
        "BatchPush effect durable + visible/claimable after kill/reopen (0 missing acked commands)"
            .into(),
    );

    // --- BatchClaim then kill/reopen and assert the item is durably Leased.
    // Lease absolute times stay within qdef().max_lease_duration_ms (60s): claim expires at 50 with
    // now=10 (40s), renew extends to 75 with now=20 (55s). Tick at 60 is past the original 50 and
    // before the renewed 75 so it proves the extended deadline survived reopen.
    let leased_id = {
        let b = make("txn1");
        let claimed = b
            .claim(claim_req(1, 50, 10))
            .await
            .map_err(|e| format!("claim: {e:?}"))?;
        ensure!(claimed.items.len() == 1, "claim leased one item");
        claimed.items[0].item_id
    };
    {
        let b = make("txn1");
        let m = b
            .metrics(&qkey())
            .await
            .map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            (m.pending, m.leased, m.complete) == (1, 1, 0),
            "BatchClaim lease not durable after kill/reopen; got pending={} leased={} complete={}",
            m.pending,
            m.leased,
            m.complete
        );
    }
    asserts.push("BatchClaim lease durable after kill/reopen".into());

    // --- BatchRenewLeases (extend the deadline 50 -> 75) then kill/reopen and prove the RENEWED
    // deadline survived: a reclaim tick at 60 (past the original 50, before the renewed 75) must NOT
    // reclaim the lease. This checkpoint runs BEFORE the finalize, so the renew effect cannot be masked.
    {
        let b = make("txn1");
        b.renew(&shard(), vec![leased_id], ts(75), ts(20), None)
            .await
            .map_err(|e| format!("renew: {e:?}"))?;
    }
    {
        let b = make("txn1");
        b.tick(ts(60)).await.map_err(|e| format!("tick: {e:?}"))?;
        let m = b
            .metrics(&qkey())
            .await
            .map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            (m.pending, m.leased) == (1, 1),
            "BatchRenewLeases deadline lost after kill/reopen: a tick at 60 reclaimed the lease that renew extended to 75; got pending={} leased={}",
            m.pending,
            m.leased
        );
    }
    asserts.push("BatchRenewLeases extended deadline durable after kill/reopen (tick before renewed deadline does not reclaim)".into());

    // --- BatchFinalize(Complete) then kill/reopen and assert the terminal state survived.
    {
        let b = make("txn1");
        b.finalize(
            &shard(),
            vec![FinalizeOutcome::new(leased_id, FinalizeKind::Complete)],
            ts(65),
            None,
        )
        .await
        .map_err(|e| format!("finalize: {e:?}"))?;
    }
    {
        let b = make("txn1");
        let m = b
            .metrics(&qkey())
            .await
            .map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            (m.pending, m.leased, m.complete) == (1, 0, 1),
            "BatchFinalize terminal state not durable after kill/reopen; got pending={} leased={} complete={}",
            m.pending,
            m.leased,
            m.complete
        );
        // The surviving pending sibling is still claimable (0 read-after-success gaps).
        let claimed = b
            .claim(claim_req(1, 150, 70))
            .await
            .map_err(|e| format!("claim: {e:?}"))?;
        ensure!(
            claimed.items.first().map(|i| i.item_id) == Some(acked[1]),
            "surviving pending item not claimable after finalize+reopen"
        );
    }
    asserts.push(
        "BatchFinalize terminal state durable after kill/reopen; sibling still claimable".into(),
    );

    // TP-003 §3.10 row 206 requires kill-after-success for EVERY mutating op. The four core lifecycle ops
    // above are checkpointed inline; the remaining named ops each get their OWN kill-after-success checkpoint
    // here, on isolated tags so no op masks another. Each records a real durability assertion, or an honest
    // Every current profile exercises both BatchUpdate and SetGates. No op is silently skipped.
    asserts.extend(ac_txn_1_kill_after_create_queue(&make).await?);
    asserts.extend(ac_txn_1_kill_after_batch_update(&make).await?);
    asserts.extend(ac_txn_1_kill_after_set_gates(&make).await?);
    asserts.extend(ac_txn_1_kill_after_purge_items(&make).await?);

    Ok(asserts)
}

/// **AC-TXN-1 / CreateQueue** kill-after-success (TP-003 §3.10 row 206). Create the queue, kill+reopen the
/// durable store, and assert the queue definition survived: the recovered store re-serves the queue (metrics
/// answered, empty) AND the queue is usable (accepts a push). No items are needed.
pub async fn ac_txn_1_kill_after_create_queue<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
) -> AcOutcome {
    {
        let a = make("txn1-createq");
        a.create_queue(qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
    }
    // Kill + reopen: the CreateQueue command must be durable so recovery re-serves the queue.
    let b = make("txn1-createq");
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics after reopen (queue not recovered?): {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete) == (0, 0, 0),
        "CreateQueue reopen surfaced phantom items; got pending={} leased={} complete={}",
        m.pending,
        m.leased,
        m.complete
    );
    // Usable: the recovered queue accepts a push (a push into an unknown queue would fail), proving the
    // definition survived rather than merely an empty shard.
    let ids = b
        .push(&shard(), vec![spec("createq-a", 1)], ts(1), None)
        .await
        .map_err(|e| format!("push into recovered queue: {e:?}"))?;
    ensure!(ids.len() == 1, "recovered queue did not accept a push");
    Ok(vec![
        "CreateQueue effect durable after kill/reopen: recovery re-serves the queue (metrics answered, empty) and it is usable (accepts a push)".into(),
    ])
}

/// **AC-TXN-1 / BatchUpdate** kill-after-success (TP-003 §3.10 row 206). `update_fields` a pending item, then
/// kill+reopen and assert the merged field survives and is visible on the recovered live-item view. The
/// mutation is log-authoritative and supported by both current durability classes.
pub async fn ac_txn_1_kill_after_batch_update<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
) -> AcOutcome {
    let key = ClientItemKey::new("batchupd-a").unwrap();
    let item_id = {
        let a = make("txn1-batchupd");
        a.create_queue(qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        let ids = a
            .push(&shard(), vec![spec("batchupd-a", 5)], ts(1), None)
            .await
            .map_err(|e| format!("seed push: {e:?}"))?;
        ensure!(ids.len() == 1, "seed push landed one item");
        let mut field_ops: BTreeMap<String, Option<Bytes>> = BTreeMap::new();
        field_ops.insert(
            "worker_stage".to_string(),
            Some(Bytes::from_static(b"ready-durable")),
        );
        a.update_fields(
            &shard(),
            ids[0],
            field_ops,
            PayloadUpdate::Keep,
            None,
            None,
            ts(2),
            None,
        )
        .await
        .map_err(|e| format!("update_fields: {e:?}"))?;
        ids[0]
    };
    // Kill + reopen: the merged field must survive and be visible on the recovered live-item view.
    let b = make("txn1-batchupd");
    let live = b
        .live_items(&shard(), std::slice::from_ref(&key))
        .await
        .map_err(|e| format!("live_items: {e:?}"))?
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| "updated item not visible by client_item_key after reopen".to_string())?;
    ensure!(
        live.item_id == item_id,
        "updated item id mismatch after reopen"
    );
    ensure!(
        live.fields.get("worker_stage").map(|v| v.as_ref()) == Some(&b"ready-durable"[..]),
        "BatchUpdate field lost across kill/reopen; got fields={:?}",
        live.fields
    );
    Ok(vec![
        "BatchUpdate (UpdateFields) effect durable + visible after kill/reopen: the merged field survives on the recovered live-item view".into(),
    ])
}

/// **AC-TXN-1 / SetGates** kill-after-success (TP-003 §3.10 row 206). On a gate-capable backend: block a gate
/// key over a gate-bearing item, kill+reopen, and assert the durable gate state survives — the gated item
/// stays hidden from claim (unclaimable) and pending on the recovered store. The current in-memory/log-replay
/// and relational projections all persist gate membership and state, so every configured profile exercises
/// this checkpoint. A future projection that honestly advertises `supports_gates() == false` still records
/// capability-N/A rather than silently passing.
pub async fn ac_txn_1_kill_after_set_gates<B: ConformanceCore + LogRead + SetGatesPort>(
    make: impl Fn(&str) -> B,
) -> AcOutcome {
    if !make("txn1-setgates").supports_gates() {
        return Ok(vec![
            "capability-N/A: SetGates requires a gate-capable projection; this profile reports supports_gates()=false and refuses SetGates (EngineError::Unavailable). This is a backend-capability property, not a coverage gap — kill-after-SetGates cannot exist on this backend".into(),
        ]);
    }
    {
        let a = make("txn1-setgates");
        a.create_queue(gate_qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        let mut gated = spec("setgates-a", 10);
        gated.gate_keys = vec!["region-eu".to_string()];
        a.push(&shard(), vec![gated], ts(0), None)
            .await
            .map_err(|e| format!("gate-bearing push: {e:?}"))?;
        a.set_gates(
            &shard(),
            SetGatesCommand {
                gate_keys: vec!["region-eu".to_string()],
                blocked: true,
            },
            ts(1),
            None,
        )
        .await
        .map_err(|e| format!("set_gates: {e:?}"))?;
    }
    // Kill + reopen: the blocked-gate state must survive so the gated item stays unclaimable but pending.
    let b = make("txn1-setgates");
    let claimed = b
        .claim(claim_req(10, 500, 100))
        .await
        .map_err(|e| format!("claim: {e:?}"))?;
    ensure!(
        claimed.items.is_empty(),
        "blocked gate did not survive kill/reopen: the gated item was claimable"
    );
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        m.pending == 1,
        "gated item not pending after reopen; got pending={}",
        m.pending
    );
    Ok(vec![
        "SetGates blocked-gate state durable after kill/reopen: the gated item stays hidden from claim (unclaimable) and pending on the recovered store".into(),
    ])
}

/// **AC-TXN-1 / PurgeItems** kill-after-success (TP-003 §3.10 row 206). Purge one of two pending items via
/// `PurgePort`, then kill+reopen and assert the purged item is GONE and does not resurrect on replay, while
/// the un-purged sibling survives and is still claimable (0 read-after-success gaps for the survivor).
pub async fn ac_txn_1_kill_after_purge_items<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
) -> AcOutcome {
    let purged_key = ClientItemKey::new("purge-a").unwrap();
    let survivor_id = {
        let a = make("txn1-purge");
        a.create_queue(qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        let ids = a
            .push(
                &shard(),
                vec![spec("purge-a", 5), spec("purge-b", 9)],
                ts(1),
                None,
            )
            .await
            .map_err(|e| format!("seed push: {e:?}"))?;
        ensure!(ids.len() == 2, "seed push landed two items");
        // Purge the first (pending, non-leased) item; force=false suffices since it is not leased.
        let removed = a
            .purge(&shard(), vec![ids[0]], false, ts(2), None)
            .await
            .map_err(|e| format!("purge: {e:?}"))?;
        ensure!(
            removed == 1,
            "purge removed exactly one item; got {removed}"
        );
        ids[1]
    };
    // Kill + reopen: the PurgeItems command must be durable so replay keeps the item gone (no resurrection).
    let b = make("txn1-purge");
    let live = b
        .live_items(&shard(), std::slice::from_ref(&purged_key))
        .await
        .map_err(|e| format!("live_items: {e:?}"))?
        .into_iter()
        .next()
        .flatten();
    ensure!(
        live.is_none(),
        "purged item resurrected on kill/reopen replay (still visible by client_item_key)"
    );
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        m.pending == 1,
        "purge not durable after reopen: expected exactly the 1 survivor pending; got pending={}",
        m.pending
    );
    // The survivor is still claimable (0 read-after-success gaps for the un-purged item).
    let claimed = b
        .claim(claim_req(1, 500, 10))
        .await
        .map_err(|e| format!("claim: {e:?}"))?;
    ensure!(
        claimed.items.first().map(|i| i.item_id) == Some(survivor_id),
        "survivor not claimable after purge+reopen"
    );
    Ok(vec![
        "PurgeItems effect durable after kill/reopen: the purged item is gone and does not resurrect on replay; the un-purged sibling survives and is claimable".into(),
    ])
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

    // (a) per-item invalid: finalize a pending (non-leased) item → structured rejection, appends nothing.
    // Classic projection finalize_validate returns Invalid("item is not leased"); the async item-id
    // path resolves leases via render_claimed first and maps missing/pending cleartext to StaleLease.
    // Both are structured rejections with zero durable effect (proven by the command-count check).
    ensure!(
        matches!(
            a.finalize(
                &shard(),
                vec![FinalizeOutcome::new(accepted_id, FinalizeKind::Complete)],
                ts(10),
                None,
            )
            .await,
            Err(EngineError::Invalid(_)) | Err(EngineError::StaleLease)
        ),
        "finalize of a pending item must be a structured rejection"
    );
    // (b) unknown-id renew → NotFound, appends nothing.
    ensure!(
        a.renew(
            &shard(),
            vec![fireweed_core::ItemId::new("404").unwrap()],
            ts(500),
            ts(10),
            None
        )
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
    let m_rejects = a
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        (
            m_rejects.pending,
            m_rejects.leased,
            m_rejects.complete,
            m_rejects.failed
        ) == (1, 0, 0, 0),
        "rejected finalize/renew changed visible state; got pending={} leased={} complete={} failed={}",
        m_rejects.pending,
        m_rejects.leased,
        m_rejects.complete,
        m_rejects.failed
    );
    // (c) request-id conflict: reuse a request_id with a different body → RequestIdConflict.
    let rid = RequestId::new("ac-txn-2-rid").unwrap();
    a.push_with_request_id(
        &shard(),
        rid.clone(),
        vec![spec("txn2-rid", 1)],
        ts(11),
        None,
    )
    .await
    .map_err(|e| format!("first request-id push: {e:?}"))?;
    let after_rid = durable_command_count(&a).await?;
    ensure!(
        matches!(
            a.push_with_request_id(
                &shard(),
                rid,
                vec![spec("txn2-rid-different", 2)],
                ts(12),
                None
            )
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
    let m = a
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete, m.failed) == (2, 0, 0, 0),
        "rejected mutations must leave 0 visible effect; got pending={} leased={} complete={} failed={}",
        m.pending,
        m.leased,
        m.complete,
        m.failed
    );
    asserts.push(
        "rejected finalize/renew/request-id-conflict appended 0 durable commands and left 0 visible effect".into(),
    );

    // TP-003 §3.10 row 207 requires the FULL rejection-class surface: envelope-invalid batches, per-item
    // invalid/conflict/stale cases, capacity/unavailable paths, AND commit-timeout paths — each leaving 0
    // durable effect (re-verified after restart+replay on durable profiles) while an accepted sibling keeps
    // normal success. The three classes above (per-item-invalid finalize, unknown-id renew,
    // request-id-conflict) are joined here by the remaining classes, each driven against the SAME profile via
    // its own isolated store tag so no scenario masks another. Capability-N/A is recorded (never a silent
    // pass) where a class genuinely cannot occur on this backend. Current profiles expose the full inherent
    // operation surface, so AC-TXN-2 records the Unavailable subclass as N/A rather than manufacturing it by
    // disabling a supported operation.
    asserts.extend(ac_txn_2_envelope_invalid_batch(&make, caps).await?);
    asserts.extend(ac_txn_2_stale_lease_conflict(&make, caps).await?);
    asserts.extend(ac_txn_2_capacity_unavailable_path(&make, caps).await?);
    asserts.extend(ac_txn_2_commit_timeout_path(&make, caps).await?);

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
        m.pending,
        m.leased,
        m.complete,
        m.failed
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

/// The post-rejection observable baseline: the accepted sibling is the ONLY visible item and a rejection
/// appended nothing. Shared assertion used by the AC-TXN-2 rejection-class functions below.
async fn assert_only_sibling_pending<B: ConformanceCore>(
    b: &B,
    where_: &str,
) -> Result<(), String> {
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics ({where_}): {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete, m.failed) == (1, 0, 0, 0),
        "{where_}: rejection changed visible state; got pending={} leased={} complete={} failed={}",
        m.pending,
        m.leased,
        m.complete,
        m.failed
    );
    Ok(())
}

/// **AC-TXN-2 / envelope-invalid batch** (TP-003 §3.10 row 207): a structurally-invalid command envelope
/// the validators reject at the ENVELOPE level — a charset-invalid `group_key`, and a `group_batching` unit
/// with no configured `max_eligible_group_size` — is rejected `Invalid` before any append. Asserts 0 durable
/// commands, 0 visible effect (re-verified after restart+replay on a durable profile), while the accepted
/// sibling push retains normal success (durable + visible).
pub async fn ac_txn_2_envelope_invalid_batch<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
    caps: TxnCaps,
) -> AcOutcome {
    let mut asserts = Vec::new();
    let accepted_key = ClientItemKey::new("txn2env-accepted").unwrap();
    let a = make("txn2-env");
    a.create_queue(qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;
    let ids = a
        .push(&shard(), vec![spec("txn2env-accepted", 5)], ts(0), None)
        .await
        .map_err(|e| format!("accepted push: {e:?}"))?;
    ensure!(ids.len() == 1, "accepted push landed one item");

    let before = durable_command_count(&a).await?;

    // (a) charset-invalid group_key: an envelope-level field the claim validator rejects (`^[A-Za-z0-9._:-]$`).
    let mut req = claim_req(10, 500, 10);
    req.compatibility = ClaimCompatibility {
        group_key: Some(GroupKey::new("bad key!").unwrap()),
        ..Default::default()
    };
    ensure!(
        matches!(a.claim(req).await, Err(EngineError::Invalid(_))),
        "a charset-invalid group_key must be rejected Invalid at envelope validation"
    );
    // (b) structurally-invalid group_batching unit (no max_eligible_group_size configured) -> Invalid.
    let mut req2 = claim_req(10, 500, 10);
    req2.compatibility = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 1 }),
        ..Default::default()
    };
    ensure!(
        matches!(a.claim(req2).await, Err(EngineError::Invalid(_))),
        "a structurally-invalid group_batching envelope must be rejected Invalid"
    );

    let after = durable_command_count(&a).await?;
    ensure!(
        after == before,
        "envelope-invalid claims appended durable commands ({before} -> {after})"
    );
    assert_only_sibling_pending(&a, "envelope-invalid").await?;
    // Accepted sibling retains normal success: durable + visible by client_item_key.
    ensure!(
        a.live_items(&shard(), std::slice::from_ref(&accepted_key))
            .await
            .map_err(|e| format!("live_items: {e:?}"))?
            .into_iter()
            .next()
            .flatten()
            .is_some(),
        "accepted sibling must be visible (normal success) alongside the rejected envelopes"
    );
    asserts.push(
        "envelope-invalid batch (charset-invalid group_key + structurally-invalid group_batching) rejected at envelope validation: 0 durable commands, 0 visible effect; accepted sibling remains visible".into(),
    );

    if !caps.durable_reopen {
        asserts.push(
            "envelope-invalid restart-replay clause N/A (non-durable in-memory dev profile)".into(),
        );
        return Ok(asserts);
    }
    drop(a);
    let b = make("txn2-env");
    assert_only_sibling_pending(&b, "envelope-invalid after restart").await?;
    ensure!(
        b.live_items(&shard(), std::slice::from_ref(&accepted_key))
            .await
            .map_err(|e| format!("live_items after restart: {e:?}"))?
            .into_iter()
            .next()
            .flatten()
            .is_some(),
        "accepted sibling must retain normal success after restart"
    );
    asserts.push(
        "envelope-invalid batch left 0 durable effect across restart+replay; accepted sibling survives".into(),
    );
    Ok(asserts)
}

/// **AC-TXN-2 / stale-lease / fenced conflict** (TP-003 §3.10 row 207): finalize/renew of an operator-fenced
/// (stale-generation) lease is rejected `StaleLease` and appends nothing, while a validly-leased sibling in
/// the same batch still finalizes normally. The 0-durable-effect of the rejection is re-verified after
/// restart+replay on a durable profile (the fence survives, so the post-restart finalize is still StaleLease).
pub async fn ac_txn_2_stale_lease_conflict<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
    caps: TxnCaps,
) -> AcOutcome {
    let mut asserts = Vec::new();
    let a = make("txn2-stale");
    a.create_queue(qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;
    // Two items: a "victim" whose lease will be fenced, and a validly-leased "sibling".
    let ids = a
        .push(
            &shard(),
            vec![spec("txn2stale-victim", 5), spec("txn2stale-sibling", 9)],
            ts(0),
            None,
        )
        .await
        .map_err(|e| format!("seed push: {e:?}"))?;
    ensure!(ids.len() == 2, "seed push landed two items");
    let victim = ids[0];
    let sibling = ids[1];
    // Lease both, then operator-fence the victim's lease (stale generation).
    let claimed = a
        .claim(claim_req(10, 500, 10))
        .await
        .map_err(|e| format!("claim: {e:?}"))?;
    ensure!(claimed.items.len() == 2, "claim leased both items");
    commit(
        &a,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![victim],
            }),
            vec![victim],
        ),
    )
    .await;

    let before = durable_command_count(&a).await?;
    // Finalize the fenced lease -> StaleLease, appends nothing.
    ensure!(
        matches!(
            a.finalize(
                &shard(),
                vec![FinalizeOutcome::new(victim, FinalizeKind::Complete)],
                ts(20),
                None,
            )
            .await,
            Err(EngineError::StaleLease)
        ),
        "finalize of an operator-fenced lease must be StaleLease"
    );
    // Renew the fenced lease -> StaleLease, appends nothing.
    // Keep renewal duration within qdef().max_lease_duration_ms (60s) so validation reaches
    // the lease/fence check rather than failing closed on Invalid("invalid lease renewal duration").
    ensure!(
        matches!(
            a.renew(&shard(), vec![victim], ts(70), ts(21), None).await,
            Err(EngineError::StaleLease)
        ),
        "renew of an operator-fenced lease must be StaleLease"
    );
    let after = durable_command_count(&a).await?;
    ensure!(
        after == before,
        "stale-lease finalize/renew appended durable commands ({before} -> {after})"
    );
    // Both items still leased (the fence keeps the victim leased; nothing finalized yet).
    let m = a
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete, m.failed) == (0, 2, 0, 0),
        "stale-lease rejection changed visible state; got pending={} leased={} complete={} failed={}",
        m.pending,
        m.leased,
        m.complete,
        m.failed
    );
    // The validly-leased sibling still finalizes normally (accepted success in the same batch).
    a.finalize(
        &shard(),
        vec![FinalizeOutcome::new(sibling, FinalizeKind::Complete)],
        ts(22),
        None,
    )
    .await
    .map_err(|e| format!("sibling finalize: {e:?}"))?;
    let m = a
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics after sibling finalize: {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete, m.failed) == (0, 1, 1, 0),
        "validly-leased sibling did not finalize normally; got pending={} leased={} complete={} failed={}",
        m.pending,
        m.leased,
        m.complete,
        m.failed
    );
    asserts.push(
        "stale-lease/fenced conflict: finalize+renew of an operator-fenced lease -> StaleLease, 0 durable commands, 0 visible effect; a validly-leased sibling finalizes normally".into(),
    );

    if !caps.durable_reopen {
        asserts.push(
            "stale-lease restart-replay clause N/A (non-durable in-memory dev profile)".into(),
        );
        return Ok(asserts);
    }
    drop(a);
    let b = make("txn2-stale");
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics after restart: {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete, m.failed) == (0, 1, 1, 0),
        "post-restart state diverged; got pending={} leased={} complete={} failed={}",
        m.pending,
        m.leased,
        m.complete,
        m.failed
    );
    // The fence survived replay: the victim's finalize is STILL StaleLease (rejection still leaves 0 effect).
    ensure!(
        matches!(
            b.finalize(
                &shard(),
                vec![FinalizeOutcome::new(victim, FinalizeKind::Complete)],
                ts(30),
                None,
            )
            .await,
            Err(EngineError::StaleLease)
        ),
        "the operator fence must survive restart so the finalize is still StaleLease (0 durable effect)"
    );
    asserts.push(
        "stale-lease/fenced conflict left 0 durable effect across restart+replay (fence survives; finalize still StaleLease); accepted sibling's Complete survives".into(),
    );
    // Unfence + finalize the victim so no leaked fenced lease outlives the scenario is unnecessary for the
    // assertion, but proves the fence was the sole reason: after unfence, finalize succeeds.
    commit(
        &b,
        envelope(
            QueueCommand::UnfenceLease(UnfenceLeaseCommand {
                item_ids: vec![victim],
            }),
            vec![victim],
        ),
    )
    .await;
    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(victim, FinalizeKind::Complete)],
        ts(31),
        None,
    )
    .await
    .map_err(|e| format!("post-unfence finalize: {e:?}"))?;
    asserts.push(
        "control: after operator UNfence the same finalize succeeds (StaleLease was the sole, fence-scoped rejection)".into(),
    );
    Ok(asserts)
}

/// **AC-TXN-2 / capacity / unavailable path** (TP-003 §3.10 row 207): a capacity/batch-limit rejection
/// (`group_batching` whose group ceiling exceeds the requested `max_items` -> `BatchTooLarge`) leaves 0
/// durable effect on EVERY backend. The Unavailable rejection subclass is capability-N/A for the current
/// full-capability profiles: construction may select storage, but it must not remove an inherent Fireweed
/// operation. The test records that N/A explicitly rather than manufacturing `Unavailable` from a supported
/// operation such as upsert or field mutation.
pub async fn ac_txn_2_capacity_unavailable_path<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
    caps: TxnCaps,
) -> AcOutcome {
    let mut asserts = Vec::new();
    let accepted_key = ClientItemKey::new("txn2cap-accepted").unwrap();
    let a = make("txn2-cap");
    // A queue with a bounded eligible group size so a whole-group claim exceeding `max_items` is a genuine
    // capacity/batch-limit rejection at envelope validation.
    let mut def = qdef();
    def.max_eligible_group_size = Some(2);
    a.create_queue(def)
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;
    let mut accepted = spec("txn2cap-accepted", 5);
    accepted.group_key = Some(GroupKey::new("txn2cap-group").unwrap());
    let ids = a
        .push(&shard(), vec![accepted], ts(0), None)
        .await
        .map_err(|e| format!("accepted push: {e:?}"))?;
    ensure!(ids.len() == 1, "accepted push landed one item");

    let before = durable_command_count(&a).await?;

    // Capacity/batch-limit: group_batching whole-group (ceiling 2) with max_items=1 -> BatchTooLarge.
    let mut req = claim_req(1, 500, 10);
    req.compatibility = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 1 }),
        ..Default::default()
    };
    ensure!(
        matches!(a.claim(req).await, Err(EngineError::BatchTooLarge)),
        "a whole-group claim whose group ceiling exceeds max_items must be BatchTooLarge"
    );

    asserts.push(
        "capability-N/A: no inherent Fireweed operation is specified to return Unavailable on any current storage profile; upsert and field mutation remain supported across durability classes, while the BatchTooLarge capacity rejection above is exercised for real".into(),
    );

    let after = durable_command_count(&a).await?;
    ensure!(
        after == before,
        "capacity/unavailable rejections appended durable commands ({before} -> {after})"
    );
    assert_only_sibling_pending(&a, "capacity/unavailable").await?;
    asserts.push(
        "capacity/batch-limit: group_batching claim exceeding max_items -> BatchTooLarge, 0 durable commands, 0 visible effect; accepted sibling remains".into(),
    );

    if !caps.durable_reopen {
        asserts.push(
            "capacity/unavailable restart-replay clause N/A (non-durable in-memory dev profile)"
                .into(),
        );
        return Ok(asserts);
    }
    drop(a);
    let b = make("txn2-cap");
    assert_only_sibling_pending(&b, "capacity/unavailable after restart").await?;
    ensure!(
        b.live_items(&shard(), std::slice::from_ref(&accepted_key))
            .await
            .map_err(|e| format!("live_items after restart: {e:?}"))?
            .into_iter()
            .next()
            .flatten()
            .is_some(),
        "accepted sibling must retain normal success after restart"
    );
    asserts.push(
        "capacity/unavailable path left 0 durable effect across restart+replay; accepted sibling survives".into(),
    );
    Ok(asserts)
}

/// **AC-TXN-2 / commit-timeout / abort path** (TP-003 §3.10 row 207): the DANGEROUS commit-timeout instant —
/// a commit that fails AFTER the durable append has begun but before the projection apply / response
/// completes ([`CutPoint::AfterAppendBeforeApply`], the [`inject_commit`] seam appends durably then skips
/// apply). The correct contract (identical to AC-TXN-3's AfterAppendBeforeApply) is exactly-once, NOT
/// no-effect-because-nothing-was-written: the call yields no client-visible success (the projection never
/// applied it — an UNKNOWN outcome), and on drop+reopen recovery replays the durable tail EXACTLY ONCE, so
/// the item ends committed once with 0 duplicate / 0 half-applied state transitions, while the accepted
/// sibling committed just before is unaffected.
///
/// Capability-N/A on the UNIFIED rebuildable-cache store (`sqlite_relational`): its [`LogStore::append`] is
/// stage-only (no durable log row) and `read_from` is empty — append+apply commit as ONE relational
/// transaction, so a durable-but-unapplied append→apply window cannot exist for a commit-timeout to strike
/// (the same reason AC-TXN-3 records that profile N/A). The abort is still shown to leave 0 durable effect.
pub async fn ac_txn_2_commit_timeout_path<B: ConformanceCore + LogRead>(
    make: impl Fn(&str) -> B,
    caps: TxnCaps,
) -> AcOutcome {
    let mut asserts = Vec::new();
    let accepted_key = ClientItemKey::new("txn2commit-accepted").unwrap();
    let aborted_key = ClientItemKey::new("txn2commit-aborted").unwrap();
    let aborted_env = || {
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("777", "txn2commit-aborted", 7)],
            }),
            vec![],
        )
    };
    let a = make("txn2-commit");
    a.create_queue(qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;
    // Accepted sibling: a normal (append+apply) push that MUST retain normal success throughout.
    let ids = a
        .push(&shard(), vec![spec("txn2commit-accepted", 5)], ts(0), None)
        .await
        .map_err(|e| format!("accepted push: {e:?}"))?;
    ensure!(ids.len() == 1, "accepted push landed one item");

    // Does this backend expose a REPLAYABLE durable command log? The composed log+projection family does
    // (the applied sibling push shows on the durable log); the unified rebuildable-cache store returns an
    // empty page from `read_from` because its append is stage-only, so it has no append→apply window.
    let has_replayable_log = durable_command_count(&a).await? > 0;

    if !has_replayable_log {
        // Unified rebuildable-cache store (sqlite_relational): drive the append→apply seam anyway and prove
        // there is NO durable-but-unapplied tail — the staged append vanishes with the un-run apply, so the
        // abort leaves 0 durable effect and the sibling is intact. This is capability-N/A (no such window),
        // NOT a coverage gap: the store's append+apply are one atomic transaction.
        let injected = inject_commit(&a, aborted_env(), CutPoint::AfterAppendBeforeApply).await;
        ensure!(
            injected.is_ok(),
            "the append→apply seam should stage positions on the unified store; got {injected:?}"
        );
        ensure!(
            durable_command_count(&a).await? == 0,
            "the unified store must stage no durable log row (append is stage-only)"
        );
        assert_only_sibling_pending(&a, "commit-timeout (unified rebuildable-cache store)").await?;
        if caps.durable_reopen {
            drop(a);
            let b = make("txn2-commit");
            assert_only_sibling_pending(&b, "commit-timeout (unified store) after restart").await?;
            ensure!(
                b.live_items(&shard(), std::slice::from_ref(&aborted_key))
                    .await
                    .map_err(|e| format!("live_items after restart: {e:?}"))?
                    .into_iter()
                    .next()
                    .flatten()
                    .is_none(),
                "the aborted, never-applied commit must not resurrect on the unified store's reopen"
            );
        }
        asserts.push(
            "capability-N/A: this backend's LogStore::append is stage-only (unified rebuildable-cache; log axis IS projection axis), so append+apply commit atomically in ONE transaction and there is no durable-but-unapplied append→apply window for a commit-timeout to strike (mirrors AC-TXN-3's sqlite_relational N/A). Verified: an apply aborted after the staged append leaves 0 durable effect and the accepted sibling is intact".into(),
        );
        return Ok(asserts);
    }

    // Composed log+projection family: strike the REAL commit-timeout window. The append lands durably on the
    // command log, but the projection apply (and thus the client response) never runs — an unknown outcome.
    let before = durable_command_count(&a).await?;
    let pos = inject_commit(&a, aborted_env(), CutPoint::AfterAppendBeforeApply)
        .await
        .map_err(|e| format!("AfterAppendBeforeApply inject: {e:?}"))?;
    ensure!(
        !pos.is_empty(),
        "the aborted commit staged a durable position"
    );
    ensure!(
        durable_command_count(&a).await? == before + 1,
        "the aborted commit-in-window must be durably logged exactly once (before={before})"
    );
    // No client-visible success: the projection never applied the aborted command, so ONLY the accepted
    // sibling is visible in-process (0 half-applied projection state) — the outcome is unknown until recovery.
    assert_only_sibling_pending(&a, "commit-timeout in-window (durable-but-unapplied)").await?;
    asserts.push(
        "commit-timeout/abort in the append→apply window: the command is durably logged but its projection apply never ran, so there is no client-visible success and no half-applied projection in-process (unknown outcome)".into(),
    );

    if !caps.durable_reopen {
        asserts.push(
            "commit-timeout recovery-replay clause N/A (non-durable in-memory dev profile cannot reopen durable state to prove exactly-once recovery)".into(),
        );
        return Ok(asserts);
    }

    // Drop + reopen: recovery replays the durable tail EXACTLY ONCE. The aborted-in-window item ends up
    // committed exactly once (0 duplicate / 0 half-applied transitions); the accepted sibling is unaffected.
    drop(a);
    let b = make("txn2-commit");
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics after restart: {e:?}"))?;
    ensure!(
        (m.pending, m.leased, m.complete, m.failed) == (2, 0, 0, 0),
        "recovery must replay the append→apply-window command EXACTLY ONCE (accepted sibling + recovered item = 2 pending, 0 duplicates/half-applies); got pending={} leased={} complete={} failed={}",
        m.pending,
        m.leased,
        m.complete,
        m.failed
    );
    ensure!(
        b.select_eligible(&shard(), ts(100), 10)
            .await
            .map_err(|e| format!("select_eligible: {e:?}"))?
            .len()
            == 2,
        "recovery applied the replayed commit exactly once (0 duplicate state transitions)"
    );
    // Both the recovered aborted item and the accepted sibling are visible exactly once by client_item_key.
    for (label, key) in [
        ("aborted-recovered", &aborted_key),
        ("sibling", &accepted_key),
    ] {
        ensure!(
            b.live_items(&shard(), std::slice::from_ref(key))
                .await
                .map_err(|e| format!("live_items ({label}): {e:?}"))?
                .into_iter()
                .next()
                .flatten()
                .is_some(),
            "{label} item must be visible exactly once after recovery"
        );
    }
    asserts.push(
        "commit-timeout/abort recovered EXACTLY ONCE on drop+reopen (the durable-but-unapplied tail replays to a single committed item — 0 partial, 0 duplicate state transitions); the accepted sibling is unaffected".into(),
    );
    Ok(asserts)
}

/// **AC-TXN-3** unknown-outcome replay across the commit cut points (INV-5, INV-14; TP-003 §3.10 row 208).
/// Each cut point kills the commit at a different instant; the retry must resolve to exactly one committed
/// result (or a fresh execution when no original commit exists), with 0 duplicate state transitions.
///
/// Coverage is capability-gated so nothing is faked. This engine has exactly TWO `request_id`-bearing mutating
/// ops — PUSH (`push_with_request_id`) and `commit_transition` (the authoritative claimed-work commit):
/// * PUSH `request_id` exactly-once replay is proven at ALL FOUR cut points: BeforeAppend + AfterResponse
///   in-process, AfterAppendBeforeApply via the now-`request_id`-bearing mid-pipeline probe
///   ([`ac_txn_3_mid_pipeline_request_id_bearing`] — recovery rebuilds the push-idempotency map from the
///   durable-but-unapplied `request_id` envelope), and AfterApplyBeforeResponse across a full restart.
/// * `commit_transition` is covered at every reachable cut point
///   ([`ac_txn_3_commit_transition_request_id`]): in-process replay on every commit-capable projection, and
///   both restart cuts on every durable log. Eventual projection materialization does not weaken the
///   request-id contract because the whole commit and its outcome marker share one authoritative log batch.
/// * The classic ports (claim/renew/finalize/update_fields/purge/replace_if_pending) carry NO `request_id`
///   and are recorded capability-N/A (covered by AC-TXN-1 durability + AC-TXN-6 parity).
pub async fn ac_txn_3_unknown_outcome_replay<
    B: ConformanceCore + LogRead + RequestIdReplayProbe + CommitTransitionPort + RecoveryReadPort,
>(
    make: impl Fn(&str) -> B,
    caps: TxnCaps,
) -> AcOutcome {
    let mut asserts = Vec::new();

    // --- PUSH BeforeAppend (in-process) + AfterResponse in-process request_id replay: EVERY profile. ---
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
        ensure!(
            ids.is_fresh(),
            "fresh execution must report Fresh disposition"
        );
        ensure!(ids.len() == 1, "fresh execution created exactly one item");
        // AfterResponse: a duplicate retry replays the one committed result (0 duplicate transitions).
        let replay = a
            .push_with_request_id(&shard(), rid, body, ts(2), None)
            .await
            .map_err(|e| format!("after-response replay: {e:?}"))?;
        ensure!(
            replay.is_replayed() && replay.item_ids == ids.item_ids,
            "after-response retry must replay the same result"
        );
        ensure!(
            a.metrics(&qkey()).await.unwrap().pending == 1,
            "BeforeAppend + fresh + replay yields exactly one item"
        );
    }
    asserts.push(
        "PUSH BeforeAppend: no original commit -> fresh execution; PUSH AfterResponse: request_id replays exactly once".into(),
    );

    // Honest per-op request_id map (which mutating ops actually carry a request_id / idempotent key in THIS
    // engine — investigated in fireweed-engine port.rs + compose.rs, not assumed).
    asserts.push(
        "request_id-bearing mutating ops in this engine: PUSH (push_with_request_id -> durable request_id/fingerprint/outcome on the log, rebuilt on recovery) and commit_transition (the authoritative claimed-work commit -> commit_idempotency cache). PUSH is covered at ALL FOUR cut points; commit_transition per its reachable cut points below.".into(),
    );
    asserts.push(
        "capability-N/A: claim / renew / finalize (classic FinalizePort) / update_fields / purge / replace_if_pending carry NO request_id or idempotent key in this engine — their ports take no request_id and dedup is item-id / lease-token / item-version based, not request_id based. So request_id unknown-outcome replay is not an applicable contract for these ops; their kill/restart durability is covered by AC-TXN-1 (row 206 per-op kill-after-success) and their cross-backend parity by AC-TXN-6 (row 212).".into(),
    );

    // --- commit_transition request_id replay at every reachable cut point. ---
    asserts.extend(ac_txn_3_commit_transition_request_id(&make, caps).await?);

    if !caps.durable_reopen {
        asserts.push(
            "PUSH AfterAppendBeforeApply + AfterApplyBeforeResponse restart cut points capability-N/A (non-durable in-memory dev profile: cannot reopen durable state)".into(),
        );
        return Ok(asserts);
    }

    // --- PUSH AfterAppendBeforeApply: NOW request_id-bearing (the mid-pipeline seam). ---
    asserts.extend(ac_txn_3_mid_pipeline_request_id_bearing(&make).await?);

    // --- PUSH AfterApplyBeforeResponse: committed+applied, RESPONSE LOST -> request_id replay after restart.
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
                .into_item_ids()
            // The client never observes this success (the response is "lost"); we drop the handle.
        };
        // Kill + restart, then retry the same request_id (the client re-sends after the timeout).
        let b = make("txn3-lost");
        let replay = b
            .push_with_request_id(&shard(), rid, body, ts(2), None)
            .await
            .map_err(|e| format!("replay after lost response: {e:?}"))?;
        ensure!(
            replay.is_replayed() && replay.item_ids == committed_ids,
            "same request_id after a lost response must replay the ONE committed result (got {replay:?} vs {committed_ids:?})"
        );
        let m = b
            .metrics(&qkey())
            .await
            .map_err(|e| format!("metrics: {e:?}"))?;
        ensure!(
            m.pending == 1,
            "lost-response replay created a duplicate committed result (pending={})",
            m.pending
        );
        ensure!(
            b.select_eligible(&shard(), ts(100), 10)
                .await
                .map_err(|e| format!("{e:?}"))?
                .len()
                == 1,
            "lost-response replay produced a duplicate state transition"
        );
    }
    asserts.push(
        "PUSH AfterApplyBeforeResponse: request_id replays exactly one committed result across restart (0 duplicate transitions)".into(),
    );

    Ok(asserts)
}

/// **AC-TXN-3 mid-pipeline seam** (TP-003 §3.10 row 208, the `after-append-before-commit(apply)` cut): prove
/// the `AfterAppendBeforeApply` window is `request_id`-BEARING, not merely item-level. Uses
/// [`RequestIdReplayProbe::build_request_id_push_envelope`] to construct the EXACT durable envelope
/// `push_with_request_id` would append (carrying the `request_id` + body fingerprint + `RequestOutcome` +
/// minted ids), drives it through [`inject_commit`] to append durably then leave unapplied (modelling a kill
/// in the append→apply window), reopens (recovery replays the tail AND rebuilds the push-idempotency map from
/// that durable `request_id` envelope), and retries the SAME `request_id` — which must replay the ONE
/// committed result with 0 duplicate state transitions. Only meaningful on a durable, reopenable profile.
pub async fn ac_txn_3_mid_pipeline_request_id_bearing<
    B: ConformanceCore + LogRead + RequestIdReplayProbe,
>(
    make: impl Fn(&str) -> B,
) -> AcOutcome {
    let rid = RequestId::new("ac-txn-3-mid-pipeline").unwrap();
    let body = vec![spec("txn3-mid", 7)];
    let committed_ids = {
        let a = make("txn3-mid");
        a.create_queue(qdef())
            .await
            .map_err(|e| format!("create_queue: {e:?}"))?;
        // Build the EXACT durable request_id-bearing push envelope push_with_request_id would append.
        let (env, ids) = a
            .build_request_id_push_envelope(&shard(), rid.clone(), body.clone(), ts(1), None)
            .map_err(|e| format!("build_request_id_push_envelope: {e:?}"))?;
        ensure!(
            env.request_id.as_ref() == Some(&rid) && env.request_fingerprint.is_some(),
            "the mid-pipeline envelope must carry the request_id AND the body fingerprint (else the cut is not request_id-bearing)"
        );
        // Drive it through the append→apply seam, killing BEFORE apply (the append→apply window).
        let pos = inject_commit(&a, env, CutPoint::AfterAppendBeforeApply)
            .await
            .map_err(|e| format!("AfterAppendBeforeApply inject: {e:?}"))?;
        ensure!(!pos.is_empty(), "append returned a durable position");
        ensure!(
            durable_command_count(&a).await? == 1,
            "the request_id-bearing command must be durable on the log after the commit->apply kill"
        );
        // Confirm the DURABLE-but-unapplied log entry actually carries the request_id (this is the crux: the
        // mid-pipeline cut is now request_id-bearing, not the old raw item-level envelope).
        let entries = a
            .read_from(&shard(), None, 10)
            .await
            .map_err(|e| format!("read_from: {e:?}"))?
            .entries;
        ensure!(
            entries.len() == 1 && entries[0].1.request_id.as_ref() == Some(&rid),
            "the durable-but-unapplied mid-pipeline command must carry the request_id (proves the append→apply cut is request_id-bearing)"
        );
        ensure!(
            a.metrics(&qkey()).await.unwrap().pending == 0,
            "apply was skipped, so the in-process projection has not applied the command"
        );
        ids
    };
    // Reopen: recovery replays the durable tail AND rebuilds the request_id -> result map from that envelope.
    let b = make("txn3-mid");
    ensure!(
        b.metrics(&qkey())
            .await
            .map_err(|e| format!("metrics: {e:?}"))?
            .pending
            == 1,
        "committed-but-unapplied request_id command must replay exactly once on recovery"
    );
    // Retry the SAME request_id with the SAME body -> replays the ONE committed result (0 duplicate).
    let replay = b
        .push_with_request_id(&shard(), rid, body, ts(2), None)
        .await
        .map_err(|e| format!("mid-pipeline request_id replay after reopen: {e:?}"))?;
    ensure!(
        replay.item_ids == committed_ids,
        "the append->apply kill-window retry by request_id must replay the ONE committed result (got {replay:?} vs {committed_ids:?})"
    );
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        m.pending == 1,
        "mid-pipeline request_id replay created a duplicate committed result (pending={})",
        m.pending
    );
    ensure!(
        b.select_eligible(&shard(), ts(100), 10)
            .await
            .map_err(|e| format!("{e:?}"))?
            .len()
            == 1,
        "mid-pipeline request_id replay produced a duplicate state transition"
    );
    Ok(vec![
        "PUSH AfterAppendBeforeApply (request_id-bearing): a kill in the append->apply window leaves the request_id-bearing push durable-but-unapplied (the durable log entry carries the request_id, verified); on reopen recovery replays it exactly once AND rebuilds the request_id->result map from that durable envelope, so a retry by request_id replays the ONE committed result (0 duplicate state transitions)".into(),
    ])
}

/// Seed a fresh durable backend for the MIXED commit_transition probes (bead pqueue-db60657d): create the
/// queue, push two items, claim both, then REASSIGN the second item's lease to a new consumer so its cached
/// [`ClaimRef`] (holding the OLD token) rejects with [`EngineError::StaleLease`] at commit — while the first
/// item's `ClaimRef` stays valid → Committed. Returns `(backend, valid_claim_ref, stale_claim_ref)`.
async fn seed_mixed_commit<B: ConformanceCore, F: Fn(&str) -> B>(
    make: &F,
    tag: &'static str,
) -> Result<(B, ClaimRef, ClaimRef), String> {
    let a = make(tag);
    a.create_queue(qdef())
        .await
        .map_err(|e| format!("mixed create_queue: {e:?}"))?;
    a.push(
        &shard(),
        vec![spec("txn3-ct-mixed-a", 5), spec("txn3-ct-mixed-b", 5)],
        ts(0),
        None,
    )
    .await
    .map_err(|e| format!("mixed seed push: {e:?}"))?;
    let claimed = a
        .claim(claim_req(2, 500, 1))
        .await
        .map_err(|e| format!("mixed claim: {e:?}"))?;
    ensure!(claimed.items.len() == 2, "mixed claim leased two items");
    let cref = |i: usize| -> Result<ClaimRef, String> {
        let ci = &claimed.items[i];
        Ok(ClaimRef {
            item_id: ci.item_id,
            lease_token: ci
                .lease_token
                .clone()
                .ok_or_else(|| "mixed claimed item is missing its lease token".to_string())?,
            lease_expires_at: ci.lease_expires_at,
            item_version: ci.item_version,
        })
    };
    let claim_ref_valid = cref(0)?;
    let claim_ref_stale = cref(1)?;
    // Reassign the SECOND item's lease to a NEW consumer: the item stays Leased but its token changes, so
    // `claim_ref_stale` (old token) → StaleLease at commit (a genuine structured rejection, not terminal/absent).
    a.reassign(
        &shard(),
        vec![claim_ref_stale.item_id],
        LeaseToken::new("mixed-reassigned").unwrap(),
        ts(500),
        ts(2),
        None,
    )
    .await
    .map_err(|e| format!("mixed reassign (make claim_ref_stale StaleLease): {e:?}"))?;
    Ok((a, claim_ref_valid, claim_ref_stale))
}

/// **AC-TXN-3 commit_transition** (TP-003 §3.10 row 208, the OTHER request_id-bearing mutating op). The
/// authoritative claimed-work commit (`commit_transition`) carries a `request_id` idempotent over the whole
/// commit body (`commit_idempotency` cache). The atomic boundary is the authoritative log batch, not
/// synchronous projection persistence: eventual-apply projections recover from that exact batch.
/// * Every commit-capable backend proves IN-PROCESS request_id replay (BeforeAppend-fresh + AfterResponse) —
///   a same-body retry replays the ONE committed per-entry outcome, a different body conflicts, and the input
///   is finalized exactly once.
/// * Every durable log backend ALSO proves the cross-restart cut points for an ALL-COMMITTED commit now that
///   recovery rebuilds `commit_idempotency` from the durable log (`rebuild_commit_idempotency_from_log`, the
///   symmetric twin of the push rebuild): `AfterApplyBeforeResponse` (commit fully, kill, reopen) and
///   `AfterAppendBeforeApply` (append the request_id-bearing commit envelope, kill before apply, reopen) both
///   replay the exact per-entry outcome across restart — a same-body retry replays it, a different body →
///   `RequestIdConflict`, and the input is finalized exactly once (0 duplicate transitions).
/// * MIXED committed+rejected commit (bead pqueue-db60657d, closed): a commit whose entries are
///   `[valid claim → Committed, stale claim → Rejected(StaleLease)]` is now replayed BYTE-IDENTICALLY across
///   restart at BOTH cut points. `commit_transition` stamps the whole per-entry vec (committed AND rejected,
///   each rejection's structured error projected via `CommitRejection`) onto a terminal
///   `RequestOutcome::CommitTransition` marker; recovery rebuilds the full `Vec<EntryRecovery>` from that
///   durable marker, so the retry replays the exact `[Committed, Rejected(StaleLease)]` (not a short/stale
///   vec, not an all-Rejected re-execution) and `explain_commit` returns the identical full vec — with the
///   committed input finalized exactly once (0 duplicate).
pub async fn ac_txn_3_commit_transition_request_id<
    B: ConformanceCore + CommitTransitionPort + LogRead + RequestIdReplayProbe + RecoveryReadPort,
>(
    make: impl Fn(&str) -> B,
    caps: TxnCaps,
) -> AcOutcome {
    let rid = RequestId::new("ac-txn-3-commit-transition").unwrap();
    // Seed a result + await pair and claim both so one entry must validate/finalize the pair atomically.
    let a = make("txn3-ct");
    a.create_queue(qdef())
        .await
        .map_err(|e| format!("create_queue: {e:?}"))?;
    a.push(
        &shard(),
        vec![spec("txn3-ct-result", 5), spec("txn3-ct-await", 6)],
        ts(0),
        None,
    )
    .await
    .map_err(|e| format!("seed push: {e:?}"))?;
    let claimed = a
        .claim(claim_req(2, 500, 1))
        .await
        .map_err(|e| format!("claim: {e:?}"))?;
    ensure!(claimed.items.len() == 2, "claim leased result + await");
    let to_ref = |ci: &ClaimedItem| -> Result<ClaimRef, String> {
        Ok(ClaimRef {
            item_id: ci.item_id,
            lease_token: ci
                .lease_token
                .clone()
                .ok_or_else(|| "claimed item is missing its lease token".to_string())?,
            lease_expires_at: ci.lease_expires_at,
            item_version: ci.item_version,
        })
    };
    let claim_ref = to_ref(&claimed.items[0])?;
    let additional_claim_ref = to_ref(&claimed.items[1])?;
    let transition = |finalize: FinalizeKind| CommitTransition {
        request_id: Some(rid.clone()),
        entries: vec![CommitTransitionEntry {
            claim_ref: claim_ref.clone(),
            additional_claim_refs: vec![additional_claim_ref.clone()],
            finalize,
            side_records: Vec::new(),
            lifecycle_items: Vec::new(),
            instance_fence: None,
        }],
    };

    // First commit under the request_id. The whole command/outcome batch commits on the authoritative log;
    // projection persistence may be synchronous or eventual without changing this request-id boundary.
    let committed = a
        .commit_transition(&shard(), transition(FinalizeKind::Complete), ts(2), None)
        .await
        .map_err(|e| format!("commit_transition first call: {e:?}"))?;
    ensure!(
        matches!(committed.as_slice(), [CommitEntryOutcome::Committed { .. }]),
        "commit_transition must commit the entry; got {committed:?}"
    );

    // In-process replay (AfterResponse): same request_id + same body replays the ONE committed outcome.
    let replay = a
        .commit_transition(&shard(), transition(FinalizeKind::Complete), ts(3), None)
        .await
        .map_err(|e| format!("commit_transition in-proc replay: {e:?}"))?;
    ensure!(
        replay == committed,
        "in-process commit_transition request_id replay must return the ONE committed outcome (got {replay:?} vs {committed:?})"
    );
    // A different body under the same request_id conflicts (checked before re-execution).
    ensure!(
        matches!(
            a.commit_transition(&shard(), transition(FinalizeKind::Fail), ts(4), None)
                .await,
            Err(EngineError::RequestIdConflict)
        ),
        "a different commit body under the same request_id must be RequestIdConflict"
    );
    // The input finalized exactly once (0 duplicate transitions).
    let m = a
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics: {e:?}"))?;
    ensure!(
        m.complete == 2 && m.leased == 0,
        "commit_transition + replay must finalize both inputs exactly once; got complete={} leased={}",
        m.complete,
        m.leased
    );
    let mut asserts = vec![
        "commit_transition (the authoritative request_id-bearing claimed-work commit) IN-PROCESS request_id replay proven (BeforeAppend-fresh + AfterResponse cuts): same request_id+body replays the ONE committed per-entry outcome, a different body -> RequestIdConflict, and the input is finalized exactly once (0 duplicate transitions)".into(),
    ];

    if !caps.durable_reopen {
        asserts.push(
            "commit_transition restart cut points (AfterAppendBeforeApply / AfterApplyBeforeResponse) capability-N/A on this non-durable in-memory dev profile (cannot reopen durable state)".into(),
        );
        return Ok(asserts);
    }

    // ---- Cut point AfterApplyBeforeResponse: the commit fully committed+applied in-process (above), the
    // response was "lost", the process is killed, and a fresh backend reopens the SAME durable state. Recovery
    // rebuilds `commit_idempotency` from the durable log (rebuild_commit_idempotency_from_log), so retrying the
    // same request_id replays the ONE committed per-entry outcome — a same-body retry Replays, a different
    // body -> RequestIdConflict, and the input stays finalized EXACTLY ONCE (0 duplicate transitions).
    drop(a);
    let b = make("txn3-ct");
    let after_restart = b
        .commit_transition(&shard(), transition(FinalizeKind::Complete), ts(5), None)
        .await
        .map_err(|e| format!("commit_transition after restart: {e:?}"))?;
    ensure!(
        after_restart == committed,
        "AfterApplyBeforeResponse: same request_id+body after kill+restart must replay the ONE committed result (got {after_restart:?} vs {committed:?})"
    );
    ensure!(
        matches!(
            b.commit_transition(&shard(), transition(FinalizeKind::Fail), ts(6), None)
                .await,
            Err(EngineError::RequestIdConflict)
        ),
        "AfterApplyBeforeResponse: a different commit body under the same request_id must be RequestIdConflict after restart (proves the fingerprint was rebuilt from the durable log)"
    );
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("metrics after restart: {e:?}"))?;
    ensure!(
        m.complete == 2 && m.leased == 0,
        "after restart both inputs must remain finalized exactly once (0 duplicate); got complete={} leased={}",
        m.complete,
        m.leased
    );
    let recovery = b
        .explain_commit(&shard(), rid.clone())
        .await
        .map_err(|e| format!("explain multi-claim commit after restart: {e:?}"))?
        .ok_or_else(|| "multi-claim recovery missing after restart".to_string())?;
    ensure!(
        recovery.entries[0].additional_consumed_input_ids == vec![additional_claim_ref.item_id],
        "AfterApplyBeforeResponse recovery must retain the additional finalized claim"
    );
    drop(b);
    asserts.push(
        "commit_transition AfterApplyBeforeResponse across-restart request_id replay PROVEN: a kill after a fully-committed+applied commit, then reopen, replays the ONE committed per-entry outcome (same body -> Replay, different body -> RequestIdConflict) because recovery rebuilds commit_idempotency from the durable log (rebuild_commit_idempotency_from_log); the input stays finalized exactly once (0 duplicate transitions)".into(),
    );

    // ---- Cut point AfterAppendBeforeApply: strike the mid-pipeline append->apply window of the OTHER
    // request_id-bearing op. `commit_transition`'s public call is atomic (append+apply under one lock), so —
    // exactly like the PUSH mid-pipeline probe — we use `build_request_id_commit_envelope` to construct the
    // EXACT durable request_id-bearing FINALIZE envelope a single-entry commit would append (same request_id +
    // whole-body fingerprint), drive it through `Backend::commit_raw` with a kill BEFORE apply, then reopen: recovery
    // replays the durable-but-unapplied commit AND rebuilds commit_idempotency from it, so a retry by
    // request_id replays the ONE committed outcome (0 duplicate transitions).
    let rid_mid = RequestId::new("ac-txn-3-commit-transition-mid").unwrap();
    let a = make("txn3-ct-mid");
    a.create_queue(qdef())
        .await
        .map_err(|e| format!("mid create_queue: {e:?}"))?;
    a.push(
        &shard(),
        vec![spec("txn3-ct-mid-result", 5), spec("txn3-ct-mid-await", 6)],
        ts(0),
        None,
    )
    .await
    .map_err(|e| format!("mid seed push: {e:?}"))?;
    let claimed = a
        .claim(claim_req(2, 500, 1))
        .await
        .map_err(|e| format!("mid claim: {e:?}"))?;
    ensure!(claimed.items.len() == 2, "mid claim leased result + await");
    let to_mid_ref = |ci: &ClaimedItem| -> Result<ClaimRef, String> {
        Ok(ClaimRef {
            item_id: ci.item_id,
            lease_token: ci
                .lease_token
                .clone()
                .ok_or_else(|| "mid claimed item is missing its lease token".to_string())?,
            lease_expires_at: ci.lease_expires_at,
            item_version: ci.item_version,
        })
    };
    let claim_ref_mid = to_mid_ref(&claimed.items[0])?;
    let additional_claim_ref_mid = to_mid_ref(&claimed.items[1])?;
    let mid_transition = |finalize: FinalizeKind| CommitTransition {
        request_id: Some(rid_mid.clone()),
        entries: vec![CommitTransitionEntry {
            claim_ref: claim_ref_mid.clone(),
            additional_claim_refs: vec![additional_claim_ref_mid.clone()],
            finalize,
            side_records: Vec::new(),
            lifecycle_items: Vec::new(),
            instance_fence: None,
        }],
    };
    // Build the EXACT durable envelopes commit_transition would append: finalize batch +
    // request_id CommitTransition marker (marker carries the replay outcome for recovery).
    let (envelopes, _fp) = a
        .build_request_id_commit_envelopes(
            &shard(),
            rid_mid.clone(),
            mid_transition(FinalizeKind::Complete).entries,
            ts(2),
            None,
        )
        .map_err(|e| format!("build_request_id_commit_envelopes: {e:?}"))?;
    ensure!(
        envelopes.len() >= 2,
        "finalize-only entry must build finalize envelope(s) plus a CommitTransition marker"
    );
    ensure!(
        envelopes.iter().any(|env| {
            env.request_id.as_ref() == Some(&rid_mid)
                && env.request_fingerprint.is_some()
                && matches!(
                    env.request_outcome,
                    Some(fireweed_engine::RequestOutcome::CommitTransition { .. })
                )
        }),
        "the mid-pipeline batch must carry a request_id CommitTransition marker (else recovery cannot rebuild commit_idempotency)"
    );
    let before = durable_command_count(&a).await?;
    let epoch = a
        .current_epoch(&shard())
        .await
        .map_err(|e| format!("mid epoch: {e:?}"))?;
    let pos = a
        .commit_raw(
            RawCommitRequest::new(shard(), envelopes, epoch)
                .with_fault(RawCommitFault::AfterAppendBeforeApply),
        )
        .await
        .map_err(|e| format!("mid AfterAppendBeforeApply inject: {e:?}"))?
        .into_positions();
    ensure!(
        !pos.is_empty(),
        "the request_id-bearing commit batch must be durable on the log after the append->apply kill"
    );
    // Confirm the DURABLE-but-unapplied batch includes the request_id marker.
    let after = durable_command_count(&a).await?;
    ensure!(
        after > before,
        "the mid-pipeline commit append must add durable commands (before={before} after={after})"
    );
    drop(a);
    // Reopen: recovery replays the durable tail (finalizing the input) AND rebuilds commit_idempotency from the
    // durable-but-unapplied request_id-bearing commit envelope.
    let b = make("txn3-ct-mid");
    let replay = b
        .commit_transition(
            &shard(),
            mid_transition(FinalizeKind::Complete),
            ts(3),
            None,
        )
        .await
        .map_err(|e| format!("mid request_id replay after reopen: {e:?}"))?;
    ensure!(
        matches!(replay.as_slice(), [CommitEntryOutcome::Committed { .. }]),
        "AfterAppendBeforeApply: the append->apply kill-window retry by request_id must replay the ONE committed outcome (got {replay:?})"
    );
    ensure!(
        matches!(
            b.commit_transition(&shard(), mid_transition(FinalizeKind::Fail), ts(4), None)
                .await,
            Err(EngineError::RequestIdConflict)
        ),
        "AfterAppendBeforeApply: a different commit body under the same request_id must be RequestIdConflict after restart"
    );
    let m = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("mid metrics after restart: {e:?}"))?;
    ensure!(
        m.complete == 2 && m.leased == 0,
        "AfterAppendBeforeApply: both inputs must be finalized exactly once across the mid-pipeline kill (0 duplicate); got complete={} leased={}",
        m.complete,
        m.leased
    );
    let recovery = b
        .explain_commit(&shard(), rid_mid.clone())
        .await
        .map_err(|e| format!("mid explain multi-claim commit after restart: {e:?}"))?
        .ok_or_else(|| "mid multi-claim recovery missing after restart".to_string())?;
    ensure!(
        recovery.entries[0].additional_consumed_input_ids == vec![additional_claim_ref_mid.item_id],
        "AfterAppendBeforeApply recovery must retain the additional finalized claim"
    );
    asserts.push(
        "commit_transition AfterAppendBeforeApply (request_id-bearing) across-restart request_id replay PROVEN: a kill in the append->apply window leaves the request_id-bearing commit durable-but-unapplied; on reopen recovery replays it exactly once AND rebuilds commit_idempotency from that durable envelope, so a retry by request_id replays the ONE committed per-entry outcome (same body -> Replay, different body -> RequestIdConflict, 0 duplicate state transitions)".into(),
    );

    // ---- MIXED committed+rejected commit across restart (bead pqueue-db60657d, faithful replay). A
    // vectorized commit whose entries are [valid claim → Committed, stale claim → Rejected(StaleLease)] records
    // a mixed per-entry vec. A rejected entry mutates and appends nothing of its own, so to reconstruct the
    // WHOLE vec on recovery `commit_transition` stamps it (committed AND rejected, each rejection's structured
    // error projected durably) onto a terminal `RequestOutcome::CommitTransition` marker. This probe proves the
    // retry replays that vec BYTE-IDENTICALLY across a real drop+reopen at BOTH cut points, `explain_commit`
    // returns the identical full vec, and the committed input stays finalized exactly once (0 duplicate).
    //
    // A MIXED commit body: entry 0 (valid) commits, entry 1 (stale token) is rejected with StaleLease.
    let mixed_entries = |valid: &ClaimRef, stale: &ClaimRef, finalize0: FinalizeKind| {
        vec![
            CommitTransitionEntry {
                claim_ref: valid.clone(),
                additional_claim_refs: Vec::new(),
                finalize: finalize0,
                side_records: Vec::new(),
                lifecycle_items: Vec::new(),
                instance_fence: None,
            },
            CommitTransitionEntry {
                claim_ref: stale.clone(),
                additional_claim_refs: Vec::new(),
                finalize: FinalizeKind::Complete,
                side_records: Vec::new(),
                lifecycle_items: Vec::new(),
                instance_fence: None,
            },
        ]
    };
    // The expected mixed outcome shape, shared by both cut points: entry 0 Committed (no lifecycle items),
    // entry 1 Rejected with the exact structured StaleLease error.
    let is_mixed_stale = |o: &[CommitEntryOutcome]| -> bool {
        matches!(
            o,
            [
                CommitEntryOutcome::Committed { lifecycle_item_ids },
                CommitEntryOutcome::Rejected(EngineError::StaleLease),
            ] if lifecycle_item_ids.is_empty()
        )
    };
    // The expected reconstructed recovery vec (what `explain_commit` returns): [Committed, Rejected(StaleLease)].
    let is_mixed_recovery = |rec: &CommitRecovery| -> bool {
        matches!(
            rec.entries.as_slice(),
            [
                EntryRecovery {
                    status: CommitEntryStatus::Committed,
                    lifecycle_item_ids: l,
                    side_record_keys: s,
                    instance: None,
                    ..
                },
                EntryRecovery {
                    status: CommitEntryStatus::Rejected(EngineError::StaleLease),
                    ..
                },
            ] if l.is_empty() && s.is_empty()
        )
    };
    // The exact expected per-entry outcome vec. No server-minted id varies (a Committed finalize-only entry
    // carries no lifecycle items; the Rejected entry carries the structured StaleLease), so this is a
    // fully-determined, backend-independent BYTE-IDENTICAL target for `retry == expected_mixed`.
    let expected_mixed = vec![
        CommitEntryOutcome::Committed {
            lifecycle_item_ids: Vec::new(),
        },
        CommitEntryOutcome::Rejected(EngineError::StaleLease),
    ];

    // ==== Cut point AfterApplyBeforeResponse (mixed): full commit in-process, drop, reopen, retry. ====
    let rid_mixed = RequestId::new("ac-txn-3-commit-transition-mixed").unwrap();
    let (a, valid, stale) = seed_mixed_commit(&make, "txn3-ct-mixed").await?;
    let mixed_body = |finalize0: FinalizeKind| CommitTransition {
        request_id: Some(rid_mixed.clone()),
        entries: mixed_entries(&valid, &stale, finalize0),
    };
    let live = a
        .commit_transition(&shard(), mixed_body(FinalizeKind::Complete), ts(3), None)
        .await
        .map_err(|e| format!("mixed commit: {e:?}"))?;
    ensure!(
        is_mixed_stale(&live) && live == expected_mixed,
        "the mixed commit must live-record [Committed, Rejected(StaleLease)]; got {live:?}"
    );
    // The full per-entry recovery vec (with the structured StaleLease) BEFORE restart, via explain_commit.
    let explain_before = a
        .explain_commit(&shard(), rid_mixed.clone())
        .await
        .map_err(|e| format!("mixed explain_commit before restart: {e:?}"))?
        .ok_or_else(|| "mixed explain_commit returned None before restart".to_string())?;
    ensure!(
        is_mixed_recovery(&explain_before),
        "explain_commit before restart must return [Committed, Rejected(StaleLease)]; got {explain_before:?}"
    );
    let m_before = a
        .metrics(&qkey())
        .await
        .map_err(|e| format!("mixed metrics before restart: {e:?}"))?;
    ensure!(
        m_before.complete == 1 && m_before.leased == 1,
        "mixed in-process: the valid input finalized exactly once, the stale input stays leased; got complete={} leased={}",
        m_before.complete,
        m_before.leased
    );
    drop(a);
    // Reopen: recovery rebuilds commit_idempotency SOLELY from the durable log (the in-memory cache is gone),
    // reconstructing the FULL [Committed, Rejected(StaleLease)] vec from the durable CommitTransition marker.
    let b = make("txn3-ct-mixed");
    let retry = b
        .commit_transition(&shard(), mixed_body(FinalizeKind::Complete), ts(4), None)
        .await
        .map_err(|e| format!("mixed retry after restart: {e:?}"))?;
    ensure!(
        retry == live,
        "AfterApplyBeforeResponse: the mixed retry across restart must replay the BYTE-IDENTICAL original vec (incl Rejected(StaleLease)); got {retry:?} vs {live:?}"
    );
    let explain_after = b
        .explain_commit(&shard(), rid_mixed.clone())
        .await
        .map_err(|e| format!("mixed explain_commit after restart: {e:?}"))?
        .ok_or_else(|| "mixed explain_commit returned None after restart".to_string())?;
    ensure!(
        explain_after == explain_before,
        "AfterApplyBeforeResponse: explain_commit after restart must return the identical full vec; got {explain_after:?} vs {explain_before:?}"
    );
    // A different body under the same request_id still conflicts after restart (fingerprint rebuilt from log).
    ensure!(
        matches!(
            b.commit_transition(&shard(), mixed_body(FinalizeKind::Fail), ts(5), None)
                .await,
            Err(EngineError::RequestIdConflict)
        ),
        "AfterApplyBeforeResponse: a different mixed body under the same request_id must be RequestIdConflict after restart"
    );
    let m_after = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("mixed metrics after restart: {e:?}"))?;
    ensure!(
        m_after.complete == 1 && m_after.leased == 1,
        "AfterApplyBeforeResponse: the committed input must stay finalized EXACTLY ONCE (0 duplicate); got complete={} leased={}",
        m_after.complete,
        m_after.leased
    );
    drop(b);
    asserts.push(
        "commit_transition MIXED committed+rejected AfterApplyBeforeResponse across-restart replay PROVEN (bead pqueue-db60657d): a [valid→Committed, stale→Rejected(StaleLease)] commit, fully committed then killed + reopened, replays the BYTE-IDENTICAL per-entry vec (Rejected carrying the same structured StaleLease) because recovery rebuilds commit_idempotency from the durable CommitTransition marker; explain_commit returns the identical full vec, a different body → RequestIdConflict, and the committed input stays finalized exactly once (0 duplicate)".into(),
    );

    // ==== Cut point AfterAppendBeforeApply (mixed): reproduce the REAL production write ordering. The
    // production `commit_transition` appends the WHOLE commit — the committed entry's Finalize AND the
    // CommitTransition outcome marker — as ONE atomic log batch (no crash window between committed-entry
    // durability and outcome durability). This probe builds that exact batch via
    // `build_request_id_commit_envelopes` and drives it through `Backend::commit_raw`, appending the whole batch
    // durably then SKIPPING apply — striking the append→apply crash window on the atomic commit unit. On
    // drop+reopen, recovery replays the durable tail (finalizing the valid input) AND rebuilds the full
    // [Committed, Rejected(StaleLease)] vec from the durable marker, so the retry replays it byte-identically. ====
    let rid_mixed_mid = RequestId::new("ac-txn-3-commit-transition-mixed-mid").unwrap();
    let (a, valid, stale) = seed_mixed_commit(&make, "txn3-ct-mixed-mid").await?;
    let mid_entries = mixed_entries(&valid, &stale, FinalizeKind::Complete);
    let (envs, _fp) = a
        .build_request_id_commit_envelopes(
            &shard(),
            rid_mixed_mid.clone(),
            mid_entries,
            ts(3),
            None,
        )
        .map_err(|e| format!("build_request_id_commit_envelopes: {e:?}"))?;
    ensure!(
        envs.len() == 2
            && matches!(
                envs[1].request_outcome,
                Some(fireweed_engine::RequestOutcome::CommitTransition { .. })
            ),
        "the mixed durable footprint must be [Finalize(valid), CommitTransition marker]; got {envs:?}"
    );
    let before = durable_command_count(&a).await?;
    // Append ALL the mixed commit's envelopes durably, then SKIP apply (the append→apply kill window).
    let epoch = a
        .current_epoch(&shard())
        .await
        .map_err(|e| format!("current_epoch: {e:?}"))?;
    let pos = a
        .commit_raw(
            RawCommitRequest::new(shard(), envs.clone(), epoch)
                .with_fault(RawCommitFault::AfterAppendBeforeApply),
        )
        .await
        .map(|outcome| outcome.into_positions())
        .map_err(|e| format!("mixed AfterAppendBeforeApply append (skip apply): {e:?}"))?;
    ensure!(!pos.is_empty(), "the mixed commit envelopes are durable");
    let after = durable_command_count(&a).await?;
    ensure!(
        after == before + envs.len(),
        "the mixed mid-pipeline append must add exactly {} durable commands (before={before} after={after})",
        envs.len()
    );
    // The apply was skipped, so the valid input is NOT yet finalized in-process (both items still leased).
    ensure!(
        a.metrics(&qkey()).await.unwrap().leased == 2,
        "apply was skipped, so the in-process projection has finalized neither input"
    );
    drop(a);
    // Reopen: recovery replays the durable-but-unapplied tail (finalizing the valid input) AND rebuilds the
    // full [Committed, Rejected(StaleLease)] vec from the durable marker.
    let b = make("txn3-ct-mixed-mid");
    let retry = b
        .commit_transition(
            &shard(),
            CommitTransition {
                request_id: Some(rid_mixed_mid.clone()),
                entries: mixed_entries(&valid, &stale, FinalizeKind::Complete),
            },
            ts(4),
            None,
        )
        .await
        .map_err(|e| format!("mixed mid retry after reopen: {e:?}"))?;
    ensure!(
        retry == expected_mixed,
        "AfterAppendBeforeApply: the mixed retry across restart must replay the BYTE-IDENTICAL [Committed, Rejected(StaleLease)] vec; got {retry:?} vs {expected_mixed:?}"
    );
    let explain_mid = b
        .explain_commit(&shard(), rid_mixed_mid.clone())
        .await
        .map_err(|e| format!("mixed mid explain_commit after reopen: {e:?}"))?
        .ok_or_else(|| "mixed mid explain_commit returned None after reopen".to_string())?;
    ensure!(
        is_mixed_recovery(&explain_mid),
        "AfterAppendBeforeApply: explain_commit after reopen must return [Committed, Rejected(StaleLease)]; got {explain_mid:?}"
    );
    let m_mid = b
        .metrics(&qkey())
        .await
        .map_err(|e| format!("mixed mid metrics after reopen: {e:?}"))?;
    ensure!(
        m_mid.complete == 1 && m_mid.leased == 1,
        "AfterAppendBeforeApply: the committed input must be finalized EXACTLY ONCE across the mid-pipeline kill (0 duplicate); got complete={} leased={}",
        m_mid.complete,
        m_mid.leased
    );
    asserts.push(
        "commit_transition MIXED committed+rejected AfterAppendBeforeApply across-restart replay PROVEN (bead pqueue-db60657d): the mixed commit's durable envelopes (the committed entry's Finalize + the CommitTransition marker) appended durably-but-unapplied, then reopened, replay the BYTE-IDENTICAL [Committed, Rejected(StaleLease)] per-entry vec (recovery replays the durable tail AND rebuilds commit_idempotency from the durable marker); explain_commit returns the identical full vec and the committed input is finalized exactly once (0 duplicate)".into(),
    );

    // ==== ALL-REJECTED commit across restart (bead pqueue-db60657d Problem 2): EVERY entry rejects, and the
    // rejection is TIME-DEPENDENT. The one entry rejects with a version `Conflict` while the lease is still
    // valid; bare re-execution AFTER the lease expires would instead reject `StaleLease` (a DIFFERENT structured
    // error — commit_validate checks lease expiry before the version fence). An all-rejected commit records a
    // durable CommitTransition marker too, so the retry replays the ORIGINAL `Conflict` byte-identically rather
    // than the time-dependent `StaleLease`. Proves the marker is genuinely load-bearing for all-rejected. ====
    let rid_allrej = RequestId::new("ac-txn-3-commit-transition-all-rejected").unwrap();
    let a = make("txn3-ct-allrej");
    a.create_queue(qdef())
        .await
        .map_err(|e| format!("all-rejected create_queue: {e:?}"))?;
    a.push(&shard(), vec![spec("txn3-ct-allrej-a", 5)], ts(0), None)
        .await
        .map_err(|e| format!("all-rejected seed push: {e:?}"))?;
    let claimed = a
        .claim(claim_req(1, 500, 1))
        .await
        .map_err(|e| format!("all-rejected claim: {e:?}"))?;
    ensure!(
        claimed.items.len() == 1,
        "all-rejected claim leased one item"
    );
    let ci = &claimed.items[0];
    let claim_ref_v0 = ClaimRef {
        item_id: ci.item_id,
        lease_token: ci
            .lease_token
            .clone()
            .ok_or_else(|| "all-rejected claimed item missing lease token".to_string())?,
        lease_expires_at: ci.lease_expires_at,
        item_version: ci.item_version,
    };
    // Bump the item's version (keeps the SAME lease + token), so the cached `claim_ref_v0` now holds a STALE
    // version → commit_validate returns Conflict (while the lease is still valid).
    a.update_fields(
        &shard(),
        claim_ref_v0.item_id,
        BTreeMap::from([("bump".to_string(), Some(Bytes::from_static(b"1")))]),
        PayloadUpdate::Keep,
        None,
        None,
        ts(1),
        None,
    )
    .await
    .map_err(|e| format!("all-rejected update_fields (bump version): {e:?}"))?;
    let allrej_body = || CommitTransition {
        request_id: Some(rid_allrej.clone()),
        entries: vec![CommitTransitionEntry {
            claim_ref: claim_ref_v0.clone(),
            additional_claim_refs: Vec::new(),
            finalize: FinalizeKind::Complete,
            side_records: Vec::new(),
            lifecycle_items: Vec::new(),
            instance_fence: None,
        }],
    };
    // Commit while the lease is still VALID (ts(490) < expiry 500): the stale version → Conflict. The commit
    // time is chosen so the request_id retention window (60s → expires ts(550)) OUTLASTS the lease expiry
    // (ts(500)), leaving a window in which the lease is expired but the idempotency record is still live.
    let live = a
        .commit_transition(&shard(), allrej_body(), ts(490), None)
        .await
        .map_err(|e| format!("all-rejected commit: {e:?}"))?;
    let expected_allrej = vec![CommitEntryOutcome::Rejected(EngineError::Conflict)];
    ensure!(
        live == expected_allrej,
        "the all-rejected commit must live-record [Rejected(Conflict)] (stale version while lease valid); got {live:?}"
    );
    let explain_allrej_before = a
        .explain_commit(&shard(), rid_allrej.clone())
        .await
        .map_err(|e| format!("all-rejected explain_commit before restart: {e:?}"))?
        .ok_or_else(|| "all-rejected explain_commit returned None before restart".to_string())?;
    ensure!(
        matches!(
            explain_allrej_before.entries.as_slice(),
            [EntryRecovery {
                status: CommitEntryStatus::Rejected(EngineError::Conflict),
                ..
            }]
        ),
        "explain_commit before restart must return [Rejected(Conflict)]; got {explain_allrej_before:?}"
    );
    drop(a);
    // Reopen and retry AFTER the lease has EXPIRED (ts(520) > expiry 500) but WHILE the request_id record is
    // still live (ts(520) < retention expiry ts(550)). Bare re-execution would now reject StaleLease; the
    // durable marker replays the ORIGINAL Conflict byte-identically.
    let b = make("txn3-ct-allrej");
    let retry = b
        .commit_transition(&shard(), allrej_body(), ts(520), None)
        .await
        .map_err(|e| format!("all-rejected retry after restart: {e:?}"))?;
    ensure!(
        retry == live && retry == expected_allrej,
        "ALL-REJECTED across-restart retry must replay the BYTE-IDENTICAL [Rejected(Conflict)] (NOT the time-dependent StaleLease bare re-execution past the lease expiry would give); got {retry:?} vs {live:?}"
    );
    let explain_allrej_after = b
        .explain_commit(&shard(), rid_allrej.clone())
        .await
        .map_err(|e| format!("all-rejected explain_commit after restart: {e:?}"))?
        .ok_or_else(|| "all-rejected explain_commit returned None after restart".to_string())?;
    ensure!(
        explain_allrej_after == explain_allrej_before,
        "all-rejected explain_commit after restart must return the identical [Rejected(Conflict)] vec; got {explain_allrej_after:?} vs {explain_allrej_before:?}"
    );
    asserts.push(
        "commit_transition ALL-REJECTED across-restart replay PROVEN (bead pqueue-db60657d Problem 2): a commit whose entry rejects with a TIME-DEPENDENT version Conflict (stale item_version while the lease is valid) records a durable CommitTransition marker; after kill+reopen a retry PAST the lease expiry replays the BYTE-IDENTICAL [Rejected(Conflict)] — not the StaleLease that bare re-execution would produce once the lease expired — and explain_commit returns the identical vec".into(),
    );
    Ok(asserts)
}

/// The backend-independent post-restart observable state compared across two AC-TXN-6 profile combinations.
struct ParityState {
    metrics: fireweed_engine::QueueMetrics,
    eligible: Vec<String>,
    pending: Vec<String>,
    /// Per-item terminal-outcome records reconstructed from the DURABLE command log (explicit ids only, so
    /// backend-independent): `["2:Complete", "3:Fail"]`. Proves both durable logs recorded the same terminal
    /// disposition per item after the failure schedule + restart, not merely the same complete/failed counts.
    terminal_outcomes: Vec<String>,
    /// Per-`request_id` idempotency-record BEHAVIOR, probed through the real
    /// [`PushPort::push_with_request_id`] replay/conflict path (server-minted ids differ per backend, so the
    /// comparable facts are the behavior, not the id value): `(replay_returns_original, replay_item_count,
    /// conflicting_body_rejected, no_phantom_commit_on_replay)`.
    rid_idempotency: (bool, usize, bool, bool),
}

/// **AC-TXN-6** cross-combination parity (TP-003 §3.10 row 212). Run the SAME operation history and the SAME
/// failure schedule on two backend profiles, then compare — after a restart of both — the final visible
/// `QueueMetrics` (including complete/failed terminal COUNTS), the `select_eligible` order, the
/// pending/active-lease set, the PER-ITEM terminal-outcome records (reconstructed from each backend's durable
/// log), and the per-`request_id` idempotency-record behavior (replay returns the original result; a
/// conflicting body under the same id is rejected; a same-body replay adds no phantom commit). Uses explicit
/// item ids for the compared lifecycle state (server-minted ids differ per backend by construction) and
/// compares the request_id records by their backend-independent BEHAVIOR rather than the minted id value.
pub async fn ac_txn_6_parity<A: ConformanceCore + LogRead, B: ConformanceCore + LogRead>(
    make_a: impl Fn(&str) -> A,
    make_b: impl Fn(&str) -> B,
) -> AcOutcome {
    // Reconstruct the per-item terminal-outcome records (Complete/Fail) from a backend's DURABLE command log,
    // restricted to the explicit-id items so the result is backend-independent (server-minted ids differ).
    async fn terminal_outcomes_from_log<X: LogRead>(x: &X) -> Result<Vec<String>, String> {
        const EXPLICIT_IDS: &[&str] = &["1", "2", "3", "4"];
        let entries = x
            .read_from(&shard(), None, 100_000)
            .await
            .map_err(|e| format!("read_from (terminal-outcome reconstruction): {e:?}"))?
            .entries;
        let mut out: Vec<String> = Vec::new();
        for (_, env) in &entries {
            if let QueueCommand::Finalize(fc) = &env.command {
                for o in &fc.outcomes {
                    if matches!(o.kind, FinalizeKind::Complete | FinalizeKind::Fail) {
                        let id = o.item_id.to_string();
                        if EXPLICIT_IDS.contains(&id.as_str()) {
                            out.push(format!("{id}:{:?}", o.kind));
                        }
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    // Drive the identical op history + failure schedule against one backend, returning its post-restart
    // observable state as a backend-independent comparable value.
    async fn run<X: ConformanceCore + LogRead>(
        make: &impl Fn(&str) -> X,
    ) -> Result<ParityState, String> {
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
            // Op history: claim + finalize the highest-priority item (p2 @ prio 10) -> terminal Complete.
            let claimed = x
                .claim(claim_req(1, 500, 10))
                .await
                .map_err(|e| format!("claim (complete): {e:?}"))?;
            let leased = claimed.items[0].item_id;
            x.finalize(
                &shard(),
                vec![FinalizeOutcome::new(leased, FinalizeKind::Complete)],
                ts(20),
                None,
            )
            .await
            .map_err(|e| format!("finalize (complete): {e:?}"))?;
            // Op history: claim + finalize the next item (p3 @ prio 20) -> terminal FAILED, so the per-item
            // terminal-outcome comparison distinguishes complete-vs-failed, not just counts.
            let claimed = x
                .claim(claim_req(1, 500, 30))
                .await
                .map_err(|e| format!("claim (fail): {e:?}"))?;
            let leased = claimed.items[0].item_id;
            x.finalize(
                &shard(),
                vec![FinalizeOutcome::new(leased, FinalizeKind::Fail)],
                ts(40),
                None,
            )
            .await
            .map_err(|e| format!("finalize (fail): {e:?}"))?;
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
        // PER-ITEM terminal-outcome records from the durable log (captured before the request_id probe below
        // adds a fresh push, though a push would not affect a Finalize-only scan).
        let terminal_outcomes = terminal_outcomes_from_log(&x).await?;

        // PER-REQUEST_ID idempotency record, exercised through the REAL idempotency path so we compare the
        // record's behavior (not the minted id, which differs per backend): a same-body retry under the same
        // request_id must replay the ORIGINAL committed result and add NO second durable commit; a conflicting
        // body under the same id must be rejected `RequestIdConflict`. Runs after the compared lifecycle state
        // is captured, so the probe's pushed item never pollutes the metrics/eligible/pending/terminal tuple.
        let rid = RequestId::new("ac-txn-6-parity-rid").unwrap();
        let body = vec![spec("txn6-rid", 25)];
        let durable_before = durable_command_count(&x).await?;
        let original = x
            .push_with_request_id(&shard(), rid.clone(), body.clone(), ts(50), None)
            .await
            .map_err(|e| format!("request_id push: {e:?}"))?;
        let replay = x
            .push_with_request_id(&shard(), rid.clone(), body, ts(51), None)
            .await
            .map_err(|e| format!("request_id same-body replay: {e:?}"))?;
        let conflict = x
            .push_with_request_id(
                &shard(),
                rid,
                vec![spec("txn6-rid-different", 26)],
                ts(52),
                None,
            )
            .await;
        let durable_after = durable_command_count(&x).await?;
        let rid_idempotency = (
            original.is_fresh() && replay.is_replayed() && replay.item_ids == original.item_ids,
            original.len(),
            matches!(conflict, Err(EngineError::RequestIdConflict)),
            // Two same-request_id same-body pushes + one conflicting push commit exactly ONE new command.
            durable_after == durable_before + 1,
        );

        Ok(ParityState {
            metrics,
            eligible,
            pending,
            terminal_outcomes,
            rid_idempotency,
        })
    }

    let a = run(&make_a).await?;
    let b = run(&make_b).await?;
    ensure!(
        a.metrics == b.metrics,
        "final visible metrics diverge across combinations: {:?} vs {:?}",
        a.metrics,
        b.metrics
    );
    ensure!(
        a.eligible == b.eligible,
        "final visible eligibility order diverges across combinations: {:?} vs {:?}",
        a.eligible,
        b.eligible
    );
    ensure!(
        a.pending == b.pending,
        "active-lease / pending set diverges across combinations: {:?} vs {:?}",
        a.pending,
        b.pending
    );
    ensure!(
        a.terminal_outcomes == b.terminal_outcomes,
        "per-item terminal-outcome records diverge across combinations: {:?} vs {:?}",
        a.terminal_outcomes,
        b.terminal_outcomes
    );
    // The request_id idempotency record must both HOLD (replay=original, conflict rejected, no phantom
    // commit) and be IDENTICAL across the two combinations.
    ensure!(
        a.rid_idempotency == b.rid_idempotency,
        "per-request_id idempotency record behavior diverges across combinations: {:?} vs {:?}",
        a.rid_idempotency,
        b.rid_idempotency
    );
    let (replay_ok, rid_items, conflict_ok, no_phantom) = a.rid_idempotency;
    ensure!(
        replay_ok && conflict_ok && no_phantom,
        "per-request_id idempotency record did not hold (replay_returns_original={replay_ok}, conflicting_body_rejected={conflict_ok}, no_phantom_commit={no_phantom})"
    );
    Ok(vec![format!(
        "identical across combinations: final visible QueueMetrics (incl. complete/failed terminal counts), select_eligible order, pending/active-lease set (item_id:attempt), PER-ITEM terminal-outcome records reconstructed from the durable log ({:?}), and per-request_id idempotency-record behavior (same-body replay returns the original {rid_items}-item result, conflicting body -> RequestIdConflict, no phantom commit) (metrics={:?}, eligible={:?}, pending={:?})",
        a.terminal_outcomes, a.metrics, a.eligible, a.pending
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
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/perf/evidence")
        })
}

fn recorded_at_stamp(body: &str) -> Option<&str> {
    const PREFIX: &str = "\"recorded_at\":\"";
    let start = body.find(PREFIX)? + PREFIX.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

/// Write the evidence file, overwriting any prior run so the JSONL reflects exactly THIS run.
pub fn write_evidence(file_name: &str, records: &[AcEvidence]) -> std::io::Result<PathBuf> {
    let dir = evidence_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(file_name);

    // Evidence is tracked in git and the workspace gate executes this test on every run. Preserve
    // the previous observation time when the newly observed records are byte-for-byte identical;
    // otherwise a successful `cargo test --workspace` dirties the worktree solely because the
    // clock advanced, which makes DDx reject an otherwise valid implementation commit.
    if let Ok(existing) = std::fs::read_to_string(&path)
        && let Some(stamp) = recorded_at_stamp(&existing)
    {
        let unchanged = records
            .iter()
            .map(|r| r.to_json_line(stamp))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        if unchanged == existing {
            return Ok(path);
        }
    }

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

#[cfg(test)]
mod evidence_jsonl_tests {
    use super::recorded_at_stamp;

    #[test]
    fn extracts_recorded_at_stamp() {
        let body = r#"{"suite":"test","recorded_at":"epoch:123"}
"#;
        assert_eq!(recorded_at_stamp(body), Some("epoch:123"));
    }

    #[test]
    fn rejects_missing_or_unterminated_recorded_at_stamp() {
        assert_eq!(recorded_at_stamp("{}\n"), None);
        assert_eq!(recorded_at_stamp(r#"{"recorded_at":"epoch:123}"#), None);
    }
}
