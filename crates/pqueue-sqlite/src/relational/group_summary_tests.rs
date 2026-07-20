//! White-box tests for `pqueue_group_summary` maintenance — they read the summary table directly
//! (it has no read port yet; BQ-14 consumes it), driving state through the public ports.
use super::*;

use std::collections::BTreeMap;

use pqueue_core::{
    ClientItemKey, GroupKey, LeaseToken, PriorityModel, PriorityValue, QueueDefinition, QueueId,
    RequestId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCompatibility, ClaimPort, ClaimRequest, CommandEnvelope, ControlPlaneStore,
    EngineError, FinalizeKind, FinalizeOutcome, FinalizePort, PurgePort, PushPort, PushSpec,
    QueueCommand, QueueKey, ReclaimDriver, SetGatesCommand, SetGatesPort, UpsertOutcome,
    UpsertPort,
};
use rusqlite::{OptionalExtension, params};

use pqueue_core::{
    EligibilityPolicy, GateKeyPolicy, OrderingMode, PriorityDirection, PriorityModelKind,
    PriorityTieBreaker, RecurrencePolicy, RetryPolicy, WorkerId,
};
use pqueue_engine::{CommandChecksum, CommandId, GroupBatching};

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}
fn qdef_gates() -> QueueDefinition {
    QueueDefinition {
        eligibility_policy: EligibilityPolicy {
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(8),
            max_gates_per_request: Some(8),
            ..EligibilityPolicy::default()
        },
        ..qdef()
    }
}

fn shard() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}
fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}
fn grouped(priority: i64, group: &str) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        group_key: Some(GroupKey::new(group).unwrap()),
        ..Default::default()
    }
}
fn grouped_not_before(priority: i64, group: &str, not_before: i64) -> PushSpec {
    PushSpec {
        not_before: Some(ts(not_before)),
        ..grouped(priority, group)
    }
}
fn gated_grouped_not_before(priority: i64, group: &str, not_before: i64, gate: &str) -> PushSpec {
    PushSpec {
        gate_keys: vec![gate.to_string()],
        ..grouped_not_before(priority, group, not_before)
    }
}
fn claim_req(max: usize, exp: i64, now: i64) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
        shard: shard(),
        worker_id: WorkerId::new("w1").unwrap(),
        max_items: max,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(exp),
        now: ts(now),
        compatibility: ClaimCompatibility::default(),
        expected_epoch: None,
    }
}
fn claim_req_compat(
    max: usize,
    exp: i64,
    now: i64,
    compatibility: ClaimCompatibility,
) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
        compatibility,
        ..claim_req(max, exp, now)
    }
}

async fn set_gate(b: &SqliteRelationalBackend, gate_key: &str, blocked: bool, now: i64) {
    b.set_gates(
        &shard(),
        SetGatesCommand {
            gate_keys: vec![gate_key.to_string()],
            blocked,
        },
        ts(now),
        None,
    )
    .await
    .unwrap();
}

/// (oldest_eligible_at, eligible_item_count, rep_item_id) for the group, or None if no row exists.
fn summary(b: &SqliteRelationalBackend, group: &str) -> Option<(Option<i64>, i64, Option<String>)> {
    let g = b.inner.lock().unwrap();
    g.conn
        .query_row(
            "SELECT oldest_eligible_at, eligible_item_count, rep_item_id \
                 FROM pqueue_group_summary WHERE tenant_id='t1' AND queue_id='q1' AND group_key=?1",
            params![group],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .unwrap()
}

fn next_seq(b: &SqliteRelationalBackend) -> i64 {
    let g = b.inner.lock().unwrap();
    g.conn
        .query_row(
            "SELECT next_seq FROM relational_cursor WHERE tenant='t1' AND queue='q1'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[tokio::test]
async fn request_id_push_replays_prior_ids_without_second_append() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();
    let request_id = RequestId::new("push-req-1").unwrap();
    let body = vec![PushSpec::default(), grouped(20, "g")];

    let first = b
        .push_with_request_id(&shard(), request_id.clone(), body.clone(), ts(0), None)
        .await
        .unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(next_seq(&b), 1);

    let replay = b
        .push_with_request_id(&shard(), request_id, body, ts(1), None)
        .await
        .unwrap();
    assert_eq!(replay, first, "same request body replays the prior ids");
    assert_eq!(next_seq(&b), 1, "replay did not append a second command");
}

#[tokio::test]
async fn request_id_push_conflicts_on_different_body() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();
    let request_id = RequestId::new("push-req-conflict").unwrap();

    b.push_with_request_id(
        &shard(),
        request_id.clone(),
        vec![PushSpec::default()],
        ts(0),
        None,
    )
    .await
    .unwrap();

    let err = b
        .push_with_request_id(
            &shard(),
            request_id,
            vec![grouped(99, "other")],
            ts(1),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::RequestIdConflict);
    assert_eq!(next_seq(&b), 1, "conflict did not append");
}

#[tokio::test]
async fn push_without_request_id_still_appends_each_call() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();

    let first = b
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let second = b
        .push(&shard(), vec![PushSpec::default()], ts(1), None)
        .await
        .unwrap();

    assert_ne!(second, first);
    assert_eq!(next_seq(&b), 2);
}

#[tokio::test]
async fn same_group_claim_discovers_group_that_becomes_due_by_time() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![grouped_not_before(10, "deferred", 10)],
            ts(0),
            None,
        )
        .await
        .unwrap();

    let early = b
        .claim(claim_req_compat(
            10,
            500,
            9,
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert!(early.items.is_empty(), "not_before is half-open before due");

    let due = b
        .claim(claim_req_compat(
            10,
            500,
            10,
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        due.items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        ids,
        "same_group_key sees the group exactly at not_before with no intervening mutation"
    );
}

#[tokio::test]
async fn group_batching_discovers_group_that_becomes_due_by_time() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(QueueDefinition {
        max_eligible_group_size: Some(5),
        ..qdef()
    })
    .await
    .unwrap();
    let ids = b
        .push(
            &shard(),
            vec![
                grouped_not_before(10, "deferred", 10),
                grouped_not_before(11, "deferred", 10),
            ],
            ts(0),
            None,
        )
        .await
        .unwrap();

    let claimed = b
        .claim(claim_req_compat(
            10,
            500,
            10,
            ClaimCompatibility {
                group_batching: Some(GroupBatching { max_groups: 1 }),
                ..Default::default()
            },
        ))
        .await
        .unwrap();

    assert_eq!(
        claimed
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        ids,
        "group_batching refreshes and leases the whole due group"
    );
}

#[tokio::test]
async fn due_refresh_keeps_gate_blocked_groups_unclaimable() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef_gates()).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![gated_grouped_not_before(10, "deferred", 10, "hold")],
            ts(0),
            None,
        )
        .await
        .unwrap();
    set_gate(&b, "hold", true, 1).await;

    let blocked = b
        .claim(claim_req_compat(
            10,
            500,
            10,
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert!(
        blocked.items.is_empty(),
        "due refresh must not make a gate-blocked group claimable"
    );

    set_gate(&b, "hold", false, 11).await;
    let unblocked = b
        .claim(claim_req_compat(
            10,
            500,
            12,
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        unblocked
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        ids,
        "clearing the gate lets the due group refresh and claim"
    );
}

#[tokio::test]
async fn group_summary_tracks_eligibility_through_the_lifecycle() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();

    // Push two grouped items (priorities 10, 20) — rep is the priority-10 item, count 2.
    let ids = b
        .push(
            &shard(),
            vec![grouped(10, "g"), grouped(20, "g")],
            ts(0),
            None,
        )
        .await
        .unwrap();
    let (oldest, count, rep) = summary(&b, "g").expect("summary row created on grouped push");
    assert_eq!(count, 2);
    assert!(
        oldest.is_some(),
        "oldest_eligible_at set while items eligible"
    );
    assert_eq!(
        rep,
        Some(ids[0].to_string()),
        "rep is the first-claimable item"
    );

    // Claim the rep (priority 10) — it leaves eligibility; count 1, rep advances to the priority-20 item.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let (_, count, rep) = summary(&b, "g").unwrap();
    assert_eq!(count, 1, "leased item leaves the eligible count");
    assert_eq!(
        rep,
        Some(ids[1].to_string()),
        "rep advances to the next eligible item"
    );

    // Purge the remaining pending grouped item — group drains to empty.
    b.purge(&shard(), vec![ids[1]], false, ts(20), None)
        .await
        .unwrap();
    let (oldest, count, rep) = summary(&b, "g").unwrap();
    assert_eq!(count, 0, "empty group has zero eligible");
    assert!(
        oldest.is_none() && rep.is_none(),
        "no representative when empty"
    );
}

#[tokio::test]
async fn lease_expiry_returns_item_to_the_group_summary() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();
    let ids = b
        .push(&shard(), vec![grouped(5, "g")], ts(0), None)
        .await
        .unwrap();
    b.claim(claim_req(1, 100, 10)).await.unwrap();
    assert_eq!(summary(&b, "g").unwrap().1, 0, "leased -> not eligible");

    // Reclaim the expired lease (tick) -> the item is pending again and back in the group's count.
    b.tick(ts(101)).await.unwrap();
    let (_, count, rep) = summary(&b, "g").unwrap();
    assert_eq!(count, 1, "reclaimed item is eligible again");
    assert_eq!(rep, Some(ids[0].to_string()));
}

/// Count of durable terminal (Complete) rows still resident in the item projection.
fn complete_count(b: &SqliteRelationalBackend) -> i64 {
    let g = b.inner.lock().unwrap();
    g.conn
        .query_row(
            "SELECT COUNT(*) FROM pqueue_items \
                 WHERE tenant_id='t1' AND queue_id='q1' AND lifecycle_state='Complete'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// The terminal item's (`terminal_command_epoch`, `last_command_sequence`) — the position the emission
/// cursor must pass before a reap is permitted for an emit-enabled queue.
fn terminal_pos(b: &SqliteRelationalBackend) -> (i64, i64) {
    let g = b.inner.lock().unwrap();
    g.conn
        .query_row(
            "SELECT terminal_command_epoch, last_command_sequence FROM pqueue_items \
                 WHERE tenant_id='t1' AND queue_id='q1' LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
}

/// Durably record the emission frontier (as the change-record sink would) so the reclaim tick can read it.
fn set_emission_cursor(b: &SqliteRelationalBackend, epoch: i64, seq: i64) {
    let g = b.inner.lock().unwrap();
    g.conn
        .execute(
            "INSERT INTO relational_emission_cursor(tenant,queue,epoch,seq) \
                 VALUES('t1','q1',?1,?2) \
                 ON CONFLICT(tenant,queue) DO UPDATE SET epoch=excluded.epoch, seq=excluded.seq",
            params![epoch, seq],
        )
        .unwrap();
}

// TD-008 CL-6: an emit-enabled queue reaps a terminal item only after BOTH its retention has elapsed AND
// the durable emission cursor has passed it. The tick applies the conjunction via the emission cursor.
#[tokio::test]
async fn sqlite_terminal_reap_sweeps_with_cursor_conjunction() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(QueueDefinition {
        terminal_retention_ms: 1,
        ..qdef()
    })
    .await
    .unwrap();
    let ids = b
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(ids[0], FinalizeKind::Complete)],
        ts(2),
        None,
    )
    .await
    .unwrap();

    let (epoch, seq) = terminal_pos(&b);

    // Retention has elapsed (ts(3), 1ms retention) but the emission cursor is strictly BEHIND the
    // terminal command — the change record is not yet durably emitted, so the tick must NOT reap.
    set_emission_cursor(&b, epoch, seq - 1);
    b.tick(ts(3)).await.unwrap();
    assert_eq!(
        complete_count(&b),
        1,
        "retention-elapsed but cursor-behind must not reap"
    );

    // The cursor now reaches the terminal command: retention AND cursor conjunction holds -> reaped.
    set_emission_cursor(&b, epoch, seq);
    b.tick(ts(5)).await.unwrap();
    assert_eq!(
        complete_count(&b),
        0,
        "retention-elapsed and cursor-passed must reap"
    );
}

// TD-008 CL-6: an opt-out queue (`emit_change_records=false`) emits no change records, so its terminal
// reap is gated on retention alone and ignores the (behind) emission cursor.
#[tokio::test]
async fn sqlite_terminal_reap_opt_out_ignores_cursor() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(QueueDefinition {
        terminal_retention_ms: 1,
        emit_change_records: false,
        ..qdef()
    })
    .await
    .unwrap();
    let ids = b
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(ids[0], FinalizeKind::Complete)],
        ts(2),
        None,
    )
    .await
    .unwrap();

    // Cursor left strictly behind the terminal command; the opt-out queue ignores it and reaps on
    // retention alone.
    let (epoch, seq) = terminal_pos(&b);
    set_emission_cursor(&b, epoch, seq - 1);
    b.tick(ts(3)).await.unwrap();
    assert_eq!(
        complete_count(&b),
        0,
        "opted-out queues reap on retention alone"
    );
}

#[tokio::test]
async fn finalize_release_returns_item_to_the_group_summary() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();
    let ids = b
        .push(&shard(), vec![grouped(5, "g")], ts(0), None)
        .await
        .unwrap();
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(summary(&b, "g").unwrap().1, 0, "leased -> not eligible");

    // Release (no-fault give-back) returns the item to pending -> back in the group's eligible count.
    b.finalize(
        &shard(),
        vec![FinalizeOutcome::new(ids[0], FinalizeKind::Release)],
        ts(20),
        None,
    )
    .await
    .unwrap();
    let (_, count, rep) = summary(&b, "g").unwrap();
    assert_eq!(count, 1, "released item is eligible again");
    assert_eq!(rep, Some(ids[0].to_string()));
}

#[tokio::test]
async fn cohort_expired_drains_the_group_summary() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();
    b.push(
        &shard(),
        vec![grouped(5, "g"), grouped(6, "g")],
        ts(0),
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary(&b, "g").unwrap().1, 2);

    // Force the whole cohort terminal -> the group's eligible summary drains to empty.
    commit_cohort_expired(&b, "g", ts(20)).await;
    let (oldest, count, rep) = summary(&b, "g").unwrap();
    assert_eq!(
        count, 0,
        "cohort-expired members are terminal -> not eligible"
    );
    assert!(oldest.is_none() && rep.is_none());
}

#[tokio::test]
async fn pending_purge_retains_client_key_against_repush() {
    let b = SqliteRelationalBackend::in_memory().unwrap();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("pk").unwrap();
    let id = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            BTreeMap::new(),
            Default::default(),
            None,
            ts(0),
            None,
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        _ => panic!("insert"),
    };
    // API-001 applies the retention tombstone to every successful removal, including PENDING.
    b.purge(&shard(), vec![id], false, ts(1), None)
        .await
        .unwrap();
    assert_eq!(
        b.replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            Default::default(),
            None,
            ts(2),
            None
        )
        .await,
        Err(EngineError::Terminal),
        "a pending purge must retain its client key until the API-001 window expires"
    );
}

/// Apply a `CohortExpired` command through the write UoW (no dedicated port).
async fn commit_cohort_expired(b: &SqliteRelationalBackend, group: &str, now: UtcTimestamp) {
    let env = CommandEnvelope {
        command_id: CommandId::new("ce"),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids: vec![],
        command: QueueCommand::CohortExpired(pqueue_engine::CohortExpiredCommand {
            group_key: GroupKey::new(group).unwrap(),
        }),
        checksum: CommandChecksum(0),
        created_at: now,
    };
    let epoch = b.current_epoch(&shard()).await.unwrap();
    b.commit_raw(pqueue_engine::RawCommitRequest::new(
        shard(),
        vec![env],
        epoch,
    ))
    .await
    .unwrap();
}

/// BQ-11d: `pqueue_group_summary` is durable — it survives a reopen with the recovered representative,
/// because it is a DB table maintained in-transaction, not in-process state.
#[tokio::test]
async fn group_summary_survives_reopen() {
    let path = std::env::temp_dir()
        .join(format!("pqueue-rel-gs-reopen-{}.db", std::process::id()))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&path);

    let rep_before;
    {
        let a = SqliteRelationalBackend::open(&path).unwrap();
        a.create_queue(qdef()).await.unwrap();
        let ids = a
            .push(
                &shard(),
                vec![grouped(10, "g"), grouped(20, "g")],
                ts(0),
                None,
            )
            .await
            .unwrap();
        let (_, count, rep) = summary(&a, "g").unwrap();
        assert_eq!(count, 2);
        assert_eq!(rep, Some(ids[0].to_string()));
        rep_before = rep;
    } // crash

    let b = SqliteRelationalBackend::open(&path).unwrap();
    let (_, count, rep) = summary(&b, "g").expect("group_summary row survives reopen");
    assert_eq!(
        count, 2,
        "eligible count recovered from the durable summary"
    );
    assert_eq!(rep, rep_before, "representative recovered unchanged");
    let _ = std::fs::remove_file(&path);
}
