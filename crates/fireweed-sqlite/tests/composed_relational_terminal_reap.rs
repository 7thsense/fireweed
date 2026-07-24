use fireweed_conformance::{qdef, shard};
use fireweed_core::{LeaseToken, UtcTimestamp, WorkerId};
use fireweed_engine::{
    ClaimPort, ClaimRequest, CommandPosition, ControlPlaneStore, FinalizeKind, FinalizeOutcome,
    FinalizePort, LogStore, ProjectionStore, PushPort, PushSpec,
};
use fireweed_sqlite::composed_sqlite_relational_in_memory;

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn claim_req() -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
        shard: shard(),
        worker_id: WorkerId::new("worker-1").unwrap(),
        max_items: 1,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(500),
        now: ts(10),
        compatibility: fireweed_engine::ClaimCompatibility::default(),
        expected_epoch: None,
    }
}

fn queue_definition(emit_change_records: bool) -> fireweed_core::QueueDefinition {
    let mut definition = qdef();
    definition.emit_change_records = emit_change_records;
    definition.terminal_retention_ms = 1;
    definition
}

#[tokio::test]
async fn reap_waits_for_emission_cursor_on_opted_in_queue() {
    let backend = composed_sqlite_relational_in_memory().unwrap();
    backend.create_queue(queue_definition(true)).await.unwrap();

    let ids = backend
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let item_id = ids[0];

    backend.claim(claim_req()).await.unwrap();
    backend
        .finalize(
            &shard(),
            vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
            ts(2),
            None,
        )
        .await
        .unwrap();

    let mut log = backend.with_log(|log| log.clone());
    log.set_emission_cursor(&shard(), CommandPosition::new(shard(), 0, 1))
        .unwrap();

    assert_eq!(
        backend
            .reap_terminal_items(&shard(), ts(3), 1, true)
            .unwrap(),
        0,
        "retention-elapsed but cursor-behind must not reap"
    );
    assert_eq!(
        backend.with_projection(|projection| projection.item_state(&shard(), &item_id).unwrap()),
        Some(fireweed_core::ItemState::Complete)
    );

    log.set_emission_cursor(&shard(), CommandPosition::new(shard(), 0, 2))
        .unwrap();
    assert_eq!(
        backend
            .reap_terminal_items(&shard(), ts(5), 1, true)
            .unwrap(),
        1,
        "retention-elapsed and cursor-passed must reap"
    );
    assert_eq!(
        backend.with_projection(|projection| projection.item_state(&shard(), &item_id).unwrap()),
        None
    );
}

#[tokio::test]
async fn reap_ignores_emission_cursor_for_opted_out_queue() {
    let backend = composed_sqlite_relational_in_memory().unwrap();
    backend.create_queue(queue_definition(false)).await.unwrap();

    let ids = backend
        .push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let item_id = ids[0];

    backend.claim(claim_req()).await.unwrap();
    backend
        .finalize(
            &shard(),
            vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
            ts(2),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        backend
            .reap_terminal_items(&shard(), ts(3), 1, false)
            .unwrap(),
        1,
        "opted-out queues reap on retention alone"
    );
    assert_eq!(
        backend.with_projection(|projection| projection.item_state(&shard(), &item_id).unwrap()),
        None
    );
}
