use std::sync::Arc;

use pqueue_conformance::{envelope, item, qdef};
use pqueue_core::{BodyHash, ItemId, ItemState, RequestId};
use pqueue_engine::{
    AsyncProjectionStore, CommandPosition, IdempotencyDecision, PushCommand, PushFingerprint,
    QueueCommand, QueueKey, RequestOutcome, push_items_fingerprint_sha256,
};
use pqueue_turso::TursoRelational;

fn push(
    shard: &QueueKey,
    id: ItemId,
    sequence: u64,
) -> (CommandPosition, pqueue_engine::CommandEnvelope) {
    (
        CommandPosition::new(shard.clone(), 0, sequence),
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item(&id.to_string(), &format!("key-{id}"), 0)],
            }),
            vec![id],
        ),
    )
}

fn replayable_push(
    shard: &QueueKey,
    id: ItemId,
    sequence: u64,
    request_id: &str,
) -> (
    CommandPosition,
    pqueue_engine::CommandEnvelope,
    RequestId,
    PushFingerprint,
) {
    let (position, mut command) = push(shard, id, sequence);
    let QueueCommand::Push(push) = &command.command else {
        unreachable!("push helper always creates a push command")
    };
    let fingerprint = PushFingerprint {
        canonical_sha256: push_items_fingerprint_sha256(&push.items).unwrap(),
        legacy_body_hash: BodyHash(7),
    };
    let request_id = RequestId::new(request_id).unwrap();
    command.request_id = Some(request_id.clone());
    command.request_fingerprint = Some(fingerprint.legacy_body_hash.0);
    command.request_outcome = Some(RequestOutcome::Push { item_ids: vec![id] });
    (position, command, request_id, fingerprint)
}

async fn replay_decision(
    store: &TursoRelational,
    shard: &QueueKey,
    request_id: RequestId,
    fingerprint: PushFingerprint,
) -> IdempotencyDecision<Vec<ItemId>> {
    AsyncProjectionStore::push_idempotency(
        store,
        shard.clone(),
        request_id,
        fingerprint,
        pqueue_conformance::ts(1),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_cuts_leave_zero_or_one_replayable_outcome_and_no_waiter_loss() {
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = Arc::new(TursoRelational::in_memory().await.unwrap());
    AsyncProjectionStore::ensure_shard(store.as_ref(), definition)
        .await
        .unwrap();

    let unpolled_id = ItemId::new("301").unwrap();
    let (position, command, request_id, fingerprint) =
        replayable_push(&shard, unpolled_id, 0, "unpolled");
    let unpolled = AsyncProjectionStore::apply_live(store.as_ref(), vec![position], vec![command]);
    drop(unpolled);
    assert_eq!(
        AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), unpolled_id)
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        replay_decision(store.as_ref(), &shard, request_id, fingerprint).await,
        IdempotencyDecision::Proceed
    ));

    let raced_id = ItemId::new("302").unwrap();
    let (position, command, raced_request_id, raced_fingerprint) =
        replayable_push(&shard, raced_id, 0, "raced");
    let raced_store = store.clone();
    let task = tokio::spawn(async move {
        AsyncProjectionStore::apply_live(raced_store.as_ref(), vec![position], vec![command]).await
    });
    tokio::task::yield_now().await;
    task.abort();
    let _ = task.await;

    let raced_state = AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), raced_id)
        .await
        .unwrap();
    assert!(matches!(raced_state, None | Some(ItemState::Pending)));
    match raced_state {
        Some(ItemState::Pending) => {
            assert_eq!(
                replay_decision(
                    store.as_ref(),
                    &shard,
                    raced_request_id.clone(),
                    raced_fingerprint,
                )
                .await,
                IdempotencyDecision::Replay(vec![raced_id])
            );
            let conflicting = PushFingerprint {
                canonical_sha256: [0xff; 32],
                legacy_body_hash: BodyHash(u64::MAX),
            };
            assert!(matches!(
                replay_decision(store.as_ref(), &shard, raced_request_id, conflicting).await,
                IdempotencyDecision::Conflict
            ));
        }
        None => assert!(matches!(
            replay_decision(store.as_ref(), &shard, raced_request_id, raced_fingerprint,).await,
            IdempotencyDecision::Proceed
        )),
        Some(other) => panic!("unexpected raced state: {other:?}"),
    }
    let next_sequence = AsyncProjectionStore::recovery_high_water(store.as_ref(), shard.clone())
        .await
        .unwrap()
        .map_or(0, |position| position.sequence + 1);
    let survivor_id = ItemId::new("303").unwrap();
    let (position, command, survivor_request_id, survivor_fingerprint) =
        replayable_push(&shard, survivor_id, next_sequence, "resolved");
    AsyncProjectionStore::apply_live(store.as_ref(), vec![position], vec![command])
        .await
        .unwrap();
    assert_eq!(
        AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), survivor_id)
            .await
            .unwrap(),
        Some(ItemState::Pending)
    );
    assert_eq!(
        replay_decision(
            store.as_ref(),
            &shard,
            survivor_request_id,
            survivor_fingerprint,
        )
        .await,
        IdempotencyDecision::Replay(vec![survivor_id])
    );
}
