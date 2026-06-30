//! ADR-012 P2 proof: the runtime-agnostic group-commit write path for `ComposedBackend` over the segmented
//! object-log axis. Asserts (1) co-buffering — N concurrent `push`es + ONE `flush_tick` seal as ONE segment
//! of N commands (mean batch ≈ N ≫ 1, NOT one seal per append); (2) claim/finalize correctness under
//! group-commit (a claim force-seals the buffer before it selects, so it observes the pushed items and two
//! claims never pick the same candidate); and (3) reopen/recovery still rebuilds the projection from the log.

use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_core::{
    EligibilityPolicy, ItemId, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, FinalizeKind, FinalizeOutcome,
    FinalizePort, ProjectionRead, PushPort, PushSpec, QueueKey,
};
use pqueue_objectlog::{SegmentConfig, composed_objectlog_backend_group_commit};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!("pqueue-objlog-gc-{tag}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn shard() -> QueueKey {
    QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("queue").unwrap(),
    )
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("tenant").unwrap(),
        queue_id: QueueId::new("queue").unwrap(),
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
        retry_policy: RetryPolicy { max_attempts: 10 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
    }
}

/// A 1 MiB segment target so N small pushes never size-seal mid-buffer; the seal is driven by `flush_tick`
/// at the 20 ms latency cap — exactly the production latency trigger.
fn gc_config() -> SegmentConfig {
    SegmentConfig::new(1 << 20, 20).unwrap()
}

fn ts(secs: i64) -> UtcTimestamp {
    UtcTimestamp::new(secs, 0).unwrap()
}

#[tokio::test]
async fn concurrent_pushes_cobuffer_into_one_sealed_segment() {
    let root = tmp_root("cobuffer");
    let backend = composed_objectlog_backend_group_commit(&root, gc_config()).expect("compose");
    backend.create_queue(qdef()).await.unwrap();
    let shard = shard();

    const N: usize = 8;
    let now = ts(1); // ts_to_ms == 1000

    // Fire N pushes: each runs its synchronous prologue (buffer + register a SealSlot) eagerly when called,
    // then yields an ack-after-seal future. None has sealed yet (below the 1 MiB size trigger).
    let pushes: Vec<_> = (0..N)
        .map(|_| backend.push(&shard, vec![PushSpec::default()], now, None))
        .collect();

    // BEFORE the flush nothing is acked: the substrate buffered all N but sealed no segment.
    assert_eq!(
        backend.with_log(|l| l.counters()).segments_sealed,
        0,
        "co-buffering: no seal per append"
    );

    // Drive the externalized flusher ONCE past the latency cap → seal the whole buffer as ONE segment.
    backend.flush_tick(1000 + 21).expect("flush_tick");

    // Every push now acks (ack-after-seal).
    let mut ids = Vec::new();
    for p in pushes {
        ids.extend(p.await.expect("push ack"));
    }
    assert_eq!(ids.len(), N);

    let counters = backend.with_log(|l| l.counters());
    assert_eq!(
        counters.segments_sealed, 1,
        "all N concurrent pushes seal as ONE segment, not one-per-append"
    );
    assert_eq!(counters.commands_committed, N as u64);
    assert_eq!(counters.group_commit_batches, vec![N]);
    assert_eq!(
        counters.mean_batch_size(),
        N as f64,
        "mean batch size == N ≫ 1 proves group-commit co-buffering"
    );

    assert_eq!(
        backend.metrics(&shard).await.unwrap().pending,
        N as u64,
        "the sealed batch applied to the projection in one apply"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn claim_force_seals_buffer_before_select_then_finalize() {
    let root = tmp_root("claim");
    let backend = composed_objectlog_backend_group_commit(&root, gc_config()).expect("compose");
    backend.create_queue(qdef()).await.unwrap();
    let shard = shard();
    let now = ts(1);

    // Push two items but do NOT flush — they are buffered, unsealed, NOT yet applied to the projection.
    let p1 = backend.push(&shard, vec![PushSpec::default()], now, None);
    let p2 = backend.push(&shard, vec![PushSpec::default()], now, None);
    assert_eq!(
        backend.with_log(|l| l.counters()).segments_sealed,
        0,
        "still buffered"
    );

    // A claim force-seals the buffered batch FIRST (so it observes the pushes), then selects from applied
    // state. This both acks the buffered pushes AND leases from them in one serialized unit.
    let claimed = backend
        .claim(ClaimRequest {
            shard: shard.clone(),
            worker_id: WorkerId::new("w1").unwrap(),
            max_items: 10,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: ts(3_600),
            now,
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        })
        .await
        .unwrap();
    assert_eq!(
        claimed.items.len(),
        2,
        "the force-seal-before-select made both buffered pushes visible to the claim"
    );

    // The buffered pushes acked because the claim's force-seal sealed their segment.
    assert_eq!(p1.await.unwrap().len(), 1);
    assert_eq!(p2.await.unwrap().len(), 1);

    let claimed_ids: Vec<ItemId> = claimed.items.iter().map(|c| c.item_id).collect();
    backend
        .finalize(
            &shard,
            claimed_ids
                .iter()
                .map(|id| FinalizeOutcome::new(*id, FinalizeKind::Complete))
                .collect(),
            now,
            None,
        )
        .await
        .unwrap();

    let m = backend.metrics(&shard).await.unwrap();
    assert_eq!(m.pending, 0);
    assert_eq!(m.complete, 2);

    // A second claim selects nothing (the candidates were leased then completed — never double-leased).
    let again = backend
        .claim(ClaimRequest {
            shard: shard.clone(),
            worker_id: WorkerId::new("w2").unwrap(),
            max_items: 10,
            lease_token: LeaseToken::new("lease-2").unwrap(),
            lease_expires_at: ts(3_600),
            now,
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        })
        .await
        .unwrap();
    assert!(again.items.is_empty());

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn group_commit_reopen_recovers_projection_from_the_log() {
    let root = tmp_root("reopen");
    {
        let backend = composed_objectlog_backend_group_commit(&root, gc_config()).expect("compose");
        backend.create_queue(qdef()).await.unwrap();
        let shard = shard();
        let now = ts(1);
        let pushes: Vec<_> = (0..5)
            .map(|_| backend.push(&shard, vec![PushSpec::default()], now, None))
            .collect();
        backend.flush_tick(1000 + 21).expect("flush");
        for p in pushes {
            p.await.unwrap();
        }
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 5);
    } // drop the backend → only the durable object log remains on disk

    // Reopen: recovery-on-open replays the manifest-committed segment tail into a fresh projection.
    let reopened =
        composed_objectlog_backend_group_commit(&root, gc_config()).expect("reopen compose");
    assert_eq!(
        reopened.metrics(&shard()).await.unwrap().pending,
        5,
        "reopen rebuilt the resident set from the durable group-commit log"
    );

    let _ = std::fs::remove_dir_all(&root);
}
