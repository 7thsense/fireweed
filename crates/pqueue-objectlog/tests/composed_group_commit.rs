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
    ClaimCompatibility, ClaimPort, ClaimRequest, ComposedBackend, ControlPlaneStore, EngineError,
    FinalizeKind, FinalizeOutcome, FinalizePort, ProjectionRead, PushPort, PushSpec, QueueKey,
};
use pqueue_objectlog::{
    ObjectLog, SegmentConfig, composed_objectlog_backend, composed_objectlog_backend_group_commit,
};
use pqueue_sqlite::HybridProjectionStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!("pqueue-objlog-gc-{tag}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn tmp_db(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "pqueue-objlog-gc-{tag}-{}-{n}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
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
        emit_change_records: true,
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

fn hybrid_backend(
    root: &std::path::Path,
    projection: &std::path::Path,
) -> ComposedBackend<ObjectLog, HybridProjectionStore, pqueue_engine::InProcessControlPlane> {
    ComposedBackend::new(
        ObjectLog::open_group_commit(root, gc_config()).expect("open object log"),
        HybridProjectionStore::open(projection.to_str().expect("utf8 projection path"))
            .expect("open hybrid projection"),
        pqueue_engine::InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover()
    .expect("recover hybrid backend")
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
async fn single_command_segment_is_only_on_distinguishable_sync_path() {
    let root = tmp_root("sync-single");
    let backend = composed_objectlog_backend(&root).expect("compose sync path");
    backend.create_queue(qdef()).await.unwrap();
    let shard = shard();

    assert!(
        !backend.group_commit_enabled(),
        "the force-seal append path is an explicit non-group-commit mode"
    );
    let ids = backend
        .push(&shard, vec![PushSpec::default()], ts(1), None)
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(
        backend.with_log(|l| l.counters()).group_commit_batches,
        vec![1],
        "single-command object segments are confined to the distinguishable sync path"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn claim_and_finalize_normal_traffic_batch_before_ack() {
    let root = tmp_root("claim");
    let backend = composed_objectlog_backend_group_commit(&root, gc_config()).expect("compose");
    backend.create_queue(qdef()).await.unwrap();
    let shard = shard();
    let now = ts(1);

    const N: usize = 4;
    let pushes: Vec<_> = (0..N)
        .map(|_| backend.push(&shard, vec![PushSpec::default()], now, None))
        .collect();
    assert_eq!(
        backend.with_log(|l| l.counters()).segments_sealed,
        0,
        "still buffered"
    );

    // The first claim force-seals the buffered pushes so selection observes durable pushed items, but the
    // claim command itself remains unacked until a later object-log group-commit seal.
    let claim1 = backend.claim(ClaimRequest {
        eligibility_time: None,
        shard: shard.clone(),
        worker_id: WorkerId::new("w1").unwrap(),
        max_items: 2,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(3_600),
        now,
        compatibility: ClaimCompatibility::default(),
        expected_epoch: None,
    });
    for p in pushes {
        assert_eq!(p.await.unwrap().len(), 1);
    }
    assert_eq!(
        backend.with_log(|l| l.counters()).group_commit_batches,
        vec![N]
    );

    // A second normal claim starts before the first claim is durable. The in-flight claim guard excludes the
    // first claim's candidates, so both claim commands can seal together without double-leasing.
    let claim2 = backend.claim(ClaimRequest {
        eligibility_time: None,
        shard: shard.clone(),
        worker_id: WorkerId::new("w2").unwrap(),
        max_items: 2,
        lease_token: LeaseToken::new("lease-2").unwrap(),
        lease_expires_at: ts(3_600),
        now,
        compatibility: ClaimCompatibility::default(),
        expected_epoch: None,
    });
    assert_eq!(
        backend.with_log(|l| l.counters()).group_commit_batches,
        vec![N],
        "normal claims are buffered, not acknowledged as one-command segments"
    );

    backend.flush_tick(1000 + 21).expect("flush claims");
    let claimed1 = claim1.await.unwrap();
    let claimed2 = claim2.await.unwrap();
    assert_eq!(claimed1.items.len(), 2);
    assert_eq!(claimed2.items.len(), 2);
    assert_ne!(claimed1.items[0].item_id, claimed2.items[0].item_id);

    let mut claimed_ids: Vec<ItemId> = claimed1.items.iter().map(|c| c.item_id).collect();
    claimed_ids.extend(claimed2.items.iter().map(|c| c.item_id));
    claimed_ids.sort();
    claimed_ids.dedup();
    assert_eq!(claimed_ids.len(), N);
    assert_eq!(
        backend.with_log(|l| l.counters()).group_commit_batches,
        vec![N, 2],
        "both normal claim commands sealed in one durable group"
    );

    let finalizes: Vec<_> = claimed_ids
        .iter()
        .map(|id| {
            backend.finalize(
                &shard,
                vec![FinalizeOutcome::new(*id, FinalizeKind::Complete)],
                now,
                None,
            )
        })
        .collect();
    assert_eq!(
        backend.with_log(|l| l.counters()).group_commit_batches,
        vec![N, 2],
        "normal finalizes are buffered until group commit"
    );
    assert_eq!(backend.metrics(&shard).await.unwrap().complete, 0);

    backend.flush_tick(1000 + 21).expect("flush finalizes");
    for finalize in finalizes {
        finalize.await.unwrap();
    }

    let m = backend.metrics(&shard).await.unwrap();
    assert_eq!(m.pending, 0);
    assert_eq!(m.complete, N as u64);

    let counters = backend.with_log(|l| l.counters());
    assert_eq!(counters.group_commit_batches, vec![N, 2, N]);
    assert!(
        counters.mean_batch_size() > 1.0,
        "mean_commands_per_segment must prove batched normal traffic"
    );
    assert!(
        counters.max_batch_size() > 1,
        "max_commands_per_segment must prove batched normal traffic"
    );
    assert!(
        counters.group_commit_batches.iter().all(|&n| n > 1),
        "normal push/claim/finalize traffic under load must not create one-command segments"
    );

    // A later claim selects nothing (the candidates were leased then completed — never double-leased).
    let again = backend
        .claim(ClaimRequest {
            eligibility_time: None,
            shard: shard.clone(),
            worker_id: WorkerId::new("w3").unwrap(),
            max_items: 10,
            lease_token: LeaseToken::new("lease-3").unwrap(),
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
async fn objectlog_hybrid_force_seals_before_claim_and_fences_stale_epoch() {
    let root = tmp_root("hybrid-force-seal");
    let projection = tmp_db("hybrid-force-seal");
    let backend = hybrid_backend(&root, &projection);
    backend.create_queue(qdef()).await.unwrap();
    let shard = shard();
    let epoch = backend.current_epoch(&shard).await.unwrap();

    let mut buffered =
        Box::pin(backend.push(&shard, vec![PushSpec::default()], ts(0), Some(epoch)));
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    assert!(
        matches!(buffered.as_mut().poll(&mut cx), std::task::Poll::Pending),
        "large-segment push buffers until the ordered barrier force-seals"
    );

    let claimed = backend.claim(ClaimRequest {
        eligibility_time: None,
        shard: shard.clone(),
        worker_id: WorkerId::new("hybrid-claimer").unwrap(),
        max_items: 1,
        lease_token: LeaseToken::new("hybrid-force-seal-lease").unwrap(),
        lease_expires_at: ts(60),
        now: ts(1),
        compatibility: ClaimCompatibility::default(),
        expected_epoch: Some(epoch),
    });
    backend.flush_tick(1000 + 21).expect("flush claim");
    let claimed = claimed.await.unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "claim selection observes the force-sealed buffered push"
    );
    let pushed = buffered.await.unwrap();
    assert_eq!(claimed.items[0].item_id, pushed[0]);

    let superseding_epoch = backend.acquire_epoch(&shard).await.unwrap();
    assert!(superseding_epoch > epoch);
    let stale = backend
        .push_with_request_id(
            &shard,
            pqueue_core::RequestId::new("stale-writer").unwrap(),
            vec![PushSpec::default()],
            ts(2),
            Some(epoch),
        )
        .await
        .unwrap_err();
    assert_eq!(stale, EngineError::EpochFenced);

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&projection);
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
