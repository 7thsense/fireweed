//! B2a (ADR-009 / TD-003 In-Process Library Owner-Runtime): the library as a **coordinated owner**.
//!
//! A `Pqueue::with_control_plane` handle acquires-and-fences before each queue-addressed op and stamps its
//! cached acquire-time epoch, so over a shared backend a superseded instance self-fences on the data path.
//! The sole-owner default (`Pqueue::new`) is unaffected — proven by the facade suite.

use std::sync::Arc;

use pqueue::{NewItem, Pqueue};
use pqueue_core::{
    EligibilityPolicy, OrderingMode, OwnerId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId,
};
use pqueue_engine::{
    Clock, ControlPlaneConfig, EngineError, InMemoryControlPlane, QueueControlPlane, QueueKey,
};
use pqueue_memory::{ManualClock, MemoryBackend};

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

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
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
    }
}

fn item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

/// A coordinated owner acquires + fences on first use and operates normally; the queue is served under a
/// real (epoch >= 1) lease, not the degenerate sole-owner path.
#[tokio::test]
async fn coordinated_owner_acquires_and_operates() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let cp: Arc<dyn QueueControlPlane> =
        Arc::new(InMemoryControlPlane::new(ControlPlaneConfig::default()));
    let pq = Pqueue::with_control_plane(
        backend.clone(),
        clock.clone(),
        OwnerId::new("owner-A").unwrap(),
        cp.clone(),
    );

    pq.create_queue(qdef()).await.unwrap();
    pq.push(&qkey(), item(5)).await.unwrap();
    assert_eq!(pq.metrics(&qkey()).await.unwrap().pending, 1);

    // The control plane records this instance as the live active owner at a granted (>= 1) epoch.
    let res = cp.resolve_queue_owner(&qkey(), clock.now()).unwrap();
    assert_eq!(res.active_owner.as_ref().map(|o| o.as_str()), Some("owner-A"));
    assert!(res.assignment_epoch.unwrap_or(0) >= 1);
}

/// Two coordinated instances over one shared backend + control plane: once A's lease expires and B reclaims
/// the queue at a greater epoch, A's NEXT data-plane op is `EpochFenced` — it stamps its cached (now stale)
/// acquire-time epoch, so it fails closed independent of whether it has noticed the handoff (ADR-009 L4 /
/// TD-003 data-path fail-closed).
#[tokio::test]
async fn superseded_owner_is_fenced_on_data_path() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let cp: Arc<dyn QueueControlPlane> =
        Arc::new(InMemoryControlPlane::new(ControlPlaneConfig::default()));
    let a = Pqueue::with_control_plane(
        backend.clone(),
        clock.clone(),
        OwnerId::new("owner-A").unwrap(),
        cp.clone(),
    );
    let b = Pqueue::with_control_plane(
        backend.clone(),
        clock.clone(),
        OwnerId::new("owner-B").unwrap(),
        cp.clone(),
    );

    a.create_queue(qdef()).await.unwrap();
    // A acquires Q (epoch e1) and pushes; its session is cached.
    a.push(&qkey(), item(5)).await.unwrap();

    // While A holds a LIVE lease, B cannot take the queue (single active lease) — owned elsewhere.
    assert!(
        matches!(b.push(&qkey(), item(6)).await, Err(EngineError::Forbidden(_))),
        "a peer cannot operate on a queue a live owner holds"
    );

    // Advance past A's lease TTL (default 15s) so the lease is reclaimable.
    clock.set(20);
    // B reclaims Q at a strictly-greater epoch (e2) and pushes — this advances the storage fence epoch.
    b.push(&qkey(), item(7)).await.unwrap();

    // A still holds its cached session (e1); its next op stamps the stale epoch and is fenced at commit,
    // regardless of the fact that A never re-resolved.
    assert!(
        matches!(a.push(&qkey(), item(8)).await, Err(EngineError::EpochFenced)),
        "a superseded owner must self-fence on the data path"
    );
    // B (the current owner) keeps operating.
    b.push(&qkey(), item(9)).await.unwrap();
}
