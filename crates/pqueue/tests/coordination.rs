//! B2a (ADR-009 / TD-003 In-Process Library Owner-Runtime): the library as a **coordinated owner**.
//!
//! A `Pqueue::with_control_plane` handle acquires-and-fences before each queue-addressed op and stamps its
//! cached acquire-time epoch, so over a shared backend a superseded instance self-fences on the data path.
//! The sole-owner default (`Pqueue::new`) is unaffected — proven by the facade suite.

use std::sync::Arc;

use pqueue::{NewItem, Ownership, Pqueue};
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
    let pq = Pqueue::with_control_plane_in_process(
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
    let a = Pqueue::with_control_plane_in_process(
        backend.clone(),
        clock.clone(),
        OwnerId::new("owner-A").unwrap(),
        cp.clone(),
    );
    let b = Pqueue::with_control_plane_in_process(
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
    // The fence dropped A's stale session; A's NEXT op re-resolves and discovers the queue is owned
    // elsewhere (target-affinity: A is no longer the rendezvous target), and `ownership` names B.
    assert!(
        matches!(
            a.push(&qkey(), item(10)).await,
            Err(EngineError::Forbidden(_))
        ),
        "a fenced owner re-resolves to owned-elsewhere"
    );
    assert!(
        matches!(a.ownership(&qkey()).await.unwrap(), Ownership::Elsewhere { owner, .. } if owner.as_str() == "owner-B"),
        "ownership names the current owner B as the redirect target"
    );
    // B (the current owner) keeps operating.
    b.push(&qkey(), item(9)).await.unwrap();
}

/// `ownership` is the value form of the redirect (ADR-009 L5): a sole-owner handle is always `Mine`; a
/// coordinated handle reports `Mine` for the queues it owns and `Unowned` before any acquire.
#[tokio::test]
async fn ownership_value_form() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));

    // Sole-owner handle: always Mine.
    let sole = Pqueue::new(backend.clone(), clock.clone());
    assert_eq!(
        sole.ownership(&qkey()).await.unwrap(),
        Ownership::Mine { epoch: None }
    );

    // Coordinated handle: Unowned before any op, Mine after acquiring.
    let cp: Arc<dyn QueueControlPlane> =
        Arc::new(InMemoryControlPlane::new(ControlPlaneConfig::default()));
    let a = Pqueue::with_control_plane_in_process(
        backend.clone(),
        clock.clone(),
        OwnerId::new("owner-A").unwrap(),
        cp.clone(),
    );
    a.create_queue(qdef()).await.unwrap();
    assert_eq!(a.ownership(&qkey()).await.unwrap(), Ownership::Unowned);
    a.push(&qkey(), item(5)).await.unwrap();
    assert!(matches!(
        a.ownership(&qkey()).await.unwrap(),
        Ownership::Mine { epoch: Some(e) } if e >= 1
    ));
}

/// Drain split (TD-003 §Graceful Drain): once a queue is `Draining` (observed on the renew loop), the owner
/// refuses a NEW claim with a retryable `Unavailable`, but keeps serving in-flight ops (finalize) + pushes.
#[tokio::test]
async fn draining_owner_refuses_new_claim_but_serves_in_flight() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let cp: Arc<dyn QueueControlPlane> =
        Arc::new(InMemoryControlPlane::new(ControlPlaneConfig::default()));
    let a = Pqueue::with_control_plane_in_process(
        backend.clone(),
        clock.clone(),
        OwnerId::new("owner-A").unwrap(),
        cp.clone(),
    );
    a.create_queue(qdef()).await.unwrap();
    a.push(&qkey(), item(5)).await.unwrap(); // A acquires the queue (epoch 1)
    let claimed = a.claim(&qkey(), 10, 1_000).await.unwrap();
    assert_eq!(claimed.len(), 1, "A claims normally before drain");
    let leased = claimed[0].item_id.clone();

    // An operator begins draining the queue toward a new target owner B.
    cp.register_owner(&OwnerId::new("owner-B").unwrap(), clock.now())
        .unwrap();
    cp.begin_drain(&qkey(), 1, &OwnerId::new("owner-B").unwrap(), clock.now())
        .unwrap();
    // A observes the drain on its renew loop.
    a.renew_owned().unwrap();

    // A now REFUSES a new claim with a retryable Unavailable...
    assert!(
        matches!(a.claim(&qkey(), 10, 1_000).await, Err(EngineError::Unavailable)),
        "a draining owner refuses a new claim"
    );
    // ...but still serves in-flight work: finalizing the already-leased item, and pushes, continue.
    a.ack(&qkey(), [leased]).await.unwrap();
    a.push(&qkey(), item(6)).await.unwrap();
    assert_eq!(a.metrics(&qkey()).await.unwrap().complete, 1, "in-flight finalize served during drain");
}

/// Runtime-refuse (ADR-009 D5 / N4a / OD-2): the durable multi-instance constructor REJECTS a control plane
/// that does not present the atomic acquire->fence capability — the in-memory reference plane is
/// single-process only, so passing an instance id with it is a misconfiguration, not a silent footgun.
#[tokio::test]
async fn durable_multi_instance_refuses_non_binding_control_plane() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(ManualClock::at(0));
    let cp: Arc<dyn QueueControlPlane> =
        Arc::new(InMemoryControlPlane::new(ControlPlaneConfig::default()));
    // The in-memory control plane does not bind the storage epoch (binds_storage_epoch == false).
    assert!(!cp.binds_storage_epoch());
    let refused =
        Pqueue::with_control_plane(backend, clock, OwnerId::new("inst-1").unwrap(), cp);
    assert!(
        matches!(refused, Err(EngineError::Invalid(_))),
        "durable multi-instance must refuse a non-atomic-acquire control plane"
    );
}
