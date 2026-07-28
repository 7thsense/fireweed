//! Live shared-control-plane/object-log ownership conformance (TD-003 + TD-004).
//! Tests loud-skip unless both disposable Postgres and MinIO endpoints are configured.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use bytes::Bytes;
use fireweed_core::{
    EligibilityPolicy, LeaseToken, Metadata, OrderingMode, OwnerId, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    AcquireOutcome, ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneConfig,
    ControlPlaneStore, EngineError, EngineResult, LeaseState, OwnerEndpointAdvertisement,
    OwnerResolution, ProjectionRead, PushPort, PushSpec, QueueControlPlane, QueueKey, QueueLease,
};
use fireweed_objectlog::segmented::{
    BlobStore, FaultCutPoint, FaultHook, NamespacedBlobStore, S3BlobStore,
};
use fireweed_postgres::PostgresControlPlane;
use fireweed_resp::{RespHooks, RouteDecision};
use fireweed_server::{OwnershipRuntime, SegmentConfig, SegmentedObjectLogSqliteBackend};

static UNIQUE: AtomicU64 = AtomicU64::new(0);
const COORDINATION_TIMEOUT_ENV: &str = "FIREWEED_TEST_COORDINATION_TIMEOUT_SECS";
const MAX_COORDINATION_TIMEOUT_SECS: u64 = 86_400;

/// Optional operational deadlock watchdog around the complete live seam. Unset means no deadline.
/// Expiry is infrastructure-indeterminate, never a correctness or performance assertion.
fn parse_coordination_watchdog_timeout(value: Option<&str>) -> Result<Option<Duration>, String> {
    let Some(raw) = value else { return Ok(None) };
    if raw.is_empty() || raw.starts_with('0') || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "{COORDINATION_TIMEOUT_ENV} must match [1-9][0-9]*, got {raw:?}"
        ));
    }
    let seconds = raw.parse::<u64>().map_err(|_| {
        format!("{COORDINATION_TIMEOUT_ENV} is not representable as seconds: {raw:?}")
    })?;
    if seconds > MAX_COORDINATION_TIMEOUT_SECS {
        return Err(format!(
            "{COORDINATION_TIMEOUT_ENV} must be <= {MAX_COORDINATION_TIMEOUT_SECS}, got {seconds}"
        ));
    }
    Ok(Some(Duration::from_secs(seconds)))
}

fn coordination_watchdog_timeout() -> Option<Duration> {
    parse_coordination_watchdog_timeout(std::env::var(COORDINATION_TIMEOUT_ENV).ok().as_deref())
        .unwrap_or_else(|message| panic!("{message}"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StaleHandoffStage {
    AwaitingFaultEntry,
    FaultEntered,
    TakeoverEpochAcquired,
    ResumeSent,
    StaleResultFenced,
    FreshOwnerAcknowledged,
}

impl StaleHandoffStage {
    fn label(self) -> &'static str {
        match self {
            Self::AwaitingFaultEntry => "awaiting_fault_entry",
            Self::FaultEntered => "fault_entered",
            Self::TakeoverEpochAcquired => "takeover_epoch_acquired",
            Self::ResumeSent => "resume_sent",
            Self::StaleResultFenced => "stale_result_fenced",
            Self::FreshOwnerAcknowledged => "fresh_owner_acknowledged",
        }
    }
}

const REQUIRED_STALE_HANDOFF_STAGES: [StaleHandoffStage; 5] = [
    StaleHandoffStage::FaultEntered,
    StaleHandoffStage::TakeoverEpochAcquired,
    StaleHandoffStage::ResumeSent,
    StaleHandoffStage::StaleResultFenced,
    StaleHandoffStage::FreshOwnerAcknowledged,
];

fn record_stale_handoff_stage(stage: &Arc<Mutex<StaleHandoffStage>>, next: StaleHandoffStage) {
    *stage.lock().unwrap() = next;
    eprintln!("E2_FAILOVER_SEAM_STAGE stage={}", next.label());
}

#[derive(Debug, Eq, PartialEq)]
enum SeamSupervision {
    Completed,
    InfrastructureIndeterminate { last_stage: StaleHandoffStage },
}

async fn supervise_stale_handoff<F>(
    seam: F,
    watchdog: Option<Duration>,
    stage: Arc<Mutex<StaleHandoffStage>>,
) -> SeamSupervision
where
    F: Future<Output = ()>,
{
    let Some(deadline) = watchdog else {
        seam.await;
        return SeamSupervision::Completed;
    };
    match tokio::time::timeout(deadline, seam).await {
        Ok(()) => SeamSupervision::Completed,
        Err(_) => SeamSupervision::InfrastructureIndeterminate {
            last_stage: *stage.lock().unwrap(),
        },
    }
}

#[test]
fn coordination_watchdog_is_opt_in_bounded_and_uses_canonical_integer_grammar() {
    assert_eq!(parse_coordination_watchdog_timeout(None).unwrap(), None);
    assert_eq!(
        parse_coordination_watchdog_timeout(Some("17")).unwrap(),
        Some(Duration::from_secs(17))
    );
    for invalid in [
        "",
        "0",
        "017",
        "+17",
        "-1",
        "1.5",
        "slow",
        "86401",
        "18446744073709551615",
        "18446744073709551616",
    ] {
        assert!(
            parse_coordination_watchdog_timeout(Some(invalid)).is_err(),
            "invalid watchdog value {invalid:?} must fail closed"
        );
    }
}

#[test]
fn stale_handoff_supervision_classifies_expiry_and_names_every_semantic_stage() {
    assert_eq!(
        REQUIRED_STALE_HANDOFF_STAGES.map(StaleHandoffStage::label),
        [
            "fault_entered",
            "takeover_epoch_acquired",
            "resume_sent",
            "stale_result_fenced",
            "fresh_owner_acknowledged",
        ]
    );
    let runtime = test_runtime();
    assert_eq!(
        runtime.block_on(supervise_stale_handoff(
            async {},
            None,
            Arc::new(Mutex::new(StaleHandoffStage::AwaitingFaultEntry)),
        )),
        SeamSupervision::Completed,
        "unset watchdog must impose no wall-clock deadline"
    );
    let stage = Arc::new(Mutex::new(StaleHandoffStage::FaultEntered));
    let outcome = runtime.block_on(supervise_stale_handoff(
        std::future::pending(),
        Some(Duration::from_millis(1)),
        stage,
    ));
    assert_eq!(
        outcome,
        SeamSupervision::InfrastructureIndeterminate {
            last_stage: StaleHandoffStage::FaultEntered
        }
    );
}

fn identifier_component(label: &str) -> String {
    label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn live_env(label: &str) -> Option<(String, Arc<dyn BlobStore>, String)> {
    let Ok(pg) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!("{label} SKIPPED — set FIREWEED_PG_TEST_URL and FIREWEED_S3_TEST_ENDPOINT");
        return None;
    };
    let Ok(endpoint) = std::env::var("FIREWEED_S3_TEST_ENDPOINT") else {
        eprintln!("{label} SKIPPED — set FIREWEED_PG_TEST_URL and FIREWEED_S3_TEST_ENDPOINT");
        return None;
    };
    let bucket =
        std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed-test".into());
    let access =
        std::env::var("FIREWEED_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".into());
    let secret =
        std::env::var("FIREWEED_S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".into());
    let region = std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    let store = S3BlobStore::new(&endpoint, &bucket, &access, &secret, &region)
        .expect("construct MinIO client");
    let n = UNIQUE.fetch_add(1, Ordering::SeqCst);
    let namespace = format!(
        "fireweed/objectlog-shared-ownership/{}-{}-{n}",
        identifier_component(label),
        std::process::id()
    );
    Some((pg, Arc::new(store), namespace))
}

fn unique(label: &str) -> (String, QueueDefinition, QueueKey) {
    let n = UNIQUE.fetch_add(1, Ordering::SeqCst);
    let tenant = TenantId::new(format!("own-{label}-{}-{n}", std::process::id())).unwrap();
    let queue_id = QueueId::new(format!("queue-{label}-{}-{n}", std::process::id())).unwrap();
    let definition = QueueDefinition {
        tenant_id: tenant.clone(),
        queue_id: queue_id.clone(),
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
        emit_change_records: false,
    };
    let key = QueueKey::new(tenant, queue_id);
    (
        format!(
            "fireweed_own_{}_{}_{}",
            identifier_component(label),
            std::process::id(),
            n
        ),
        definition,
        key,
    )
}

fn owner(name: &str) -> OwnerId {
    OwnerId::new(name).unwrap()
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn projection_path(label: &str) -> String {
    let n = UNIQUE.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir()
        .join(format!(
            "fireweed-own-{label}-{}-{n}.sqlite",
            std::process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

fn backend(
    store: Arc<dyn BlobStore>,
    namespace: &str,
    label: &str,
) -> Arc<SegmentedObjectLogSqliteBackend> {
    let namespaced: Arc<dyn BlobStore> = Arc::new(
        NamespacedBlobStore::new(store, namespace).expect("construct unique object namespace"),
    );
    Arc::new(
        SegmentedObjectLogSqliteBackend::open_with_blob_store(
            namespaced,
            &projection_path(label),
            SegmentConfig::new(262_144, 5).unwrap(),
        )
        .unwrap(),
    )
}

fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn spec(payload: &str) -> PushSpec {
    PushSpec {
        client_item_key: None,
        priority: None,
        not_before: None,
        group_key: None,
        payload: Some(Bytes::from(payload.to_owned())),
        fields: BTreeMap::new(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity: None,
    }
}

struct PauseOnce {
    cut: FaultCutPoint,
    fault_entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
    fired: AtomicBool,
}

impl FaultHook for PauseOnce {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == self.cut && !self.fired.swap(true, Ordering::SeqCst) {
            self.fault_entered
                .send(())
                .map_err(|_| EngineError::Storage("fault-entry observer dropped".into()))?;
            self.resume
                .lock()
                .unwrap()
                .recv()
                .map_err(|_| EngineError::Storage("fault-resume sender dropped".into()))?;
        }
        Ok(())
    }
}

struct FailOnce {
    cut: FaultCutPoint,
    fired: AtomicBool,
}

impl FaultHook for FailOnce {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == self.cut && !self.fired.swap(true, Ordering::SeqCst) {
            return Err(EngineError::Storage(format!("fault at {cut:?}")));
        }
        Ok(())
    }
}

/// Delegating live Postgres control plane whose first compensation release fails. The underlying acquired
/// row remains durably PendingFence, which is the state restart safety must rely on.
struct FailReleaseControlPlane {
    inner: PostgresControlPlane,
    fail_release: AtomicBool,
}

impl QueueControlPlane for FailReleaseControlPlane {
    fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        self.inner.register_owner(owner, now)
    }
    fn advertise_owner_endpoint(
        &self,
        owner: &OwnerId,
        endpoint: &str,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.inner.advertise_owner_endpoint(owner, endpoint, now)
    }
    fn live_owner_endpoints(
        &self,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
        self.inner.live_owner_endpoints(now)
    }
    fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        self.inner.heartbeat(owner, now)
    }
    fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<OwnerResolution> {
        self.inner.resolve_queue_owner(queue, now)
    }
    fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        self.inner.acquire_queue_lease(queue, owner, now)
    }
    fn confirm_queue_lease_fence(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.inner
            .confirm_queue_lease_fence(queue, owner, expected_epoch, now)
    }
    fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.inner
            .renew_queue_lease(queue, owner, expected_epoch, now)
    }
    fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.inner
            .begin_drain(queue, expected_epoch, target_owner, now)
    }
    fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        if !self.fail_release.swap(true, Ordering::SeqCst) {
            return Err(EngineError::Storage("injected PG release failure".into()));
        }
        self.inner
            .release_queue_lease(queue, owner, expected_epoch, now)
    }
    fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
        self.inner.lease(queue)
    }
}

struct PauseAfterAcquireControlPlane {
    inner: PostgresControlPlane,
    entered: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl QueueControlPlane for PauseAfterAcquireControlPlane {
    fn register_owner(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        self.inner.register_owner(owner, now)
    }
    fn advertise_owner_endpoint(
        &self,
        owner: &OwnerId,
        endpoint: &str,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.inner.advertise_owner_endpoint(owner, endpoint, now)
    }
    fn live_owner_endpoints(
        &self,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<OwnerEndpointAdvertisement>> {
        self.inner.live_owner_endpoints(now)
    }
    fn heartbeat(&self, owner: &OwnerId, now: UtcTimestamp) -> EngineResult<()> {
        self.inner.heartbeat(owner, now)
    }
    fn resolve_queue_owner(
        &self,
        queue: &QueueKey,
        now: UtcTimestamp,
    ) -> EngineResult<OwnerResolution> {
        self.inner.resolve_queue_owner(queue, now)
    }
    fn acquire_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<AcquireOutcome> {
        let outcome = self.inner.acquire_queue_lease(queue, owner, now)?;
        self.entered
            .send(())
            .map_err(|_| EngineError::Unavailable)?;
        self.resume
            .lock()
            .expect("pause receiver poisoned")
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|_| EngineError::Unavailable)?;
        Ok(outcome)
    }
    fn confirm_queue_lease_fence(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.inner
            .confirm_queue_lease_fence(queue, owner, expected_epoch, now)
    }
    fn renew_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.inner
            .renew_queue_lease(queue, owner, expected_epoch, now)
    }
    fn begin_drain(
        &self,
        queue: &QueueKey,
        expected_epoch: u64,
        target_owner: &OwnerId,
        now: UtcTimestamp,
    ) -> EngineResult<QueueLease> {
        self.inner
            .begin_drain(queue, expected_epoch, target_owner, now)
    }
    fn release_queue_lease(
        &self,
        queue: &QueueKey,
        owner: &OwnerId,
        expected_epoch: u64,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.inner
            .release_queue_lease(queue, owner, expected_epoch, now)
    }
    fn lease(&self, queue: &QueueKey) -> EngineResult<QueueLease> {
        self.inner.lease(queue)
    }
}

#[test]
fn acquire_fences_manifest_before_serving() {
    let Some((pg, store, namespace)) = live_env("OBJECTLOG SHARED OWNERSHIP ACQUIRE") else {
        return;
    };
    let (schema, definition, queue) = unique("acquire");
    let config = ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 10_000,
    };
    let cp = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let backend = backend(store, &namespace, "acquire");
    let runtime = OwnershipRuntime::new(
        backend.clone(),
        cp.clone(),
        owner("owner-a"),
        "127.0.0.1:7101".into(),
    );
    runtime.register_owner(ts(0)).unwrap();
    let rt = test_runtime();
    rt.block_on(async {
        backend.create_queue(definition).await.unwrap();
        backend.fence_epoch(&queue, 0).await.unwrap();
        runtime.acquire_queue(&queue, ts(0)).await.unwrap();
        assert_eq!(backend.current_epoch(&queue).await.unwrap(), 1);
        assert_eq!(
            runtime
                .expected_epoch_for_write(&queue, ts(1), false)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(backend.fence_epoch(&queue, 1).await.unwrap(), 1);
        assert_eq!(
            backend.fence_epoch(&queue, 0).await,
            Err(EngineError::EpochFenced)
        );
    });
    let lease = cp.lease(&queue).unwrap();
    assert_eq!(lease.state, LeaseState::Assigned);
    assert_eq!(lease.assignment_epoch, 1);
}

#[test]
fn concurrent_acquires_publish_exactly_one_usable_owner() {
    let Some((pg, store, namespace)) = live_env("OBJECTLOG SHARED OWNERSHIP CONCURRENT ACQUIRE")
    else {
        return;
    };
    let (schema, definition, queue) = unique("concurrent");
    let config = ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 10_000,
    };
    let cp_a = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let cp_b = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let observer = PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap();
    let backend = backend(store, &namespace, "concurrent");
    let a = Arc::new(OwnershipRuntime::new(
        backend.clone(),
        cp_a,
        owner("owner-a"),
        "127.0.0.1:7101".into(),
    ));
    let b = Arc::new(OwnershipRuntime::new(
        backend.clone(),
        cp_b,
        owner("owner-b"),
        "127.0.0.1:7102".into(),
    ));
    a.register_owner(ts(0)).unwrap();
    b.register_owner(ts(0)).unwrap();
    let rt = test_runtime();
    rt.block_on(async {
        backend.create_queue(definition).await.unwrap();
        backend.fence_epoch(&queue, 0).await.unwrap();
        let (a_result, b_result) = tokio::join!(
            a.acquire_queue(&queue, ts(0)),
            b.acquire_queue(&queue, ts(0))
        );
        assert_ne!(a_result.is_ok(), b_result.is_ok());
        assert_eq!(backend.current_epoch(&queue).await.unwrap(), 1);
    });
    let lease = observer.lease(&queue).unwrap();
    assert_eq!(lease.state, LeaseState::Assigned);
    assert_eq!(lease.assignment_epoch, 1);
}

#[test]
fn pending_fence_gap_linearizes_old_commit_before_storage_fence_then_rejects_stale_retry() {
    let Some((pg, store, namespace)) = live_env("OBJECTLOG PENDING FENCE LINEARIZATION") else {
        return;
    };
    let (schema, definition, queue) = unique("pending-gap");
    let config = ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 10_000,
    };
    let cp_a = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let cp_b = Arc::new(PauseAfterAcquireControlPlane {
        inner: PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap(),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    });
    let observer = PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap();
    let a_backend = backend(store.clone(), &namespace, "pending-gap-a");
    let b_backend = backend(store, &namespace, "pending-gap-b");
    let a = Arc::new(OwnershipRuntime::new(
        a_backend.clone(),
        cp_a,
        owner("owner-a"),
        "127.0.0.1:7101".into(),
    ));
    let b = Arc::new(OwnershipRuntime::new(
        b_backend.clone(),
        cp_b,
        owner("owner-b"),
        "127.0.0.1:7102".into(),
    ));
    a.register_owner(ts(0)).unwrap();
    b.register_owner(ts(20)).unwrap();

    test_runtime().block_on(async {
        a_backend.create_queue(definition.clone()).await.unwrap();
        b_backend.create_queue(definition).await.unwrap();
        a_backend.fence_epoch(&queue, 0).await.unwrap();
        a.acquire_queue(&queue, ts(0)).await.unwrap();
        let flusher_a = a_backend.spawn_flusher();

        let acquiring = {
            let b = b.clone();
            let queue = queue.clone();
            tokio::spawn(async move { b.acquire_queue(&queue, ts(20)).await })
        };
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            tokio::task::block_in_place(|| observer.lease(&queue).unwrap()).state,
            LeaseState::PendingFence
        );
        let routing_key = format!("{}:{}", queue.tenant_id.as_str(), queue.queue_id.as_str());
        assert_eq!(
            a.route_command("GET", &[], routing_key.as_bytes(), ts(20), false)
                .await
                .unwrap(),
            RouteDecision::Unavailable
        );

        // This operation was admitted at epoch 1 before the handoff. With owner B still non-serving it may
        // linearize before the epoch-2 storage fence; no epoch-2 response can overlap it.
        a_backend
            .push(&queue, vec![spec("gap-prefix")], ts(20), Some(1))
            .await
            .unwrap();
        resume_tx.send(()).unwrap();
        acquiring.await.unwrap().unwrap();

        assert_eq!(
            a_backend
                .push(&queue, vec![spec("stale-retry")], ts(21), Some(1))
                .await,
            Err(EngineError::EpochFenced)
        );
        flusher_a.abort();
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 1);
        assert_eq!(b_backend.current_epoch(&queue).await.unwrap(), 2);
    });
}

#[test]
fn greater_epoch_owner_hydrates_snapshot_tail_before_serving() {
    let Some((pg, store, namespace)) = live_env("OBJECTLOG SHARED OWNERSHIP TAKEOVER HYDRATION")
    else {
        return;
    };
    let (schema, definition, queue) = unique("hydrate");
    let config = ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 10_000,
    };
    let cp_a = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let cp_b = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let observer = PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap();
    let a_backend = backend(store.clone(), &namespace, "hydrate-a");
    let b_backend = backend(store, &namespace, "hydrate-b");
    let a = OwnershipRuntime::new(
        a_backend.clone(),
        cp_a,
        owner("owner-a"),
        "127.0.0.1:7101".into(),
    );
    let b = OwnershipRuntime::new(
        b_backend.clone(),
        cp_b,
        owner("owner-b"),
        "127.0.0.1:7102".into(),
    );
    a.register_owner(ts(0)).unwrap();
    b.register_owner(ts(20)).unwrap();

    test_runtime().block_on(async {
        a_backend.create_queue(definition.clone()).await.unwrap();
        // Standby initialization happens before any owner writes, matching the multi-pod Helm profile.
        b_backend.create_queue(definition).await.unwrap();
        a_backend.fence_epoch(&queue, 0).await.unwrap();
        a.acquire_queue(&queue, ts(0)).await.unwrap();
        let flusher = a_backend.spawn_flusher();

        a_backend
            .push(
                &queue,
                vec![spec("prefix-1"), spec("prefix-2")],
                ts(1),
                Some(1),
            )
            .await
            .unwrap();
        // Materialize a durable standby snapshot/high-water, then deliberately leave it behind the log.
        b_backend
            .hydrate_projection_for_ownership(&queue)
            .await
            .unwrap();
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 2);
        a_backend
            .push(&queue, vec![spec("tail-1"), spec("tail-2")], ts(2), Some(1))
            .await
            .unwrap();
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 2);

        // Epoch-2 acquisition must replay the missing tail before Postgres publishes owner-b as serving.
        b.acquire_queue(&queue, ts(20)).await.unwrap();
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 4);
        let recovery = b_backend.recovery_stats(&queue).unwrap();
        assert!(recovery.snapshot_used);
        assert!(recovery.start_seq > 0);
        assert!(recovery.tail_replayed > 0);

        // The old epoch is rejected before mutation, and exact visible state remains four.
        assert_eq!(
            a_backend
                .push(&queue, vec![spec("stale")], ts(21), Some(1))
                .await,
            Err(EngineError::EpochFenced)
        );
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 4);

        let first = b_backend
            .claim(ClaimRequest {
                shard: queue.clone(),
                worker_id: WorkerId::new("worker-a").unwrap(),
                max_items: 4,
                lease_token: LeaseToken::new("lease-a").unwrap(),
                lease_expires_at: ts(80),
                now: ts(22),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: Some(2),
            })
            .await
            .unwrap();
        assert_eq!(first.items.len(), 4);
        let second = b_backend
            .claim(ClaimRequest {
                shard: queue.clone(),
                worker_id: WorkerId::new("worker-b").unwrap(),
                max_items: 4,
                lease_token: LeaseToken::new("lease-b").unwrap(),
                lease_expires_at: ts(80),
                now: ts(23),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: Some(2),
            })
            .await
            .unwrap();
        assert!(
            second.items.is_empty(),
            "no item may receive a double lease"
        );
        let metrics = b_backend.metrics(&queue).await.unwrap();
        assert_eq!(metrics.pending, 0);
        assert_eq!(metrics.leased, 4);
        flusher.abort();
    });
    let lease = observer.lease(&queue).unwrap();
    assert_eq!(lease.state, LeaseState::Assigned);
    assert_eq!(lease.active_owner_id, Some(owner("owner-b")));
    assert_eq!(lease.assignment_epoch, 2);
}

#[test]
fn greater_epoch_owner_rebuilds_projection_initialized_before_writes() {
    let Some((pg, store, namespace)) =
        live_env("OBJECTLOG SHARED OWNERSHIP EMPTY TAKEOVER REBUILD")
    else {
        return;
    };
    let (schema, definition, queue) = unique("empty_hydrate");
    let config = ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 10_000,
    };
    let cp_a = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let cp_b = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let observer = PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap();
    let a_backend = backend(store.clone(), &namespace, "empty-hydrate-a");
    let b_backend = backend(store, &namespace, "empty-hydrate-b");
    let a = OwnershipRuntime::new(
        a_backend.clone(),
        cp_a,
        owner("owner-a"),
        "127.0.0.1:7101".into(),
    );
    let b = OwnershipRuntime::new(
        b_backend.clone(),
        cp_b,
        owner("owner-b"),
        "127.0.0.1:7102".into(),
    );
    a.register_owner(ts(0)).unwrap();
    b.register_owner(ts(20)).unwrap();

    test_runtime().block_on(async {
        a_backend.create_queue(definition.clone()).await.unwrap();
        b_backend.create_queue(definition).await.unwrap();
        a_backend.fence_epoch(&queue, 0).await.unwrap();
        a.acquire_queue(&queue, ts(0)).await.unwrap();
        let flusher = a_backend.spawn_flusher();
        a_backend
            .push(
                &queue,
                vec![spec("one"), spec("two"), spec("three"), spec("four")],
                ts(1),
                Some(1),
            )
            .await
            .unwrap();
        assert_eq!(a_backend.metrics(&queue).await.unwrap().pending, 4);
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 0);

        b.acquire_queue(&queue, ts(20)).await.unwrap();
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 4);
        let recovery = b_backend.recovery_stats(&queue).unwrap();
        assert!(
            !recovery.snapshot_used,
            "empty standby must use safe genesis fallback"
        );
        assert_eq!(recovery.start_seq, 0);
        assert!(recovery.tail_replayed > 0);
        flusher.abort();
    });
    let lease = observer.lease(&queue).unwrap();
    assert_eq!(lease.state, LeaseState::Assigned);
    assert_eq!(lease.active_owner_id, Some(owner("owner-b")));
    assert_eq!(lease.assignment_epoch, 2);
}

#[test]
fn stale_append_paused_before_authority_cannot_survive_handoff() {
    // Parse explicit watchdog configuration before the live-environment loud skip. Invalid operator
    // configuration must fail closed even when Postgres/MinIO are absent.
    let coordination_watchdog = coordination_watchdog_timeout();
    let Some((pg, store, namespace)) = live_env("OBJECTLOG SHARED OWNERSHIP STALE APPEND RACE")
    else {
        return;
    };
    let (schema, definition, queue) = unique("race");
    let config = ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 10_000,
    };
    let cp_a = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let cp_b = Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let a_backend = backend(store.clone(), &namespace, "race-a");
    let b_backend = backend(store.clone(), &namespace, "race-b");
    let a = Arc::new(OwnershipRuntime::new(
        a_backend.clone(),
        cp_a,
        owner("owner-a"),
        "127.0.0.1:7101".into(),
    ));
    a.register_owner(ts(0)).unwrap();
    let b = OwnershipRuntime::new(
        b_backend.clone(),
        cp_b,
        owner("owner-b"),
        "127.0.0.1:7102".into(),
    );
    b.register_owner(ts(20)).unwrap();

    let (fault_entered_tx, fault_entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    a_backend.set_object_log_fault_hook(Some(Arc::new(PauseOnce {
        cut: FaultCutPoint::AfterManifestCandidateBeforeHead,
        fault_entered: fault_entered_tx,
        resume: Mutex::new(resume_rx),
        fired: AtomicBool::new(false),
    })));
    let reopened = backend(store, &namespace, "race-reopen");
    let seam_stage = Arc::new(Mutex::new(StaleHandoffStage::AwaitingFaultEntry));
    let supervised_stage = seam_stage.clone();
    test_runtime().block_on(async {
        let seam = async {
            a_backend.create_queue(definition.clone()).await.unwrap();
            b_backend.create_queue(definition.clone()).await.unwrap();
            a_backend.fence_epoch(&queue, 0).await.unwrap();
            a.acquire_queue(&queue, ts(0)).await.unwrap();
            let flusher_a = a_backend.spawn_flusher();
            let stale_push = {
                let backend = a_backend.clone();
                let queue = queue.clone();
                tokio::spawn(async move {
                    backend
                        .push(&queue, vec![spec("stale")], ts(1), Some(1))
                        .await
                })
            };
            tokio::task::spawn_blocking(move || fault_entered_rx.recv())
                .await
                .unwrap()
                .unwrap();
            record_stale_handoff_stage(&seam_stage, StaleHandoffStage::FaultEntered);

            b.acquire_queue(&queue, ts(20)).await.unwrap();
            record_stale_handoff_stage(&seam_stage, StaleHandoffStage::TakeoverEpochAcquired);

            resume_tx.send(()).unwrap();
            record_stale_handoff_stage(&seam_stage, StaleHandoffStage::ResumeSent);

            let stale = stale_push.await.unwrap();
            assert_eq!(stale, Err(EngineError::EpochFenced));
            record_stale_handoff_stage(&seam_stage, StaleHandoffStage::StaleResultFenced);
            flusher_a.abort();

            let flusher_b = b_backend.spawn_flusher();
            b_backend
                .push(&queue, vec![spec("fresh")], ts(21), Some(2))
                .await
                .unwrap();
            record_stale_handoff_stage(&seam_stage, StaleHandoffStage::FreshOwnerAcknowledged);
            flusher_b.abort();
            reopened.create_queue(definition).await.unwrap();
            assert_eq!(reopened.metrics(&queue).await.unwrap().pending, 1);
        };
        match supervise_stale_handoff(seam, coordination_watchdog, supervised_stage).await {
            SeamSupervision::Completed => {}
            SeamSupervision::InfrastructureIndeterminate { last_stage } => panic!(
                "E2_FAILOVER_INFRASTRUCTURE_INDETERMINATE classification=coordination_watchdog \
                 retryable=true release_bar=false last_stage={} watchdog_seconds={}",
                last_stage.label(),
                coordination_watchdog.unwrap().as_secs()
            ),
        }
    });
}

#[test]
fn failed_fence_and_failed_compensation_remain_non_serving_across_restart() {
    let Some((pg, store, namespace)) = live_env("OBJECTLOG SHARED OWNERSHIP FAILURE RECOVERY")
    else {
        return;
    };
    let (schema, definition, queue) = unique("failure");
    let config = ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 10_000,
    };
    let backend = backend(store, &namespace, "failure");
    let failing_cp = Arc::new(FailReleaseControlPlane {
        inner: PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap(),
        fail_release: AtomicBool::new(false),
    });
    let first = OwnershipRuntime::new(
        backend.clone(),
        failing_cp,
        owner("stable-owner"),
        "127.0.0.1:7101".into(),
    );
    first.register_owner(ts(0)).unwrap();
    let restarted_cp =
        Arc::new(PostgresControlPlane::connect_in_schema(&pg, &schema, config).unwrap());
    let restarted = OwnershipRuntime::new(
        backend.clone(),
        restarted_cp.clone(),
        owner("stable-owner"),
        "127.0.0.1:7101".into(),
    );
    restarted.register_owner(ts(1)).unwrap();
    let rt = test_runtime();
    rt.block_on(async {
        backend.create_queue(definition).await.unwrap();
        backend.fence_epoch(&queue, 0).await.unwrap();
        backend.set_object_log_fault_hook(Some(Arc::new(FailOnce {
            cut: FaultCutPoint::BeforeAuthorityHeadUpdate,
            fired: AtomicBool::new(false),
        })));
        assert!(matches!(
            first.acquire_queue(&queue, ts(0)).await,
            Err(EngineError::Storage(_))
        ));
    });

    let pending = restarted_cp.lease(&queue).unwrap();
    assert_eq!(pending.state, LeaseState::PendingFence);
    assert_eq!(pending.assignment_epoch, 1);
    let routing_key = format!("{}:{}", queue.tenant_id.as_str(), queue.queue_id.as_str());
    rt.block_on(async {
        assert_eq!(
            restarted
                .route_command("GET", &[], routing_key.as_bytes(), ts(1), false)
                .await
                .unwrap(),
            RouteDecision::Unavailable
        );
        assert_eq!(
            restarted.acquire_queue(&queue, ts(1)).await,
            Err(EngineError::Unavailable)
        );
    });
    restarted.register_owner(ts(20)).unwrap();
    rt.block_on(async {
        restarted.acquire_queue(&queue, ts(20)).await.unwrap();
        assert_eq!(backend.current_epoch(&queue).await.unwrap(), 2);
    });
    let recovered = restarted_cp.lease(&queue).unwrap();
    assert_eq!(recovered.state, LeaseState::Assigned);
    assert_eq!(recovered.assignment_epoch, 2);
}
