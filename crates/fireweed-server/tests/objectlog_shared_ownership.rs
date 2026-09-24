//! TP-002 E2 ownership seams for the public S3 object-log × Turso cell.
//!
//! Each owner is its own `ObjectLogEngineStore` over one shared S3 namespace plus a pod-local Turso
//! projection, coordinated by a live Postgres control plane. This is the multi-pod Helm shape the
//! kind failover proof (`scripts/perf/tp002-e2-failover-kind.sh`) runs these seams against.
//!
//! Live fixtures are fail-closed: `FIREWEED_PG_TEST_URL` is required, and S3 comes from
//! `fireweed_objectlog::shared_s3_test_env` (`FIREWEED_S3_TEST_*`, else a local RustFS).
#![cfg(feature = "postgres")]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use fireweed::turso_compose::{DerivedObjectLogTursoBackend, open_turso_projection_async};
use fireweed_core::{
    EligibilityPolicy, LeaseToken, OrderingMode, OwnerId, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneConfig, ControlPlaneStore, LeaseState,
    ProjectionRead, PushPort, PushSpec, QueueControlPlane, QueueKey,
};
use fireweed_objectlog::head_publish_pause::HeadPublishPause;
use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment, shared_s3_test_env};
use fireweed_postgres::PostgresControlPlane;
use fireweed_server::OwnershipRuntime;

static UNIQUE: AtomicU64 = AtomicU64::new(0);

const CONTROL_PLANE: ControlPlaneConfig = ControlPlaneConfig {
    heartbeat_ttl_ms: 5_000,
    lease_ttl_ms: 10_000,
};

fn next() -> u64 {
    UNIQUE.fetch_add(1, Ordering::SeqCst)
}

fn pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)")
}

/// A fresh Postgres schema, S3 namespace and queue for one seam.
struct Seam {
    schema: String,
    namespace: String,
    definition: QueueDefinition,
    queue: QueueKey,
}

fn seam(label: &str) -> Seam {
    let n = next();
    let pid = std::process::id();
    let tenant = TenantId::new(format!("e2-{label}-{pid}-{n}")).unwrap();
    let queue_id = QueueId::new(format!("queue-{label}-{pid}-{n}")).unwrap();
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
    Seam {
        schema: format!("fireweed_e2_{label}_{pid}_{n}"),
        namespace: format!("fireweed-e2-seams/{label}-{pid}-{n}"),
        queue: QueueKey::new(tenant, queue_id),
        definition,
    }
}

fn owner(name: &str) -> OwnerId {
    OwnerId::new(name).unwrap()
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn spec(payload: &str) -> PushSpec {
    PushSpec {
        payload: Some(Bytes::from(payload.to_owned())),
        ..Default::default()
    }
}

/// The Postgres control plane is a synchronous client: build, use and drop it off the runtime.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn control_plane(schema: &str) -> Arc<PostgresControlPlane> {
    Arc::new(PostgresControlPlane::connect_in_schema(&pg_url(), schema, CONTROL_PLANE).unwrap())
}

/// One pod: its own handle on the shared S3 log and its own Turso projection file. A `pause`
/// wraps this pod's log I/O so one of its head publishes can be held.
async fn pod(
    namespace: &str,
    node_id: u8,
    pause: Option<&Arc<HeadPublishPause>>,
) -> Arc<DerivedObjectLogTursoBackend> {
    let s3 = shared_s3_test_env();
    let meta_prefix = format!("{namespace}/fwmeta/");
    let manifest_prefix = format!("{meta_prefix}manifest/");
    let log = ObjectLogEngineStore::open_s3_with_prefixes_wrapping_blob(
        &s3.endpoint,
        &s3.region,
        &s3.bucket,
        &s3.access_key,
        &s3.secret_key,
        format!("{namespace}/fwlog/"),
        meta_prefix,
        flush_config_from_segment(262_144, 5),
        |blob| match pause {
            Some(pause) => pause.wrap(blob, manifest_prefix),
            None => blob,
        },
    )
    .await
    .expect("open shared S3 object log");
    let path: PathBuf = std::env::temp_dir().join(format!(
        "fireweed-e2-seam-{}-{}-node{node_id}.db",
        std::process::id(),
        next()
    ));
    let projection = open_turso_projection_async(&path)
        .await
        .expect("open pod-local Turso projection");
    Arc::new(
        DerivedObjectLogTursoBackend::from_log_and_projection(log, projection, path, node_id, None)
            .await
            .expect("compose S3 object log × Turso"),
    )
}

fn claim(queue: &QueueKey, worker: &str, now: i64, epoch: u64) -> ClaimRequest {
    ClaimRequest {
        shard: queue.clone(),
        worker_id: WorkerId::new(worker).unwrap(),
        max_items: 4,
        lease_token: LeaseToken::new(format!("lease-{worker}")).unwrap(),
        lease_expires_at: ts(80),
        now: ts(now),
        eligibility_time: None,
        compatibility: ClaimCompatibility::default(),
        expected_epoch: Some(epoch),
        request_id: None,
    }
}

/// A standby pod that initialized its projection before the owner wrote the tail must replay that
/// tail from the shared S3 log when it takes over at a greater epoch, before it serves. The old
/// owner's writes are then fenced, and the new owner leases every item exactly once.
#[test]
fn greater_epoch_owner_hydrates_snapshot_tail_before_serving() {
    let Seam {
        schema,
        namespace,
        definition,
        queue,
    } = seam("hydrate");
    let observer = control_plane(&schema);
    let cp_a = control_plane(&schema);
    let cp_b = control_plane(&schema);
    let rt = runtime();
    let (a_backend, b_backend) = rt.block_on(async {
        // The standby opens before any owner write, as a replica does under the multi-pod profile.
        (
            pod(&namespace, 1, None).await,
            pod(&namespace, 2, None).await,
        )
    });
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

    let b_epoch = rt.block_on(async {
        a_backend.create_queue(definition.clone()).await.unwrap();
        b_backend.create_queue(definition.clone()).await.unwrap();
        a.acquire_queue(&queue, ts(0)).await.unwrap();
        let a_epoch = a_backend.current_epoch(&queue).await.unwrap();
        assert_eq!(a_epoch, 1, "first owner serves at epoch one");

        a_backend
            .push(
                &queue,
                vec![spec("prefix-1"), spec("prefix-2")],
                ts(1),
                Some(a_epoch),
            )
            .await
            .unwrap();
        // Materialize the standby's projection, then deliberately leave it behind the log.
        b_backend
            .hydrate_projection_for_ownership(&queue)
            .await
            .unwrap();
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 2);
        let snapshot = b_backend
            .projection()
            .writer_recovery_high_water(&queue)
            .await
            .unwrap()
            .expect("standby projection has a durable high-water");
        a_backend
            .push(
                &queue,
                vec![spec("tail-1"), spec("tail-2")],
                ts(2),
                Some(a_epoch),
            )
            .await
            .unwrap();
        assert_eq!(
            b_backend.metrics(&queue).await.unwrap().pending,
            2,
            "the standby does not apply the owner's tail by itself"
        );

        // The greater-epoch acquire must replay the missing tail before Postgres publishes owner-b.
        b.acquire_queue(&queue, ts(20)).await.unwrap();
        let b_epoch = b_backend.current_epoch(&queue).await.unwrap();
        assert!(
            b_epoch > a_epoch,
            "takeover epoch {b_epoch} must exceed {a_epoch}"
        );
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 4);
        let hydrated = b_backend
            .projection()
            .writer_recovery_high_water(&queue)
            .await
            .unwrap()
            .expect("hydrated projection has a durable high-water");
        assert!(
            hydrated.sequence > snapshot.sequence,
            "takeover replayed the log tail past the standby snapshot ({} -> {})",
            snapshot.sequence,
            hydrated.sequence
        );

        // The old owner's write is rejected, and the new owner's view is unchanged.
        let stale = a_backend
            .push(&queue, vec![spec("stale")], ts(21), Some(a_epoch))
            .await;
        assert!(
            stale.is_err(),
            "a stale-epoch push must not be acknowledged"
        );
        eprintln!("E2_FAILOVER_SEAM stale_push_error={:?}", stale.unwrap_err());
        assert_eq!(b_backend.metrics(&queue).await.unwrap().pending, 4);

        let first = b_backend
            .claim(claim(&queue, "worker-a", 22, b_epoch))
            .await
            .unwrap();
        assert_eq!(first.items.len(), 4);
        let second = b_backend
            .claim(claim(&queue, "worker-b", 23, b_epoch))
            .await
            .unwrap();
        assert!(
            second.items.is_empty(),
            "no item may receive a double lease"
        );
        let metrics = b_backend.metrics(&queue).await.unwrap();
        assert_eq!((metrics.pending, metrics.leased), (0, 4));
        b_epoch
    });

    let lease = observer.lease(&queue).unwrap();
    assert_eq!(lease.state, LeaseState::Assigned);
    assert_eq!(lease.active_owner_id, Some(owner("owner-b")));
    assert_eq!(lease.assignment_epoch, b_epoch);
}

const COORDINATION_TIMEOUT_ENV: &str = "FIREWEED_TEST_COORDINATION_TIMEOUT_SECS";

/// Opt-in watchdog for the kind failover lane: a live-fixture stall is reported as retryable
/// infrastructure, never as a product pass. Invalid configuration fails closed.
fn coordination_watchdog() -> Option<Duration> {
    let value = std::env::var(COORDINATION_TIMEOUT_ENV).ok()?;
    let seconds: u64 = value
        .parse()
        .ok()
        .filter(|seconds| (1..=86_400).contains(seconds) && !value.starts_with('0'))
        .unwrap_or_else(|| {
            panic!("{COORDINATION_TIMEOUT_ENV} must match [1-9][0-9]* and be <= 86400")
        });
    Some(Duration::from_secs(seconds))
}

fn stage(current: &Mutex<&'static str>, next: &'static str) {
    *current.lock().unwrap() = next;
    eprintln!("E2_FAILOVER_SEAM_STAGE stage={next}");
}

/// An append the old owner started before a takeover, and whose head publish lands after it, must
/// be rejected: it never becomes visible, and only the new owner's write survives a reopen.
#[test]
fn stale_append_paused_before_authority_cannot_survive_handoff() {
    // Parse the watchdog before touching live fixtures so bad configuration fails closed.
    let watchdog = coordination_watchdog();
    let Seam {
        schema,
        namespace,
        definition,
        queue,
    } = seam("race");
    let cp_a = control_plane(&schema);
    let cp_b = control_plane(&schema);
    let rt = runtime();
    let pause = HeadPublishPause::new();
    let (a_backend, b_backend) = rt.block_on(async {
        (
            pod(&namespace, 1, Some(&pause)).await,
            pod(&namespace, 2, None).await,
        )
    });
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
    let current = Arc::new(Mutex::new("awaiting_fault_entry"));

    let seam_run = {
        let current = current.clone();
        let (a, b) = (&a, &b);
        async move {
            a_backend.create_queue(definition.clone()).await.unwrap();
            b_backend.create_queue(definition.clone()).await.unwrap();
            a.acquire_queue(&queue, ts(0)).await.unwrap();
            let a_epoch = a_backend.current_epoch(&queue).await.unwrap();

            pause.arm();
            let stale_push = {
                let backend = a_backend.clone();
                let queue = queue.clone();
                tokio::spawn(async move {
                    backend
                        .push(&queue, vec![spec("stale")], ts(1), Some(a_epoch))
                        .await
                })
            };
            pause.entered().await;
            stage(&current, "fault_entered");

            b.acquire_queue(&queue, ts(20)).await.unwrap();
            let b_epoch = b_backend.current_epoch(&queue).await.unwrap();
            assert!(
                b_epoch > a_epoch,
                "takeover epoch {b_epoch} must exceed {a_epoch}"
            );
            stage(&current, "takeover_epoch_acquired");

            pause.release();
            stage(&current, "resume_sent");

            let stale = stale_push.await.unwrap();
            assert!(
                stale.is_err(),
                "an append whose head publish follows the takeover must not be acknowledged"
            );
            eprintln!("E2_FAILOVER_SEAM stale_push_error={:?}", stale.unwrap_err());
            stage(&current, "stale_result_fenced");

            b_backend
                .push(&queue, vec![spec("fresh")], ts(21), Some(b_epoch))
                .await
                .unwrap();
            stage(&current, "fresh_owner_acknowledged");

            // A pod opened afterwards rebuilds from the shared log alone.
            let reopened = pod(&namespace, 3, None).await;
            reopened.create_queue(definition).await.unwrap();
            assert_eq!(
                reopened.metrics(&queue).await.unwrap().pending,
                1,
                "only the new owner's write is in the shared log"
            );
        }
    };
    rt.block_on(async {
    match watchdog {
        None => seam_run.await,
        Some(deadline) => {
            if tokio::time::timeout(deadline, seam_run).await.is_err() {
                panic!(
                    "E2_FAILOVER_INFRASTRUCTURE_INDETERMINATE classification=coordination_watchdog \
                     retryable=true release_bar=false last_stage={} watchdog_seconds={}",
                    current.lock().unwrap(),
                    deadline.as_secs()
                );
            }
        }
    }
    });
}
