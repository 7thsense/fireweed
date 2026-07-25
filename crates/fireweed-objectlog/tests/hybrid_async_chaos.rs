//! End-to-end crash / chaos coverage for the `objectlog/hybrid-async` converged plan over the REAL object-log
//! substrate (bead pqueue-fed791af, parent pqueue-b207e65d; TD-004).
//!
//! Unlike the `fireweed-sqlite/tests/hybrid_async_chaos.rs` unit suite (which drives the async checkpoint /
//! debt controller in isolation through the crate-boundary values), this suite exercises the full
//! `ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>` — the production object-log
//! group-commit LOG driving the hybrid SQLite+memory PROJECTION — and injects a crash by DROPPING the backend
//! (no graceful drain) and REOPENING it, which runs recovery-on-open: the durable object log is the source of
//! truth, so a reopen replays the log tail beyond the persisted SQLite high-water.
//!
//! Windows covered:
//!   * crash after object-log commit, before recovery caught the projection up — reopen replays the log;
//!   * crash MID-LEASE (claim committed, no finalize) — the lease is neither lost nor duplicated on reopen;
//!   * crash AFTER finalize — the acked item is not redelivered;
//!   * disk-loss of the SQLite projection image — reopen replays the object log from genesis;
//!   * request-id replay across a crash — a committed-but-unreturned push converges to the same ids;
//!   * crash BETWEEN claim and finalize — the in-flight lease is recoverable and finalizes exactly once.
//!
//! Every window asserts the load-bearing safety invariants: no lost / duplicate leases, no orphaned in-flight
//! records, and (via the request-id path) no duplicate work minted before response delivery.

use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_core::{
    EligibilityPolicy, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RequestId, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, ComposedBackend, ControlPlaneStore, EngineError,
    FinalizeKind, FinalizeOutcome, FinalizePort, InProcessControlPlane, ProjectionRead, PushPort,
    PushSpec, QueueKey,
};
use fireweed_objectlog::ObjectLog;
use fireweed_sqlite::HybridProjectionStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "fireweed-objlog-hybrid-async-chaos-{tag}-{}-{n}",
        std::process::id()
    ));
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
        request_id_retention_ms: 600_000,
        client_item_key_retention_ms: 600_000,
        terminal_retention_ms: 600_000,
        max_lease_duration_ms: 3_600_000,
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

fn ts(secs: i64) -> UtcTimestamp {
    UtcTimestamp::new(secs, 0).unwrap()
}

/// Open (and recover) the object-log + hybrid composition at `root` / `sqlite_path`. A fresh call after a
/// drop is a simulated crash-then-restart: recovery replays the durable object log beyond the SQLite image.
///
/// The synchronous (non-group-commit) write path is used deliberately: each push/claim/finalize commits and
/// applies immediately, so a `drop` of the backend is a clean crash boundary with no un-flushed group-commit
/// buffer (the group-commit latency flusher is a server-runtime concern, exercised by the end-to-end server
/// chaos tests, not this raw-backend suite).
fn open_hybrid(root: &std::path::Path, sqlite_path: &std::path::Path) -> HybridBackend {
    ComposedBackend::new(
        ObjectLog::open(root).expect("open object log"),
        HybridProjectionStore::open(sqlite_path.to_str().expect("utf8 sqlite path"))
            .expect("open hybrid projection"),
        InProcessControlPlane::new(),
    )
    .recover()
    .expect("recover hybrid backend")
}

fn spec() -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(1)),
        ..Default::default()
    }
}

fn claim_one(worker: &str, lease: &str, now: i64) -> ClaimRequest {
    ClaimRequest {
        eligibility_time: None,
        shard: shard(),
        worker_id: WorkerId::new(worker).unwrap(),
        max_items: 1,
        lease_token: LeaseToken::new(lease).unwrap(),
        lease_expires_at: ts(3_600),
        now: ts(now),
        compatibility: ClaimCompatibility::default(),
        expected_epoch: None,
    }
}

// ---------------------------------------------------------------------------
// Window: crash after object-log commit, before recovery caught up.
// ---------------------------------------------------------------------------

/// Two pushes commit to the durable object log; the backend is DROPPED (crash, no graceful drain). Reopening
/// replays the log — both items are resident, none lost, none duplicated.
#[tokio::test]
async fn hybrid_async_chaos_crash_after_commit_replays_pushes_on_reopen() {
    let root = tmp_root("commit-replay");
    let sqlite_path = root.join("projection.sqlite");
    let (first, second) = {
        let backend = open_hybrid(&root, &sqlite_path);
        backend.create_queue(qdef()).await.unwrap();
        let a = backend
            .push(&shard(), vec![spec()], ts(1), None)
            .await
            .unwrap();
        let b = backend
            .push(&shard(), vec![spec()], ts(2), None)
            .await
            .unwrap();
        assert_eq!(backend.metrics(&shard()).await.unwrap().pending, 2);
        (a, b)
    }; // crash

    let reopened = open_hybrid(&root, &sqlite_path);
    let m = reopened.metrics(&shard()).await.unwrap();
    assert_eq!(
        m.pending, 2,
        "both committed pushes replayed from the object log"
    );
    assert_eq!(m.leased, 0);
    assert_ne!(
        first, second,
        "distinct ids minted, none duplicated on replay"
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// Window: crash MID-LEASE (claim committed, finalize never ran).
// ---------------------------------------------------------------------------

/// A claimed (in-flight) item survives a crash as exactly one lease: reopen shows it leased, NOT re-queued as
/// pending and NOT lost, and a fresh claim while the lease is still valid returns nothing (no duplicate
/// delivery of the same work).
#[tokio::test]
async fn hybrid_async_chaos_crash_mid_lease_neither_loses_nor_duplicates_the_lease() {
    let root = tmp_root("mid-lease");
    let sqlite_path = root.join("projection.sqlite");
    let claimed_id = {
        let backend = open_hybrid(&root, &sqlite_path);
        backend.create_queue(qdef()).await.unwrap();
        backend
            .push(&shard(), vec![spec()], ts(1), None)
            .await
            .unwrap();
        let claimed = backend.claim(claim_one("w1", "lease-1", 2)).await.unwrap();
        assert_eq!(claimed.items.len(), 1);
        let m = backend.metrics(&shard()).await.unwrap();
        assert_eq!((m.pending, m.leased), (0, 1));
        claimed.items[0].item_id
    }; // crash mid-lease

    let reopened = open_hybrid(&root, &sqlite_path);
    let m = reopened.metrics(&shard()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased),
        (0, 1),
        "the in-flight lease survived the crash: not re-queued, not lost"
    );

    // The lease is still valid (expires far in the future), so a fresh claim delivers nothing — the same work
    // is not handed out twice.
    let again = reopened.claim(claim_one("w2", "lease-2", 3)).await.unwrap();
    assert!(
        again.items.is_empty(),
        "a still-valid lease is not double-delivered after recovery"
    );

    // Finalizing the recovered lease drives it terminal exactly once.
    reopened
        .finalize(
            &shard(),
            vec![FinalizeOutcome::new(claimed_id, FinalizeKind::Complete)],
            ts(4),
            None,
        )
        .await
        .unwrap();
    let m = reopened.metrics(&shard()).await.unwrap();
    assert_eq!((m.pending, m.leased, m.complete), (0, 0, 1));
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// Window: crash AFTER finalize — the acked item is not redelivered.
// ---------------------------------------------------------------------------

/// A push → claim → finalize(complete) then crash: on reopen the item is terminal and is NOT redelivered by a
/// fresh claim.
#[tokio::test]
async fn hybrid_async_chaos_crash_after_finalize_does_not_redeliver() {
    let root = tmp_root("post-finalize");
    let sqlite_path = root.join("projection.sqlite");
    {
        let backend = open_hybrid(&root, &sqlite_path);
        backend.create_queue(qdef()).await.unwrap();
        backend
            .push(&shard(), vec![spec()], ts(1), None)
            .await
            .unwrap();
        let claimed = backend.claim(claim_one("w1", "lease-1", 2)).await.unwrap();
        let id = claimed.items[0].item_id;
        backend
            .finalize(
                &shard(),
                vec![FinalizeOutcome::new(id, FinalizeKind::Complete)],
                ts(3),
                None,
            )
            .await
            .unwrap();
        assert_eq!(backend.metrics(&shard()).await.unwrap().complete, 1);
    }; // crash after ack

    let reopened = open_hybrid(&root, &sqlite_path);
    let m = reopened.metrics(&shard()).await.unwrap();
    assert_eq!(
        m.complete, 1,
        "the acked item remained terminal after recovery"
    );
    let claim = reopened.claim(claim_one("w2", "lease-2", 4)).await.unwrap();
    assert!(
        claim.items.is_empty(),
        "an acked item is never redelivered after a crash (no duplicate lease)"
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// Window: disk-loss of the SQLite projection image.
// ---------------------------------------------------------------------------

/// The SQLite projection file is DELETED (disk loss). Because the object log is the source of truth, reopening
/// against a fresh SQLite image replays the whole durable log from genesis and reconstructs the exact resident
/// set — a pending item and a leased item — with nothing lost or duplicated.
#[tokio::test]
async fn hybrid_async_chaos_disk_loss_of_sqlite_replays_object_log_from_genesis() {
    let root = tmp_root("disk-loss");
    let sqlite_path = root.join("projection.sqlite");
    {
        let backend = open_hybrid(&root, &sqlite_path);
        backend.create_queue(qdef()).await.unwrap();
        backend
            .push(&shard(), vec![spec()], ts(1), None)
            .await
            .unwrap();
        backend
            .push(&shard(), vec![spec()], ts(2), None)
            .await
            .unwrap();
        // Claim one → one leased, one pending.
        backend.claim(claim_one("w1", "lease-1", 3)).await.unwrap();
        let m = backend.metrics(&shard()).await.unwrap();
        assert_eq!((m.pending, m.leased), (1, 1));
    }; // crash

    // DISK LOSS: wipe the SQLite image (and any WAL/shm sidecars).
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", sqlite_path.display()));
    }
    assert!(!sqlite_path.exists(), "sqlite projection image was removed");

    let reopened = open_hybrid(&root, &sqlite_path);
    let m = reopened.metrics(&shard()).await.unwrap();
    assert_eq!(
        (m.pending, m.leased),
        (1, 1),
        "genesis replay of the durable object log reconstructed the exact resident set"
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// Window: request-id replay across a crash (before response delivery).
// ---------------------------------------------------------------------------

/// A push committed under a request id but the client never saw the response (crash). On restart the SAME
/// request+body replays the original ids and appends nothing; a CONFLICTING body is rejected; and a claim
/// after replay hands out exactly one item — the crash minted no duplicate work.
#[tokio::test]
async fn hybrid_async_chaos_request_id_replay_converges_and_claims_once() {
    let root = tmp_root("request-id");
    let sqlite_path = root.join("projection.sqlite");
    let request = RequestId::new("chaos-req-1").unwrap();
    let body = vec![spec()];

    let first = {
        let backend = open_hybrid(&root, &sqlite_path);
        backend.create_queue(qdef()).await.unwrap();
        backend
            .push_with_request_id(&shard(), request.clone(), body.clone(), ts(1), None)
            .await
            .unwrap()
    }; // crash before response delivery

    let reopened = open_hybrid(&root, &sqlite_path);
    let replayed = reopened
        .push_with_request_id(&shard(), request.clone(), body, ts(2), None)
        .await
        .unwrap();
    assert_eq!(
        replayed, first,
        "same request/body converges to the original ids"
    );
    assert_eq!(
        reopened.metrics(&shard()).await.unwrap().pending,
        1,
        "replay appended no second item"
    );

    let conflict = reopened
        .push_with_request_id(
            &shard(),
            request,
            vec![PushSpec {
                priority: Some(PriorityValue::Int64(999)),
                ..Default::default()
            }],
            ts(3),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(conflict, EngineError::RequestIdConflict);

    // Exactly one unit of work exists to claim — the crash did not duplicate the lease.
    let claimed = reopened.claim(claim_one("w1", "lease-1", 4)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].item_id, first[0]);
    let empty = reopened.claim(claim_one("w2", "lease-2", 5)).await.unwrap();
    assert!(
        empty.items.is_empty(),
        "no duplicate work after request-id replay"
    );
    let _ = std::fs::remove_dir_all(&root);
}
