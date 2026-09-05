#![allow(dead_code, unused_imports)]
#![cfg(feature = "turso")]

//! Whitebox coverage for the Turso compositions' recovery-read ports (bead fireweed-82211ac4).
//!
//! Every `impl_turso_product_ports!` instantiation used to leave `RecoveryReadPort` empty, so the
//! fail-closed trait defaults surfaced `EngineError::Unavailable` for `side_record`,
//! `side_records_by_prefix`, and `explain_commit` on every Turso cell (snorri-40f3739d). These
//! tests prove one atomic-log cell (sqlite × turso) and the derived object-log cell
//! (filesystem × turso, `DurabilityClass::EventualApply`) serve all three reads after a commit
//! and again after a reopen.
//!
//! The commit is driven through `Backend::commit_raw` — the raw authoritative boundary (log
//! append + Turso apply) that persists `WriteSideRecords` and the retained
//! `RequestOutcome::CommitTransition` row. The composed `commit_transition` deliberately still
//! declines side-record entries on Turso products until the full Strict commit surface is wired,
//! so it cannot carry this scenario.

use std::path::PathBuf;
use std::sync::Arc;

use fireweed::{
    Bytes, ClaimRef, EntryOutcome, FinalizeKind, Fireweed, InstanceFence, LogConfig,
    MultiClaimCommitEntry, MultiClaimCommitRequest, NewItem, ObjectLogAuthority, PriorityValue,
    ProjectionStoreConfig, RecoveryPolicy, ResponseBarrier, SegmentConfig, SegmentSettings,
    StorageConfig, SystemClock, open,
};
use fireweed_core::{
    EligibilityPolicy, ItemId, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RequestId,
    RetryPolicy, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    Backend, CommandChecksum, CommandEnvelope, CommandId, CommitEntryStatus, CommitOutcomeEntry,
    ControlPlaneStore, QueueCommand, QueueKey, RawCommitRequest, RecoveryReadPort, RequestOutcome,
    SideRecord, WriteSideRecordsCommand,
};

fn definition(tenant: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
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
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 3_600_000,
        terminal_retention_ms: 3_600_000,
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

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn side(key: &str, payload: &str) -> SideRecord {
    SideRecord {
        key: key.as_bytes().to_vec(),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    }
}

fn consumed_input() -> ItemId {
    ItemId::new("301").unwrap()
}

/// One committed transition's raw envelope: the side-record writes plus the retained whole-body
/// commit outcome (`RequestOutcome::CommitTransition`), exactly what the vectorized commit path
/// lowers into the log for the Turso apply arm to persist.
fn commit_envelope(request_id: &RequestId) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new("turso-recovery-reads-commit-1"),
        request_id: Some(request_id.clone()),
        request_fingerprint: Some(42),
        request_outcome: Some(RequestOutcome::CommitTransition {
            entries: vec![CommitOutcomeEntry {
                consumed_input_id: consumed_input(),
                additional_consumed_input_ids: vec![],
                instance: None,
                side_record_keys: vec![],
                lifecycle_item_ids: vec![],
                rejection: None,
            }],
        }),
        item_ids: vec![],
        command: QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
            records: vec![
                side("audit:i-1:001", "a1"),
                side("audit:i-1:003", "a3"),
                side("audit:i-1:002", "a2"),
                side("audit:i-2:001", "other"),
            ],
        }),
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

fn unique_root(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "fw-turso-recovery-reads-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

/// Point get, ordered cursor-paged prefix scan, and commit recovery must all serve from the
/// composed backend (never `EngineError::Unavailable`).
async fn assert_recovery_reads<B: RecoveryReadPort>(
    backend: &B,
    q: &QueueKey,
    request_id: &RequestId,
) {
    assert_eq!(
        backend.side_record(q, b"audit:i-1:002").await.unwrap(),
        Some(Bytes::from_static(b"a2"))
    );
    assert_eq!(
        backend.side_record(q, b"audit:missing").await.unwrap(),
        None
    );

    let first_page = backend
        .side_records_by_prefix(q, b"audit:i-1:", 2, None)
        .await
        .unwrap();
    assert_eq!(
        first_page.entries,
        vec![
            (b"audit:i-1:001".to_vec(), Bytes::from_static(b"a1")),
            (b"audit:i-1:002".to_vec(), Bytes::from_static(b"a2")),
        ]
    );
    let cursor = first_page
        .next_cursor
        .clone()
        .expect("a third matching entry remains");
    assert_eq!(cursor, b"audit:i-1:003".to_vec());
    let second_page = backend
        .side_records_by_prefix(q, b"audit:i-1:", 2, Some(cursor))
        .await
        .unwrap();
    assert_eq!(
        second_page.entries,
        vec![(b"audit:i-1:003".to_vec(), Bytes::from_static(b"a3"))]
    );
    assert_eq!(second_page.next_cursor, None);

    // A sibling prefix stays isolated.
    let other = backend
        .side_records_by_prefix(q, b"audit:i-2:", 10, None)
        .await
        .unwrap();
    assert_eq!(
        other.entries,
        vec![(b"audit:i-2:001".to_vec(), Bytes::from_static(b"other"))]
    );

    let recovery = backend
        .explain_commit(q, request_id.clone())
        .await
        .unwrap()
        .expect("retained commit recovery");
    assert_eq!(recovery.request_id, *request_id);
    assert_eq!(recovery.entries.len(), 1);
    assert_eq!(recovery.entries[0].consumed_input_id, consumed_input());
    assert!(matches!(
        recovery.entries[0].status,
        CommitEntryStatus::Committed
    ));
    assert_eq!(
        backend
            .explain_commit(q, RequestId::new("never-committed").unwrap())
            .await
            .unwrap(),
        None
    );
}

/// Create the queue, commit one side-record-writing transition through the raw commit boundary,
/// and prove all three recovery reads serve.
async fn create_commit_read<B>(backend: &B, def: QueueDefinition, request_id: &RequestId)
where
    B: Backend + RecoveryReadPort + ControlPlaneStore,
{
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    ControlPlaneStore::create_queue(backend, def).await.unwrap();
    let epoch = ControlPlaneStore::current_epoch(backend, &q).await.unwrap();
    Backend::commit_raw(
        backend,
        RawCommitRequest::new(q.clone(), vec![commit_envelope(request_id)], epoch),
    )
    .await
    .unwrap();
    assert_recovery_reads(backend, &q, request_id).await;
}

/// Reopened backends must serve the same reads from durable state (log replay + the durable
/// Turso projection).
async fn reread_after_reopen<B>(backend: &B, def: QueueDefinition, request_id: &RequestId)
where
    B: Backend + RecoveryReadPort + ControlPlaneStore,
{
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    ControlPlaneStore::create_queue(backend, def).await.unwrap();
    assert_recovery_reads(backend, &q, request_id).await;
}

/// Atomic-log variant: sqlite log × turso projection (synchronous Turso apply).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn turso_sqlite_log_serves_recovery_reads_after_commit_and_reopen() {
    let root = unique_root("sqlite-log");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let log_path = root.join("log.db");
    let projection_path = root.join("projection.turso");
    let def = definition("rr-sqlite");
    let request_id = RequestId::new("turso-recovery-sqlite-log").unwrap();

    let backend = crate::turso_compose::assemble_sqlite_log_turso(
        log_path.to_str().expect("utf-8 path"),
        projection_path.clone(),
    )
    .expect("assemble sqlite-log × turso");
    create_commit_read(&backend, def.clone(), &request_id).await;
    drop(backend);

    let reopened = crate::turso_compose::assemble_sqlite_log_turso(
        log_path.to_str().expect("utf-8 path"),
        projection_path,
    )
    .expect("reopen sqlite-log × turso");
    reread_after_reopen(&reopened, def, &request_id).await;
    drop(reopened);
    let _ = std::fs::remove_dir_all(&root);
}

/// Derived object-log variant: filesystem log × turso projection
/// (`DurabilityClass::EventualApply`); recovery reads catch the projection up to the log
/// high-water before answering.
#[cfg(feature = "objectlog")]
#[tokio::test]
async fn turso_filesystem_log_serves_recovery_reads_after_commit_and_reopen() {
    let root = unique_root("fs-log");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let log_root = root.join("fs-log");
    let projection_path = root.join("projection.turso");
    let def = definition("rr-fs");
    let request_id = RequestId::new("turso-recovery-fs-log").unwrap();
    let segments = || SegmentSettings {
        target_bytes: 1024 * 1024,
        max_latency_ms: 5,
    };

    let log = crate::open_composed_object_log_engine(&log_root, "turso-recovery-reads", segments())
        .expect("open filesystem object log");
    let backend =
        crate::turso_compose::assemble_objectlog_turso(log, projection_path.clone(), None)
            .expect("assemble filesystem-log × turso");
    create_commit_read(&backend, def.clone(), &request_id).await;
    drop(backend);

    let log = crate::open_composed_object_log_engine(&log_root, "turso-recovery-reads", segments())
        .expect("reopen filesystem object log");
    let reopened = crate::turso_compose::assemble_objectlog_turso(log, projection_path, None)
        .expect("reopen filesystem-log × turso");
    reread_after_reopen(&reopened, def, &request_id).await;
    drop(reopened);
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// Full commit_transition surface through the public facade (the path Snorri uses)
// ---------------------------------------------------------------------------

fn facade_config(log: LogConfig, projection_path: std::path::PathBuf) -> StorageConfig {
    let mut cfg = StorageConfig {
        log,
        projection: ProjectionStoreConfig::Turso {
            path: projection_path,
        },
        control_plane: None,
        authority: None,
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig {
            target_bytes: 1024 * 1024,
            max_latency_ms: 5,
        },
        namespace: "turso-commit-surface".to_owned(),
        recovery: RecoveryPolicy::default(),
    };
    if matches!(
        &cfg.log,
        LogConfig::Filesystem { .. } | LogConfig::S3 { .. }
    ) {
        cfg.authority = Some(ObjectLogAuthority::NativeConditionalWrite);
    }
    cfg
}

fn side_new(key: &str, payload: &str) -> fireweed::SideRecord {
    fireweed::SideRecord {
        key: key.as_bytes().to_vec(),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    }
}

fn lifecycle_item() -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(20)),
        ..NewItem::default()
    }
}

async fn claim_one(fw: &Fireweed, q: &fireweed_engine::QueueKey) -> ClaimRef {
    fw.push(
        q,
        NewItem {
            priority: Some(PriorityValue::Int64(1)),
            ..NewItem::default()
        },
    )
    .await
    .unwrap();
    let claimed = fw.claim(q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let c = &claimed[0];
    ClaimRef {
        item_id: c.item_id,
        lease_token: c.lease_token.clone().expect("claimed item carries token"),
        lease_expires_at: c.lease_expires_at,
        item_version: c.item_version,
    }
}

fn full_commit_request(claim_ref: ClaimRef, request_id: &RequestId) -> MultiClaimCommitRequest {
    MultiClaimCommitRequest {
        request_id: Some(request_id.clone()),
        entries: vec![MultiClaimCommitEntry {
            claim_ref,
            additional_claim_refs: vec![],
            finalize: FinalizeKind::Complete,
            side_records: vec![
                side_new("audit:i-1:001", "a1"),
                side_new("audit:i-1:003", "a3"),
                side_new("audit:i-1:002", "a2"),
                side_new("audit:i-2:001", "other"),
            ],
            lifecycle_items: vec![lifecycle_item()],
            instance_fence: Some(InstanceFence {
                instance_key: b"wf-1".to_vec(),
                expected: 0,
                next: 1,
            }),
        }],
    }
}

/// Side records, the advanced fence, and the retained per-entry outcome must all read back
/// through the facade's recovery ports.
async fn assert_full_commit_reads(
    fw: &Fireweed,
    q: &QueueKey,
    input_id: ItemId,
    lifecycle_id: ItemId,
    request_id: &RequestId,
) {
    assert_eq!(
        fw.side_record(q, b"audit:i-1:002").await.unwrap(),
        Some(Bytes::from_static(b"a2"))
    );
    assert_eq!(fw.side_record(q, b"audit:missing").await.unwrap(), None);
    let first_page = fw
        .side_records_by_prefix(q, b"audit:i-1:", 2, None)
        .await
        .unwrap();
    assert_eq!(
        first_page.entries,
        vec![
            (b"audit:i-1:001".to_vec(), Bytes::from_static(b"a1")),
            (b"audit:i-1:002".to_vec(), Bytes::from_static(b"a2")),
        ]
    );
    let cursor = first_page.next_cursor.clone().expect("third entry remains");
    let second_page = fw
        .side_records_by_prefix(q, b"audit:i-1:", 2, Some(cursor))
        .await
        .unwrap();
    assert_eq!(
        second_page.entries,
        vec![(b"audit:i-1:003".to_vec(), Bytes::from_static(b"a3"))]
    );
    assert_eq!(second_page.next_cursor, None);

    let recovery = fw
        .explain_commit(q, request_id.clone())
        .await
        .unwrap()
        .expect("retained commit recovery");
    assert_eq!(recovery.request_id, *request_id);
    assert_eq!(recovery.entries.len(), 1);
    assert_eq!(recovery.entries[0].consumed_input_id, input_id);
    assert_eq!(recovery.entries[0].instance, Some((b"wf-1".to_vec(), 1)));
    assert_eq!(recovery.entries[0].lifecycle_item_ids, vec![lifecycle_id]);
    assert!(matches!(
        recovery.entries[0].status,
        fireweed::CommitEntryStatus::Committed
    ));
}

/// Full commit_transition (side records + instance fence + lifecycle item) through the public
/// facade, with per-entry outcomes, replay idempotency, a fabricated-lease rejection, and reopen
/// durability. This is the surface the previous fallback rejected with `Unavailable`.
async fn full_commit_transition_cell(tag: &str, make_log: impl Fn(&std::path::Path) -> LogConfig) {
    let root = unique_root(tag);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let projection_path = root.join("projection.turso");
    let clock = Arc::new(SystemClock);
    let def = definition("commit-surface");
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let request_id = RequestId::new(format!("turso-full-commit-{tag}")).unwrap();

    let fw = open(
        facade_config(make_log(&root), projection_path.clone()),
        Arc::clone(&clock) as _,
    )
    .expect("open turso composition");
    fw.create_queue(def.clone()).await.unwrap();
    let claim_ref = claim_one(&fw, &q).await;
    let input_id = claim_ref.item_id;
    let request = full_commit_request(claim_ref, &request_id);

    let outcomes = fw.commit_multi_claim(&q, request.clone()).await.unwrap();
    let lifecycle_id = match outcomes.as_slice() {
        [EntryOutcome::Committed { lifecycle_item_ids }] if lifecycle_item_ids.len() == 1 => {
            lifecycle_item_ids[0]
        }
        other => panic!("expected one Committed entry with one lifecycle id, got {other:?}"),
    };
    assert_ne!(lifecycle_id, input_id);
    assert_full_commit_reads(&fw, &q, input_id, lifecycle_id, &request_id).await;

    // Replay: the same request_id + identical body returns the retained outcome verbatim.
    let replay = fw.commit_multi_claim(&q, request.clone()).await.unwrap();
    assert_eq!(replay, outcomes);

    // The lifecycle item is genuinely claimable work.
    let metrics = fw.metrics(&q).await.unwrap();
    assert_eq!(
        (metrics.pending, metrics.leased, metrics.complete),
        (1, 0, 1),
        "input completed; only the lifecycle item remains pending"
    );

    // A fabricated lease token rejects per-entry with StaleLease and writes nothing.
    let mut forged = claim_one(&fw, &q).await;
    forged.lease_token = LeaseToken::new("not-the-real-token").unwrap();
    let rejected = fw
        .commit_multi_claim(
            &q,
            MultiClaimCommitRequest {
                request_id: None,
                entries: vec![MultiClaimCommitEntry {
                    claim_ref: forged,
                    additional_claim_refs: vec![],
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side_new("audit:rejected", "must-not-exist")],
                    lifecycle_items: vec![],
                    instance_fence: None,
                }],
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(
            rejected.as_slice(),
            [EntryOutcome::Rejected(fireweed::EngineError::StaleLease)]
        ),
        "expected Rejected(StaleLease), got {rejected:?}"
    );
    assert_eq!(fw.side_record(&q, b"audit:rejected").await.unwrap(), None);
    drop(fw);

    // Reopen: reads and replay serve from durable state (retained commit row + side records).
    let reopened = open(facade_config(make_log(&root), projection_path), clock)
        .expect("reopen turso composition");
    assert_full_commit_reads(&reopened, &q, input_id, lifecycle_id, &request_id).await;
    let replay = reopened
        .commit_multi_claim(&q, request.clone())
        .await
        .unwrap();
    assert_eq!(replay, outcomes, "durable replay after reopen");
    drop(reopened);
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn turso_sqlite_log_full_commit_transition_replay_and_reopen() {
    full_commit_transition_cell("full-sqlite-log", |root| LogConfig::Sqlite {
        path: root.join("log.db"),
    })
    .await;
}

#[cfg(feature = "objectlog")]
#[tokio::test]
async fn turso_filesystem_log_full_commit_transition_replay_and_reopen() {
    full_commit_transition_cell("full-fs-log", |root| LogConfig::Filesystem {
        root: root.join("fs-log"),
    })
    .await;
}

/// The shared Snorri commit-transition qualification (the same scenarios the memory, sqlite, and
/// postgres products run) against the atomic sqlite-log × turso composition.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn turso_commit_transition_shared_conformance_scenarios() {
    use fireweed_conformance::scenarios::{
        commit_transition_rejects_bad_token_without_writing,
        commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen,
    };

    let root = unique_root("shared-conformance");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    {
        let log_path = root.join("positive-log.db");
        let projection_path = root.join("positive-projection.turso");
        let make = move || {
            crate::turso_compose::assemble_sqlite_log_turso(
                log_path.to_str().expect("utf-8 path"),
                projection_path.clone(),
            )
            .expect("assemble sqlite-log × turso")
        };
        commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen(
            make,
        )
        .await;
    }
    {
        let log_path = root.join("reject-log.db");
        let projection_path = root.join("reject-projection.turso");
        let make = move || {
            crate::turso_compose::assemble_sqlite_log_turso(
                log_path.to_str().expect("utf-8 path"),
                projection_path.clone(),
            )
            .expect("assemble sqlite-log × turso")
        };
        commit_transition_rejects_bad_token_without_writing(make).await;
    }
    let _ = std::fs::remove_dir_all(&root);
}
