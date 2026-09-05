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
    AddressedMutation, Backend, CommandChecksum, CommandEnvelope, CommandId, CommitEntryStatus,
    CommitOutcomeEntry, ControlPlaneStore, EngineError, ItemMutationOperation, ItemMutationOutcome,
    ItemMutationRequest, ItemPatch, PayloadUpdate, PushPort, PushSpec, QueueCommand, QueueKey,
    RawCommitRequest, RecoveryReadPort, RequestOutcome, SideRecord, UpdateFieldsPort,
    WriteSideRecordsCommand,
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

/// Probe for the snorri `object_log_turso` default functional cell: with the
/// ASYNC-PROJECTION barrier (a live apply coordinator — what an embedder
/// composes), a plain push must become claimable once the projection has
/// caught up; a recovery read forces that catch-up (fireweed-82211ac4).
#[cfg(feature = "objectlog")]
#[tokio::test]
async fn derived_turso_async_barrier_push_is_claimable_after_catch_up() {
    let root = unique_root("async-claim");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let projection_path = root.join("projection.turso");
    let def = definition("async-claim");
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());

    let mut cfg = facade_config(
        LogConfig::Filesystem {
            root: root.join("fs-log"),
        },
        projection_path,
    );
    cfg.response_barrier = ResponseBarrier::AsyncProjection;
    cfg.async_projection = Some(fireweed_engine::AsyncProjectionSpec {
        apply_lag_max_commands: 500,
        apply_debt_max_bytes: 8 * 1024 * 1024,
        apply_queue_depth_max: 64,
        oldest_unapplied_max_ms: 5_000,
        apply_poison_retry_threshold: 5,
        apply_start_delay_ms: 0,
    });
    let fw = open(cfg, Arc::new(SystemClock) as _).expect("open async-barrier turso composition");
    fw.create_queue(def).await.unwrap();
    let pushed = fw
        .push(
            &q,
            NewItem {
                priority: Some(PriorityValue::Int64(1)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();

    // A recovery read forces projection catch-up before answering.
    let _ = fw.side_record(&q, b"probe").await.unwrap();

    let claimed = fw.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "an applied push must be claimable on the async-barrier turso cell"
    );
    assert_eq!(claimed[0].item_id, pushed);
    drop(fw);
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// Upsert / update_fields / mutate_items ports (bead fireweed-82211ac4, round 3)
// ---------------------------------------------------------------------------

fn field(bytes: &str) -> Option<Bytes> {
    Some(Bytes::copy_from_slice(bytes.as_bytes()))
}

/// `replace_if_pending` (Snorri's enroll path) and `mutate_items` (scenario-step live mutations)
/// through the public facade: fresh key inserts, still-pending key replaces with a new id, the
/// replacement's fields/entity/priority are intact on claim, and an addressed mutation has
/// sqlite-parity outcome, replay, and version-precondition semantics.
async fn upsert_and_mutation_cell(tag: &str, make_log: impl Fn(&std::path::Path) -> LogConfig) {
    let root = unique_root(tag);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let projection_path = root.join("projection.turso");
    let clock = Arc::new(SystemClock);
    let def = definition("ports");
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());

    let fw = open(
        facade_config(make_log(&root), projection_path.clone()),
        Arc::clone(&clock) as _,
    )
    .expect("open turso composition");
    fw.create_queue(def.clone()).await.unwrap();

    // (a) Fresh key inserts.
    let key = fireweed_core::ClientItemKey::new("workflow-1").unwrap();
    let inserted = fw
        .upsert(
            &q,
            key.clone(),
            NewItem {
                priority: Some(PriorityValue::Int64(5)),
                fields: [("stage".to_string(), Bytes::from_static(b"one"))]
                    .into_iter()
                    .collect(),
                entity: Some(serde_json::json!({"kind": "alpha"})),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    let fireweed::UpsertOutcome::Inserted { item_id: first_id } = inserted else {
        panic!("expected Inserted, got {inserted:?}");
    };

    // Still-pending key replaces atomically with a NEW monotonic id.
    let replaced = fw
        .upsert(
            &q,
            key.clone(),
            NewItem {
                priority: Some(PriorityValue::Int64(7)),
                fields: [("stage".to_string(), Bytes::from_static(b"two"))]
                    .into_iter()
                    .collect(),
                entity: Some(serde_json::json!({"kind": "beta"})),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    let fireweed::UpsertOutcome::Replaced {
        new_item_id,
        superseded_item_id,
    } = replaced
    else {
        panic!("expected Replaced, got {replaced:?}");
    };
    assert_eq!(superseded_item_id, first_id);
    assert_ne!(new_item_id, first_id);

    // (b) Addressed mutation on the pending replacement: Updated outcome + field edit.
    let mutation = |rid: &str, expected_item_version: Option<u64>| ItemMutationRequest {
        request_id: RequestId::new(rid).unwrap(),
        evaluated_at: UtcTimestamp::new(5, 0).unwrap(),
        dry_run: false,
        returning: Default::default(),
        gate_changes: vec![],
        operation: ItemMutationOperation::Addressed {
            entries: vec![AddressedMutation {
                item_id: new_item_id,
                expected_item_version,
                predicates: vec![],
                lease_guard: Default::default(),
                patch: ItemPatch {
                    field_edits: [("note".to_string(), field("m1"))].into_iter().collect(),
                    ..ItemPatch::default()
                },
            }],
        },
    };
    let first_response = fw
        .mutate_items(&q, mutation("ports-m1", None))
        .await
        .unwrap();
    assert_eq!(first_response.results.len(), 1);
    assert_eq!(first_response.results[0].item_id, new_item_id);
    let mutated_version = match &first_response.results[0].outcome {
        ItemMutationOutcome::Updated {
            item_version,
            state,
        } => {
            assert_eq!(*state, fireweed_core::ItemState::Pending);
            *item_version
        }
        other => panic!("expected Updated, got {other:?}"),
    };

    // Replay: the same request_id + identical body returns the retained response verbatim.
    let replay = fw
        .mutate_items(&q, mutation("ports-m1", None))
        .await
        .unwrap();
    assert_eq!(replay, first_response);

    // Precondition: a version fence mismatch is a per-item Conflict outcome, nothing mutated.
    let conflicted = fw
        .mutate_items(&q, mutation("ports-m2", Some(999)))
        .await
        .unwrap();
    assert!(
        matches!(
            conflicted.results[0].outcome,
            ItemMutationOutcome::Conflict { actual_version } if actual_version == mutated_version
        ),
        "expected Conflict at v{mutated_version}, got {:?}",
        conflicted.results[0].outcome
    );

    // The replacement (not the superseded insert) is the claimable item, with the replacement's
    // priority/fields/entity plus the mutation's field edit intact.
    let claimed = fw.claim(&q, 10, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1, "only the replacement is claimable");
    let c = &claimed[0];
    assert_eq!(c.item_id, new_item_id);
    assert_eq!(c.priority, Some(PriorityValue::Int64(7)));
    assert_eq!(
        c.fields.get("stage").map(|b| b.as_ref()),
        Some(b"two".as_ref())
    );
    assert_eq!(
        c.fields.get("note").map(|b| b.as_ref()),
        Some(b"m1".as_ref())
    );
    assert_eq!(c.entity, Some(serde_json::json!({"kind": "beta"})));
    drop(fw);
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn turso_sqlite_log_upsert_and_mutate_items() {
    upsert_and_mutation_cell("ports-sqlite-log", |root| LogConfig::Sqlite {
        path: root.join("log.db"),
    })
    .await;
}

#[cfg(feature = "objectlog")]
#[tokio::test]
async fn turso_filesystem_log_upsert_and_mutate_items() {
    upsert_and_mutation_cell("ports-fs-log", |root| LogConfig::Filesystem {
        root: root.join("fs-log"),
    })
    .await;
}

/// (c) `update_fields` on the composed backends directly: bumps the version, honors the version
/// fence (Conflict) and absent-id (NotFound) guards — sqlite-relational parity.
async fn update_fields_asserts<B>(backend: &B, def: QueueDefinition)
where
    B: ControlPlaneStore + PushPort + UpdateFieldsPort,
{
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    ControlPlaneStore::create_queue(backend, def).await.unwrap();
    let ids = PushPort::push(
        backend,
        &q,
        vec![PushSpec {
            priority: Some(PriorityValue::Int64(1)),
            ..PushSpec::default()
        }],
        ts(0),
        None,
    )
    .await
    .unwrap();
    let id = ids[0];
    let version = UpdateFieldsPort::update_fields(
        backend,
        &q,
        id,
        [("stage".to_string(), field("one"))].into_iter().collect(),
        PayloadUpdate::Keep,
        None,
        Some(1),
        ts(1),
        None,
    )
    .await
    .unwrap();
    assert_eq!(version, 2, "update bumps the item version");
    let err = UpdateFieldsPort::update_fields(
        backend,
        &q,
        id,
        [("stage".to_string(), field("two"))].into_iter().collect(),
        PayloadUpdate::Keep,
        None,
        Some(99),
        ts(2),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err, EngineError::Conflict, "version fence mismatch");
    let err = UpdateFieldsPort::update_fields(
        backend,
        &q,
        fireweed_core::ItemId::new("909090").unwrap(),
        [("stage".to_string(), field("x"))].into_iter().collect(),
        PayloadUpdate::Keep,
        None,
        None,
        ts(3),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err, EngineError::NotFound, "absent item fails closed");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn turso_sqlite_log_update_fields_guards_and_versions() {
    let root = unique_root("uf-sqlite-log");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let backend = crate::turso_compose::assemble_sqlite_log_turso(
        root.join("log.db").to_str().expect("utf-8 path"),
        root.join("projection.turso"),
    )
    .expect("assemble sqlite-log × turso");
    update_fields_asserts(&backend, definition("uf-sqlite")).await;
    drop(backend);
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(feature = "objectlog")]
#[tokio::test]
async fn turso_filesystem_log_update_fields_guards_and_versions() {
    let root = unique_root("uf-fs-log");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let log = crate::open_composed_object_log_engine(
        &root.join("fs-log"),
        "turso-update-fields",
        SegmentSettings {
            target_bytes: 1024 * 1024,
            max_latency_ms: 5,
        },
    )
    .expect("open filesystem object log");
    let backend =
        crate::turso_compose::assemble_objectlog_turso(log, root.join("projection.turso"), None)
            .expect("assemble filesystem-log × turso");
    update_fields_asserts(&backend, definition("uf-fs")).await;
    drop(backend);
    let _ = std::fs::remove_dir_all(&root);
}

/// The shared ADR-011 typed-index qualifications the sqlite/postgres products run, against the
/// atomic sqlite-log × turso composition (update_fields re-key + upsert unique-conflict atomicity).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn turso_adr011_shared_conformance_scenarios() {
    use fireweed_conformance::scenarios::{
        adr011_typed_update_fields_and_replace_rekey,
        adr011_typed_upsert_insert_unique_conflict_is_atomic,
    };
    let root = unique_root("adr011-conformance");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    {
        let log_path = root.join("rekey-log.db");
        let projection_path = root.join("rekey-projection.turso");
        let make = move || {
            crate::turso_compose::assemble_sqlite_log_turso(
                log_path.to_str().expect("utf-8 path"),
                projection_path.clone(),
            )
            .expect("assemble sqlite-log × turso")
        };
        adr011_typed_update_fields_and_replace_rekey(make).await;
    }
    {
        let log_path = root.join("upsert-log.db");
        let projection_path = root.join("upsert-projection.turso");
        let make = move || {
            crate::turso_compose::assemble_sqlite_log_turso(
                log_path.to_str().expect("utf-8 path"),
                projection_path.clone(),
            )
            .expect("assemble sqlite-log × turso")
        };
        adr011_typed_upsert_insert_unique_conflict_is_atomic(make).await;
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// Minimal repro for the snorri enroll gap: on the ASYNC-barrier derived
/// cell, a `push_batch_with_request_id` (the idempotent request-id push the
/// embedder's enroll uses) must produce a claimable item exactly like a
/// plain push does.
#[cfg(feature = "objectlog")]
#[tokio::test]
async fn derived_turso_async_barrier_request_id_push_is_claimable() {
    let root = unique_root("async-reqid-push");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let projection_path = root.join("projection.turso");
    let def = definition("async-reqid");
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());

    let mut cfg = facade_config(
        LogConfig::Filesystem {
            root: root.join("fs-log"),
        },
        projection_path,
    );
    cfg.response_barrier = ResponseBarrier::AsyncProjection;
    cfg.async_projection = Some(fireweed_engine::AsyncProjectionSpec {
        apply_lag_max_commands: 500,
        apply_debt_max_bytes: 8 * 1024 * 1024,
        apply_queue_depth_max: 64,
        oldest_unapplied_max_ms: 5_000,
        apply_poison_retry_threshold: 5,
        apply_start_delay_ms: 0,
    });
    let fw = open(cfg, Arc::new(SystemClock) as _).expect("open async-barrier turso composition");
    fw.create_queue(def).await.unwrap();
    let request_id = RequestId::new("reqid-push-1").unwrap();
    let pushed = fw
        .push_batch_with_request_id(
            &q,
            request_id.clone(),
            vec![NewItem {
                priority: Some(PriorityValue::Int64(1)),
                ..NewItem::default()
            }],
        )
        .await
        .unwrap()
        .into_item_ids();
    assert_eq!(pushed.len(), 1);

    // Recovery read forces projection catch-up.
    let _ = fw.side_record(&q, b"probe").await.unwrap();

    let claimed = fw.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "a request-id push must be claimable on the async-barrier turso cell"
    );
    assert_eq!(claimed[0].item_id, pushed[0]);
    drop(fw);
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// Snorri-shaped enroll repro (bead fireweed-82211ac4, round 4)
// ---------------------------------------------------------------------------

/// The 19 compound typed indexes snorri's scheduled-action queue declares
/// (`scheduled_action_typed_indexes`), including the datetime-typed
/// `scheduled_at` fields and the unique `by_run_target_key`.
fn snorri_like_typed_indexes() -> Vec<fireweed_core::QueueIndex> {
    use fireweed_core::{CompoundIndexDef, CompoundIndexField, IndexType};
    let indexes: [(&str, bool, &[(&str, IndexType)]); 19] = [
        (
            "by_record_kind_key",
            false,
            &[
                ("record_kind", IndexType::String),
                ("idempotency_key", IndexType::String),
            ],
        ),
        (
            "by_record_kind_scheduled_at",
            false,
            &[
                ("record_kind", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
            ],
        ),
        (
            "by_tenant_status",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("status", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
            ],
        ),
        (
            "by_tenant_action_type",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
            ],
        ),
        (
            "by_tenant_action_type_status",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
                ("status", IndexType::String),
            ],
        ),
        (
            "by_tenant_action_type_recycling",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
                ("suppressed_by_recycling", IndexType::Boolean),
            ],
        ),
        (
            "by_tenant_engagement_probability",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("engagement_probability", IndexType::Float),
            ],
        ),
        (
            "by_workflow_status",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("workflow_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("status", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
            ],
        ),
        (
            "by_workflow_action_type",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("workflow_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
            ],
        ),
        (
            "by_workflow_action_type_status",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("workflow_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
                ("status", IndexType::String),
            ],
        ),
        (
            "by_workflow_action_type_recycling",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("workflow_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
                ("suppressed_by_recycling", IndexType::Boolean),
            ],
        ),
        (
            "by_workflow_engagement_probability",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("workflow_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("engagement_probability", IndexType::Float),
            ],
        ),
        (
            "by_run_status",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("run_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("status", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
            ],
        ),
        (
            "by_run_action_type",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("run_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
            ],
        ),
        (
            "by_run_action_type_status",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("run_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
                ("status", IndexType::String),
            ],
        ),
        (
            "by_run_action_type_recycling",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("run_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("action_type", IndexType::String),
                ("scheduled_at", IndexType::Datetime),
                ("suppressed_by_recycling", IndexType::Boolean),
            ],
        ),
        (
            "by_run_recycling",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("run_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("suppressed_by_recycling", IndexType::Boolean),
                ("scheduled_at", IndexType::Datetime),
            ],
        ),
        (
            "by_run_engagement_probability",
            false,
            &[
                ("tenant_id", IndexType::String),
                ("run_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("engagement_probability", IndexType::Float),
            ],
        ),
        (
            "by_run_target_key",
            true,
            &[
                ("tenant_id", IndexType::String),
                ("run_id", IndexType::String),
                ("projection_kind", IndexType::String),
                ("projection_schema_version", IndexType::Integer),
                ("target_key", IndexType::String),
            ],
        ),
    ];
    indexes
        .into_iter()
        .map(|(name, unique, fields)| fireweed_core::QueueIndex {
            name: name.to_string(),
            declaration: fireweed_core::IndexDeclaration::Compound(CompoundIndexDef {
                fields: fields
                    .iter()
                    .map(|(field, index_type)| CompoundIndexField {
                        field: field.to_string(),
                        index_type: index_type.clone(),
                    })
                    .collect(),
                unique,
            }),
        })
        .collect()
}

/// Snorri's scheduled-action queue definition shape (`queue_definition`):
/// timestamp-ascending priority model + the 19 typed indexes.
fn snorri_like_definition(tenant: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new("lifecycle").unwrap(),
        priority_model: PriorityModel::timestamp_ascending(),
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 86_400_000,
        client_item_key_retention_ms: 86_400_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 300_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 10_000,
        max_claim_batch_size: 10_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: snorri_like_typed_indexes(),
        emit_change_records: false,
    }
}

/// A full snorri ENROLL work item (`workflow_item_from_facade` -> `workflow_item` with the probe's
/// `NewWorkflowWorkItem`): Timestamp priority AND not_before, entity JSON with a null
/// engagement_probability, the 11-key native `index_fields` map (engagement_probability absent, as
/// the probe payload carries none) with a datetime `scheduled_at`, the execution-remainder fields
/// blob, snorri's four metadata keys (including an Integer value), and a colon-joined
/// client_item_key.
fn snorri_like_item(key: &str, target: &str) -> NewItem {
    use fireweed_core::TypedValue;
    let scheduled = UtcTimestamp::new(1_700_000_000, 0).unwrap();
    let mut index_fields = std::collections::BTreeMap::new();
    index_fields.insert(
        "record_kind".to_string(),
        TypedValue::String("transition".to_string()),
    );
    index_fields.insert(
        "tenant_id".to_string(),
        TypedValue::String("t-e2e".to_string()),
    );
    index_fields.insert(
        "projection_kind".to_string(),
        TypedValue::String("scheduled_action".to_string()),
    );
    index_fields.insert(
        "projection_schema_version".to_string(),
        TypedValue::Integer(1),
    );
    index_fields.insert(
        "workflow_id".to_string(),
        TypedValue::String("wf-1".to_string()),
    );
    index_fields.insert(
        "run_id".to_string(),
        TypedValue::String("run-1".to_string()),
    );
    index_fields.insert(
        "target_key".to_string(),
        TypedValue::String(target.to_string()),
    );
    index_fields.insert("scheduled_at".to_string(), TypedValue::DateTime(scheduled));
    index_fields.insert(
        "status".to_string(),
        TypedValue::String("scheduled".to_string()),
    );
    index_fields.insert(
        "action_type".to_string(),
        TypedValue::String("message.send".to_string()),
    );
    index_fields.insert(
        "suppressed_by_recycling".to_string(),
        TypedValue::Bool(false),
    );
    // engagement_probability deliberately ABSENT (the probe payload carries none; the legacy
    // derive pass skipped null JSON values, so the native map must too).
    let payload = serde_json::json!({ "subject_id": target });
    let mut metadata = fireweed_core::Metadata::default();
    metadata.insert(
        "snorri.tenant_id",
        fireweed_core::MetadataValue::String("t-e2e".to_string()),
    );
    metadata.insert(
        "snorri.item_kind",
        fireweed_core::MetadataValue::String("workflow.entry".to_string()),
    );
    metadata.insert(
        "snorri.run_id",
        fireweed_core::MetadataValue::String("run-1".to_string()),
    );
    metadata.insert("snorri.priority", fireweed_core::MetadataValue::Integer(0));
    let remainder = serde_json::json!({
        "input_event_type": "workflow.entry",
        "input_provenance": "Queued",
        "subject_context": { "subject_ref": { "provider_id": "crm.probe",
            "subject_type": "contact", "subject_id": target },
            "schema_version": null, "snapshot_ref": null, "payload": {} },
    });
    NewItem {
        client_item_key: Some(fireweed_core::ClientItemKey::new(key).unwrap()),
        priority: Some(PriorityValue::Timestamp(scheduled)),
        not_before: Some(scheduled),
        payload: Some(Bytes::from(serde_json::to_vec(&payload).unwrap())),
        fields: [(
            "snorri.execution_rem.v2".to_string(),
            Bytes::from(serde_json::to_vec(&remainder).unwrap()),
        )]
        .into_iter()
        .collect(),
        metadata,
        index_fields,
        entity: Some(serde_json::json!({
            "record_kind": "transition",
            "tenant_id": "t-e2e",
            "account_id": "acct_default",
            "projection_kind": "scheduled_action",
            "projection_schema_version": 1,
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "action_id": key,
            "instance_id": "run-1",
            "target_key": target,
            "scheduled_at": "2023-11-14T22:13:20Z",
            "status": "scheduled",
            "action_type": "message.send",
            "scheduler_algorithm": "personalized",
            "engagement_probability": null,
            "engagement_threshold": 0.10,
            "suppressed_by_recycling": false,
            "is_enrolled_using_open_rate_filter": false,
            "input_event_id": key,
            "workflow_instance_id": "run-1",
            "artifact_digest": "digest-1",
            "workflow_version": "v1",
            "source_state_version": 1,
            "transition": "entry",
            "payload": payload,
        })),
        ..NewItem::default()
    }
}

/// Snorri's `workflow_claim_work` fallback claim path: reclaim expired leases at the probe's
/// lease_time, then a portable claim with explicit eligibility + lease times.
async fn claim_like_snorri(
    fw: &Fireweed,
    q: &QueueKey,
    when: UtcTimestamp,
) -> Vec<fireweed_engine::ClaimedItem> {
    fw.reclaim_expired_at(q, None, when).await.unwrap();
    fw.claim_at(
        q,
        fireweed::ClaimAt::new(10, 60_000)
            .eligibility_time(when)
            .lease_time(when),
    )
    .await
    .unwrap()
}

/// Repro for the snorri enroll gap: on the ASYNC-barrier derived cell, a request-id push of the
/// FULL snorri work-item shape must mint ids that become claimable — exactly like a plain push of
/// the same shape and like a request-id push of a plain item (both green in this file).
#[cfg(feature = "objectlog")]
#[tokio::test]
async fn derived_turso_async_barrier_snorri_shaped_request_id_push_is_claimable() {
    let root = unique_root("async-snorri-shape");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("fixture root");
    let projection_path = root.join("projection.turso");
    let def = snorri_like_definition("snorri-shape");
    let q = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());

    let mut cfg = facade_config(
        LogConfig::Filesystem {
            root: root.join("fs-log"),
        },
        projection_path,
    );
    cfg.response_barrier = ResponseBarrier::AsyncProjection;
    cfg.async_projection = Some(fireweed_engine::AsyncProjectionSpec {
        apply_lag_max_commands: 500,
        apply_debt_max_bytes: 8 * 1024 * 1024,
        apply_queue_depth_max: 64,
        oldest_unapplied_max_ms: 5_000,
        apply_poison_retry_threshold: 5,
        apply_start_delay_ms: 0,
    });
    let fw = open(cfg, Arc::new(SystemClock) as _).expect("open async-barrier turso composition");
    fw.create_queue(def).await.unwrap();

    let when = UtcTimestamp::new(1_700_000_000, 0).unwrap();

    // Probe A: plain push of the full snorri shape.
    let plain_id = fw
        .push(
            &q,
            snorri_like_item("enroll-1:run-1:workflow.entry", "target-a"),
        )
        .await
        .unwrap();
    let _ = fw.side_record(&q, b"probe").await.unwrap();
    eprintln!("metrics after plain push: {:?}", fw.metrics(&q).await);
    let claimed = claim_like_snorri(&fw, &q, when).await;
    assert_eq!(
        claimed.len(),
        1,
        "plain push of the snorri-shaped item must be claimable"
    );
    assert_eq!(claimed[0].item_id, plain_id);

    // Probe B: request-id push (the enroll path) of the same full shape.
    let request_id = RequestId::new("enroll-1.s0").unwrap();
    let pushed = fw
        .push_batch_with_request_id(
            &q,
            request_id.clone(),
            vec![snorri_like_item(
                "enroll-1:run-2:workflow.entry",
                "target-b",
            )],
        )
        .await
        .unwrap()
        .into_item_ids();
    assert_eq!(pushed.len(), 1);
    let _ = fw.side_record(&q, b"probe").await.unwrap();
    eprintln!("metrics after reqid push: {:?}", fw.metrics(&q).await);
    let claimed = claim_like_snorri(&fw, &q, when).await;
    assert_eq!(
        claimed.len(),
        1,
        "request-id push of the snorri-shaped item must be claimable"
    );
    assert_eq!(claimed[0].item_id, pushed[0]);

    // Probe C: snorri's PRIMARY claim path — API-004 claim_by_query on the declared
    // record_kind × scheduled_at index (previously Unavailable on every Turso cell, which forced
    // snorri onto its facade-gated fallback and dropped enroll items).
    let request_id_c = RequestId::new("enroll-2.s0").unwrap();
    let pushed_c = fw
        .push_batch_with_request_id(
            &q,
            request_id_c,
            vec![snorri_like_item(
                "enroll-2:run-3:workflow.entry",
                "target-c",
            )],
        )
        .await
        .unwrap()
        .into_item_ids();
    let query_request = || fireweed_core::ClaimByQueryRequest {
        index: Some("by_record_kind_scheduled_at".to_string()),
        filters: vec![fireweed_core::QueryFilter {
            field: "record_kind".to_string(),
            op: fireweed_core::FilterOp::Eq,
            value: fireweed_core::TypedValue::String("transition".to_string()),
        }],
        order_by: fireweed_core::OrderField {
            field: "scheduled_at".to_string(),
            direction: fireweed_core::SortDirection::Ascending,
        },
        max_items: 10,
        lease_duration_ms: 60_000,
        worker_id: fireweed_core::WorkerId::new("snorri-workflow").unwrap(),
        request_id: Some(RequestId::new("claim-query-1").unwrap()),
    };
    let by_query = fw
        .claim_by_query_at(
            &q,
            query_request(),
            fireweed::ClaimByQueryAt::new()
                .eligibility_time(when)
                .lease_time(when),
        )
        .await
        .unwrap();
    assert_eq!(
        by_query.items.len(),
        1,
        "claim_by_query must serve the snorri transition index on the turso cell"
    );
    assert_eq!(by_query.items[0].item_id, pushed_c[0]);
    assert!(by_query.items[0].lease_token.is_some());
    // Same request id + identical body replays the same lease.
    let replayed = fw
        .claim_by_query_at(
            &q,
            query_request(),
            fireweed::ClaimByQueryAt::new()
                .eligibility_time(when)
                .lease_time(when),
        )
        .await
        .unwrap();
    assert_eq!(replayed.items.len(), 1);
    assert_eq!(replayed.items[0].item_id, pushed_c[0]);
    assert_eq!(
        replayed.items[0].lease_token, by_query.items[0].lease_token,
        "claim_by_query replay returns the retained lease"
    );
    drop(fw);
    let _ = std::fs::remove_dir_all(&root);
}
