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

use fireweed::{Bytes, SegmentSettings};
use fireweed_core::{
    EligibilityPolicy, ItemId, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RequestId, RetryPolicy,
    TenantId, UtcTimestamp,
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
