//! fireweed-2a564ff7: concurrent direct log appends coalesce seals (group-commit).
//!
//! Product claim/commit serializes on a queue-local admit permit, so multi-worker
//! claim+commit rarely has concurrent appends on one queue. This stress test drives
//! `AsyncLogStore::append` concurrently to prove seals < logical appends.
//!
//! ```text
//! cargo test -p fireweed --test sqlite_log_group_commit_stress --release --features sqlite \
//!   -- --nocapture
//! ```

#![cfg(feature = "sqlite")]

use std::sync::Arc;

use fireweed::*;
use fireweed_core::{ClientItemKey, ItemId, UtcTimestamp};
use fireweed_engine::{
    AsyncLogStore, CommandChecksum, CommandEnvelope, CommandId, PushCommand, PushItem, QueueCommand,
};
use fireweed_memory::ManualClock;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appends_coalesce_group_commit_seals() {
    let path = std::env::temp_dir().join(format!(
        "fw-gc-stress-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let path_s = path.to_string_lossy().into_owned();

    let (fw, backend) =
        open_sqlite_with_lock_stats_handle(&path_s, Arc::new(ManualClock::at(0))).expect("open");

    let def = QueueDefinition {
        tenant_id: TenantId::new("t-gc").unwrap(),
        queue_id: QueueId::new("q-gc").unwrap(),
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
        max_push_batch_size: 10_000,
        max_claim_batch_size: 10_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: false,
    };
    let queue = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    fw.create_queue(def).await.expect("create");
    let epoch = backend
        .log_store()
        .current_epoch(queue.clone())
        .await
        .expect("epoch");

    backend.reset_log_group_commit_stats();

    const N: usize = 64;
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let log = backend.log_store();
        let queue = queue.clone();
        handles.push(tokio::spawn(async move {
            let item_id = ItemId::mint(epoch, 0, i as u32 + 1);
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("c-{i}")),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: vec![item_id],
                command: QueueCommand::Push(PushCommand {
                    items: vec![PushItem {
                        client_item_key: ClientItemKey::new(format!("k-{i}")).unwrap(),
                        item_id,
                        priority: None,
                        not_before: None,
                        group_key: None,
                        max_attempts: 3,
                        payload: None,
                        fields: Default::default(),
                        metadata: Default::default(),
                        cohort_size: None,
                        gate_keys: vec![],
                        index_fields: Default::default(),
                        entity_document: None,
                    }],
                }),
                checksum: CommandChecksum(0),
                created_at: UtcTimestamp {
                    seconds: 0,
                    nanoseconds: 0,
                },
            };
            log.append(queue, vec![env], epoch).await.expect("append")
        }));
    }
    for h in handles {
        let positions = h.await.expect("join");
        assert_eq!(positions.len(), 1);
    }

    let (seals, appends) = backend
        .log_group_commit_stats()
        .expect("group-commit enabled on open_sqlite");
    eprintln!(
        "group-commit stress: seals={seals} appends={appends} (need seals < appends for coalesce)"
    );
    assert_eq!(appends, N as u64, "every waiter should complete");
    assert!(
        seals < appends,
        "group-commit must coalesce: seals={seals} appends={appends}"
    );
    assert!(seals > 0, "at least one seal");

    let _ = std::fs::remove_file(&path);
}
