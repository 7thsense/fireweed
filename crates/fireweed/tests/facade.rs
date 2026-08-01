//! The ergonomic library facade exercised over real backends (memory = atomic class; objectlog =
//! eventual-apply class), proving the singular verbs compose the engine ports correctly.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use bytes::Bytes;
use fireweed::{
    ClaimAt, ClaimCompatibility, ClaimRef, CommitEntry, CommitRequest, ControlPlaneConfig,
    DiscoveryGranularity, EngineError, FinalizeKind, GateKeyPolicy, GroupKey, Metadata,
    MetadataValue, Nack, NewItem, OwnerId, PayloadUpdate, RequestId, RuntimeCore, UtcTimestamp,
};
use fireweed_core::{
    ClientItemKey, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_engine::QueueKey;
use fireweed_memory::{ManualClock, composed_memory_backend};
use fireweed_sqlite::SqliteRelationalBackend;

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
        emit_change_records: true,
    }
}

fn at(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

struct GuardedClock {
    seconds: AtomicI64,
    panic_on_read: AtomicBool,
}

impl GuardedClock {
    fn at(seconds: i64) -> Self {
        Self {
            seconds: AtomicI64::new(seconds),
            panic_on_read: AtomicBool::new(false),
        }
    }

    fn reject_reads(&self) {
        self.panic_on_read.store(true, Ordering::SeqCst);
    }
}

impl fireweed::Clock for GuardedClock {
    fn now(&self) -> UtcTimestamp {
        assert!(
            !self.panic_on_read.load(Ordering::SeqCst),
            "reclaim_expired_at must not read the handle clock"
        );
        ts(self.seconds.load(Ordering::SeqCst))
    }
}

fn metadata_fence_item(priority: i64, group: &str, region: &str) -> NewItem {
    let mut metadata = Metadata::new();
    metadata.insert("region", MetadataValue::String(region.to_string()));
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        group_key: Some(GroupKey::new(group).unwrap()),
        metadata,
        ..Default::default()
    }
}

fn metadata_equals_compatibility(group: &str, region: &str) -> ClaimCompatibility {
    ClaimCompatibility {
        group_key: Some(GroupKey::new(group).unwrap()),
        metadata_equals: BTreeMap::from([(
            "region".to_string(),
            MetadataValue::String(region.to_string()),
        )]),
        ..ClaimCompatibility::default()
    }
}

/// Compatibility-constrained item claims must not fail closed as Unavailable on memory projections
/// (AsyncLogReplayBackend / InMemoryProjection path).
#[tokio::test]
async fn claim_with_metadata_equals_filters_over_memory() {
    let fireweed = fireweed::open_memory(Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let match_id = fireweed
        .push(&q, metadata_fence_item(10, "group-a", "east"))
        .await
        .unwrap();
    fireweed
        .push(&q, metadata_fence_item(5, "group-a", "west"))
        .await
        .unwrap();
    fireweed
        .push(&q, metadata_fence_item(1, "group-b", "east"))
        .await
        .unwrap();

    let claimed = fireweed
        .claim_with(&q, 10, 30_000, metadata_equals_compatibility("group-a", "east"))
        .await
        .expect("metadata_equals claim must not be Unavailable on memory");
    assert_eq!(
        claimed.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        vec![match_id]
    );
    assert_eq!(
        claimed[0].group_key.as_ref().map(|g| g.as_str()),
        Some("group-a")
    );
}

/// Same fence over sqlite (log × memory projection via open_sqlite, and relational projection).
#[tokio::test]
async fn claim_with_metadata_equals_filters_over_sqlite() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let log_path = std::env::temp_dir().join(format!(
        "fireweed-facade-compat-claim-log-{}-{nonce}.db",
        std::process::id()
    ));
    let rel_path = std::env::temp_dir().join(format!(
        "fireweed-facade-compat-claim-rel-{}-{nonce}.db",
        std::process::id()
    ));

    async fn exercise(label: &str, fireweed: fireweed::Fireweed) {
        let q = qkey();
        fireweed.create_queue(qdef()).await.unwrap();
        let match_id = fireweed
            .push(&q, metadata_fence_item(10, "group-a", "east"))
            .await
            .unwrap();
        fireweed
            .push(&q, metadata_fence_item(5, "group-a", "west"))
            .await
            .unwrap();
        fireweed
            .push(&q, metadata_fence_item(1, "group-b", "east"))
            .await
            .unwrap();

        let claimed = fireweed
            .claim_with(
                &q,
                10,
                30_000,
                metadata_equals_compatibility("group-a", "east"),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{label}: metadata_equals claim must not fail: {error:?}")
            });
        assert_eq!(
            claimed.iter().map(|item| item.item_id).collect::<Vec<_>>(),
            vec![match_id],
            "{label}: only the group-a/east fence match is claimed"
        );
    }

    exercise(
        "open_sqlite",
        fireweed::open_sqlite(log_path.to_str().unwrap(), Arc::new(ManualClock::at(0))).unwrap(),
    )
    .await;
    exercise(
        "open_sqlite_relational",
        fireweed::open_sqlite_relational(rel_path.to_str().unwrap(), Arc::new(ManualClock::at(0)))
            .unwrap(),
    )
    .await;

    let _ = std::fs::remove_file(log_path);
    let _ = std::fs::remove_file(rel_path);
}

#[tokio::test]
async fn push_claim_ack_nack_lifecycle_over_memory() {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    // push out of priority order.
    for p in [30, 10, 20] {
        fireweed.push(&q, at(p)).await.unwrap();
    }

    // peek is priority-ordered (ascending Int64): 10, 20, 30.
    let peeked: Vec<i64> = fireweed
        .peek(&q, 10)
        .await
        .unwrap()
        .iter()
        .map(|v| match v.priority {
            Some(PriorityValue::Int64(n)) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(peeked, vec![10, 20, 30]);

    // claim 2 highest-priority → 10, 20; both leased.
    let claimed = fireweed.claim(&q, 2, 30_000).await.unwrap();
    let claimed_pri: Vec<i64> = claimed
        .iter()
        .map(|c| match c.priority {
            Some(PriorityValue::Int64(n)) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(claimed_pri, vec![10, 20]);
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!((m.pending, m.leased), (1, 2));

    // ack them → complete.
    fireweed
        .ack(&q, claimed.iter().map(|c| c.item_id))
        .await
        .unwrap();
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!((m.complete, m.leased), (2, 0));

    // claim the last (30), nack Retry → back to pending, claimable again with a bumped attempt.
    let last = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].attempt_count, 1);
    fireweed
        .nack(
            &q,
            last.iter().map(|c| c.item_id),
            Nack::Retry { not_before: None },
        )
        .await
        .unwrap();
    let again = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(again.len(), 1, "retried item is claimable again");
    assert!(again[0].attempt_count > 1, "redelivery bumps attempt_count");
}

#[tokio::test]
async fn lifecycle_convenience_verbs_preserve_batch_state_and_structured_errors() {
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(Arc::new(composed_memory_backend()), clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let leased_id = fireweed.push(&q, at(10)).await.unwrap();
    let pending_id = fireweed.push(&q, at(20)).await.unwrap();
    assert_eq!(
        fireweed.claim(&q, 1, 30_000).await.unwrap()[0].item_id,
        leased_id
    );

    let complete_error = fireweed
        .complete(&q, [leased_id, pending_id])
        .await
        .unwrap_err();
    let existing_error = fireweed.ack(&q, [leased_id, pending_id]).await.unwrap_err();
    assert_eq!(complete_error, existing_error);
    let unchanged = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (unchanged.pending, unchanged.leased, unchanged.complete),
        (1, 1, 0)
    );

    fireweed.complete(&q, [leased_id]).await.unwrap();
    let released = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(released[0].item_id, pending_id);
    fireweed.release(&q, [pending_id]).await.unwrap();
    let after_release = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (
            after_release.pending,
            after_release.leased,
            after_release.complete
        ),
        (1, 0, 1)
    );
}

#[tokio::test]
async fn retry_aliases_match_absolute_relative_and_exhaustion_behavior() {
    let clock = Arc::new(ManualClock::at(100));
    let fireweed = RuntimeCore::new(Arc::new(composed_memory_backend()), clock.clone());
    let q = qkey();
    let mut definition = qdef();
    definition.retry_policy.max_attempts = 2;
    fireweed.create_queue(definition).await.unwrap();

    for priority in [10, 20, 30, 40] {
        fireweed.push(&q, at(priority)).await.unwrap();
    }
    let claimed = fireweed.claim(&q, 4, 30_000).await.unwrap();
    fireweed
        .retry(&q, [claimed[0].item_id], Some(ts(110)))
        .await
        .unwrap();
    fireweed
        .nack(
            &q,
            [claimed[1].item_id],
            Nack::Retry {
                not_before: Some(ts(110)),
            },
        )
        .await
        .unwrap();
    fireweed
        .retry_after(&q, [claimed[2].item_id], 20_000)
        .await
        .unwrap();
    fireweed
        .nack_retry_after(&q, [claimed[3].item_id], 20_000)
        .await
        .unwrap();

    clock.set(109);
    assert!(fireweed.claim(&q, 4, 30_000).await.unwrap().is_empty());
    clock.set(110);
    let absolute = fireweed.claim(&q, 4, 30_000).await.unwrap();
    assert_eq!(absolute.len(), 2);
    assert_eq!(
        absolute.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        vec![claimed[0].item_id, claimed[1].item_id,]
    );
    fireweed
        .retry(&q, absolute.iter().map(|item| item.item_id), None)
        .await
        .unwrap();
    let exhausted = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        exhausted.failed, 2,
        "retry exhaustion matches nack retry semantics"
    );

    clock.set(119);
    assert!(fireweed.claim(&q, 4, 30_000).await.unwrap().is_empty());
    clock.set(120);
    let relative = fireweed.claim(&q, 4, 30_000).await.unwrap();
    assert_eq!(relative.len(), 2);
    assert_eq!(
        relative.iter().map(|item| item.item_id).collect::<Vec<_>>(),
        vec![claimed[2].item_id, claimed[3].item_id,]
    );
}

#[tokio::test]
async fn discover_alias_preserves_exact_scope_order() {
    let clock = Arc::new(ManualClock::at(0));
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "fireweed-facade-discover-alias-{}-{nonce}.db",
        std::process::id()
    ));
    let backend = Arc::new(SqliteRelationalBackend::open(path.to_str().unwrap()).unwrap());
    let fireweed = RuntimeCore::new(backend, clock.clone());
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let mut old = at(10);
    old.group_key = Some(GroupKey::new("old").unwrap());
    fireweed.push(&q, old).await.unwrap();
    clock.set(10);
    let mut new = at(20);
    new.group_key = Some(GroupKey::new("new").unwrap());
    fireweed.push(&q, new).await.unwrap();
    clock.set(20);

    let existing = fireweed
        .discover_active_scopes(&q, DiscoveryGranularity::Group)
        .await
        .unwrap();
    let alias = fireweed
        .discover(&q, DiscoveryGranularity::Group)
        .await
        .unwrap();
    assert_eq!(alias, existing);
    assert_eq!(
        alias
            .iter()
            .map(|scope| scope.group_key.clone())
            .collect::<Vec<_>>(),
        vec![Some("old".to_string()), Some("new".to_string())]
    );

    drop(fireweed);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn stamped_discovery_preserves_ungrouped_order_through_relational_constructor() {
    let clock = Arc::new(ManualClock::at(10));
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "fireweed-facade-stamped-discovery-{}-{nonce}.db",
        std::process::id()
    ));
    let fireweed = fireweed::open_sqlite_relational(path.to_str().unwrap(), clock).unwrap();
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let ungrouped = at(10);
    let mut keyed = at(20);
    keyed.group_key = Some(GroupKey::new("keyed").unwrap());
    fireweed
        .push_batch(&q, vec![ungrouped, keyed])
        .await
        .unwrap();

    let existing = fireweed
        .discover_active_scopes(&q, DiscoveryGranularity::Group)
        .await
        .unwrap();
    let stamped = fireweed
        .discover_active_scopes_stamped(&q, DiscoveryGranularity::Group)
        .await
        .unwrap();
    assert_eq!(stamped.queue, q);
    assert_eq!(stamped.granularity, DiscoveryGranularity::Group);
    assert_eq!(stamped.scopes, existing);
    assert_eq!(
        stamped
            .scopes
            .iter()
            .map(|scope| scope.group_key.as_deref())
            .collect::<Vec<_>>(),
        vec![None, Some("keyed")]
    );

    drop(fireweed);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn request_id_push_replays_over_sqlite_relational_facade() {
    let clock = Arc::new(ManualClock::at(0));
    let path = std::env::temp_dir()
        .join(format!(
            "fireweed-facade-request-id-{}.db",
            std::process::id()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&path);
    let backend = Arc::new(SqliteRelationalBackend::open(&path).unwrap());
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let request_id = RequestId::new("push-req-1").unwrap();

    let first = fireweed
        .push_batch_with_request_id(&q, request_id.clone(), vec![at(10), at(20)])
        .await
        .unwrap();
    let replay = fireweed
        .push_batch_with_request_id(&q, request_id, vec![at(10), at(20)])
        .await
        .unwrap();

    assert!(first.is_fresh());
    assert!(replay.is_replayed());
    assert_eq!(replay.item_ids, first.item_ids);
    assert_eq!(fireweed.metrics(&q).await.unwrap().pending, 2);
    drop(fireweed);
    let _ = std::fs::remove_file(&path);
}

/// fireweed-01802c42: facade-level request-id retention across reopen on the public
/// `open_sqlite` async log-replay product (Fresh / Replayed / RequestIdConflict).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn request_id_push_retention_survives_open_sqlite_reopen() {
    use fireweed::open_sqlite;
    let path = std::env::temp_dir()
        .join(format!(
            "fireweed-facade-request-id-reopen-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&path);
    let q = qkey();
    let rid = RequestId::new("facade-reopen-req").unwrap();
    let first_ids = {
        let fireweed = open_sqlite(&path, Arc::new(ManualClock::at(0))).unwrap();
        fireweed.create_queue(qdef()).await.unwrap();
        assert!(
            fireweed
                .commit_capabilities(&q)
                .unwrap()
                .retained_commit_idempotency
        );
        let first = fireweed
            .push_batch_with_request_id(&q, rid.clone(), vec![at(10), at(20)])
            .await
            .unwrap();
        assert!(first.is_fresh());
        let replay = fireweed
            .push_batch_with_request_id(&q, rid.clone(), vec![at(10), at(20)])
            .await
            .unwrap();
        assert!(replay.is_replayed());
        assert_eq!(replay.item_ids, first.item_ids);
        assert_eq!(
            fireweed
                .push_batch_with_request_id(&q, rid.clone(), vec![at(11), at(22)])
                .await
                .unwrap_err(),
            EngineError::RequestIdConflict
        );
        first.item_ids.clone()
    };
    let reopened = open_sqlite(&path, Arc::new(ManualClock::at(0))).unwrap();
    reopened.create_queue(qdef()).await.unwrap();
    let replay = reopened
        .push_batch_with_request_id(&q, rid.clone(), vec![at(10), at(20)])
        .await
        .unwrap();
    assert!(
        replay.is_replayed(),
        "facade reopen must Replayed same body under retained request id"
    );
    assert_eq!(replay.item_ids, first_ids);
    assert_eq!(
        reopened
            .push_batch_with_request_id(&q, rid, vec![at(11), at(22)])
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict,
        "facade reopen must RequestIdConflict on changed body"
    );
    drop(reopened);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn request_id_push_is_idempotent_on_memory_backend() {
    // The memory reference backend now wires the retained request-id idempotency cache (ddx-pqueue-2201fd37,
    // foundation for the Snorri authoritative commit boundary): a request-id'd push succeeds and a same-body
    // replay returns the original id without a second append. Full replay/conflict/expired coverage lives in
    // `tests/request_id_idempotency.rs`.
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let rid = RequestId::new("push-req-1").unwrap();
    let (first, first_disp) = fireweed
        .push_with_request_id(&q, rid.clone(), at(10))
        .await
        .unwrap();
    let (replay, replay_disp) = fireweed
        .push_with_request_id(&q, rid, at(10))
        .await
        .unwrap();

    assert_eq!(first_disp, fireweed::PushDisposition::Fresh);
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(first, replay, "same request id + same body replays the id");
    assert_eq!(
        fireweed.metrics(&q).await.unwrap().pending,
        1,
        "replay must not enqueue a duplicate"
    );
}

#[tokio::test]
async fn claimed_item_exposes_api001_shape_over_facade() {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(100));
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    let mut def = qdef();
    def.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    def.eligibility_policy.max_gate_keys_per_item = Some(8);
    fireweed.create_queue(def).await.unwrap();

    // NB: no `gate_keys` here — the memory reference backend is not gate-capable (`supports_gates()` is
    // false), so the in-tree gate-validation guard rejects a gate-bearing push on it. Gate round-trip in
    // the claimed-item shape is covered against a gate-capable (relational) backend.
    let mut item = NewItem {
        priority: Some(PriorityValue::Int64(7)),
        group_key: Some(GroupKey::new("group-a").unwrap()),
        not_before: Some(UtcTimestamp::new(100, 0).unwrap()),
        payload: Some(Bytes::from_static(b"opaque")),
        fields: BTreeMap::from([("field-a".to_string(), Bytes::from_static(b"value-a"))]),
        ..Default::default()
    };
    item.metadata
        .insert("tenant_segment", MetadataValue::String("vip".to_string()));

    let id = fireweed.push(&q, item).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let got = &claimed[0];
    assert_eq!(got.item_id, id);
    assert_eq!(got.client_item_key.as_str(), id.to_string());
    assert_eq!(got.item_version, 2);
    assert_eq!(got.priority, Some(PriorityValue::Int64(7)));
    assert_eq!(got.group_key, Some(GroupKey::new("group-a").unwrap()));
    assert_eq!(got.not_before, Some(UtcTimestamp::new(100, 0).unwrap()));
    assert!(got.lease_token.is_some());
    assert_eq!(got.lease_expires_at, UtcTimestamp::new(130, 0).unwrap());
    assert_eq!(got.payload.as_deref(), Some(&b"opaque"[..]));
    assert_eq!(
        got.fields.get("field-a").map(|bytes| bytes.as_ref()),
        Some(&b"value-a"[..])
    );
    assert_eq!(
        got.metadata.get("tenant_segment"),
        Some(&MetadataValue::String("vip".to_string()))
    );
    assert!(
        got.gate_keys.is_empty(),
        "no gate keys carried on the non-gate-capable memory backend"
    );
}

#[tokio::test]
async fn upsert_dedups_on_client_item_key_over_memory() {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let key = ClientItemKey::new("dup").unwrap();
    fireweed.upsert(&q, key.clone(), at(50)).await.unwrap();
    fireweed.upsert(&q, key, at(20)).await.unwrap(); // replaces the pending item

    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(m.pending, 1, "same key upserts to a single pending item");
    let peeked: Vec<i64> = fireweed
        .peek(&q, 10)
        .await
        .unwrap()
        .iter()
        .map(|v| match v.priority {
            Some(PriorityValue::Int64(n)) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(peeked, vec![20], "the replacement's priority survives");
}

#[tokio::test]
#[ignore = "objectlog LogEngine product stubs UpsertPort as Unavailable (Eventual product; not yet wired)"]
async fn composed_objectlog_supports_atomic_upsert() {
    use fireweed_objectlog::composed_objectlog_backend;
    let root = std::env::temp_dir().join(format!("fireweed-facade-objlog-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let backend = Arc::new(composed_objectlog_backend(&root).unwrap());
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let key = ClientItemKey::new("k").unwrap();
    let first = fireweed.upsert(&q, key.clone(), at(5)).await.unwrap();
    let first_id = match first {
        fireweed_engine::UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("first upsert must insert, got {other:?}"),
    };
    let second = fireweed.upsert(&q, key, at(3)).await.unwrap();
    match second {
        fireweed_engine::UpsertOutcome::Replaced {
            superseded_item_id, ..
        } => assert_eq!(superseded_item_id, first_id),
        other => panic!("second upsert must replace, got {other:?}"),
    }
    assert_eq!(fireweed.metrics(&q).await.unwrap().pending, 1);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn two_handles_on_one_backend_do_not_collide_ids() {
    // B2 regression: ids are backend-assigned (not a per-handle counter), so two `RuntimeCore` handles
    // sharing one backend mint DISTINCT item ids and both items coexist.
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let a = RuntimeCore::new(backend.clone(), clock.clone());
    let b = RuntimeCore::new(backend.clone(), clock);
    let q = qkey();
    a.create_queue(qdef()).await.unwrap();

    let id_a = a.push(&q, at(10)).await.unwrap();
    let id_b = b.push(&q, at(20)).await.unwrap();
    assert_ne!(id_a, id_b, "distinct backend-assigned ids across handles");
    assert_eq!(
        a.metrics(&q).await.unwrap().pending,
        2,
        "both pushes coexist (no silent overwrite)"
    );
}

#[tokio::test]
async fn ack_of_non_leased_id_is_a_structured_error() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let id = fireweed.push(&q, at(5)).await.unwrap(); // pending, never claimed
    let err = fireweed.ack(&q, [id]).await.unwrap_err();
    assert_eq!(
        err,
        EngineError::Invalid("item is not leased"),
        "ack of a never-leased item is rejected, not a silent success"
    );
}

#[tokio::test]
async fn fail_dead_letters_a_claimed_item() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed.push(&q, at(5)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    fireweed
        .fail(&q, claimed.iter().map(|c| c.item_id))
        .await
        .unwrap();
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!(
        (m.failed, m.leased),
        (1, 0),
        "fail moves the item to terminal failed"
    );
}

#[tokio::test]
async fn renew_extends_lease_without_charging_a_delivery() {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed.push(&q, at(5)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap(); // lease_expires_at = 30s, attempt 1
    let id = claimed[0].item_id;
    assert_eq!(claimed[0].attempt_count, 1);

    // Renew to 60s from now: the lease deadline extends, the delivery count does NOT change.
    fireweed.renew(&q, [id], 60_000).await.unwrap();
    let view = fireweed
        .claimed(&q, std::slice::from_ref(&id))
        .await
        .unwrap();
    assert_eq!(view.len(), 1);
    assert_eq!(view[0].attempt_count, 1, "renew does not charge a delivery");
    assert_eq!(
        view[0].lease_expires_at,
        fireweed_core::UtcTimestamp::new(60, 0).unwrap(),
        "renew extended the lease deadline"
    );
}

#[tokio::test]
async fn reassign_transfers_and_charges_one_delivery() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed.push(&q, at(5)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap(); // attempt 1
    let id = claimed[0].item_id;

    fireweed.reassign(&q, [id], 30_000).await.unwrap();
    let view = fireweed
        .claimed(&q, std::slice::from_ref(&id))
        .await
        .unwrap();
    assert_eq!(
        view[0].attempt_count, 2,
        "reassign is a re-delivery (claim 1 + reassign 1)"
    );
    assert_eq!(
        fireweed.metrics(&q).await.unwrap().leased,
        1,
        "still leased under the new owner"
    );
}

#[tokio::test]
async fn rearm_resets_attempt_and_requeues_the_item() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed.push(&q, at(5)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(claimed[0].attempt_count, 1);

    // Re-arm: the item returns to pending with attempt_count reset to 0.
    fireweed
        .rearm(&q, claimed.iter().map(|c| c.item_id))
        .await
        .unwrap();
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!((m.pending, m.leased), (1, 0), "rearm re-queues the item");
    let again = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(
        again[0].attempt_count, 1,
        "the fresh delivery starts at 1 (attempt was reset)"
    );
}

#[tokio::test]
async fn purge_force_removes_a_leased_item_and_gates_without_force() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed.push(&q, at(5)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    let id = claimed[0].item_id;

    // Without force, purging a leased item is a structured Conflict (nothing removed).
    assert_eq!(
        fireweed.purge(&q, [id], false).await.unwrap_err(),
        EngineError::Conflict
    );
    assert_eq!(fireweed.metrics(&q).await.unwrap().leased, 1);
    // With force, it is removed; the count reflects one removal.
    assert_eq!(fireweed.purge(&q, [id], true).await.unwrap(), 1);
    assert_eq!(
        fireweed.metrics(&q).await.unwrap().leased,
        0,
        "force-purged"
    );
}

#[tokio::test]
async fn claimed_renders_only_leased_items() {
    let backend = Arc::new(composed_memory_backend());
    let fireweed = RuntimeCore::new(backend, Arc::new(ManualClock::at(0)));
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let lo = fireweed.push(&q, at(5)).await.unwrap(); // top priority
    let hi = fireweed.push(&q, at(9)).await.unwrap(); // stays pending
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(claimed[0].item_id, lo);

    let view = fireweed.claimed(&q, &[lo, hi]).await.unwrap();
    assert_eq!(
        view.len(),
        1,
        "only the leased item renders; the pending one is omitted"
    );
    assert_eq!(view[0].item_id, lo);
}

fn with_fields(priority: i64, fields: &[(&str, &[u8])], payload: &[u8]) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(Bytes::copy_from_slice(payload)),
        fields: fields
            .iter()
            .map(|(k, v)| (k.to_string(), Bytes::copy_from_slice(v)))
            .collect(),
        ..Default::default()
    }
}

/// FAC-1: `update_fields` merges a leased item's hot-storage fields/payload in place (set + remove),
/// bumps `item_version`, and honors the optimistic `expected_item_version` CAS.
#[tokio::test]
async fn update_fields_merges_versions_and_cas_over_memory() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let id = fireweed
        .push(&q, with_fields(5, &[("a", b"1"), ("b", b"2")], b"p0"))
        .await
        .unwrap();
    // Lease it, then mutate the leased item in place — the path that upsert/replace-if-pending refuses.
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(claimed[0].item_id, id);

    let ops = BTreeMap::from([
        ("a".to_string(), Some(Bytes::from_static(b"9"))), // overwrite
        ("b".to_string(), None),                           // remove
        ("c".to_string(), Some(Bytes::from_static(b"3"))), // add
    ]);
    let v = fireweed
        .update_fields(
            &q,
            id,
            ops,
            PayloadUpdate::Set(Some(Bytes::from_static(b"p1"))),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(v >= 2, "item_version bumped past the genesis 1");

    let key = ClientItemKey::new(id.to_string()).unwrap();
    let live = fireweed
        .live_item(&q, key.clone())
        .await
        .unwrap()
        .expect("live");
    assert_eq!(live.fields.get("a").map(|b| b.as_ref()), Some(&b"9"[..]));
    assert_eq!(live.fields.get("c").map(|b| b.as_ref()), Some(&b"3"[..]));
    assert!(!live.fields.contains_key("b"), "removed key is gone");
    assert_eq!(live.payload.as_deref(), Some(&b"p1"[..]));
    assert_eq!(live.item_version, v);

    // A stale CAS rejects with Conflict and commits nothing.
    let stale = fireweed
        .update_fields(
            &q,
            id,
            BTreeMap::from([("a".to_string(), Some(Bytes::from_static(b"x")))]),
            PayloadUpdate::Keep,
            None,
            Some(v - 1),
        )
        .await;
    assert!(matches!(stale, Err(EngineError::Conflict)));
    let live2 = fireweed.live_item(&q, key).await.unwrap().expect("live");
    assert_eq!(
        live2.item_version, v,
        "rejected CAS left the item unchanged"
    );
    assert_eq!(live2.fields.get("a").map(|b| b.as_ref()), Some(&b"9"[..]));
}

/// FAC-1: a terminal item rejects `update_fields` with the structured `Terminal` (parity with finalize).
#[tokio::test]
async fn update_fields_rejects_terminal_over_memory() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let id = fireweed.push(&q, at(5)).await.unwrap();
    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    fireweed
        .ack(&q, claimed.iter().map(|c| c.item_id))
        .await
        .unwrap();
    let r = fireweed
        .update_fields(&q, id, BTreeMap::new(), PayloadUpdate::Keep, None, None)
        .await;
    assert!(matches!(r, Err(EngineError::Terminal)));
}

/// API-001: reserved write-field names are blocked before the library facade dispatches to the backend.
#[tokio::test]
async fn api001_reservation_policy_is_recorded_or_enforced() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let id = fireweed.push(&q, at(5)).await.unwrap();
    fireweed.claim(&q, 1, 30_000).await.unwrap();

    let payload_ok = fireweed
        .update_fields(
            &q,
            id,
            BTreeMap::new(),
            PayloadUpdate::Set(Some(Bytes::from_static(b"payload-1"))),
            None,
            None,
        )
        .await
        .unwrap();
    assert!(payload_ok >= 2);

    let err = fireweed
        .update_fields(
            &q,
            id,
            BTreeMap::from([
                (
                    "lease_token".to_string(),
                    Some(Bytes::from_static(b"user-value")),
                ),
                (
                    "payload".to_string(),
                    Some(Bytes::from_static(b"user-payload")),
                ),
            ]),
            PayloadUpdate::Keep,
            None,
            None,
        )
        .await
        .expect_err("reserved names must be rejected");
    assert!(matches!(err, EngineError::Invalid(_)));
    let live = fireweed
        .live_item(&q, ClientItemKey::new(id.to_string()).unwrap())
        .await
        .unwrap()
        .expect("live");
    assert_eq!(live.payload.as_deref(), Some(&b"payload-1"[..]));
}

#[tokio::test]
#[ignore = "objectlog LogEngine product stubs UpdateFieldsPort as Unavailable (not yet wired on AsyncObjectLog*Backend)"]
async fn composed_objectlog_supports_read_your_write_field_mutation() {
    use fireweed_objectlog::composed_objectlog_backend;
    let root =
        std::env::temp_dir().join(format!("fireweed-facade-uf-objlog-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let fireweed = RuntimeCore::new(
        Arc::new(composed_objectlog_backend(&root).unwrap()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let id = fireweed.push(&q, at(5)).await.unwrap();
    let version = fireweed
        .update_fields(
            &q,
            id,
            BTreeMap::from([("state".to_owned(), Some(Bytes::from_static(b"ready")))]),
            PayloadUpdate::Set(Some(Bytes::from_static(b"updated"))),
            None,
            Some(1),
        )
        .await
        .unwrap();
    assert_eq!(version, 2);
    let live = fireweed
        .live_item(&q, ClientItemKey::new(id.to_string()).unwrap())
        .await
        .unwrap()
        .expect("updated item remains live");
    assert_eq!(
        live.fields.get("state").map(Bytes::as_ref),
        Some(&b"ready"[..])
    );
    assert_eq!(live.payload.as_deref(), Some(&b"updated"[..]));
    let _ = std::fs::remove_dir_all(&root);
}

/// FAC-2: `reclaim_expired` is the host-driven, per-queue lease sweep — expired leases return to Pending
/// (claimable again), the reclaimed ids are returned, and it is idempotent.
#[tokio::test]
async fn reclaim_expired_convenience_uses_handle_clock() {
    let clock = Arc::new(ManualClock::at(0));
    let fireweed = RuntimeCore::new(Arc::new(composed_memory_backend()), clock.clone());
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();
    let id = fireweed.push(&q, at(5)).await.unwrap();
    fireweed.claim(&q, 1, 30_000).await.unwrap(); // lease for 30s
    assert_eq!(fireweed.metrics(&q).await.unwrap().leased, 1);

    // Before the lease expires: nothing to reclaim (half-open — still valid at the boundary).
    clock.set(10);
    assert!(fireweed.reclaim_expired(&q, None).await.unwrap().is_empty());

    // Past the 30s lease: the sweep returns the id and the item is Pending again.
    clock.set(40);
    let reclaimed = fireweed.reclaim_expired(&q, None).await.unwrap();
    assert_eq!(reclaimed, vec![id]);
    let m = fireweed.metrics(&q).await.unwrap();
    assert_eq!((m.pending, m.leased), (1, 0));
    // Idempotent: a second sweep finds nothing.
    assert!(fireweed.reclaim_expired(&q, None).await.unwrap().is_empty());
    // And the item is claimable again.
    assert_eq!(fireweed.claim(&q, 1, 30_000).await.unwrap().len(), 1);
}

#[tokio::test]
async fn reclaim_expired_at_honors_caller_time_without_reading_handle_clock() {
    let clock = Arc::new(GuardedClock::at(0));
    let backend = Arc::new(composed_memory_backend());
    let setup = RuntimeCore::new(backend.clone(), clock.clone());
    let q = qkey();
    setup.create_queue(qdef()).await.unwrap();
    let id = setup.push(&q, at(5)).await.unwrap();
    let claimed = setup
        .claim_at(
            &q,
            ClaimAt::new(1, 10_000)
                .eligibility_time(ts(10))
                .lease_time(ts(10)),
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);

    let control_plane: Arc<dyn fireweed_engine::QueueControlPlane> = Arc::new(
        fireweed_engine::InMemoryControlPlane::new(ControlPlaneConfig::default()),
    );
    let fireweed = RuntimeCore::with_control_plane_in_process(
        backend,
        clock.clone(),
        OwnerId::new("reclaim-owner").unwrap(),
        control_plane,
    );
    clock.reject_reads();
    assert!(
        fireweed
            .reclaim_expired_at(&q, None, ts(19))
            .await
            .unwrap()
            .is_empty(),
        "a caller time before lease expiry must not reclaim"
    );
    assert_eq!(
        fireweed.reclaim_expired_at(&q, None, ts(21)).await.unwrap(),
        vec![id],
        "a caller time after lease expiry must reclaim"
    );
}

#[tokio::test]
async fn reclaim_expired_at_is_deterministic_under_divergent_frozen_clocks() {
    let left_clock = Arc::new(ManualClock::at(0));
    let right_clock = Arc::new(ManualClock::at(0));
    let left = RuntimeCore::new(Arc::new(composed_memory_backend()), left_clock.clone());
    let right = RuntimeCore::new(Arc::new(composed_memory_backend()), right_clock.clone());
    let q = qkey();

    for fireweed in [&left, &right] {
        fireweed.create_queue(qdef()).await.unwrap();
        fireweed.push(&q, at(5)).await.unwrap();
        fireweed
            .claim_at(
                &q,
                ClaimAt::new(1, 10_000)
                    .eligibility_time(ts(10))
                    .lease_time(ts(10)),
            )
            .await
            .unwrap();
    }

    left_clock.set(-1_000);
    right_clock.set(1_000_000);
    let left_result = left.reclaim_expired_at(&q, None, ts(21)).await.unwrap();
    let right_result = right.reclaim_expired_at(&q, None, ts(21)).await.unwrap();

    assert_eq!(left_result, right_result);
    assert_eq!(
        left.metrics(&q).await.unwrap(),
        right.metrics(&q).await.unwrap()
    );
}

// ---------------------------------------------------------------------------
// ADR-011 JSON entity document support (pqueue-1b13d001)
// ---------------------------------------------------------------------------

/// Opaque-byte items — bytes payload + BTreeMap<String, Bytes> fields — push and claim with
/// full fidelity through the current NewItem shape. This test pins the supported binary representation so a
/// future typed-payload refactor cannot silently break it.
#[tokio::test]
async fn opaque_bytes_items_push_claim_roundtrip() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let opaque_payload = Bytes::from_static(b"\x00\x01\x02\x03opaque-non-utf8");
    let id = fireweed
        .push(
            &q,
            NewItem {
                priority: Some(PriorityValue::Int64(1)),
                payload: Some(opaque_payload.clone()),
                fields: BTreeMap::from([
                    ("k1".to_string(), Bytes::from_static(b"v1")),
                    ("k2".to_string(), Bytes::from_static(b"\xff\xfe")),
                ]),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].item_id, id);
    assert_eq!(claimed[0].payload.as_deref(), Some(opaque_payload.as_ref()));
    assert_eq!(
        claimed[0].fields.get("k1").map(|b| b.as_ref()),
        Some(&b"v1"[..])
    );
    assert_eq!(
        claimed[0].fields.get("k2").map(|b| b.as_ref()),
        Some(&b"\xff\xfe"[..])
    );
}

/// Typed JSON items — payload is a JSON document as UTF-8 bytes — round-trip through push/claim AND through
/// the vectorized commit lifecycle path (lifecycle_items in CommitEntry). Proves the canonical JSON
/// representation survives both paths identically: the bytes carrier is agnostic to payload content.
#[tokio::test]
async fn typed_json_payload_round_trips_push_claim_and_commit_lifecycle() {
    let fireweed = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    let json_doc = Bytes::from_static(br#"{"status":"pending","count":42,"tags":["a","b"]}"#);

    // --- push/claim path ---
    let id = fireweed
        .push(
            &q,
            NewItem {
                priority: Some(PriorityValue::Int64(1)),
                payload: Some(json_doc.clone()),
                fields: BTreeMap::from([
                    ("entity_type".to_string(), Bytes::from_static(b"job")),
                    ("tenant".to_string(), Bytes::from_static(b"acme")),
                ]),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let claimed = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].item_id, id);
    // JSON payload survives the push/claim round-trip verbatim.
    assert_eq!(claimed[0].payload.as_deref(), Some(json_doc.as_ref()));
    // Fields survive alongside the JSON payload.
    assert_eq!(
        claimed[0].fields.get("entity_type").map(|b| b.as_ref()),
        Some(&b"job"[..])
    );

    let input_ref = ClaimRef {
        item_id: claimed[0].item_id,
        lease_token: claimed[0].lease_token.clone().expect("lease token"),
        lease_expires_at: claimed[0].lease_expires_at,
        item_version: claimed[0].item_version,
    };

    // --- vectorized commit lifecycle path ---
    let lifecycle_json = Bytes::from_static(br#"{"status":"dispatched","parent":"root"}"#);
    let outcomes = fireweed
        .commit(
            &q,
            CommitRequest {
                request_id: None,
                entries: vec![CommitEntry {
                    claim_ref: input_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![NewItem {
                        priority: Some(PriorityValue::Int64(2)),
                        payload: Some(lifecycle_json.clone()),
                        ..Default::default()
                    }],
                    instance_fence: None,
                }],
            },
        )
        .await
        .unwrap();

    assert_eq!(outcomes.len(), 1);
    let lifecycle_ids = match &outcomes[0] {
        fireweed::EntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids.clone(),
        other => panic!("expected Committed, got {other:?}"),
    };
    assert_eq!(lifecycle_ids.len(), 1, "one lifecycle item enqueued");

    // Claim the lifecycle item and verify the JSON payload survived the commit path.
    let follow_up = fireweed.claim(&q, 1, 30_000).await.unwrap();
    assert_eq!(follow_up.len(), 1);
    assert_eq!(follow_up[0].item_id, lifecycle_ids[0]);
    assert_eq!(
        follow_up[0].payload.as_deref(),
        Some(lifecycle_json.as_ref()),
        "JSON payload in lifecycle_item survives commit path verbatim"
    );
}

/// Scheduled-work selection at a caller-resolved execution epoch: `claim_at` decides due-ness at
/// `eligibility_time` while the lease keeps running off the handle's clock. The clock is NEVER moved to
/// fake the epoch — that would be a shared mutation visible to every other caller on this handle (and would
/// silently re-date pushes and lease expiries) — so the operational time here deliberately sits BEFORE the
/// epoch being selected for.
#[tokio::test]
async fn claim_at_resolves_eligibility_at_an_explicit_epoch_over_memory() {
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(0)); // operational time: ts(0), and it stays there
    let fireweed = RuntimeCore::new(backend, clock);
    let q = qkey();
    fireweed.create_queue(qdef()).await.unwrap();

    // Three items scheduled ahead of the operational clock (ascending priority = the claim order).
    for (priority, not_before) in [(10, 100), (20, 200), (30, 300)] {
        fireweed
            .push(
                &q,
                NewItem {
                    not_before: Some(UtcTimestamp::new(not_before, 0).unwrap()),
                    ..at(priority)
                },
            )
            .await
            .unwrap();
    }

    // Nothing is due at the operational clock, so the ordinary claim is empty...
    assert!(
        fireweed.claim(&q, 10, 60_000).await.unwrap().is_empty(),
        "no item is due at the operational clock"
    );

    // ...but a claim resolved AT the ts(200) execution epoch takes the work scheduled by then. The
    // boundary is inclusive, so the item scheduled exactly at 200 is due; the one at 300 is not.
    let due = fireweed
        .claim_at(
            &q,
            ClaimAt::new(10, 60_000).eligibility_time(UtcTimestamp::new(200, 0).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(
        due.iter().map(|i| i.not_before).collect::<Vec<_>>(),
        vec![
            Some(UtcTimestamp::new(100, 0).unwrap()),
            Some(UtcTimestamp::new(200, 0).unwrap()),
        ],
        "due at the eligibility epoch: not_before <= 200, inclusive at the boundary"
    );
    for item in &due {
        assert_eq!(
            item.lease_expires_at,
            UtcTimestamp::new(60, 0).unwrap(),
            "lease is measured from the operational clock (0) + 60s — never from the eligibility epoch"
        );
    }

    // The eligibility epoch selected work without disturbing the clock: the remaining item is still not
    // due at operational time, and a plain claim still sees an empty queue.
    assert!(
        fireweed.claim(&q, 10, 60_000).await.unwrap().is_empty(),
        "claim_at left the handle's clock untouched"
    );

    // lease_time is independently steerable: select the last item at its epoch, but anchor its lease to a
    // caller-supplied operational instant.
    let late = fireweed
        .claim_at(
            &q,
            ClaimAt::new(10, 60_000)
                .eligibility_time(UtcTimestamp::new(300, 0).unwrap())
                .lease_time(UtcTimestamp::new(1_000, 0).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(late.len(), 1, "only the item scheduled at 300 was left");
    assert_eq!(
        late[0].lease_expires_at,
        UtcTimestamp::new(1_060, 0).unwrap(),
        "lease expiry is lease_time + duration"
    );
}
