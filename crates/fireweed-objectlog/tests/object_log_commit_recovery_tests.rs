//! Segment accounting and manifest recovery tests for the supported object-log primitives.

use fireweed_conformance::{envelope, item};
use fireweed_core::{
    EligibilityPolicy, ItemId, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_engine::{CommandEnvelope, CommandPosition, PushCommand, QueueCommand};
use fireweed_objectlog::{
    LocalObjectLog, ObjectLogSegmentConfig, SegmentConfig,
    segmented::{BlobStore, InMemoryBlobStore, SegmentedObjectLog},
};

/// The E0 per-queue throughput floor (TP-002): 10,000,000 accepted items/hr == 2,777.78 items/s.
#[derive(Default)]
struct FailingDeleteBlobStore {
    inner: InMemoryBlobStore,
    fail_delete: std::sync::Mutex<Option<String>>,
}

impl FailingDeleteBlobStore {
    fn arm_delete(&self, substr: &str) {
        *self.fail_delete.lock().unwrap() = Some(substr.to_owned());
    }

    fn disarm(&self) {
        *self.fail_delete.lock().unwrap() = None;
    }

    fn armed(&self, key: &str) -> bool {
        self.fail_delete
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|substr| key.contains(substr))
    }
}

impl BlobStore for FailingDeleteBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> fireweed_engine::EngineResult<()> {
        self.inner.put(key, body)
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> fireweed_engine::EngineResult<bool> {
        self.inner.put_if_absent(key, body)
    }

    fn get(&self, key: &str) -> fireweed_engine::EngineResult<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn delete(&self, key: &str) -> fireweed_engine::EngineResult<bool> {
        if self.armed(key) {
            return Err(fireweed_engine::EngineError::Storage(format!(
                "injected delete failure: {key}"
            )));
        }
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> fireweed_engine::EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("fireweed-objlog-e3-{tag}-{}", std::process::id()))
}

fn sk(tenant: &str, queue: &str) -> fireweed_engine::QueueKey {
    fireweed_engine::QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn shard_prefix_s(shard: &fireweed_engine::QueueKey) -> String {
    format!(
        "t/{}/q/{}/",
        hex(shard.tenant_id.as_str().as_bytes()),
        hex(shard.queue_id.as_str().as_bytes())
    )
}

fn manifest_prefix_s(shard: &fireweed_engine::QueueKey) -> String {
    format!("{}manifest/", shard_prefix_s(shard))
}

fn manifest_head_prefix_s(shard: &fireweed_engine::QueueKey) -> String {
    format!("{}manifest_head/", shard_prefix_s(shard))
}

fn manifest_key_s(shard: &fireweed_engine::QueueKey, index: u64) -> String {
    format!("{}{index:020}.json", manifest_prefix_s(shard))
}

fn retired_horizon_fixture_key(shard: &fireweed_engine::QueueKey) -> String {
    format!("{}read_horizon.json", shard_prefix_s(shard))
}

fn retired_manifest_fixture_bytes(
    index: u64,
    epoch: u64,
    first_seq: u64,
    last_seq: u64,
    fence: bool,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "index": index,
        "epoch": epoch,
        "fence": fence,
        "segment_key": if fence {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(format!("seg-{index}.seg"))
        },
        "first_seq": first_seq,
        "last_seq": last_seq,
        "visible_last_seq": serde_json::Value::Null,
        "checksum": 0u64,
        "committed_at_ms": 1_000_i64 + index as i64,
        "retention_floor_through": serde_json::Value::Null,
        "compacted_through_index": serde_json::Value::Null,
    }))
    .unwrap()
}

fn write_retired_manifest_fixture<S: BlobStore>(
    store: &S,
    shard: &fireweed_engine::QueueKey,
    index: u64,
    epoch: u64,
    first_seq: u64,
    last_seq: u64,
    fence: bool,
) {
    store
        .put(
            &manifest_key_s(shard, index),
            &retired_manifest_fixture_bytes(index, epoch, first_seq, last_seq, fence),
        )
        .unwrap();
}

fn delete_watermark_marker<S: BlobStore>(store: &S, shard: &fireweed_engine::QueueKey) {
    for prefix in [manifest_head_prefix_s(shard), manifest_prefix_s(shard)] {
        for key in store.list(&prefix).unwrap() {
            if key.ends_with("~watermark.json") {
                store.delete(&key).unwrap();
            }
        }
    }
}

fn seg_pushes(n: u64) -> Vec<fireweed_engine::CommandEnvelope> {
    (0..n)
        .map(|i| {
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item(&format!("{i}"), &format!("k{i}"), (i % 10) as i64)],
                }),
                vec![],
            )
        })
        .collect()
}

fn seg_trim_cycle<S: BlobStore>(
    log: &SegmentedObjectLog<S>,
    shard: &fireweed_engine::QueueKey,
    through_seq: u64,
    _epoch: u64,
    now_ms: i64,
) {
    let epoch = log.acquire_epoch(shard, now_ms).unwrap();
    log.advance_retention_floor(
        shard,
        fireweed_engine::CommandPosition::new(shard.clone(), epoch, through_seq),
        epoch,
    )
    .unwrap();
    log.expire_segments_through(shard, through_seq, now_ms)
        .unwrap();
}

fn big_qdef(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
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
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 3_600_000,
        retry_policy: RetryPolicy {
            max_attempts: 1_000_000,
        },
        max_push_batch_size: 10_000_000,
        max_claim_batch_size: 10_000_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn segmented_pushes(n: u64) -> Vec<CommandEnvelope> {
    (0..n)
        .map(|i| {
            envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![item(&format!("{i}"), &format!("k{i}"), i as i64)],
                }),
                vec![],
            )
        })
        .collect()
}

#[tokio::test]
async fn segment_counters_are_reported_for_release_rows() {
    let root = tmp_root("segment-counters");
    let _ = std::fs::remove_dir_all(&root);
    let shard = sk("segment", "counters");
    let store = LocalObjectLog::open_with_config(
        &root,
        ObjectLogSegmentConfig {
            segment_max_commands: 2,
            segment_max_bytes: 0,
            segment_max_latency_ms: 5,
        },
    )
    .expect("open");
    store.create_queue(big_qdef("segment", "counters")).unwrap();
    store
        .append(
            &shard,
            &[
                envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item("1", "k1", 1)],
                    }),
                    vec![ItemId::new("1").unwrap()],
                ),
                envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item("2", "k2", 1)],
                    }),
                    vec![ItemId::new("2").unwrap()],
                ),
                envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item("3", "k3", 1)],
                    }),
                    vec![ItemId::new("3").unwrap()],
                ),
            ],
            0,
        )
        .expect("append");

    let stats = store.segment_stats(&shard).expect("segment stats");
    assert_eq!(stats.segment_objects, 2);
    assert_eq!(stats.command_objects, 3);

    let row = fireweed_release::LedgerRow {
        suite: "object_log_commit_recovery_tests".into(),
        command: "cargo test -p fireweed-objectlog --test object_log_commit_recovery_tests -- --exact segment_counters_are_reported_for_release_rows".into(),
        backend_profile: "object_log_file_reference".into(),
        scale: "smoke".into(),
        seed: 0,
        environment: "in-process object-log reference with reported segment counters".into(),
        exit_status: 0,
        ac_ids: vec![],
        inv_ids: vec![],
        pass_bar: "segment and command counters are observable from the backend".into(),
        evidence_tier: "release".into(),
        measurements: fireweed_release::Measurements {
            tp002_evidence_ids: vec!["E3".into()],
            values: std::collections::BTreeMap::from([
                ("segment_objects".into(), serde_json::json!(stats.segment_objects)),
                ("command_objects".into(), serde_json::json!(stats.command_objects)),
            ]),
        },
    };
    let path = fireweed_release::ledger_path(
        env!("CARGO_MANIFEST_DIR"),
        "segment_counters_are_reported_for_release_rows",
    );
    let _ = std::fs::remove_file(&path);
    fireweed_release::append_row(&path, &row).expect("emit release row");
    let summary =
        fireweed_release::verify_ledger(&path, true).expect("emitted release row validates strict");
    assert!(
        summary.evidence_ids.contains("E3"),
        "release-tier row carries the E3 evidence id"
    );
    assert_eq!(
        serde_json::from_str::<fireweed_release::LedgerRow>(
            &std::fs::read_to_string(&path).unwrap()
        )
        .unwrap()
        .measurements
        .values
        .get("segment_objects")
        .and_then(|v| v.as_u64()),
        Some(2)
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkRecoveryRoundTrip() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = sk("watermark", "roundtrip");
    let def = big_qdef("watermark", "roundtrip");
    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&def).unwrap();

    for i in 0..4u64 {
        log.enqueue(&shard, &seg_pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }
    seg_trim_cycle(&log, &shard, 3, 0, 1_000);

    let recovered = log
        .read_manifest_deletion_watermark(&shard)
        .unwrap()
        .unwrap();
    let marker_keys: Vec<String> = store
        .list(&manifest_head_prefix_s(&shard))
        .unwrap()
        .into_iter()
        .filter(|key| {
            store
                .get(key)
                .unwrap()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .and_then(|obj| obj.get("compacted_through_index").cloned())
                .is_some()
        })
        .collect();
    assert!(
        !marker_keys.is_empty(),
        "trim writes a manifest watermark marker that recovery can read back"
    );

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&def).unwrap();
    assert_eq!(
        reopened.read_manifest_deletion_watermark(&shard).unwrap(),
        Some(recovered),
        "reopen restores the same manifest deletion watermark"
    );
    assert_eq!(
        reopened
            .read_from(&shard, 4)
            .unwrap()
            .iter()
            .map(|(pos, _)| pos.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7],
        "reopen still replays the live tail above the reclaimed prefix"
    );

    let reopened_again = SegmentedObjectLog::open(store.clone(), cfg);
    reopened_again.create_queue(&def).unwrap();
    assert_eq!(
        reopened_again
            .read_manifest_deletion_watermark(&shard)
            .unwrap(),
        Some(recovered),
        "a second reopen preserves the same recovered watermark"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestManifestWatermarkRecoveryPersistence() {
    TestManifestDeletionWatermarkRecoveryRoundTrip();
}

fn manifest_watermark_restart_and_fallback_round_trip() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = sk("watermark", "recovery");
    let def = big_qdef("watermark", "recovery");

    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&def).unwrap();
    for i in 0..4u64 {
        log.enqueue(&shard, &seg_pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    seg_trim_cycle(&log, &shard, 3, 0, 1_000);
    let persisted = log
        .read_manifest_deletion_watermark(&shard)
        .unwrap()
        .expect("watermark persisted after reclaim");
    assert_eq!(
        persisted, 1,
        "the durable deletion watermark records the physically reclaimed prefix"
    );

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&def).unwrap();
    assert_eq!(
        reopened.read_manifest_deletion_watermark(&shard).unwrap(),
        Some(persisted),
        "restart reloads the durable deletion watermark"
    );
    assert_eq!(
        reopened
            .read_from(&shard, 4)
            .unwrap()
            .iter()
            .map(|(pos, _)| pos.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7],
        "the live tail stays readable above the recovered deletion watermark"
    );

    delete_watermark_marker(store.as_ref(), &shard);

    let conservative = SegmentedObjectLog::open(store.clone(), cfg);
    conservative.create_queue(&def).unwrap();
    assert!(
        conservative
            .read_manifest_deletion_watermark(&shard)
            .unwrap()
            .is_none(),
        "without persisted watermark metadata the recovery path falls back conservatively"
    );
    assert!(matches!(
        conservative.read_from(&shard, 4),
        Err(fireweed_engine::EngineError::Conflict)
    ));
}

#[test]
#[allow(non_snake_case)]
fn TestObjectLogCommitRecoveryManifestWatermark() {
    manifest_watermark_restart_and_fallback_round_trip();
}

#[test]
#[allow(non_snake_case)]
fn TestOwnerFenceDeleteOnlyEvaluation() {
    // pqueue-c33c367e owner-fence wiring does not change the current index-CAS safety envelope, so a
    // cheaper delete-only compaction variant remains unsupported here. Recovery must stay conservative and
    // must not infer deletion from the cache alone.
    manifest_watermark_restart_and_fallback_round_trip();
}

#[test]
#[allow(non_snake_case)]
fn TestManifestWatermarkRecoveryKeepsPresentEntriesReadable() {
    TestManifestDeletionWatermarkRecoveryRoundTrip();
}

#[test]
#[allow(non_snake_case)]
fn TestManifestDeletionWatermarkPersistsAndRecoversMetadata() {
    let store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = sk("meta", "persist");
    let qdef = big_qdef("meta", "persist");

    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef).unwrap();
    log.enqueue(&shard, &segmented_pushes(2), 0, 10).unwrap();
    log.seal(&shard, 0, 11).unwrap();
    log.enqueue(&shard, &segmented_pushes(2), 0, 20).unwrap();
    log.seal(&shard, 0, 21).unwrap();

    let owner_epoch = log.acquire_epoch(&shard, 1_000).unwrap();
    log.advance_retention_floor(
        &shard,
        CommandPosition::new(shard.clone(), owner_epoch, 1),
        owner_epoch,
    )
    .unwrap();
    log.expire_segments_through(&shard, 1, 1_000).unwrap();

    let persisted = log
        .read_manifest_deletion_watermark(&shard)
        .unwrap()
        .expect("watermark persisted after reclamation");
    assert_eq!(
        persisted, 0,
        "the durable floor is recovered from persisted metadata"
    );
    assert_eq!(
        log.current_epoch(&shard).unwrap(),
        owner_epoch,
        "persisting the deletion watermark does not change the permanent-head fence"
    );

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef).unwrap();
    assert_eq!(
        reopened.read_manifest_deletion_watermark(&shard).unwrap(),
        Some(persisted),
        "reopening recovers the same durable manifest deletion watermark"
    );
    assert_eq!(
        reopened.current_epoch(&shard).unwrap(),
        owner_epoch,
        "reopening preserves the permanent-head stale-writer fence"
    );
}

#[test]
#[allow(non_snake_case)]
fn TestRetiredDurableNamespacesAreRejected() {
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();

    let cache_store = std::sync::Arc::new(InMemoryBlobStore::new());
    let cache_shard = sk("retired", "cache");
    let cache_def = big_qdef("retired", "cache");
    let cache_log = SegmentedObjectLog::open(cache_store.clone(), cfg);
    cache_log.create_queue(&cache_def).unwrap();
    cache_store
        .put(
            &retired_horizon_fixture_key(&cache_shard),
            br#"{"index":0}"#,
        )
        .unwrap();
    assert!(matches!(
        SegmentedObjectLog::open(cache_store, cfg).create_queue(&cache_def),
        Err(fireweed_engine::EngineError::DurableDataCorrupt { .. })
    ));

    let mirror_store = std::sync::Arc::new(InMemoryBlobStore::new());
    let mirror_shard = sk("retired", "mirror");
    let mirror_def = big_qdef("retired", "mirror");
    let mirror_log = SegmentedObjectLog::open(mirror_store.clone(), cfg);
    mirror_log.create_queue(&mirror_def).unwrap();
    write_retired_manifest_fixture(mirror_store.as_ref(), &mirror_shard, 0, 0, 0, 0, false);
    assert!(matches!(
        SegmentedObjectLog::open(mirror_store, cfg).create_queue(&mirror_def),
        Err(fireweed_engine::EngineError::DurableDataCorrupt { .. })
    ));

    let append_only_store = std::sync::Arc::new(InMemoryBlobStore::new());
    let append_only_shard = sk("retired", "append-only");
    let append_only_def = big_qdef("retired", "append-only");
    write_retired_manifest_fixture(
        append_only_store.as_ref(),
        &append_only_shard,
        0,
        7,
        0,
        0,
        false,
    );
    assert!(matches!(
        SegmentedObjectLog::open(append_only_store, cfg).create_queue(&append_only_def),
        Err(fireweed_engine::EngineError::DurableDataCorrupt { .. })
    ));
}

#[test]
#[allow(non_snake_case)]
fn TestPartialExpireRecoveryKeepsVisibleUndeletedSegments() {
    let store = std::sync::Arc::new(FailingDeleteBlobStore::default());
    let cfg = SegmentConfig::new(10_000_000, 100).unwrap();
    let shard = sk("partial", "expire");
    let qdef = big_qdef("partial", "expire");

    let log = SegmentedObjectLog::open(store.clone(), cfg);
    log.create_queue(&qdef).unwrap();
    for i in 0..4u64 {
        log.enqueue(&shard, &segmented_pushes(2), 0, (i as i64 + 1) * 10)
            .unwrap();
        log.seal(&shard, 0, (i as i64 + 1) * 10 + 1).unwrap();
    }

    let owner_epoch = log.acquire_epoch(&shard, 1_000).unwrap();
    log.advance_retention_floor(
        &shard,
        CommandPosition::new(shard.clone(), owner_epoch, 7),
        owner_epoch,
    )
    .unwrap();
    store.arm_delete(".seg");

    let err = log.expire_segments_through(&shard, 7, 1_000).unwrap_err();
    assert!(
        matches!(err, fireweed_engine::EngineError::Storage(_)),
        "the injected delete failure must abort the partial expire"
    );
    assert_eq!(
        log.read_manifest_deletion_watermark(&shard).unwrap(),
        None,
        "no safe reclaimed prefix is recorded when the first reclaim delete fails"
    );

    drop(log);
    store.disarm();

    assert!(
        !store
            .list(&format!("{}manifest_candidates/", shard_prefix_s(&shard)))
            .unwrap()
            .is_empty(),
        "the interrupted reclaim preserves authoritative manifest candidates"
    );

    let reopened = SegmentedObjectLog::open(store.clone(), cfg);
    reopened.create_queue(&qdef).unwrap();
    assert_eq!(
        reopened.read_manifest_deletion_watermark(&shard).unwrap(),
        None,
        "reopen preserves the absence of a manifest-deletion watermark from the interrupted reclaim"
    );

    let floor = reopened.read_retention_floor(&shard).unwrap().unwrap();
    assert_eq!(
        floor.sequence, 7,
        "reopen reconstructs the authoritative floor from the durable manifest tail"
    );
    assert_eq!(
        reopened
            .read_from(&shard, 0)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5, 6, 7],
        "reopen keeps every undeleted below-floor manifest entry visible"
    );
    assert_eq!(
        reopened
            .read_from(&shard, 4)
            .unwrap()
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7],
        "reopen keeps the undeleted tail visible at the partial-expiry boundary"
    );
    assert!(
        reopened.read_from(&shard, 8).unwrap().is_empty(),
        "reopen keeps the partial-expiry boundary above the undeleted tail"
    );
}
