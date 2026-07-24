use super::*;

use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, RecurrencePolicy, RetryPolicy, WorkerId,
};
use fireweed_engine::{ClaimPort, ControlPlaneStore, ProjectionRead, PushPort};
use fireweed_objectlog::object_store_observability::{
    BlobBackendKind, BlobMetricsRecorder, InstrumentedBlobStore,
};
use fireweed_objectlog::segmented::{FaultCutPoint, InMemoryBlobStore, ObjectStoreStats};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScriptedStoreOperation {
    Put,
    PutIfAbsent,
    Get,
    Delete,
    List,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScriptedStoreEvent {
    operation: ScriptedStoreOperation,
    key: String,
    request_bytes: u64,
    response_bytes: u64,
}

/// Deterministic `BlobStore` latency model for SP-06. Calls execute against the real in-memory contract
/// while every physical request receives a fixed modeled latency, avoiding wall-clock sleeps. Listing is a
/// single-page model; this harness does not model S3 pagination or claim live S3 evidence.
struct ScriptedS3Store {
    inner: InMemoryBlobStore,
    events: Mutex<Vec<ScriptedStoreEvent>>,
}

impl ScriptedS3Store {
    fn new(_latency_ms: u64) -> Self {
        Self {
            inner: InMemoryBlobStore::new(),
            events: Mutex::new(Vec::new()),
        }
    }

    fn record(
        &self,
        operation: ScriptedStoreOperation,
        key: &str,
        request_bytes: u64,
        response_bytes: u64,
    ) {
        self.events
            .lock()
            .expect("scripted S3 events poisoned")
            .push(ScriptedStoreEvent {
                operation,
                key: key.to_string(),
                request_bytes,
                response_bytes,
            });
    }

    fn clear_events(&self) {
        self.events
            .lock()
            .expect("scripted S3 events poisoned")
            .clear();
    }

    fn take_events(&self) -> Vec<ScriptedStoreEvent> {
        std::mem::take(&mut *self.events.lock().expect("scripted S3 events poisoned"))
    }
}

impl BlobStore for ScriptedS3Store {
    fn backend_kind(&self) -> BlobBackendKind {
        BlobBackendKind::S3
    }

    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        let result = self.inner.put(key, body);
        self.record(ScriptedStoreOperation::Put, key, body.len() as u64, 0);
        result
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        let result = self.inner.put_if_absent(key, body);
        self.record(
            ScriptedStoreOperation::PutIfAbsent,
            key,
            body.len() as u64,
            0,
        );
        result
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        let result = self.inner.get(key);
        let response_bytes = result
            .as_ref()
            .ok()
            .and_then(|value| value.as_ref())
            .map_or(0, |value| value.len() as u64);
        self.record(ScriptedStoreOperation::Get, key, 0, response_bytes);
        result
    }

    fn delete(&self, key: &str) -> EngineResult<bool> {
        let result = self.inner.delete(key);
        self.record(ScriptedStoreOperation::Delete, key, 0, 0);
        result
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        let result = self.inner.list(prefix);
        self.record(ScriptedStoreOperation::List, prefix, 0, 0);
        result
    }

    fn stats(&self, prefix: &str) -> EngineResult<ObjectStoreStats> {
        self.inner.stats(prefix)
    }
}

struct FailOnceDuringOwnerReassignment(std::sync::atomic::AtomicBool);

impl FaultHook for FailOnceDuringOwnerReassignment {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == FaultCutPoint::DuringOwnerReassignment && self.0.swap(false, Ordering::SeqCst) {
            Err(EngineError::Storage(
                "scripted SP-06 owner reassignment fault".into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct HandoffProfile {
    samples: u64,
    physical_requests: u64,
    modeled_latency_ms: u64,
    p95_modeled_latency_ms: u64,
    immutable_gets: u64,
    avoidable_immutable_gets: u64,
    repeated_immutable_gets: u64,
    manifest_candidate_gets: u64,
    avoidable_manifest_candidate_gets: u64,
    repeated_manifest_candidate_gets: u64,
    segment_gets: u64,
    avoidable_segment_gets: u64,
    immutable_bytes: u64,
    tail_commands_replayed: u64,
    first_local_read_requests: u64,
    p95_perfect_cache_latency_ms: u64,
}

struct TmpDir(PathBuf);

impl TmpDir {
    fn new(label: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pqueue-sp06-{label}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn projection(&self) -> String {
        self.0.join("projection.db").to_string_lossy().into_owned()
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn queue_def() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("sp06").unwrap(),
        queue_id: QueueId::new("profile").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 2_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 1_000,
        max_claim_batch_size: 1_000,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn spec(payload: &str) -> PushSpec {
    PushSpec {
        client_item_key: None,
        priority: None,
        not_before: None,
        group_key: None,
        payload: Some(Bytes::from(payload.to_string())),
        fields: BTreeMap::new(),
        metadata: Metadata::default(),
        cohort_size: None,
        gate_keys: Vec::new(),
        entity: None,
    }
}

fn ts() -> UtcTimestamp {
    UtcTimestamp::new(1_700_000_000, 0).unwrap()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImmutableRecoveryClass {
    Segment,
    Snapshot,
    ManifestCandidate,
}

fn immutable_recovery_class(key: &str) -> Option<ImmutableRecoveryClass> {
    if key.contains("/seg_candidates/")
        || key.contains("/seg_attempt/")
        || key.contains("/segments/")
    {
        Some(ImmutableRecoveryClass::Segment)
    } else if key.contains("/snap/") {
        Some(ImmutableRecoveryClass::Snapshot)
    } else if key.contains("/manifest_candidates/") {
        Some(ImmutableRecoveryClass::ManifestCandidate)
    } else {
        None
    }
}

async fn profile_sqlite_handoffs(
    queue_items: usize,
    latency_ms: u64,
    handoffs: usize,
    unapplied_tail_per_handoff: bool,
) -> HandoffProfile {
    let tmp = TmpDir::new(&format!("{queue_items}-{latency_ms}"));
    let def = queue_def();
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let retry_max = def.retry_policy.max_attempts;
    let recorder = Arc::new(BlobMetricsRecorder::new());
    let scripted = Arc::new(ScriptedS3Store::new(latency_ms));
    let observed = InstrumentedBlobStore::new(
        Arc::clone(&scripted),
        Arc::clone(&recorder),
        BlobBackendKind::S3,
    );
    let store: Arc<dyn BlobStore> = Arc::new(observed);
    let backend = SegmentedObjectLogSqliteBackend::open_with_blob_store(
        store,
        &tmp.projection(),
        SegmentConfig::new(1, 1_000).unwrap(),
    )
    .unwrap();
    backend.create_queue(def).await.unwrap();
    backend.fence_epoch(&shard, 1).await.unwrap();
    backend
        .push(
            &shard,
            (0..queue_items)
                .map(|index| spec(&format!("item-{index}")))
                .collect(),
            ts(),
            None,
        )
        .await
        .unwrap();

    let mut profile = HandoffProfile::default();
    let mut immutable_keys_seen = HashSet::new();
    let mut sample_latencies_ms = Vec::with_capacity(handoffs);
    let mut perfect_cache_latencies_ms = Vec::with_capacity(handoffs);
    for sample in 0..handoffs {
        let old_epoch = backend.current_epoch(&shard).await.unwrap();
        if unapplied_tail_per_handoff {
            let (items, ids) = build_push_items(
                vec![spec(&format!("tail-{sample}"))],
                old_epoch,
                0,
                10_000 + sample as u32,
                retry_max,
            );
            let envelope =
                backend.next_envelope(QueueCommand::Push(PushCommand { items }), ids, ts());
            let outcome = backend
                .log
                .enqueue(
                    &shard,
                    std::slice::from_ref(&envelope),
                    old_epoch,
                    system_now_ms(),
                )
                .unwrap();
            assert_eq!(outcome.committed.len(), 1);
        }
        let durable_before_fence: HashSet<String> =
            scripted.inner.list("").unwrap().into_iter().collect();
        let target_epoch = old_epoch + 1;
        backend.fence_epoch(&shard, target_epoch).await.unwrap();

        scripted.clear_events();
        let before = recorder.snapshot();
        backend
            .hydrate_projection_for_ownership(&shard)
            .await
            .unwrap();
        profile.tail_commands_replayed += backend
            .recovery_stats(&shard)
            .expect("hydration profile")
            .tail_replayed;
        let claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new(format!("worker-{sample}")).unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new(format!("lease-{sample}")).unwrap(),
                lease_expires_at: UtcTimestamp::new(1_700_000_100, 0).unwrap(),
                now: ts(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: Some(target_epoch),
            })
            .await
            .unwrap();
        assert_eq!(claimed.items.len(), 1);

        let delta = recorder.snapshot().delta(&before).physical_totals();
        let events = scripted.take_events();
        let physical_requests = delta.puts + delta.gets + delta.lists + delta.deletes;
        assert_eq!(physical_requests, events.len() as u64);
        assert_eq!(
            delta.request_bytes,
            events.iter().map(|event| event.request_bytes).sum::<u64>()
        );
        assert_eq!(
            delta.response_bytes,
            events.iter().map(|event| event.response_bytes).sum::<u64>()
        );
        profile.samples += 1;
        profile.physical_requests += physical_requests;
        let sample_latency_ms = physical_requests * latency_ms;
        profile.modeled_latency_ms += sample_latency_ms;
        sample_latencies_ms.push(sample_latency_ms);
        let mut sample_avoidable_gets = 0u64;
        for event in events {
            if event.operation == ScriptedStoreOperation::Get
                && let Some(class) = immutable_recovery_class(&event.key)
            {
                profile.immutable_gets += 1;
                profile.immutable_bytes += event.response_bytes;
                let avoidable = durable_before_fence.contains(&event.key);
                if avoidable {
                    profile.avoidable_immutable_gets += 1;
                    sample_avoidable_gets += 1;
                }
                match class {
                    ImmutableRecoveryClass::Segment => {
                        profile.segment_gets += 1;
                        profile.avoidable_segment_gets += u64::from(avoidable);
                    }
                    ImmutableRecoveryClass::ManifestCandidate => {
                        profile.manifest_candidate_gets += 1;
                        profile.avoidable_manifest_candidate_gets += u64::from(avoidable);
                    }
                    ImmutableRecoveryClass::Snapshot => {}
                }
                if !immutable_keys_seen.insert(event.key) {
                    profile.repeated_immutable_gets += 1;
                    if class == ImmutableRecoveryClass::ManifestCandidate {
                        profile.repeated_manifest_candidate_gets += 1;
                    }
                }
            }
        }
        perfect_cache_latencies_ms.push(
            physical_requests
                .saturating_sub(sample_avoidable_gets)
                .saturating_mul(latency_ms),
        );

        scripted.clear_events();
        let _ = backend.metrics(&shard).await.unwrap();
        profile.first_local_read_requests += scripted.take_events().len() as u64;
    }
    sample_latencies_ms.sort_unstable();
    let p95_index = sample_latencies_ms
        .len()
        .saturating_mul(95)
        .div_ceil(100)
        .saturating_sub(1);
    profile.p95_modeled_latency_ms = sample_latencies_ms.get(p95_index).copied().unwrap_or(0);
    perfect_cache_latencies_ms.sort_unstable();
    profile.p95_perfect_cache_latency_ms = perfect_cache_latencies_ms
        .get(p95_index)
        .copied()
        .unwrap_or(0);
    profile
}

#[tokio::test]
#[ignore = "bounded SP-06 evidence matrix; run explicitly, never in ordinary CI"]
async fn sp06_full_handoff_profile_classifies_metadata_and_required_tail() {
    const HANDOFFS: usize = 200;
    let mut profiles = Vec::new();
    for queue_items in [256, 1_000] {
        for latency_ms in [25, 100] {
            for unapplied_tail in [false, true] {
                let profile =
                    profile_sqlite_handoffs(queue_items, latency_ms, HANDOFFS, unapplied_tail)
                        .await;
                eprintln!(
                    "sp06 queue_items={queue_items} latency_ms={latency_ms} unapplied_tail={unapplied_tail} profile={profile:?}"
                );
                profiles.push((unapplied_tail, profile));
            }
        }
    }
    for (unapplied_tail, profile) in profiles {
        assert_eq!(profile.samples, HANDOFFS as u64);
        assert!(profile.physical_requests > 0 && profile.p95_modeled_latency_ms > 0);
        assert!(profile.avoidable_immutable_gets <= profile.immutable_gets);
        assert!(profile.p95_perfect_cache_latency_ms < profile.p95_modeled_latency_ms);
        assert!(profile.manifest_candidate_gets > 0);
        assert!(profile.repeated_manifest_candidate_gets > 0);
        let immutable = u128::from(profile.immutable_gets);
        let avoidable = u128::from(profile.avoidable_immutable_gets);
        assert!(avoidable.saturating_mul(100) >= immutable.saturating_mul(70));
        assert!(profile.p95_modeled_latency_ms >= 2_000 / 4);
        let absolute_gain = profile
            .p95_modeled_latency_ms
            .saturating_sub(profile.p95_perfect_cache_latency_ms);
        assert!(absolute_gain >= 50);
        assert!(
            u128::from(absolute_gain).saturating_mul(100)
                < u128::from(profile.p95_modeled_latency_ms).saturating_mul(20)
        );
        if unapplied_tail {
            assert_eq!(profile.segment_gets, HANDOFFS as u64);
            assert_eq!(profile.avoidable_segment_gets, HANDOFFS as u64);
            assert!(profile.immutable_bytes > 0);
            assert_eq!(profile.tail_commands_replayed, HANDOFFS as u64);
        } else {
            assert_eq!(profile.segment_gets, 0);
            assert_eq!(profile.avoidable_segment_gets, 0);
            assert_eq!(profile.tail_commands_replayed, 0);
        }
        assert_eq!(profile.first_local_read_requests, 0);
    }
}

#[tokio::test]
async fn sp06_handoff_profile_smoke_reconciles_required_tail_reads() {
    let clean = profile_sqlite_handoffs(8, 25, 2, false).await;
    assert_eq!((clean.segment_gets, clean.tail_commands_replayed), (0, 0));
    assert!(clean.manifest_candidate_gets > 0);

    let tail = profile_sqlite_handoffs(8, 25, 2, true).await;
    assert_eq!((tail.segment_gets, tail.tail_commands_replayed), (2, 2));
    assert_eq!(tail.avoidable_segment_gets, 2);
    assert_eq!(tail.first_local_read_requests, 0);
}

#[tokio::test]
async fn sp06_reassignment_fault_recovers_without_warm_state() {
    let tmp = TmpDir::new("fault");
    let def = queue_def();
    let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
    let scripted = Arc::new(ScriptedS3Store::new(25));
    let observed = InstrumentedBlobStore::new(
        Arc::clone(&scripted),
        Arc::new(BlobMetricsRecorder::new()),
        BlobBackendKind::S3,
    );
    let backend = SegmentedObjectLogSqliteBackend::open_with_blob_store(
        Arc::new(observed),
        &tmp.projection(),
        SegmentConfig::new(1, 1_000).unwrap(),
    )
    .unwrap();
    backend.create_queue(def).await.unwrap();
    backend.fence_epoch(&shard, 1).await.unwrap();
    backend
        .push(&shard, vec![spec("durable")], ts(), Some(1))
        .await
        .unwrap();

    backend.set_object_log_fault_hook(Some(Arc::new(FailOnceDuringOwnerReassignment(
        std::sync::atomic::AtomicBool::new(true),
    ))));
    assert!(matches!(
        backend.fence_epoch(&shard, 2).await,
        Err(EngineError::Storage(_))
    ));
    backend.set_object_log_fault_hook(None);
    assert_eq!(backend.fence_epoch(&shard, 2).await.unwrap(), 2);
    scripted.clear_events();
    backend
        .hydrate_projection_for_ownership(&shard)
        .await
        .unwrap();
    assert_eq!(backend.recovery_stats(&shard).unwrap().tail_replayed, 0);
    assert!(scripted.take_events().iter().all(|event| {
        event.operation != ScriptedStoreOperation::Get
            || immutable_recovery_class(&event.key) != Some(ImmutableRecoveryClass::Segment)
    }));
}
