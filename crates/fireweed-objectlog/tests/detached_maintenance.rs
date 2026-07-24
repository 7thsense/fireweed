use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use fireweed_engine::{
    ComposedBackend, ControlPlaneStore, InProcessControlPlane, PushPort, PushSpec, QueueKey,
};
use fireweed_objectlog::ObjectLog;
use fireweed_objectlog::segmented::{BlobStore, InMemoryBlobStore, ObjectStoreStats};
use fireweed_projection::InMemoryProjection;
use futures::executor::block_on;

#[derive(Default)]
struct ProviderBlock {
    state: Mutex<ProviderBlockState>,
    changed: Condvar,
}

#[derive(Default)]
struct ProviderBlockState {
    armed: bool,
    entered: bool,
    released: bool,
}

impl ProviderBlock {
    fn arm(&self) {
        let mut state = self.state.lock().expect("provider block poisoned");
        *state = ProviderBlockState {
            armed: true,
            entered: false,
            released: false,
        };
    }

    fn block_first_get(&self) {
        let mut state = self.state.lock().expect("provider block poisoned");
        if !state.armed {
            return;
        }
        state.armed = false;
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("provider block poisoned");
        }
    }

    fn wait_until_entered(&self) {
        let state = self.state.lock().expect("provider block poisoned");
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(2), |state| !state.entered)
            .expect("provider block poisoned");
        assert!(state.entered, "maintenance never reached provider GET");
        assert!(!timeout.timed_out(), "maintenance provider wait timed out");
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("provider block poisoned");
        state.released = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
struct StallingBlobStore {
    inner: InMemoryBlobStore,
    provider_block: ProviderBlock,
}

impl BlobStore for StallingBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> fireweed_engine::EngineResult<()> {
        self.inner.put(key, body)
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> fireweed_engine::EngineResult<bool> {
        self.inner.put_if_absent(key, body)
    }

    fn get(&self, key: &str) -> fireweed_engine::EngineResult<Option<Vec<u8>>> {
        self.provider_block.block_first_get();
        self.inner.get(key)
    }

    fn delete(&self, key: &str) -> fireweed_engine::EngineResult<bool> {
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> fireweed_engine::EngineResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn stats(&self, prefix: &str) -> fireweed_engine::EngineResult<ObjectStoreStats> {
        self.inner.stats(prefix)
    }
}

fn definition(queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("detached-maintenance").unwrap(),
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
        request_id_retention_ms: 1,
        client_item_key_retention_ms: 1,
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
    }
}

fn timestamp(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

#[test]
fn stalled_queue_a_provider_maintenance_does_not_block_queue_b_push() {
    let store = Arc::new(StallingBlobStore::default());
    let blob_store: Arc<dyn BlobStore> = store.clone();
    let backend = Arc::new(ComposedBackend::new(
        ObjectLog::open_with_blob_store(blob_store).expect("object log"),
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    ));
    let first_definition = definition("queue-a");
    let second_definition = definition("queue-b");
    let first = QueueKey::new(
        first_definition.tenant_id.clone(),
        first_definition.queue_id.clone(),
    );
    let second = QueueKey::new(
        second_definition.tenant_id.clone(),
        second_definition.queue_id.clone(),
    );
    block_on(backend.create_queue(first_definition)).expect("create queue A");
    block_on(backend.create_queue(second_definition)).expect("create queue B");
    let first_epoch = block_on(backend.acquire_epoch(&first)).expect("own queue A");
    let second_epoch = block_on(backend.acquire_epoch(&second)).expect("own queue B");
    block_on(backend.push(
        &first,
        vec![PushSpec::default()],
        timestamp(0),
        Some(first_epoch),
    ))
    .expect("seed queue A");

    store.provider_block.arm();
    let maintenance_backend = Arc::clone(&backend);
    let maintenance_queue = first.clone();
    let maintenance = std::thread::spawn(move || {
        block_on(maintenance_backend.trim_reclaimable_segments_async(
            maintenance_queue,
            1,
            timestamp(100),
        ))
    });
    store.provider_block.wait_until_entered();

    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let push_backend = Arc::clone(&backend);
    let push = std::thread::spawn(move || {
        let result = block_on(push_backend.push(
            &second,
            vec![PushSpec::default()],
            timestamp(100),
            Some(second_epoch),
        ));
        completed_tx.send(result).expect("send queue B result");
    });
    let queue_b_result = completed_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("queue B must progress while queue A provider maintenance is stalled");
    queue_b_result.expect("queue B push");

    store.provider_block.release();
    maintenance
        .join()
        .expect("maintenance thread")
        .expect("maintenance result");
    push.join().expect("push thread");
}
