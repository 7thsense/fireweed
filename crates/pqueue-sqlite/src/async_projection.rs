use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

use pqueue_core::{ItemId, ItemState, QueueDefinition, RequestId, UtcTimestamp};
use pqueue_engine::{
    AsyncProjectionStore, ClaimCompatibility, ClaimedItem, CohortLeaseTarget, CommandEnvelope,
    CommandPosition, EngineError, EngineResult, FinalizeTarget, IdempotencyDecision,
    ProjectionStore, PushFingerprint, PushItem, QueueKey, RenewTarget,
};
use pqueue_projection::ProjectionImage;

use crate::SqliteProjectionStore;

/// Default number of complete projection operations that may wait behind the operation currently running.
pub const DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY: usize = 64;

const WORKER_NAME: &str = "pqueue-sqlite-projection";

type Job = Box<dyn FnOnce(&mut SqliteProjectionStore) + Send + 'static>;

struct ReplyState<T> {
    value: Option<EngineResult<T>>,
    waker: Option<Waker>,
}

struct Reply<T> {
    state: Arc<Mutex<ReplyState<T>>>,
}

impl<T> Future for Reply<T> {
    type Output = EngineResult<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().expect("SQLite actor reply poisoned");
        if let Some(value) = state.value.take() {
            Poll::Ready(value)
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

struct ReplySender<T> {
    state: Option<Arc<Mutex<ReplyState<T>>>>,
}

impl<T> ReplySender<T> {
    fn send(mut self, value: EngineResult<T>) {
        let state = self
            .state
            .take()
            .expect("SQLite actor reply sender already completed");
        let waker = {
            let mut state = state.lock().expect("SQLite actor reply poisoned");
            state.value = Some(value);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Drop for ReplySender<T> {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let waker = {
            let mut state = state.lock().expect("SQLite actor reply poisoned");
            state.value = Some(Err(EngineError::Storage(
                "SQLite projection actor exited before replying".to_string(),
            )));
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

fn reply_channel<T>() -> (ReplySender<T>, Reply<T>) {
    let state = Arc::new(Mutex::new(ReplyState {
        value: None,
        waker: None,
    }));
    (
        ReplySender {
            state: Some(Arc::clone(&state)),
        },
        Reply { state },
    )
}

#[derive(Default)]
struct CompletionState {
    result: Option<EngineResult<()>>,
    wakers: Vec<Waker>,
}

#[derive(Clone, Default)]
struct Completion {
    state: Arc<Mutex<CompletionState>>,
}

impl Completion {
    fn finish(&self, result: EngineResult<()>) {
        let wakers = {
            let mut state = self.state.lock().expect("SQLite actor completion poisoned");
            if state.result.is_some() {
                return;
            }
            state.result = Some(result);
            std::mem::take(&mut state.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }

    fn wait(&self) -> CompletionFuture {
        CompletionFuture {
            completion: self.clone(),
        }
    }
}

struct CompletionFuture {
    completion: Completion,
}

impl Future for CompletionFuture {
    type Output = EngineResult<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self
            .completion
            .state
            .lock()
            .expect("SQLite actor completion poisoned");
        if let Some(result) = &state.result {
            Poll::Ready(result.clone())
        } else {
            if !state
                .wakers
                .iter()
                .any(|registered| registered.will_wake(context.waker()))
            {
                state.wakers.push(context.waker().clone());
            }
            Poll::Pending
        }
    }
}

struct WorkerExitGuard {
    completion: Completion,
    clean: bool,
}

impl Drop for WorkerExitGuard {
    fn drop(&mut self) {
        if !self.clean {
            self.completion.finish(Err(EngineError::Storage(
                "SQLite projection actor exited unexpectedly".to_string(),
            )));
        }
    }
}

struct Admission {
    sender: Option<SyncSender<Job>>,
}

struct Actor {
    admission: Mutex<Admission>,
    completion: Completion,
    _worker: JoinHandle<()>,
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.admission
            .get_mut()
            .expect("SQLite actor admission poisoned")
            .sender
            .take();
    }
}

/// Async adapter for [`SqliteProjectionStore`] backed by one dedicated blocking worker thread.
///
/// The worker owns the SQLite store for its entire lifetime. Each accepted mailbox job is one complete
/// [`AsyncProjectionStore`] operation, including any transaction opened by the synchronous implementation.
/// Dropping a caller future discards only its reply; the accepted job remains owned by the mailbox.
#[derive(Clone)]
pub struct AsyncSqliteProjectionStore {
    actor: Arc<Actor>,
}

impl AsyncSqliteProjectionStore {
    pub async fn open(path: &str) -> EngineResult<Self> {
        Self::open_with_capacity(path, DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY).await
    }

    pub async fn open_with_capacity(path: &str, mailbox_capacity: usize) -> EngineResult<Self> {
        let path = path.to_string();
        let (actor, opened) =
            Self::spawn(mailbox_capacity, move || SqliteProjectionStore::open(&path))?;
        opened.await?;
        Ok(actor)
    }

    pub async fn in_memory() -> EngineResult<Self> {
        Self::in_memory_with_capacity(DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY).await
    }

    pub async fn in_memory_with_capacity(mailbox_capacity: usize) -> EngineResult<Self> {
        let (actor, opened) = Self::spawn(mailbox_capacity, SqliteProjectionStore::in_memory)?;
        opened.await?;
        Ok(actor)
    }

    fn spawn<F>(mailbox_capacity: usize, open: F) -> EngineResult<(Self, Reply<()>)>
    where
        F: FnOnce() -> EngineResult<SqliteProjectionStore> + Send + 'static,
    {
        if mailbox_capacity == 0 {
            return Err(EngineError::Invalid(
                "SQLite projection actor mailbox capacity must be positive",
            ));
        }
        let (sender, receiver) = mpsc::sync_channel::<Job>(mailbox_capacity);
        let (opened_sender, opened) = reply_channel();
        let completion = Completion::default();
        let worker_completion = completion.clone();
        let worker = thread::Builder::new()
            .name(WORKER_NAME.to_string())
            .spawn(move || {
                let mut exit = WorkerExitGuard {
                    completion: worker_completion.clone(),
                    clean: false,
                };
                let mut store = match open() {
                    Ok(store) => {
                        opened_sender.send(Ok(()));
                        store
                    }
                    Err(error) => {
                        opened_sender.send(Err(error.clone()));
                        worker_completion.finish(Err(error));
                        exit.clean = true;
                        return;
                    }
                };
                while let Ok(job) = receiver.recv() {
                    job(&mut store);
                }
                worker_completion.finish(Ok(()));
                exit.clean = true;
            })
            .map_err(|error| EngineError::Storage(error.to_string()))?;

        Ok((
            Self {
                actor: Arc::new(Actor {
                    admission: Mutex::new(Admission {
                        sender: Some(sender),
                    }),
                    completion,
                    _worker: worker,
                }),
            },
            opened,
        ))
    }

    fn enqueue<T, F>(&self, operation: F) -> EngineResult<Reply<T>>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteProjectionStore) -> EngineResult<T> + Send + 'static,
    {
        let (reply_sender, reply) = reply_channel();
        let job: Job = Box::new(move |store| reply_sender.send(operation(store)));
        let mut admission = self
            .actor
            .admission
            .lock()
            .expect("SQLite actor admission poisoned");
        let Some(sender) = admission.sender.as_ref() else {
            return Err(EngineError::Unavailable);
        };
        match sender.try_send(job) {
            Ok(()) => Ok(reply),
            Err(TrySendError::Full(_)) => Err(EngineError::Unavailable),
            Err(TrySendError::Disconnected(_)) => {
                admission.sender.take();
                Err(EngineError::Unavailable)
            }
        }
    }

    async fn execute<T, F>(&self, operation: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteProjectionStore) -> EngineResult<T> + Send + 'static,
    {
        self.enqueue(operation)?.await
    }

    /// Stop admission. Calls racing with this method linearize under the actor admission mutex.
    pub fn close(&self) {
        self.actor
            .admission
            .lock()
            .expect("SQLite actor admission poisoned")
            .sender
            .take();
    }

    /// Stop admission and asynchronously wait until every accepted job has run and the worker has exited.
    pub async fn close_and_drain(&self) -> EngineResult<()> {
        self.close();
        self.actor.completion.wait().await
    }

    /// Export the complete durable serving image through the same whole-operation actor boundary.
    pub async fn export_projection_image(&self, shard: QueueKey) -> EngineResult<ProjectionImage> {
        self.execute(move |store| store.export_projection_image(&shard))
            .await
    }
}

impl AsyncProjectionStore for AsyncSqliteProjectionStore {
    fn supports_gates(&self) -> bool {
        true
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::ensure_shard(store, &definition))
                .await
        }
    }

    fn admit_mutation(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::admit_mutation(store, &shard))
                .await
        }
    }

    fn validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| store.validate_push_constraints(&shard, &items, now))
                .await
        }
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<bool>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| store.pause_blocks_push_intake(&shard))
                .await
        }
    }

    fn push_idempotency(
        &self,
        shard: QueueKey,
        request_id: RequestId,
        fingerprint: PushFingerprint,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<IdempotencyDecision<Vec<ItemId>>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| {
                    store.push_idempotency_decision(&shard, &request_id, fingerprint, now)
                })
                .await
        }
    }

    fn renew_validate(
        &self,
        shard: QueueKey,
        targets: Vec<RenewTarget>,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| store.renew_targets_validate(&shard, &targets, now))
                .await
        }
    }

    fn finalize_validate(
        &self,
        shard: QueueKey,
        targets: Vec<FinalizeTarget>,
        now: UtcTimestamp,
        _default_max_attempts: u32,
    ) -> impl Future<Output = EngineResult<Vec<pqueue_engine::FinalizeLeaseMember>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| store.finalize_targets_validate(&shard, &targets, now))
                .await
        }
    }

    fn cohort_lease_validate(
        &self,
        shard: QueueKey,
        target: CohortLeaseTarget,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<Vec<pqueue_engine::CohortLeaseMember>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| store.cohort_lease_validate(&shard, &target, now))
                .await
        }
    }

    fn purge_validate(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
        force: bool,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| store.purge_items_validate(&shard, &ids, force))
                .await
        }
    }

    fn expired_leases(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let actor = self.clone();
        async move {
            if max == 0 {
                return Ok(Vec::new());
            }
            actor
                .execute(move |store| store.expired_leases_bounded(&shard, now, max))
                .await
        }
    }

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::apply_live_owned(store, positions, commands))
                .await
        }
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::apply_recovery(store, &positions, &commands))
                .await
        }
    }

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::eligible_candidates(store, &shard, now, max))
                .await
        }
    }

    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| {
                    ProjectionStore::select_item_claim(store, &shard, &compatibility, now, max)
                })
                .await
        }
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::render_claimed(store, &shard, &ids))
                .await
        }
    }

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<ItemState>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::item_state(store, &shard, &id))
                .await
        }
    }

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<u64>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::item_version(store, &shard, &id))
                .await
        }
    }

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| ProjectionStore::recovery_high_water(store, &shard))
                .await
        }
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(|store| ProjectionStore::recover_definitions(store))
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pqueue_core::{
        BodyHash, ClientItemKey, CohortOnIncomplete, CohortPolicy, EligibilityPolicy, GroupKey,
        LeaseToken, Metadata, MetadataValue, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, QueueId, RecurrencePolicy, RequestId, RetryPolicy,
        TenantId,
    };
    use pqueue_engine::{
        ClaimCommand, CommandChecksum, CommandEnvelope, CommandId, FinalizeKind, FinalizeTarget,
        PauseQueueCommand, PushCommand, PushItem, QueueCommand, RenewTarget, RequestOutcome,
    };

    use super::*;

    #[test]
    fn expired_lease_selection_is_indexed_bounded_and_ordinary_only() {
        let sql = crate::relational::EXPIRED_LEASES_BOUNDED_SQL;
        for predicate in [
            "cohort_size IS NULL",
            "fenced=0",
            "superseded=0",
            "lease_expires_at<?3",
            "ORDER BY item_id",
            "LIMIT ?4",
        ] {
            assert!(sql.contains(predicate));
        }
        assert!(pqueue_relational::RELATIONAL_SCHEMA.contains("pqueue_items_expired_lease_idx"));
    }

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    fn definition() -> QueueDefinition {
        let shard = shard();
        QueueDefinition {
            tenant_id: shard.tenant_id,
            queue_id: shard.queue_id,
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
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    fn push(item_id: ItemId) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new("push"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    client_item_key: ClientItemKey::new("item").unwrap(),
                    item_id,
                    priority: None,
                    not_before: None,
                    group_key: None,
                    max_attempts: 3,
                    payload: None,
                    fields: Default::default(),
                    metadata: Metadata::default(),
                    cohort_size: None,
                    gate_keys: Vec::new(),
                    entity_document: None,
                }],
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(10, 0).unwrap(),
        }
    }

    #[tokio::test]
    async fn actor_item_selector_preserves_group_and_metadata_filters() {
        let store = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        AsyncProjectionStore::ensure_shard(&store, definition())
            .await
            .unwrap();
        let item_id = ItemId::mint(4, 0, 0);
        let mut envelope = push(item_id);
        let QueueCommand::Push(command) = &mut envelope.command else {
            unreachable!()
        };
        command.items[0].group_key = Some(GroupKey::new("group-a").unwrap());
        command.items[0]
            .metadata
            .insert("region", MetadataValue::String("east".to_string()));
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard(), 1, 0)],
            vec![envelope],
        )
        .await
        .unwrap();
        let selected = AsyncProjectionStore::select_item_claim(
            &store,
            shard(),
            ClaimCompatibility {
                group_key: Some(GroupKey::new("group-a").unwrap()),
                metadata_equals: std::collections::BTreeMap::from([(
                    "region".to_string(),
                    MetadataValue::String("east".to_string()),
                )]),
                ..ClaimCompatibility::default()
            },
            UtcTimestamp::new(10, 0).unwrap(),
            1,
        )
        .await
        .unwrap();
        assert_eq!(selected, vec![item_id]);
    }

    fn assert_send<T: Send>(_: T) {}

    fn legacy_fingerprint(value: u64) -> PushFingerprint {
        PushFingerprint {
            canonical_sha256: [0; 32],
            legacy_body_hash: BodyHash(value),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn every_async_projection_future_is_send() {
        assert_send(AsyncSqliteProjectionStore::in_memory());
        assert_send(AsyncSqliteProjectionStore::in_memory_with_capacity(1));
        assert_send(AsyncSqliteProjectionStore::open(":memory:"));
        assert_send(AsyncSqliteProjectionStore::open_with_capacity(
            ":memory:", 1,
        ));
        let store = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        let item = ItemId::mint(1, 0, 0);
        assert_send(store.ensure_shard(definition()));
        assert_send(store.admit_mutation(shard()));
        assert_send(store.validate_push(shard(), Vec::new(), UtcTimestamp::new(0, 0).unwrap()));
        assert_send(store.pause_blocks_intake(shard()));
        assert_send(store.push_idempotency(
            shard(),
            RequestId::new("request").unwrap(),
            legacy_fingerprint(1),
            UtcTimestamp::new(0, 0).unwrap(),
        ));
        assert_send(store.renew_validate(
            shard(),
            vec![RenewTarget {
                item_id: item,
                lease_token: LeaseToken::new("token").unwrap(),
            }],
            UtcTimestamp::new(0, 0).unwrap(),
        ));
        assert_send(store.finalize_validate(
            shard(),
            vec![FinalizeTarget {
                item_id: item,
                lease_token: LeaseToken::new("token").unwrap(),
                item_version: 1,
                kind: FinalizeKind::Complete,
                not_before: None,
            }],
            UtcTimestamp::new(0, 0).unwrap(),
            3,
        ));
        assert_send(store.apply_live(Vec::new(), Vec::new()));
        assert_send(store.apply_recovery(Vec::new(), Vec::new()));
        assert_send(store.eligible_candidates(shard(), UtcTimestamp::new(0, 0).unwrap(), 1));
        assert_send(store.render_claimed(shard(), vec![item]));
        assert_send(store.item_state(shard(), item));
        assert_send(store.item_version(shard(), item));
        assert_send(store.recovery_high_water(shard()));
        assert_send(store.recover_definitions());
        assert_send(store.close_and_drain());
    }

    #[tokio::test]
    async fn renew_validation_checks_token_and_live_expiry() {
        let store = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        store.ensure_shard(definition()).await.unwrap();
        let item = ItemId::mint(9, 0, 0);
        let token = LeaseToken::new("renew-token").unwrap();
        let claim = CommandEnvelope {
            command_id: CommandId::new("claim"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item],
            command: QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item],
                lease_token: token.clone(),
                lease_expires_at: UtcTimestamp::new(20, 0).unwrap(),
                worker_id: None,
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(2, 0).unwrap(),
        };
        store
            .apply_live(
                vec![
                    CommandPosition::new(shard(), 1, 0),
                    CommandPosition::new(shard(), 1, 1),
                ],
                vec![push(item), claim],
            )
            .await
            .unwrap();
        store
            .renew_validate(
                shard(),
                vec![RenewTarget {
                    item_id: item,
                    lease_token: token.clone(),
                }],
                UtcTimestamp::new(10, 0).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            store.purge_validate(shard(), vec![item], false).await,
            Err(EngineError::Conflict)
        );
        assert_eq!(
            store
                .purge_validate(shard(), vec![item, item], true)
                .await
                .unwrap(),
            vec![item]
        );
        let mut other_definition = definition();
        other_definition.queue_id = QueueId::new("other-queue").unwrap();
        let other_shard = QueueKey::new(
            other_definition.tenant_id.clone(),
            other_definition.queue_id.clone(),
        );
        store.ensure_shard(other_definition).await.unwrap();
        assert_eq!(
            store
                .renew_validate(
                    other_shard,
                    vec![RenewTarget {
                        item_id: item,
                        lease_token: token.clone(),
                    }],
                    UtcTimestamp::new(10, 0).unwrap(),
                )
                .await,
            Err(EngineError::NotFound)
        );
        assert_eq!(
            store
                .renew_validate(
                    shard(),
                    vec![RenewTarget {
                        item_id: item,
                        lease_token: LeaseToken::new("wrong").unwrap(),
                    }],
                    UtcTimestamp::new(10, 0).unwrap(),
                )
                .await,
            Err(EngineError::StaleLease)
        );
        store
            .renew_validate(
                shard(),
                vec![RenewTarget {
                    item_id: item,
                    lease_token: token.clone(),
                }],
                UtcTimestamp::new(20, 0).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .renew_validate(
                    shard(),
                    vec![RenewTarget {
                        item_id: item,
                        lease_token: token,
                    }],
                    UtcTimestamp::new(20, 1).unwrap(),
                )
                .await,
            Err(EngineError::StaleLease)
        );
    }

    #[tokio::test]
    async fn purge_validation_and_apply_handle_missing_dedup_and_second_noop() {
        let store = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        store.ensure_shard(definition()).await.unwrap();
        let item = ItemId::mint(11, 0, 0);
        let missing = ItemId::mint(11, 0, 1);
        store
            .apply_live(vec![CommandPosition::new(shard(), 1, 0)], vec![push(item)])
            .await
            .unwrap();
        assert_eq!(
            store
                .purge_validate(shard(), vec![missing, item, item], false)
                .await
                .unwrap(),
            vec![item]
        );
        let env = CommandEnvelope {
            command_id: CommandId::new("purge"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item],
            command: QueueCommand::PurgeItems(pqueue_engine::PurgeItemsCommand {
                item_ids: vec![item],
                force: false,
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(11, 0).unwrap(),
        };
        store
            .apply_live(vec![CommandPosition::new(shard(), 1, 1)], vec![env])
            .await
            .unwrap();
        assert_eq!(store.item_state(shard(), item).await.unwrap(), None);
        assert!(
            store
                .purge_validate(shard(), vec![item, missing], false)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn active_lease_reopen_uses_durable_hash_for_renew_validation() {
        let path = std::env::temp_dir().join(format!(
            "pqueue-renew-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path_str = path.to_str().unwrap().to_string();
        let store = AsyncSqliteProjectionStore::open(&path_str).await.unwrap();
        store.ensure_shard(definition()).await.unwrap();
        let item = ItemId::mint(10, 0, 0);
        let token = LeaseToken::new("reopen-token").unwrap();
        let claim = CommandEnvelope {
            command_id: CommandId::new("reopen-claim"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item],
            command: QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item],
                lease_token: token.clone(),
                lease_expires_at: UtcTimestamp::new(20, 0).unwrap(),
                worker_id: None,
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(2, 0).unwrap(),
        };
        store
            .apply_live(
                vec![
                    CommandPosition::new(shard(), 1, 0),
                    CommandPosition::new(shard(), 1, 1),
                ],
                vec![push(item), claim],
            )
            .await
            .unwrap();
        store.close_and_drain().await.unwrap();
        drop(store);

        let reopened = AsyncSqliteProjectionStore::open(&path_str).await.unwrap();
        reopened
            .renew_validate(
                shard(),
                vec![RenewTarget {
                    item_id: item,
                    lease_token: token.clone(),
                }],
                UtcTimestamp::new(10, 0).unwrap(),
            )
            .await
            .unwrap();
        let item_version = reopened.item_version(shard(), item).await.unwrap().unwrap();
        let target = FinalizeTarget {
            item_id: item,
            lease_token: token.clone(),
            item_version,
            kind: FinalizeKind::Complete,
            not_before: None,
        };
        assert_eq!(
            reopened
                .finalize_validate(
                    shard(),
                    vec![target.clone()],
                    UtcTimestamp::new(10, 0).unwrap(),
                    3,
                )
                .await
                .unwrap(),
            vec![pqueue_engine::FinalizeLeaseMember {
                item_id: item,
                attempt_count: 1,
                max_attempts: 3,
            }]
        );
        let mut stale_version = target;
        stale_version.item_version += 1;
        assert_eq!(
            reopened
                .finalize_validate(
                    shard(),
                    vec![stale_version],
                    UtcTimestamp::new(10, 0).unwrap(),
                    3,
                )
                .await,
            Err(EngineError::Conflict)
        );
        reopened.close_and_drain().await.unwrap();
        drop(reopened);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_capabilities_are_read_only_and_follow_durable_projection_state() {
        let store = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        store.ensure_shard(definition()).await.unwrap();
        assert!(store.supports_gates());

        let first_id = ItemId::mint(3, 0, 0);
        let first = push(first_id);
        let first_item = match &first.command {
            QueueCommand::Push(command) => command.items[0].clone(),
            _ => unreachable!(),
        };
        store
            .validate_push(
                shard(),
                vec![first_item.clone()],
                UtcTimestamp::new(1, 0).unwrap(),
            )
            .await
            .unwrap();
        // Validation does not materialize its candidate.
        assert_eq!(store.item_state(shard(), first_id).await.unwrap(), None);

        store
            .apply_live(vec![CommandPosition::new(shard(), 3, 0)], vec![first])
            .await
            .unwrap();
        let mut duplicate_key = first_item.clone();
        duplicate_key.item_id = ItemId::mint(3, 0, 1);
        assert_eq!(
            store
                .validate_push(
                    shard(),
                    vec![duplicate_key],
                    UtcTimestamp::new(1, 0).unwrap()
                )
                .await,
            Err(EngineError::Conflict)
        );
        let mut duplicate_id = first_item.clone();
        duplicate_id.client_item_key = ClientItemKey::new("different-key").unwrap();
        assert_eq!(
            store
                .validate_push(
                    shard(),
                    vec![duplicate_id],
                    UtcTimestamp::new(1, 0).unwrap(),
                )
                .await,
            Err(EngineError::Conflict)
        );
        store
            .enqueue(|projection| {
                let g = projection.lock();
                g.conn
                    .execute(
                        "UPDATE pqueue_items SET superseded=1 WHERE tenant_id='tenant' \
                         AND queue_id='queue' AND client_item_key='item'",
                        [],
                    )
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                Ok(())
            })
            .unwrap()
            .await
            .unwrap();
        let mut reusable_superseded_key = first_item.clone();
        reusable_superseded_key.item_id = ItemId::mint(3, 0, 5);
        store
            .validate_push(
                shard(),
                vec![reusable_superseded_key],
                UtcTimestamp::new(1, 0).unwrap(),
            )
            .await
            .unwrap();
        let mut duplicate_in_batch = first_item.clone();
        duplicate_in_batch.item_id = ItemId::mint(3, 0, 2);
        duplicate_in_batch.client_item_key = ClientItemKey::new("batch-key").unwrap();
        let mut second_duplicate = duplicate_in_batch.clone();
        second_duplicate.item_id = ItemId::mint(3, 0, 3);
        assert_eq!(
            store
                .validate_push(
                    shard(),
                    vec![duplicate_in_batch, second_duplicate],
                    UtcTimestamp::new(1, 0).unwrap()
                )
                .await,
            Err(EngineError::Conflict)
        );

        assert!(!store.pause_blocks_intake(shard()).await.unwrap());
        let pause = |drain_intake, sequence| CommandEnvelope {
            command_id: CommandId::new(format!("pause-{sequence}")),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: Vec::new(),
            command: QueueCommand::PauseQueue(PauseQueueCommand { drain_intake }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(11 + sequence as i64, 0).unwrap(),
        };
        store
            .apply_live(
                vec![CommandPosition::new(shard(), 3, 1)],
                vec![pause(false, 1)],
            )
            .await
            .unwrap();
        assert!(!store.pause_blocks_intake(shard()).await.unwrap());
        store
            .apply_live(
                vec![CommandPosition::new(shard(), 3, 2)],
                vec![pause(true, 2)],
            )
            .await
            .unwrap();
        assert!(store.pause_blocks_intake(shard()).await.unwrap());

        let request_id = RequestId::new("durable-push").unwrap();
        let replay_id = ItemId::mint(3, 0, 4);
        let mut recorded = push(replay_id);
        recorded.command_id = CommandId::new("recorded-push");
        recorded.request_id = Some(request_id.clone());
        recorded.request_fingerprint = Some(42);
        recorded.request_outcome = Some(RequestOutcome::Push {
            item_ids: vec![replay_id],
        });
        recorded.created_at = UtcTimestamp::new(20, 0).unwrap();
        if let QueueCommand::Push(command) = &mut recorded.command {
            command.items[0].client_item_key = ClientItemKey::new("replay-key").unwrap();
        }
        let recorded_fingerprint = match &recorded.command {
            QueueCommand::Push(command) => PushFingerprint {
                canonical_sha256: pqueue_engine::push_items_fingerprint_sha256(&command.items)
                    .unwrap(),
                legacy_body_hash: BodyHash(42),
            },
            _ => unreachable!(),
        };
        store
            .apply_live(vec![CommandPosition::new(shard(), 3, 3)], vec![recorded])
            .await
            .unwrap();
        assert_eq!(
            store
                .push_idempotency(
                    shard(),
                    request_id.clone(),
                    recorded_fingerprint,
                    UtcTimestamp::new(21, 0).unwrap(),
                )
                .await
                .unwrap(),
            IdempotencyDecision::Replay(vec![replay_id])
        );
        assert_eq!(
            store
                .push_idempotency(
                    shard(),
                    request_id.clone(),
                    PushFingerprint {
                        canonical_sha256: [43; 32],
                        legacy_body_hash: BodyHash(43),
                    },
                    UtcTimestamp::new(21, 0).unwrap(),
                )
                .await
                .unwrap(),
            IdempotencyDecision::Conflict
        );
        store
            .enqueue(|projection| {
                let g = projection.lock();
                g.conn
                    .execute(
                        "UPDATE pqueue_request_idempotency SET request_fingerprint=?1 \
                         WHERE tenant_id='tenant' AND queue_id='queue' AND operation='push' \
                         AND request_id='durable-push'",
                        [BodyHash(42).0.to_be_bytes().as_slice()],
                    )
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                Ok(())
            })
            .unwrap()
            .await
            .unwrap();
        assert_eq!(
            store
                .push_idempotency(
                    shard(),
                    request_id.clone(),
                    recorded_fingerprint,
                    UtcTimestamp::new(21, 0).unwrap(),
                )
                .await
                .unwrap(),
            IdempotencyDecision::Replay(vec![replay_id]),
            "legacy eight-byte fingerprints remain replayable during migration"
        );
        assert_eq!(
            store
                .push_idempotency(
                    shard(),
                    request_id,
                    recorded_fingerprint,
                    UtcTimestamp::new(81, 0).unwrap(),
                )
                .await
                .unwrap(),
            IdempotencyDecision::Expired
        );
        store.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn push_validation_enforces_timed_relational_constraints_exactly() {
        let store = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        let mut queue = definition();
        queue.max_eligible_group_size = Some(2);
        queue.cohort_policy = Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(1_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(2),
        });
        store.ensure_shard(queue).await.unwrap();

        let mut candidate = match push(ItemId::mint(4, 0, 0)).command {
            QueueCommand::Push(command) => command.items[0].clone(),
            _ => unreachable!(),
        };
        candidate.client_item_key = ClientItemKey::new("retained-key").unwrap();
        store
            .enqueue(|projection| {
                let g = projection.lock();
                g.conn
                    .execute(
                        "INSERT INTO pqueue_item_key_retention \
                         (tenant_id,queue_id,client_item_key,item_id,expires_at) \
                         VALUES ('tenant','queue','retained-key','old',10000000000)",
                        [],
                    )
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                Ok(())
            })
            .unwrap()
            .await
            .unwrap();
        assert_eq!(
            store
                .validate_push(
                    shard(),
                    vec![candidate.clone()],
                    UtcTimestamp::new(9, 0).unwrap(),
                )
                .await,
            Err(EngineError::Conflict)
        );
        store
            .validate_push(
                shard(),
                vec![candidate.clone()],
                UtcTimestamp::new(10, 0).unwrap(),
            )
            .await
            .unwrap();

        let mut malformed = candidate.clone();
        malformed.client_item_key = ClientItemKey::new("malformed").unwrap();
        malformed.cohort_size = Some(1);
        assert!(matches!(
            store
                .validate_push(shard(), vec![malformed], UtcTimestamp::new(10, 0).unwrap(),)
                .await,
            Err(EngineError::Invalid("cohort_size requires group_key"))
        ));

        let mut group_items = Vec::new();
        for index in 0..3 {
            let mut item = candidate.clone();
            item.item_id = ItemId::mint(4, 0, 10 + index);
            item.client_item_key = ClientItemKey::new(format!("group-{index}")).unwrap();
            item.group_key = Some(GroupKey::new("bounded-group").unwrap());
            if index == 0 {
                item.cohort_size = Some(2);
            }
            group_items.push(item);
        }
        assert_eq!(
            store
                .validate_push(shard(), group_items, UtcTimestamp::new(10, 0).unwrap())
                .await,
            Err(EngineError::Conflict)
        );

        store
            .enqueue(|projection| {
                let g = projection.lock();
                g.conn
                    .execute(
                        "INSERT INTO pqueue_cohorts \
                         (tenant_id,queue_id,group_key,cohort_id,cohort_size,member_count,state,\
                          cohort_created_at,retention_until,created_at) \
                         VALUES ('tenant','queue','generation','old-generation',5,5,'terminal',0,10000000000,0)",
                        [],
                    )
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                Ok(())
            })
            .unwrap()
            .await
            .unwrap();
        let mut next_generation = candidate;
        next_generation.item_id = ItemId::mint(4, 0, 20);
        next_generation.client_item_key = ClientItemKey::new("next-generation").unwrap();
        next_generation.group_key = Some(GroupKey::new("generation").unwrap());
        next_generation.cohort_size = Some(2);
        assert_eq!(
            store
                .validate_push(
                    shard(),
                    vec![next_generation.clone()],
                    UtcTimestamp::new(9, 0).unwrap(),
                )
                .await,
            Err(EngineError::Conflict)
        );
        store
            .validate_push(
                shard(),
                vec![next_generation],
                UtcTimestamp::new(10, 0).unwrap(),
            )
            .await
            .unwrap();
        store.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn non_push_envelope_cannot_create_a_push_replay_row() {
        let store = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        store.ensure_shard(definition()).await.unwrap();
        let request_id = RequestId::new("not-a-push").unwrap();
        let envelope = CommandEnvelope {
            command_id: CommandId::new("pause-with-forged-push-outcome"),
            request_id: Some(request_id.clone()),
            request_fingerprint: Some(7),
            request_outcome: Some(RequestOutcome::Push {
                item_ids: vec![ItemId::mint(5, 0, 0)],
            }),
            item_ids: Vec::new(),
            command: QueueCommand::PauseQueue(PauseQueueCommand::default()),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        };
        store
            .apply_live(vec![CommandPosition::new(shard(), 1, 0)], vec![envelope])
            .await
            .unwrap();
        assert_eq!(
            store
                .push_idempotency(
                    shard(),
                    request_id,
                    legacy_fingerprint(7),
                    UtcTimestamp::new(2, 0).unwrap(),
                )
                .await
                .unwrap(),
            IdempotencyDecision::Proceed
        );
        store.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn conflicting_push_replay_row_rolls_back_the_whole_apply_batch() {
        let store = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        store.ensure_shard(definition()).await.unwrap();
        let request_id = RequestId::new("colliding-request").unwrap();
        let first_id = ItemId::mint(6, 0, 0);
        let second_id = ItemId::mint(6, 0, 1);
        let mut first = push(first_id);
        first.command_id = CommandId::new("first-collision");
        first.request_id = Some(request_id.clone());
        first.request_fingerprint = Some(11);
        first.request_outcome = Some(RequestOutcome::Push {
            item_ids: vec![first_id],
        });
        let mut second = push(second_id);
        second.command_id = CommandId::new("second-collision");
        second.request_id = Some(request_id.clone());
        second.request_fingerprint = Some(12);
        second.request_outcome = Some(RequestOutcome::Push {
            item_ids: vec![second_id],
        });
        if let QueueCommand::Push(command) = &mut second.command {
            command.items[0].client_item_key = ClientItemKey::new("second-item").unwrap();
        }

        assert_eq!(
            store
                .apply_live(
                    vec![
                        CommandPosition::new(shard(), 6, 0),
                        CommandPosition::new(shard(), 6, 1),
                    ],
                    vec![first, second],
                )
                .await,
            Err(EngineError::RequestIdConflict)
        );
        assert_eq!(store.item_state(shard(), first_id).await.unwrap(), None);
        assert_eq!(store.item_state(shard(), second_id).await.unwrap(), None);
        assert_eq!(store.recovery_high_water(shard()).await.unwrap(), None);
        assert_eq!(
            store
                .push_idempotency(
                    shard(),
                    request_id,
                    legacy_fingerprint(11),
                    UtcTimestamp::new(11, 0).unwrap(),
                )
                .await
                .unwrap(),
            IdempotencyDecision::Proceed
        );
        store.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn live_apply_fences_stale_epochs_but_recovery_accepts_historical_epochs() {
        let live = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        live.ensure_shard(definition()).await.unwrap();
        let current_id = ItemId::mint(7, 0, 0);
        live.apply_live(
            vec![CommandPosition::new(shard(), 7, 0)],
            vec![push(current_id)],
        )
        .await
        .unwrap();
        let stale_id = ItemId::mint(6, 0, 1);
        let mut stale = push(stale_id);
        if let QueueCommand::Push(command) = &mut stale.command {
            command.items[0].client_item_key = ClientItemKey::new("stale-item").unwrap();
        }
        assert_eq!(
            live.apply_live(vec![CommandPosition::new(shard(), 6, 1)], vec![stale],)
                .await,
            Err(EngineError::EpochFenced)
        );
        assert_eq!(live.item_state(shard(), stale_id).await.unwrap(), None);
        assert_eq!(
            live.recovery_high_water(shard()).await.unwrap(),
            Some(CommandPosition::new(shard(), 7, 0))
        );
        live.close_and_drain().await.unwrap();

        let recovery = AsyncSqliteProjectionStore::in_memory().await.unwrap();
        recovery.ensure_shard(definition()).await.unwrap();
        let older_id = ItemId::mint(6, 0, 1);
        let mut older = push(older_id);
        if let QueueCommand::Push(command) = &mut older.command {
            command.items[0].client_item_key = ClientItemKey::new("historical-item").unwrap();
        }
        recovery
            .apply_recovery(
                vec![
                    CommandPosition::new(shard(), 7, 0),
                    CommandPosition::new(shard(), 6, 1),
                ],
                vec![push(current_id), older],
            )
            .await
            .unwrap();
        assert_eq!(
            recovery.item_state(shard(), current_id).await.unwrap(),
            Some(ItemState::Pending)
        );
        assert_eq!(
            recovery.item_state(shard(), older_id).await.unwrap(),
            Some(ItemState::Pending)
        );
        assert_eq!(
            recovery.recovery_high_water(shard()).await.unwrap(),
            Some(CommandPosition::new(shard(), 7, 1))
        );
        recovery.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_worker_and_full_mailbox_do_not_stall_async_heartbeat() {
        let store = AsyncSqliteProjectionStore::in_memory_with_capacity(1)
            .await
            .unwrap();
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let stalled = store
            .enqueue(move |_| {
                assert_eq!(thread::current().name(), Some(WORKER_NAME));
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started_receiver.recv().unwrap();
        let queued = store.enqueue(|_| Ok(())).unwrap();
        assert!(matches!(
            store.enqueue(|_| Ok::<_, EngineError>(())),
            Err(EngineError::Unavailable)
        ));

        let heartbeat = tokio::spawn(async {
            tokio::task::yield_now().await;
            7
        });
        assert_eq!(heartbeat.await.unwrap(), 7);

        release_sender.send(()).unwrap();
        stalled.await.unwrap();
        queued.await.unwrap();
        store.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn caller_cancellation_after_acceptance_does_not_cancel_operation() {
        let store = AsyncSqliteProjectionStore::in_memory_with_capacity(1)
            .await
            .unwrap();
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let stalled = store
            .enqueue(move |_| {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started_receiver.recv().unwrap();

        let mut caller = Box::pin(store.ensure_shard(definition()));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(caller.as_mut().poll(&mut context), Poll::Pending));
        drop(caller);
        release_sender.send(()).unwrap();
        stalled.await.unwrap();

        assert_eq!(store.recover_definitions().await.unwrap().len(), 1);
        store.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_rejects_new_work_and_drains_all_accepted_jobs() {
        let store = AsyncSqliteProjectionStore::in_memory_with_capacity(2)
            .await
            .unwrap();
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let effects = Arc::new(AtomicUsize::new(0));
        let first_effects = Arc::clone(&effects);
        let first = store
            .enqueue(move |_| {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                first_effects.fetch_add(1, Ordering::AcqRel);
                Ok(())
            })
            .unwrap();
        started_receiver.recv().unwrap();
        let second_effects = Arc::clone(&effects);
        let second = store
            .enqueue(move |_| {
                second_effects.fetch_add(1, Ordering::AcqRel);
                Ok(())
            })
            .unwrap();

        store.close();
        assert!(matches!(
            store.ensure_shard(definition()).await,
            Err(EngineError::Unavailable)
        ));
        let drain_store = store.clone();
        let drain = tokio::spawn(async move { drain_store.close_and_drain().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());

        release_sender.send(()).unwrap();
        first.await.unwrap();
        second.await.unwrap();
        drain.await.unwrap().unwrap();
        assert_eq!(effects.load(Ordering::Acquire), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_panic_resolves_accepted_replies_and_drain_with_errors() {
        let store = AsyncSqliteProjectionStore::in_memory_with_capacity(2)
            .await
            .unwrap();
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let panicked = store
            .enqueue::<(), _>(move |_| {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                panic!("intentional SQLite actor test panic")
            })
            .unwrap();
        started_receiver.recv().unwrap();
        let queued = store.enqueue(|_| Ok(())).unwrap();
        release_sender.send(()).unwrap();

        assert!(matches!(panicked.await, Err(EngineError::Storage(_))));
        assert!(matches!(queued.await, Err(EngineError::Storage(_))));
        assert!(matches!(
            store.close_and_drain().await,
            Err(EngineError::Storage(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn file_backed_actor_reopens_with_projection_parity() {
        static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "pqueue-async-projection-{}-{suffix}.sqlite",
            std::process::id()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let item = ItemId::mint(2, 0, 0);

        let store = AsyncSqliteProjectionStore::open_with_capacity(&path_string, 4)
            .await
            .unwrap();
        store.ensure_shard(definition()).await.unwrap();
        store
            .apply_live(vec![CommandPosition::new(shard(), 3, 0)], vec![push(item)])
            .await
            .unwrap();
        store.close_and_drain().await.unwrap();
        drop(store);

        let reopened = AsyncSqliteProjectionStore::open(&path_string)
            .await
            .unwrap();
        assert_eq!(
            reopened.recover_definitions().await.unwrap(),
            vec![definition()]
        );
        assert_eq!(
            reopened.item_state(shard(), item).await.unwrap(),
            Some(ItemState::Pending)
        );
        assert_eq!(
            reopened.recovery_high_water(shard()).await.unwrap(),
            Some(CommandPosition::new(shard(), 3, 0))
        );
        reopened.close_and_drain().await.unwrap();
        drop(reopened);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
