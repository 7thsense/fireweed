use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

use pqueue_core::{ItemId, ItemState, QueueDefinition, UtcTimestamp};
use pqueue_engine::{
    AsyncProjectionStore, ClaimedItem, CommandEnvelope, CommandPosition, EngineError, EngineResult,
    ProjectionStore, QueueKey,
};

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
}

impl AsyncProjectionStore for AsyncSqliteProjectionStore {
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
        ClientItemKey, EligibilityPolicy, Metadata, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    };
    use pqueue_engine::{
        CommandChecksum, CommandEnvelope, CommandId, PushCommand, PushItem, QueueCommand,
    };

    use super::*;

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

    fn assert_send<T: Send>(_: T) {}

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
