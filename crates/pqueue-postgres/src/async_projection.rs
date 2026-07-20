//! Bounded asynchronous ownership adapter for the synchronous Postgres relational projection.
//!
//! A dedicated OS thread owns the [`PostgresRelational`] value and its connection for the adapter's
//! entire lifetime. Each accepted mailbox job is one complete [`AsyncProjectionStore`] operation, so a
//! Postgres transaction is never split across blocking tasks and no blocking client call runs on an async
//! executor thread.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

use pqueue_core::{ItemId, ItemState, QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp};
use pqueue_engine::{
    AsyncControlPlane, AsyncProjectionStore, ClaimCompatibility, ClaimUnit, ClaimedItem,
    CohortLeaseTarget, CommandEnvelope, CommandPosition, ControlPlaneStore, CreateQueueOutcome,
    EngineError, EngineResult, FinalizeTarget, IdempotencyDecision, ProjectionStore,
    PushFingerprint, PushItem, QueueKey, RenewTarget, RichClaimSelection,
};

use crate::{PostgresRelational, PostgresRelationalBackend};

/// Default number of complete projection operations that may wait behind the operation currently running.
pub const DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY: usize = 64;

/// Default number of complete control-plane operations that may wait behind the operation currently running.
pub const DEFAULT_ASYNC_CONTROL_PLANE_MAILBOX_CAPACITY: usize = 64;

const PROJECTION_WORKER_NAME: &str = "pqueue-postgres-projection";
const CONTROL_PLANE_WORKER_NAME: &str = "pqueue-postgres-control-plane";

fn resolve_eager<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("sync Postgres control-plane port returned a pending future"),
    }
}

type Job<S> = Box<dyn FnOnce(&mut S) + Send + 'static>;

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
        let mut state = self
            .state
            .lock()
            .expect("Postgres projection actor reply poisoned");
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
            .expect("Postgres projection actor reply sender already completed");
        let waker = {
            let mut state = state
                .lock()
                .expect("Postgres projection actor reply poisoned");
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
            let mut state = state
                .lock()
                .expect("Postgres projection actor reply poisoned");
            state.value = Some(Err(EngineError::Storage(
                "Postgres projection actor exited before replying".to_string(),
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
            let mut state = self
                .state
                .lock()
                .expect("Postgres projection actor completion poisoned");
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
            .expect("Postgres projection actor completion poisoned");
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
                "Postgres projection actor exited unexpectedly".to_string(),
            )));
        }
    }
}

struct Admission<S> {
    sender: Option<SyncSender<Job<S>>>,
}

struct Actor<S> {
    admission: Mutex<Admission<S>>,
    completion: Completion,
    _worker: JoinHandle<()>,
}

impl<S> Drop for Actor<S> {
    fn drop(&mut self) {
        self.admission
            .get_mut()
            .expect("Postgres projection actor admission poisoned")
            .sender
            .take();
    }
}

struct ActorHandle<S> {
    actor: Arc<Actor<S>>,
}

impl<S> Clone for ActorHandle<S> {
    fn clone(&self) -> Self {
        Self {
            actor: Arc::clone(&self.actor),
        }
    }
}

impl<S: Send + 'static> ActorHandle<S> {
    fn spawn<F>(
        mailbox_capacity: usize,
        worker_name: &'static str,
        open: F,
    ) -> EngineResult<(Self, Reply<()>)>
    where
        F: FnOnce() -> EngineResult<S> + Send + 'static,
    {
        if mailbox_capacity == 0 {
            return Err(EngineError::Invalid(
                "Postgres projection actor mailbox capacity must be positive",
            ));
        }
        let (sender, receiver) = mpsc::sync_channel::<Job<S>>(mailbox_capacity);
        let (opened_sender, opened) = reply_channel();
        let completion = Completion::default();
        let worker_completion = completion.clone();
        let worker = thread::Builder::new()
            .name(worker_name.to_string())
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
                drop(store);
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
        F: FnOnce(&mut S) -> EngineResult<T> + Send + 'static,
    {
        let (reply_sender, reply) = reply_channel();
        let job: Job<S> = Box::new(move |store| reply_sender.send(operation(store)));
        let mut admission = self
            .actor
            .admission
            .lock()
            .expect("Postgres projection actor admission poisoned");
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
        F: FnOnce(&mut S) -> EngineResult<T> + Send + 'static,
    {
        self.enqueue(operation)?.await
    }

    fn close(&self) {
        self.actor
            .admission
            .lock()
            .expect("Postgres projection actor admission poisoned")
            .sender
            .take();
    }

    async fn close_and_drain(&self) -> EngineResult<()> {
        self.close();
        self.actor.completion.wait().await
    }
}

/// Async projection adapter backed by one bounded, dedicated blocking owner thread.
///
/// Once an operation is accepted, dropping its caller future discards only the reply. The owned operation
/// remains queued and runs to completion. Admission saturation and admission after close fail explicitly
/// with [`EngineError::Unavailable`]. The worker also owns synchronous client teardown.
#[derive(Clone)]
pub struct AsyncPostgresRelationalProjection {
    actor: ActorHandle<PostgresRelational>,
}

impl AsyncPostgresRelationalProjection {
    pub async fn connect(url: &str) -> EngineResult<Self> {
        Self::connect_with_capacity(url, DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY).await
    }

    pub async fn connect_with_capacity(url: &str, mailbox_capacity: usize) -> EngineResult<Self> {
        let url = url.to_string();
        let (actor, opened) =
            ActorHandle::spawn(mailbox_capacity, PROJECTION_WORKER_NAME, move || {
                PostgresRelational::connect(&url)
            })?;
        opened.await?;
        Ok(Self { actor })
    }

    pub async fn connect_in_schema(url: &str, schema: &str) -> EngineResult<Self> {
        Self::connect_in_schema_with_capacity(
            url,
            schema,
            DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY,
        )
        .await
    }

    pub async fn connect_in_schema_with_capacity(
        url: &str,
        schema: &str,
        mailbox_capacity: usize,
    ) -> EngineResult<Self> {
        let url = url.to_string();
        let schema = schema.to_string();
        let (actor, opened) =
            ActorHandle::spawn(mailbox_capacity, PROJECTION_WORKER_NAME, move || {
                PostgresRelational::connect_in_schema(&url, &schema)
            })?;
        opened.await?;
        Ok(Self { actor })
    }

    async fn execute<T, F>(&self, operation: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut PostgresRelational) -> EngineResult<T> + Send + 'static,
    {
        self.actor.execute(operation).await
    }

    /// Stop admission. Calls racing with this method linearize under the actor admission mutex.
    pub fn close(&self) {
        self.actor.close();
    }

    /// Stop admission and asynchronously wait for all accepted operations and client teardown.
    pub async fn close_and_drain(&self) -> EngineResult<()> {
        self.actor.close_and_drain().await
    }
}

impl AsyncProjectionStore for AsyncPostgresRelationalProjection {
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
                .execute(move |store| store.async_validate_push(&shard, &items, now))
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
                .execute(move |store| store.async_pause_blocks_intake(&shard))
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
                    store.async_push_idempotency(&shard, &request_id, fingerprint, now)
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
                .execute(move |store| store.async_renew_targets_validate(&shard, &targets, now))
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
                .execute(move |store| store.async_finalize_targets_validate(&shard, &targets, now))
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
                .execute(move |store| store.async_cohort_lease_validate(&shard, &target, now))
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
                .execute(move |store| store.async_purge_items_validate(&shard, &ids, force))
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
                .execute(move |store| store.async_expired_leases_bounded(&shard, now, max))
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

    fn select_rich_claim(
        &self,
        shard: QueueKey,
        unit: ClaimUnit,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> impl Future<Output = EngineResult<RichClaimSelection>> + Send {
        let actor = self.clone();
        async move {
            actor
                .execute(move |store| {
                    ProjectionStore::select_rich_claim(
                        store,
                        &shard,
                        unit,
                        &compatibility,
                        now,
                        max_items,
                    )
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

/// Async queue-definition control plane backed by one bounded, dedicated Postgres owner thread.
///
/// The owned [`PostgresRelationalBackend`] supplies the existing durable queue-definition semantics. Each
/// accepted request remains owned by the actor after caller cancellation and runs as one complete control-
/// plane operation. This adapter intentionally exposes only [`AsyncControlPlane`], not the backend's data-
/// plane capabilities.
#[derive(Clone)]
pub struct AsyncPostgresRelationalControlPlane {
    actor: ActorHandle<PostgresRelationalBackend>,
}

impl AsyncPostgresRelationalControlPlane {
    pub async fn connect(url: &str) -> EngineResult<Self> {
        Self::connect_with_capacity(url, DEFAULT_ASYNC_CONTROL_PLANE_MAILBOX_CAPACITY).await
    }

    pub async fn connect_with_capacity(url: &str, mailbox_capacity: usize) -> EngineResult<Self> {
        let url = url.to_string();
        let (actor, opened) =
            ActorHandle::spawn(mailbox_capacity, CONTROL_PLANE_WORKER_NAME, move || {
                PostgresRelationalBackend::connect(&url)
            })?;
        opened.await?;
        Ok(Self { actor })
    }

    pub async fn connect_in_schema(url: &str, schema: &str) -> EngineResult<Self> {
        Self::connect_in_schema_with_capacity(
            url,
            schema,
            DEFAULT_ASYNC_CONTROL_PLANE_MAILBOX_CAPACITY,
        )
        .await
    }

    pub async fn connect_in_schema_with_capacity(
        url: &str,
        schema: &str,
        mailbox_capacity: usize,
    ) -> EngineResult<Self> {
        let url = url.to_string();
        let schema = schema.to_string();
        let (actor, opened) =
            ActorHandle::spawn(mailbox_capacity, CONTROL_PLANE_WORKER_NAME, move || {
                PostgresRelationalBackend::connect_in_schema(&url, &schema)
            })?;
        opened.await?;
        Ok(Self { actor })
    }

    /// Stop admission. Calls racing with this method linearize under the actor admission mutex.
    pub fn close(&self) {
        self.actor.close();
    }

    /// Stop admission and asynchronously wait for all accepted operations and client teardown.
    pub async fn close_and_drain(&self) -> EngineResult<()> {
        self.actor.close_and_drain().await
    }
}

impl AsyncControlPlane for AsyncPostgresRelationalControlPlane {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| {
                    resolve_eager(ControlPlaneStore::create_queue(store, definition))
                })
                .await
        }
    }

    fn queue_definition(
        &self,
        key: QueueKey,
    ) -> impl Future<Output = EngineResult<QueueDefinition>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| {
                    resolve_eager(ControlPlaneStore::queue_definition(store, &key))
                })
                .await
        }
    }

    fn list_queues(
        &self,
        tenant: TenantId,
    ) -> impl Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| resolve_eager(ControlPlaneStore::list_queues(store, &tenant)))
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn actor<S: Send + 'static>(store: S, capacity: usize) -> ActorHandle<S> {
        let (actor, opened) =
            ActorHandle::spawn(capacity, PROJECTION_WORKER_NAME, move || Ok(store)).unwrap();
        futures::executor::block_on(opened).unwrap();
        actor
    }

    #[test]
    fn rejects_zero_capacity_without_spawning() {
        let result = ActorHandle::<()>::spawn(0, PROJECTION_WORKER_NAME, || {
            unreachable!("zero-capacity admission must fail before opening")
        });
        assert!(matches!(result, Err(EngineError::Invalid(_))));
    }

    #[test]
    fn opening_failure_is_reported_and_drain_resolves() {
        let (actor, opened) = ActorHandle::<()>::spawn(1, PROJECTION_WORKER_NAME, || {
            Err(EngineError::Storage("open failed".to_string()))
        })
        .unwrap();
        assert!(matches!(
            futures::executor::block_on(opened),
            Err(EngineError::Storage(message)) if message == "open failed"
        ));
        assert!(matches!(
            futures::executor::block_on(actor.close_and_drain()),
            Err(EngineError::Storage(message)) if message == "open failed"
        ));
    }

    #[test]
    fn accepted_operation_survives_reply_cancellation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let worker_calls = Arc::clone(&calls);
        let actor = actor((), 1);
        let reply = actor
            .enqueue(move |_| {
                worker_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        drop(reply);

        futures::executor::block_on(actor.close_and_drain()).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mailbox_backpressure_is_bounded_and_explicit() {
        let actor = actor((), 1);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first = actor
            .enqueue(move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(1_u8)
            })
            .unwrap();
        started_rx.recv().unwrap();
        let second = actor.enqueue(|_| Ok(2_u8)).unwrap();
        assert!(matches!(
            actor.enqueue(|_| Ok(3_u8)),
            Err(EngineError::Unavailable)
        ));

        release_tx.send(()).unwrap();
        assert_eq!(futures::executor::block_on(first).unwrap(), 1);
        assert_eq!(futures::executor::block_on(second).unwrap(), 2);
        futures::executor::block_on(actor.close_and_drain()).unwrap();
    }

    #[test]
    fn close_rejects_new_work_and_drains_accepted_work() {
        let actor = actor(0_u8, 1);
        let accepted = actor
            .enqueue(|value| {
                *value += 1;
                Ok(*value)
            })
            .unwrap();
        actor.close();
        assert!(matches!(
            actor.enqueue(|_| Ok(())),
            Err(EngineError::Unavailable)
        ));
        assert_eq!(futures::executor::block_on(accepted).unwrap(), 1);
        futures::executor::block_on(actor.close_and_drain()).unwrap();
    }

    #[test]
    fn worker_panic_resolves_running_queued_and_drain_waiters() {
        let actor = actor((), 2);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let panicking = actor
            .enqueue::<(), _>(move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                panic!("intentional actor failure")
            })
            .unwrap();
        started_rx.recv().unwrap();
        let queued = actor.enqueue(|_| Ok(())).unwrap();
        actor.close();
        release_tx.send(()).unwrap();

        assert!(matches!(
            futures::executor::block_on(panicking),
            Err(EngineError::Storage(_))
        ));
        assert!(matches!(
            futures::executor::block_on(queued),
            Err(EngineError::Storage(_))
        ));
        assert!(matches!(
            futures::executor::block_on(actor.close_and_drain()),
            Err(EngineError::Storage(_))
        ));
    }

    #[test]
    fn public_constructor_futures_are_send() {
        fn assert_send<T: Send>(_: T) {}

        assert_send(AsyncPostgresRelationalProjection::connect(
            "postgres://unused",
        ));
        assert_send(AsyncPostgresRelationalProjection::connect_with_capacity(
            "postgres://unused",
            1,
        ));
        assert_send(AsyncPostgresRelationalProjection::connect_in_schema(
            "postgres://unused",
            "schema",
        ));
        assert_send(
            AsyncPostgresRelationalProjection::connect_in_schema_with_capacity(
                "postgres://unused",
                "schema",
                1,
            ),
        );
        assert_send(AsyncPostgresRelationalControlPlane::connect(
            "postgres://unused",
        ));
        assert_send(AsyncPostgresRelationalControlPlane::connect_with_capacity(
            "postgres://unused",
            1,
        ));
        assert_send(AsyncPostgresRelationalControlPlane::connect_in_schema(
            "postgres://unused",
            "schema",
        ));
        assert_send(
            AsyncPostgresRelationalControlPlane::connect_in_schema_with_capacity(
                "postgres://unused",
                "schema",
                1,
            ),
        );
    }
}
