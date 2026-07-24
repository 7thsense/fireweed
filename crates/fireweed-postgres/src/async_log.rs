//! Bounded asynchronous ownership adapter for the synchronous Postgres command-log axis.
//!
//! A dedicated OS thread owns the [`PostgresLog`] and its connection for the adapter's entire lifetime.
//! Each accepted mailbox job is one complete [`AsyncLogStore`] operation, so a Postgres transaction is
//! never split across blocking tasks and no blocking client call runs on an async executor thread.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

use fireweed_engine::{
    AsyncLogStore, CommandEnvelope, CommandPage, CommandPosition, DurabilityClass, EngineError,
    EngineResult, LogStore, QueueKey,
};

use crate::{PostgresConnectConfig, PostgresLog};

/// Default number of complete log operations that may wait behind the operation currently running.
pub const DEFAULT_ASYNC_LOG_MAILBOX_CAPACITY: usize = 64;

const WORKER_NAME: &str = "fireweed-postgres-log";

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
            .expect("Postgres log actor reply poisoned");
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
            .expect("Postgres log actor reply sender already completed");
        let waker = {
            let mut state = state.lock().expect("Postgres log actor reply poisoned");
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
            let mut state = state.lock().expect("Postgres log actor reply poisoned");
            state.value = Some(Err(EngineError::Storage(
                "Postgres log actor exited before replying".to_string(),
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
                .expect("Postgres log actor completion poisoned");
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
            .expect("Postgres log actor completion poisoned");
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
                "Postgres log actor exited unexpectedly".to_string(),
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
            .expect("Postgres log actor admission poisoned")
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
    fn spawn<F>(mailbox_capacity: usize, open: F) -> EngineResult<(Self, Reply<()>)>
    where
        F: FnOnce() -> EngineResult<S> + Send + 'static,
    {
        if mailbox_capacity == 0 {
            return Err(EngineError::Invalid(
                "Postgres log actor mailbox capacity must be positive",
            ));
        }
        let (sender, receiver) = mpsc::sync_channel::<Job<S>>(mailbox_capacity);
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
                // Drop the sync client on its owning blocking thread before reporting completion.
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
            .expect("Postgres log actor admission poisoned");
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
            .expect("Postgres log actor admission poisoned")
            .sender
            .take();
    }

    async fn close_and_drain(&self) -> EngineResult<()> {
        self.close();
        self.actor.completion.wait().await
    }
}

/// Async command-log adapter backed by one bounded, dedicated blocking owner thread.
///
/// Once an operation is accepted, dropping its caller future discards only the reply. The owned operation
/// remains queued and runs to completion. Admission saturation and admission after close fail explicitly
/// with [`EngineError::Unavailable`].
#[derive(Clone)]
pub struct AsyncPostgresLog {
    actor: ActorHandle<PostgresLog>,
}

impl AsyncPostgresLog {
    pub async fn connect(url: &str) -> EngineResult<Self> {
        Self::connect_with_config(PostgresConnectConfig::new(url)).await
    }

    pub async fn connect_with_config(config: PostgresConnectConfig) -> EngineResult<Self> {
        Self::connect_with_config_and_capacity(config, DEFAULT_ASYNC_LOG_MAILBOX_CAPACITY).await
    }

    pub async fn connect_with_config_and_capacity(
        config: PostgresConnectConfig,
        mailbox_capacity: usize,
    ) -> EngineResult<Self> {
        let (actor, opened) = ActorHandle::spawn(mailbox_capacity, move || {
            PostgresLog::connect_with_config(config)
        })?;
        opened.await?;
        Ok(Self { actor })
    }

    pub async fn connect_in_schema(url: &str, schema: &str) -> EngineResult<Self> {
        Self::connect_in_schema_with_capacity(url, schema, DEFAULT_ASYNC_LOG_MAILBOX_CAPACITY).await
    }

    pub async fn connect_in_schema_with_capacity(
        url: &str,
        schema: &str,
        mailbox_capacity: usize,
    ) -> EngineResult<Self> {
        let url = url.to_string();
        let schema = schema.to_string();
        let (actor, opened) = ActorHandle::spawn(mailbox_capacity, move || {
            PostgresLog::connect_in_schema(&url, &schema)
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

impl AsyncLogStore for AsyncPostgresLog {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn ensure_shard(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| LogStore::ensure_shard(store, &shard))
                .await
        }
    }

    fn current_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| LogStore::current_epoch(store, &shard))
                .await
        }
    }

    fn acquire_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| LogStore::acquire_epoch(store, &shard))
                .await
        }
    }

    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl Future<Output = EngineResult<Vec<CommandPosition>>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| LogStore::append(store, &shard, &commands, expected_epoch))
                .await
        }
    }

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl Future<Output = EngineResult<CommandPage>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| LogStore::read_from(store, &shard, from, limit))
                .await
        }
    }

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| LogStore::high_water(store, &shard))
                .await
        }
    }

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.actor.clone();
        async move {
            actor
                .execute(move |store| LogStore::set_high_water(store, &shard, position))
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn actor<S: Send + 'static>(store: S, capacity: usize) -> ActorHandle<S> {
        let (actor, opened) = ActorHandle::spawn(capacity, move || Ok(store)).unwrap();
        futures::executor::block_on(opened).unwrap();
        actor
    }

    #[test]
    fn rejects_zero_capacity_without_spawning() {
        let result = ActorHandle::spawn(0, || Ok(()));
        assert!(matches!(result, Err(EngineError::Invalid(_))));
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
}
