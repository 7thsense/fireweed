use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

use pqueue_engine::{
    AsyncLogStore, CommandEnvelope, CommandPage, CommandPosition, DurabilityClass, EngineError,
    EngineResult, LogStore, QueueKey,
};

use crate::ObjectLog;
use crate::segmented::{BlobStore, SegmentConfig};

/// Maximum accepted operations, including operations currently running, for the default adapter.
pub const DEFAULT_ASYNC_OBJECT_LOG_CAPACITY: usize = 64;
/// Blocking workers shared by the keyed object-log executor by default.
pub const DEFAULT_ASYNC_OBJECT_LOG_WORKERS: usize = 4;

const WORKER_NAME: &str = "pqueue-objectlog";

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
        let mut state = self.state.lock().expect("object-log actor reply poisoned");
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
            .expect("object-log actor reply sender already completed");
        let waker = {
            let mut state = state.lock().expect("object-log actor reply poisoned");
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
            let mut state = state.lock().expect("object-log actor reply poisoned");
            state.value = Some(Err(EngineError::Storage(
                "object-log actor exited before replying".to_string(),
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
                .expect("object-log actor completion poisoned");
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
            .expect("object-log actor completion poisoned");
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

type Operation = Box<dyn FnOnce(&ObjectLog) + Send + 'static>;

struct Job {
    operation: Operation,
}

struct ExecutorState {
    closed: bool,
    failure: Option<EngineError>,
    outstanding: usize,
    live_workers: usize,
    queues: HashMap<QueueKey, VecDeque<Job>>,
    active: HashSet<QueueKey>,
}

struct SharedExecutor {
    log: Arc<ObjectLog>,
    capacity: usize,
    state: Mutex<ExecutorState>,
    ready: Condvar,
    completion: Completion,
}

impl SharedExecutor {
    fn take_job(&self) -> Option<(QueueKey, Job)> {
        let mut state = self.state.lock().expect("object-log executor poisoned");
        loop {
            let ready_key = state
                .queues
                .iter()
                .find(|(key, queue)| !queue.is_empty() && !state.active.contains(*key))
                .map(|(key, _)| key.clone());
            if let Some(key) = ready_key {
                let job = state
                    .queues
                    .get_mut(&key)
                    .expect("ready object-log shard missing")
                    .pop_front()
                    .expect("ready object-log shard empty");
                state.active.insert(key.clone());
                return Some((key, job));
            }
            if state.closed && state.outstanding == 0 {
                return None;
            }
            state = self
                .ready
                .wait(state)
                .expect("object-log executor poisoned while waiting");
        }
    }

    fn finish_job(&self, key: &QueueKey) {
        let mut state = self.state.lock().expect("object-log executor poisoned");
        state.active.remove(key);
        state.outstanding -= 1;
        if state.queues.get(key).is_some_and(VecDeque::is_empty) {
            state.queues.remove(key);
        }
        self.ready.notify_all();
    }

    fn fail_job(&self, key: &QueueKey) {
        let queued = {
            let mut state = self.state.lock().expect("object-log executor poisoned");
            state.active.remove(key);
            state.outstanding -= 1;
            state.closed = true;
            state.failure.get_or_insert_with(|| {
                EngineError::Storage("object-log worker exited unexpectedly".to_string())
            });
            let queued = state
                .queues
                .drain()
                .flat_map(|(_, jobs)| jobs)
                .collect::<Vec<_>>();
            state.outstanding -= queued.len();
            queued
        };
        drop(queued);
        self.ready.notify_all();
    }

    fn worker_exited(&self) {
        let result = {
            let mut state = self.state.lock().expect("object-log executor poisoned");
            state.live_workers -= 1;
            (state.live_workers == 0).then(|| match &state.failure {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            })
        };
        if let Some(result) = result {
            self.completion.finish(result);
        }
    }
}

struct WorkerExitGuard {
    shared: Arc<SharedExecutor>,
}

impl Drop for WorkerExitGuard {
    fn drop(&mut self) {
        self.shared.worker_exited();
    }
}

struct RunningJobGuard {
    shared: Arc<SharedExecutor>,
    key: QueueKey,
    complete: bool,
}

impl RunningJobGuard {
    fn complete(mut self) {
        self.shared.finish_job(&self.key);
        self.complete = true;
    }
}

impl Drop for RunningJobGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.shared.fail_job(&self.key);
        }
    }
}

fn worker_loop(shared: Arc<SharedExecutor>) {
    let _exit = WorkerExitGuard {
        shared: Arc::clone(&shared),
    };
    while let Some((key, job)) = shared.take_job() {
        let running = RunningJobGuard {
            shared: Arc::clone(&shared),
            key,
            complete: false,
        };
        (job.operation)(&shared.log);
        running.complete();
    }
}

struct Actor {
    shared: Arc<SharedExecutor>,
    group_commit: bool,
    group_shards: Mutex<HashSet<QueueKey>>,
    _workers: Vec<JoinHandle<()>>,
}

impl Actor {
    fn new(log: ObjectLog, capacity: usize, worker_count: usize) -> EngineResult<Self> {
        if capacity == 0 {
            return Err(EngineError::Invalid(
                "object-log executor capacity must be positive",
            ));
        }
        if worker_count == 0 {
            return Err(EngineError::Invalid(
                "object-log executor worker count must be positive",
            ));
        }
        let group_commit = log.shared_is_group_commit();
        let shared = Arc::new(SharedExecutor {
            log: Arc::new(log),
            capacity,
            state: Mutex::new(ExecutorState {
                closed: false,
                failure: None,
                outstanding: 0,
                live_workers: 0,
                queues: HashMap::new(),
                active: HashSet::new(),
            }),
            ready: Condvar::new(),
            completion: Completion::default(),
        });
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            {
                let mut state = shared.state.lock().expect("object-log executor poisoned");
                state.live_workers += 1;
            }
            let worker_shared = Arc::clone(&shared);
            match thread::Builder::new()
                .name(format!("{WORKER_NAME}-{index}"))
                .spawn(move || worker_loop(worker_shared))
            {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    let mut state = shared.state.lock().expect("object-log executor poisoned");
                    state.live_workers -= 1;
                    state.closed = true;
                    shared.ready.notify_all();
                    return Err(EngineError::Storage(error.to_string()));
                }
            }
        }
        Ok(Self {
            shared,
            group_commit,
            group_shards: Mutex::new(HashSet::new()),
            _workers: workers,
        })
    }

    fn close(&self) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("object-log executor poisoned");
        state.closed = true;
        self.shared.ready.notify_all();
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.close();
    }
}

/// Async keyed executor for the production segmented [`ObjectLog`].
///
/// Operations are FIFO within a shard. Independent shards may run concurrently on separate workers while
/// sharing the same segmented log and group-commit buffers. The capacity is shared across running and queued
/// operations; dropping an accepted caller future discards only its reply.
#[derive(Clone)]
pub struct AsyncObjectLog {
    actor: Arc<Actor>,
}

impl AsyncObjectLog {
    pub async fn open(root: impl Into<PathBuf>) -> EngineResult<Self> {
        Self::open_with_limits(
            root,
            DEFAULT_ASYNC_OBJECT_LOG_CAPACITY,
            DEFAULT_ASYNC_OBJECT_LOG_WORKERS,
        )
        .await
    }

    pub async fn open_with_limits(
        root: impl Into<PathBuf>,
        capacity: usize,
        workers: usize,
    ) -> EngineResult<Self> {
        let root = root.into();
        Self::open_from(capacity, workers, move || ObjectLog::open(root)).await
    }

    pub async fn open_with_blob_store(store: Arc<dyn BlobStore>) -> EngineResult<Self> {
        Self::open_with_blob_store_and_limits(
            store,
            DEFAULT_ASYNC_OBJECT_LOG_CAPACITY,
            DEFAULT_ASYNC_OBJECT_LOG_WORKERS,
        )
        .await
    }

    pub async fn open_with_blob_store_and_limits(
        store: Arc<dyn BlobStore>,
        capacity: usize,
        workers: usize,
    ) -> EngineResult<Self> {
        Self::open_from(capacity, workers, move || {
            ObjectLog::open_with_blob_store(store)
        })
        .await
    }

    pub async fn open_group_commit(
        root: impl Into<PathBuf>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        Self::open_group_commit_with_limits(
            root,
            config,
            DEFAULT_ASYNC_OBJECT_LOG_CAPACITY,
            DEFAULT_ASYNC_OBJECT_LOG_WORKERS,
        )
        .await
    }

    pub async fn open_group_commit_with_limits(
        root: impl Into<PathBuf>,
        config: SegmentConfig,
        capacity: usize,
        workers: usize,
    ) -> EngineResult<Self> {
        let root = root.into();
        Self::open_from(capacity, workers, move || {
            ObjectLog::open_group_commit(root, config)
        })
        .await
    }

    pub async fn open_group_commit_with_blob_store(
        store: Arc<dyn BlobStore>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        Self::open_group_commit_with_blob_store_and_limits(
            store,
            config,
            DEFAULT_ASYNC_OBJECT_LOG_CAPACITY,
            DEFAULT_ASYNC_OBJECT_LOG_WORKERS,
        )
        .await
    }

    pub async fn open_group_commit_with_blob_store_and_limits(
        store: Arc<dyn BlobStore>,
        config: SegmentConfig,
        capacity: usize,
        workers: usize,
    ) -> EngineResult<Self> {
        Self::open_from(capacity, workers, move || {
            ObjectLog::open_group_commit_with_blob_store(store, config)
        })
        .await
    }

    async fn open_from<F>(capacity: usize, workers: usize, open: F) -> EngineResult<Self>
    where
        F: FnOnce() -> EngineResult<ObjectLog> + Send + 'static,
    {
        if capacity == 0 {
            return Err(EngineError::Invalid(
                "object-log executor capacity must be positive",
            ));
        }
        if workers == 0 {
            return Err(EngineError::Invalid(
                "object-log executor worker count must be positive",
            ));
        }
        let (sender, opened) = reply_channel();
        thread::Builder::new()
            .name(format!("{WORKER_NAME}-open"))
            .spawn(move || sender.send(open()))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        let log = opened.await?;
        Ok(Self {
            actor: Arc::new(Actor::new(log, capacity, workers)?),
        })
    }

    fn enqueue<T, F>(&self, key: QueueKey, operation: F) -> EngineResult<Reply<T>>
    where
        T: Send + 'static,
        F: FnOnce(&ObjectLog) -> EngineResult<T> + Send + 'static,
    {
        let (reply_sender, reply) = reply_channel();
        let job = Job {
            operation: Box::new(move |log| reply_sender.send(operation(log))),
        };
        let mut state = self
            .actor
            .shared
            .state
            .lock()
            .expect("object-log executor poisoned");
        if state.closed || state.outstanding >= self.actor.shared.capacity {
            return Err(EngineError::Unavailable);
        }
        state.outstanding += 1;
        state.queues.entry(key).or_default().push_back(job);
        self.actor.shared.ready.notify_one();
        Ok(reply)
    }

    async fn execute<T, F>(&self, key: QueueKey, operation: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&ObjectLog) -> EngineResult<T> + Send + 'static,
    {
        self.enqueue(key, operation)?.await
    }

    /// Buffer a group-commit batch and atomically advance high-water if this enqueue triggers a seal.
    ///
    /// An empty result for a non-empty batch means the commands remain unsealed and are not durable or
    /// acknowledged. A higher-level commit task must retain its response ownership until a later seal returns
    /// their positions and projection distribution completes.
    pub async fn group_commit_enqueue(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.actor.group_commit {
            return Err(EngineError::Unavailable);
        }
        let key = shard.clone();
        self.actor
            .group_shards
            .lock()
            .expect("object-log group shard set poisoned")
            .insert(key.clone());
        self.execute(key, move |log| {
            log.shared_gc_enqueue_and_advance(&shard, &commands, expected_epoch, now_ms)
        })
        .await
    }

    /// Force-seal a group-commit buffer and monotonically advance high-water in the same keyed operation.
    pub async fn group_commit_seal(
        &self,
        shard: QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.actor.group_commit {
            return Err(EngineError::Unavailable);
        }
        let key = shard.clone();
        self.execute(key, move |log| {
            log.shared_gc_seal_and_advance(&shard, expected_epoch, now_ms)
        })
        .await
    }

    /// Enqueue this owned batch and force-seal the queue buffer in one keyed actor operation.
    /// No public enqueue, flush, or second committer can interleave between the two substrate calls.
    pub async fn group_commit_enqueue_and_seal(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.actor.group_commit {
            return Err(EngineError::Unavailable);
        }
        let key = shard.clone();
        self.actor
            .group_shards
            .lock()
            .expect("object-log group shard set poisoned")
            .insert(key.clone());
        self.execute(key, move |log| {
            log.shared_gc_enqueue_seal_and_advance(&shard, &commands, expected_epoch, now_ms)
        })
        .await
    }

    /// Seal a latency-due group buffer and monotonically advance high-water as one keyed operation.
    pub async fn group_commit_flush_due(
        &self,
        shard: QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.actor.group_commit {
            return Err(EngineError::Unavailable);
        }
        let key = shard.clone();
        self.execute(key, move |log| {
            log.shared_gc_flush_due_and_advance(&shard, expected_epoch, now_ms)
        })
        .await
    }

    /// Stop admission without sealing group buffers.
    ///
    /// This is the crash-style stop primitive. Group-commit callers that need graceful shutdown must seal
    /// every accepted buffer first and then use [`Self::close_and_drain`].
    pub fn close(&self) {
        self.actor.close();
    }

    /// Drain accepted operations and fail closed if group-mode commands remain buffered and unsealed.
    pub async fn close_and_drain(&self) -> EngineResult<()> {
        self.close();
        self.actor.shared.completion.wait().await?;
        if self.actor.group_commit {
            let shards = self
                .actor
                .group_shards
                .lock()
                .expect("object-log group shard set poisoned")
                .clone();
            if shards
                .iter()
                .any(|shard| self.actor.shared.log.shared_pending(shard) != 0)
            {
                return Err(EngineError::Storage(
                    "object-log group-commit shutdown has unsealed commands".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl AsyncLogStore for AsyncObjectLog {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }

    fn ensure_shard(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.clone();
        let key = shard.clone();
        async move {
            actor
                .execute(key, move |log| log.shared_ensure_shard(&shard))
                .await
        }
    }

    fn current_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let actor = self.clone();
        let key = shard.clone();
        async move {
            actor
                .execute(key, move |log| LogStore::current_epoch(log, &shard))
                .await
        }
    }

    fn acquire_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let actor = self.clone();
        let key = shard.clone();
        async move {
            actor
                .execute(key, move |log| log.shared_acquire_epoch(&shard))
                .await
        }
    }

    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl Future<Output = EngineResult<Vec<CommandPosition>>> + Send {
        let actor = self.clone();
        let key = shard.clone();
        async move {
            if actor.actor.group_commit {
                return Err(EngineError::Unavailable);
            }
            actor
                .execute(key, move |log| {
                    log.shared_append(&shard, &commands, expected_epoch)
                })
                .await
        }
    }

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl Future<Output = EngineResult<CommandPage>> + Send {
        let actor = self.clone();
        let key = shard.clone();
        async move {
            actor
                .execute(key, move |log| {
                    LogStore::read_from(log, &shard, from, limit)
                })
                .await
        }
    }

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let actor = self.clone();
        let key = shard.clone();
        async move {
            actor
                .execute(key, move |log| LogStore::high_water(log, &shard))
                .await
        }
    }

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let actor = self.clone();
        let key = shard.clone();
        async move {
            actor
                .execute(key, move |log| log.shared_set_high_water(&shard, position))
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use pqueue_conformance::{envelope, item};
    use pqueue_core::{ItemId, QueueId, TenantId};
    use pqueue_engine::{PushCommand, QueueCommand};

    use super::*;
    use crate::segmented::InMemoryBlobStore;

    static NEXT_ROOT: AtomicUsize = AtomicUsize::new(0);

    fn root() -> PathBuf {
        std::env::temp_dir().join(format!(
            "pqueue-async-objectlog-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn shard(name: &str) -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new(name).unwrap(),
        )
    }

    fn command(id: &str) -> CommandEnvelope {
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item(id, id, 1)],
            }),
            vec![ItemId::new(id).unwrap()],
        )
    }

    fn assert_send<T: Send>(_: T) {}

    #[tokio::test(flavor = "current_thread")]
    async fn constructors_and_log_futures_are_send() {
        assert_send(AsyncObjectLog::open(root()));
        assert_send(AsyncObjectLog::open_with_limits(root(), 2, 2));
        assert_send(AsyncObjectLog::open_group_commit(
            root(),
            SegmentConfig::new(1024, 100).unwrap(),
        ));
        let log = AsyncObjectLog::open(root()).await.unwrap();
        assert_send(log.ensure_shard(shard("queue")));
        assert_send(log.close_and_drain());
        let group_root = root();
        let group =
            AsyncObjectLog::open_group_commit(&group_root, SegmentConfig::new(1024, 100).unwrap())
                .await
                .unwrap();
        group.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(group_root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn independent_shards_progress_while_one_shard_is_stalled() {
        let log = AsyncObjectLog::open_with_limits(root(), 4, 2)
            .await
            .unwrap();
        let shard_a = shard("a");
        let shard_b = shard("b");
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let queued_a_ran = Arc::new(AtomicBool::new(false));
        let stalled = log
            .enqueue(shard_a.clone(), move |_| {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started_receiver.recv().unwrap();
        let queued_flag = Arc::clone(&queued_a_ran);
        let queued_a = log
            .enqueue(shard_a, move |_| {
                queued_flag.store(true, Ordering::Release);
                Ok(())
            })
            .unwrap();
        let shard_b_done = log.enqueue(shard_b, |_| Ok(())).unwrap();

        shard_b_done.await.unwrap();
        assert!(!queued_a_ran.load(Ordering::Acquire));
        release_sender.send(()).unwrap();
        stalled.await.unwrap();
        queued_a.await.unwrap();
        {
            let state = log
                .actor
                .shared
                .state
                .lock()
                .expect("object-log executor poisoned");
            assert_eq!(state.outstanding, 0);
            assert!(state.active.is_empty());
            assert!(state.queues.is_empty());
        }
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_open_progress_is_runtime_independent() {
        let root = root();
        let open_root = root.clone();
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let mut opening = Box::pin(AsyncObjectLog::open_from(2, 2, move || {
            started_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            ObjectLog::open(open_root)
        }));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(opening.as_mut().poll(&mut context), Poll::Pending));
        started_receiver.recv().unwrap();
        let heartbeat = tokio::spawn(async {
            tokio::task::yield_now().await;
            9
        });
        assert_eq!(heartbeat.await.unwrap(), 9);
        release_sender.send(()).unwrap();
        let log = opening.await.unwrap();
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shared_capacity_and_close_drain_are_bounded_and_async() {
        let log = AsyncObjectLog::open_with_limits(root(), 2, 2)
            .await
            .unwrap();
        let key = shard("queue");
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let stalled = log
            .enqueue(key.clone(), move |_| {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started_receiver.recv().unwrap();
        let queued = log.enqueue(key.clone(), |_| Ok(())).unwrap();
        assert!(matches!(
            log.enqueue(key, |_| Ok::<_, EngineError>(())),
            Err(EngineError::Unavailable)
        ));
        log.close();
        let mut drain = Box::pin(log.close_and_drain());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(drain.as_mut().poll(&mut context), Poll::Pending));
        release_sender.send(()).unwrap();
        stalled.await.unwrap();
        queued.await.unwrap();
        drain.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_after_acceptance_keeps_owned_operation() {
        let log = AsyncObjectLog::open_with_limits(root(), 2, 2)
            .await
            .unwrap();
        let key = shard("queue");
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let stalled = log
            .enqueue(key.clone(), move |_| {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started_receiver.recv().unwrap();
        let mut accepted = Box::pin(log.ensure_shard(key.clone()));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            accepted.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(accepted);
        release_sender.send(()).unwrap();
        stalled.await.unwrap();
        assert_eq!(log.current_epoch(key).await.unwrap(), 0);
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_panic_fails_accepted_queue_and_drain() {
        let log = AsyncObjectLog::open_with_limits(root(), 2, 1)
            .await
            .unwrap();
        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let panicked = log
            .enqueue::<(), _>(shard("a"), move |_| {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                panic!("intentional object-log actor test panic")
            })
            .unwrap();
        started_receiver.recv().unwrap();
        let queued = log.enqueue(shard("b"), |_| Ok(())).unwrap();
        release_sender.send(()).unwrap();
        assert!(matches!(panicked.await, Err(EngineError::Storage(_))));
        assert!(matches!(queued.await, Err(EngineError::Storage(_))));
        assert!(matches!(
            log.close_and_drain().await,
            Err(EngineError::Storage(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn group_commit_blob_store_constructor_buffers_then_seals() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            4,
            2,
        )
        .await
        .unwrap();
        let key = shard("queue");
        log.ensure_shard(key.clone()).await.unwrap();
        let buffered = log
            .group_commit_enqueue(key.clone(), vec![command("1")], 0, 0)
            .await
            .unwrap();
        assert!(buffered.is_empty());
        assert_eq!(log.high_water(key.clone()).await.unwrap(), None);
        let sealed = log.group_commit_seal(key.clone(), 0, 0).await.unwrap();
        assert_eq!(sealed, vec![CommandPosition::new(key.clone(), 0, 0)]);
        assert_eq!(log.high_water(key).await.unwrap(), Some(sealed[0].clone()));
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn force_seal_append_replays_through_async_trait() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        let key = shard("queue");
        log.ensure_shard(key.clone()).await.unwrap();
        assert!(matches!(
            log.group_commit_enqueue(key.clone(), vec![command("2")], 0, 0)
                .await,
            Err(EngineError::Unavailable)
        ));
        let command = command("1");
        let positions = log
            .append(key.clone(), vec![command.clone()], 0)
            .await
            .unwrap();
        let page = log.read_from(key, None, 1).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].0, positions[0]);
        assert_eq!(page.entries[0].1.item_ids, command.item_ids);
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn group_mode_rejects_ordinary_append_without_sealing_prior_buffer() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            4,
            2,
        )
        .await
        .unwrap();
        let key = shard("mixed");
        log.ensure_shard(key.clone()).await.unwrap();
        assert!(
            log.group_commit_enqueue(key.clone(), vec![command("1")], 0, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            log.append(key.clone(), vec![command("2")], 0).await,
            Err(EngineError::Unavailable)
        ));
        let sealed = log.group_commit_seal(key.clone(), 0, 0).await.unwrap();
        assert_eq!(sealed, vec![CommandPosition::new(key.clone(), 0, 0)]);
        let page = log.read_from(key, None, 10).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].1.item_ids, command("1").item_ids);
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compound_seal_prevents_interleaved_high_water_regression() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            4,
            2,
        )
        .await
        .unwrap();
        let key = shard("monotonic");
        log.ensure_shard(key.clone()).await.unwrap();
        log.group_commit_enqueue(key.clone(), vec![command("1")], 0, 0)
            .await
            .unwrap();
        let first = log.group_commit_seal(key.clone(), 0, 0).await.unwrap();
        log.group_commit_enqueue(key.clone(), vec![command("2")], 0, 0)
            .await
            .unwrap();

        let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
        let stalled = log
            .enqueue(key.clone(), move |_| {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started_receiver.recv().unwrap();
        let mut seal = Box::pin(log.group_commit_seal(key.clone(), 0, 0));
        let mut regress = Box::pin(log.set_high_water(key.clone(), first[0].clone()));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(seal.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(regress.as_mut().poll(&mut context), Poll::Pending));
        release_sender.send(()).unwrap();
        stalled.await.unwrap();
        let second = seal.await.unwrap();
        assert!(matches!(regress.await, Err(EngineError::Invalid(_))));
        assert_eq!(log.high_water(key).await.unwrap(), Some(second[0].clone()));
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_and_drain_fails_closed_with_unsealed_group_buffer() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            2,
            2,
        )
        .await
        .unwrap();
        let key = shard("pending");
        log.ensure_shard(key.clone()).await.unwrap();
        log.group_commit_enqueue(key, vec![command("1")], 0, 0)
            .await
            .unwrap();
        assert!(matches!(
            log.close_and_drain().await,
            Err(EngineError::Storage(_))
        ));
    }
}
