//! The buffered, multiplexing log engine.

use crate::budget::{
    BudgetConfig, BudgetMode, BudgetRuntime, EffectiveKnob, EffectiveReason, PipelineSnapshot,
};
use crate::sequencer::BatchLocation;
use crate::{
    BlobStore, CommitBatch, CommitOutcome, IndexEntry, ObjectLogError, PartitionKey, Sequencer,
};
use bytes::Bytes;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::task::JoinHandle as TokioJoinHandle;

const STORAGE_RETRY_ATTEMPTS: usize = 5;
const STORAGE_RETRY_BASE_DELAY: Duration = Duration::from_millis(25);

/// The durability point a [`LogEngine::produce`] call resolves at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Resolve as soon as the batch is buffered (fire-and-forget; may be lost on
    /// crash before the flush). No offset is returned.
    Buffered,
    /// Resolve once the containing object is durably PUT (survives crash). No
    /// offset yet — the commit has not run.
    Durable,
    /// Resolve once the batch is durably PUT **and** sequenced (has a stable
    /// offset). This is the strong, no-loss level.
    Sequenced,
}

/// Flush-trigger policy for the engine's group-commit buffer.
#[derive(Clone, Copy, Debug)]
pub struct FlushConfig {
    /// Hard ceiling on one sealed object's payload size (memory / object-store
    /// physics). **Not** the primary packing control: under normal load packing
    /// is `ingest_rate × linger`. Keep this high enough that **linger binds**
    /// before size (default **1 GiB**). Lower only for tight RAM or backend caps.
    pub max_bytes: usize,
    /// Flush once this many batches are buffered (secondary ceiling).
    pub max_batches: usize,
    /// **Maximum** time a batch may wait before a deadline flush (hard ceiling).
    /// This is the latency↔throughput control surface: longer wait ⇒ more bytes
    /// per seal ⇒ fewer durable ops/s. The budget controller may use a shorter
    /// *effective* linger when media is idle (TD-004). `ZERO` = seal as soon as
    /// any data is buffered (no co-buffer wait). Default `50ms`.
    pub linger: Duration,
    /// Max sealed objects PUT concurrently. Default **1** (single-flight bulk
    /// path). Raise for parallel S3 PUTs. Also bounds ready objects per atomic
    /// commit when the sequencer opts in to multi-object commits.
    pub max_inflight_flushes: usize,
    /// Max bytes in the mutable queue plus in-flight seals. Producers block when
    /// exceeded. Default **2 GiB**.
    pub max_buffered_bytes: usize,
    /// Durable-ops budget controller (default on for Fjord).
    pub budget: BudgetConfig,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            // Physics/safety ceiling so linger defines segment size under load.
            max_bytes: 1024 * 1024 * 1024,
            max_batches: 10_000,
            linger: Duration::from_millis(50),
            // 1 = single-flight seal (best Local bulk); raise for parallel S3 PUTs.
            max_inflight_flushes: 1,
            max_buffered_bytes: 2 * 1024 * 1024 * 1024,
            budget: BudgetConfig::default(),
        }
    }
}

/// Outcome of a [`LogEngine::produce`] call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    /// First assigned offset, when resolved at [`Durability::Sequenced`].
    pub base_offset: Option<i64>,
    /// Last assigned offset, when resolved at [`Durability::Sequenced`].
    pub last_offset: Option<i64>,
    /// Whether the batch is durably stored.
    pub durable: bool,
    /// Whether the batch has been sequenced (has an offset).
    pub sequenced: bool,
}

/// A batch read back by [`LogEngine::fetch`], with its assigned base offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedBatch {
    /// First offset of the batch.
    pub base_offset: i64,
    /// Number of records in the batch.
    pub record_count: i32,
    /// The opaque batch payload.
    pub payload: Bytes,
}

/// Snapshot of the engine's buffering envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BufferStats {
    /// Bytes still waiting in the mutable accumulation queue.
    pub queued_bytes: usize,
    /// Bytes owned by queued plus sealed in-flight flush work.
    pub bytes_in_use: usize,
    /// Batches still waiting in the mutable accumulation queue.
    pub queued_batches: usize,
    /// Configured upper bound for `bytes_in_use`.
    pub max_buffered_bytes: usize,
}

type Responder = oneshot::Sender<Result<AppendOutcome, ObjectLogError>>;

fn retryable_storage_error(err: &ObjectLogError) -> bool {
    matches!(err, ObjectLogError::StorageUnavailable(_))
}

async fn retry_delay(attempt: usize) {
    let multiplier = 1u32 << attempt.min(5);
    tokio::time::sleep(STORAGE_RETRY_BASE_DELAY * multiplier).await;
}

async fn put_chunks_with_retries(
    blob: &Arc<dyn BlobStore>,
    key: &str,
    chunks: Vec<Bytes>,
) -> Result<(), ObjectLogError> {
    // `Bytes::clone` is refcount-only; Local put_chunks streams without re-merge.
    let mut attempt = 0usize;
    loop {
        match blob.put_chunks(key, chunks.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) if retryable_storage_error(&err) && attempt < STORAGE_RETRY_ATTEMPTS => {
                retry_delay(attempt).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn get_range_with_retries(
    blob: &Arc<dyn BlobStore>,
    key: &str,
    range: std::ops::Range<u64>,
) -> Result<Option<Bytes>, ObjectLogError> {
    let mut attempt = 0usize;
    loop {
        match blob.get_range(key, range.clone()).await {
            Ok(bytes) => return Ok(bytes),
            Err(err) if retryable_storage_error(&err) && attempt < STORAGE_RETRY_ATTEMPTS => {
                retry_delay(attempt).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

struct Pending<M> {
    partition: PartitionKey,
    record_count: i32,
    payload: Bytes,
    meta: M,
    durability: Durability,
    responder: Option<Responder>,
    /// Monotonic enqueue id for [`LogEngine::flush`] barriers.
    seq: u64,
}

struct FlushWork<M> {
    batch: Vec<Pending<M>>,
    locations: Vec<BatchLocation>,
    responders: Vec<(Durability, Option<Responder>)>,
    bytes: usize,
    /// Highest enqueue seq included in this flush object.
    max_seq: u64,
    /// Durable data objects represented by this ordered commit group.
    data_objects: usize,
    put: Option<TokioJoinHandle<Result<(), ObjectLogError>>>,
    put_started: Instant,
    put_result: Option<Result<Duration, ObjectLogError>>,
}

struct CommitJob {
    task: TokioJoinHandle<usize>,
    first_seq: u64,
}

enum TakeBatch<M> {
    Batch(Vec<Pending<M>>),
    Empty,
    Shutdown,
}

type FlushWaiter = (u64, oneshot::Sender<Result<(), ObjectLogError>>);

struct Queue<M> {
    items: VecDeque<Pending<M>>,
    bytes: usize,
    bytes_in_use: usize,
    shutdown: bool,
    /// Next seq to assign on enqueue.
    next_seq: u64,
    /// Highest settled enqueue sequence; failures are retained separately.
    completed_through: u64,
    /// Earliest failed enqueue prefix. A later success cannot make it durable.
    first_failed: Option<(u64, ObjectLogError)>,
    /// When true, take_batch flushes even if under size/linger.
    force_flush: bool,
    /// Waiters: (barrier_seq inclusive, responder).
    flush_waiters: Vec<FlushWaiter>,
    /// Last successful produce enqueue (for idle early-flush).
    last_enqueue: Option<Instant>,
    /// Enqueue time of the front item (linger deadline = oldest + linger).
    oldest_enqueue: Option<Instant>,
}

struct Shared<M> {
    queue: Mutex<Queue<M>>,
    cv: Condvar,
    max_buffered_bytes: usize,
    /// Budget controller + inspectable counters (TD-004).
    budget: Mutex<BudgetRuntime>,
    /// Max linger (operator ceiling) and budget config copy for the flush loop.
    flush_config: FlushConfig,
}

/// A buffered, multiplexing append-log engine over a [`BlobStore`], with
/// sequencing delegated to a [`Sequencer`].
///
/// `produce` group-commits: many batches across many partitions are multiplexed
/// into one object, PUT durably, then handed to the sequencer in a single call —
/// so PUT count is decoupled from produce count. A single flush worker preserves
/// per-[`PartitionKey`] arrival order and never splits a partition across
/// concurrent commits.
pub struct LogEngine<S: Sequencer> {
    shared: Arc<Shared<S::Meta>>,
    blob: Arc<dyn BlobStore>,
    sequencer: Arc<S>,
    /// Data-object key prefix (`<prefix><counter:020>`). Used by orphan reaping.
    key_prefix: String,
    flush_thread: Option<JoinHandle<()>>,
}

impl<S> LogEngine<S>
where
    S: Sequencer + 'static,
    S::Meta: Send + 'static,
{
    /// Create an engine over `blob` and `sequencer` with the given flush policy.
    /// Objects are keyed `<key_prefix><counter>`; pick a prefix unique to this
    /// engine instance if several share a store.
    pub fn new(
        blob: Arc<dyn BlobStore>,
        sequencer: Arc<S>,
        config: FlushConfig,
        key_prefix: impl Into<String>,
    ) -> Self {
        if let Err(msg) = config.budget.validate() {
            panic!("invalid FlushConfig.budget: {msg}");
        }
        if std::env::var("OLOG_DEBUG_FLUSH_CONFIG").is_ok() {
            eprintln!("object-log flush config: {config:?}");
        }
        let budget_rt = BudgetRuntime::new(config.budget);
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                items: VecDeque::new(),
                bytes: 0,
                bytes_in_use: 0,
                shutdown: false,
                next_seq: 1, // first enqueued batch gets seq 1
                completed_through: 0,
                first_failed: None,
                force_flush: false,
                flush_waiters: Vec::new(),
                last_enqueue: None,
                oldest_enqueue: None,
            }),
            cv: Condvar::new(),
            max_buffered_bytes: config.max_buffered_bytes.max(config.max_bytes),
            budget: Mutex::new(budget_rt),
            flush_config: config,
        });
        let key_prefix = key_prefix.into();
        let flush_thread = {
            let shared = Arc::clone(&shared);
            let blob = Arc::clone(&blob);
            let sequencer = Arc::clone(&sequencer);
            let prefix = key_prefix.clone();
            std::thread::Builder::new()
                .name("object-log-flush".into())
                .spawn(move || flush_loop(shared, blob, sequencer, config, prefix))
                .expect("spawn flush thread")
        };
        Self {
            shared,
            blob,
            sequencer,
            key_prefix,
            flush_thread: Some(flush_thread),
        }
    }

    /// Prefix used for data objects written by this engine.
    pub fn data_prefix(&self) -> &str {
        &self.key_prefix
    }

    /// Borrow the engine's blob store (e.g. for orphan reaping or inspection).
    pub fn blob_store(&self) -> &Arc<dyn BlobStore> {
        &self.blob
    }

    /// Borrow the engine's sequencer.
    pub fn sequencer(&self) -> &Arc<S> {
        &self.sequencer
    }

    /// Buffer a batch and resolve at the requested [`Durability`].
    pub async fn produce(
        &self,
        partition: PartitionKey,
        payload: Bytes,
        record_count: i32,
        meta: S::Meta,
        durability: Durability,
    ) -> Result<AppendOutcome, ObjectLogError> {
        if record_count <= 0 {
            return Err(ObjectLogError::InvalidBatch(
                "record_count must be > 0".into(),
            ));
        }
        if matches!(durability, Durability::Buffered) {
            self.enqueue(Pending {
                partition,
                record_count,
                payload,
                meta,
                durability,
                responder: None,
                seq: 0, // filled in enqueue
            })?;
            return Ok(AppendOutcome {
                base_offset: None,
                last_offset: None,
                durable: false,
                sequenced: false,
            });
        }
        // fail_closed: reserve predicted media ops before waiting on flush.
        {
            let mut budget = self.shared.budget.lock().expect("poisoned");
            if budget.config.enabled
                && budget.config.mode == BudgetMode::FailClosed
                && !budget.reserve_for_fail_closed(Instant::now())
            {
                return Err(ObjectLogError::BudgetExceeded(
                    "insufficient durable-ops budget to admit produce".into(),
                ));
            }
        }
        // budget_priority: wait briefly for tokens when empty.
        if self.shared.flush_config.budget.enabled
            && self.shared.flush_config.budget.mode == BudgetMode::BudgetPriority
        {
            let timeout = self.shared.flush_config.budget.admission_timeout;
            let deadline = Instant::now() + timeout;
            loop {
                let mut budget = self.shared.budget.lock().expect("poisoned");
                if budget.can_admit_now(Instant::now()) {
                    break;
                }
                drop(budget);
                if Instant::now() >= deadline {
                    return Err(ObjectLogError::BudgetExceeded(
                        "timed out waiting for durable-ops budget".into(),
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(Pending {
            partition,
            record_count,
            payload,
            meta,
            durability,
            responder: Some(tx),
            seq: 0,
        })?;
        rx.await
            .map_err(|_| ObjectLogError::Sequencer("flush worker stopped".into()))?
    }

    /// Seal every batch enqueued **at or before** this call (barrier).
    ///
    /// Use after [`Durability::Buffered`] produces to wait for durable PUT (and
    /// sequencing) of that prior work. Concurrent produces enqueued after this
    /// call are not required to finish. An empty successful buffer returns immediately.
    /// A failed PUT or commit remains an error for every barrier covering that
    /// enqueue prefix, even after later appends succeed. Reopen the engine to
    /// establish a new prefix from its successfully committed index.
    pub async fn flush(&self) -> Result<(), ObjectLogError> {
        let rx = {
            let mut q = self.shared.queue.lock().expect("poisoned");
            if q.shutdown {
                return Err(ObjectLogError::Sequencer("engine shutting down".into()));
            }
            if q.next_seq == 1 {
                // Nothing has ever been enqueued.
                return Ok(());
            }
            let barrier = q.next_seq - 1;
            if let Some((sequence, error)) = &q.first_failed {
                if *sequence <= barrier {
                    return Err(error.clone());
                }
            }
            if q.completed_through >= barrier && q.items.is_empty() && q.bytes_in_use == 0 {
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            q.force_flush = true;
            q.flush_waiters.push((barrier, tx));
            self.shared.cv.notify_all();
            rx
        };
        rx.await
            .map_err(|_| ObjectLogError::Sequencer("flush worker stopped".into()))?
    }

    fn enqueue(&self, mut item: Pending<S::Meta>) -> Result<(), ObjectLogError> {
        let item_len = item.payload.len();
        let max_buffered_bytes = self.shared.max_buffered_bytes;
        let shared = Arc::clone(&self.shared);
        let mut q = shared.queue.lock().expect("poisoned");
        while !q.shutdown
            && q.bytes_in_use > 0
            && q.bytes_in_use.saturating_add(item_len) > max_buffered_bytes
        {
            q = shared.cv.wait(q).expect("poisoned");
        }
        if q.shutdown {
            return Err(ObjectLogError::Sequencer("engine shutting down".into()));
        }
        let now = Instant::now();
        item.seq = q.next_seq;
        q.next_seq = q.next_seq.saturating_add(1);
        q.bytes += item_len;
        q.bytes_in_use += item_len;
        q.last_enqueue = Some(now);
        if q.oldest_enqueue.is_none() {
            q.oldest_enqueue = Some(now);
        }
        q.items.push_back(item);
        shared.cv.notify_all();
        Ok(())
    }

    /// Read batches covering offsets at/after `offset`, up to ~`max_bytes`.
    pub async fn fetch(
        &self,
        partition: &PartitionKey,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<FetchedBatch>, ObjectLogError> {
        let entries = self.sequencer.lookup(partition, offset)?;
        let mut out = Vec::new();
        let mut total = 0usize;
        for e in entries {
            if total >= max_bytes && !out.is_empty() {
                break;
            }
            let batch = self.load_entry(&e).await?;
            total += batch.payload.len();
            out.push(batch);
        }
        Ok(out)
    }

    /// Stream batches from `offset` onward without materializing a full `Vec`.
    ///
    /// Calls `visit` once per index entry in order. Suitable for wide offset
    /// windows (bounded-RAM replay). Stops and returns the error if `visit`
    /// fails. Unlike [`Self::fetch`], there is no byte budget — visit every remaining
    /// batch (or stop yourself inside `visit`).
    pub async fn fetch_stream<F>(
        &self,
        partition: &PartitionKey,
        offset: i64,
        mut visit: F,
    ) -> Result<(), ObjectLogError>
    where
        F: FnMut(FetchedBatch) -> Result<(), ObjectLogError>,
    {
        let entries = self.sequencer.lookup(partition, offset)?;
        for e in entries {
            visit(self.load_entry(&e).await?)?;
        }
        Ok(())
    }

    async fn load_entry(&self, e: &IndexEntry) -> Result<FetchedBatch, ObjectLogError> {
        let start = e.location.byte_start as u64;
        let end = start + e.location.byte_len as u64;
        let bytes = get_range_with_retries(&self.blob, &e.location.object_id, start..end)
            .await?
            .ok_or_else(|| ObjectLogError::MissingObject(e.location.object_id.clone()))?;
        Ok(FetchedBatch {
            base_offset: e.base_offset,
            record_count: e.record_count,
            payload: bytes,
        })
    }

    /// Drop the partition's log below `offset` and delete any object that thereby
    /// becomes fully unreferenced.
    pub async fn truncate_before(
        &self,
        partition: &PartitionKey,
        offset: i64,
    ) -> Result<(), ObjectLogError> {
        let dead = self.sequencer.truncate_before(partition, offset)?;
        for object_id in dead {
            self.blob.delete(&object_id).await?;
        }
        Ok(())
    }

    /// Delete data-prefix objects that are not in `live`.
    ///
    /// **Safety:** call only when this engine (and any other writer using the
    /// same prefix) is quiescent — otherwise an in-flight put that has not yet
    /// been committed may be deleted as an "orphan". Prefer running offline or
    /// after drop. Does not touch keys outside [`data_prefix`](Self::data_prefix)
    /// (e.g. a separate manifest prefix is safe).
    ///
    /// Returns the deleted object keys.
    pub async fn reap_orphans(
        &self,
        live: &HashSet<String>,
    ) -> Result<Vec<String>, ObjectLogError> {
        reap_orphans(self.blob.as_ref(), &self.key_prefix, live).await
    }

    /// Return a point-in-time snapshot of queued and in-flight payload bytes.
    pub fn buffer_stats(&self) -> BufferStats {
        let q = self.shared.queue.lock().expect("poisoned");
        BufferStats {
            queued_bytes: q.bytes,
            bytes_in_use: q.bytes_in_use,
            queued_batches: q.items.len(),
            max_buffered_bytes: self.shared.max_buffered_bytes,
        }
    }

    /// Inspect budget controller layers and counters (TD-004).
    pub fn pipeline_snapshot(&self) -> PipelineSnapshot {
        let now = Instant::now();
        let mut budget = self.shared.budget.lock().expect("poisoned");
        budget.refill(now);
        let effective = budget.effective_budget_per_sec;
        let reason = if !budget.config.enabled {
            EffectiveReason::Disabled
        } else if budget.config.budget_per_sec_cap.is_some()
            && budget
                .config
                .budget_per_sec_cap
                .is_some_and(|c| (effective - c).abs() < 1e-9)
        {
            EffectiveReason::ConfigCap
        } else if budget.ongoing_capacity.is_some() {
            EffectiveReason::Ongoing
        } else if budget.startup_capacity.is_some() {
            EffectiveReason::StartupProbe
        } else {
            EffectiveReason::DefaultCapacity
        };
        let (queued, last_enq) = {
            let q = self.shared.queue.lock().expect("poisoned");
            (q.bytes, q.last_enqueue)
        };
        let early = budget.allow_early_flush(now, queued, last_enq);
        let max_linger = self.shared.flush_config.linger;
        let eff_linger = if !budget.config.enabled {
            max_linger
        } else if early || max_linger.is_zero() {
            Duration::ZERO
        } else {
            max_linger
        };
        PipelineSnapshot {
            budget_per_sec: EffectiveKnob {
                configured: budget.config.budget_per_sec_cap,
                startup_measured: budget.startup_capacity,
                ongoing_measured: budget.ongoing_capacity,
                effective,
                reason,
            },
            effective_linger_ms: EffectiveKnob {
                configured: Some(max_linger.as_millis() as u64),
                startup_measured: None,
                ongoing_measured: None,
                effective: eff_linger.as_millis() as u64,
                reason: if early {
                    EffectiveReason::Ongoing
                } else {
                    EffectiveReason::Configured
                },
            },
            max_linger_ms: max_linger.as_millis() as u64,
            token_fill_ratio: budget.fill_ratio(),
            media_ops_total: budget.media_ops_total,
            overdraft_total: budget.overdraft_total,
            flushes_total: budget.flushes_total,
            undersized_deadline_flushes: budget.undersized_deadline_flushes,
            predicted_media_ops: budget.predicted_media_ops,
            budget_enabled: budget.config.enabled,
            budget_mode: budget.config.mode,
        }
    }
}

/// Delete objects under `data_prefix` whose keys are absent from `live`.
///
/// See [`LogEngine::reap_orphans`] for safety notes. `live` is typically
/// [`InMemorySequencer::live_object_ids`](crate::InMemorySequencer::live_object_ids)
/// or [`ManifestSequencer::live_object_ids`](crate::ManifestSequencer::live_object_ids).
pub async fn reap_orphans(
    blob: &dyn BlobStore,
    data_prefix: &str,
    live: &HashSet<String>,
) -> Result<Vec<String>, ObjectLogError> {
    let keys = blob.list(data_prefix).await?;
    let mut deleted = Vec::new();
    for key in keys {
        if live.contains(&key) {
            continue;
        }
        blob.delete(&key).await?;
        deleted.push(key);
    }
    Ok(deleted)
}

impl<S: Sequencer> Drop for LogEngine<S> {
    fn drop(&mut self) {
        {
            let mut q = self.shared.queue.lock().expect("poisoned");
            q.shutdown = true;
        }
        self.shared.cv.notify_all();
        if let Some(t) = self.flush_thread.take() {
            let _ = t.join();
        }
    }
}

/// Highest numeric suffix already present under `data_prefix` (keys shaped
/// `{prefix}{counter:020}`). Used so a reopened engine never reissues object ids.
async fn recover_data_object_counter(
    blob: &dyn BlobStore,
    prefix: &str,
) -> Result<u64, ObjectLogError> {
    let keys = blob.list(prefix).await?;
    let mut max = 0u64;
    for key in keys {
        let Some(suffix) = key.strip_prefix(prefix) else {
            continue;
        };
        if let Ok(n) = suffix.parse::<u64>() {
            max = max.max(n);
        }
    }
    Ok(max)
}

fn flush_loop<S>(
    shared: Arc<Shared<S::Meta>>,
    blob: Arc<dyn BlobStore>,
    sequencer: Arc<S>,
    config: FlushConfig,
    prefix: String,
) where
    S: Sequencer + 'static,
    S::Meta: Send + 'static,
{
    let max_inflight = config.max_inflight_flushes.max(1);
    let commit_group_limit = if sequencer.supports_multi_object_commit() {
        max_inflight
    } else {
        1
    };
    let worker_threads = std::env::var("OBJECT_LOG_FLUSH_RUNTIME_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or_else(|| max_inflight.min(8));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("flush runtime");
    // Resume the data-object counter past any keys already under `prefix`. Restarting
    // at 0 on reopen overwrites sealed objects while manifests still point at the old
    // byte ranges → RangeOutOfBounds / mid-JSON EOF on fetch (fireweed-481d3e43).
    let mut counter = match rt.block_on(recover_data_object_counter(blob.as_ref(), &prefix)) {
        Ok(counter) => counter,
        Err(error) => {
            // A failed listing is not an empty prefix. Fail admission before
            // any PUT can reuse a sealed object's name.
            let _ = fail_engine_and_take_pending_uploads(&shared, &mut VecDeque::new(), 0, error);
            return;
        }
    };
    let mut pending: VecDeque<FlushWork<S::Meta>> = VecDeque::new();
    let mut active_puts = 0usize;
    let mut committing: Option<CommitJob> = None;
    let mut shutdown = false;

    loop {
        while !shutdown && active_puts < max_inflight {
            // Poll the commit job even with no queued uploads: it owns bytes
            // whose release may be needed to admit the next producer.
            let wait_for_more = pending.is_empty() && committing.is_none();
            match take_batch(&shared, config, wait_for_more) {
                TakeBatch::Batch(batch) => {
                    counter += 1;
                    let concurrent = max_inflight > 1;
                    let work = start_flush_work(&rt, &blob, &prefix, counter, batch, concurrent);
                    if concurrent {
                        active_puts += 1;
                    }
                    pending.push_back(work);
                }
                TakeBatch::Empty => break,
                TakeBatch::Shutdown => shutdown = true,
            }
        }

        let mut made_progress = false;
        for work in pending.iter_mut() {
            let Some(put) = work.put.as_ref() else {
                continue;
            };
            if !put.is_finished() {
                continue;
            }
            let put = work.put.take().expect("put handle exists");
            let elapsed = work.put_started.elapsed();
            work.put_result = Some(match rt.block_on(put) {
                Ok(Ok(())) => Ok(elapsed),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(ObjectLogError::StorageUnavailable(format!(
                    "flush task failed: {e}"
                ))),
            });
            active_puts = active_puts.saturating_sub(1);
            made_progress = true;
        }

        if committing
            .as_ref()
            .is_some_and(|job| job.task.is_finished())
        {
            let job = committing.take().expect("completed commit exists");
            match rt.block_on(job.task) {
                Ok(released) => {
                    let mut q = shared.queue.lock().expect("poisoned");
                    q.bytes_in_use = q.bytes_in_use.saturating_sub(released);
                    shared.cv.notify_all();
                }
                Err(error) => {
                    let uploads = fail_engine_and_take_pending_uploads(
                        &shared,
                        &mut pending,
                        job.first_seq,
                        ObjectLogError::Sequencer(format!("commit worker failed: {error}")),
                    );
                    // A shared runtime can outlive this engine. Await accepted uploads
                    // explicitly so close/reopen/reaping cannot race orphan writes.
                    // They are never sequenced after the failed commit.
                    for upload in uploads {
                        let _ = rt.block_on(upload);
                    }
                    return;
                }
            }
            made_progress = true;
        }

        if committing.is_none() {
            if let Some(work) = take_ready_commit_group(&mut pending, commit_group_limit) {
                if max_inflight == 1 {
                    // Preserve the low-overhead single-flight path.
                    let released = finish_flush_work(&shared, &blob, &sequencer, work);
                    let mut q = shared.queue.lock().expect("poisoned");
                    q.bytes_in_use = q.bytes_in_use.saturating_sub(released);
                    shared.cv.notify_all();
                } else {
                    // One ordered committer; uploads can progress during its
                    // durable manifest I/O. Bytes remain charged until completion.
                    let first_seq = work
                        .batch
                        .iter()
                        .map(|p| p.seq)
                        .min()
                        .unwrap_or(work.max_seq);
                    let shared = shared.clone();
                    let blob = blob.clone();
                    let sequencer = sequencer.clone();
                    committing = Some(CommitJob {
                        task: rt.spawn_blocking(move || {
                            finish_flush_work(&shared, &blob, &sequencer, work)
                        }),
                        first_seq,
                    });
                }
                made_progress = true;
            }
        }

        if pending.is_empty() && committing.is_none() {
            {
                let mut q = shared.queue.lock().expect("poisoned");
                if q.items.is_empty() && q.bytes_in_use == 0 {
                    // Nothing in flight: any remaining waiters with barrier already
                    // covered (or no work after last seal) can complete.
                    notify_flush_waiters(&mut q);
                }
            }
            shared.cv.notify_all();
            if shutdown {
                // Fail any remaining flush waiters.
                let mut q = shared.queue.lock().expect("poisoned");
                for (_, tx) in q.flush_waiters.drain(..) {
                    let _ = tx.send(Err(ObjectLogError::Sequencer(
                        "engine shutting down".into(),
                    )));
                }
                return;
            }
            continue;
        }

        if !made_progress {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Close admission and fail queued callers/barriers after an unrecoverable
/// startup or commit failure. Return started uploads for explicit draining.
fn fail_engine_and_take_pending_uploads<M>(
    shared: &Arc<Shared<M>>,
    pending: &mut VecDeque<FlushWork<M>>,
    first_seq: u64,
    error: ObjectLogError,
) -> Vec<TokioJoinHandle<Result<(), ObjectLogError>>> {
    let mut uploads = Vec::new();
    for mut work in pending.drain(..) {
        if let Some(put) = work.put.take() {
            uploads.push(put);
        }
        send_storage_error(&mut work.responders, error.clone());
    }
    // Recover a poisoned guard only to close the engine, never to resume writes.
    let mut q = shared
        .queue
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    q.shutdown = true;
    q.first_failed.get_or_insert((first_seq, error.clone()));
    for mut item in q.items.drain(..) {
        if let Some(tx) = item.responder.take() {
            let _ = tx.send(Err(error.clone()));
        }
    }
    q.bytes = 0;
    // Keep outstanding bytes conservatively charged until the failed engine
    // and its accepted uploads have completed.
    for (_, tx) in q.flush_waiters.drain(..) {
        let _ = tx.send(Err(error.clone()));
    }
    q.force_flush = false;
    shared.cv.notify_all();
    uploads
}

/// Combine only an already-ready contiguous success prefix. Never wait to fill
/// a group or cross an unfinished/failed PUT; preserve object and batch order.
fn take_ready_commit_group<M>(
    pending: &mut VecDeque<FlushWork<M>>,
    max_objects: usize,
) -> Option<FlushWork<M>> {
    pending.front()?.put_result.as_ref()?;
    let mut group = pending.pop_front().expect("ready front exists");
    if group.put_result.as_ref().is_some_and(Result::is_err) {
        return Some(group);
    }
    while group.data_objects < max_objects.max(1) {
        let Some(next) = pending.front() else { break };
        if !next.put_result.as_ref().is_some_and(Result::is_ok)
            || group.data_objects.saturating_add(next.data_objects) > max_objects
        {
            break;
        }
        let mut next = pending.pop_front().expect("ready successor exists");
        group.batch.append(&mut next.batch);
        group.locations.append(&mut next.locations);
        group.responders.append(&mut next.responders);
        group.bytes = group
            .bytes
            .checked_add(next.bytes)
            .expect("bounded pending bytes");
        group.max_seq = group.max_seq.max(next.max_seq);
        group.data_objects += next.data_objects;
        let next_elapsed = next.put_result.take().unwrap().unwrap();
        let elapsed = group.put_result.as_mut().unwrap().as_mut().unwrap();
        *elapsed = (*elapsed).max(next_elapsed);
    }
    Some(group)
}

/// How long to sleep before re-evaluating a seal decision.
/// - `ZERO` means seal now (early-flush or linger deadline hit).
/// - Otherwise wait at most this long (interrupted by new produces).
fn effective_linger<M>(
    shared: &Shared<M>,
    config: FlushConfig,
    queued_bytes: usize,
    last_enqueue: Option<Instant>,
    oldest_enqueue: Option<Instant>,
) -> Duration {
    if config.linger.is_zero() {
        return Duration::ZERO;
    }
    let now = Instant::now();
    if config.budget.enabled {
        let mut budget = shared.budget.lock().expect("poisoned");
        budget.refill(now);
        if budget.allow_early_flush(now, queued_bytes, last_enqueue) {
            budget.note_early_flush(now);
            return Duration::ZERO;
        }
    }
    // True linger: seal when the oldest buffered item reaches max wait.
    if let Some(oldest) = oldest_enqueue {
        let deadline = oldest + config.linger;
        if now >= deadline {
            return Duration::ZERO;
        }
        let until_deadline = deadline.saturating_duration_since(now);
        // Also wake when idle gate could open, so sparse produces don't wait full linger.
        if config.budget.enabled
            && !config.budget.early_flush_idle.is_zero()
            && let Some(last) = last_enqueue
        {
            let idle_at = last + config.budget.early_flush_idle;
            if idle_at > now && idle_at < deadline {
                return idle_at.saturating_duration_since(now);
            }
        }
        return until_deadline;
    }
    config.linger
}

fn take_batch<M>(
    shared: &Arc<Shared<M>>,
    config: FlushConfig,
    wait_when_empty: bool,
) -> TakeBatch<M> {
    let mut q = shared.queue.lock().expect("poisoned");
    loop {
        if q.items.is_empty() {
            if q.shutdown {
                return TakeBatch::Shutdown;
            }
            if !wait_when_empty {
                return TakeBatch::Empty;
            }
            // No pending work needs a timer. Enqueue and shutdown change the
            // predicate under this mutex and notify. Waiting without
            // dropping/reacquiring it first also avoids a lost notification.
            q = shared.cv.wait(q).expect("poisoned");
            continue;
        }

        let linger = {
            let queued = q.bytes;
            let last_enq = q.last_enqueue;
            let oldest = q.oldest_enqueue;
            drop(q);
            let l = effective_linger(shared, config, queued, last_enq, oldest);
            q = shared.queue.lock().expect("poisoned");
            l
        };

        let size_trigger = q.bytes >= config.max_bytes || q.items.len() >= config.max_batches;
        let force = q.force_flush;
        let triggered = q.shutdown || size_trigger || linger.is_zero() || force;
        if triggered {
            if !size_trigger && !q.shutdown && !q.items.is_empty() {
                let mut budget = shared.budget.lock().expect("poisoned");
                if q.bytes < config.max_bytes && q.items.len() < config.max_batches {
                    budget.undersized_deadline_flushes += 1;
                }
            }
            break;
        }

        // Short wait (until idle gate or linger deadline); re-evaluate, do not
        // seal solely because a probe sleep timed out.
        let (guard, _timeout) = shared.cv.wait_timeout(q, linger).expect("poisoned");
        q = guard;
    }

    let force_drain = q.force_flush;
    let mut items = Vec::new();
    let mut bytes = 0usize;
    // On force_flush, still respect max_bytes so one object stays bounded, but
    // ignore max_batches so a barrier can drain large queues across objects.
    while let Some(item) = q.items.pop_front() {
        q.bytes = q.bytes.saturating_sub(item.payload.len());
        bytes += item.payload.len();
        items.push(item);
        if bytes >= config.max_bytes {
            break;
        }
        if !force_drain && items.len() >= config.max_batches {
            break;
        }
    }
    q.oldest_enqueue = if q.items.is_empty() {
        None
    } else {
        // Approximate: next seal window starts now for remaining items.
        Some(Instant::now())
    };
    // Keep forcing while a barrier still has queued work.
    if force_drain {
        q.force_flush = !q.items.is_empty() || !q.flush_waiters.is_empty();
    }
    if items.is_empty() {
        TakeBatch::Empty
    } else {
        TakeBatch::Batch(items)
    }
}

fn notify_flush_waiters<M>(q: &mut Queue<M>) {
    let done = q.completed_through;
    let mut i = 0;
    while i < q.flush_waiters.len() {
        let barrier = q.flush_waiters[i].0;
        let failure = q
            .first_failed
            .as_ref()
            .filter(|(sequence, _)| *sequence <= barrier)
            .map(|(_, error)| error.clone());
        if barrier <= done || failure.is_some() {
            let (_, tx) = q.flush_waiters.swap_remove(i);
            let _ = tx.send(failure.map_or(Ok(()), Err));
        } else {
            i += 1;
        }
    }
    if q.flush_waiters.is_empty() {
        q.force_flush = false;
    } else if q.items.is_empty() && q.bytes_in_use == 0 {
        // Barriers still waiting but no work — should not happen; clear force.
        q.force_flush = true;
    } else {
        q.force_flush = true;
    }
}

fn send_storage_error(responders: &mut [(Durability, Option<Responder>)], err: ObjectLogError) {
    for (_, tx) in responders.iter_mut() {
        if let Some(tx) = tx.take() {
            let _ = tx.send(Err(err.clone()));
        }
    }
}

fn send_durable_acks(responders: &mut [(Durability, Option<Responder>)]) {
    for (durability, tx) in responders.iter_mut() {
        if *durability == Durability::Durable
            && let Some(tx) = tx.take()
        {
            let _ = tx.send(Ok(AppendOutcome {
                base_offset: None,
                last_offset: None,
                durable: true,
                sequenced: false,
            }));
        }
    }
}

fn prepare_flush_work<M>(
    prefix: &str,
    counter: u64,
    mut batch: Vec<Pending<M>>,
) -> (FlushWork<M>, String, Vec<Bytes>) {
    let mut locations: Vec<BatchLocation> = Vec::with_capacity(batch.len());
    let mut chunks: Vec<Bytes> = Vec::with_capacity(batch.len());
    let key = format!("{prefix}{counter:020}");
    let mut offset = 0usize;
    for p in &batch {
        let start = offset as u32;
        offset += p.payload.len();
        chunks.push(p.payload.clone());
        locations.push(BatchLocation {
            object_id: key.clone(),
            byte_start: start,
            byte_len: p.payload.len() as u32,
        });
    }
    if std::env::var("OLOG_DEBUG_FLUSH_CONFIG").is_ok() {
        eprintln!(
            "object-log seal: key={key} batches={} bytes={offset}",
            batch.len()
        );
    }
    let responders: Vec<(Durability, Option<Responder>)> = batch
        .iter_mut()
        .map(|p| (p.durability, p.responder.take()))
        .collect();
    let max_seq = batch.iter().map(|p| p.seq).max().unwrap_or(0);
    let work = FlushWork {
        batch,
        locations,
        responders,
        bytes: offset,
        max_seq,
        data_objects: 1,
        put: None,
        put_started: Instant::now(),
        put_result: None,
    };
    (work, key, chunks)
}

/// Start a durable put for a sealed batch.
///
/// - `concurrent == false` (default `max_inflight_flushes == 1`): `block_on` the
///   put on the flush thread; Local uses `block_in_place` (no spawn_blocking queue).
/// - `concurrent == true`: spawn put tasks for parallel S3-style throughput.
fn start_flush_work<M>(
    rt: &tokio::runtime::Runtime,
    blob: &Arc<dyn BlobStore>,
    prefix: &str,
    counter: u64,
    batch: Vec<Pending<M>>,
    concurrent: bool,
) -> FlushWork<M> {
    let (mut work, key, chunks) = prepare_flush_work(prefix, counter, batch);
    let blob = Arc::clone(blob);
    let _ = blob.take_media_op_stats();
    work.put_started = Instant::now();
    if concurrent {
        work.put =
            Some(rt.spawn(async move { put_chunks_with_retries(&blob, &key, chunks).await }));
    } else {
        let put_result = rt.block_on(put_chunks_with_retries(&blob, &key, chunks));
        let elapsed = work.put_started.elapsed();
        work.put_result = Some(match put_result {
            Ok(()) => Ok(elapsed),
            Err(e) => Err(e),
        });
    }
    work
}

fn finish_flush_work<S, M>(
    shared: &Arc<Shared<M>>,
    blob: &Arc<dyn BlobStore>,
    sequencer: &Arc<S>,
    mut work: FlushWork<S::Meta>,
) -> usize
where
    S: Sequencer + 'static,
    S::Meta: Send + 'static,
{
    let release_bytes = work.bytes;
    let first_seq = work
        .batch
        .iter()
        .map(|pending| pending.seq)
        .min()
        .unwrap_or(work.max_seq);
    // Durable-then-sequence: the object PUT may have overlapped later PUTs, but
    // sequencer commits are still completed in object creation order.
    // Grouped put_ms is the largest observed member PUT duration, not a sum.
    let timing = std::env::var("OLOG_DEBUG_FLUSH_TIMING").is_ok();
    let put_elapsed = match work.put_result.take().expect("put result is ready") {
        Ok(elapsed) => elapsed,
        Err(e) => {
            send_storage_error(&mut work.responders, e.clone());
            let mut q = shared.queue.lock().expect("poisoned");
            // Unblock flush waiters covering this seq with the storage error.
            let done = work.max_seq;
            if done >= q.completed_through {
                q.completed_through = done;
            }
            q.first_failed.get_or_insert((first_seq, e));
            notify_flush_waiters(&mut q);
            shared.cv.notify_all();
            return release_bytes;
        }
    };

    // Media ops for the data object put.
    let mut media_ops = blob
        .take_media_op_stats()
        .map(|s| s.media_ops)
        .unwrap_or(work.data_objects as u64); // fallback: 1 per successful put

    // Signal Durable-level waiters now (after PUT, before commit).
    send_durable_acks(&mut work.responders);

    // Sequence the ready group atomically, preserving each object's locations.
    let commit_batches: Vec<CommitBatch<'_, S::Meta>> = work
        .batch
        .iter()
        .zip(work.locations.iter())
        .map(|(p, loc)| CommitBatch {
            partition: p.partition.clone(),
            record_count: p.record_count,
            location: loc.clone(),
            meta: &p.meta,
        })
        .collect();

    let commit_started = timing.then(Instant::now);
    // Clear stats again so sequencer durable work (e.g. ManifestSequencer put)
    // on the shared store is counted into the same budget.
    let _ = blob.take_media_op_stats();
    let mut commit_error = None;
    match sequencer.commit(&commit_batches) {
        Ok(outcomes) => {
            if let Some(commit_started) = commit_started {
                eprintln!(
                    "object-log flush timing: bytes={} batches={} put_ms={} commit_ms={} objects={}",
                    work.bytes,
                    commit_batches.len(),
                    put_elapsed.as_millis(),
                    commit_started.elapsed().as_millis(),
                    work.data_objects
                );
            }
            if let Some(s) = blob.take_media_op_stats() {
                media_ops = media_ops.saturating_add(s.media_ops);
            }
            for (outcome, (_, tx)) in outcomes.into_iter().zip(work.responders.iter_mut()) {
                if let Some(tx) = tx.take() {
                    let (base, last) = match outcome {
                        CommitOutcome::Assigned {
                            base_offset,
                            record_count,
                        } => (
                            Some(base_offset),
                            Some(base_offset + record_count as i64 - 1),
                        ),
                        CommitOutcome::Duplicate { base_offset } => (Some(base_offset), None),
                    };
                    let _ = tx.send(Ok(AppendOutcome {
                        base_offset: base,
                        last_offset: last,
                        durable: true,
                        sequenced: true,
                    }));
                }
            }
        }
        Err(e) => {
            if let Some(s) = blob.take_media_op_stats() {
                media_ops = media_ops.saturating_add(s.media_ops);
            }
            send_storage_error(&mut work.responders, e.clone());
            commit_error = Some(e);
        }
    }

    {
        let mut budget = shared.budget.lock().expect("poisoned");
        budget.consume_after_flush(media_ops, Instant::now());
    }
    {
        let mut q = shared.queue.lock().expect("poisoned");
        if let Some(error) = commit_error {
            q.first_failed.get_or_insert((first_seq, error));
        }
        if work.max_seq >= q.completed_through {
            q.completed_through = work.max_seq;
        }
        notify_flush_waiters(&mut q);
    }
    shared.cv.notify_all();

    release_bytes
}

#[cfg(test)]
mod flush_barrier_tests {
    use super::*;

    #[test]
    fn failure_only_rejects_barriers_covering_its_enqueue_position() {
        let error = ObjectLogError::Sequencer("manifest failed".into());
        let mut queue = Queue::<()> {
            items: VecDeque::new(),
            bytes: 0,
            bytes_in_use: 1,
            shutdown: false,
            next_seq: 4,
            completed_through: 2,
            first_failed: Some((2, error.clone())),
            force_flush: true,
            flush_waiters: Vec::new(),
            last_enqueue: None,
            oldest_enqueue: None,
        };
        let mut receivers = Vec::new();
        for barrier in [1, 2, 3] {
            let (tx, rx) = oneshot::channel();
            queue.flush_waiters.push((barrier, tx));
            receivers.push(rx);
        }
        notify_flush_waiters(&mut queue);
        assert_eq!(receivers[0].try_recv().unwrap(), Ok(()));
        assert_eq!(receivers[1].try_recv().unwrap(), Err(error.clone()));
        assert_eq!(receivers[2].try_recv().unwrap(), Err(error));
        assert!(queue.flush_waiters.is_empty());
    }
}

#[cfg(test)]
mod ready_commit_group_tests {
    use super::*;
    use crate::{ManifestSequencer, MemoryBlobStore};
    use async_trait::async_trait;
    use std::ops::Range;

    fn work(
        n: u64,
        durability: Durability,
    ) -> (
        FlushWork<()>,
        String,
        Vec<Bytes>,
        oneshot::Receiver<Result<AppendOutcome, ObjectLogError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let (mut work, key, chunks) = prepare_flush_work(
            "data/",
            n,
            vec![Pending {
                partition: PartitionKey("shared".into()),
                record_count: 1,
                payload: Bytes::from(format!("body-{n}")),
                meta: (),
                durability,
                responder: Some(tx),
                seq: n,
            }],
        );
        work.put_result = Some(Ok(Duration::from_millis(n)));
        (work, key, chunks, rx)
    }

    #[test]
    fn ready_groups_are_bounded_and_do_not_cross_unfinished_or_failed_puts() {
        let mut pending = (1..=5)
            .map(|n| work(n, Durability::Sequenced).0)
            .collect::<VecDeque<_>>();
        let original_bytes: usize = pending.iter().map(|w| w.bytes).sum();
        let mut released = 0;
        for expected in [vec![1, 2], vec![3, 4], vec![5]] {
            let group = take_ready_commit_group(&mut pending, 2).unwrap();
            assert_eq!(
                group.batch.iter().map(|p| p.seq).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(group.data_objects, expected.len());
            assert_eq!(group.responders.len(), expected.len());
            assert_eq!(group.max_seq, *expected.last().unwrap());
            for (location, n) in group.locations.iter().zip(&expected) {
                assert_eq!(location.object_id, format!("data/{n:020}"));
                assert_eq!(location.byte_start, 0);
                assert_eq!(location.byte_len as usize, format!("body-{n}").len());
            }
            released += group.bytes;
        }
        assert!(pending.is_empty());
        assert_eq!(released, original_bytes);

        let mut first = work(1, Durability::Sequenced).0;
        first.put_result = None;
        let mut pending = VecDeque::from([first, work(2, Durability::Sequenced).0]);
        assert!(take_ready_commit_group(&mut pending, 8).is_none());
        assert_eq!(
            pending.len(),
            2,
            "a later ready PUT cannot pass an unfinished head"
        );
        pending[0].put_result = Some(Err(ObjectLogError::StorageUnavailable("failed PUT".into())));
        let failed = take_ready_commit_group(&mut pending, 8).unwrap();
        assert_eq!(failed.data_objects, 1);
        assert!(failed.put_result.unwrap().is_err());
        assert_eq!(pending[0].max_seq, 2);

        let mut failed = work(2, Durability::Sequenced).0;
        failed.put_result = Some(Err(ObjectLogError::StorageUnavailable("failed PUT".into())));
        let mut pending = VecDeque::from([
            work(1, Durability::Sequenced).0,
            failed,
            work(3, Durability::Sequenced).0,
        ]);
        assert_eq!(take_ready_commit_group(&mut pending, 8).unwrap().max_seq, 1);
        assert_eq!(
            pending.len(),
            2,
            "a success group cannot cross a failed PUT"
        );
        let mut pending = VecDeque::from([
            work(1, Durability::Sequenced).0,
            work(2, Durability::Sequenced).0,
        ]);
        assert_eq!(
            take_ready_commit_group(&mut pending, 1)
                .unwrap()
                .data_objects,
            1
        );
        assert_eq!(
            pending.len(),
            1,
            "one-object sequencers retain their call boundary"
        );
    }

    struct GatedManifestStore {
        inner: MemoryBlobStore,
        entered: std::sync::mpsc::SyncSender<()>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
        fail: bool,
        panic_on_commit: bool,
    }

    #[async_trait]
    impl BlobStore for GatedManifestStore {
        async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
            if key.starts_with("manifest/") {
                let release = self.release.lock().unwrap().take();
                if let Some(release) = release {
                    self.entered.send(()).unwrap();
                    release.await.unwrap();
                    assert!(!self.panic_on_commit, "injected commit panic");
                    if self.fail {
                        return Err(ObjectLogError::StorageUnavailable(
                            "manifest failure".into(),
                        ));
                    }
                }
            }
            self.inner.put(key, value).await
        }
        async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
            self.inner.get(key).await
        }
        async fn get_range(
            &self,
            key: &str,
            range: Range<u64>,
        ) -> Result<Option<Bytes>, ObjectLogError> {
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
            self.inner.delete(key).await
        }
        fn take_media_op_stats(&self) -> Option<crate::MediaOpStats> {
            self.inner.take_media_op_stats()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grouped_manifest_controls_acknowledgements_and_reopens_exact_locations() {
        for fail in [false, true] {
            let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
            let (release_tx, release_rx) = oneshot::channel();
            let blob: Arc<dyn BlobStore> = Arc::new(GatedManifestStore {
                inner: MemoryBlobStore::new(),
                entered: entered_tx,
                release: Mutex::new(Some(release_rx)),
                fail,
                panic_on_commit: false,
            });
            let sequencer = Arc::new(
                ManifestSequencer::open(blob.clone(), "manifest/")
                    .await
                    .unwrap(),
            );
            assert!(sequencer.supports_multi_object_commit());
            let (first, first_key, first_chunks, mut sequenced_rx) = work(1, Durability::Sequenced);
            let (mut second, second_key, second_chunks, mut durable_rx) =
                work(2, Durability::Durable);
            second.batch[0].partition = PartitionKey("other".into());
            let (third, third_key, third_chunks, mut third_rx) = work(3, Durability::Sequenced);
            blob.put_chunks(&first_key, first_chunks).await.unwrap();
            blob.put_chunks(&second_key, second_chunks).await.unwrap();
            blob.put_chunks(&third_key, third_chunks).await.unwrap();
            let mut pending = VecDeque::from([first, second, third]);
            let group = take_ready_commit_group(&mut pending, 8).unwrap();
            let bytes = group.bytes;
            assert_eq!(group.data_objects, 3);
            let (flush_tx, mut flush_rx) = oneshot::channel();
            let mut config = FlushConfig::default();
            config.budget.enabled = false;
            let shared: Arc<Shared<()>> = Arc::new(Shared {
                queue: Mutex::new(Queue {
                    items: VecDeque::new(),
                    bytes: 0,
                    bytes_in_use: bytes,
                    shutdown: false,
                    next_seq: 4,
                    completed_through: 0,
                    first_failed: None,
                    force_flush: true,
                    flush_waiters: vec![(3, flush_tx)],
                    last_enqueue: None,
                    oldest_enqueue: None,
                }),
                cv: Condvar::new(),
                max_buffered_bytes: bytes,
                budget: Mutex::new(BudgetRuntime::new(config.budget)),
                flush_config: config,
            });
            let worker = {
                let shared = shared.clone();
                let blob = blob.clone();
                let sequencer = sequencer.clone();
                std::thread::spawn(move || finish_flush_work(&shared, &blob, &sequencer, group))
            };
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(matches!(
                sequenced_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                third_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                flush_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            let durable = durable_rx.try_recv().unwrap().unwrap();
            assert!(durable.durable && !durable.sequenced);
            release_tx.send(()).unwrap();
            assert_eq!(worker.join().unwrap(), bytes);
            if fail {
                assert!(sequenced_rx.await.unwrap().is_err());
                assert!(third_rx.await.unwrap().is_err());
                assert!(flush_rx.await.unwrap().is_err());
                assert_eq!(
                    sequencer
                        .high_watermark(&PartitionKey("shared".into()))
                        .unwrap(),
                    0
                );
                assert!(blob.list("manifest/").await.unwrap().is_empty());
            } else {
                assert_eq!(sequenced_rx.await.unwrap().unwrap().base_offset, Some(0));
                assert_eq!(third_rx.await.unwrap().unwrap().base_offset, Some(1));
                flush_rx.await.unwrap().unwrap();
                assert_eq!(blob.list("manifest/").await.unwrap().len(), 1);
            }
            assert_eq!(blob.list("data/").await.unwrap().len(), 3);
            let reopened = ManifestSequencer::open(blob.clone(), "manifest/")
                .await
                .unwrap();
            for (partition, objects) in [("shared", vec![1, 3]), ("other", vec![2])] {
                let entries = reopened.lookup(&PartitionKey(partition.into()), 0).unwrap();
                assert_eq!(entries.len(), if fail { 0 } else { objects.len() });
                for (offset, (entry, object)) in entries.iter().zip(objects).enumerate() {
                    assert_eq!(entry.base_offset, offset as i64);
                    let loc = &entry.location;
                    assert_eq!(loc.object_id, format!("data/{object:020}"));
                    let body = blob
                        .get_range(
                            &loc.object_id,
                            loc.byte_start as u64..(loc.byte_start + loc.byte_len) as u64,
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(body, format!("body-{object}"));
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uploads_continue_while_the_ordered_manifest_commit_is_blocked() {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = oneshot::channel();
        let blob: Arc<dyn BlobStore> = Arc::new(GatedManifestStore {
            inner: MemoryBlobStore::new(),
            entered: entered_tx,
            release: Mutex::new(Some(release_rx)),
            fail: false,
            panic_on_commit: false,
        });
        let sequencer = Arc::new(
            ManifestSequencer::open(blob.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let mut config = FlushConfig::default();
        config.max_batches = 1;
        config.max_inflight_flushes = 4;
        config.linger = Duration::ZERO;
        config.budget.enabled = false;
        let engine = Arc::new(LogEngine::new(blob.clone(), sequencer, config, "data/"));
        let first = {
            let engine = engine.clone();
            tokio::spawn(async move {
                engine
                    .produce(
                        PartitionKey("p".into()),
                        Bytes::from_static(b"first"),
                        1,
                        (),
                        Durability::Sequenced,
                    )
                    .await
            })
        };
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        for n in 1..4 {
            engine
                .produce(
                    PartitionKey("p".into()),
                    Bytes::from(format!("later-{n}")),
                    1,
                    (),
                    Durability::Buffered,
                )
                .await
                .unwrap();
        }
        let upload_progress = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if blob.list("data/").await.unwrap().len() == 4 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .is_ok();
        let early_ack = first.is_finished();
        // Always release before assertions so a failed regression can drain.
        release_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        engine.flush().await.unwrap();
        let rows = engine
            .fetch(&PartitionKey("p".into()), 0, 1024)
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(|r| r.base_offset).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        drop(engine);
        assert!(
            !early_ack,
            "sequenced acknowledgement preceded manifest durability"
        );
        assert!(
            upload_progress,
            "blocked manifest prevented independent data uploads"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_completion_releases_admission_bytes_without_more_uploads() {
        let blob: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
        let sequencer = Arc::new(
            ManifestSequencer::open(blob.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let mut config = FlushConfig::default();
        config.max_bytes = 4;
        config.max_buffered_bytes = 4;
        config.max_batches = 1;
        config.max_inflight_flushes = 4;
        config.linger = Duration::ZERO;
        config.budget.enabled = false;
        let engine = LogEngine::new(blob, sequencer, config, "data/");
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"full"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while engine.buffer_stats().bytes_in_use != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("completed committer stranded admission bytes");
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"next"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        engine.flush().await.unwrap();
        assert_eq!(
            engine
                .fetch(&PartitionKey("p".into()), 0, 100)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    struct FailureInjectingStore {
        inner: Arc<dyn BlobStore>,
        second_upload_gate: Mutex<Option<(std::sync::mpsc::SyncSender<()>, oneshot::Receiver<()>)>>,
        second_upload_completed: std::sync::atomic::AtomicBool,
        fail_data_listing: std::sync::atomic::AtomicBool,
    }

    impl FailureInjectingStore {
        fn new(inner: Arc<dyn BlobStore>) -> Self {
            Self {
                inner,
                second_upload_gate: Mutex::new(None),
                second_upload_completed: std::sync::atomic::AtomicBool::new(false),
                fail_data_listing: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl BlobStore for FailureInjectingStore {
        async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
            let gate = if key == "data/00000000000000000002" {
                self.second_upload_gate.lock().unwrap().take()
            } else {
                None
            };
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.await.unwrap();
                let result = self.inner.put(key, value).await;
                if result.is_ok() {
                    self.second_upload_completed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                return result;
            }
            self.inner.put(key, value).await
        }
        async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
            self.inner.get(key).await
        }
        async fn get_range(
            &self,
            key: &str,
            range: Range<u64>,
        ) -> Result<Option<Bytes>, ObjectLogError> {
            self.inner.get_range(key, range).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
            if prefix == "data/"
                && self
                    .fail_data_listing
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(ObjectLogError::StorageUnavailable(
                    "injected object listing failure".into(),
                ));
            }
            self.inner.list(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
            self.inner.delete(key).await
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_counter_listing_cannot_overwrite_existing_log_objects() {
        let inner = Arc::new(MemoryBlobStore::new());
        let key = "data/00000000000000000001";
        let original = Bytes::from_static(b"existing immutable object");
        inner.put(key, original.clone()).await.unwrap();
        let store = Arc::new(FailureInjectingStore::new(inner.clone()));
        store
            .fail_data_listing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut config = FlushConfig::default();
        config.max_batches = 1;
        config.max_inflight_flushes = 4;
        config.linger = Duration::ZERO;
        config.budget.enabled = false;
        let engine = LogEngine::new(
            store,
            Arc::new(crate::InMemorySequencer::new()),
            config,
            "data/",
        );
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            engine.produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"must not overwrite"),
                1,
                (),
                Durability::Sequenced,
            ),
        )
        .await;
        let flushed = tokio::time::timeout(Duration::from_secs(5), engine.flush()).await;
        tokio::task::spawn_blocking(move || drop(engine))
            .await
            .unwrap();
        let retained = inner.get(key).await.unwrap();
        assert_eq!(
            retained,
            Some(original),
            "failed listing must preserve sealed bytes"
        );
        assert!(
            result.unwrap().is_err(),
            "failed prefix recovery must reject admission"
        );
        assert!(
            flushed.unwrap().is_err(),
            "flush must report failed initialization"
        );
        assert_eq!(inner.list("data/").await.unwrap(), vec![key.to_string()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn engines_commit_and_close_independently() {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = oneshot::channel();
        let a_blob = Arc::new(FailureInjectingStore::new(Arc::new(GatedManifestStore {
            inner: MemoryBlobStore::new(),
            entered: entered_tx,
            release: Mutex::new(Some(release_rx)),
            fail: false,
            panic_on_commit: false,
        })));
        let b_blob = Arc::new(FailureInjectingStore::new(Arc::new(MemoryBlobStore::new())));
        let mut config = FlushConfig::default();
        config.max_batches = 1;
        config.max_inflight_flushes = 4;
        config.linger = Duration::ZERO;
        config.budget.enabled = false;
        let a_seq = Arc::new(
            ManifestSequencer::open(a_blob.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let b_seq = Arc::new(
            ManifestSequencer::open(b_blob.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let a = LogEngine::new(a_blob.clone(), a_seq, config, "data/");
        a.produce(
            PartitionKey("p".into()),
            Bytes::from_static(b"a-first"),
            1,
            (),
            Durability::Buffered,
        )
        .await
        .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let b = LogEngine::new(b_blob.clone(), b_seq, config, "data/");
        let b_result = tokio::time::timeout(
            Duration::from_secs(5),
            b.produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"b-first"),
                1,
                (),
                Durability::Sequenced,
            ),
        )
        .await;
        let mut closing_b = tokio::task::spawn_blocking(move || drop(b));
        let b_closed = tokio::time::timeout(Duration::from_secs(5), &mut closing_b).await;
        // Always release the held manifest before assertions, including the pre-change red run.
        release_tx.send(()).unwrap();
        if b_closed.is_err() {
            closing_b.await.unwrap();
        }
        assert!(
            b_closed.is_ok(),
            "closing B waited for A's blocked manifest"
        );
        assert_eq!(b_result.unwrap().unwrap().base_offset, Some(0));
        a.flush().await.unwrap();
        assert_eq!(
            a.produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"a-second"),
                1,
                (),
                Durability::Sequenced
            )
            .await
            .unwrap()
            .base_offset,
            Some(1)
        );
        let rows = a.fetch(&PartitionKey("p".into()), 0, 100).await.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.payload.clone()).collect::<Vec<_>>(),
            vec![
                Bytes::from_static(b"a-first"),
                Bytes::from_static(b"a-second")
            ]
        );
        tokio::task::spawn_blocking(move || drop(a)).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_engine_drains_accepted_uploads_before_close_and_reopen() {
        let (manifest_entered_tx, manifest_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (manifest_release_tx, manifest_release_rx) = oneshot::channel();
        let (upload_entered_tx, upload_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (upload_release_tx, upload_release_rx) = oneshot::channel();
        let store = Arc::new(FailureInjectingStore::new(Arc::new(GatedManifestStore {
            inner: MemoryBlobStore::new(),
            entered: manifest_entered_tx,
            release: Mutex::new(Some(manifest_release_rx)),
            fail: false,
            panic_on_commit: true,
        })));
        *store.second_upload_gate.lock().unwrap() = Some((upload_entered_tx, upload_release_rx));
        let mut config = FlushConfig::default();
        config.max_batches = 1;
        config.max_inflight_flushes = 4;
        config.linger = Duration::ZERO;
        config.budget.enabled = false;
        let sequencer = Arc::new(
            ManifestSequencer::open(store.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let engine = LogEngine::new(store.clone(), sequencer, config, "data/");
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"first"),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
        manifest_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"second"),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
        upload_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        manifest_release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !engine.shared.queue.lock().unwrap().shutdown {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let flush_failed = engine.flush().await.is_err();
        // A failed committer closes admission first; its accepted PUT must still
        // complete before close returns, even though no manifest may reference it.
        let upload_was_owned = upload_release_tx.send(()).is_ok();
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(move || drop(engine)),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(flush_failed);
        assert!(upload_was_owned);
        assert!(
            store
                .second_upload_completed
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert_eq!(store.list("data/").await.unwrap().len(), 2);
        let sequencer = Arc::new(
            ManifestSequencer::open(store.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let reopened = LogEngine::new(store.clone(), sequencer, config, "data/");
        assert!(
            reopened
                .fetch(&PartitionKey("p".into()), 0, 100)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            reopened
                .produce(
                    PartitionKey("p".into()),
                    Bytes::from_static(b"after-reopen"),
                    1,
                    (),
                    Durability::Sequenced
                )
                .await
                .unwrap()
                .base_offset,
            Some(0)
        );
        let rows = reopened
            .fetch(&PartitionKey("p".into()), 0, 100)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload, Bytes::from_static(b"after-reopen"));
        assert_eq!(store.list("data/").await.unwrap().len(), 3);
        tokio::task::spawn_blocking(move || drop(reopened))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_a_blocked_committer_and_queued_buffered_work() {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = oneshot::channel();
        let blob: Arc<dyn BlobStore> = Arc::new(GatedManifestStore {
            inner: MemoryBlobStore::new(),
            entered: entered_tx,
            release: Mutex::new(Some(release_rx)),
            fail: false,
            panic_on_commit: false,
        });
        let sequencer = Arc::new(
            ManifestSequencer::open(blob.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let mut config = FlushConfig::default();
        config.max_batches = 1;
        config.max_inflight_flushes = 4;
        config.linger = Duration::ZERO;
        config.budget.enabled = false;
        let engine = LogEngine::new(blob.clone(), sequencer, config, "data/");
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"first"),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"second"),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
        let shared = engine.shared.clone();
        let dropping = tokio::task::spawn_blocking(move || drop(engine));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !shared.queue.lock().unwrap().shutdown {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let premature = dropping.is_finished();
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), dropping)
            .await
            .unwrap()
            .unwrap();
        assert!(!premature, "shutdown returned before manifest durability");
        let reopened = Arc::new(
            ManifestSequencer::open(blob.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let engine = LogEngine::new(blob, reopened, config, "data/");
        let rows = engine
            .fetch(&PartitionKey("p".into()), 0, 100)
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(|r| r.payload.as_ref()).collect::<Vec<_>>(),
            vec![b"first".as_slice(), b"second".as_slice()]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panicked_committer_closes_admission_and_fails_flush() {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = oneshot::channel();
        let blob: Arc<dyn BlobStore> = Arc::new(GatedManifestStore {
            inner: MemoryBlobStore::new(),
            entered: entered_tx,
            release: Mutex::new(Some(release_rx)),
            fail: false,
            panic_on_commit: true,
        });
        let sequencer = Arc::new(
            ManifestSequencer::open(blob.clone(), "manifest/")
                .await
                .unwrap(),
        );
        let mut config = FlushConfig::default();
        config.max_batches = 1;
        config.max_inflight_flushes = 4;
        config.linger = Duration::ZERO;
        config.budget.enabled = false;
        let engine = Arc::new(LogEngine::new(blob, sequencer, config, "data/"));
        let first = {
            let engine = engine.clone();
            tokio::spawn(async move {
                engine
                    .produce(
                        PartitionKey("p".into()),
                        Bytes::from_static(b"first"),
                        1,
                        (),
                        Durability::Sequenced,
                    )
                    .await
            })
        };
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"later"),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
        let flushing = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.flush().await })
        };
        release_tx.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), first)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(5), flushing)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while !engine.shared.queue.lock().unwrap().shutdown {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            engine
                .produce(
                    PartitionKey("p".into()),
                    Bytes::from_static(b"rejected"),
                    1,
                    (),
                    Durability::Buffered
                )
                .await
                .is_err()
        );
        assert!(engine.flush().await.is_err());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_reads_continue_during_new_manifest_publication() {
        for fail in [false, true] {
            let memory = MemoryBlobStore::new();
            let seed_blob: Arc<dyn BlobStore> = Arc::new(memory.clone());
            let seed_sequencer = Arc::new(
                ManifestSequencer::open(seed_blob.clone(), "manifest/")
                    .await
                    .unwrap(),
            );
            let mut config = FlushConfig::default();
            config.max_batches = 1;
            config.max_inflight_flushes = 4;
            config.linger = Duration::ZERO;
            config.budget.enabled = false;
            let seed = LogEngine::new(seed_blob, seed_sequencer, config, "data/");
            seed.produce(
                PartitionKey("p".into()),
                Bytes::from_static(b"committed"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
            drop(seed);
            let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
            let (release_tx, release_rx) = oneshot::channel();
            let blob: Arc<dyn BlobStore> = Arc::new(GatedManifestStore {
                inner: memory,
                entered: entered_tx,
                release: Mutex::new(Some(release_rx)),
                fail,
                panic_on_commit: false,
            });
            let sequencer = Arc::new(
                ManifestSequencer::open(blob.clone(), "manifest/")
                    .await
                    .unwrap(),
            );
            let engine = Arc::new(LogEngine::new(
                blob.clone(),
                sequencer.clone(),
                config,
                "data/",
            ));
            let producing = {
                let engine = engine.clone();
                tokio::spawn(async move {
                    engine
                        .produce(
                            PartitionKey("p".into()),
                            Bytes::from_static(b"new"),
                            1,
                            (),
                            Durability::Sequenced,
                        )
                        .await
                })
            };
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut reading = tokio::task::spawn_blocking(move || {
                let p = PartitionKey("p".into());
                (
                    sequencer.high_watermark(&p).unwrap(),
                    sequencer.lookup(&p, 0).unwrap(),
                    sequencer.snapshot(),
                )
            });
            let observed = tokio::time::timeout(Duration::from_secs(1), &mut reading).await;
            let early_ack = producing.is_finished();
            // Release even on regression failure so all worker threads can drain.
            release_tx.send(()).unwrap();
            let outcome = producing.await.unwrap();
            assert_eq!(outcome.is_err(), fail);
            let reads_progressed = observed.is_ok();
            let (watermark, entries, snapshot) = match observed {
                Ok(result) => result.unwrap(),
                Err(_) => reading.await.unwrap(),
            };
            assert!(!early_ack);
            assert!(
                reads_progressed,
                "durable manifest publication blocked committed index reads"
            );
            assert_eq!(watermark, 1);
            assert_eq!(entries.len(), 1);
            assert_eq!(snapshot.manifest_count, 1);
            let rows = engine
                .fetch(&PartitionKey("p".into()), 0, 100)
                .await
                .unwrap();
            assert_eq!(rows.len(), if fail { 1 } else { 2 });
            assert_eq!(rows[0].payload.as_ref(), b"committed");
            drop(engine);
            let reopened = Arc::new(
                ManifestSequencer::open(blob.clone(), "manifest/")
                    .await
                    .unwrap(),
            );
            let engine = LogEngine::new(blob, reopened, config, "data/");
            assert_eq!(
                engine
                    .fetch(&PartitionKey("p".into()), 0, 100)
                    .await
                    .unwrap()
                    .len(),
                if fail { 1 } else { 2 }
            );
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manifest_mutations_remain_serialized_while_readers_progress() {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = oneshot::channel();
        let blob: Arc<dyn BlobStore> = Arc::new(GatedManifestStore {
            inner: MemoryBlobStore::new(),
            entered: entered_tx,
            release: Mutex::new(Some(release_rx)),
            fail: false,
            panic_on_commit: false,
        });
        let sequencer = Arc::new(ManifestSequencer::open(blob, "manifest/").await.unwrap());
        let commit = |sequencer: Arc<ManifestSequencer>, key: &'static str| {
            tokio::task::spawn_blocking(move || {
                sequencer.commit(&[CommitBatch {
                    partition: PartitionKey("p".into()),
                    record_count: 1,
                    location: BatchLocation {
                        object_id: key.into(),
                        byte_start: 0,
                        byte_len: 1,
                    },
                    meta: &(),
                }])
            })
        };
        let first = commit(sequencer.clone(), "data/first");
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = commit(sequencer.clone(), "data/second");
        let truncating = {
            let sequencer = sequencer.clone();
            tokio::task::spawn_blocking(move || {
                sequencer.truncate_before(&PartitionKey("p".into()), 1)
            })
        };
        let reading = {
            let sequencer = sequencer.clone();
            tokio::task::spawn_blocking(move || sequencer.high_watermark(&PartitionKey("p".into())))
        };
        let observed = tokio::time::timeout(Duration::from_secs(1), reading).await;
        release_tx.send(()).unwrap();
        assert_eq!(
            observed.expect("committed reads blocked").unwrap().unwrap(),
            0
        );
        let first = tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let dropped = tokio::time::timeout(Duration::from_secs(5), truncating)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            vec![CommitOutcome::Assigned {
                base_offset: 0,
                record_count: 1
            }]
        );
        assert_eq!(
            second,
            vec![CommitOutcome::Assigned {
                base_offset: 1,
                record_count: 1
            }]
        );
        assert_eq!(dropped, vec!["data/first".to_string()]);
        let p = PartitionKey("p".into());
        assert_eq!(sequencer.high_watermark(&p).unwrap(), 2);
        assert_eq!(sequencer.log_start_offset(&p).unwrap(), 1);
        let entries = sequencer.lookup(&p, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].location.object_id, "data/second");
        assert_eq!(sequencer.snapshot().manifest_count, 2);
    }
}

#[cfg(test)]
mod idle_batch_wait_tests {
    use super::*;
    use std::sync::mpsc;

    fn empty_shared(config: FlushConfig) -> Arc<Shared<()>> {
        Arc::new(Shared {
            queue: Mutex::new(Queue {
                items: VecDeque::new(),
                bytes: 0,
                bytes_in_use: 0,
                shutdown: false,
                next_seq: 1,
                completed_through: 0,
                first_failed: None,
                force_flush: false,
                flush_waiters: Vec::new(),
                last_enqueue: None,
                oldest_enqueue: None,
            }),
            cv: Condvar::new(),
            max_buffered_bytes: config.max_buffered_bytes.max(config.max_bytes),
            budget: Mutex::new(BudgetRuntime::new(config.budget)),
            flush_config: config,
        })
    }

    #[test]
    fn idle_wait_has_no_periodic_empty_return_and_wakes_for_enqueue_or_shutdown() {
        for linger in [Duration::ZERO, Duration::from_millis(1)] {
            for shutdown in [false, true] {
                let config = FlushConfig {
                    linger,
                    ..FlushConfig::default()
                };
                let shared = empty_shared(config);
                let (started_tx, started_rx) = mpsc::channel();
                let (result_tx, result_rx) = mpsc::channel();
                let worker_shared = shared.clone();
                let worker = std::thread::spawn(move || {
                    started_tx.send(()).unwrap();
                    assert!(result_tx.send(take_batch(&worker_shared, config, true)).is_ok());
                });
                started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                let early = result_rx.recv_timeout(Duration::from_millis(100));
                let returned_while_idle = early.is_ok();
                {
                    let mut q = shared.queue.lock().unwrap();
                    if shutdown {
                        q.shutdown = true;
                    } else {
                        let payload = Bytes::from_static(b"first-after-idle");
                        q.bytes = payload.len();
                        q.bytes_in_use = payload.len();
                        q.next_seq = 2;
                        q.last_enqueue = Some(Instant::now());
                        q.oldest_enqueue = q.last_enqueue;
                        q.force_flush = true;
                        q.items.push_back(Pending {
                            partition: PartitionKey("p".into()),
                            record_count: 1,
                            payload,
                            meta: (),
                            durability: Durability::Buffered,
                            responder: None,
                            seq: 1,
                        });
                    }
                    shared.cv.notify_all();
                }
                let result = early
                    .or_else(|_| result_rx.recv_timeout(Duration::from_secs(2)))
                    .unwrap();
                worker.join().unwrap();
                assert!(
                    !returned_while_idle,
                    "idle worker polled with linger={linger:?}"
                );
                match result {
                    TakeBatch::Shutdown => assert!(shutdown),
                    TakeBatch::Batch(items) => {
                        assert!(!shutdown);
                        assert_eq!(items.len(), 1);
                        assert_eq!(items[0].payload.as_ref(), b"first-after-idle");
                        assert_eq!(items[0].seq, 1);
                    }
                    TakeBatch::Empty => panic!("idle worker returned without work or shutdown"),
                }
            }
        }
    }
}
