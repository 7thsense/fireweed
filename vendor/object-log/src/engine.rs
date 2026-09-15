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
    /// path). Raise for parallel S3 PUTs.
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
    put: Option<TokioJoinHandle<Result<(), ObjectLogError>>>,
    put_started: Instant,
    put_result: Option<Result<Duration, ObjectLogError>>,
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
    /// Highest seq fully sealed (put + sequencer commit finished).
    completed_through: u64,
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
    /// call are not required to finish. Empty buffer returns immediately.
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
    let mut counter = rt
        .block_on(recover_data_object_counter(blob.as_ref(), &prefix))
        .unwrap_or(0);
    let mut pending: VecDeque<FlushWork<S::Meta>> = VecDeque::new();
    let mut active_puts = 0usize;
    let mut shutdown = false;

    loop {
        while !shutdown && active_puts < max_inflight {
            let wait_for_more = pending.is_empty();
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

        while pending
            .front()
            .is_some_and(|work| work.put_result.is_some())
        {
            let work = pending.pop_front().expect("front exists");
            let released = finish_flush_work(&shared, &blob, &sequencer, work);
            let mut q = shared.queue.lock().expect("poisoned");
            q.bytes_in_use = q.bytes_in_use.saturating_sub(released);
            shared.cv.notify_all();
            made_progress = true;
        }

        if pending.is_empty() {
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
            let linger = {
                let queued = q.bytes;
                let last_enq = q.last_enqueue;
                let oldest = q.oldest_enqueue;
                drop(q);
                let l = effective_linger(shared, config, queued, last_enq, oldest);
                q = shared.queue.lock().expect("poisoned");
                l
            };
            if linger.is_zero() {
                q = shared.cv.wait(q).expect("poisoned");
            } else {
                let (guard, timeout) = shared.cv.wait_timeout(q, linger).expect("poisoned");
                q = guard;
                if timeout.timed_out() && q.items.is_empty() {
                    return TakeBatch::Empty;
                }
            }
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
        if q.flush_waiters[i].0 <= done {
            let (_, tx) = q.flush_waiters.swap_remove(i);
            let _ = tx.send(Ok(()));
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
    // Durable-then-sequence: the object PUT may have overlapped later PUTs, but
    // sequencer commits are still completed in object creation order.
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
            let mut i = 0;
            while i < q.flush_waiters.len() {
                if q.flush_waiters[i].0 <= done {
                    let (_, tx) = q.flush_waiters.swap_remove(i);
                    let _ = tx.send(Err(e.clone()));
                } else {
                    i += 1;
                }
            }
            shared.cv.notify_all();
            return release_bytes;
        }
    };

    // Media ops for the data object put.
    let mut media_ops = blob.take_media_op_stats().map(|s| s.media_ops).unwrap_or(1); // fallback: 1 per successful put

    // Signal Durable-level waiters now (after PUT, before commit).
    send_durable_acks(&mut work.responders);

    // Sequence the whole object atomically.
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
    match sequencer.commit(&commit_batches) {
        Ok(outcomes) => {
            if let Some(commit_started) = commit_started {
                eprintln!(
                    "object-log flush timing: bytes={} batches={} put_ms={} commit_ms={}",
                    work.bytes,
                    commit_batches.len(),
                    put_elapsed.as_millis(),
                    commit_started.elapsed().as_millis()
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
            send_storage_error(&mut work.responders, e);
        }
    }

    {
        let mut budget = shared.budget.lock().expect("poisoned");
        budget.consume_after_flush(media_ops, Instant::now());
    }
    {
        let mut q = shared.queue.lock().expect("poisoned");
        if work.max_seq >= q.completed_through {
            q.completed_through = work.max_seq;
        }
        notify_flush_waiters(&mut q);
    }
    shared.cv.notify_all();

    release_bytes
}
