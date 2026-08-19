//! Provider-neutral bounded apply coordination for object-log projections.
//!
//! The serving projection remains on the response path. This coordinator owns only the selected
//! projection that may lag under `ResponseBarrier::AsyncProjection`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use fireweed_engine::{
    AsyncProjectionSpec, AsyncProjectionStore, CommandEnvelope, CommandPosition, EngineError,
    EngineResult, QueueKey,
};
use tokio::sync::{Mutex, Notify};

/// One admission reserved before the authoritative append begins.
#[derive(Debug)]
pub struct AsyncProjectionApplyReservation {
    id: u64,
    shard: QueueKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncProjectionApplySnapshot {
    pub apply_lag_commands: u64,
    pub apply_debt_bytes: u64,
    pub apply_queue_depth: usize,
    pub oldest_unapplied_age_ms: u64,
    pub apply_retry_count: u32,
    pub applied_high_water: Option<CommandPosition>,
    pub poison_reason: Option<String>,
    pub paused: bool,
}

pub struct AsyncProjectionApplyCoordinator<P>
where
    P: AsyncProjectionStore + 'static,
{
    inner: Arc<CoordinatorInner<P>>,
}

impl<P> Clone for AsyncProjectionApplyCoordinator<P>
where
    P: AsyncProjectionStore + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct CoordinatorInner<P>
where
    P: AsyncProjectionStore + 'static,
{
    projection: Arc<P>,
    spec: AsyncProjectionSpec,
    next_id: AtomicU64,
    paused: AtomicBool,
    worker_running: AtomicBool,
    state: Mutex<CoordinatorState>,
    poisoned: std::sync::RwLock<HashMap<QueueKey, String>>,
    changed: Notify,
    #[cfg(test)]
    injected_apply_failures: std::sync::atomic::AtomicU32,
}

#[derive(Default)]
struct CoordinatorState {
    entries: VecDeque<ApplyEntry>,
    shards: HashMap<QueueKey, ShardApplyState>,
}

enum ApplyEntry {
    Reserved {
        id: u64,
        shard: QueueKey,
        command_count: u64,
        debt_bytes: u64,
        enqueued_at: Instant,
    },
    Ready(ApplyBatch),
}

impl ApplyEntry {
    fn id(&self) -> u64 {
        match self {
            Self::Reserved { id, .. } => *id,
            Self::Ready(batch) => batch.id,
        }
    }

    fn shard(&self) -> &QueueKey {
        match self {
            Self::Reserved { shard, .. } => shard,
            Self::Ready(batch) => &batch.shard,
        }
    }

    fn command_count(&self) -> u64 {
        match self {
            Self::Reserved { command_count, .. } => *command_count,
            Self::Ready(batch) => batch.command_count,
        }
    }

    fn debt_bytes(&self) -> u64 {
        match self {
            Self::Reserved { debt_bytes, .. } => *debt_bytes,
            Self::Ready(batch) => batch.debt_bytes,
        }
    }

    fn enqueued_at(&self) -> Instant {
        match self {
            Self::Reserved { enqueued_at, .. } => *enqueued_at,
            Self::Ready(batch) => batch.enqueued_at,
        }
    }
}

#[derive(Clone)]
struct ApplyBatch {
    id: u64,
    shard: QueueKey,
    positions: Vec<CommandPosition>,
    commands: Vec<CommandEnvelope>,
    command_count: u64,
    debt_bytes: u64,
    enqueued_at: Instant,
}

#[derive(Default)]
struct ShardApplyState {
    retry_count: u32,
    applied_high_water: Option<CommandPosition>,
    poison_reason: Option<String>,
}

impl<P> AsyncProjectionApplyCoordinator<P>
where
    P: AsyncProjectionStore + 'static,
{
    pub fn new(projection: Arc<P>, spec: AsyncProjectionSpec) -> EngineResult<Self> {
        let spec = AsyncProjectionSpec::new(
            spec.apply_lag_max_commands,
            spec.apply_debt_max_bytes,
            spec.apply_queue_depth_max,
            spec.oldest_unapplied_max_ms,
            spec.apply_poison_retry_threshold,
        )?;
        Ok(Self {
            inner: Arc::new(CoordinatorInner {
                projection,
                spec,
                next_id: AtomicU64::new(1),
                paused: AtomicBool::new(false),
                worker_running: AtomicBool::new(false),
                state: Mutex::new(CoordinatorState::default()),
                poisoned: std::sync::RwLock::new(HashMap::new()),
                changed: Notify::new(),
                #[cfg(test)]
                injected_apply_failures: std::sync::atomic::AtomicU32::new(0),
            }),
        })
    }

    pub fn spec(&self) -> AsyncProjectionSpec {
        self.inner.spec
    }

    pub(crate) fn projection(&self) -> &Arc<P> {
        &self.inner.projection
    }

    /// Reserve bounded debt before append so a successful append never exceeds configured debt.
    pub async fn reserve(
        &self,
        shard: QueueKey,
        commands: &[CommandEnvelope],
    ) -> EngineResult<AsyncProjectionApplyReservation> {
        let command_count = u64::try_from(commands.len())
            .map_err(|_| EngineError::Storage("async projection command count overflow".into()))?;
        let mut counter = CountingWriter::default();
        serde_json::to_writer(&mut counter, commands)
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        let debt_bytes = counter.bytes;
        let now = Instant::now();
        let mut state = self.inner.state.lock().await;
        let shard_state = state.shards.entry(shard.clone()).or_default();
        if let Some(reason) = &shard_state.poison_reason {
            return Err(poisoned(reason));
        }

        let debt = debt_for(&state, &shard, now);
        if debt.oldest_unapplied_age_ms >= self.inner.spec.oldest_unapplied_max_ms
            && debt.apply_queue_depth > 0
        {
            return Err(backpressure("async-projection-oldest-unapplied-age"));
        }
        if debt
            .apply_lag_commands
            .checked_add(command_count)
            .is_none_or(|value| value > self.inner.spec.apply_lag_max_commands)
        {
            return Err(backpressure("async-projection-apply-lag-commands"));
        }
        if debt
            .apply_debt_bytes
            .checked_add(debt_bytes)
            .is_none_or(|value| value > self.inner.spec.apply_debt_max_bytes)
        {
            return Err(backpressure("async-projection-apply-debt-bytes"));
        }
        if debt
            .apply_queue_depth
            .checked_add(1)
            .is_none_or(|value| value > self.inner.spec.apply_queue_depth_max)
        {
            return Err(backpressure("async-projection-apply-queue-depth"));
        }

        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        state.entries.push_back(ApplyEntry::Reserved {
            id,
            shard: shard.clone(),
            command_count,
            debt_bytes,
            enqueued_at: now,
        });
        drop(state);
        self.inner.changed.notify_waiters();
        Ok(AsyncProjectionApplyReservation { id, shard })
    }

    /// Cancel a pre-append reservation after append rejection or a deliberate crash cut.
    pub async fn cancel(&self, reservation: AsyncProjectionApplyReservation) {
        let mut state = self.inner.state.lock().await;
        if let Some(index) = state
            .entries
            .iter()
            .position(|entry| entry.id() == reservation.id && entry.shard() == &reservation.shard)
        {
            state.entries.remove(index);
        }
        drop(state);
        self.inner.changed.notify_waiters();
        self.kick();
    }

    /// Publish an appended batch for ordered background apply.
    pub async fn enqueue_reserved(
        &self,
        reservation: AsyncProjectionApplyReservation,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> EngineResult<()> {
        if positions.len() != commands.len()
            || positions
                .iter()
                .any(|position| position.queue != reservation.shard)
            || positions.windows(2).any(|pair| !pair[0].precedes(&pair[1]))
            || positions.windows(2).any(|pair| {
                pair[0].backend_epoch == pair[1].backend_epoch
                    && pair[0].sequence.checked_add(1) != Some(pair[1].sequence)
            })
        {
            return self
                .poison(
                    reservation.shard,
                    "async projection received a non-contiguous committed batch".into(),
                )
                .await;
        }

        let mut state = self.inner.state.lock().await;
        let Some(index) = state
            .entries
            .iter()
            .position(|entry| entry.id() == reservation.id && entry.shard() == &reservation.shard)
        else {
            drop(state);
            return self
                .poison(
                    reservation.shard,
                    "async projection reservation disappeared before enqueue".into(),
                )
                .await;
        };
        let entry = state
            .entries
            .remove(index)
            .expect("entry index was present");
        let ApplyEntry::Reserved {
            id,
            shard,
            command_count,
            debt_bytes,
            enqueued_at,
        } = entry
        else {
            drop(state);
            return self
                .poison(
                    reservation.shard,
                    "async projection reservation was published twice".into(),
                )
                .await;
        };
        state.entries.insert(
            index,
            ApplyEntry::Ready(ApplyBatch {
                id,
                shard,
                positions,
                commands,
                command_count,
                debt_bytes,
                enqueued_at,
            }),
        );
        drop(state);
        self.inner.changed.notify_waiters();
        self.kick();
        Ok(())
    }

    /// Pause background apply. Admissions remain bounded and eventually enter backpressure.
    pub fn pause(&self) {
        self.inner.paused.store(true, Ordering::Release);
    }

    pub fn resume(&self) {
        self.inner.paused.store(false, Ordering::Release);
        self.kick();
    }

    pub async fn snapshot(&self, shard: &QueueKey) -> AsyncProjectionApplySnapshot {
        let state = self.inner.state.lock().await;
        snapshot_for(
            &state,
            shard,
            Instant::now(),
            self.inner.paused.load(Ordering::Acquire),
        )
    }

    /// Reject projection-dependent work synchronously after poison latches for `shard`.
    pub fn ensure_healthy(&self, shard: &QueueKey) -> EngineResult<()> {
        let poison_registry = self.inner.poisoned.read().map_err(|_| {
            EngineError::Storage("async projection poison registry lock failed".into())
        })?;
        match poison_registry.get(shard) {
            Some(reason) => Err(poisoned(reason)),
            None => Ok(()),
        }
    }

    /// Wait until at least one apply batch completes (or the queue is already empty).
    pub async fn wait_for_progress(&self, shard: &QueueKey) -> EngineResult<()> {
        let changed = self.inner.changed.notified();
        let snapshot = self.snapshot(shard).await;
        if let Some(reason) = snapshot.poison_reason {
            return Err(poisoned(&reason));
        }
        if snapshot.apply_queue_depth == 0 {
            return Ok(());
        }
        changed.await;
        Ok(())
    }

    /// Wait until the selected projection covers every currently admitted batch for `shard`.
    pub async fn wait_for_catch_up(&self, shard: &QueueKey) -> EngineResult<()> {
        loop {
            let changed = self.inner.changed.notified();
            let snapshot = self.snapshot(shard).await;
            if let Some(reason) = snapshot.poison_reason {
                return Err(poisoned(&reason));
            }
            if snapshot.apply_queue_depth == 0 {
                return Ok(());
            }
            changed.await;
        }
    }

    /// Seed diagnostic high-water after authoritative recovery has rebuilt the selected projection.
    pub async fn seed_high_water(&self, shard: QueueKey, high_water: Option<CommandPosition>) {
        let mut state = self.inner.state.lock().await;
        state.shards.entry(shard).or_default().applied_high_water = high_water;
    }

    /// Reset one shard after an operator-driven projection rebuild.
    ///
    /// The lifecycle boundary first prevents new admissions and drains admitted work. Rebuild can
    /// then replace the selected projection and clear any prior apply poison without allowing a
    /// stale queued batch to race the recovered image.
    pub async fn reset_after_rebuild(&self, shard: QueueKey, high_water: Option<CommandPosition>) {
        let mut state = self.inner.state.lock().await;
        state.entries.retain(|entry| entry.shard() != &shard);
        state.shards.insert(
            shard.clone(),
            ShardApplyState {
                retry_count: 0,
                applied_high_water: high_water,
                poison_reason: None,
            },
        );
        drop(state);
        if let Ok(mut poisoned) = self.inner.poisoned.write() {
            poisoned.remove(&shard);
        }
        self.inner.changed.notify_waiters();
    }

    #[cfg(test)]
    pub(crate) fn inject_apply_failures(&self, count: u32) {
        self.inner
            .injected_apply_failures
            .store(count, Ordering::Release);
    }

    async fn poison(&self, shard: QueueKey, reason: String) -> EngineResult<()> {
        let mut state = self.inner.state.lock().await;
        state.shards.entry(shard.clone()).or_default().poison_reason = Some(reason.clone());
        drop(state);
        if let Ok(mut poisoned) = self.inner.poisoned.write() {
            poisoned.insert(shard, reason.clone());
        }
        self.inner.changed.notify_waiters();
        Err(poisoned(&reason))
    }

    fn kick(&self) {
        if self.inner.paused.load(Ordering::Acquire)
            || self
                .inner
                .worker_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        let inner = Arc::downgrade(&self.inner);
        crate::compose_log::objectlog_shared_runtime().spawn(async move {
            let Some(inner) = inner.upgrade() else {
                return;
            };
            run_worker(inner).await;
        });
    }
}

async fn run_worker<P>(inner: Arc<CoordinatorInner<P>>)
where
    P: AsyncProjectionStore + 'static,
{
    if inner.spec.apply_start_delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(
            inner.spec.apply_start_delay_ms,
        ))
        .await;
    }
    loop {
        if inner.paused.load(Ordering::Acquire) {
            inner.worker_running.store(false, Ordering::Release);
            if !inner.paused.load(Ordering::Acquire)
                && inner
                    .worker_running
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                continue;
            }
            return;
        }
        let next = {
            let state = inner.state.lock().await;
            next_runnable(&state)
        };
        let Some((_index, batch)) = next else {
            inner.worker_running.store(false, Ordering::Release);
            let has_work = {
                let state = inner.state.lock().await;
                next_runnable(&state).is_some()
            };
            if !inner.paused.load(Ordering::Acquire)
                && has_work
                && inner
                    .worker_running
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                continue;
            }
            return;
        };

        let lineage_error = {
            let state = inner.state.lock().await;
            state
                .shards
                .get(&batch.shard)
                .and_then(|shard_state| shard_state.applied_high_water.as_ref())
                .and_then(|high_water| {
                    let first = batch.positions.first()?;
                    let ordered = high_water.precedes(first);
                    let contiguous = high_water.backend_epoch != first.backend_epoch
                        || high_water.sequence.checked_add(1) == Some(first.sequence);
                    (!ordered || !contiguous).then(|| {
                        format!(
                            "async projection batch does not follow applied high-water: {high_water:?} -> {first:?}"
                        )
                    })
                })
        };
        if let Some(reason) = lineage_error {
            let mut state = inner.state.lock().await;
            state
                .shards
                .entry(batch.shard.clone())
                .or_default()
                .poison_reason = Some(reason.clone());
            drop(state);
            if let Ok(mut poisoned) = inner.poisoned.write() {
                poisoned.insert(batch.shard.clone(), reason);
            }
            inner.changed.notify_waiters();
            continue;
        }

        #[cfg(test)]
        let injected_failure = inner
            .injected_apply_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        #[cfg(not(test))]
        let injected_failure = false;

        let result = if injected_failure {
            Err(EngineError::Storage(
                "injected async projection apply failure".into(),
            ))
        } else {
            AsyncProjectionStore::apply_live(
                inner.projection.as_ref(),
                batch.positions.clone(),
                batch.commands.clone(),
            )
            .await
        };

        let mut state = inner.state.lock().await;
        match result {
            Ok(()) => {
                let matching_index = state
                    .entries
                    .iter()
                    .position(|entry| entry.id() == batch.id);
                if let Some(matching_index) = matching_index {
                    state.entries.remove(matching_index);
                    let shard_state = state.shards.entry(batch.shard.clone()).or_default();
                    shard_state.retry_count = 0;
                    shard_state.applied_high_water = batch.positions.last().cloned();
                } else {
                    let shard_state = state.shards.entry(batch.shard.clone()).or_default();
                    let reason: String =
                        "async projection apply queue changed while a batch was in flight".into();
                    shard_state.poison_reason = Some(reason.clone());
                    if let Ok(mut poisoned) = inner.poisoned.write() {
                        poisoned.insert(batch.shard.clone(), reason);
                    }
                }
            }
            Err(error) => {
                let shard_state = state.shards.entry(batch.shard.clone()).or_default();
                shard_state.retry_count = shard_state.retry_count.saturating_add(1);
                if shard_state.retry_count >= inner.spec.apply_poison_retry_threshold {
                    let reason = format!(
                        "async projection apply failed {} times: {error}",
                        shard_state.retry_count
                    );
                    shard_state.poison_reason = Some(reason.clone());
                    if let Ok(mut poisoned) = inner.poisoned.write() {
                        poisoned.insert(batch.shard.clone(), reason);
                    }
                }
            }
        }
        drop(state);
        inner.changed.notify_waiters();
        tokio::task::yield_now().await;
    }
}

fn next_runnable(state: &CoordinatorState) -> Option<(usize, ApplyBatch)> {
    let mut blocked_shards = HashSet::new();
    for (index, entry) in state.entries.iter().enumerate() {
        let shard = entry.shard();
        if state
            .shards
            .get(shard)
            .is_some_and(|shard_state| shard_state.poison_reason.is_some())
        {
            blocked_shards.insert(shard.clone());
            continue;
        }
        match entry {
            ApplyEntry::Reserved { .. } => {
                blocked_shards.insert(shard.clone());
            }
            ApplyEntry::Ready(batch) if !blocked_shards.contains(shard) => {
                return Some((index, batch.clone()));
            }
            ApplyEntry::Ready(_) => {}
        }
    }
    None
}

struct Debt {
    apply_lag_commands: u64,
    apply_debt_bytes: u64,
    apply_queue_depth: usize,
    oldest_unapplied_age_ms: u64,
}

fn debt_for(state: &CoordinatorState, shard: &QueueKey, now: Instant) -> Debt {
    let mut lag = 0_u64;
    let mut bytes = 0_u64;
    let mut depth = 0_usize;
    let mut oldest = None;
    for entry in state.entries.iter().filter(|entry| entry.shard() == shard) {
        lag = lag.saturating_add(entry.command_count());
        bytes = bytes.saturating_add(entry.debt_bytes());
        depth = depth.saturating_add(1);
        oldest = Some(oldest.map_or(entry.enqueued_at(), |prior: Instant| {
            prior.min(entry.enqueued_at())
        }));
    }
    Debt {
        apply_lag_commands: lag,
        apply_debt_bytes: bytes,
        apply_queue_depth: depth,
        oldest_unapplied_age_ms: oldest
            .map(|at| {
                u64::try_from(now.saturating_duration_since(at).as_millis()).unwrap_or(u64::MAX)
            })
            .unwrap_or(0),
    }
}

fn snapshot_for(
    state: &CoordinatorState,
    shard: &QueueKey,
    now: Instant,
    paused: bool,
) -> AsyncProjectionApplySnapshot {
    let debt = debt_for(state, shard, now);
    let shard_state = state.shards.get(shard);
    AsyncProjectionApplySnapshot {
        apply_lag_commands: debt.apply_lag_commands,
        apply_debt_bytes: debt.apply_debt_bytes,
        apply_queue_depth: debt.apply_queue_depth,
        oldest_unapplied_age_ms: debt.oldest_unapplied_age_ms,
        apply_retry_count: shard_state.map_or(0, |state| state.retry_count),
        applied_high_water: shard_state.and_then(|state| state.applied_high_water.clone()),
        poison_reason: shard_state.and_then(|state| state.poison_reason.clone()),
        paused,
    }
}

fn backpressure(resource: &'static str) -> EngineError {
    EngineError::Backpressure { resource }
}

fn poisoned(reason: &str) -> EngineError {
    EngineError::Storage(format!("async projection poisoned: {reason}"))
}

#[derive(Default)]
struct CountingWriter {
    bytes: u64,
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buffer.len() as u64);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
