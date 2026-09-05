//! Provider-neutral bounded apply coordination for object-log projections.
//!
//! The serving projection remains on the response path. This coordinator owns only the selected
//! projection that may lag under `ResponseBarrier::AsyncProjection`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fireweed_core::ItemId;
use fireweed_engine::{
    AsyncProjectionSpec, AsyncProjectionStore, CLAIM_GENERATION_MAX_REQUESTS, CommandEnvelope,
    CommandPosition, EngineError, EngineResult, GENERATION_MAX_ITEMS,
    GENERATION_MAX_RESPONSE_BYTES, QueueCommand, QueueKey,
};
use tokio::sync::{Mutex, Notify};

use crate::PackedAppendError;

/// One admission reserved before the authoritative append begins.
#[derive(Debug)]
pub struct AsyncProjectionApplyReservation {
    id: u64,
    shard: QueueKey,
}

impl AsyncProjectionApplyReservation {
    /// Packer-visible identifier used to transfer co-sealed followers into the leader.
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn shard(&self) -> &QueueKey {
        &self.shard
    }
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
    injected_apply_failures: AtomicU32,
    #[cfg(test)]
    apply_live_calls: AtomicU32,
    #[cfg(test)]
    apply_live_command_counts: std::sync::Mutex<Vec<usize>>,
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

/// One writer-side apply generation: a strict-log-order prefix of same-shard Ready packs.
struct ApplyGeneration {
    shard: QueueKey,
    entry_ids: Vec<u64>,
    positions: Vec<CommandPosition>,
    commands: Vec<CommandEnvelope>,
}

#[derive(Default)]
struct ShardApplyState {
    retry_count: u32,
    applied_high_water: Option<CommandPosition>,
    poison_reason: Option<String>,
    produce_delay_spent: bool,
}

impl<P> AsyncProjectionApplyCoordinator<P>
where
    P: AsyncProjectionStore + 'static,
{
    pub fn new(projection: Arc<P>, spec: AsyncProjectionSpec) -> EngineResult<Self> {
        let apply_start_delay_ms = spec.apply_start_delay_ms;
        let mut spec = AsyncProjectionSpec::new(
            spec.apply_lag_max_commands,
            spec.apply_debt_max_bytes,
            spec.apply_queue_depth_max,
            spec.oldest_unapplied_max_ms,
            spec.apply_poison_retry_threshold,
        )?;
        spec.apply_start_delay_ms = apply_start_delay_ms;
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
                injected_apply_failures: AtomicU32::new(0),
                #[cfg(test)]
                apply_live_calls: AtomicU32::new(0),
                #[cfg(test)]
                apply_live_command_counts: std::sync::Mutex::new(Vec::new()),
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
        let debt_bytes = crate::log_engine_store::exact_envelope_bytes(commands)?;
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

    /// Merge co-sealed follower reservations into the leader and charge exact packed envelope bytes.
    ///
    /// Followers must not cancel after this transfer: the leader publishes the combined debt.
    pub async fn transfer_followers_and_recharge(
        &self,
        leader: &AsyncProjectionApplyReservation,
        follower_ids: &[u64],
        packed_commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        let command_count = u64::try_from(packed_commands.len())
            .map_err(|_| EngineError::Storage("async projection command count overflow".into()))?;
        let debt_bytes = crate::log_engine_store::exact_envelope_bytes(packed_commands)?;
        let mut state = self.inner.state.lock().await;
        if follower_ids.iter().any(|id| *id == leader.id) {
            drop(state);
            return self
                .poison(
                    leader.shard.clone(),
                    "async projection leader reservation was transferred as a follower".into(),
                )
                .await;
        }
        state
            .entries
            .retain(|entry| entry.shard() != &leader.shard || !follower_ids.contains(&entry.id()));
        let Some(entry) = state
            .entries
            .iter_mut()
            .find(|entry| entry.id() == leader.id && entry.shard() == &leader.shard)
        else {
            drop(state);
            return self
                .poison(
                    leader.shard.clone(),
                    "async projection leader reservation disappeared before transfer".into(),
                )
                .await;
        };
        match entry {
            ApplyEntry::Reserved {
                command_count: reserved_count,
                debt_bytes: reserved_debt,
                ..
            } => {
                *reserved_count = command_count;
                *reserved_debt = debt_bytes;
            }
            ApplyEntry::Ready(_) => {
                drop(state);
                return self
                    .poison(
                        leader.shard.clone(),
                        "async projection leader reservation was published before transfer".into(),
                    )
                    .await;
            }
        }
        drop(state);
        self.inner.changed.notify_waiters();
        Ok(())
    }

    /// True while the reservation is still Reserved or Ready (not cancelled).
    pub async fn reservation_outstanding(
        &self,
        reservation: &AsyncProjectionApplyReservation,
    ) -> bool {
        let state = self.inner.state.lock().await;
        state
            .entries
            .iter()
            .any(|entry| entry.id() == reservation.id && entry.shard() == &reservation.shard)
    }

    /// Latch shard poison for post-position append ambiguity. Does not cancel reservations.
    pub async fn latch_poison(&self, shard: QueueKey, reason: String) {
        let _ = self.poison(shard, reason).await;
    }

    /// Cancel only a BeforePosition reservation. Post-position ambiguity latches
    /// poison and leaves the reservation outstanding.
    pub async fn dispose_packed_append_error(
        coordinator: Option<&Self>,
        reservation: Option<AsyncProjectionApplyReservation>,
        error: PackedAppendError,
    ) -> EngineError {
        match error {
            PackedAppendError::BeforePosition(inner) => {
                if let (Some(coordinator), Some(reservation)) = (coordinator, reservation) {
                    coordinator.cancel(reservation).await;
                }
                inner
            }
            PackedAppendError::PostPositionAmbiguous { shard, reason } => {
                if let Some(coordinator) = coordinator {
                    coordinator
                        .latch_poison(shard.clone(), reason.clone())
                        .await;
                }
                PackedAppendError::PostPositionAmbiguous { shard, reason }.into_engine()
            }
        }
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

    pub async fn has_ready(&self, shard: &QueueKey) -> bool {
        let state = self.inner.state.lock().await;
        state
            .entries
            .iter()
            .any(|entry| matches!(entry, ApplyEntry::Ready(batch) if batch.shard == *shard))
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

    /// Wait until coordinator-authoritative `applied_high_water` covers `target`.
    ///
    /// Empty apply queues and not-yet-ready reservations are not coverage. Expiry is retryable
    /// `Backpressure { resource: "projection coverage" }`.
    pub async fn wait_until_covers(
        &self,
        shard: &QueueKey,
        target: &CommandPosition,
        deadline: Duration,
    ) -> EngineResult<()> {
        let started = Instant::now();
        loop {
            self.ensure_healthy(shard)?;
            let changed = self.inner.changed.notified();
            let snapshot = self.snapshot(shard).await;
            if let Some(reason) = snapshot.poison_reason {
                return Err(poisoned(&reason));
            }
            if position_covers(snapshot.applied_high_water.as_ref(), target) {
                return Ok(());
            }
            let remaining = deadline.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(backpressure("projection coverage"));
            }
            match tokio::time::timeout(remaining, changed).await {
                Ok(()) => {}
                Err(_) => {
                    self.ensure_healthy(shard)?;
                    let snapshot = self.snapshot(shard).await;
                    if let Some(reason) = snapshot.poison_reason {
                        return Err(poisoned(&reason));
                    }
                    if position_covers(snapshot.applied_high_water.as_ref(), target) {
                        return Ok(());
                    }
                    return Err(backpressure("projection coverage"));
                }
            }
        }
    }

    /// Seed applied high-water after authoritative tail equality.
    pub async fn seed_high_water(&self, shard: QueueKey, high_water: Option<CommandPosition>) {
        let mut state = self.inner.state.lock().await;
        state.shards.entry(shard).or_default().applied_high_water = high_water;
        drop(state);
        self.inner.changed.notify_waiters();
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
                produce_delay_spent: false,
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

    #[cfg(test)]
    pub(crate) fn apply_live_call_count(&self) -> u32 {
        self.inner.apply_live_calls.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn apply_live_command_counts(&self) -> Vec<usize> {
        self.inner
            .apply_live_command_counts
            .lock()
            .expect("apply command-count mutex")
            .clone()
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
            next_coalesced_generation(&state)
        };
        let Some(mut generation) = next else {
            inner.worker_running.store(false, Ordering::Release);
            let has_work = {
                let state = inner.state.lock().await;
                next_coalesced_generation(&state).is_some()
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

        if generation_is_claim_without_complete(&generation) {
            let deadline = Instant::now() + Duration::from_millis(CLAIM_COMPLETE_JOIN_MS);
            while generation_is_claim_without_complete(&generation) {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                if remaining.is_zero() {
                    break;
                }
                tokio::select! {
                    _ = inner.changed.notified() => {}
                    _ = tokio::time::sleep(remaining) => {}
                }
                let state = inner.state.lock().await;
                let Some(again) = next_coalesced_generation(&state) else {
                    continue;
                };
                if again
                    .entry_ids
                    .iter()
                    .any(|id| generation.entry_ids.contains(id))
                {
                    generation = again;
                }
            }
        }

        #[cfg(test)]
        {
            inner.apply_live_calls.fetch_add(1, Ordering::AcqRel);
            inner
                .apply_live_command_counts
                .lock()
                .expect("apply command-count mutex")
                .push(generation.commands.len());
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

        if inner.spec.apply_start_delay_ms > 0 && batch_is_produce(&generation.commands) {
            let needs_delay = {
                let state = inner.state.lock().await;
                !state
                    .shards
                    .get(&generation.shard)
                    .is_some_and(|shard| shard.produce_delay_spent)
            };
            if needs_delay {
                tokio::time::sleep(std::time::Duration::from_millis(
                    inner.spec.apply_start_delay_ms,
                ))
                .await;
                let mut state = inner.state.lock().await;
                state
                    .shards
                    .entry(generation.shard.clone())
                    .or_default()
                    .produce_delay_spent = true;
            }
        }

        let result = if injected_failure {
            Err(EngineError::Storage(
                "injected async projection apply failure".into(),
            ))
        } else {
            AsyncProjectionStore::apply_live(
                inner.projection.as_ref(),
                generation.positions.clone(),
                generation.commands.clone(),
            )
            .await
        };

        let mut state = inner.state.lock().await;
        match result {
            Ok(()) => {
                let all_present = generation.entry_ids.iter().all(|id| {
                    state
                        .entries
                        .iter()
                        .any(|entry| entry.id() == *id && entry.shard() == &generation.shard)
                });
                if all_present {
                    state.entries.retain(|entry| {
                        entry.shard() != &generation.shard
                            || !generation.entry_ids.contains(&entry.id())
                    });
                    let shard_state = state.shards.entry(generation.shard.clone()).or_default();
                    shard_state.retry_count = 0;
                    shard_state.applied_high_water = generation.positions.last().cloned();
                } else {
                    let shard_state = state.shards.entry(generation.shard.clone()).or_default();
                    let reason: String =
                        "async projection apply queue changed while a batch was in flight".into();
                    shard_state.poison_reason = Some(reason.clone());
                    if let Ok(mut poisoned) = inner.poisoned.write() {
                        poisoned.insert(generation.shard.clone(), reason);
                    }
                }
            }
            Err(error) => {
                let shard_state = state.shards.entry(generation.shard.clone()).or_default();
                shard_state.retry_count = shard_state.retry_count.saturating_add(1);
                if shard_state.retry_count >= inner.spec.apply_poison_retry_threshold {
                    let reason = format!(
                        "async projection apply failed {} times: {error}",
                        shard_state.retry_count
                    );
                    shard_state.poison_reason = Some(reason.clone());
                    if let Ok(mut poisoned) = inner.poisoned.write() {
                        poisoned.insert(generation.shard.clone(), reason);
                    }
                }
            }
        }
        drop(state);
        inner.changed.notify_waiters();
        tokio::task::yield_now().await;
    }
}

fn batch_is_produce(commands: &[CommandEnvelope]) -> bool {
    commands.iter().any(|envelope| {
        matches!(
            envelope.command,
            QueueCommand::Push(_)
                | QueueCommand::UpdateFields(_)
                | QueueCommand::UpdateFieldsBatch(_)
        )
    })
}

/// Wait for Complete envelopes of the same ids before applying a Claim-only
/// generation. Completes are packed after Claim responses return (~30ms);
/// applying Claim first writes Leased rows that Complete immediately discards.
const CLAIM_COMPLETE_JOIN_MS: u64 = 80;

fn generation_is_claim_without_complete(generation: &ApplyGeneration) -> bool {
    let mut claim = false;
    let mut complete = false;
    for envelope in &generation.commands {
        match &envelope.command {
            QueueCommand::Claim(_) => claim = true,
            QueueCommand::Finalize(finalize)
                if finalize.outcomes.iter().all(|outcome| {
                    matches!(outcome.kind, fireweed_engine::FinalizeKind::Complete)
                }) =>
            {
                complete = true;
            }
            _ => {}
        }
    }
    claim && !complete
}

fn next_coalesced_generation(state: &CoordinatorState) -> Option<ApplyGeneration> {
    let (_, first) = next_runnable(state)?;
    let Some(mut last) = first.positions.last().cloned() else {
        return None;
    };
    let mut envelopes = generation_envelope_count(&first);
    let mut seen_items = HashSet::new();
    insert_batch_item_ids(&mut seen_items, &first);
    let mut debt = first.debt_bytes;
    let mut generation = ApplyGeneration {
        shard: first.shard.clone(),
        entry_ids: vec![first.id],
        positions: first.positions.clone(),
        commands: first.commands.clone(),
    };

    let mut candidates: Vec<&ApplyBatch> = state
        .entries
        .iter()
        .filter_map(|entry| match entry {
            ApplyEntry::Ready(batch) if batch.shard == first.shard && batch.id != first.id => {
                Some(batch)
            }
            _ => None,
        })
        .collect();
    candidates.sort_by_key(|batch| {
        batch
            .positions
            .first()
            .map(|position| (position.backend_epoch, position.sequence))
            .unwrap_or((u64::MAX, u64::MAX))
    });

    for batch in candidates {
        let Some(next_first) = batch.positions.first() else {
            continue;
        };
        if !ready_contiguous_follow(&last, next_first) {
            break;
        }
        if reserved_between(state, &first.shard, first.id, batch.id) {
            break;
        }
        let add_envelopes = generation_envelope_count(batch);
        let add_unique_items = unique_item_growth(&seen_items, batch);
        let compose_same_items =
            batch_has_identities(batch) && add_unique_items == 0 && !seen_items.is_empty();
        let envelope_cap = if compose_same_items {
            CLAIM_GENERATION_MAX_REQUESTS.saturating_mul(2)
        } else {
            CLAIM_GENERATION_MAX_REQUESTS
        };
        if envelopes.saturating_add(add_envelopes) > envelope_cap
            || seen_items.len().saturating_add(add_unique_items) > GENERATION_MAX_ITEMS
            || debt.saturating_add(batch.debt_bytes) > GENERATION_MAX_RESPONSE_BYTES as u64
        {
            break;
        }
        envelopes = envelopes.saturating_add(add_envelopes);
        insert_batch_item_ids(&mut seen_items, batch);
        debt = debt.saturating_add(batch.debt_bytes);
        generation.entry_ids.push(batch.id);
        generation.positions.extend(batch.positions.iter().cloned());
        generation.commands.extend(batch.commands.iter().cloned());
        if let Some(next_last) = batch.positions.last() {
            last = next_last.clone();
        }
    }
    Some(generation)
}

fn generation_envelope_count(batch: &ApplyBatch) -> usize {
    if batch.commands.is_empty() {
        usize::try_from(batch.command_count).unwrap_or(usize::MAX)
    } else {
        batch.commands.len()
    }
}

fn batch_item_ids(batch: &ApplyBatch) -> impl Iterator<Item = ItemId> + '_ {
    batch.commands.iter().flat_map(|envelope| {
        envelope
            .item_ids
            .iter()
            .copied()
            .filter(|id| id.as_u64() != 0)
    })
}

fn batch_has_identities(batch: &ApplyBatch) -> bool {
    batch_item_ids(batch).next().is_some()
}

fn unique_item_growth(seen: &HashSet<ItemId>, batch: &ApplyBatch) -> usize {
    batch_item_ids(batch)
        .filter(|id| !seen.contains(id))
        .collect::<HashSet<_>>()
        .len()
}

fn insert_batch_item_ids(seen: &mut HashSet<ItemId>, batch: &ApplyBatch) {
    seen.extend(batch_item_ids(batch));
}

fn reserved_between(state: &CoordinatorState, shard: &QueueKey, left: u64, right: u64) -> bool {
    let (lo, hi) = if left < right {
        (left, right)
    } else {
        (right, left)
    };
    state.entries.iter().any(|entry| {
        matches!(entry, ApplyEntry::Reserved { .. })
            && entry.shard() == shard
            && entry.id() > lo
            && entry.id() < hi
    })
}

fn ready_contiguous_follow(prev_last: &CommandPosition, next_first: &CommandPosition) -> bool {
    prev_last.queue == next_first.queue
        && prev_last.precedes(next_first)
        && (prev_last.backend_epoch != next_first.backend_epoch
            || prev_last.sequence.checked_add(1) == Some(next_first.sequence))
}

fn next_runnable(state: &CoordinatorState) -> Option<(usize, ApplyBatch)> {
    let mut best: Option<(usize, ApplyBatch)> = None;
    for (index, entry) in state.entries.iter().enumerate() {
        let shard = entry.shard();
        if state
            .shards
            .get(shard)
            .is_some_and(|shard_state| shard_state.poison_reason.is_some())
        {
            continue;
        }
        let ApplyEntry::Ready(batch) = entry else {
            continue;
        };
        if !ready_follows_high_water(state, batch) {
            continue;
        }
        let first = batch
            .positions
            .first()
            .map(|p| (p.backend_epoch, p.sequence));
        let better = match &best {
            None => true,
            Some((_, other)) => {
                other.shard != batch.shard
                    || first
                        < other
                            .positions
                            .first()
                            .map(|p| (p.backend_epoch, p.sequence))
            }
        };
        if better {
            best = Some((index, batch.clone()));
        }
    }
    best
}

fn ready_follows_high_water(state: &CoordinatorState, batch: &ApplyBatch) -> bool {
    let Some(first) = batch.positions.first() else {
        return false;
    };
    match state
        .shards
        .get(&batch.shard)
        .and_then(|shard| shard.applied_high_water.as_ref())
    {
        None => !state.entries.iter().any(|entry| {
            let ApplyEntry::Ready(other) = entry else {
                return false;
            };
            other.shard == batch.shard
                && other.id != batch.id
                && other.positions.first().is_some_and(|position| {
                    (position.backend_epoch, position.sequence)
                        < (first.backend_epoch, first.sequence)
                })
        }),
        Some(high_water) => {
            high_water.precedes(first)
                && (high_water.backend_epoch != first.backend_epoch
                    || high_water.sequence.checked_add(1) == Some(first.sequence))
        }
    }
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

fn position_covers(have: Option<&CommandPosition>, target: &CommandPosition) -> bool {
    have.is_some_and(|have| {
        have.backend_epoch > target.backend_epoch
            || (have.backend_epoch == target.backend_epoch && have.sequence >= target.sequence)
    })
}

fn poisoned(reason: &str) -> EngineError {
    EngineError::Storage(format!("async projection poisoned: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::{
        ClientItemKey, EligibilityPolicy, ItemId, OrderingMode, PriorityModel, QueueDefinition,
        QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
    };
    use fireweed_engine::{CommandChecksum, CommandId, ProjectionStore, PushCommand, PushItem};

    fn shard() -> QueueKey {
        QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap())
    }

    fn shard_named(queue: &str) -> QueueKey {
        QueueKey::new(TenantId::new("t").unwrap(), QueueId::new(queue).unwrap())
    }

    fn pos(sequence: u64) -> CommandPosition {
        CommandPosition::new(shard(), 0, sequence)
    }

    fn pos_on(queue: QueueKey, sequence: u64) -> CommandPosition {
        CommandPosition::new(queue, 0, sequence)
    }

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
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

    fn pause_env(id: &str) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new(id),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: Vec::new(),
            command: QueueCommand::PauseQueue(Default::default()),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        }
    }

    fn push_env(id: &str, item: u32) -> CommandEnvelope {
        let item_id = ItemId::mint(1, 0, item);
        CommandEnvelope {
            command_id: CommandId::new(id),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    client_item_key: ClientItemKey::new(id).unwrap(),
                    item_id,
                    priority: None,
                    not_before: None,
                    group_key: None,
                    max_attempts: 3,
                    payload: None,
                    fields: Default::default(),
                    metadata: Default::default(),
                    cohort_size: None,
                    gate_keys: Vec::new(),
                    index_fields: Default::default(),
                    entity_document: None,
                }],
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        }
    }

    fn items_env(id: &str, items: usize) -> CommandEnvelope {
        items_env_from(id, 0, items)
    }

    fn items_env_from(id: &str, start: usize, items: usize) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new(id),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: (start..start + items)
                .map(|index| ItemId::mint(1, 0, u32::try_from(index).expect("item index")))
                .collect(),
            command: QueueCommand::PauseQueue(Default::default()),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        }
    }

    fn ready_item_range(id: u64, sequence: u64, start: usize, items: usize) -> ApplyEntry {
        ApplyEntry::Ready(ApplyBatch {
            id,
            shard: shard(),
            positions: vec![pos(sequence)],
            commands: vec![items_env_from(&id.to_string(), start, items)],
            command_count: 1,
            debt_bytes: 0,
            enqueued_at: Instant::now(),
        })
    }

    fn ready(id: u64, sequence: u64) -> ApplyEntry {
        ready_sized(id, sequence, 1, 0, 0)
    }

    fn ready_sized(
        id: u64,
        sequence: u64,
        envelopes: u64,
        items: usize,
        debt_bytes: u64,
    ) -> ApplyEntry {
        ready_on(shard(), id, sequence, envelopes, items, debt_bytes)
    }

    fn ready_on(
        queue: QueueKey,
        id: u64,
        sequence: u64,
        envelopes: u64,
        items: usize,
        debt_bytes: u64,
    ) -> ApplyEntry {
        let commands = if items == 0 {
            Vec::new()
        } else {
            vec![items_env(&id.to_string(), items)]
        };
        ApplyEntry::Ready(ApplyBatch {
            id,
            shard: queue.clone(),
            positions: vec![pos_on(queue, sequence)],
            commands,
            command_count: envelopes,
            debt_bytes,
            enqueued_at: Instant::now(),
        })
    }

    fn reserved(id: u64) -> ApplyEntry {
        ApplyEntry::Reserved {
            id,
            shard: shard(),
            command_count: 1,
            debt_bytes: 0,
            enqueued_at: Instant::now(),
        }
    }

    fn generation_ids(state: &CoordinatorState) -> Vec<u64> {
        next_coalesced_generation(state)
            .map(|generation| generation.entry_ids)
            .unwrap_or_default()
    }

    #[test]
    fn holds_non_contiguous_ready_until_the_hole_is_filled() {
        let mut state = CoordinatorState::default();
        state.shards.entry(shard()).or_default().applied_high_water = Some(pos(300));
        state.entries.push_back(ready(1, 302));
        assert!(next_runnable(&state).is_none());
        state.entries.push_front(ready(2, 301));
        let (_index, batch) = next_runnable(&state).expect("301 is runnable");
        assert_eq!(batch.positions[0].sequence, 301);
    }

    #[test]
    fn later_contiguous_ready_is_not_hidden_by_an_earlier_gap() {
        let mut state = CoordinatorState::default();
        state.shards.entry(shard()).or_default().applied_high_water = Some(pos(300));
        state.entries.push_back(ready(1, 302));
        state.entries.push_back(ready(2, 301));
        let (_index, batch) = next_runnable(&state).expect("301 is runnable behind 302");
        assert_eq!(batch.positions[0].sequence, 301);
    }

    #[test]
    fn reserved_does_not_hide_a_contiguous_ready_batch() {
        let mut state = CoordinatorState::default();
        state.shards.entry(shard()).or_default().applied_high_water = Some(pos(300));
        state.entries.push_back(ApplyEntry::Reserved {
            id: 1,
            shard: shard(),
            command_count: 1,
            debt_bytes: 0,
            enqueued_at: Instant::now(),
        });
        state.entries.push_back(ready(2, 301));
        let (_index, batch) = next_runnable(&state).expect("ready 301 despite reserved");
        assert_eq!(batch.positions[0].sequence, 301);
    }

    #[test]
    fn coalesces_contiguous_underfilled_ready_entries() {
        let mut state = CoordinatorState::default();
        state.entries.push_back(ready(1, 1));
        state.entries.push_back(ready(2, 2));
        state.entries.push_back(ready(3, 3));
        assert_eq!(generation_ids(&state), vec![1, 2, 3]);
    }

    #[test]
    fn does_not_coalesce_across_sequence_gap() {
        let mut state = CoordinatorState::default();
        state.entries.push_back(ready(1, 1));
        state.entries.push_back(ready(2, 3));
        assert_eq!(generation_ids(&state), vec![1]);
    }

    #[test]
    fn does_not_coalesce_across_outstanding_earlier_reservation() {
        let mut state = CoordinatorState::default();
        state.entries.push_back(ready(1, 1));
        state.entries.push_back(reserved(2));
        state.entries.push_back(ready(3, 2));
        assert_eq!(generation_ids(&state), vec![1]);
    }

    #[test]
    fn does_not_coalesce_across_shard() {
        let mut state = CoordinatorState::default();
        let other = shard_named("other");
        state.shards.entry(other.clone()).or_default().poison_reason = Some("other".into());
        state.entries.push_back(ready(1, 1));
        state.entries.push_back(ready_on(other, 2, 1, 1, 0, 0));
        state.entries.push_back(ready(3, 2));
        assert_eq!(generation_ids(&state), vec![1, 3]);
    }

    #[test]
    fn does_not_coalesce_poisoned_shard() {
        let mut state = CoordinatorState::default();
        state.shards.entry(shard()).or_default().poison_reason = Some("poison".into());
        state.entries.push_back(ready(1, 1));
        state.entries.push_back(ready(2, 2));
        assert!(next_coalesced_generation(&state).is_none());
    }

    #[test]
    fn stops_at_eight_envelope_bound() {
        let mut state = CoordinatorState::default();
        state.entries.push_back(ready_sized(
            1,
            1,
            CLAIM_GENERATION_MAX_REQUESTS as u64,
            0,
            0,
        ));
        state.entries.push_back(ready(2, 2));
        assert_eq!(generation_ids(&state), vec![1]);
    }

    #[test]
    fn joins_underfilled_packs_up_to_eight_envelopes() {
        let mut state = CoordinatorState::default();
        state.entries.push_back(ready_sized(1, 1, 5, 0, 0));
        state.entries.push_back(ready_sized(2, 2, 3, 0, 0));
        assert_eq!(generation_ids(&state), vec![1, 2]);
    }

    #[test]
    fn does_not_join_nine_and_seven_empty_envelopes() {
        let mut state = CoordinatorState::default();
        state.entries.push_back(ready_sized(1, 1, 9, 0, 0));
        state.entries.push_back(ready_sized(2, 2, 7, 0, 0));
        assert_eq!(generation_ids(&state), vec![1]);
    }

    #[test]
    fn does_not_join_two_sequencer_generations_of_empty_envelopes() {
        let mut state = CoordinatorState::default();
        state.entries.push_back(ready_sized(
            1,
            1,
            CLAIM_GENERATION_MAX_REQUESTS as u64,
            0,
            0,
        ));
        state.entries.push_back(ready_sized(
            2,
            2,
            CLAIM_GENERATION_MAX_REQUESTS as u64,
            0,
            0,
        ));
        assert_eq!(generation_ids(&state), vec![1]);
        state.entries.push_back(ready(3, 3));
        assert_eq!(generation_ids(&state), vec![1]);
    }

    #[test]
    fn stops_at_eight_hundred_item_bound() {
        let mut state = CoordinatorState::default();
        state
            .entries
            .push_back(ready_sized(1, 1, 1, GENERATION_MAX_ITEMS - 1, 0));
        state
            .entries
            .push_back(ready_item_range(2, 2, GENERATION_MAX_ITEMS - 1, 2));
        assert_eq!(generation_ids(&state), vec![1]);
    }

    #[test]
    fn coalesces_underfilled_claim_and_complete_of_the_same_items() {
        let mut state = CoordinatorState::default();
        state
            .entries
            .push_back(ready_sized(1, 1, 1, GENERATION_MAX_ITEMS, 0));
        state
            .entries
            .push_back(ready_sized(2, 2, 1, GENERATION_MAX_ITEMS, 0));
        assert_eq!(generation_ids(&state), vec![1, 2]);
    }

    #[test]
    fn does_not_coalesce_when_follower_introduces_an_801st_distinct_item() {
        let mut state = CoordinatorState::default();
        state
            .entries
            .push_back(ready_sized(1, 1, 1, GENERATION_MAX_ITEMS, 0));
        state
            .entries
            .push_back(ready_item_range(2, 2, GENERATION_MAX_ITEMS, 1));
        assert_eq!(generation_ids(&state), vec![1]);
    }

    #[test]
    fn stops_at_four_mib_debt_bound() {
        let mut state = CoordinatorState::default();
        let almost_full = GENERATION_MAX_RESPONSE_BYTES as u64 - 1;
        state
            .entries
            .push_back(ready_sized(1, 1, 1, 0, almost_full));
        state.entries.push_back(ready_sized(2, 2, 1, 0, 2));
        assert_eq!(generation_ids(&state), vec![1]);
    }

    async fn enqueue_pause(
        coordinator: &AsyncProjectionApplyCoordinator<fireweed_projection::AsyncInMemoryProjection>,
        sequence: u64,
        command_id: &str,
    ) {
        let commands = vec![pause_env(command_id)];
        let reservation = coordinator
            .reserve(shard(), &commands)
            .await
            .expect("reserve");
        coordinator
            .enqueue_reserved(reservation, vec![pos(sequence)], commands)
            .await
            .expect("enqueue");
    }

    #[tokio::test]
    async fn coordinator_coalesces_contiguous_underfilled_ready_entries() {
        let coordinator = coordinator();
        coordinator.pause();
        enqueue_pause(&coordinator, 1, "p1").await;
        enqueue_pause(&coordinator, 2, "p2").await;
        enqueue_pause(&coordinator, 3, "p3").await;
        assert_eq!(coordinator.snapshot(&shard()).await.apply_queue_depth, 3);
        coordinator.resume();
        coordinator
            .wait_for_catch_up(&shard())
            .await
            .expect("catch up");
        assert_eq!(
            coordinator.apply_live_call_count(),
            1,
            "contiguous underfilled Ready entries must drain as one apply_live"
        );
        assert_eq!(coordinator.apply_live_command_counts(), vec![3]);
        assert_eq!(coordinator.snapshot(&shard()).await.apply_queue_depth, 0);
        assert_eq!(
            coordinator
                .snapshot(&shard())
                .await
                .applied_high_water
                .map(|position| position.sequence),
            Some(3)
        );
        coordinator
            .wait_until_covers(&shard(), &pos(3), Duration::from_millis(40))
            .await
            .expect("combined high-water covers the prefix");
    }

    #[tokio::test]
    async fn injected_failure_retries_the_same_combined_prefix() {
        let coordinator = coordinator();
        coordinator.pause();
        enqueue_pause(&coordinator, 1, "p1").await;
        enqueue_pause(&coordinator, 2, "p2").await;
        coordinator.inject_apply_failures(3);
        coordinator.resume();
        let error = coordinator
            .wait_for_catch_up(&shard())
            .await
            .expect_err("poison after retry threshold");
        assert!(
            format!("{error}").contains("async projection poisoned"),
            "{error}"
        );
        let snapshot = coordinator.snapshot(&shard()).await;
        assert_eq!(
            snapshot.apply_queue_depth, 2,
            "failure must keep both entries"
        );
        assert!(snapshot.applied_high_water.is_none());
        assert_eq!(coordinator.apply_live_command_counts(), vec![2, 2, 2]);
    }

    #[tokio::test]
    async fn first_produce_delay_is_not_multiplied_across_coalesced_packs() {
        let coordinator = AsyncProjectionApplyCoordinator::new(
            seeded_projection(),
            fireweed_engine::AsyncProjectionSpec {
                apply_start_delay_ms: 80,
                ..AsyncProjectionSpec::new(32, 4096, 16, 30_000, 3).unwrap()
            },
        )
        .expect("coordinator");
        coordinator.pause();
        let first = vec![push_env("p1", 1)];
        let first_res = coordinator.reserve(shard(), &first).await.expect("reserve");
        coordinator
            .enqueue_reserved(first_res, vec![pos(1)], first)
            .await
            .expect("enqueue");
        let second = vec![push_env("p2", 2)];
        let second_res = coordinator
            .reserve(shard(), &second)
            .await
            .expect("reserve");
        coordinator
            .enqueue_reserved(second_res, vec![pos(2)], second)
            .await
            .expect("enqueue");
        let started = Instant::now();
        coordinator.resume();
        coordinator
            .wait_for_catch_up(&shard())
            .await
            .expect("catch up");
        let elapsed = started.elapsed();
        assert_eq!(coordinator.apply_live_call_count(), 1);
        assert!(
            elapsed >= Duration::from_millis(80) && elapsed < Duration::from_millis(140),
            "one-shot produce delay must run once for the coalesced prefix, elapsed {elapsed:?}"
        );
    }

    fn seeded_projection() -> Arc<fireweed_projection::AsyncInMemoryProjection> {
        let mut inner = fireweed_projection::InMemoryProjection::new();
        ProjectionStore::ensure_shard(&mut inner, &qdef()).expect("ensure shard");
        Arc::new(fireweed_projection::AsyncInMemoryProjection::new(inner))
    }

    fn coordinator() -> AsyncProjectionApplyCoordinator<fireweed_projection::AsyncInMemoryProjection>
    {
        AsyncProjectionApplyCoordinator::new(
            seeded_projection(),
            AsyncProjectionSpec::new(32, 4096, 16, 30_000, 3).unwrap(),
        )
        .expect("coordinator")
    }

    #[tokio::test]
    async fn empty_queue_is_not_applied_high_water_coverage() {
        let coordinator = coordinator();
        assert!(coordinator.snapshot(&shard()).await.apply_queue_depth == 0);
        assert!(!coordinator.has_ready(&shard()).await);
        let error = coordinator
            .wait_until_covers(&shard(), &pos(1), Duration::from_millis(40))
            .await
            .expect_err("empty/not-ready is not coverage");
        assert_eq!(
            error,
            EngineError::Backpressure {
                resource: "projection coverage",
            }
        );
    }

    #[tokio::test]
    async fn seeded_high_water_covers_without_ready_entries() {
        let coordinator = coordinator();
        coordinator.seed_high_water(shard(), Some(pos(1))).await;
        coordinator
            .wait_until_covers(&shard(), &pos(1), Duration::from_millis(40))
            .await
            .expect("seeded high-water is coverage");
    }
}
