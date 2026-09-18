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

/// Count-relevant facts only; payloads and metadata remain in the durable commands.
#[derive(Debug, Clone)]
pub enum RetainedLifecycleChange {
    Claim(Vec<ItemId>),
    Replace(Vec<(ItemId, fireweed_core::ItemState, u64)>),
}

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

/// Compact, non-authoritative row identities borrowed from retained durable commands.
#[derive(Debug, Clone)]
pub enum RetainedMembershipChange {
    Push(Vec<(ItemId, fireweed_core::ClientItemKey)>),
    Purge(Vec<ItemId>),
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
    coverage_waiters: std::sync::Mutex<HashMap<QueueKey, u64>>,
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

// Selection can defer a claim repeatedly while notifications arrive. Borrow the
// retained retry batches until a generation is actually chosen for application.
struct ApplyGenerationPlan<'a> {
    batches: Vec<&'a ApplyBatch>,
}

impl ApplyGenerationPlan<'_> {
    fn commands(&self) -> impl Iterator<Item = &CommandEnvelope> {
        self.batches.iter().flat_map(|batch| batch.commands.iter())
    }

    fn materialize(self) -> ApplyGeneration {
        ApplyGeneration {
            shard: self.batches[0].shard.clone(),
            entry_ids: self.batches.iter().map(|batch| batch.id).collect(),
            positions: self
                .batches
                .iter()
                .flat_map(|batch| batch.positions.iter().cloned())
                .collect(),
            commands: self.commands().cloned().collect(),
        }
    }
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
                coverage_waiters: std::sync::Mutex::new(HashMap::new()),
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
        if follower_ids.contains(&leader.id) {
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

    pub fn is_paused(&self) -> bool {
        self.inner.paused.load(Ordering::Acquire)
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

    /// Reuse acknowledged commands still owned by the bounded apply queue.
    /// Only a complete tail of at most sixteen disjoint authoritative claims is
    /// returned. Missing/applied entries, other commands and epoch changes fall
    /// back to the caller's authoritative-log read or coverage barrier.
    pub async fn retained_claim_tail(
        &self,
        applied: &CommandPosition,
        target: &CommandPosition,
    ) -> EngineResult<Option<Vec<fireweed_engine::ClaimCommand>>> {
        self.ensure_healthy(&target.queue)?;
        if applied.queue != target.queue
            || applied.backend_epoch != target.backend_epoch
            || !matches!(target.sequence.checked_sub(applied.sequence), Some(1..=16))
        {
            return Ok(None);
        }
        let state = self.inner.state.lock().await;
        let mut entries = Vec::new();
        for entry in &state.entries {
            let ApplyEntry::Ready(batch) = entry else {
                continue;
            };
            if batch.shard != target.queue {
                continue;
            }
            for (position, envelope) in batch.positions.iter().zip(&batch.commands) {
                if position.backend_epoch != target.backend_epoch
                    || position.sequence <= applied.sequence
                    || position.sequence > target.sequence
                {
                    continue;
                }
                if !matches!(&envelope.command, QueueCommand::Claim(claim) if claim.authority_first)
                    || entries.len() == 16
                {
                    return Ok(None);
                }
                entries.push((position.clone(), envelope.clone()));
            }
        }
        entries.sort_by_key(|(position, _)| position.sequence);
        Ok(claim_only_tail(applied, target, entries))
    }

    /// Copy only row identities from a complete retained Push/Purge tail.
    /// No payloads or authoritative side counters are retained by this read.
    pub async fn retained_membership_tail(
        &self,
        applied: Option<&CommandPosition>,
        target: &CommandPosition,
    ) -> EngineResult<Option<Vec<(CommandPosition, RetainedMembershipChange)>>> {
        self.ensure_healthy(&target.queue)?;
        let start = match applied {
            Some(applied)
                if applied.queue == target.queue
                    && applied.backend_epoch == target.backend_epoch =>
            {
                let Some(start) = applied.sequence.checked_add(1) else {
                    return Ok(None);
                };
                start
            }
            None if target.backend_epoch == 0 => 0,
            _ => return Ok(None),
        };
        let Some(count @ 1..=16) = target
            .sequence
            .checked_sub(start)
            .and_then(|n| n.checked_add(1))
        else {
            return Ok(None);
        };
        let state = self.inner.state.lock().await;
        let mut entries = Vec::new();
        let mut remaining = 8192_usize;
        for entry in &state.entries {
            let ApplyEntry::Ready(batch) = entry else {
                continue;
            };
            if batch.shard != target.queue {
                continue;
            }
            if batch.positions.len() != batch.commands.len() {
                return Ok(None);
            }
            for (position, envelope) in batch.positions.iter().zip(&batch.commands) {
                if position.backend_epoch != target.backend_epoch
                    || position.sequence < start
                    || position.sequence > target.sequence
                {
                    continue;
                }
                if position.queue != target.queue || entries.len() == 16 {
                    return Ok(None);
                }
                let change = match &envelope.command {
                    QueueCommand::Push(command) => {
                        let Some(left) = remaining.checked_sub(command.items.len()) else {
                            return Ok(None);
                        };
                        remaining = left;
                        RetainedMembershipChange::Push(
                            command
                                .items
                                .iter()
                                .map(|item| (item.item_id, item.client_item_key.clone()))
                                .collect(),
                        )
                    }
                    QueueCommand::PurgeItems(command) => {
                        let Some(left) = remaining.checked_sub(command.item_ids.len()) else {
                            return Ok(None);
                        };
                        remaining = left;
                        RetainedMembershipChange::Purge(command.item_ids.clone())
                    }
                    _ => return Ok(None),
                };
                entries.push((position.clone(), change));
            }
        }
        entries.sort_by_key(|(position, _)| position.sequence);
        if entries.len() as u64 != count
            || entries
                .iter()
                .enumerate()
                .any(|(i, (position, _))| position.sequence != start + i as u64)
        {
            return Ok(None);
        }
        // Restrict this shortcut to a single operation family. Mixed tails use
        // the existing coverage barrier, avoiding interactions between purges
        // and reused client keys.
        let push = matches!(entries[0].1, RetainedMembershipChange::Push(_));
        let mut ids = HashSet::new();
        let mut keys = HashSet::new();
        for (_, change) in &entries {
            match change {
                RetainedMembershipChange::Push(items) if push => {
                    for (id, key) in items {
                        if !ids.insert(*id) || !keys.insert(key.clone()) {
                            return Ok(None);
                        }
                    }
                }
                RetainedMembershipChange::Purge(items) if !push => {
                    for id in items {
                        if !ids.insert(*id) {
                            return Ok(None);
                        }
                    }
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(entries))
    }

    /// Retain only count-relevant facts from a complete, bounded lifecycle tail.
    /// Row state/version guards are checked against one SQL snapshot by the reader.
    pub async fn retained_lifecycle_tail(
        &self,
        applied: &CommandPosition,
        target: &CommandPosition,
    ) -> EngineResult<Option<Vec<(CommandPosition, RetainedLifecycleChange)>>> {
        self.ensure_healthy(&target.queue)?;
        if applied.queue != target.queue
            || applied.backend_epoch != target.backend_epoch
            || !matches!(target.sequence.checked_sub(applied.sequence), Some(1..=16))
        {
            return Ok(None);
        }
        let state = self.inner.state.lock().await;
        let mut entries = Vec::new();
        let mut remaining = 8192usize;
        for entry in &state.entries {
            let ApplyEntry::Ready(batch) = entry else {
                continue;
            };
            if batch.shard != target.queue {
                continue;
            }
            if batch.positions.len() != batch.commands.len() {
                return Ok(None);
            }
            for (position, envelope) in batch.positions.iter().zip(&batch.commands) {
                if position.backend_epoch != target.backend_epoch
                    || position.sequence <= applied.sequence
                    || position.sequence > target.sequence
                {
                    continue;
                }
                if position.queue != target.queue || entries.len() == 16 {
                    return Ok(None);
                }
                let change = match &envelope.command {
                    QueueCommand::Claim(claim) if claim.authority_first => {
                        let Some(left) = remaining.checked_sub(claim.item_ids.len()) else {
                            return Ok(None);
                        };
                        remaining = left;
                        let mut unique = HashSet::new();
                        if claim.item_ids.iter().any(|id| !unique.insert(*id)) {
                            return Ok(None);
                        }
                        RetainedLifecycleChange::Claim(claim.item_ids.clone())
                    }
                    QueueCommand::MutateItems(command) if command.gate_changes.is_empty() => {
                        let Some(left) = remaining.checked_sub(command.items.len()) else {
                            return Ok(None);
                        };
                        remaining = left;
                        let mut unique = HashSet::new();
                        let mut replacements = Vec::with_capacity(command.items.len());
                        for item in &command.items {
                            let Some(values) = item.action.replacement_values() else {
                                return Ok(None);
                            };
                            if !values.invalidate_lease
                                || values.state == fireweed_core::ItemState::Leased
                                || values.item_version == 0
                                || values.item_version > i64::MAX as u64
                                || !values.gate_keys.is_empty()
                                || !values.index_fields.is_empty()
                                || !unique.insert(item.item_id)
                            {
                                return Ok(None);
                            }
                            replacements.push((item.item_id, values.state, values.item_version));
                        }
                        RetainedLifecycleChange::Replace(replacements)
                    }
                    _ => return Ok(None),
                };
                entries.push((position.clone(), change));
            }
        }
        entries.sort_by_key(|(position, _)| position.sequence);
        if entries.len() as u64 != target.sequence - applied.sequence
            || entries
                .iter()
                .enumerate()
                .any(|(i, (position, _))| position.sequence != applied.sequence + 1 + i as u64)
        {
            return Ok(None);
        }
        Ok(Some(entries))
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
        // Already-covered reads must not interrupt a pending Claim's follow-up
        // window. Notifying before this check can wake apply on another runtime
        // thread while the transient coverage-waiter count is still nonzero.
        self.ensure_healthy(shard)?;
        let snapshot = self.snapshot(shard).await;
        if let Some(reason) = snapshot.poison_reason {
            return Err(poisoned(&reason));
        }
        if position_covers(snapshot.applied_high_water.as_ref(), target) {
            return Ok(());
        }
        // A dependent read needs Claim applied now. Waiting for a follow-up
        // cannot help when that Complete itself needs projection coverage first.
        // A neighboring queue's read does not depend on this queue's Claim.
        // Track cancellation-safe registrations per queue, retaining the bounded
        // join window and FIFO selection between runnable queues.
        struct CoverageWaiter<'a>(&'a std::sync::Mutex<HashMap<QueueKey, u64>>, QueueKey);
        impl Drop for CoverageWaiter<'_> {
            fn drop(&mut self) {
                let mut waiters = self.0.lock().expect("coverage waiter mutex");
                let count = waiters
                    .get_mut(&self.1)
                    .expect("registered coverage waiter");
                *count -= 1;
                if *count == 0 {
                    waiters.remove(&self.1);
                }
            }
        }
        *self
            .inner
            .coverage_waiters
            .lock()
            .expect("coverage waiter mutex")
            .entry(shard.clone())
            .or_default() += 1;
        let _coverage_waiter = CoverageWaiter(&self.inner.coverage_waiters, shard.clone());
        self.inner.changed.notify_waiters();
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

struct ClaimJoinWindow {
    started: Instant,
    commands_before: usize,
}

enum WorkerSelection {
    Ready(ApplyGeneration),
    WaitUntil(Instant),
    Idle,
}

fn select_worker_generation(
    state: &CoordinatorState,
    joins: &mut HashMap<u64, ClaimJoinWindow>,
    now: Instant,
    has_waiter: impl Fn(&QueueKey) -> bool,
) -> WorkerSelection {
    // Join windows belong to the first retained entry, not notifications. New
    // work in any queue cannot restart the deadline and starve an old claim.
    joins.retain(|id, _| state.entries.iter().any(|entry| entry.id() == *id));
    let mut deferred = HashSet::new();
    let mut earliest = None;
    loop {
        let Some(generation) = next_coalesced_generation_excluding(state, &deferred) else {
            return earliest.map_or(WorkerSelection::Idle, WorkerSelection::WaitUntil);
        };
        if commands_are_claim_without_complete(generation.commands()) {
            let window = joins
                .entry(generation.batches[0].id)
                .or_insert(ClaimJoinWindow {
                    started: now,
                    commands_before: generation.commands().count(),
                });
            let deadline = window.started + Duration::from_millis(CLAIM_COMPLETE_JOIN_MS);
            if now < deadline && !has_waiter(&generation.batches[0].shard) {
                earliest = Some(earliest.map_or(deadline, |prior: Instant| prior.min(deadline)));
                deferred.insert(generation.batches[0].shard.clone());
                continue;
            }
        }
        return WorkerSelection::Ready(generation.materialize());
    }
}

async fn run_worker<P>(inner: Arc<CoordinatorInner<P>>)
where
    P: AsyncProjectionStore + 'static,
{
    let mut joins = HashMap::new();
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
        let generation = {
            // Arm before inspecting readiness: notify_waiters does not retain a
            // permit for a future created after the notification.
            let changed = inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let selection = {
                let state = inner.state.lock().await;
                select_worker_generation(&state, &mut joins, Instant::now(), |queue| {
                    queue_has_coverage_waiter(&inner, queue)
                })
            };
            match selection {
                WorkerSelection::Ready(generation) => generation,
                WorkerSelection::WaitUntil(deadline) => {
                    tokio::select! {
                        _ = changed => {}
                        _ = tokio::time::sleep_until(deadline.into()) => {}
                    }
                    continue;
                }
                WorkerSelection::Idle => {
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
                }
            }
        };
        if let Some(window) = joins.remove(&generation.entry_ids[0])
            && std::env::var_os("FIREWEED_APPLY_TRACE").is_some()
        {
            eprintln!(
                "apply_join us={} before={} after={} queue_waiter={}",
                window.started.elapsed().as_micros(),
                window.commands_before,
                generation.commands.len(),
                queue_has_coverage_waiter(&inner, &generation.shard),
            );
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
                generation.commands,
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

/// Briefly join a follow-up that invalidates the claimed leases before applying
/// intermediate Leased rows. Coverage waiters bypass this bounded background
/// delay, and notifications cannot restart the original deadline. The one-second
/// experiment did not improve sustained throughput; retain the 500 ms bound.
const CLAIM_COMPLETE_JOIN_MS: u64 = 500;

fn queue_has_coverage_waiter<P: AsyncProjectionStore + 'static>(
    inner: &CoordinatorInner<P>,
    shard: &QueueKey,
) -> bool {
    inner
        .coverage_waiters
        .lock()
        .expect("coverage waiter mutex")
        .get(shard)
        .copied()
        .unwrap_or(0)
        != 0
}

#[cfg(test)]
fn generation_is_claim_without_complete(generation: &ApplyGeneration) -> bool {
    commands_are_claim_without_complete(generation.commands.iter())
}

fn commands_are_claim_without_complete<'a>(
    commands: impl IntoIterator<Item = &'a CommandEnvelope>,
) -> bool {
    let mut claim = false;
    let mut complete = false;
    for envelope in commands {
        match &envelope.command {
            QueueCommand::Claim(_) => claim = true,
            QueueCommand::Finalize(finalize)
                if finalize.outcomes.iter().all(|outcome| {
                    matches!(outcome.kind, fireweed_engine::FinalizeKind::Complete)
                }) =>
            {
                complete = true;
            }
            QueueCommand::MutateItems(mutation)
                if mutation.items.iter().any(|item| {
                    item.action
                        .replacement_values()
                        .is_some_and(|values| values.invalidate_lease)
                }) =>
            {
                complete = true
            }
            _ => {}
        }
    }
    claim && !complete
}

#[cfg(test)]
fn next_coalesced_generation(state: &CoordinatorState) -> Option<ApplyGeneration> {
    next_coalesced_generation_excluding(state, &HashSet::new())
        .map(ApplyGenerationPlan::materialize)
}

fn next_coalesced_generation_excluding<'a>(
    state: &'a CoordinatorState,
    excluded: &HashSet<QueueKey>,
) -> Option<ApplyGenerationPlan<'a>> {
    let (_, first) = next_runnable_excluding(state, excluded)?;
    let mut last = first.positions.last().cloned()?;
    let mut envelopes = generation_envelope_count(first);
    let mut seen_items = HashSet::new();
    insert_batch_item_ids(&mut seen_items, first);
    let mut debt = first.debt_bytes;
    let mut generation = ApplyGenerationPlan {
        batches: vec![first],
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
        generation.batches.push(batch);
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

/// Accept only a complete, contiguous tail of disjoint authoritative claims.
/// Anything else retains the normal projection-coverage barrier.
pub fn claim_only_tail(
    applied: &CommandPosition,
    target: &CommandPosition,
    entries: Vec<(CommandPosition, CommandEnvelope)>,
) -> Option<Vec<fireweed_engine::ClaimCommand>> {
    if applied.backend_epoch != target.backend_epoch || applied.queue != target.queue {
        return None;
    }
    let mut sequence = applied.sequence;
    let mut seen = HashSet::new();
    let mut claims = Vec::new();
    for (position, envelope) in entries {
        sequence = sequence.checked_add(1)?;
        if position.queue != target.queue
            || position.backend_epoch != target.backend_epoch
            || position.sequence != sequence
            || sequence > target.sequence
        {
            return None;
        }
        let QueueCommand::Claim(claim) = envelope.command else {
            return None;
        };
        if !claim.authority_first || claim.item_ids.iter().any(|id| !seen.insert(*id)) {
            return None;
        }
        claims.push(claim);
    }
    (sequence == target.sequence && !claims.is_empty()).then_some(claims)
}

fn next_runnable(state: &CoordinatorState) -> Option<(usize, &ApplyBatch)> {
    next_runnable_excluding(state, &HashSet::new())
}

fn next_runnable_excluding<'a>(
    state: &'a CoordinatorState,
    excluded: &HashSet<QueueKey>,
) -> Option<(usize, &'a ApplyBatch)> {
    let mut best: Option<(usize, &ApplyBatch)> = None;
    for (index, entry) in state.entries.iter().enumerate() {
        let shard = entry.shard();
        if excluded.contains(shard) {
            continue;
        }
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
                // Preserve the first runnable queue in admission order. Within
                // that queue choose its earliest log position, even if readiness
                // notifications arrived out of order.
                other.shard == batch.shard
                    && first
                        < other
                            .positions
                            .first()
                            .map(|p| (p.backend_epoch, p.sequence))
            }
        };
        if better {
            best = Some((index, batch));
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

    #[tokio::test]
    async fn lifecycle_tail_requires_contiguous_bounded_guarded_commands() {
        use fireweed_engine::{
            ClaimCommand, MutateItemsCommand, ResolvedItemMutation, ResolvedItemMutationAction,
            ResolvedItemValues,
        };
        let coordinator = coordinator();
        coordinator.pause();
        let id = fireweed_core::ItemId::mint(1, 0, 1);
        let claim = ClaimCommand::new(
            vec![id],
            fireweed_core::LeaseToken::new("lifecycle").unwrap(),
            fireweed_core::UtcTimestamp::new(30, 0).unwrap(),
            None,
        )
        .with_authority_first();
        let values = ResolvedItemValues {
            state: fireweed_core::ItemState::Pending,
            item_version: 3,
            priority: None,
            not_before: None,
            eligible_since: fireweed_core::UtcTimestamp::new(1, 0).unwrap(),
            payload: None,
            fields: Default::default(),
            metadata: Default::default(),
            gate_keys: vec![],
            index_fields: Default::default(),
            entity_document: None,
            invalidate_lease: true,
        };
        let mutation = MutateItemsCommand {
            items: vec![ResolvedItemMutation {
                item_id: id,
                action: ResolvedItemMutationAction::ReplaceKeepingPayload(Box::new(values.clone())),
            }],
            gate_changes: vec![],
        };
        let batch = |sequence, command| {
            let mut envelope = pause_env("lifecycle");
            envelope.command = command;
            ApplyBatch {
                id: sequence,
                shard: shard(),
                positions: vec![pos(sequence)],
                commands: vec![envelope],
                command_count: 1,
                debt_bytes: 0,
                enqueued_at: Instant::now(),
            }
        };
        let coordinator_ref = &coordinator;
        let install = |batches: Vec<ApplyBatch>| async move {
            coordinator_ref.inner.state.lock().await.entries =
                batches.into_iter().map(ApplyEntry::Ready).collect();
        };
        let first = batch(1, QueueCommand::Claim(claim.clone()));
        let second = batch(2, QueueCommand::MutateItems(mutation.clone()));
        install(vec![second.clone(), first.clone()]).await;
        let retained = coordinator
            .retained_lifecycle_tail(&pos(0), &pos(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            retained.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(
            matches!(&retained[1].1, RetainedLifecycleChange::Replace(items) if items == &vec![(id, fireweed_core::ItemState::Pending, 3)])
        );
        for target in [pos(0), pos(3), pos(17), CommandPosition::new(shard(), 1, 2)] {
            assert!(
                coordinator
                    .retained_lifecycle_tail(&pos(0), &target)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        install(vec![second.clone()]).await;
        assert!(
            coordinator
                .retained_lifecycle_tail(&pos(0), &pos(2))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            coordinator
                .retained_lifecycle_tail(&pos(1), &pos(2))
                .await
                .unwrap()
                .is_some()
        );
        for (case, mut bad) in [claim.clone(), claim.clone(), claim.clone()]
            .into_iter()
            .enumerate()
        {
            match case {
                0 => bad.authority_first = false,
                1 => bad.item_ids.push(id),
                _ => {
                    bad.item_ids = (0..8193)
                        .map(|n| fireweed_core::ItemId::mint(1, 0, n))
                        .collect()
                }
            }
            install(vec![batch(1, QueueCommand::Claim(bad)), second.clone()]).await;
            assert!(
                coordinator
                    .retained_lifecycle_tail(&pos(0), &pos(2))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        for case in 0..5 {
            let mut bad = mutation.clone();
            match case {
                0 => bad.items.push(bad.items[0].clone()),
                1 => bad.items[0].action = ResolvedItemMutationAction::Purge,
                _ => {
                    let mut v = values.clone();
                    match case {
                        2 => v.invalidate_lease = false,
                        3 => v.item_version = 0,
                        _ => v.gate_keys.push("gate".into()),
                    }
                    bad.items[0].action =
                        ResolvedItemMutationAction::ReplaceKeepingPayload(Box::new(v));
                }
            }
            install(vec![
                first.clone(),
                batch(2, QueueCommand::MutateItems(bad)),
            ])
            .await;
            assert!(
                coordinator
                    .retained_lifecycle_tail(&pos(0), &pos(2))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn membership_tail_requires_complete_unique_bounded_single_family() {
        let coordinator = coordinator();
        coordinator.pause();
        let mut batches = Vec::new();
        for sequence in 1..=2 {
            let mut envelope = pause_env("membership");
            envelope.command = QueueCommand::Push(fireweed_engine::PushCommand {
                items: vec![fireweed_conformance::item(
                    &sequence.to_string(),
                    &format!("key-{sequence}"),
                    1,
                )],
            });
            batches.push(ApplyBatch {
                id: sequence,
                shard: shard(),
                positions: vec![pos(sequence)],
                commands: vec![envelope],
                command_count: 1,
                debt_bytes: 0,
                enqueued_at: Instant::now(),
            });
        }
        let coordinator_ref = &coordinator;
        let install = |batches: Vec<ApplyBatch>| async move {
            let mut state = coordinator_ref.inner.state.lock().await;
            state.entries = batches.into_iter().map(ApplyEntry::Ready).collect();
        };
        let mut genesis = batches[0].clone();
        genesis.positions = vec![pos(0)];
        install(vec![genesis.clone()]).await;
        assert_eq!(
            coordinator
                .retained_membership_tail(None, &pos(0))
                .await
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        assert!(
            coordinator
                .retained_membership_tail(None, &pos(1))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            coordinator
                .retained_membership_tail(None, &CommandPosition::new(shard(), 1, 0))
                .await
                .unwrap()
                .is_none()
        );
        install(vec![batches[1].clone(), batches[0].clone()]).await;
        assert!(
            coordinator
                .retained_membership_tail(None, &pos(2))
                .await
                .unwrap()
                .is_none()
        );
        let tail = coordinator
            .retained_membership_tail(Some(&pos(0)), &pos(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            tail.iter().map(|(p, _)| p.sequence).collect::<Vec<_>>(),
            vec![1, 2]
        );
        for target in [pos(3), pos(17), CommandPosition::new(shard(), 1, 2)] {
            assert!(
                coordinator
                    .retained_membership_tail(Some(&pos(0)), &target)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let mut duplicate = batches.clone();
        duplicate[1].commands = duplicate[0].commands.clone();
        install(duplicate).await;
        assert!(
            coordinator
                .retained_membership_tail(Some(&pos(0)), &pos(2))
                .await
                .unwrap()
                .is_none()
        );
        let mut mixed = batches.clone();
        mixed[1].commands[0].command =
            QueueCommand::PurgeItems(fireweed_engine::PurgeItemsCommand {
                item_ids: vec![ItemId::mint(1, 0, 7)],
                force: true,
            });
        install(mixed).await;
        assert!(
            coordinator
                .retained_membership_tail(Some(&pos(0)), &pos(2))
                .await
                .unwrap()
                .is_none()
        );
        let mut large = batches.clone();
        let QueueCommand::Push(push) = &mut large[0].commands[0].command else {
            unreachable!()
        };
        push.items = vec![push.items[0].clone(); 8193];
        install(large).await;
        assert!(
            coordinator
                .retained_membership_tail(Some(&pos(0)), &pos(2))
                .await
                .unwrap()
                .is_none()
        );
        install(vec![batches[1].clone()]).await;
        assert!(
            coordinator
                .retained_membership_tail(Some(&pos(0)), &pos(2))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            coordinator
                .retained_membership_tail(Some(&pos(1)), &pos(2))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn retained_claim_tail_requires_complete_bounded_authority() {
        let coordinator = coordinator();
        coordinator.pause();
        let first_id = ItemId::mint(1, 0, 1);
        let mut batches = Vec::new();
        for sequence in 1..=2 {
            let mut envelope = pause_env("retained");
            envelope.command = QueueCommand::Claim(
                fireweed_engine::ClaimCommand::new(
                    vec![ItemId::mint(1, 0, sequence as u32)],
                    fireweed_core::LeaseToken::new(format!("token-{sequence}")).unwrap(),
                    UtcTimestamp::new(30, 0).unwrap(),
                    None,
                )
                .with_authority_first(),
            );
            batches.push(ApplyBatch {
                id: sequence,
                shard: shard(),
                positions: vec![pos(sequence)],
                commands: vec![envelope],
                command_count: 1,
                debt_bytes: 0,
                enqueued_at: Instant::now(),
            });
        }
        {
            let mut state = coordinator.inner.state.lock().await;
            // Ready entries may arrive out of order; other queues are irrelevant.
            state
                .entries
                .push_back(ApplyEntry::Ready(batches[1].clone()));
            state
                .entries
                .push_back(ready_on(shard_named("foreign"), 9, 1, 1, 1, 0));
            state
                .entries
                .push_back(ApplyEntry::Ready(batches[0].clone()));
        }
        let tail = coordinator
            .retained_claim_tail(&pos(0), &pos(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].item_ids, vec![first_id]);
        for target in [
            pos(3),
            pos(17),
            CommandPosition::new(shard(), 2, 2),
            pos_on(shard_named("foreign"), 2),
        ] {
            assert!(
                coordinator
                    .retained_claim_tail(&pos(0), &target)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        for bad in 0..4 {
            let mut changed = batches.clone();
            match bad {
                0 => {
                    changed.remove(0);
                } // missing acknowledged prefix
                1 => {
                    changed[1].commands[0].command = QueueCommand::PauseQueue(Default::default());
                }
                2 | 3 => {
                    let QueueCommand::Claim(claim) = &mut changed[1].commands[0].command else {
                        unreachable!()
                    };
                    if bad == 2 {
                        claim.item_ids = vec![first_id];
                    } else {
                        claim.authority_first = false;
                    }
                }
                _ => unreachable!(),
            }
            coordinator.inner.state.lock().await.entries =
                changed.into_iter().map(ApplyEntry::Ready).collect();
            assert!(
                coordinator
                    .retained_claim_tail(&pos(0), &pos(2))
                    .await
                    .unwrap()
                    .is_none(),
                "invalid tail {bad}"
            );
        }
        coordinator
            .latch_poison(shard(), "test poison".into())
            .await;
        assert!(
            coordinator
                .retained_claim_tail(&pos(0), &pos(2))
                .await
                .is_err()
        );
    }

    #[test]
    fn neighboring_ready_work_does_not_hide_a_claim_followup() {
        let mut state = CoordinatorState::default();
        let mut claim = pause_env("waiting-claim");
        claim.command = QueueCommand::Claim(fireweed_engine::ClaimCommand::new(
            vec![],
            fireweed_core::LeaseToken::new("token").unwrap(),
            UtcTimestamp::new(30, 0).unwrap(),
            None,
        ));
        let entry = |id, sequence, command| {
            ApplyEntry::Ready(ApplyBatch {
                id,
                shard: shard(),
                positions: vec![pos(sequence)],
                commands: vec![command],
                command_count: 1,
                debt_bytes: 0,
                enqueued_at: Instant::now(),
            })
        };
        state.entries.push_back(entry(1, 1, claim));
        let waiting = next_coalesced_generation(&state).unwrap();
        assert!(generation_is_claim_without_complete(&waiting));
        state
            .entries
            .push_back(ready_on(shard_named("neighbor"), 2, 1, 1, 1, 0));
        let mut complete = pause_env("followup");
        complete.command =
            QueueCommand::Finalize(fireweed_engine::FinalizeCommand { outcomes: vec![] });
        state.entries.push_back(entry(3, 2, complete));
        let refreshed = next_coalesced_generation(&state).unwrap();
        assert_eq!(
            refreshed.entry_ids,
            vec![1, 3],
            "a neighbor must not mask the awaited followup"
        );
        assert!(!generation_is_claim_without_complete(&refreshed));
    }

    #[test]
    fn ready_queues_keep_fifo_turns_under_replenishment() {
        let mut state = CoordinatorState::default();
        let first = shard();
        let second = shard_named("second");
        state
            .entries
            .push_back(ready_on(first.clone(), 1, 1, 1, 0, 0));
        state
            .entries
            .push_back(ready_on(second.clone(), 2, 1, 1, 0, 0));
        let (index, batch) = next_runnable(&state).unwrap();
        assert_eq!(
            batch.shard, first,
            "later queues must not overtake ready work"
        );
        state.entries.remove(index);
        state
            .shards
            .entry(first.clone())
            .or_default()
            .applied_high_water = Some(pos_on(first.clone(), 1));
        state.entries.push_back(ready_on(first, 3, 2, 1, 0, 0));
        let (_, batch) = next_runnable(&state).unwrap();
        assert_eq!(
            batch.shard, second,
            "a replenished queue must wait its turn"
        );
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
    async fn covered_read_does_not_interrupt_background_coalescing() {
        let coordinator = coordinator();
        coordinator.seed_high_water(shard(), Some(pos(1))).await;
        let changed = coordinator.inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        coordinator
            .wait_until_covers(&shard(), &pos(1), Duration::from_millis(20))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(2), changed)
                .await
                .is_err(),
            "a covered read must not wake apply and prematurely publish later claims"
        );
    }

    #[tokio::test]
    async fn coverage_read_bypasses_claim_followup_delay() {
        let coordinator = coordinator();
        let mut envelope = pause_env("claim-for-read");
        envelope.command = QueueCommand::Claim(fireweed_engine::ClaimCommand::new(
            vec![],
            fireweed_core::LeaseToken::new("read-token").unwrap(),
            fireweed_core::UtcTimestamp::new(30, 0).unwrap(),
            None,
        ));
        let commands = vec![envelope];
        let reservation = coordinator.reserve(shard(), &commands).await.unwrap();
        coordinator
            .enqueue_reserved(reservation, vec![pos(1)], commands)
            .await
            .unwrap();
        // Let the background worker enter its join window before requesting coverage.
        tokio::time::sleep(Duration::from_millis(20)).await;
        coordinator
            .wait_until_covers(&shard(), &pos(1), Duration::from_millis(200))
            .await
            .expect("dependent read must bypass the 500 ms background join window");
        assert_eq!(coordinator.apply_live_call_count(), 1);
    }

    #[tokio::test]
    async fn unrelated_queue_coverage_does_not_break_claim_followup_join() {
        use std::future::Future;
        use std::task::Poll;
        let coordinator = coordinator();
        coordinator.pause();
        let mut claim = pause_env("claim-with-neighbor");
        claim.command = QueueCommand::Claim(fireweed_engine::ClaimCommand::new(
            vec![],
            fireweed_core::LeaseToken::new("neighbor-token").unwrap(),
            UtcTimestamp::new(30, 0).unwrap(),
            None,
        ));
        let reservation = coordinator
            .reserve(shard(), &[claim.clone()])
            .await
            .unwrap();
        coordinator
            .enqueue_reserved(reservation, vec![pos(1)], vec![claim])
            .await
            .unwrap();
        let other = shard_named("other");
        let target = pos_on(other.clone(), 1);
        let mut waiting =
            Box::pin(coordinator.wait_until_covers(&other, &target, Duration::from_secs(5)));
        // Poll once to register the uncovered neighbor before the worker resumes.
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        coordinator.resume();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            coordinator.apply_live_call_count(),
            0,
            "another queue's uncovered read must not flush this queue's claim"
        );
        let mut complete = pause_env("complete-with-neighbor");
        complete.command =
            QueueCommand::Finalize(fireweed_engine::FinalizeCommand { outcomes: vec![] });
        let reservation = coordinator
            .reserve(shard(), &[complete.clone()])
            .await
            .unwrap();
        coordinator
            .enqueue_reserved(reservation, vec![pos(2)], vec![complete])
            .await
            .unwrap();
        coordinator.wait_for_catch_up(&shard()).await.unwrap();
        assert_eq!(
            *coordinator.inner.apply_live_command_counts.lock().unwrap(),
            vec![2]
        );
        drop(waiting); // cancellation must release the neighbor's coverage registration
        assert!(
            coordinator
                .inner
                .coverage_waiters
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    fn waiting_claim(queue: QueueKey, id: u64) -> ApplyEntry {
        let mut entry = ready_on(queue, id, 1, 1, 1, 0);
        let ApplyEntry::Ready(batch) = &mut entry else {
            unreachable!()
        };
        batch.commands[0].item_ids.clear();
        batch.commands[0].command = QueueCommand::Claim(fireweed_engine::ClaimCommand::new(
            vec![],
            fireweed_core::LeaseToken::new("window-token").unwrap(),
            UtcTimestamp::new(30, 0).unwrap(),
            None,
        ));
        entry
    }

    #[test]
    fn late_followup_can_join_without_restarting_the_claim_deadline() {
        let now = Instant::now();
        let mut state = CoordinatorState {
            entries: VecDeque::from([waiting_claim(shard(), 1)]),
            ..Default::default()
        };
        let mut joins = HashMap::new();
        let WorkerSelection::WaitUntil(deadline) =
            select_worker_generation(&state, &mut joins, now, |_| false)
        else {
            panic!("claim should await its follow-up");
        };
        let late = now + Duration::from_millis(CLAIM_COMPLETE_JOIN_MS - 1);
        let WorkerSelection::WaitUntil(unchanged_deadline) =
            select_worker_generation(&state, &mut joins, late, |_| false)
        else {
            panic!("do not materialize the claim before its original deadline");
        };
        assert_eq!(unchanged_deadline, deadline);
        let mut followup = ready_on(shard(), 2, 2, 1, 1, 0);
        let ApplyEntry::Ready(batch) = &mut followup else {
            unreachable!()
        };
        batch.commands[0].command =
            QueueCommand::Finalize(fireweed_engine::FinalizeCommand { outcomes: vec![] });
        state.entries.push_back(followup);
        let WorkerSelection::Ready(next) =
            select_worker_generation(&state, &mut joins, late, |_| false)
        else {
            panic!("ready follow-up should release the joined prefix immediately");
        };
        assert_eq!(next.entry_ids, vec![1, 2]);
    }

    #[test]
    fn claim_join_deadline_is_not_extended_by_ready_neighbors() {
        let now = Instant::now();
        let state = CoordinatorState {
            entries: VecDeque::from([
                waiting_claim(shard(), 1),
                ready_on(shard_named("other"), 2, 1, 1, 1, 0),
            ]),
            ..Default::default()
        };
        let mut joins = HashMap::new();
        for elapsed in [0, 100, CLAIM_COMPLETE_JOIN_MS - 1] {
            let WorkerSelection::Ready(next) = select_worker_generation(
                &state,
                &mut joins,
                now + Duration::from_millis(elapsed),
                |_| false,
            ) else {
                panic!("ready neighbor")
            };
            assert_eq!(next.entry_ids, vec![2]);
        }
        let WorkerSelection::Ready(next) = select_worker_generation(
            &state,
            &mut joins,
            now + Duration::from_millis(CLAIM_COMPLETE_JOIN_MS),
            |_| false,
        ) else {
            panic!("expired claim window")
        };
        assert_eq!(
            next.entry_ids,
            vec![1],
            "an expired oldest claim regains its FIFO turn"
        );
    }

    #[test]
    fn independent_claim_windows_overlap_and_coverage_preempts_only_its_queue() {
        let now = Instant::now();
        let other = shard_named("other");
        let mut state = CoordinatorState {
            entries: VecDeque::from([waiting_claim(shard(), 1), waiting_claim(other.clone(), 2)]),
            ..Default::default()
        };
        let mut joins = HashMap::new();
        let WorkerSelection::WaitUntil(deadline) =
            select_worker_generation(&state, &mut joins, now, |_| false)
        else {
            panic!("both handlers are pending")
        };
        assert_eq!(
            deadline,
            now + Duration::from_millis(CLAIM_COMPLETE_JOIN_MS)
        );
        assert_eq!(joins.len(), 2);
        let WorkerSelection::Ready(next) =
            select_worker_generation(&state, &mut joins, now + Duration::from_millis(1), |q| {
                q == &other
            })
        else {
            panic!("neighbor's coverage")
        };
        assert_eq!(next.entry_ids, vec![2]);
        let WorkerSelection::Ready(first) =
            select_worker_generation(&state, &mut joins, deadline, |_| false)
        else {
            panic!("first deadline")
        };
        assert_eq!(first.entry_ids, vec![1]);
        state.entries.pop_front();
        let WorkerSelection::Ready(second) =
            select_worker_generation(&state, &mut joins, deadline, |_| false)
        else {
            panic!("second window must already have elapsed")
        };
        assert_eq!(second.entry_ids, vec![2]);
    }

    #[tokio::test]
    async fn claim_join_does_not_block_ready_neighbor_or_force_claim_apply() {
        let other = shard_named("other");
        let mut projection = fireweed_projection::InMemoryProjection::new();
        ProjectionStore::ensure_shard(&mut projection, &qdef()).unwrap();
        let mut other_definition = qdef();
        other_definition.queue_id = other.queue_id.clone();
        ProjectionStore::ensure_shard(&mut projection, &other_definition).unwrap();
        let coordinator = AsyncProjectionApplyCoordinator::new(
            Arc::new(fireweed_projection::AsyncInMemoryProjection::new(
                projection,
            )),
            AsyncProjectionSpec::new(32, 4096, 16, 30_000, 3).unwrap(),
        )
        .unwrap();
        coordinator.pause();
        let mut claim = pause_env("claim-waiting-for-handler");
        claim.command = QueueCommand::Claim(fireweed_engine::ClaimCommand::new(
            vec![],
            fireweed_core::LeaseToken::new("waiting-token").unwrap(),
            UtcTimestamp::new(30, 0).unwrap(),
            None,
        ));
        let reservation = coordinator
            .reserve(shard(), &[claim.clone()])
            .await
            .unwrap();
        coordinator
            .enqueue_reserved(reservation, vec![pos(1)], vec![claim])
            .await
            .unwrap();
        let neighbor = pause_env("neighbor-ready");
        let reservation = coordinator
            .reserve(other.clone(), std::slice::from_ref(&neighbor))
            .await
            .unwrap();
        coordinator
            .enqueue_reserved(reservation, vec![pos_on(other.clone(), 1)], vec![neighbor])
            .await
            .unwrap();
        coordinator.resume();
        coordinator
            .wait_until_covers(
                &other,
                &pos_on(other.clone(), 1),
                Duration::from_millis(200),
            )
            .await
            .expect("a ready neighbor must not wait for the other queue's 500 ms handler join");
        assert!(
            coordinator
                .snapshot(&shard())
                .await
                .applied_high_water
                .is_none(),
            "serving the neighbor must retain the claim's opportunity to fuse"
        );
        let mut complete = pause_env("handler-completed");
        complete.command =
            QueueCommand::Finalize(fireweed_engine::FinalizeCommand { outcomes: vec![] });
        let reservation = coordinator
            .reserve(shard(), &[complete.clone()])
            .await
            .unwrap();
        coordinator
            .enqueue_reserved(reservation, vec![pos(2)], vec![complete])
            .await
            .unwrap();
        coordinator.wait_for_catch_up(&shard()).await.unwrap();
        assert_eq!(coordinator.apply_live_command_counts(), vec![1, 2]);
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
