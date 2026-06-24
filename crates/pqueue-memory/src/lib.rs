#![forbid(unsafe_code)]
//! # pqueue-memory
//!
//! In-memory reference backend (atomic durability class). It is a thin **persistence wrapper** over the
//! shared projection state machine in [`pqueue_projection`]: one `Mutex<State>` holding a `LogData` +
//! `ProjectionData` per shard, with `write` taking the lock for the whole unit of work so append +
//! apply commit together (TD-007 §1 atomic class). All apply/eligibility/lease/metrics logic lives in
//! `pqueue-projection` and is shared with the durable backends; this crate only locks and delegates.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, PriorityValue, QueueDefinition, QueueId, TenantId,
    UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimPort, ClaimRequest, Claimed, ClaimedItem, Clock, CommandChecksum,
    CommandEnvelope, CommandId, CommandPage, CommandPosition, ControlPlaneStore, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, FinalizeCommand, FinalizeOutcome, FinalizePort, IdGen,
    ItemView, LeaseExpiredCommand, LeaseView, LogRead, LogWriter, ProjectionRead, ProjectionSnapshot,
    ProjectionWriter, PushCommand, PushItem, QueueCommand, QueueKey, QueueMetrics, ReclaimDriver,
    ReplacePendingCommand, ShardId, ShardKey, SnapshotRef, SnapshotStore, TickReport, UpsertOutcome,
    UpsertPort,
};
use pqueue_engine::{PushPort, PushSpec, build_push_items};
use pqueue_projection::{LogData, ProjectionData, commit};

// ---------------------------------------------------------------------------
// UoW writer views (disjoint borrows of logs / projections — review M2)
// ---------------------------------------------------------------------------

struct LogWriterView<'a> {
    logs: &'a mut HashMap<ShardKey, LogData>,
}

impl LogWriter for LogWriterView<'_> {
    fn append(
        &mut self,
        shard: &ShardKey,
        commands: &[CommandEnvelope],
    ) -> EngineResult<Vec<CommandPosition>> {
        self.logs
            .get_mut(shard)
            .ok_or(EngineError::NotFound)?
            .append(shard, commands)
    }
}

struct ProjectionWriterView<'a> {
    projections: &'a mut HashMap<ShardKey, ProjectionData>,
}

impl ProjectionWriter for ProjectionWriterView<'_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, cmd) in positions.iter().zip(commands) {
            self.projections
                .get_mut(&pos.shard_key)
                .ok_or(EngineError::NotFound)?
                .apply_command(&cmd.command)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// State + backend
// ---------------------------------------------------------------------------

#[derive(Default)]
struct State {
    logs: HashMap<ShardKey, LogData>,
    projections: HashMap<ShardKey, ProjectionData>,
    queues: HashMap<QueueKey, QueueDefinition>,
}

/// In-memory atomic-class backend. One `Mutex<State>`; `write` takes the lock for the whole unit of
/// work, so append + apply commit together (TD-007 §1 atomic class).
pub struct MemoryBackend {
    state: Mutex<State>,
    cmd_seq: AtomicU64,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            cmd_seq: AtomicU64::new(0),
        }
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn launch_shard(key: &QueueKey) -> ShardKey {
        ShardKey::new(key.tenant_id.clone(), key.queue_id.clone(), ShardId::ZERO)
    }

    fn make_envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        created_at: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("mem-{n}")),
            request_id: None,
            shard_id: ShardId::ZERO,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    /// Append `env` to the shard log and apply it to the projection under the already-held lock — the
    /// atomic append+apply unit of work the claim/upsert/reclaim ports rely on (shared `commit`).
    fn commit_locked(state: &mut State, shard: &ShardKey, env: CommandEnvelope) -> EngineResult<()> {
        let State {
            logs, projections, ..
        } = state;
        let log = logs.get_mut(shard).ok_or(EngineError::NotFound)?;
        let proj = projections.get_mut(shard).ok_or(EngineError::NotFound)?;
        commit(log, proj, shard, env)
    }
}

impl ClaimPort for MemoryBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            // Select priority-ordered eligible candidates (Invariant 1: per-item, in eligible order).
            let candidates: Vec<ItemId> = {
                let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
                proj.eligible_candidates(req.now, req.max_items)
            };
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            // Lease them atomically (append Claim command + apply, one lock).
            let cmd = QueueCommand::Claim(ClaimCommand {
                item_ids: candidates.clone(),
                lease_token: req.lease_token.clone(),
                lease_expires_at: req.lease_expires_at,
            });
            let env = self.make_envelope(cmd, candidates.clone(), req.now);
            Self::commit_locked(&mut g, &req.shard, env)?;
            // Render the now-leased records into the rich claimed-item shape.
            let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
            let items: Vec<ClaimedItem> = proj.render_claimed(&candidates);
            // Every just-leased candidate must render (lease fields are Some under this lock).
            debug_assert_eq!(items.len(), candidates.len(), "leased candidate failed to render");
            Ok(Claimed { items })
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for MemoryBackend {
    #[allow(clippy::too_many_arguments)]
    fn replace_if_pending(
        &self,
        shard: &ShardKey,
        client_item_key: &ClientItemKey,
        new_item_id: ItemId,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let existing = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.lookup_by_key(client_item_key)
            };
            let max_attempts = g
                .queues
                .get(&shard.queue_key())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            let build_item = |item_id: ItemId| PushItem {
                client_item_key: client_item_key.clone(),
                item_id,
                priority: priority.clone(),
                not_before,
                group_key: group_key.clone(),
                max_attempts,
                payload: payload.clone(),
            };
            match existing {
                None => {
                    // No collision: plain insert.
                    let cmd = QueueCommand::Push(PushCommand {
                        items: vec![build_item(new_item_id.clone())],
                    });
                    let env = self.make_envelope(cmd, vec![new_item_id.clone()], now);
                    Self::commit_locked(&mut g, shard, env)?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                Some(existing_id) => {
                    let state = {
                        let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                        proj.item_state(&existing_id).ok_or(EngineError::NotFound)?
                    };
                    match state {
                        ItemState::Pending => {
                            let cmd = QueueCommand::ReplacePending(ReplacePendingCommand {
                                client_item_key: client_item_key.clone(),
                                superseded_item_id: existing_id.clone(),
                                replacement: build_item(new_item_id.clone()),
                            });
                            let env = self.make_envelope(cmd, vec![new_item_id.clone()], now);
                            Self::commit_locked(&mut g, shard, env)?;
                            Ok(UpsertOutcome::Replaced {
                                new_item_id,
                                superseded_item_id: existing_id,
                            })
                        }
                        // Collision with in-flight work — no lifecycle transition allowed.
                        ItemState::Leased => Err(EngineError::Invalid("collision with claimed item")),
                        // Terminal collision.
                        ItemState::Complete | ItemState::Failed => Err(EngineError::Terminal),
                    }
                }
            }
        })();
        std::future::ready(result)
    }
}

impl PushPort for MemoryBackend {
    fn push(
        &self,
        shard: &ShardKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let max_attempts = g
                .queues
                .get(&shard.queue_key())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            // ONE command-sequence number stamps the command id AND all item ids, so they are unique
            // across handles + restart (cmd_seq is the backend's, not a caller counter).
            let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
            let (push_items, ids) = build_push_items(items, n, "mem", max_attempts);
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("mem-{n}")),
                request_id: None,
                shard_id: ShardId::ZERO,
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            // commit_locked fetches the shard's projection first (NotFound if absent) BEFORE appending,
            // and Push apply is infallible, so the log can never lead the projection.
            Self::commit_locked(&mut g, shard, env)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl FinalizePort for MemoryBackend {
    fn finalize(
        &self,
        shard: &ShardKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            // Pre-commit validation so apply_command(Finalize) is infallible (commit has no rollback):
            // each item must be Leased and not fenced, else reject WITHOUT appending.
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.finalize_validate(&outcomes)?;
            }
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id.clone()).collect();
            let cmd = QueueCommand::Finalize(FinalizeCommand { outcomes });
            let env = self.make_envelope(cmd, item_ids, now);
            Self::commit_locked(&mut g, shard, env)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl ReclaimDriver for MemoryBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            // Collect expired leases per shard (read), then reclaim them (write) — no client traffic
            // required, closing the orphan-on-quiet-queue gap (TD-007 §3).
            let expired: Vec<(ShardKey, Vec<ItemId>)> = g
                .projections
                .iter()
                .filter_map(|(shard, proj)| {
                    let ids = proj.expired_leases(now);
                    (!ids.is_empty()).then(|| (shard.clone(), ids))
                })
                .collect();
            let mut report = TickReport::default();
            for (shard, ids) in expired {
                let cmd = QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: ids.clone(),
                });
                let env = self.make_envelope(cmd, ids.clone(), now);
                Self::commit_locked(&mut g, &shard, env)?;
                report.leases_reclaimed += ids.len() as u64;
            }
            // Cohort-timeout firing and progress-bound metering need state not yet modeled; reported as
            // zero rather than faked (plan §3, TD-007 §3 D2 meter-only).
            Ok(report)
        })();
        std::future::ready(result)
    }
}

impl Backend for MemoryBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        // Whole UoW under one lock; the closure is synchronous (no await), so the !Send guard never
        // crosses an await point and the returned future is Send.
        let result = {
            let mut guard = self.state.lock().expect("memory backend poisoned");
            let State {
                logs, projections, ..
            } = &mut *guard;
            let mut lw = LogWriterView { logs };
            let mut pw = ProjectionWriterView { projections };
            f(&mut lw, &mut pw)
        };
        std::future::ready(result)
    }
}

impl ControlPlaneStore for MemoryBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            if let Some(existing) = g.queues.get(&key) {
                // Idempotent create: compatible iff the placement-identity fields match (API-001).
                if existing.group_co_residency != definition.group_co_residency
                    || existing.shard_count != definition.shard_count
                {
                    return Err(EngineError::QueueDefinitionConflict);
                }
                return Ok(CreateQueueOutcome {
                    created: false,
                    definition: existing.clone(),
                });
            }
            let shard = Self::launch_shard(&key);
            g.logs.entry(shard.clone()).or_default();
            g.projections
                .entry(shard)
                .or_insert_with(|| ProjectionData::new(definition.priority_model));
            g.queues.insert(key, definition.clone());
            Ok(CreateQueueOutcome {
                created: true,
                definition,
            })
        })();
        std::future::ready(result)
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .state
            .lock()
            .expect("poisoned")
            .queues
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result: Vec<QueueId> = self
            .state
            .lock()
            .expect("poisoned")
            .queues
            .keys()
            .filter(|k| k.tenant_id.as_str() == tenant.as_str())
            .map(|k| k.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn current_epoch(
        &self,
        shard: &ShardKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = Ok(self
            .state
            .lock()
            .expect("poisoned")
            .logs
            .get(shard)
            .map(|l| l.epoch())
            .unwrap_or(0));
        std::future::ready(result)
    }
}

impl LogRead for MemoryBackend {
    fn read_from(
        &self,
        shard: &ShardKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let log = g.logs.get(shard).ok_or(EngineError::NotFound)?;
            Ok(log.read_from(shard, from, limit))
        })();
        std::future::ready(result)
    }
}

impl ProjectionRead for MemoryBackend {
    fn select_eligible(
        &self,
        shard: &ShardKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.select_eligible(now, limit))
        })();
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &ShardKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.peek(limit))
        })();
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &ShardKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.pending_leases())
        })();
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let shard = MemoryBackend::launch_shard(queue);
            let proj = g.projections.get(&shard).ok_or(EngineError::NotFound)?;
            Ok(proj.metrics())
        })();
        std::future::ready(result)
    }
}

impl SnapshotStore for MemoryBackend {
    fn write_snapshot(
        &self,
        shard: &ShardKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let log = g.logs.get_mut(shard).ok_or(EngineError::NotFound)?;
            Ok(log.write_snapshot(shard, position, snapshot))
        })();
        std::future::ready(result)
    }

    fn latest_snapshot(
        &self,
        shard: &ShardKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let result = Ok(self
            .state
            .lock()
            .expect("poisoned")
            .logs
            .get(shard)
            .and_then(|l| l.latest_snapshot()));
        std::future::ready(result)
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let log = g
                .logs
                .get(&snapshot_ref.shard_key)
                .ok_or(EngineError::NotFound)?;
            log.read_snapshot(snapshot_ref)
        })();
        std::future::ready(result)
    }

    fn high_water(
        &self,
        shard: &ShardKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let result = Ok(self
            .state
            .lock()
            .expect("poisoned")
            .logs
            .get(shard)
            .and_then(|l| l.high_water()));
        std::future::ready(result)
    }

    fn set_high_water(
        &self,
        shard: &ShardKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let log = g.logs.get_mut(shard).ok_or(EngineError::NotFound)?;
            log.set_high_water(position)
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// Injected utilities: a controllable clock and a sequential id generator
// ---------------------------------------------------------------------------

/// A clock you set explicitly — keeps reclaim/lease tests deterministic.
pub struct ManualClock {
    seconds: AtomicI64,
}

impl ManualClock {
    pub fn at(seconds: i64) -> Self {
        Self {
            seconds: AtomicI64::new(seconds),
        }
    }

    pub fn set(&self, seconds: i64) {
        self.seconds.store(seconds, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::new(self.seconds.load(Ordering::SeqCst), 0).expect("valid timestamp")
    }
}

/// Sequential id generation.
pub struct SeqIdGen {
    counter: AtomicU64,
}

impl Default for SeqIdGen {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl IdGen for SeqIdGen {
    fn next_item_id(&self) -> ItemId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        ItemId::new(format!("item-{n}")).expect("valid id")
    }

    fn next_command_id(&self) -> CommandId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        CommandId::new(format!("cmd-{n}"))
    }
}

#[cfg(test)]
mod tests;
