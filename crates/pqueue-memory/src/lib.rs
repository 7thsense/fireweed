#![forbid(unsafe_code)]
//! # pqueue-memory
//!
//! In-memory reference backend (atomic durability class). It is a thin **persistence wrapper** over the
//! shared projection state machine in [`pqueue_projection`]: one `Mutex<State>` holding a `LogData` +
//! `ProjectionData` per shard, with `write` taking the lock for the whole unit of work so append +
//! apply commit together (TD-007 §1 atomic class). All apply/eligibility/lease/metrics logic lives in
//! `pqueue-projection` and is shared with the durable backends; this crate only locks and delegates.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, PriorityValue, QueueDefinition,
    QueueId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimPort, ClaimRequest, Claimed, ClaimedItem, Clock, CommandChecksum,
    CommandEnvelope, CommandId, CommandPage, CommandPosition, ControlPlaneStore,
    CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeCommand,
    FinalizeOutcome, FinalizePort, IdGen, ItemView, LeaseExpiredCommand, LeaseView, LiveItemView,
    LogRead, LogWriter, ProjectionRead, ProjectionSnapshot, ProjectionWriter, PushCommand,
    PushItem, QueueCommand, QueueKey, QueueMetrics, ReclaimDriver, ReplacePendingCommand,
    SnapshotRef, SnapshotStore, TickReport, UpsertOutcome, UpsertPort,
};
use pqueue_engine::{
    ClaimCompatibility, PurgeItemsCommand, PurgePort, PushPort, PushSpec, ReassignLeaseCommand,
    ReassignLeasePort, RenewLeaseCommand, RenewLeasePort, build_push_items,
    require_item_level_claim, validate_purge_force,
};
use pqueue_projection::{LogData, ProjectionData, commit};

// ---------------------------------------------------------------------------
// UoW writer views (disjoint borrows of logs / projections — review M2)
// ---------------------------------------------------------------------------

struct LogWriterView<'a> {
    logs: &'a mut HashMap<QueueKey, LogData>,
}

impl LogWriter for LogWriterView<'_> {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        self.logs
            .get_mut(shard)
            .ok_or(EngineError::NotFound)?
            .append(shard, commands, expected_epoch)
    }
}

struct ProjectionWriterView<'a> {
    projections: &'a mut HashMap<QueueKey, ProjectionData>,
}

impl ProjectionWriter for ProjectionWriterView<'_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, cmd) in positions.iter().zip(commands) {
            self.projections
                .get_mut(&pos.queue)
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
    logs: HashMap<QueueKey, LogData>,
    projections: HashMap<QueueKey, ProjectionData>,
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
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    /// Append `env` to the shard log and apply it to the projection under the already-held lock — the
    /// atomic append+apply unit of work the claim/upsert/reclaim ports rely on (shared `commit`).
    fn commit_locked(
        state: &mut State,
        shard: &QueueKey,
        env: CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        let State {
            logs, projections, ..
        } = state;
        let log = logs.get_mut(shard).ok_or(EngineError::NotFound)?;
        let proj = projections.get_mut(shard).ok_or(EngineError::NotFound)?;
        commit(log, proj, shard, env, expected_epoch)
    }
}

impl ClaimPort for MemoryBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            // BQ-14a: resolve the claim unit from the compatibility options. Item-level (the default) is
            // unchanged; a group/cohort/same-group unit is refused with `Unavailable` until its selection
            // lands (BQ-14b/c). The item-level hot path skips this entirely (byte-identical).
            if req.compatibility != ClaimCompatibility::default() {
                let def = g.queues.get(&req.shard).ok_or(EngineError::NotFound)?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, def)?;
            }
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
            Self::commit_locked(&mut g, &req.shard, env, req.expected_epoch)?;
            // Render the now-leased records into the rich claimed-item shape.
            let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
            let items: Vec<ClaimedItem> = proj.render_claimed(&candidates);
            // Every just-leased candidate must render (lease fields are Some under this lock).
            debug_assert_eq!(
                items.len(),
                candidates.len(),
                "leased candidate failed to render"
            );
            Ok(Claimed { items })
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for MemoryBackend {
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: BTreeMap<String, Bytes>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let existing = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.lookup_by_key(client_item_key)
            };
            let max_attempts = g
                .queues
                .get(&shard.clone())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            // ONE command-sequence number stamps both the command id and the assigned item id
            // (restart-safe, unique across handles — callers never supply an id).
            let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
            let new_item_id = ItemId::new(format!("mem-{n}-0")).expect("id");
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id.clone(),
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
                fields,
                cohort_size: None,
                gate_keys: Vec::new(),
            };
            let mk = |command: QueueCommand| CommandEnvelope {
                command_id: CommandId::new(format!("mem-{n}")),
                request_id: None,
                item_ids: vec![new_item_id.clone()],
                command,
                checksum: CommandChecksum(0),
                created_at: now,
            };
            match existing {
                None => {
                    let env = mk(QueueCommand::Push(PushCommand { items: vec![item] }));
                    Self::commit_locked(&mut g, shard, env, expected_epoch)?;
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
                            let env = mk(QueueCommand::ReplacePending(ReplacePendingCommand {
                                client_item_key: client_item_key.clone(),
                                superseded_item_id: existing_id.clone(),
                                replacement: item,
                            }));
                            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
                            Ok(UpsertOutcome::Replaced {
                                new_item_id,
                                superseded_item_id: existing_id,
                            })
                        }
                        // Collision with in-flight work — no lifecycle transition allowed.
                        ItemState::Leased => {
                            Err(EngineError::Invalid("collision with claimed item"))
                        }
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
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let max_attempts = g
                .queues
                .get(&shard.clone())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            // ONE command-sequence number stamps the command id AND all item ids, so they are unique
            // across handles + restart (cmd_seq is the backend's, not a caller counter).
            let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
            let (push_items, ids) = build_push_items(items, n, "mem", max_attempts);
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("mem-{n}")),
                request_id: None,
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            // commit_locked fetches the shard's projection first (NotFound if absent) BEFORE appending,
            // and Push apply is infallible, so the log can never lead the projection.
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl FinalizePort for MemoryBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
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
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl RenewLeasePort for MemoryBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.renew_validate(&item_ids)?;
            }
            let cmd = QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: item_ids.clone(),
                lease_expires_at: new_lease_expires_at,
            });
            let env = self.make_envelope(cmd, item_ids, now);
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl ReassignLeasePort for MemoryBackend {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.reassign_validate(&item_ids)?;
            }
            let cmd = QueueCommand::ReassignLease(ReassignLeaseCommand {
                item_ids: item_ids.clone(),
                lease_token: new_lease_token,
                lease_expires_at: new_lease_expires_at,
            });
            let env = self.make_envelope(cmd, item_ids, now);
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl PurgePort for MemoryBackend {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            // Pre-commit: enforce the force gate per id (a leased item needs force) and collect the ids
            // actually present (absent ids are no-ops, like Redis XDEL). Validation precedes the append.
            let present: Vec<ItemId> = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                let mut present = Vec::new();
                for id in &item_ids {
                    // De-dup: a repeated id removes once and counts once (Redis XDEL semantics; the
                    // apply arm's second `remove` would be a no-op but `present.len()` would over-count).
                    if present.contains(id) {
                        continue;
                    }
                    if let Some(state) = proj.item_state(id) {
                        validate_purge_force(state == ItemState::Leased, force)?;
                        present.push(id.clone());
                    }
                }
                present
            };
            if present.is_empty() {
                return Ok(0);
            }
            let count = present.len() as u64;
            let cmd = QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: present.clone(),
                force,
            });
            let env = self.make_envelope(cmd, present, now);
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(count)
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
            let expired: Vec<(QueueKey, Vec<ItemId>)> = g
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
                Self::commit_locked(&mut g, &shard, env, None)?;
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
                if existing.ordering_mode != definition.ordering_mode
                    || existing.priority_model != definition.priority_model
                {
                    return Err(EngineError::QueueDefinitionConflict);
                }
                return Ok(CreateQueueOutcome {
                    created: false,
                    definition: existing.clone(),
                });
            }
            let shard = key.clone();
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
        shard: &QueueKey,
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

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = {
            let mut g = self.state.lock().expect("poisoned");
            g.logs
                .get_mut(shard)
                .map(|l| l.advance_epoch())
                .ok_or(EngineError::NotFound)
        };
        std::future::ready(result)
    }
}

impl LogRead for MemoryBackend {
    fn read_from(
        &self,
        shard: &QueueKey,
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
        shard: &QueueKey,
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
        shard: &QueueKey,
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
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.pending_leases())
        })();
        std::future::ready(result)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.render_claimed(ids))
        })();
        std::future::ready(result)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.live_items_by_key(keys))
        })();
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let shard = queue.clone();
            let proj = g.projections.get(&shard).ok_or(EngineError::NotFound)?;
            Ok(proj.metrics())
        })();
        std::future::ready(result)
    }
}

impl SnapshotStore for MemoryBackend {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
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
        shard: &QueueKey,
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
                .get(&snapshot_ref.queue)
                .ok_or(EngineError::NotFound)?;
            log.read_snapshot(snapshot_ref)
        })();
        std::future::ready(result)
    }

    fn high_water(
        &self,
        shard: &QueueKey,
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
        shard: &QueueKey,
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
