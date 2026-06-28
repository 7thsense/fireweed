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
    BodyHash, ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityValue,
    QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    AdvanceInstanceFenceCommand, ClaimCompatibility, CommitCapabilities, CommitEntryOutcome,
    CommitEntryStatus, CommitRecovery, CommitTransition, CommitTransitionPort, EntryRecovery,
    IdempotencyDecision, IndexHit, IndexQueryPort, PayloadUpdate, PurgeItemsCommand, PurgePort,
    PushPort, PushSpec, QueueCounters, QueueIdempotencyCache, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimPort, RecoveryReadPort, RenewLeaseCommand, RenewLeasePort,
    UpdateFieldsCommand, UpdateFieldsPort, WriteSideRecordsCommand, build_push_items,
    require_item_level_claim, validate_gate_command, validate_gate_push, validate_instance_fence,
    validate_purge_force,
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
        for env in commands {
            validate_gate_command(false, &env.command)?;
        }
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
    /// Per-queue retained request-id idempotency cache (TD-007 §4). Kept under the SAME `State` lock as
    /// the log/projection so a request-id'd write does check + append + record in one atomic unit of work.
    /// The cached outcome is the response ids the original request produced, so a replay returns them
    /// verbatim without re-appending. Empty for the default (request-id-less) push path.
    idempotency: HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>,
    /// Per-queue retained request-id cache for the vectorized claimed-work COMMIT path (epic
    /// pqueue-2201fd37). Same `QueueIdempotencyCache` machinery as `idempotency`, but the cached outcome is
    /// the whole `Vec<CommitEntryOutcome>` so a body+request_id replay returns the prior per-entry outcomes
    /// verbatim with NO double-write. Held under the same `State` lock so check + append + record is atomic.
    commit_idempotency: HashMap<QueueKey, QueueIdempotencyCache<Vec<EntryRecovery>>>,
}

/// Project the retained per-entry recovery records into the public per-entry outcomes (the commit return /
/// replay value). The recovery record is the superset (it ALSO carries the consumed input id, instance fence,
/// and side-record keys for `explain_commit`).
fn outcomes_from_recovery(recovery: &[EntryRecovery]) -> Vec<CommitEntryOutcome> {
    recovery
        .iter()
        .map(|r| match &r.status {
            CommitEntryStatus::Committed => CommitEntryOutcome::Committed {
                lifecycle_item_ids: r.lifecycle_item_ids.clone(),
            },
            CommitEntryStatus::Rejected(e) => CommitEntryOutcome::Rejected(e.clone()),
        })
        .collect()
}

/// Stable body fingerprint for request-id conflict detection: a non-cryptographic hash over the
/// serialized push specs. A different body under the same request id is a `RequestIdConflict`; an equal
/// body replays. (Memory is the reference backend; the durable relational backend uses SHA-256 over the
/// same serialization — both only need determinism + collision-safety, not cryptographic strength.)
fn push_body_hash(items: &[PushSpec]) -> EngineResult<BodyHash> {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

/// Stable body fingerprint for the vectorized commit path: a non-cryptographic hash over the serialized
/// commit entries (the request_id is the cache KEY, not part of the body). A different body under the same
/// request id is a `RequestIdConflict`; an equal body replays the prior per-entry outcomes.
fn commit_body_hash(entries: &[pqueue_engine::CommitTransitionEntry]) -> EngineResult<BodyHash> {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(entries).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

/// `now + retention_ms` as the idempotency entry expiry.
fn request_expires_at(now: UtcTimestamp, retention_ms: u64) -> UtcTimestamp {
    let total = now.seconds as i128 * 1_000_000_000
        + now.nanoseconds as i128
        + retention_ms as i128 * 1_000_000;
    UtcTimestamp::new(
        total.div_euclid(1_000_000_000) as i64,
        total.rem_euclid(1_000_000_000) as u32,
    )
    .expect("valid ts")
}

/// In-memory atomic-class backend. One `Mutex<State>`; `write` takes the lock for the whole unit of
/// work, so append + apply commit together (TD-007 §1 atomic class).
pub struct MemoryBackend {
    state: Mutex<State>,
    cmd_seq: AtomicU64,
    /// This instance's node id, packed into every minted [`ItemId`] (ADR-009) so concurrent writers never
    /// collide. `0` for the default single-instance backend.
    node_id: u8,
    /// Per-(queue, epoch) item-id sequence — see [`QueueCounters`].
    counters: QueueCounters,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            cmd_seq: AtomicU64::new(0),
            node_id: 0,
            counters: QueueCounters::default(),
        }
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Tag this backend with `node_id` — the value packed into the disambiguation byte of every minted
    /// [`ItemId`]. Distinct nodes competing for the same queue MUST pass distinct ids.
    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
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
        validate_gate_command(false, &env.command)?;
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
            // Resolve the claim unit from the compatibility options. Item-level (the default) is unchanged;
            // this backend refuses richer claim units with `Unavailable` rather than silently downgrading
            // them to item-level delivery. The item-level hot path skips this entirely (byte-identical).
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
            Ok(Claimed {
                items,
                ..Default::default()
            })
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
        metadata: Metadata,
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
            // The command id stays a backend-local sequence; the item id is minted from
            // (epoch, node, per-queue counter) like a push so it never collides across writers (ADR-009).
            let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, 1);
            let new_item_id = ItemId::mint(epoch, self.node_id, counter_base);
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id,
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
                fields,
                metadata,
                cohort_size: None,
                gate_keys: Vec::new(),
            };
            let mk = |command: QueueCommand| CommandEnvelope {
                command_id: CommandId::new(format!("mem-{}-{n}", self.node_id)),
                request_id: None,
                item_ids: vec![new_item_id],
                command,
                checksum: CommandChecksum(0),
                created_at: now,
            };
            match existing {
                None => {
                    // Pre-commit unique-index validation (ADR-010 §5.1): a violating insert appends nothing.
                    {
                        let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                        proj.index_validate(&item.item_id, &item.fields, None)?;
                    }
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
                            // The superseded item is removed in the same command, so it does not conflict.
                            {
                                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                                proj.index_validate_replace(&existing_id, &item)?;
                            }
                            let env = mk(QueueCommand::ReplacePending(ReplacePendingCommand {
                                client_item_key: client_item_key.clone(),
                                superseded_item_id: existing_id,
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
            validate_gate_push(self.supports_gates(), &items)?;
            let mut g = self.state.lock().expect("poisoned");
            let max_attempts = g
                .queues
                .get(&shard.clone())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            // The command id stays a backend-local sequence; the ITEM ids are minted from
            // (epoch, node, per-queue counter) so concurrent writers to one queue never collide (ADR-009).
            let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            // Pre-commit unique-index validation (ADR-010 §5.1): a violating push appends nothing.
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.index_validate_push(&push_items)?;
            }
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("mem-{}-{n}", self.node_id)),
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

    /// Request-id'd push with retained replay/conflict/expired semantics (API-001 / TD-007 §4). Unlike the
    /// default trait impl (which refuses with `Unavailable`), the memory reference backend wires a per-queue
    /// [`QueueIdempotencyCache`]: a retried body under the same `request_id` REPLAYS the original ids without
    /// a second append, a different body under that id is `RequestIdConflict`, and a retry after the
    /// queue's `request_id_retention_ms` window is treated as a genuinely new request (push semantics —
    /// the prior leases/ids are gone). The caller's `request_id` propagates into the committed
    /// [`CommandEnvelope`] (no longer hardcoded `request_id: None`), so the durable log records it.
    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let fingerprint = push_body_hash(&items)?;
            let mut g = self.state.lock().expect("poisoned");
            let def = g.queues.get(shard).ok_or(EngineError::NotFound)?;
            let max_attempts = def.retry_policy.max_attempts;
            let expires_at = request_expires_at(now, def.request_id_retention_ms);
            // Check the retained cache FIRST (still under the State lock, so check+append+record is atomic).
            match g.idempotency.entry(shard.clone()).or_default().check(
                &request_id,
                fingerprint,
                now,
            ) {
                // A live record with the same body — replay the original response ids, append nothing.
                IdempotencyDecision::Replay(ids) => return Ok(ids),
                // Same request id, different body — structural conflict (API-001 request-id-conflict).
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                // No record, or the retention window elapsed: proceed as a fresh push (push treats an
                // expired entry as a genuinely new logical request, per the module mapping in `idempotency`).
                IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
            }
            // The ITEM ids are minted from (epoch, node, per-queue counter) so concurrent writers never
            // collide (ADR-009), exactly as the request-id-less push path does.
            let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.index_validate_push(&push_items)?;
            }
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("mem-{}-{n}", self.node_id)),
                // Propagate the caller's request id into the durable envelope (no longer `request_id: None`).
                request_id: Some(request_id.clone()),
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            // Record the outcome only AFTER a successful commit, so a rejected append leaves no replay entry.
            g.idempotency.entry(shard.clone()).or_default().record(
                request_id,
                fingerprint,
                ids.clone(),
                expires_at,
            );
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
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            let cmd = QueueCommand::Finalize(FinalizeCommand { outcomes });
            let env = self.make_envelope(cmd, item_ids, now);
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl CommitTransitionPort for MemoryBackend {
    /// Authoritative vectorized claimed-work commit (Snorri StateStore boundary, epic pqueue-2201fd37). The
    /// whole operation runs under ONE `State` lock so request-id check + per-entry validate + append + apply
    /// + record is a single atomic unit of work.
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommitEntryOutcome>>> + Send {
        let result = (|| {
            let CommitTransition {
                request_id,
                entries,
            } = transition;
            let fingerprint = commit_body_hash(&entries)?;
            let mut g = self.state.lock().expect("poisoned");
            let (max_attempts, retention) = {
                let def = g.queues.get(shard).ok_or(EngineError::NotFound)?;
                (def.retry_policy.max_attempts, def.request_id_retention_ms)
            };

            // (1) Request-id idempotency over the WHOLE commit body (same machinery as the push path). A
            //     retained body+id REPLAYS the prior per-entry outcomes (no re-write); a different body under
            //     that id is `RequestIdConflict`; an expired/absent entry proceeds fresh.
            if let Some(rid) = &request_id {
                match g
                    .commit_idempotency
                    .entry(shard.clone())
                    .or_default()
                    .check(rid, fingerprint, now)
                {
                    IdempotencyDecision::Replay(recovery) => {
                        return Ok(outcomes_from_recovery(&recovery));
                    }
                    IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                    IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
                }
            }

            // (2) Per entry: validate the lease-token + version-fenced claim_ref AND the optional instance
            //     fence, then commit the entry's side-records + fence advance + lifecycle push + input finalize
            //     atomically. A rejected entry mutates nothing. Each entry's `EntryRecovery` (the superset of
            //     its outcome) is retained so `explain_commit` can reconstruct the transition.
            let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
            for entry in entries {
                let claim_ref = entry.claim_ref;
                let consumed_input_id = claim_ref.item_id;
                let reject = |e: EngineError| EntryRecovery {
                    consumed_input_id,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                };

                if let Err(e) = {
                    let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                    proj.commit_validate(std::slice::from_ref(&claim_ref), now)
                } {
                    recovery.push(reject(e));
                    continue;
                }

                // C6: validate the caller-supplied instance fence against the stored fence (absent == 0).
                // A stale `expected` -> Conflict, a non-monotonic `next` -> Invalid; NOTHING is written.
                if let Some(fence) = &entry.instance_fence {
                    let stored = {
                        let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                        proj.instance_fence(&fence.instance_key).unwrap_or(0)
                    };
                    if let Err(e) = validate_instance_fence(stored, fence) {
                        recovery.push(reject(e));
                        continue;
                    }
                }

                // Capture the recovery facts BEFORE moving the entry's records into commands.
                let side_record_keys: Vec<Vec<u8>> =
                    entry.side_records.iter().map(|r| r.key.clone()).collect();
                let instance = entry
                    .instance_fence
                    .as_ref()
                    .map(|f| (f.instance_key.clone(), f.next));

                // Build the entry's envelopes WITHOUT committing yet, so a build-time rejection (e.g. a unique
                // -index conflict on a lifecycle item) leaves nothing mutated. The caller's request_id
                // propagates into every envelope (no `request_id: None` on this path).
                let mk = |command: QueueCommand, item_ids: Vec<ItemId>| {
                    let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
                    CommandEnvelope {
                        command_id: CommandId::new(format!("mem-{}-{n}", self.node_id)),
                        request_id: request_id.clone(),
                        item_ids,
                        command,
                        checksum: CommandChecksum(0),
                        created_at: now,
                    }
                };
                let mut envelopes: Vec<CommandEnvelope> = Vec::new();
                if !entry.side_records.is_empty() {
                    envelopes.push(mk(
                        QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                            records: entry.side_records,
                        }),
                        Vec::new(),
                    ));
                }
                if let Some(fence) = entry.instance_fence {
                    envelopes.push(mk(
                        QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                            instance_key: fence.instance_key,
                            expected: fence.expected,
                            next: fence.next,
                        }),
                        Vec::new(),
                    ));
                }
                let mut lifecycle_item_ids = Vec::new();
                if !entry.lifecycle_items.is_empty() {
                    let epoch = expected_epoch.unwrap_or(0);
                    let counter_base =
                        self.counters
                            .reserve(shard, epoch, entry.lifecycle_items.len() as u32);
                    let (push_items, ids) = build_push_items(
                        entry.lifecycle_items,
                        epoch,
                        self.node_id,
                        counter_base,
                        max_attempts,
                    );
                    if let Err(e) = {
                        let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                        proj.index_validate_push(&push_items)
                    } {
                        recovery.push(reject(e));
                        continue;
                    }
                    lifecycle_item_ids = ids.clone();
                    envelopes.push(mk(
                        QueueCommand::Push(PushCommand { items: push_items }),
                        ids,
                    ));
                }
                envelopes.push(mk(
                    QueueCommand::Finalize(FinalizeCommand {
                        outcomes: vec![FinalizeOutcome::new(claim_ref.item_id, entry.finalize)],
                    }),
                    vec![claim_ref.item_id],
                ));

                // Commit the entry's envelopes under the held lock. The epoch cannot change while we hold the
                // lock, so either the first append fences (EpochFenced, before any mutation) or all of the
                // entry's appends commit — each entry's writes are atomic.
                for env in envelopes {
                    Self::commit_locked(&mut g, shard, env, expected_epoch)?;
                }
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    instance,
                    side_record_keys,
                    lifecycle_item_ids,
                    status: CommitEntryStatus::Committed,
                });
            }

            // (3) Record the whole-body recovery only AFTER success, so a later replay/explain returns it
            //     verbatim with no second append.
            let outcomes = outcomes_from_recovery(&recovery);
            if let Some(rid) = request_id {
                let expires_at = request_expires_at(now, retention);
                g.commit_idempotency
                    .entry(shard.clone())
                    .or_default()
                    .record(rid, fingerprint, recovery, expires_at);
            }
            Ok(outcomes)
        })();
        std::future::ready(result)
    }
}

impl RecoveryReadPort for MemoryBackend {
    /// Reconstruct a committed transition from the retained commit idempotency record (epic
    /// pqueue-2201fd37 acceptance #5). The retained `Vec<EntryRecovery>` already holds every field; we only
    /// re-attach the `request_id`. `Ok(None)` when nothing is retained under that id (never committed, or
    /// compacted away).
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        let g = self.state.lock().expect("poisoned");
        let found = g
            .commit_idempotency
            .get(shard)
            .and_then(|c| c.peek(&request_id))
            .map(|entries| CommitRecovery {
                request_id,
                entries,
            });
        std::future::ready(Ok(found))
    }

    /// Read an opaque non-work side record by key (recovery/audit read). Disjoint from the work-item
    /// projection, so it never reflects claimable work and survives input finalization.
    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let g = self.state.lock().expect("poisoned");
        let found = g
            .projections
            .get(shard)
            .and_then(|proj| proj.side_record(key).cloned());
        std::future::ready(Ok(found))
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
                        present.push(*id);
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

impl UpdateFieldsPort for MemoryBackend {
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.update_fields_validate(&item_id, expected_item_version)?;
                // Pre-commit unique-index validation (ADR-010 §5.1): a violating update appends nothing.
                proj.index_validate_update(&item_id, &field_ops)?;
            }
            let cmd = QueueCommand::UpdateFields(UpdateFieldsCommand {
                item_id,
                field_ops,
                payload,
            });
            let env = self.make_envelope(cmd, vec![item_id], now);
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            // Read the bumped version back from the just-applied projection.
            g.projections
                .get(shard)
                .and_then(|p| p.item_version(&item_id))
                .ok_or(EngineError::NotFound)
        })();
        std::future::ready(result)
    }
}

impl ReclaimPort for MemoryBackend {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let mut ids = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.expired_leases(now)
            };
            if let Some(limit) = limit {
                ids.truncate(limit);
            }
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // Per-queue and FENCED (unlike the global ReclaimDriver::tick, which passes None).
            let cmd = QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: ids.clone(),
            });
            let env = self.make_envelope(cmd, ids.clone(), now);
            Self::commit_locked(&mut g, shard, env, expected_epoch)?;
            Ok(ids)
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

    /// Authoritative-commit capabilities (epic pqueue-2201fd37). The atomic in-memory reference backend
    /// implements the full vectorized claimed-work commit boundary: atomic per-entry transition, vectorized
    /// commit, lease-token + version + lease-expiry validation, retained whole-body request-id idempotency,
    /// opaque non-work side records, and authoritative recovery/explain reads. Delayed/timer lifecycle work is
    /// supported (`not_before` on lifecycle items). The boundary is `Atomic`.
    fn commit_capabilities(&self) -> CommitCapabilities {
        CommitCapabilities {
            atomic_transition_commit: true,
            vectorized_commit: true,
            lease_validation: true,
            retained_commit_idempotency: true,
            non_work_side_records: true,
            authoritative_recovery_reads: true,
            delayed_awaits_timers: true,
            durability_class: DurabilityClass::Atomic,
            consistency: "atomic append+apply under one in-memory lock",
        }
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
            g.projections.entry(shard).or_insert_with(|| {
                ProjectionData::new(definition.priority_model, &definition.secondary_indexes)
            });
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

impl IndexQueryPort for MemoryBackend {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            proj.index_get_unique(index, key)
        })();
        std::future::ready(result)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            proj.index_lookup(index, key)
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
        ItemId::from_u64(n)
    }

    fn next_command_id(&self) -> CommandId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        CommandId::new(format!("cmd-{n}"))
    }
}

#[cfg(test)]
mod tests;
