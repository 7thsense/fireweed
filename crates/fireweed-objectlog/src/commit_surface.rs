//! Shared Strict / AsyncProjection commit-capability surface for LogEngine objectlog products.
//!
//! # Decision (fireweed-b6ab5738)
//!
//! `ResponseBarrier::Strict` on LogEngine objectlog compositions means **atomic
//! response-after-apply**: success is returned only after the authoritative object-log
//! append **and** the projection apply have both completed. Under that barrier the product
//! truthfully advertises [`DurabilityClass::Atomic`] and
//! `atomic_transition_commit: true`, and implements [`CommitTransitionPort`] /
//! [`RecoveryReadPort`] (Snorri CONTRACT-003 / historical embedded Strict path).
//!
//! `ResponseBarrier::AsyncProjection` remains eventual-apply: the product advertises
//! [`DurabilityClass::EventualApply`] with `atomic_transition_commit: false` and
//! `commit_transition` returns [`EngineError::Unavailable`].
//!
//! Separate append then apply (SeparateReplayCommit) is retained for crash-window recovery
//! and fault injection; Strict does not claim a single substrate transaction, only that
//! client-visible success implies both axes have applied and that the transition batch is
//! one atomic unit of work at the response boundary.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_core::{BodyHash, ItemId, RequestId, UtcTimestamp};
use fireweed_engine::{
    AdvanceInstanceFenceCommand, AsyncControlPlane, CommandChecksum, CommandEnvelope,
    CommitCapabilities, CommitEntryOutcome, CommitEntryStatus, CommitOutcomeEntry, CommitRecovery,
    CommitTransition, CommitTransitionEntry, DurabilityClass, EngineError, EngineResult,
    EntryRecovery, FinalizeCommand, FinalizeOutcome, IdGen, IdempotencyDecision,
    InProcessControlPlane, InProcessProjectionStore, ProjectionStore, PushCommand, PushItem,
    QueueCommand, QueueCounters, QueueIdempotencyCache, QueueKey, RequestOutcome,
    WriteSideRecordsCommand, build_push_items, commit_body_hash, compile_entity_schema,
    outcome_entry_from_recovery, outcomes_from_recovery, recovery_from_outcome_entry,
    request_expires_at, validate_distinct_commit_claims, validate_entity, validate_instance_fence,
};

use crate::async_product::SeqIdGen;

/// In-process commit request-id cache (parity with [`fireweed_engine::AsyncLogReplayBackend`]).
pub type CommitIdempotency =
    Arc<Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<EntryRecovery>>>>>;

pub fn new_commit_idempotency() -> CommitIdempotency {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Capability descriptor for Strict (atomic response-after-apply) objectlog products.
pub fn strict_commit_capabilities(consistency: &'static str) -> CommitCapabilities {
    CommitCapabilities {
        atomic_transition_commit: true,
        vectorized_commit: true,
        lease_validation: true,
        retained_commit_idempotency: true,
        non_work_side_records: true,
        authoritative_recovery_reads: true,
        delayed_awaits_timers: true,
        durability_class: DurabilityClass::Atomic,
        consistency,
    }
}

/// Capability descriptor for AsyncProjection / non-strict objectlog products.
pub fn eventual_commit_capabilities(consistency: &'static str) -> CommitCapabilities {
    CommitCapabilities {
        atomic_transition_commit: false,
        vectorized_commit: true,
        lease_validation: true,
        retained_commit_idempotency: true,
        non_work_side_records: true,
        authoritative_recovery_reads: true,
        delayed_awaits_timers: true,
        durability_class: DurabilityClass::EventualApply,
        consistency,
    }
}

/// Product-level durability for the configured response barrier.
pub fn durability_for_strict(strict: bool) -> DurabilityClass {
    if strict {
        DurabilityClass::Atomic
    } else {
        DurabilityClass::EventualApply
    }
}

/// Result of planning a Strict commit transition (before log append).
pub enum PreparedCommitTransition {
    /// Replay prior outcomes (request_id hit, equal body).
    Replay(Vec<CommitEntryOutcome>),
    /// Fresh batch ready to append+apply; record idempotency after successful submit.
    Proceed {
        envelopes: Vec<CommandEnvelope>,
        recovery: Vec<EntryRecovery>,
        request_id: Option<RequestId>,
        fingerprint: BodyHash,
        retention_ms: u64,
    },
}

/// Plan a Strict `commit_transition`. Caller submits envelopes then calls [`record_commit_idempotency`].
pub async fn prepare_commit_transition<P>(
    projection: &InProcessProjectionStore<P>,
    control: &InProcessControlPlane,
    ids: &SeqIdGen,
    counters: &QueueCounters,
    node_id: u8,
    commit_idempotency: &CommitIdempotency,
    epoch: u64,
    shard: &QueueKey,
    transition: CommitTransition,
    now: UtcTimestamp,
) -> EngineResult<PreparedCommitTransition>
where
    P: ProjectionStore + Send + 'static,
{
    let CommitTransition {
        request_id,
        entries,
    } = transition;
    let fingerprint = commit_body_hash(&entries)?;
    let definition = AsyncControlPlane::queue_definition(control, shard.clone()).await?;
    let max_attempts = definition.retry_policy.max_attempts;
    let retention_ms = definition.request_id_retention_ms;
    let schema = definition
        .entity_schema
        .as_ref()
        .and_then(|esd| esd.entity_schema.as_ref())
        .map(compile_entity_schema)
        .transpose()?;

    if let Some(rid) = &request_id {
        let cached = {
            let cache = commit_idempotency
                .lock()
                .expect("commit idempotency poisoned");
            cache.get(shard).map(|c| c.check(rid, fingerprint, now))
        };
        if let Some(decision) = cached {
            match decision {
                IdempotencyDecision::Replay(recovery) if recovery.len() == entries.len() => {
                    return Ok(PreparedCommitTransition::Replay(outcomes_from_recovery(
                        &recovery,
                    )));
                }
                IdempotencyDecision::Conflict => {
                    return Err(EngineError::RequestIdConflict);
                }
                IdempotencyDecision::Replay(_)
                | IdempotencyDecision::Proceed
                | IdempotencyDecision::Expired => {}
            }
        }
        if let Some(entries) = projection.with_store_mut(|p| {
            ProjectionStore::replay_durable_commit(p, shard, rid, fingerprint.0, now)
        })? {
            let recovery = entries
                .into_iter()
                .map(recovery_from_outcome_entry)
                .collect::<Vec<_>>();
            record_commit_idempotency(
                commit_idempotency,
                shard,
                rid.clone(),
                fingerprint,
                recovery.clone(),
                now,
                retention_ms,
            );
            return Ok(PreparedCommitTransition::Replay(outcomes_from_recovery(
                &recovery,
            )));
        }
    }

    let commit_fingerprint = fingerprint.0;
    let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
    let mut committed_envelopes: Vec<CommandEnvelope> = Vec::new();
    let mut finalized_in_commit: HashSet<ItemId> = HashSet::new();
    let mut staged_fences: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut committed_pushes: Vec<PushItem> = Vec::new();

    for entry in entries {
        let CommitTransitionEntry {
            claim_ref,
            additional_claim_refs,
            finalize,
            side_records,
            lifecycle_items,
            instance_fence,
        } = entry;
        let consumed_input_id = claim_ref.item_id;
        let additional_consumed_input_ids = additional_claim_refs
            .iter()
            .map(|c| c.item_id)
            .collect::<Vec<_>>();
        let mut claim_refs = Vec::with_capacity(1 + additional_claim_refs.len());
        claim_refs.push(claim_ref);
        claim_refs.extend(additional_claim_refs);
        let reject = |e: EngineError| EntryRecovery {
            consumed_input_id,
            additional_consumed_input_ids: additional_consumed_input_ids.clone(),
            instance: None,
            side_record_keys: Vec::new(),
            lifecycle_item_ids: Vec::new(),
            status: CommitEntryStatus::Rejected(e),
        };

        if let Err(error) = validate_distinct_commit_claims(&claim_refs[0], &claim_refs[1..]) {
            recovery.push(reject(error));
            continue;
        }
        if claim_refs
            .iter()
            .any(|c| finalized_in_commit.contains(&c.item_id))
        {
            recovery.push(reject(EngineError::Terminal));
            continue;
        }
        if let Err(e) =
            projection.with_store(|p| ProjectionStore::commit_validate(p, shard, &claim_refs, now))
        {
            recovery.push(reject(e));
            continue;
        }
        if let Some(fence) = &instance_fence {
            let stored = match staged_fences.get(&fence.instance_key) {
                Some(v) => *v,
                None => projection
                    .with_store(|p| ProjectionStore::instance_fence(p, shard, &fence.instance_key))?
                    .unwrap_or(0),
            };
            if let Err(e) = validate_instance_fence(stored, fence) {
                recovery.push(reject(e));
                continue;
            }
        }

        let side_record_keys: Vec<Vec<u8>> = side_records.iter().map(|r| r.key.clone()).collect();
        let instance = instance_fence
            .as_ref()
            .map(|f| (f.instance_key.clone(), f.next));
        let mut envelopes: Vec<CommandEnvelope> = Vec::new();
        let mk_env = |command: QueueCommand, item_ids: Vec<ItemId>| CommandEnvelope {
            command_id: ids.next_command_id(),
            request_id: request_id.clone(),
            request_fingerprint: Some(commit_fingerprint),
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        };

        if !side_records.is_empty() {
            envelopes.push(mk_env(
                QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                    records: side_records,
                }),
                Vec::new(),
            ));
        }
        if let Some(fence) = instance_fence {
            envelopes.push(mk_env(
                QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                    instance_key: fence.instance_key,
                    expected: fence.expected,
                    next: fence.next,
                }),
                Vec::new(),
            ));
        }

        let mut lifecycle_item_ids = Vec::new();
        let mut entry_pushes: Vec<PushItem> = Vec::new();
        if !lifecycle_items.is_empty() {
            if let Some(e) = lifecycle_items
                .iter()
                .find_map(|item| validate_entity(schema.as_ref(), item.entity.as_ref()).err())
            {
                recovery.push(reject(e));
                continue;
            }
            let counter_base = counters.reserve(shard, epoch, lifecycle_items.len() as u32);
            let (push_items, push_ids) =
                build_push_items(lifecycle_items, epoch, node_id, counter_base, max_attempts);
            let mut candidate = committed_pushes.clone();
            candidate.extend(push_items.iter().cloned());
            if let Err(e) = projection
                .with_store(|p| ProjectionStore::index_validate_push(p, shard, &candidate))
            {
                recovery.push(reject(e));
                continue;
            }
            lifecycle_item_ids = push_ids.clone();
            entry_pushes = push_items.clone();
            envelopes.push(mk_env(
                QueueCommand::Push(PushCommand { items: push_items }),
                push_ids,
            ));
        }

        envelopes.push(mk_env(
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: claim_refs
                    .iter()
                    .map(|c| FinalizeOutcome::new(c.item_id, finalize))
                    .collect(),
            }),
            claim_refs.iter().map(|c| c.item_id).collect(),
        ));

        finalized_in_commit.extend(claim_refs.iter().map(|c| c.item_id));
        if let Some((key, next)) = &instance {
            staged_fences.insert(key.clone(), *next);
        }
        committed_pushes.extend(entry_pushes);
        committed_envelopes.append(&mut envelopes);
        recovery.push(EntryRecovery {
            consumed_input_id,
            additional_consumed_input_ids,
            instance,
            side_record_keys,
            lifecycle_item_ids,
            status: CommitEntryStatus::Committed,
        });
    }

    let mut envelopes = committed_envelopes;
    if let Some(rid) = &request_id {
        let outcome_entries: Vec<CommitOutcomeEntry> =
            recovery.iter().map(outcome_entry_from_recovery).collect();
        envelopes.push(CommandEnvelope {
            command_id: ids.next_command_id(),
            request_id: Some(rid.clone()),
            request_fingerprint: Some(commit_fingerprint),
            request_outcome: Some(RequestOutcome::CommitTransition {
                entries: outcome_entries,
            }),
            item_ids: Vec::new(),
            command: QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: Vec::new(),
            }),
            checksum: CommandChecksum(0),
            created_at: now,
        });
    }

    Ok(PreparedCommitTransition::Proceed {
        envelopes,
        recovery,
        request_id,
        fingerprint,
        retention_ms,
    })
}

pub fn record_commit_idempotency(
    commit_idempotency: &CommitIdempotency,
    shard: &QueueKey,
    request_id: RequestId,
    fingerprint: BodyHash,
    recovery: Vec<EntryRecovery>,
    now: UtcTimestamp,
    retention_ms: u64,
) {
    commit_idempotency
        .lock()
        .expect("commit idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .record(
            request_id,
            fingerprint,
            recovery,
            request_expires_at(now, retention_ms),
        );
}

/// Finish a prepared Strict commit: append+apply envelopes via `commit` (must already hold the
/// queue-local admission permit — use `strategy.commit`, not `engine.submit_commit`), then record
/// request-id idempotency.
///
/// # Why the permit must cover prepare + this finish (fireweed-5497780d)
///
/// Instance-fence validation in [`prepare_commit_transition`] reads the projection, then side
/// records and fence advances are applied only after log append. If two concurrent
/// `commit_transition` calls both prepare against the same stored fence and only serialize at
/// `submit_commit`, both pass validation, both append, and `WriteSideRecords` last-writer-wins —
/// a stale candidate can overwrite a newer fence-ordered side record. Holding the same
/// queue-local permit across prepare and append+apply closes that TOCTOU window (parity with
/// claim/push plan+commit and the sync relational single-transaction path).
pub async fn finish_prepared_commit_transition<Commit, CommitFut>(
    shard: &QueueKey,
    epoch: u64,
    prepared: PreparedCommitTransition,
    commit_idempotency: &CommitIdempotency,
    now: UtcTimestamp,
    commit: Commit,
) -> EngineResult<Vec<CommitEntryOutcome>>
where
    Commit: FnOnce(fireweed_engine::RawCommitRequest) -> CommitFut,
    CommitFut: std::future::Future<Output = EngineResult<fireweed_engine::RawCommitOutcome>>,
{
    match prepared {
        PreparedCommitTransition::Replay(outcomes) => Ok(outcomes),
        PreparedCommitTransition::Proceed {
            envelopes,
            recovery,
            request_id,
            fingerprint,
            retention_ms,
        } => {
            if !envelopes.is_empty() {
                commit(fireweed_engine::RawCommitRequest::new(
                    shard.clone(),
                    envelopes,
                    epoch,
                ))
                .await?;
            }
            let outcomes = outcomes_of(&recovery);
            if let Some(rid) = request_id {
                record_commit_idempotency(
                    commit_idempotency,
                    shard,
                    rid,
                    fingerprint,
                    recovery,
                    now,
                    retention_ms,
                );
            }
            Ok(outcomes)
        }
    }
}

/// Map an async composition submit error into a storage engine error.
pub fn map_submit_error(error: impl std::fmt::Debug) -> EngineError {
    EngineError::Storage(format!(
        "async commit_transition submission failed: {error:?}"
    ))
}

pub fn outcomes_of(recovery: &[EntryRecovery]) -> Vec<CommitEntryOutcome> {
    outcomes_from_recovery(recovery)
}

/// `explain_commit` when authoritative; otherwise Unavailable.
pub fn explain_commit_if_authoritative<P>(
    authoritative: bool,
    projection: &InProcessProjectionStore<P>,
    commit_idempotency: &CommitIdempotency,
    shard: &QueueKey,
    request_id: RequestId,
) -> EngineResult<Option<CommitRecovery>>
where
    P: ProjectionStore + Send + 'static,
{
    if !authoritative {
        return Err(EngineError::Unavailable);
    }
    if let Some(recovery) = commit_idempotency
        .lock()
        .expect("commit idempotency poisoned")
        .get(shard)
        .and_then(|c| c.peek(&request_id))
    {
        return Ok(Some(CommitRecovery {
            request_id,
            entries: recovery,
        }));
    }
    let durable =
        projection.with_store(|p| ProjectionStore::read_durable_commit(p, shard, &request_id))?;
    Ok(durable.map(|entries| CommitRecovery {
        request_id,
        entries: entries
            .into_iter()
            .map(recovery_from_outcome_entry)
            .collect(),
    }))
}

/// Side-record read (available whenever the projection materializes side records).
pub fn side_record<P>(
    projection: &InProcessProjectionStore<P>,
    shard: &QueueKey,
    key: &[u8],
) -> EngineResult<Option<Bytes>>
where
    P: ProjectionStore + Send + 'static,
{
    projection.with_store(|p| ProjectionStore::side_record(p, shard, key))
}
