//! Shared commit-transition planning and recovery for log/projection compositions.
//!
//! # Decision (fireweed-b6ab5738)
//!
//! `ResponseBarrier::Strict` on LogEngine objectlog compositions means **atomic
//! response-after-apply**: success is returned only after the authoritative object-log
//! append **and** the projection apply have both completed. Under that barrier the product
//! truthfully advertises [`DurabilityClass::Atomic`] and
//! `atomic_transition_commit: true`, and implements [`crate::CommitTransitionPort`] /
//! [`crate::RecoveryReadPort`] (Snorri CONTRACT-003 / historical embedded Strict path).
//!
//! `ResponseBarrier::AsyncProjection` remains eventual-apply: the product advertises
//! [`DurabilityClass::EventualApply`] with `atomic_transition_commit: false`. Vectorized
//! transitions still commit through the authoritative object log and hot projection; only the
//! projection application may lag the response; the log is the durability authority.
//!
//! Separate append then apply (SeparateReplayCommit) is retained for crash-window recovery
//! and fault injection; Strict does not claim a single substrate transaction, only that
//! client-visible success implies both axes have applied and that the transition batch is
//! one atomic unit of work at the response boundary.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::{
    AdvanceInstanceFenceCommand, AsyncControlPlane, AsyncProjectionStore, CommandChecksum,
    CommandEnvelope, CommitCapabilities, CommitEntryOutcome, CommitEntryStatus, CommitOutcomeEntry,
    CommitRecovery, CommitTransition, CommitTransitionEntry, DurabilityClass, EngineError,
    EngineResult, EntryRecovery, FinalizeCommand, FinalizeOutcome, IdGen, IdempotencyDecision,
    InProcessControlPlane, PushCommand, QueueCommand, QueueCounters, QueueIdempotencyCache,
    QueueKey, RequestOutcome, SideRecordPage, WriteSideRecordsCommand, build_push_items,
    commit_body_hash, compile_entity_schema, outcome_entry_from_recovery, outcomes_from_recovery,
    recovery_from_outcome_entry, request_expires_at, stage_unique_push_keys,
    validate_distinct_commit_claims, validate_entity, validate_instance_fence,
};
use bytes::Bytes;
use fireweed_core::{BodyHash, ItemId, RequestId, UtcTimestamp};

/// In-process commit request-id cache (parity with [`crate::AsyncLogReplayBackend`]).
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
#[allow(
    clippy::too_many_arguments,
    reason = "commit planning keeps authority, identity, and idempotency inputs explicit"
)]
pub async fn prepare_commit_transition<P, I>(
    projection: &P,
    control: &InProcessControlPlane,
    ids: &I,
    counters: &QueueCounters,
    node_id: u8,
    commit_idempotency: &CommitIdempotency,
    epoch: u64,
    shard: &QueueKey,
    transition: CommitTransition,
    now: UtcTimestamp,
) -> EngineResult<PreparedCommitTransition>
where
    P: AsyncProjectionStore,
    I: IdGen + ?Sized,
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
        let durable_replay = match AsyncProjectionStore::replay_durable_commit(
            projection,
            shard.clone(),
            rid.clone(),
            fingerprint.0,
            now,
        )
        .await
        {
            Ok(entries) => entries,
            // Turso and other projection-only adapters fail closed at this seam;
            // retained commit replay is the in-process product cache above.
            Err(EngineError::Unavailable) => None,
            Err(error) => return Err(error),
        };
        if let Some(entries) = durable_replay {
            let recovery = entries
                .into_iter()
                .map(recovery_from_outcome_entry)
                .collect::<Vec<_>>();
            // The replay port does not return the original expiry. Re-caching
            // with `now + retention` would extend the durable request window.
            return Ok(PreparedCommitTransition::Replay(outcomes_from_recovery(
                &recovery,
            )));
        }
    }

    let commit_fingerprint = fingerprint.0;
    // fireweed-a355d82b / fireweed-60ca4bfd: always validate only this entry's push delta against
    // durable state. Unique-index queues track staged keys incrementally (O(1) per key) so
    // within-commit cross-entry uniqueness does not re-scan the full staged set each entry.
    let requires_cross_entry_push_validation = definition.requires_cross_entry_push_validation();
    let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
    let mut committed_envelopes: Vec<CommandEnvelope> = Vec::new();
    let mut finalized_in_commit: HashSet<ItemId> = HashSet::new();
    let mut staged_fences: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut staged_unique_keys: HashMap<(String, Vec<u8>), ItemId> = HashMap::new();
    let mut staged_client_keys = HashSet::new();

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
        if let Err(e) = AsyncProjectionStore::commit_validate(
            projection,
            shard.clone(),
            claim_refs.clone(),
            now,
        )
        .await
        {
            recovery.push(reject(e));
            continue;
        }
        if let Some(fence) = &instance_fence {
            let stored = match staged_fences.get(&fence.instance_key) {
                Some(v) => *v,
                None => AsyncProjectionStore::instance_fence(
                    projection,
                    shard.clone(),
                    fence.instance_key.clone(),
                )
                .await?
                .unwrap_or(0),
            };
            if let Err(e) = validate_instance_fence(stored, fence) {
                recovery.push(reject(e));
                continue;
            }
        }

        // fireweed-bf03cbf5: not retained. `side_record_keys` is a pure function of the caller's own
        // `side_records` for this entry — echoing it back in the durable retained outcome cost ~948 B/entry
        // (500-entry batches: up to 781 KB/row) for data the caller already has. See
        // `EntryRecovery::side_record_keys` for the consumer-facing contract.
        let side_record_keys: Vec<Vec<u8>> = Vec::new();
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
        if !lifecycle_items.is_empty() {
            if let Err(error) = crate::validate_push_shape(&definition, &lifecycle_items) {
                recovery.push(reject(error));
                continue;
            }
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
            let entry_client_keys = match candidate_client_keys(&push_items, &staged_client_keys) {
                Ok(keys) => keys,
                Err(error) => {
                    recovery.push(reject(error));
                    continue;
                }
            };
            // fireweed-a355d82b / fireweed-60ca4bfd: validate only this entry's push delta against
            // durable projection. Cross-entry uniqueness for unique-index queues uses staged keys.
            if let Err(e) = AsyncProjectionStore::index_validate_push(
                projection,
                shard.clone(),
                push_items.clone(),
            )
            .await
            {
                recovery.push(reject(e));
                continue;
            }
            if requires_cross_entry_push_validation
                && let Err(e) =
                    stage_unique_push_keys(&definition, &push_items, &mut staged_unique_keys)
            {
                recovery.push(reject(e));
                continue;
            }
            // Only accepted entries reserve keys; a later validation rejection
            // must leave its otherwise available client keys for subsequent entries.
            staged_client_keys.extend(entry_client_keys);
            lifecycle_item_ids = push_ids.clone();
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

/// Check one entry without reserving keys until all its other checks have passed.
pub(crate) fn candidate_client_keys(
    items: &[crate::PushItem],
    staged: &HashSet<fireweed_core::ClientItemKey>,
) -> EngineResult<HashSet<fireweed_core::ClientItemKey>> {
    let mut keys = HashSet::with_capacity(items.len());
    for item in items {
        if staged.contains(&item.client_item_key) || !keys.insert(item.client_item_key.clone()) {
            return Err(EngineError::Conflict);
        }
    }
    Ok(keys)
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
    Commit: FnOnce(crate::RawCommitRequest) -> CommitFut,
    CommitFut: std::future::Future<Output = EngineResult<crate::RawCommitOutcome>>,
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
                commit(crate::RawCommitRequest::new(
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
pub async fn explain_commit_if_authoritative<P>(
    authoritative: bool,
    projection: &P,
    commit_idempotency: &CommitIdempotency,
    shard: &QueueKey,
    request_id: RequestId,
) -> EngineResult<Option<CommitRecovery>>
where
    P: AsyncProjectionStore,
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
        AsyncProjectionStore::read_durable_commit(projection, shard.clone(), request_id.clone())
            .await?;
    Ok(durable.map(|entries| CommitRecovery {
        request_id,
        entries: entries
            .into_iter()
            .map(recovery_from_outcome_entry)
            .collect(),
    }))
}

/// Side-record read (available whenever the projection materializes side records).
pub async fn side_record<P>(
    projection: &P,
    shard: &QueueKey,
    key: &[u8],
) -> EngineResult<Option<Bytes>>
where
    P: AsyncProjectionStore,
{
    AsyncProjectionStore::side_record(projection, shard.clone(), key.to_vec()).await
}

/// Paged, key-ascending scan of opaque side records whose key starts with `prefix` (bead
/// fireweed-6072ff52; available whenever the projection materializes side records).
pub async fn side_records_by_prefix<P>(
    projection: &P,
    shard: &QueueKey,
    prefix: &[u8],
    page_size: usize,
    cursor: Option<Vec<u8>>,
) -> EngineResult<SideRecordPage>
where
    P: AsyncProjectionStore,
{
    AsyncProjectionStore::side_records_by_prefix(
        projection,
        shard.clone(),
        prefix.to_vec(),
        page_size,
        cursor,
    )
    .await
}

#[cfg(test)]
mod tests {
    #![allow(refining_impl_trait)]

    use super::*;
    use crate::{
        ClaimRef, ClaimedItem, CommandPosition, FinalizeKind, PushItem, PushSpec, SeqIdGen,
    };
    use fireweed_conformance::{qdef, ts};
    use fireweed_core::{ClientItemKey, IndexSpec, ItemState, LeaseToken, QueueDefinition};
    use std::future::{Ready, ready};

    #[derive(Default)]
    struct PlanningProjection {
        receipt_expires: Option<UtcTimestamp>,
    }

    impl AsyncProjectionStore for PlanningProjection {
        fn ensure_shard(&self, _: QueueDefinition) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn admit_mutation(&self, _: QueueKey) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn apply_live(
            &self,
            _: Vec<CommandPosition>,
            _: Vec<CommandEnvelope>,
        ) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn apply_recovery(
            &self,
            _: Vec<CommandPosition>,
            _: Vec<CommandEnvelope>,
        ) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn eligible_candidates(
            &self,
            _: QueueKey,
            _: UtcTimestamp,
            _: usize,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn render_claimed(
            &self,
            _: QueueKey,
            _: Vec<ItemId>,
        ) -> Ready<EngineResult<Vec<ClaimedItem>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn item_state(&self, _: QueueKey, _: ItemId) -> Ready<EngineResult<Option<ItemState>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn item_version(&self, _: QueueKey, _: ItemId) -> Ready<EngineResult<Option<u64>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn recovery_high_water(&self, _: QueueKey) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn recover_definitions(&self) -> Ready<EngineResult<Vec<QueueDefinition>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn commit_validate(
            &self,
            _: QueueKey,
            _: Vec<ClaimRef>,
            _: UtcTimestamp,
        ) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }
        fn index_validate_push(
            &self,
            _: QueueKey,
            items: Vec<PushItem>,
        ) -> Ready<EngineResult<()>> {
            ready(
                if items
                    .iter()
                    .any(|item| item.client_item_key.as_str() == "durable")
                {
                    Err(EngineError::Conflict)
                } else {
                    Ok(())
                },
            )
        }
        fn replay_durable_commit(
            &self,
            _: QueueKey,
            _: RequestId,
            _: u64,
            now: UtcTimestamp,
        ) -> Ready<EngineResult<Option<Vec<CommitOutcomeEntry>>>> {
            ready(Ok(self
                .receipt_expires
                .filter(|expiry| now < *expiry)
                .map(|_| Vec::new())))
        }
    }

    fn entry(n: u64, keys: &[&str]) -> CommitTransitionEntry {
        CommitTransitionEntry {
            claim_ref: ClaimRef {
                item_id: ItemId::from_u64(n),
                lease_token: LeaseToken::new("lease").unwrap(),
                lease_expires_at: ts(100),
                item_version: 2,
            },
            additional_claim_refs: vec![],
            finalize: FinalizeKind::Complete,
            side_records: vec![crate::SideRecord {
                key: n.to_be_bytes().to_vec(),
                payload: Bytes::from_static(b"accepted"),
            }],
            lifecycle_items: keys
                .iter()
                .map(|key| PushSpec {
                    client_item_key: Some(ClientItemKey::new(*key).unwrap()),
                    ..Default::default()
                })
                .collect(),
            instance_fence: None,
        }
    }

    #[test]
    fn transition_client_keys_reject_duplicates_without_reserving_rejected_entries() {
        futures::executor::block_on(async {
            for indexed in [false, true] {
                let mut definition = qdef();
                if indexed {
                    definition.secondary_indexes = vec![IndexSpec {
                        name: "unique_external".into(),
                        fields: vec!["external".into()],
                        unique: true,
                    }];
                }
                let shard =
                    QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
                let control = InProcessControlPlane::new();
                AsyncControlPlane::create_queue(&control, definition)
                    .await
                    .unwrap();
                let mut malformed = entry(8, &["malformed"]);
                malformed.lifecycle_items[0].priority =
                    Some(fireweed_core::PriorityValue::Text("wrong-model".into()));
                let mut gated = entry(9, &["gated"]);
                gated.lifecycle_items[0].gate_keys = vec!["disabled-gate".into()];
                let prepared = prepare_commit_transition(
                    &PlanningProjection::default(),
                    &control,
                    &SeqIdGen::default(),
                    &QueueCounters::default(),
                    0,
                    &new_commit_idempotency(),
                    1,
                    &shard,
                    CommitTransition {
                        request_id: None,
                        entries: vec![
                            entry(1, &["shared"]),
                            entry(2, &["fresh", "shared"]),
                            entry(3, &["fresh"]),
                            entry(4, &["duplicate", "duplicate"]),
                            entry(5, &["duplicate"]),
                            entry(6, &["after-rejection", "durable"]),
                            entry(7, &["after-rejection"]),
                            malformed,
                            entry(8, &["after-shape"]),
                            gated,
                            entry(9, &["after-gate"]),
                        ],
                    },
                    ts(1),
                )
                .await
                .unwrap();
                let PreparedCommitTransition::Proceed {
                    envelopes,
                    recovery,
                    ..
                } = prepared
                else {
                    panic!("fresh transition must be planned");
                };
                assert_eq!(
                    recovery
                        .iter()
                        .map(|entry| entry.status.clone())
                        .collect::<Vec<_>>(),
                    vec![
                        CommitEntryStatus::Committed,
                        CommitEntryStatus::Rejected(EngineError::Conflict),
                        CommitEntryStatus::Committed,
                        CommitEntryStatus::Rejected(EngineError::Conflict),
                        CommitEntryStatus::Committed,
                        CommitEntryStatus::Rejected(EngineError::Conflict),
                        CommitEntryStatus::Committed,
                        CommitEntryStatus::Rejected(EngineError::Invalid(
                            "priority does not match queue model"
                        )),
                        CommitEntryStatus::Committed,
                        CommitEntryStatus::Rejected(EngineError::Invalid(
                            "queue does not allow gate keys"
                        )),
                        CommitEntryStatus::Committed,
                    ]
                );
                let mut pushed = Vec::new();
                let mut finalized = Vec::new();
                let mut side_keys = Vec::new();
                for envelope in envelopes {
                    match envelope.command {
                        QueueCommand::Push(command) => pushed.extend(
                            command
                                .items
                                .into_iter()
                                .map(|item| item.client_item_key.as_str().to_owned()),
                        ),
                        QueueCommand::Finalize(command) => finalized
                            .extend(command.outcomes.into_iter().map(|outcome| outcome.item_id)),
                        QueueCommand::WriteSideRecords(command) => {
                            side_keys.extend(command.records.into_iter().map(|record| record.key))
                        }
                        _ => panic!("unexpected transition command"),
                    }
                }
                assert_eq!(
                    pushed,
                    [
                        "shared",
                        "fresh",
                        "duplicate",
                        "after-rejection",
                        "after-shape",
                        "after-gate"
                    ]
                );
                assert_eq!(finalized, [1, 3, 5, 7, 8, 9].map(ItemId::from_u64));
                assert_eq!(
                    side_keys,
                    [1_u64, 3, 5, 7, 8, 9].map(|n| n.to_be_bytes().to_vec())
                );
            }
        });
    }

    #[test]
    fn durable_transition_replay_does_not_extend_original_expiry() {
        futures::executor::block_on(async {
            let mut definition = qdef();
            definition.request_id_retention_ms = 10_000;
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let control = InProcessControlPlane::new();
            AsyncControlPlane::create_queue(&control, definition)
                .await
                .unwrap();
            let projection = PlanningProjection {
                receipt_expires: Some(ts(10)),
            };
            let cache = new_commit_idempotency();
            let ids = SeqIdGen::default();
            let counters = QueueCounters::default();
            let request = || CommitTransition {
                request_id: Some(RequestId::new("original-receipt").unwrap()),
                entries: vec![],
            };
            assert!(matches!(
                prepare_commit_transition(
                    &projection,
                    &control,
                    &ids,
                    &counters,
                    0,
                    &cache,
                    1,
                    &shard,
                    request(),
                    ts(9)
                )
                .await
                .unwrap(),
                PreparedCommitTransition::Replay(_)
            ));
            assert!(matches!(
                prepare_commit_transition(
                    &projection,
                    &control,
                    &ids,
                    &counters,
                    0,
                    &cache,
                    1,
                    &shard,
                    request(),
                    ts(10)
                )
                .await
                .unwrap(),
                PreparedCommitTransition::Proceed { .. }
            ));
        });
    }
}
