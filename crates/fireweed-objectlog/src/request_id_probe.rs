//! Shared [`RequestIdReplayProbe`] + [`LogRead`] helpers for LogEngine objectlog products.
//!
//! Harness-only mid-pipeline probe for AC-TXN-3 (append→apply window). Used by memory, sqlite,
//! and hybrid products so TP-003 / E3 matrix cells can strike the same durable envelope shapes.

use std::sync::Arc;

use fireweed_core::{BodyHash, RequestId, UtcTimestamp};
use fireweed_engine::{
    AsyncLogStore, ClaimRef, CommandChecksum, CommandEnvelope, CommandPage, CommandPosition,
    CommitEntryStatus, CommitOutcomeEntry, CommitTransitionEntry, ControlPlane, EngineError,
    EngineResult, EntryRecovery, FinalizeCommand, FinalizeKind, FinalizeOutcome, IdGen,
    InProcessControlPlane, InProcessProjectionStore, ProjectionStore, PushCommand, PushSpec,
    QueueCommand, QueueCounters, QueueKey, RequestOutcome, WriteSideRecordsCommand,
    build_push_items, commit_body_hash, compile_entity_schema, outcome_entry_from_recovery,
    push_body_hash, validate_distinct_commit_claims, validate_entity, validate_gate_push,
};

use crate::ObjectLogEngineStore;
use crate::async_product::SeqIdGen;

/// LogRead for any product that holds an [`ObjectLogEngineStore`].
pub(crate) fn read_from_log(
    log: &ObjectLogEngineStore,
    shard: QueueKey,
    from: Option<CommandPosition>,
    limit: usize,
) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send + '_ {
    AsyncLogStore::read_from(log, shard, from, limit)
}

/// Build RequestIdReplayProbe methods against shared product axes.
pub(crate) struct ProbeAxes<'a, P>
where
    P: ProjectionStore + Send + 'static,
{
    pub log: &'a ObjectLogEngineStore,
    pub projection: &'a InProcessProjectionStore<P>,
    pub control: &'a InProcessControlPlane,
    pub ids: &'a SeqIdGen,
    pub counters: &'a QueueCounters,
    pub node_id: u8,
}

impl<P> ProbeAxes<'_, P>
where
    P: ProjectionStore + Send + 'static,
{
    pub fn build_request_id_push_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, Vec<fireweed_core::ItemId>)> {
        let supports_gates = self.projection.with_store(|p| p.supports_gates());
        validate_gate_push(supports_gates, &items)?;
        let fingerprint = push_body_hash(&items)?;
        let def = ControlPlane::queue_definition(self.control, shard)?;
        if items.is_empty() {
            return Err(EngineError::Invalid("push requires at least one item"));
        }
        let schema = def
            .entity_schema
            .as_ref()
            .and_then(|esd| esd.entity_schema.as_ref())
            .map(compile_entity_schema)
            .transpose()?;
        for item in &items {
            validate_entity(schema.as_ref(), item.entity.as_ref())?;
        }
        let max_attempts = def.retry_policy.max_attempts;
        let epoch = expected_epoch.unwrap_or_else(|| {
            crate::block_on_objectlog(AsyncLogStore::current_epoch(self.log, shard.clone()))
                .unwrap_or(0)
        });
        let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
        let (push_items, ids) =
            build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
        self.projection
            .with_store(|p| p.index_validate_push(shard, &push_items))?;
        let env = CommandEnvelope {
            command_id: self.ids.next_command_id(),
            request_id: Some(request_id),
            request_fingerprint: Some(fingerprint.0),
            request_outcome: Some(RequestOutcome::Push {
                item_ids: ids.clone(),
            }),
            item_ids: ids.clone(),
            command: QueueCommand::Push(PushCommand { items: push_items }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        Ok((env, ids))
    }

    pub fn build_request_id_commit_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        claim_ref: ClaimRef,
        finalize: FinalizeKind,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, BodyHash)> {
        let entry = CommitTransitionEntry {
            claim_ref: claim_ref.clone(),
            additional_claim_refs: Vec::new(),
            finalize,
            side_records: Vec::new(),
            lifecycle_items: Vec::new(),
            instance_fence: None,
        };
        let fingerprint = commit_body_hash(std::slice::from_ref(&entry))?;
        let item_id = claim_ref.item_id;
        let _ = expected_epoch;
        let supports = self
            .projection
            .with_store(|p| p.supports_commit_transition());
        if !supports {
            return Err(EngineError::Unavailable);
        }
        self.projection
            .with_store(|p| p.commit_validate(shard, std::slice::from_ref(&claim_ref), now))?;
        let env = CommandEnvelope {
            command_id: self.ids.next_command_id(),
            request_id: Some(request_id),
            request_fingerprint: Some(fingerprint.0),
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(item_id, finalize)],
            }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        Ok((env, fingerprint))
    }

    pub fn build_request_id_commit_envelopes(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        entries: Vec<CommitTransitionEntry>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(Vec<CommandEnvelope>, BodyHash)> {
        let fingerprint = commit_body_hash(&entries)?;
        let _ = expected_epoch;
        let supports = self
            .projection
            .with_store(|p| p.supports_commit_transition());
        if !supports {
            return Err(EngineError::Unavailable);
        }
        let commit_fingerprint = fingerprint.0;
        let mut envelopes: Vec<CommandEnvelope> = Vec::new();
        let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
        for entry in entries {
            if !entry.side_records.is_empty()
                || !entry.lifecycle_items.is_empty()
                || entry.instance_fence.is_some()
            {
                return Err(EngineError::Invalid(
                    "build_request_id_commit_envelopes: finalize-only entries",
                ));
            }
            let claim_ref = entry.claim_ref;
            let consumed_input_id = claim_ref.item_id;
            let additional_claim_refs = entry.additional_claim_refs;
            let additional_consumed_input_ids = additional_claim_refs
                .iter()
                .map(|claim| claim.item_id)
                .collect::<Vec<_>>();
            let mut claim_refs = Vec::with_capacity(1 + additional_claim_refs.len());
            claim_refs.push(claim_ref);
            claim_refs.extend(additional_claim_refs);
            if let Err(error) = validate_distinct_commit_claims(&claim_refs[0], &claim_refs[1..]) {
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(error),
                });
                continue;
            }
            match self
                .projection
                .with_store(|p| p.commit_validate(shard, &claim_refs, now))
            {
                Ok(()) => {
                    envelopes.push(CommandEnvelope {
                        command_id: self.ids.next_command_id(),
                        request_id: Some(request_id.clone()),
                        request_fingerprint: Some(commit_fingerprint),
                        request_outcome: None,
                        item_ids: claim_refs.iter().map(|claim| claim.item_id).collect(),
                        command: QueueCommand::Finalize(FinalizeCommand {
                            outcomes: claim_refs
                                .iter()
                                .map(|claim| FinalizeOutcome::new(claim.item_id, entry.finalize))
                                .collect(),
                        }),
                        checksum: CommandChecksum(0),
                        created_at: now,
                    });
                    recovery.push(EntryRecovery {
                        consumed_input_id,
                        additional_consumed_input_ids,
                        instance: None,
                        side_record_keys: Vec::new(),
                        lifecycle_item_ids: Vec::new(),
                        status: CommitEntryStatus::Committed,
                    });
                }
                Err(error) => recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(error),
                }),
            }
        }
        let has_rejected = recovery
            .iter()
            .any(|r| matches!(r.status, CommitEntryStatus::Rejected(_)));
        if has_rejected {
            let outcome_entries: Vec<CommitOutcomeEntry> =
                recovery.iter().map(outcome_entry_from_recovery).collect();
            envelopes.push(CommandEnvelope {
                command_id: self.ids.next_command_id(),
                request_id: Some(request_id),
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
        Ok((envelopes, fingerprint))
    }
}

/// Macro-free thin wrappers for product types.
pub(crate) fn probe_axes<'a, P>(
    log: &'a Arc<ObjectLogEngineStore>,
    projection: &'a Arc<InProcessProjectionStore<P>>,
    control: &'a Arc<InProcessControlPlane>,
    ids: &'a Arc<SeqIdGen>,
    counters: &'a Arc<QueueCounters>,
    node_id: u8,
) -> ProbeAxes<'a, P>
where
    P: ProjectionStore + Send + 'static,
{
    ProbeAxes {
        log: log.as_ref(),
        projection: projection.as_ref(),
        control: control.as_ref(),
        ids: ids.as_ref(),
        counters: counters.as_ref(),
        node_id,
    }
}
