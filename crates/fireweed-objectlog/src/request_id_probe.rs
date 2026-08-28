//! Shared [`RequestIdReplayProbe`] + [`LogRead`] helpers for LogEngine objectlog products.
//!
//! Harness-only mid-pipeline probe for AC-TXN-3 (append→apply window). Used by memory, sqlite,
//! and selected-projection products so TP-003 / E3 cells can strike the same durable envelope shapes.
//! `AfterAppendBeforeApply` withholds applied success without cancelling a durable reservation;
//! reopen/rebuild of request-id maps remains authoritative. Read-side poison visibility is S3c.

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
        // P14: bounded sync bridge for harness-only probe. No silent epoch-0 on read failure.
        let epoch = match expected_epoch {
            Some(e) => fireweed_engine::resolve_bounded_epoch(e, Some(e))?,
            None => fireweed_engine::resolve_write_epoch_sync(None, || {
                crate::block_on_objectlog(AsyncLogStore::current_epoch(self.log, shard.clone()))
            })?,
        };
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
        // Always stamp a CommitTransition marker when request_id is present — parity with
        // prepare_commit_transition. AfterAppendBeforeApply withholds apply without cancelling
        // the durable reservation; recovery rebuilds commit_idempotency from this marker so a
        // post-reopen retry stays Replay rather than Rejected(Terminal). Read-side poison is S3c.
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use fireweed_core::{
        ClientItemKey, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
        RecurrencePolicy, RequestId, RetryPolicy, TenantId, UtcTimestamp,
    };
    use fireweed_engine::{
        Backend, ControlPlaneStore, LogRead, ProjectionRead, PushPort, PushSpec, QueueKey,
        RawCommitFault, RawCommitRequest, RequestIdReplayProbe,
    };

    use crate::{AsyncObjectLogMemoryBackend, flush_config_from_segment};

    static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q").unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
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

    fn shard() -> QueueKey {
        QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap())
    }

    fn body() -> Vec<PushSpec> {
        vec![PushSpec {
            client_item_key: Some(ClientItemKey::new("s3v-rid").unwrap()),
            priority: Some(PriorityValue::Int64(7)),
            payload: Some(bytes::Bytes::from_static(b"s3v-rid")),
            ..PushSpec::default()
        }]
    }

    async fn open(root: &std::path::Path) -> AsyncObjectLogMemoryBackend {
        AsyncObjectLogMemoryBackend::open_local(root, flush_config_from_segment(256 * 1_024, 50))
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn after_append_before_apply_poisons_then_recovers_authoritatively() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3v-request-id-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let rid = RequestId::new("s3v-after-append").unwrap();
        let committed_ids = {
            let backend = open(&root).await;
            backend.create_queue(qdef()).await.unwrap();
            let (env, ids) = backend
                .build_request_id_push_envelope(
                    &shard(),
                    rid.clone(),
                    body(),
                    UtcTimestamp::new(1, 0).unwrap(),
                    None,
                )
                .unwrap();
            assert_eq!(env.request_id.as_ref(), Some(&rid));
            let epoch = backend.current_epoch(&shard()).await.unwrap();
            let outcome = backend
                .commit_raw(
                    RawCommitRequest::new(shard(), vec![env], epoch)
                        .with_fault(RawCommitFault::AfterAppendBeforeApply),
                )
                .await
                .expect("AfterAppendBeforeApply must withhold applied success");
            assert!(
                !outcome.projection_applied(),
                "withheld-success must remain unapplied"
            );
            let page = backend.read_from(&shard(), None, 16).await.unwrap();
            assert_eq!(
                page.entries.len(),
                1,
                "durable request-id reservation must not be cancelled"
            );
            assert_eq!(page.entries[0].1.request_id.as_ref(), Some(&rid));
            assert_eq!(
                backend.metrics(&shard()).await.unwrap().pending,
                0,
                "apply was withheld; serving projection is unchanged until reopen (S3c owns live poison reads)"
            );
            ids
        };

        let recovered = open(&root).await;
        assert_eq!(
            recovered.metrics(&shard()).await.unwrap().pending,
            1,
            "reopen must rebuild the request-id reservation authoritatively"
        );
        let replay = recovered
            .push_with_request_id(
                &shard(),
                rid,
                body(),
                UtcTimestamp::new(2, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            replay.item_ids, committed_ids,
            "request-id retry after reopen must replay the one committed result"
        );
        assert_eq!(recovered.metrics(&shard()).await.unwrap().pending, 1);
        drop(recovered);
        let _ = std::fs::remove_dir_all(root);
    }
}
