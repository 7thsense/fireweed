//! Logged orchestration for the native projection's query and mutation APIs.
//!
//! These operations share the queue gate with existing writes. Derived products
//! additionally freeze selection and cover the durable frontier before planning.
//! Ordinary workflow mutations keep their existing generation fast path.

use super::*;
use fireweed_engine::commit_surface::{
    CommitIdempotency, finish_prepared_commit_transition, prepare_commit_transition,
};

type Commit =
    Arc<dyn Fn(RawCommitRequest) -> OwnedTask<EngineResult<RawCommitOutcome>> + Send + Sync>;

pub(super) struct Operation {
    pub shard: QueueKey,
    pub epoch: u64,
    pub definition: QueueDefinition,
    pub projection: Arc<TursoRelational>,
    pub control: Arc<InProcessControlPlane>,
    pub ids: Arc<SeqIdGen>,
    pub counters: Arc<QueueCounters>,
    pub node_id: u8,
    pub commit_idempotency: CommitIdempotency,
    pub commit: Commit,
}

impl Operation {
    fn envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        CommandEnvelope {
            command_id: self.ids.next_command_id(),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    async fn append(&self, commands: Vec<CommandEnvelope>) -> EngineResult<RawCommitOutcome> {
        (self.commit)(RawCommitRequest::new(
            self.shard.clone(),
            commands,
            self.epoch,
        ))
        .await
    }

    pub async fn mutate(self, request: ItemMutationRequest) -> EngineResult<ItemMutationResponse> {
        let fingerprint = fireweed_engine::item_mutation_fingerprint(&request)?;
        if let Some(response) = self
            .projection
            .item_mutation_replay(&self.shard, &request, fingerprint)
            .await?
        {
            return Ok(response);
        }
        let plan = self
            .projection
            .plan_item_mutation(&self.shard, &self.definition, &request, &[])
            .await?;
        let mut response = plan.response;
        if request.dry_run {
            return Ok(response);
        }
        let item_ids = plan.command.items.iter().map(|item| item.item_id).collect();
        let mut envelope = self.envelope(
            QueueCommand::MutateItems(plan.command),
            item_ids,
            request.evaluated_at,
        );
        envelope.request_id = Some(request.request_id);
        envelope.request_fingerprint = Some(fingerprint);
        envelope.request_outcome = Some(RequestOutcome::ItemMutation {
            response_payload: serde_json::to_string(&response)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
        });
        let outcome = self.append(vec![envelope]).await?;
        response.position = outcome.positions().last().cloned();
        Ok(response)
    }

    pub async fn batch_update(
        self,
        request: fireweed_engine::BatchUpdateRequest,
        now: UtcTimestamp,
    ) -> EngineResult<fireweed_engine::BatchUpdateResponse> {
        if request.updates.is_empty() {
            return Err(EngineError::Invalid("empty batch update"));
        }
        if request.updates.len() > 1000 {
            return Err(EngineError::BatchTooLarge);
        }
        let fingerprint = fireweed_engine::batch_update_body_hash(&request)?;
        if let Some(response) = self
            .projection
            .batch_update_replay(&self.shard, &request.request_id, fingerprint, now)
            .await?
        {
            return Ok(response);
        }
        let plan = self
            .projection
            .server_plan_batch_update(&self.shard, &self.definition, request.updates)
            .await?;
        let updates: Vec<_> = plan
            .commands
            .into_iter()
            .map(|(_, command)| command)
            .collect();
        let item_ids = updates.iter().map(|command| command.item_id).collect();
        let response = fireweed_engine::BatchUpdateResponse {
            request_id: request.request_id.clone(),
            results: plan.outcomes,
        };
        let mut envelope = self.envelope(
            QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand { updates }),
            item_ids,
            now,
        );
        envelope.request_id = Some(request.request_id);
        envelope.request_fingerprint = Some(fingerprint.0);
        envelope.request_outcome = Some(RequestOutcome::BatchUpdate {
            response_payload: serde_json::to_string(&response)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
        });
        self.append(vec![envelope]).await?;
        Ok(response)
    }

    pub async fn upsert(
        self,
        mut item: PushItem,
        now: UtcTimestamp,
    ) -> EngineResult<UpsertOutcome> {
        let old = self
            .projection
            .server_live_items(&self.shard, std::slice::from_ref(&item.client_item_key))
            .await?
            .into_iter()
            .next()
            .flatten();
        let replaced = match old {
            None => None,
            Some(view) if view.lifecycle_state == ItemState::Pending => Some(view.item_id),
            Some(view) if view.lifecycle_state == ItemState::Leased => {
                return Err(EngineError::Invalid("collision with claimed item"));
            }
            Some(_) => return Err(EngineError::Terminal),
        };
        item.item_id = ItemId::mint(
            self.epoch,
            self.node_id,
            self.counters.reserve(&self.shard, self.epoch, 1),
        );
        item.max_attempts = self.definition.retry_policy.max_attempts;
        self.projection
            .server_validate_upsert(&self.shard, &self.definition, &item, replaced, now)
            .await?;
        let item_id = item.item_id;
        let (command, outcome) = match replaced {
            None => (
                QueueCommand::Push(PushCommand { items: vec![item] }),
                UpsertOutcome::Inserted { item_id },
            ),
            Some(superseded_item_id) => (
                QueueCommand::ReplacePending(ReplacePendingCommand {
                    client_item_key: item.client_item_key.clone(),
                    superseded_item_id,
                    replacement: item,
                }),
                UpsertOutcome::Replaced {
                    new_item_id: item_id,
                    superseded_item_id,
                },
            ),
        };
        self.append(vec![self.envelope(command, vec![item_id], now)])
            .await?;
        Ok(outcome)
    }

    pub async fn set_gates(
        self,
        command: fireweed_engine::SetGatesCommand,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let command = QueueCommand::SetGates(command);
        fireweed_engine::validate_gate_command_definition(&self.definition, &command)?;
        self.append(vec![self.envelope(command, vec![], now)])
            .await?;
        Ok(())
    }

    pub async fn update_fields(
        self,
        command: fireweed_engine::UpdateFieldsCommand,
        expected_version: Option<u64>,
        now: UtcTimestamp,
    ) -> EngineResult<u64> {
        let old = self
            .projection
            .server_validate_update_fields(
                &self.shard,
                &self.definition,
                &command,
                expected_version,
            )
            .await?;
        let version = old
            .checked_add(1)
            .ok_or(EngineError::Invalid("item version overflow"))?;
        let item_id = command.item_id;
        self.append(vec![self.envelope(
            QueueCommand::UpdateFields(command),
            vec![item_id],
            now,
        )])
        .await?;
        Ok(version)
    }

    pub async fn bounded_mutation(
        self,
        request: fireweed_core::BoundedMutationRequest,
        now: UtcTimestamp,
    ) -> EngineResult<fireweed_core::BoundedMutationResponse> {
        let plan = self
            .projection
            .server_plan_bounded_mutation(&self.shard, request)
            .await?;
        self.projection
            .server_validate_bounded_updates(&self.shard, &self.definition, &plan.updates)
            .await?;
        let commands = plan
            .updates
            .into_iter()
            .map(|update| {
                let id = update.command.item_id;
                self.envelope(QueueCommand::UpdateFields(update.command), vec![id], now)
            })
            .collect::<Vec<_>>();
        if !commands.is_empty() {
            self.append(commands).await?;
        }
        Ok(plan.response)
    }

    async fn replay_claim(
        &self,
        receipt: &serde_json::Value,
        ids_field: &str,
        now: UtcTimestamp,
    ) -> EngineResult<Claimed> {
        let ids: Vec<ItemId> = serde_json::from_value(receipt[ids_field].clone())
            .map_err(|e| EngineError::Storage(e.to_string()))?;
        let token: LeaseToken = serde_json::from_value(receipt["lease_token"].clone())
            .map_err(|e| EngineError::Storage(e.to_string()))?;
        let items = AsyncProjectionStore::render_claimed(
            self.projection.as_ref(),
            self.shard.clone(),
            ids.clone(),
        )
        .await?;
        if items.len() != ids.len()
            || items.iter().any(|item| {
                item.lease_expires_at <= now || item.lease_token.as_ref() != Some(&token)
            })
        {
            return Err(EngineError::RequestExpired);
        }
        Ok(Claimed {
            items,
            ..Default::default()
        })
    }

    fn validate_query_lease(&self, count: usize, duration: u64) -> EngineResult<()> {
        if count == 0 || count as u64 > self.definition.max_claim_batch_size {
            return Err(EngineError::Invalid("invalid query claim batch size"));
        }
        if duration == 0 || duration > self.definition.max_lease_duration_ms {
            return Err(EngineError::Invalid("invalid query claim lease duration"));
        }
        Ok(())
    }

    pub async fn claim_by_query(
        self,
        request: fireweed_core::ClaimByQueryRequest,
        context: fireweed_engine::ClaimByQueryContext,
    ) -> EngineResult<Claimed> {
        self.validate_query_lease(request.max_items as usize, request.lease_duration_ms)?;
        let request_id = request
            .request_id
            .clone()
            .ok_or(EngineError::Invalid("claim_by_query request_id required"))?;
        let fingerprint = fireweed_engine::claim_by_query_body_hash(&request)?.0;
        if let Some(receipt) = self
            .projection
            .server_query_claim_replay(
                &self.shard,
                "claim_by_query",
                &request_id,
                fingerprint,
                context.now,
            )
            .await?
        {
            return self.replay_claim(&receipt, "item_ids", context.now).await;
        }
        let item_ids = self
            .projection
            .server_select_claim_by_query(
                &self.shard,
                request.index.as_deref(),
                &request.filters,
                &request.order_by,
                request.max_items as usize,
                context.eligibility_at(),
            )
            .await?;
        let token = fireweed_engine::generate_query_lease_token()?;
        let mut envelope = self.envelope(
            QueueCommand::Claim(ClaimCommand {
                item_ids: item_ids.clone(),
                lease_token: token.clone(),
                lease_expires_at: context.lease_expires_at(request.lease_duration_ms),
                worker_id: Some(request.worker_id.clone()),
                authority_first: false,
            }),
            item_ids.clone(),
            context.now,
        );
        envelope.request_id = Some(request_id);
        envelope.request_fingerprint = Some(fingerprint);
        envelope.request_outcome = Some(RequestOutcome::ClaimByQuery {
            item_ids: item_ids.clone(),
            lease_token: token.clone(),
            worker_id: Some(request.worker_id),
        });
        let items = self
            .projection
            .materialize_pending_on_serving_reader(
                &self.shard,
                &item_ids,
                &token,
                context.lease_expires_at(request.lease_duration_ms),
            )
            .await?;
        if items.len() != item_ids.len() {
            return Err(EngineError::Storage(
                "query claim materialization lost selected rows".into(),
            ));
        }
        self.append(vec![envelope]).await?;
        Ok(Claimed {
            items,
            ..Default::default()
        })
    }

    pub async fn claim_by_item_ids(
        self,
        request: fireweed_core::ClaimByItemIdsRequest,
        context: fireweed_engine::ClaimByQueryContext,
    ) -> EngineResult<fireweed_engine::ClaimByItemIdsResponse> {
        let mut seen = HashSet::new();
        let ids: Vec<_> = request
            .item_ids
            .iter()
            .copied()
            .filter(|id| seen.insert(*id))
            .collect();
        self.validate_query_lease(ids.len(), request.lease_duration_ms)?;
        let fingerprint = fireweed_engine::claim_by_item_ids_body_hash(&request)?.0;
        if let Some(receipt) = self
            .projection
            .server_query_claim_replay(
                &self.shard,
                "claim_by_item_ids",
                &request.request_id,
                fingerprint,
                context.now,
            )
            .await?
        {
            let claimed = self
                .replay_claim(&receipt, "claimed_item_ids", context.now)
                .await?;
            let outcomes = serde_json::from_value(receipt["outcomes"].clone())
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            return Ok(fireweed_engine::ClaimByItemIdsResponse {
                items: claimed.items,
                outcomes,
            });
        }
        let classes = self
            .projection
            .server_classify_claim_by_item_ids(&self.shard, &ids, context.eligibility_at())
            .await?;
        let item_ids: Vec<_> = classes
            .iter()
            .filter(|(_, class)| *class == fireweed_core::ClaimByItemIdClass::Claimable)
            .map(|(id, _)| *id)
            .collect();
        let outcomes: Vec<_> = classes
            .into_iter()
            .map(|(item_id, class)| fireweed_core::ClaimByItemIdsOutcome {
                item_id,
                disposition: class.into(),
            })
            .collect();
        let token = match request.lease_token {
            Some(token) => token,
            None => fireweed_engine::generate_query_lease_token()?,
        };
        let mut envelope = self.envelope(
            QueueCommand::Claim(ClaimCommand {
                item_ids: item_ids.clone(),
                lease_token: token.clone(),
                lease_expires_at: context.lease_expires_at(request.lease_duration_ms),
                worker_id: Some(request.worker_id.clone()),
                authority_first: false,
            }),
            item_ids.clone(),
            context.now,
        );
        envelope.request_id = Some(request.request_id);
        envelope.request_fingerprint = Some(fingerprint);
        envelope.request_outcome = Some(RequestOutcome::ClaimByItemIds {
            claimed_item_ids: item_ids.clone(),
            lease_token: token.clone(),
            outcomes: outcomes.clone(),
            worker_id: Some(request.worker_id),
        });
        let items = self
            .projection
            .materialize_pending_on_serving_reader(
                &self.shard,
                &item_ids,
                &token,
                context.lease_expires_at(request.lease_duration_ms),
            )
            .await?;
        if items.len() != item_ids.len() {
            return Err(EngineError::Storage(
                "addressed claim materialization lost selected rows".into(),
            ));
        }
        self.append(vec![envelope]).await?;
        Ok(fireweed_engine::ClaimByItemIdsResponse { items, outcomes })
    }

    pub async fn transition(
        self,
        transition: fireweed_engine::CommitTransition,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<fireweed_engine::CommitEntryOutcome>> {
        let prepared = prepare_commit_transition(
            self.projection.as_ref(),
            self.control.as_ref(),
            self.ids.as_ref(),
            self.counters.as_ref(),
            self.node_id,
            &self.commit_idempotency,
            self.epoch,
            &self.shard,
            transition,
            now,
        )
        .await?;
        finish_prepared_commit_transition(
            &self.shard,
            self.epoch,
            prepared,
            &self.commit_idempotency,
            now,
            |request| (self.commit)(request),
        )
        .await
    }
}

impl<L: AsyncLogStore + 'static> AtomicTursoBackend<L> {
    pub(super) async fn parity_operation<T, F>(
        &self,
        shard: &QueueKey,
        expected_epoch: Option<u64>,
        operation: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Operation) -> OwnedTask<EngineResult<T>> + Send + 'static,
    {
        let shard = shard.clone();
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
        let ids = Arc::clone(&self.ids);
        let counters = Arc::clone(&self.counters);
        let commit_idempotency = Arc::clone(&self.commit_idempotency);
        let node_id = self.node_id;
        let strategy = self.engine.commit_strategy();
        self.engine
            .submit_operation(shard.clone(), move || {
                Box::pin(async move {
                    let epoch = AsyncLogStore::current_epoch(log.as_ref(), shard.clone()).await?;
                    if expected_epoch.is_some_and(|expected| expected != epoch) {
                        return Err(EngineError::EpochFenced);
                    }
                    let definition =
                        AsyncControlPlane::queue_definition(control.as_ref(), shard.clone())
                            .await?;
                    let commit: Commit = Arc::new(move |request| strategy.commit(request));
                    operation(Operation {
                        shard,
                        epoch,
                        definition,
                        projection,
                        control,
                        ids,
                        counters,
                        node_id,
                        commit_idempotency,
                        commit,
                    })
                    .await
                })
            })
            .await
            .map_err(|error| map_submit("native projection operation", error))?
    }
}

#[cfg(feature = "objectlog")]
impl DerivedObjectLogTursoBackend {
    pub(super) async fn parity_operation<T, F>(
        &self,
        shard: &QueueKey,
        expected_epoch: Option<u64>,
        operation: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Operation) -> OwnedTask<EngineResult<T>> + Send + 'static,
    {
        let shard = shard.clone();
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
        let ids = Arc::clone(&self.ids);
        let counters = Arc::clone(&self.counters);
        let commit_idempotency = Arc::clone(&self.commit_idempotency);
        let node_id = self.node_id;
        let strategy = self.engine.commit_strategy();
        let coordinator = self.async_apply.clone();
        let fence = self.selection_fence.clone();
        let admission = self.fence_admission.clone();
        self.engine
            .submit_operation(shard.clone(), move || {
                Box::pin(async move {
                    let _waiter = admission.admit_waiter().map_err(map_coord)?;
                    let _fence = fence
                        .acquire_exclusive(shard.clone())
                        .await
                        .map_err(map_coord)?;
                    let epoch = AsyncLogStore::current_epoch(log.as_ref(), shard.clone()).await?;
                    if expected_epoch.is_some_and(|expected| expected != epoch) {
                        return Err(EngineError::EpochFenced);
                    }
                    if let Some(coordinator) = &coordinator
                        && let Some(target) =
                            AsyncLogStore::high_water(log.as_ref(), shard.clone()).await?
                    {
                        coordinator
                            .wait_until_covers(&shard, &target, S3S_DERIVED_COVERAGE_OR_WORK_WAIT)
                            .await?;
                    }
                    let definition =
                        AsyncControlPlane::queue_definition(control.as_ref(), shard.clone())
                            .await?;
                    let commit: Commit = Arc::new(move |request| {
                        let strategy = Arc::clone(&strategy);
                        Box::pin(async move {
                            strategy
                                .commit(request.with_append_admission(
                                    AppendAdmissionClass::SharedSelectionLive,
                                ))
                                .await
                        })
                    });
                    operation(Operation {
                        shard,
                        epoch,
                        definition,
                        projection,
                        control,
                        ids,
                        counters,
                        node_id,
                        commit_idempotency,
                        commit,
                    })
                    .await
                })
            })
            .await
            .map_err(|error| map_submit("native projection operation", error))?
    }
}
