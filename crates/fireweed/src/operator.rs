//! API-002 operator repair/redrive surface on [`RuntimeCore`] / [`Fireweed`].
//!
//! Implements pause/resume, token-redacted inspection, fenced force-release/reschedule,
//! failed-item redrive, dry-run and executed bounded purge/archive, auth denial for data
//! principals, redacted audit records, and idempotent async operation replay — transport-neutral
//! over the same composition used by every storage matrix cell.

use super::*;
use fireweed_core::{BodyHash, Metadata, MetadataValue};
use fireweed_engine::{
    AuthContext, CommandChecksum, CommandEnvelope, CommandId, OperationHandle, OperationId,
    OperatorAsyncAccept, OperatorAuditRecord, OperatorItemView, OperatorOpKind, OperatorOpPayload,
    OperatorOperationState, OperatorOperationStore, OperatorProgress, PauseQueueCommand,
    QueueAdminState, QueueCommand, RawCommitRequest, RepairAction, RetryCountMode,
    deterministic_operation_id, hash_lease_token, operator_body_fingerprint,
};

/// Process-local operator control-plane state for one Fireweed handle.
#[derive(Default)]
pub(crate) struct OperatorRuntimeState {
    /// Per-queue permanent async-operation store (API-002).
    by_queue: HashMap<QueueKey, OperatorOperationStore<OperatorOpPayload>>,
    /// Process-local mirror of durable `queue_admin_paused` (set only after a successful pause commit).
    paused: HashSet<QueueKey>,
    /// Redacted audit trail (no payloads / lease tokens).
    audit: Vec<OperatorAuditRecord>,
}

impl OperatorRuntimeState {
    fn store_mut(&mut self, queue: &QueueKey) -> &mut OperatorOperationStore<OperatorOpPayload> {
        self.by_queue.entry(queue.clone()).or_default()
    }
}

/// Metadata key used when archive marks items retained in place (no external sink configured).
pub const OPERATOR_ARCHIVED_METADATA_KEY: &str = "fireweed.archived";

fn priority_debug(p: &Option<fireweed_core::PriorityValue>) -> Option<String> {
    p.as_ref().map(|v| format!("{v:?}"))
}

fn item_state_label(state: fireweed_core::ItemState) -> String {
    match state {
        fireweed_core::ItemState::Pending => "pending".into(),
        fireweed_core::ItemState::Leased => "leased".into(),
        fireweed_core::ItemState::Complete => "complete".into(),
        fireweed_core::ItemState::Failed => "failed".into(),
    }
}

impl<B: LibBackend> RuntimeCore<B> {
    fn operator_state(&self) -> std::sync::MutexGuard<'_, OperatorRuntimeState> {
        self.operator.lock().expect("operator state poisoned")
    }

    fn authorize_operator(auth: &AuthContext, queue: &QueueKey) -> EngineResult<()> {
        auth.authorize_tenant(queue.tenant_id.as_str())?;
        auth.authorize_operator()?;
        Ok(())
    }

    fn record_audit(&self, record: OperatorAuditRecord) {
        self.operator_state().audit.push(record);
    }

    /// Emit a redacted audit record for an operator action (never logs payloads or lease tokens).
    pub fn operator_audit_records(&self) -> Vec<OperatorAuditRecord> {
        self.operator_state().audit.clone()
    }

    /// API-002 `PauseQueue`: durable admin pause; claims return empty while paused.
    pub async fn operator_pause(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        audit_reason: Option<String>,
    ) -> EngineResult<QueueAdminState>
    where
        B: ItemMutationPort,
    {
        Self::authorize_operator(auth, queue)?;
        let body = format!("pause|{}", audit_reason.as_deref().unwrap_or(""));
        let fp = operator_body_fingerprint(OperatorOpKind::Pause, &body);
        let accept = self
            .operator_begin(
                auth,
                queue,
                &request_id,
                OperatorOpKind::Pause,
                fp,
                false,
                audit_reason.clone(),
            )
            .await?;
        if !accept.replayed {
            self.commit_admin_command(
                queue,
                &request_id,
                QueueCommand::PauseQueue(PauseQueueCommand::default()),
            )
            .await?;
            self.operator_state().paused.insert(queue.clone());
            self.operator_finish(
                queue,
                &accept.operation_id,
                OperatorOperationState::Succeeded,
                0,
                1,
                0,
            );
        }
        self.record_audit(OperatorAuditRecord {
            request_id: request_id.as_str().to_string(),
            operation_id: Some(accept.operation_id.as_str().to_string()),
            principal_id: auth.principal_id().to_string(),
            kind: OperatorOpKind::Pause,
            tenant_id: queue.tenant_id.as_str().to_string(),
            queue_id: queue.queue_id.as_str().to_string(),
            selector_fingerprint: fp.0,
            matched: 0,
            affected: 1,
            dry_run: false,
            audit_reason,
            payload_logged: false,
            lease_token_redacted: true,
        });
        Ok(self.operator_admin_state_unlocked(queue))
    }

    /// API-002 `ResumeQueue`.
    pub async fn operator_resume(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        audit_reason: Option<String>,
    ) -> EngineResult<QueueAdminState>
    where
        B: ItemMutationPort,
    {
        Self::authorize_operator(auth, queue)?;
        let body = format!("resume|{}", audit_reason.as_deref().unwrap_or(""));
        let fp = operator_body_fingerprint(OperatorOpKind::Resume, &body);
        let accept = self
            .operator_begin(
                auth,
                queue,
                &request_id,
                OperatorOpKind::Resume,
                fp,
                false,
                audit_reason.clone(),
            )
            .await?;
        if !accept.replayed {
            self.commit_admin_command(queue, &request_id, QueueCommand::ResumeQueue)
                .await?;
            self.operator_state().paused.remove(queue);
            self.operator_finish(
                queue,
                &accept.operation_id,
                OperatorOperationState::Succeeded,
                0,
                1,
                0,
            );
        }
        self.record_audit(OperatorAuditRecord {
            request_id: request_id.as_str().to_string(),
            operation_id: Some(accept.operation_id.as_str().to_string()),
            principal_id: auth.principal_id().to_string(),
            kind: OperatorOpKind::Resume,
            tenant_id: queue.tenant_id.as_str().to_string(),
            queue_id: queue.queue_id.as_str().to_string(),
            selector_fingerprint: fp.0,
            matched: 0,
            affected: 1,
            dry_run: false,
            audit_reason,
            payload_logged: false,
            lease_token_redacted: true,
        });
        Ok(self.operator_admin_state_unlocked(queue))
    }

    /// API-002 `GetQueueAdminState`.
    pub fn operator_admin_state(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
    ) -> EngineResult<QueueAdminState> {
        Self::authorize_operator(auth, queue)?;
        Ok(self.operator_admin_state_unlocked(queue))
    }

    fn operator_admin_state_unlocked(&self, queue: &QueueKey) -> QueueAdminState {
        let paused = self.operator_state().paused.contains(queue);
        QueueAdminState {
            paused,
            queue_admin_paused: paused,
            eligible_age_accrues: !paused,
        }
    }

    /// Token-redacted operator inspection of a live item by client key (API-002 GetItem).
    pub async fn operator_inspect_item(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        key: ClientItemKey,
    ) -> EngineResult<Option<OperatorItemView>> {
        Self::authorize_operator(auth, queue)?;
        let live = self.live_item(queue, key).await?;
        let Some(live) = live else {
            return Ok(None);
        };
        // Cross-check leased state via PEL without ever returning the lease token.
        let pending = self.backend.pending(queue).await.unwrap_or_default();
        let lease = pending.iter().find(|l| l.item_id == live.item_id);
        let (lease_expires_at_ms, worker_id, lease_present) = if let Some(l) = lease {
            // Touch the token only to prove redaction path — hash it, never surface it.
            let _ = hash_lease_token(l.lease_token.as_str());
            (Some(l.lease_expires_at.seconds * 1000), None, true)
        } else {
            (None, None, false)
        };
        Ok(Some(OperatorItemView {
            item_id: live.item_id.to_string(),
            client_item_key: live.client_item_key.as_str().to_string(),
            item_version: live.item_version,
            lifecycle_state: if lease_present {
                "leased".into()
            } else {
                item_state_label(live.lifecycle_state)
            },
            priority: priority_debug(&live.priority),
            not_before_ms: live.not_before.map(|t| t.seconds * 1000),
            attempt_count: live.attempt_count,
            worker_id,
            lease_expires_at_ms,
            lease_token_present: lease_present,
            lease_token_redacted: true,
        }))
    }

    /// API-002 `RepairItems` over explicit item ids (force_release / reschedule / force_* / clear_lease).
    #[allow(clippy::too_many_arguments)]
    pub async fn operator_repair(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        action: RepairAction,
        item_ids: Vec<ItemId>,
        priority: Option<PriorityValue>,
        not_before: Option<UtcTimestamp>,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<(OperatorAsyncAccept, ItemMutationResponse)>
    where
        B: ItemMutationPort,
    {
        Self::authorize_operator(auth, queue)?;
        let body = serde_json::json!({
            "action": format!("{action:?}"),
            "item_ids": item_ids.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
            "priority": priority.as_ref().map(|p| format!("{p:?}")),
            "not_before": not_before.map(|t| t.seconds),
            "dry_run": dry_run,
        })
        .to_string();
        let fp = operator_body_fingerprint(OperatorOpKind::Repair, &body);
        let accept = self
            .operator_begin(
                auth,
                queue,
                &request_id,
                OperatorOpKind::Repair,
                fp,
                dry_run,
                audit_reason.clone(),
            )
            .await?;
        if accept.replayed {
            // Re-execute mutation under the same request_id to obtain retained response (ItemMutationPort
            // permanent request-id replay) without starting a second operator operation.
            let req = self.repair_mutation_request(
                request_id.clone(),
                action,
                item_ids,
                priority,
                not_before,
                dry_run,
            );
            let response = self.mutate_items(queue, req).await?;
            return Ok((accept, response));
        }

        let req = self.repair_mutation_request(
            request_id.clone(),
            action,
            item_ids,
            priority,
            not_before,
            dry_run,
        );
        let response = self.mutate_items(queue, req).await?;
        let matched = response.summary.matched;
        let affected = response.summary.changed + response.summary.purged;
        let failed = response.summary.rejected;
        self.operator_finish(
            queue,
            &accept.operation_id,
            OperatorOperationState::Succeeded,
            matched,
            affected,
            failed,
        );
        self.record_audit(OperatorAuditRecord {
            request_id: request_id.as_str().to_string(),
            operation_id: Some(accept.operation_id.as_str().to_string()),
            principal_id: auth.principal_id().to_string(),
            kind: OperatorOpKind::Repair,
            tenant_id: queue.tenant_id.as_str().to_string(),
            queue_id: queue.queue_id.as_str().to_string(),
            selector_fingerprint: fp.0,
            matched,
            affected,
            dry_run,
            audit_reason,
            payload_logged: false,
            lease_token_redacted: true,
        });
        Ok((
            OperatorAsyncAccept {
                progress: OperatorProgress {
                    matched,
                    affected,
                    failed,
                    updated_at_ms: self.clock.now().seconds * 1000,
                },
                ..accept
            },
            response,
        ))
    }

    /// API-002 `RedriveItems`: return terminal-failed items to pending/eligible.
    #[allow(clippy::too_many_arguments)]
    pub async fn operator_redrive(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        item_ids: Vec<ItemId>,
        not_before: Option<UtcTimestamp>,
        priority: Option<PriorityValue>,
        _retry_count_mode: RetryCountMode,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<(OperatorAsyncAccept, ItemMutationResponse)>
    where
        B: ItemMutationPort,
    {
        Self::authorize_operator(auth, queue)?;
        let body = serde_json::json!({
            "item_ids": item_ids.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
            "not_before": not_before.map(|t| t.seconds),
            "priority": priority.as_ref().map(|p| format!("{p:?}")),
            "dry_run": dry_run,
        })
        .to_string();
        let fp = operator_body_fingerprint(OperatorOpKind::Redrive, &body);
        let accept = self
            .operator_begin(
                auth,
                queue,
                &request_id,
                OperatorOpKind::Redrive,
                fp,
                dry_run,
                audit_reason.clone(),
            )
            .await?;
        let evaluated_at = self.clock.now();
        let entries = item_ids
            .into_iter()
            .map(|item_id| AddressedMutation {
                item_id,
                expected_item_version: None,
                predicates: vec![ItemPredicate::StateIn(vec![
                    fireweed_core::ItemState::Failed,
                ])],
                lease_guard: LeaseGuard::RejectActive,
                patch: ItemPatch {
                    lifecycle: LifecyclePatch::SetPending,
                    priority: match priority.clone() {
                        Some(p) => BatchUpdateValue::Replace(Some(p)),
                        None => BatchUpdateValue::Keep,
                    },
                    not_before: match not_before {
                        Some(t) => BatchUpdateValue::Replace(Some(t)),
                        None => BatchUpdateValue::Replace(None),
                    },
                    ..Default::default()
                },
            })
            .collect();
        let req = ItemMutationRequest {
            request_id: request_id.clone(),
            evaluated_at,
            dry_run,
            returning: ItemMutationReturning::BeforeSnapshot,
            gate_changes: vec![],
            operation: ItemMutationOperation::Addressed { entries },
        };
        let response = self.mutate_items(queue, req).await?;
        let matched = response.summary.matched;
        let affected = response.summary.changed;
        let failed = response.summary.rejected;
        if !accept.replayed {
            self.operator_finish(
                queue,
                &accept.operation_id,
                OperatorOperationState::Succeeded,
                matched,
                affected,
                failed,
            );
            self.record_audit(OperatorAuditRecord {
                request_id: request_id.as_str().to_string(),
                operation_id: Some(accept.operation_id.as_str().to_string()),
                principal_id: auth.principal_id().to_string(),
                kind: OperatorOpKind::Redrive,
                tenant_id: queue.tenant_id.as_str().to_string(),
                queue_id: queue.queue_id.as_str().to_string(),
                selector_fingerprint: fp.0,
                matched,
                affected,
                dry_run,
                audit_reason,
                payload_logged: false,
                lease_token_redacted: true,
            });
        }
        Ok((
            OperatorAsyncAccept {
                progress: OperatorProgress {
                    matched,
                    affected,
                    failed,
                    updated_at_ms: self.clock.now().seconds * 1000,
                },
                ..accept
            },
            response,
        ))
    }

    /// API-002 `PurgeQueueItems` over explicit item ids (bounded; supports dry_run).
    pub async fn operator_purge(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        item_ids: Vec<ItemId>,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<(OperatorAsyncAccept, ItemMutationResponse)>
    where
        B: ItemMutationPort,
    {
        Self::authorize_operator(auth, queue)?;
        let body = serde_json::json!({
            "item_ids": item_ids.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
            "dry_run": dry_run,
        })
        .to_string();
        let fp = operator_body_fingerprint(OperatorOpKind::Purge, &body);
        let accept = self
            .operator_begin(
                auth,
                queue,
                &request_id,
                OperatorOpKind::Purge,
                fp,
                dry_run,
                audit_reason.clone(),
            )
            .await?;
        let evaluated_at = self.clock.now();
        let entries = item_ids
            .into_iter()
            .map(|item_id| AddressedMutation {
                item_id,
                expected_item_version: None,
                predicates: vec![],
                // Purge of a leased item must fence the lease (API-002).
                lease_guard: LeaseGuard::InvalidateActive,
                patch: ItemPatch {
                    lifecycle: LifecyclePatch::Purge,
                    ..Default::default()
                },
            })
            .collect();
        let req = ItemMutationRequest {
            request_id: request_id.clone(),
            evaluated_at,
            dry_run,
            returning: ItemMutationReturning::BeforeSnapshot,
            gate_changes: vec![],
            operation: ItemMutationOperation::Addressed { entries },
        };
        let response = self.mutate_items(queue, req).await?;
        let matched = response.summary.matched;
        let affected = response.summary.purged;
        let failed = response.summary.rejected;
        if !accept.replayed {
            self.operator_finish(
                queue,
                &accept.operation_id,
                OperatorOperationState::Succeeded,
                matched,
                affected,
                failed,
            );
            self.record_audit(OperatorAuditRecord {
                request_id: request_id.as_str().to_string(),
                operation_id: Some(accept.operation_id.as_str().to_string()),
                principal_id: auth.principal_id().to_string(),
                kind: OperatorOpKind::Purge,
                tenant_id: queue.tenant_id.as_str().to_string(),
                queue_id: queue.queue_id.as_str().to_string(),
                selector_fingerprint: fp.0,
                matched,
                affected,
                dry_run,
                audit_reason,
                payload_logged: false,
                lease_token_redacted: true,
            });
        }
        Ok((
            OperatorAsyncAccept {
                progress: OperatorProgress {
                    matched,
                    affected,
                    failed,
                    updated_at_ms: self.clock.now().seconds * 1000,
                },
                ..accept
            },
            response,
        ))
    }

    /// API-002 `ArchiveItems`: mark items retained in place via metadata (no external sink).
    pub async fn operator_archive(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        item_ids: Vec<ItemId>,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<(OperatorAsyncAccept, ItemMutationResponse)>
    where
        B: ItemMutationPort,
    {
        Self::authorize_operator(auth, queue)?;
        let body = serde_json::json!({
            "item_ids": item_ids.iter().map(|i| i.to_string()).collect::<Vec<_>>(),
            "dry_run": dry_run,
        })
        .to_string();
        let fp = operator_body_fingerprint(OperatorOpKind::Archive, &body);
        let accept = self
            .operator_begin(
                auth,
                queue,
                &request_id,
                OperatorOpKind::Archive,
                fp,
                dry_run,
                audit_reason.clone(),
            )
            .await?;
        let evaluated_at = self.clock.now();
        let entries = item_ids
            .into_iter()
            .map(|item_id| AddressedMutation {
                item_id,
                expected_item_version: None,
                predicates: vec![],
                lease_guard: LeaseGuard::InvalidateActive,
                patch: ItemPatch {
                    metadata: BatchUpdateValue::Replace({
                        let mut m = Metadata::new();
                        m.insert(
                            OPERATOR_ARCHIVED_METADATA_KEY.to_string(),
                            MetadataValue::Bool(true),
                        );
                        m
                    }),
                    field_edits: BTreeMap::from([(
                        OPERATOR_ARCHIVED_METADATA_KEY.to_string(),
                        Some(bytes::Bytes::from_static(b"1")),
                    )]),
                    ..Default::default()
                },
            })
            .collect();
        let req = ItemMutationRequest {
            request_id: request_id.clone(),
            evaluated_at,
            dry_run,
            returning: ItemMutationReturning::BeforeSnapshot,
            gate_changes: vec![],
            operation: ItemMutationOperation::Addressed { entries },
        };
        let response = self.mutate_items(queue, req).await?;
        let matched = response.summary.matched;
        let affected = response.summary.changed;
        let failed = response.summary.rejected;
        if !accept.replayed {
            self.operator_finish(
                queue,
                &accept.operation_id,
                OperatorOperationState::Succeeded,
                matched,
                affected,
                failed,
            );
            self.record_audit(OperatorAuditRecord {
                request_id: request_id.as_str().to_string(),
                operation_id: Some(accept.operation_id.as_str().to_string()),
                principal_id: auth.principal_id().to_string(),
                kind: OperatorOpKind::Archive,
                tenant_id: queue.tenant_id.as_str().to_string(),
                queue_id: queue.queue_id.as_str().to_string(),
                selector_fingerprint: fp.0,
                matched,
                affected,
                dry_run,
                audit_reason,
                payload_logged: false,
                lease_token_redacted: true,
            });
        }
        Ok((
            OperatorAsyncAccept {
                progress: OperatorProgress {
                    matched,
                    affected,
                    failed,
                    updated_at_ms: self.clock.now().seconds * 1000,
                },
                ..accept
            },
            response,
        ))
    }

    /// API-002 `GetOperation`.
    pub fn operator_get_operation(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        operation_id: &OperationId,
    ) -> EngineResult<Option<OperationHandle<OperatorOpPayload>>> {
        Self::authorize_operator(auth, queue)?;
        Ok(self
            .operator_state()
            .by_queue
            .get(queue)
            .and_then(|s| s.get(operation_id)))
    }

    async fn commit_admin_command(
        &self,
        queue: &QueueKey,
        request_id: &RequestId,
        command: QueueCommand,
    ) -> EngineResult<()> {
        let epoch = match self.session_epoch(queue).await? {
            Some(e) => e,
            None => self.backend.current_epoch(queue).await?,
        };
        let now = self.clock.now();
        let envelope = CommandEnvelope {
            command_id: CommandId::new(format!(
                "op-{}-{}",
                request_id.as_str(),
                self.ids.fetch_add(1, Ordering::SeqCst)
            )),
            request_id: Some(request_id.clone()),
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![],
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        };
        self.backend
            .commit_raw(RawCommitRequest::new(queue.clone(), vec![envelope], epoch))
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn operator_begin(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: &RequestId,
        kind: OperatorOpKind,
        fingerprint: BodyHash,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<OperatorAsyncAccept> {
        let operation_id = deterministic_operation_id(
            queue.tenant_id.as_str(),
            queue.queue_id.as_str(),
            kind,
            request_id.as_str(),
        );
        {
            let mut state = self.operator_state();
            let store = state.store_mut(queue);
            if let Some(existing) = store.lookup(request_id, fingerprint)? {
                return Ok(OperatorAsyncAccept {
                    request_id: request_id.as_str().to_string(),
                    operation_id: existing.operation_id,
                    state: existing.state,
                    progress: existing.payload.progress,
                    dry_run,
                    replayed: true,
                });
            }
            let payload = OperatorOpPayload {
                kind,
                queue_tenant: queue.tenant_id.as_str().to_string(),
                queue_id: queue.queue_id.as_str().to_string(),
                request_id: request_id.as_str().to_string(),
                dry_run,
                audit_reason,
                progress: OperatorProgress {
                    matched: 0,
                    affected: 0,
                    failed: 0,
                    updated_at_ms: self.clock.now().seconds * 1000,
                },
                selector_fingerprint: fingerprint.0,
            };
            store.record(
                request_id.clone(),
                fingerprint,
                operation_id.clone(),
                OperatorOperationState::Accepted,
                payload,
            );
            let _ = auth; // principal already authorized by caller
        }
        Ok(OperatorAsyncAccept {
            request_id: request_id.as_str().to_string(),
            operation_id,
            state: OperatorOperationState::Accepted,
            progress: OperatorProgress::default(),
            dry_run,
            replayed: false,
        })
    }

    fn operator_finish(
        &self,
        queue: &QueueKey,
        operation_id: &OperationId,
        state: OperatorOperationState,
        matched: u64,
        affected: u64,
        failed: u64,
    ) {
        let mut guard = self.operator_state();
        let Some(store) = guard.by_queue.get_mut(queue) else {
            return;
        };
        let Some(current) = store.get(operation_id) else {
            return;
        };
        let mut payload = current.payload;
        payload.progress = OperatorProgress {
            matched,
            affected,
            failed,
            updated_at_ms: self.clock.now().seconds * 1000,
        };
        let _ = store.advance(operation_id, state, payload);
    }

    fn repair_mutation_request(
        &self,
        request_id: RequestId,
        action: RepairAction,
        item_ids: Vec<ItemId>,
        priority: Option<PriorityValue>,
        not_before: Option<UtcTimestamp>,
        dry_run: bool,
    ) -> ItemMutationRequest {
        let evaluated_at = self.clock.now();
        let entries = item_ids
            .into_iter()
            .map(|item_id| {
                let (lease_guard, lifecycle, prio, nbf) = match action {
                    RepairAction::ForceRelease | RepairAction::ClearLease => (
                        LeaseGuard::InvalidateActive,
                        LifecyclePatch::SetPending,
                        BatchUpdateValue::Keep,
                        BatchUpdateValue::Keep,
                    ),
                    RepairAction::Reschedule => (
                        LeaseGuard::InvalidateActive,
                        LifecyclePatch::Keep,
                        match priority.clone() {
                            Some(p) => BatchUpdateValue::Replace(Some(p)),
                            None => BatchUpdateValue::Keep,
                        },
                        match not_before {
                            Some(t) => BatchUpdateValue::Replace(Some(t)),
                            None => BatchUpdateValue::Keep,
                        },
                    ),
                    RepairAction::ForceRetry => (
                        LeaseGuard::InvalidateActive,
                        LifecyclePatch::SetPending,
                        match priority.clone() {
                            Some(p) => BatchUpdateValue::Replace(Some(p)),
                            None => BatchUpdateValue::Keep,
                        },
                        match not_before {
                            Some(t) => BatchUpdateValue::Replace(Some(t)),
                            None => BatchUpdateValue::Replace(None),
                        },
                    ),
                    RepairAction::ForceFail => (
                        LeaseGuard::InvalidateActive,
                        LifecyclePatch::SetFailed,
                        BatchUpdateValue::Keep,
                        BatchUpdateValue::Keep,
                    ),
                    RepairAction::ForceComplete => (
                        LeaseGuard::InvalidateActive,
                        LifecyclePatch::SetComplete,
                        BatchUpdateValue::Keep,
                        BatchUpdateValue::Keep,
                    ),
                };
                AddressedMutation {
                    item_id,
                    expected_item_version: None,
                    predicates: vec![],
                    lease_guard,
                    patch: ItemPatch {
                        lifecycle,
                        priority: prio,
                        not_before: nbf,
                        ..Default::default()
                    },
                }
            })
            .collect();
        ItemMutationRequest {
            request_id,
            evaluated_at,
            dry_run,
            returning: ItemMutationReturning::BeforeSnapshot,
            gate_changes: vec![],
            operation: ItemMutationOperation::Addressed { entries },
        }
    }
}

// ---------------------------------------------------------------------------
// Public Fireweed operator plane
// ---------------------------------------------------------------------------

impl Fireweed {
    /// API-002 `PauseQueue` through the public facade.
    pub async fn operator_pause(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        audit_reason: Option<String>,
    ) -> EngineResult<QueueAdminState> {
        self.operator
            .pause(auth, queue, request_id, audit_reason)
            .await
    }

    /// API-002 `ResumeQueue`.
    pub async fn operator_resume(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        audit_reason: Option<String>,
    ) -> EngineResult<QueueAdminState> {
        self.operator
            .resume(auth, queue, request_id, audit_reason)
            .await
    }

    /// API-002 `GetQueueAdminState`.
    pub fn operator_admin_state(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
    ) -> EngineResult<QueueAdminState> {
        self.operator.admin_state(auth, queue)
    }

    /// Token-redacted operator inspection.
    pub async fn operator_inspect_item(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        key: ClientItemKey,
    ) -> EngineResult<Option<OperatorItemView>> {
        self.operator.inspect_item(auth, queue, key).await
    }

    /// API-002 `RepairItems`.
    #[allow(clippy::too_many_arguments)]
    pub async fn operator_repair(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        action: RepairAction,
        item_ids: Vec<ItemId>,
        priority: Option<PriorityValue>,
        not_before: Option<UtcTimestamp>,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<(OperatorAsyncAccept, ItemMutationResponse)> {
        self.operator
            .repair(
                auth,
                queue,
                request_id,
                action,
                item_ids,
                priority,
                not_before,
                dry_run,
                audit_reason,
            )
            .await
    }

    /// API-002 `RedriveItems`.
    #[allow(clippy::too_many_arguments)]
    pub async fn operator_redrive(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        item_ids: Vec<ItemId>,
        not_before: Option<UtcTimestamp>,
        priority: Option<PriorityValue>,
        retry_count_mode: RetryCountMode,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<(OperatorAsyncAccept, ItemMutationResponse)> {
        self.operator
            .redrive(
                auth,
                queue,
                request_id,
                item_ids,
                not_before,
                priority,
                retry_count_mode,
                dry_run,
                audit_reason,
            )
            .await
    }

    /// API-002 `PurgeQueueItems`.
    pub async fn operator_purge(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        item_ids: Vec<ItemId>,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<(OperatorAsyncAccept, ItemMutationResponse)> {
        self.operator
            .purge(auth, queue, request_id, item_ids, dry_run, audit_reason)
            .await
    }

    /// API-002 `ArchiveItems`.
    pub async fn operator_archive(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        request_id: RequestId,
        item_ids: Vec<ItemId>,
        dry_run: bool,
        audit_reason: Option<String>,
    ) -> EngineResult<(OperatorAsyncAccept, ItemMutationResponse)> {
        self.operator
            .archive(auth, queue, request_id, item_ids, dry_run, audit_reason)
            .await
    }

    /// API-002 `GetOperation`.
    pub fn operator_get_operation(
        &self,
        auth: &AuthContext,
        queue: &QueueKey,
        operation_id: &OperationId,
    ) -> EngineResult<Option<OperationHandle<OperatorOpPayload>>> {
        self.operator.get_operation(auth, queue, operation_id)
    }

    /// Redacted operator audit trail for this handle.
    pub fn operator_audit_records(&self) -> Vec<OperatorAuditRecord> {
        self.operator.audit_records()
    }
}
