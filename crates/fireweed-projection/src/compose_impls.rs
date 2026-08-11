//! Axis implementations over the in-process projection state machine (ADR-012, Phase 1).
//!
//! - [`MemoryLog`] — a [`fireweed_engine::LogStore`] over a per-shard [`LogData`] map (in-process command
//!   log + epoch authority). The log substrate of the composed memory backend.
//! - [`InMemoryProjection`] — a [`fireweed_engine::ProjectionStore`] over a per-shard [`ProjectionData`]
//!   map. The shared materialized read model; reused by BOTH the composed memory backend (with
//!   [`MemoryLog`]) and the composed sqlite backend (with the durable sqlite log).
//!
//! These are extracted verbatim from the `State`/`Inner` internals of the monolithic `MemoryBackend` and
//! `SqliteBackend`, so the compositions are behaviorally identical to the monoliths (proven by running the
//! shared TD-001 conformance suite against both).

use std::collections::BTreeMap;

use rustc_hash::FxHashMap;

use bytes::Bytes;
use fireweed_core::{
    BoundedMutationRequest, BoundedMutationResponse, DeclaredBucketSegmentRequest,
    DeclaredBucketSegmentResponse, GroupedAggregateRequest, GroupedAggregateResponse,
    MetricsByQueryRequest, QueryCapabilityFlags, RangeScanRequest, RangeScanResponse,
};
use fireweed_core::{
    ClientItemKey, CohortId, GroupKey, ItemId, ItemState, LeaseToken, MetadataValue,
    QueueDefinition, UtcTimestamp,
};
use fireweed_engine::{
    ActiveScope, AsOfProjectionStore, BatchUpdateItemRef, BatchUpdateSnapshotItem,
    BoundedMutationPlan, ClaimCompatibility, ClaimRef, ClaimUnit, ClaimedItem, CommandEnvelope,
    CommandPage, CommandPosition, DiscoveryGranularity, EngineError, EngineResult,
    ExpiredLeaseCursor, ExpiredLeasePage, FinalizeOutcome, InProcessLogStore,
    InProcessProjectionStore, IndexHit, ItemView, LeaseView, LiveItemView, LogStore, PendingPage,
    PendingSummary, ProjectionSnapshot, ProjectionStore, PushItem, QueueCounters, QueueKey,
    QueueMetrics, RichClaimSelection, SideRecordPage, SnapshotRef, TerminalEmissionMetrics,
    UpdateFieldsCommand,
};

use crate::{LogData, ProjectionData, ProjectionImage};

// ---------------------------------------------------------------------------
// MemoryLog — the in-process command-log axis
// ---------------------------------------------------------------------------

/// The in-process command-log axis (ADR-012): one [`LogData`] per shard (command log + epoch authority +
/// snapshots + high-water). The log substrate of the composed memory backend.
#[derive(Default)]
pub struct MemoryLog {
    logs: FxHashMap<QueueKey, LogData>,
}

impl MemoryLog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogStore for MemoryLog {
    fn is_durable_log(&self) -> bool {
        // ADR-013 Class B: the in-process log is ordering/fence authority only while the process is
        // alive. After process death the log is gone; recovery is projection-only.
        false
    }

    fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()> {
        self.logs.entry(shard.clone()).or_default();
        Ok(())
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        self.logs
            .get(shard)
            .map(|l| l.epoch())
            .ok_or(EngineError::NotFound)
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        self.logs
            .get_mut(shard)
            .map(|l| l.advance_epoch())
            .ok_or(EngineError::NotFound)
    }

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

    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> EngineResult<CommandPage> {
        Ok(self
            .logs
            .get(shard)
            .ok_or(EngineError::NotFound)?
            .read_from(shard, from, limit))
    }

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        Ok(self.logs.get(shard).and_then(|l| l.high_water()))
    }

    fn set_high_water(&mut self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()> {
        self.logs
            .get_mut(shard)
            .ok_or(EngineError::NotFound)?
            .set_high_water(position)
    }

    fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef> {
        Ok(self
            .logs
            .get_mut(shard)
            .ok_or(EngineError::NotFound)?
            .write_snapshot(shard, position, snapshot))
    }

    fn latest_snapshot(&self, shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
        Ok(self.logs.get(shard).and_then(|l| l.latest_snapshot()))
    }

    fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        self.logs
            .get(&snapshot_ref.queue)
            .ok_or(EngineError::NotFound)?
            .read_snapshot(snapshot_ref)
    }

    fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> EngineResult<Option<SnapshotRef>> {
        Ok(self
            .logs
            .get(shard)
            .and_then(|log| log.snapshot_at_or_before(position)))
    }
}

/// Runtime-neutral async view of the synchronous in-memory log.
///
/// Memory uses the explicit immediate adapter at the storage boundary instead of maintaining a second,
/// independently locked implementation of the same axis.
pub type AsyncMemoryLog = InProcessLogStore<MemoryLog>;

// ---------------------------------------------------------------------------
// InMemoryProjection — the in-memory materialized-read-model axis
// ---------------------------------------------------------------------------

/// The in-memory projection axis (ADR-012): one [`ProjectionData`] per shard. The shared read model used
/// by every log-replay composition (memory + sqlite-log), so the two backends are byte-for-byte identical
/// on the projection.
#[derive(Default)]
pub struct InMemoryProjection {
    projections: FxHashMap<QueueKey, ProjectionData>,
}

impl InMemoryProjection {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bounded page of authoritative pending order after an optional cursor item.
    ///
    /// Used by recovery fingerprint paths (TP-002 E3) that must page the full live
    /// order without materializing the entire resident set in one call.
    pub fn peek_page(
        &self,
        shard: &QueueKey,
        after: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<Vec<ItemView>> {
        Ok(self.get(shard)?.peek_page(after, limit))
    }

    /// Replace one in-memory shard with a fully materialized projection image.
    pub fn hydrate_shard(
        &mut self,
        definition: &QueueDefinition,
        image: ProjectionImage,
    ) -> EngineResult<()> {
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let expected_metrics = image.metrics.clone();
        let projection = ProjectionData::from_image(definition, image)?;
        if projection.metrics() != expected_metrics {
            return Err(EngineError::Storage(format!(
                "projection hydration metrics mismatch: image {:?}, rebuilt {:?}",
                expected_metrics,
                projection.metrics()
            )));
        }
        // Parsing and parity validation happen against private state; insertion is the only mutation.
        self.projections.insert(key, projection);
        Ok(())
    }

    pub fn apply_borrowed(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, cmd) in positions.iter().zip(commands) {
            self.projections
                .get_mut(&pos.queue)
                .ok_or(EngineError::NotFound)?
                .apply_command_at(Some(cmd.created_at), Some(pos), &cmd.command)?;
        }
        Ok(())
    }

    fn get(&self, shard: &QueueKey) -> EngineResult<&ProjectionData> {
        self.projections.get(shard).ok_or(EngineError::NotFound)
    }

    fn get_mut(&mut self, shard: &QueueKey) -> EngineResult<&mut ProjectionData> {
        self.projections.get_mut(shard).ok_or(EngineError::NotFound)
    }

    pub fn observe_item_counters(
        &self,
        shard: &QueueKey,
        counters: &QueueCounters,
    ) -> EngineResult<()> {
        self.get(shard)?.observe_item_counters(shard, counters);
        Ok(())
    }

    fn metadata_matches(
        projection: &ProjectionData,
        item_id: &ItemId,
        required: &BTreeMap<String, MetadataValue>,
    ) -> bool {
        projection.items.get(item_id).is_some_and(|item| {
            required
                .iter()
                .all(|(key, value)| item.metadata.get(key) == Some(value))
        })
    }

    fn select_group_batching(
        projection: &ProjectionData,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> EngineResult<Vec<ItemId>> {
        let max_groups = compatibility
            .group_batching
            .as_ref()
            .map(|batching| batching.max_groups as usize)
            .unwrap_or(0);
        if max_groups == 0 || max_items == 0 {
            return Ok(Vec::new());
        }

        let mut groups = Vec::<(GroupKey, Vec<ItemId>)>::new();
        for item_id in projection.eligible_candidates(now, usize::MAX) {
            let item = projection
                .items
                .get(&item_id)
                .expect("eligibility index references a live item");
            let Some(group_key) = item.group_key.clone() else {
                continue;
            };
            if item.cohort_size.is_some()
                || !Self::metadata_matches(projection, &item_id, &compatibility.metadata_equals)
            {
                continue;
            }
            if projection.items.values().any(|member| {
                member.group_key.as_ref() == Some(&group_key)
                    && member.cohort_size.is_none()
                    && !member.superseded
                    && member.state == ItemState::Leased
            }) {
                continue;
            }
            match groups.iter_mut().find(|(group, _)| group == &group_key) {
                Some((_, items)) => items.push(item_id),
                None => groups.push((group_key, vec![item_id])),
            }
        }

        let mut selected = Vec::new();
        for (_, group) in groups.into_iter().take(max_groups) {
            if group.len() > max_items {
                return Err(EngineError::BatchTooLarge);
            }
            if selected.len().saturating_add(group.len()) > max_items {
                break;
            }
            selected.extend(group);
        }
        Ok(selected)
    }

    fn select_same_group(
        projection: &ProjectionData,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> Vec<ItemId> {
        let mut selected_group = None::<GroupKey>;
        let mut selected = Vec::new();
        for item_id in projection.eligible_candidates(now, usize::MAX) {
            let item = projection
                .items
                .get(&item_id)
                .expect("eligibility index references a live item");
            let Some(group_key) = item.group_key.as_ref() else {
                continue;
            };
            if item.cohort_size.is_some()
                || compatibility
                    .group_key
                    .as_ref()
                    .is_some_and(|required| required != group_key)
                || !Self::metadata_matches(projection, &item_id, &compatibility.metadata_equals)
            {
                continue;
            }
            match &selected_group {
                None => selected_group = Some(group_key.clone()),
                Some(selected) if selected != group_key => continue,
                Some(_) => {}
            }
            selected.push(item_id);
            if selected.len() == max_items {
                break;
            }
        }
        selected
    }

    fn select_whole_cohort(
        projection: &ProjectionData,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> EngineResult<RichClaimSelection> {
        if projection.paused {
            return Ok(RichClaimSelection::default());
        }
        let eligible = projection
            .eligible_candidates(now, usize::MAX)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let mut cohorts = BTreeMap::<GroupKey, Vec<_>>::new();
        for item in projection.items.values().filter(|item| {
            !item.superseded && !item.state.is_terminal() && item.cohort_size.is_some()
        }) {
            let Some(group) = item.group_key.clone() else {
                continue;
            };
            cohorts.entry(group).or_default().push(item);
        }
        let mut complete = cohorts
            .into_iter()
            .filter_map(|(group, mut members)| {
                members.sort_by_key(|item| item.created_seq);
                let declared = members.first()?.cohort_size?;
                let complete = members.len() as u64 == declared
                    && members.iter().all(|item| {
                        item.cohort_size == Some(declared)
                            && eligible.contains(&item.item_id)
                            && Self::metadata_matches(
                                projection,
                                &item.item_id,
                                &compatibility.metadata_equals,
                            )
                    });
                complete.then(|| (members[0].created_seq, group, members))
            })
            .collect::<Vec<_>>();
        complete.sort_by(|(left_seq, left_group, _), (right_seq, right_group, _)| {
            left_seq
                .cmp(right_seq)
                .then_with(|| left_group.cmp(right_group))
        });
        let Some((created_seq, group, members)) = complete.into_iter().next() else {
            return Ok(RichClaimSelection::default());
        };
        if members.len() > max_items {
            return Err(EngineError::BatchTooLarge);
        }
        let cohort_id = CohortId::new(format!("coh:{}:{created_seq}", group.as_str()))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        let member_ids = members
            .into_iter()
            .map(|member| member.item_id)
            .collect::<std::collections::BTreeSet<_>>();
        let item_ids = projection
            .eligible_candidates(now, usize::MAX)
            .into_iter()
            .filter(|item_id| member_ids.contains(item_id))
            .collect();
        Ok(RichClaimSelection {
            item_ids,
            cohort_id: Some(cohort_id),
        })
    }
}

impl ProjectionStore for InMemoryProjection {
    fn supports_gates(&self) -> bool {
        true
    }

    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<ActiveScope>> {
        Ok(self
            .get(shard)?
            .discover_active_scopes(shard.queue_id.as_str(), granularity, now))
    }

    fn select_rich_claim(
        &self,
        shard: &QueueKey,
        unit: ClaimUnit,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> EngineResult<RichClaimSelection> {
        let projection = self.get(shard)?;
        let item_ids = match unit {
            ClaimUnit::Item => return Err(EngineError::Unavailable),
            ClaimUnit::WholeGroup => {
                Self::select_group_batching(projection, compatibility, now, max_items)?
            }
            ClaimUnit::SameGroupKey => {
                Self::select_same_group(projection, compatibility, now, max_items)
            }
            ClaimUnit::WholeCohort => {
                return Self::select_whole_cohort(projection, compatibility, now, max_items);
            }
        };
        Ok(RichClaimSelection {
            item_ids,
            cohort_id: None,
        })
    }

    fn hot_projection_capabilities(&self) -> QueryCapabilityFlags {
        QueryCapabilityFlags {
            range_scan: true,
            grouped_aggregate: true,
            declared_bucket_segment: true,
            bounded_mutation: true,
            claim_by_query: true,
            side_record_query: false,
            claim_by_item_ids: true,
        }
    }

    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        self.projections.entry(key).or_insert_with(|| {
            ProjectionData::new(
                definition.priority_model,
                definition.ordering_mode,
                definition.max_rank_error,
                definition.recurrence,
                &definition.secondary_indexes,
            )
            .with_typed_indexes(&definition.typed_indexes)
            .with_eligibility_policy(&definition.eligibility_policy)
        });
        Ok(())
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.apply_borrowed(positions, commands)
    }

    fn apply_live_owned(
        &mut self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> EngineResult<()> {
        if positions.len() != commands.len() {
            return Err(EngineError::Storage(
                "apply_live_owned: positions/commands length mismatch".into(),
            ));
        }
        for (pos, cmd) in positions.iter().zip(commands) {
            self.projections
                .get_mut(&pos.queue)
                .ok_or(EngineError::NotFound)?
                .apply_command_owned_at(Some(cmd.created_at), Some(pos), cmd.command)?;
        }
        Ok(())
    }

    fn install_recovery_shard(
        &mut self,
        definition: &QueueDefinition,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        if positions.len() != commands.len() {
            return Err(EngineError::Storage(
                "recovery install positions/commands length mismatch".into(),
            ));
        }
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let mut replacement = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        )
        .with_typed_indexes(&definition.typed_indexes)
        .with_eligibility_policy(&definition.eligibility_policy);
        for (position, command) in positions.iter().zip(commands) {
            replacement.apply_command_at(
                Some(command.created_at),
                Some(position),
                &command.command,
            )?;
        }
        // All fallible materialization happened against the private replacement. This insertion is the only
        // serving-state mutation and cannot fail, so create-loser hydration is atomic from readers' view.
        self.projections.insert(key, replacement);
        Ok(())
    }

    fn pause_blocks_intake(&self, shard: &QueueKey) -> EngineResult<bool> {
        Ok(self.get(shard)?.is_intake_blocked())
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        Ok(self.get(shard)?.eligible_candidates(now, max))
    }

    /// Item-level claim selection with optional `group_key` / `metadata_equals` fences (API-001).
    ///
    /// Matches the v0.23.3 relational filter: eligible pending non-cohort items, ordered by the
    /// projection's eligibility index, restricted to an exact group when requested and to a conjunctive
    /// metadata equality fence. Without either predicate this is exactly [`eligible_candidates`].
    fn select_item_claim(
        &self,
        shard: &QueueKey,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        if compatibility.group_key.is_none() && compatibility.metadata_equals.is_empty() {
            return self.eligible_candidates(shard, now, max);
        }
        let projection = self.get(shard)?;
        let mut selected = Vec::new();
        for item_id in projection.eligible_candidates(now, usize::MAX) {
            let item = projection
                .items
                .get(&item_id)
                .expect("eligibility index references a live item");
            // Item-unit claims never lease cohort members; whole_cohort is a separate claim unit.
            if item.cohort_size.is_some() {
                continue;
            }
            if compatibility
                .group_key
                .as_ref()
                .is_some_and(|required| item.group_key.as_ref() != Some(required))
            {
                continue;
            }
            if !Self::metadata_matches(projection, &item_id, &compatibility.metadata_equals) {
                continue;
            }
            selected.push(item_id);
            if selected.len() == max {
                break;
            }
        }
        Ok(selected)
    }

    fn eligible_candidates_after(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        after: Option<ItemId>,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        Ok(self.get(shard)?.eligible_candidates_after(now, after, max))
    }

    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        Ok(self.get(shard)?.render_claimed(ids))
    }

    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        Ok(self.get(shard)?.lookup_by_key(client_item_key))
    }

    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>> {
        Ok(self.get(shard)?.item_state(id))
    }

    fn classify_claim_by_item_id(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        now: UtcTimestamp,
    ) -> EngineResult<fireweed_core::ClaimByItemIdClass> {
        Ok(self.get(shard)?.classify_claim_by_item_id(id, now))
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
        Ok(self.get(shard)?.item_version(id))
    }

    fn batch_update_snapshot(
        &self,
        shard: &QueueKey,
        refs: &[BatchUpdateItemRef],
    ) -> EngineResult<Vec<BatchUpdateSnapshotItem>> {
        Ok(self.get(shard)?.batch_update_snapshot(refs))
    }

    fn batch_update_preflight(
        &self,
        shard: &QueueKey,
        commands: &[UpdateFieldsCommand],
    ) -> EngineResult<Vec<bool>> {
        self.get(shard)?.batch_update_preflight(commands)
    }

    fn plan_item_mutation(
        &self,
        shard: &QueueKey,
        request: &fireweed_engine::ItemMutationRequest,
    ) -> EngineResult<fireweed_engine::ItemMutationPlan> {
        self.get(shard)?.plan_item_mutation(request)
    }

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        Ok(self.get(shard)?.expired_leases(now))
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        self.projections
            .iter()
            .filter_map(|(shard, proj)| {
                let ids = proj.expired_leases(now);
                (!ids.is_empty()).then(|| (shard.clone(), ids))
            })
            .collect()
    }

    fn expired_leases_page(
        &self,
        now: UtcTimestamp,
        cursor: Option<&ExpiredLeaseCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<ExpiredLeasePage> {
        if limit == 0 {
            return Err(EngineError::Invalid(
                "expired lease page limit must be nonzero",
            ));
        }
        let after = cursor.map(ExpiredLeaseCursor::row_parts).transpose()?;
        let after = after
            .map(|(expiry, tenant, queue, item)| {
                Ok::<_, EngineError>((
                    UtcTimestamp::new(
                        expiry.div_euclid(1_000_000_000),
                        expiry.rem_euclid(1_000_000_000) as u32,
                    )
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
                    tenant,
                    queue,
                    ItemId::new(item).map_err(|error| EngineError::Storage(error.to_string()))?,
                ))
            })
            .transpose()?;
        let mut rows =
            Vec::<(UtcTimestamp, QueueKey, ItemId)>::with_capacity(limit.saturating_add(1));
        for (queue, projection) in &self.projections {
            if worker_partition.is_some_and(|(index, partitions)| {
                fireweed_engine::queue_worker_partition(queue, partitions) != index
            }) {
                continue;
            }
            let queue_order = after.as_ref().map(|(_, tenant, queue_id, _)| {
                (queue.tenant_id.as_str(), queue.queue_id.as_str())
                    .cmp(&(tenant.as_str(), queue_id.as_str()))
            });
            let projection_after = after
                .as_ref()
                .zip(queue_order)
                .map(|((expiry, _, _, item), order)| (*expiry, order, item));
            let (projection_rows, _) =
                projection.expired_leases_after(now, projection_after, limit.saturating_add(1));
            rows.extend(
                projection_rows
                    .into_iter()
                    .map(|(expiry, item)| (expiry, queue.clone(), item)),
            );
            rows.sort_unstable_by(
                |(left_expiry, left_queue, left_item), (right_expiry, right_queue, right_item)| {
                    (
                        left_expiry,
                        &left_queue.tenant_id,
                        &left_queue.queue_id,
                        left_item,
                    )
                        .cmp(&(
                            right_expiry,
                            &right_queue.tenant_id,
                            &right_queue.queue_id,
                            right_item,
                        ))
                },
            );
            rows.truncate(limit.saturating_add(1));
        }
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next = has_more.then(|| {
            let (expiry, queue, item) = rows.last().expect("nonzero bounded page");
            ExpiredLeaseCursor::from_row(
                expiry
                    .seconds
                    .saturating_mul(1_000_000_000)
                    .saturating_add(expiry.nanoseconds as i64),
                queue,
                item,
            )
        });
        let mut leases = Vec::<(QueueKey, Vec<ItemId>)>::new();
        for (_, queue, item) in rows {
            match leases.last_mut() {
                Some((last, ids)) if *last == queue => ids.push(item),
                _ => leases.push((queue, vec![item])),
            }
        }
        Ok(ExpiredLeasePage { leases, next })
    }

    fn finalize_validate(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<()> {
        self.get(shard)?.finalize_validate(outcomes)
    }

    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        self.get(shard)?.renew_validate(ids)
    }

    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        self.get(shard)?.reassign_validate(ids)
    }

    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        self.get(shard)?
            .update_fields_validate(id, expected_item_version)
    }

    fn index_validate(
        &self,
        shard: &QueueKey,
        item_id: &ItemId,
        fields: &BTreeMap<String, Bytes>,
        entity: Option<&serde_json::Value>,
        exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        self.get(shard)?
            .index_validate_with_entity(item_id, fields, entity, exclude)
    }

    fn index_validate_push(&self, shard: &QueueKey, items: &[PushItem]) -> EngineResult<()> {
        self.get(shard)?.index_validate_push(items)
    }

    fn index_validate_replace(
        &self,
        shard: &QueueKey,
        existing_id: &ItemId,
        item: &PushItem,
    ) -> EngineResult<()> {
        self.get(shard)?.index_validate_replace(existing_id, item)
    }

    fn index_validate_update(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        field_ops: &BTreeMap<String, Option<Bytes>>,
        entity: Option<&serde_json::Value>,
    ) -> EngineResult<()> {
        self.get(shard)?
            .index_validate_update_with_entity(id, field_ops, entity)
    }

    // -- commit-class: the in-memory projection materializes the full Snorri commit-class read model
    //    (side records + instance fences + lease-token/version commit validation), lifted verbatim from
    //    `ProjectionData`, so the composed memory backend reaches capability parity with `MemoryBackend`.

    /// After full-log recovery into an empty in-memory image, seed mint counters past every
    /// materialised item id so a post-reopen push never re-mints a live id (fireweed-6e38e2b4).
    fn restore_counters(&self, shard: &QueueKey, counters: &QueueCounters) -> EngineResult<()> {
        self.observe_item_counters(shard, counters)
    }

    fn supports_commit_transition(&self) -> bool {
        true
    }

    fn commit_validate(
        &self,
        shard: &QueueKey,
        refs: &[ClaimRef],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.get(shard)?.commit_validate(refs, now)
    }

    fn instance_fence(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<u64>> {
        Ok(self.get(shard)?.instance_fence(key))
    }

    fn side_record(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        Ok(self.get(shard)?.side_record(key).cloned())
    }

    fn side_records_by_prefix(
        &self,
        shard: &QueueKey,
        prefix: &[u8],
        page_size: usize,
        cursor: Option<Vec<u8>>,
    ) -> EngineResult<SideRecordPage> {
        Ok(self
            .get(shard)?
            .side_records_by_prefix(prefix, page_size, cursor))
    }

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>> {
        Ok(self.get(shard)?.select_eligible(now, limit))
    }

    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        Ok(self.get(shard)?.peek(limit))
    }

    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        Ok(self.get(shard)?.pending_leases())
    }

    fn pending_summary(&self, shard: &QueueKey) -> EngineResult<PendingSummary> {
        Ok(self.get(shard)?.pending_summary())
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<PendingPage> {
        Ok(self.get(shard)?.pending_page(start, limit))
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> EngineResult<Vec<LeaseView>> {
        Ok(self.get(shard)?.pending_range(start, end, consumer, limit))
    }

    fn pending_by_ids(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<LeaseView>> {
        Ok(self.get(shard)?.pending_by_ids(ids))
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        Ok(self.get(shard)?.metrics())
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<TerminalEmissionMetrics> {
        Ok(self
            .get(shard)?
            .terminal_emission_metrics(now, emit_change_records, emission_cursor))
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        Ok(self.get(shard)?.live_items_by_key(keys))
    }

    fn reap_terminal_items(
        &mut self,
        shard: &QueueKey,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<Vec<ItemId>> {
        Ok(self.get_mut(shard)?.reap_terminal_items(
            now,
            terminal_retention_ms,
            emit_change_records,
            emission_cursor,
        ))
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> EngineResult<RangeScanResponse> {
        let _ = shard;
        self.get(shard)?.range_scan(request)
    }

    fn select_claim_by_query(
        &self,
        shard: &QueueKey,
        index: Option<&str>,
        filters: &[fireweed_core::QueryFilter],
        order_by: &fireweed_core::OrderField,
        max_items: usize,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<ItemId>> {
        self.get(shard)?
            .select_claim_by_query(index, filters, order_by, max_items, now)
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> EngineResult<GroupedAggregateResponse> {
        self.get(shard)?.grouped_aggregate(request)
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
        self.get(shard)?.metrics_by_query(request)
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> EngineResult<DeclaredBucketSegmentResponse> {
        self.get(shard)?.declared_bucket_segment(request)
    }

    fn bounded_mutation(
        &mut self,
        shard: &QueueKey,
        request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationResponse> {
        self.get_mut(shard)?.bounded_mutation(request)
    }

    fn plan_bounded_mutation(
        &self,
        shard: &QueueKey,
        request: BoundedMutationRequest,
    ) -> EngineResult<BoundedMutationPlan> {
        self.get(shard)?.plan_bounded_mutation(request)
    }

    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        self.get(shard)?.index_get_unique(index, key)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        self.get(shard)?.index_lookup(index, key)
    }
}

/// Runtime-neutral async view of the synchronous in-memory projection.
pub type AsyncInMemoryProjection = InProcessProjectionStore<InMemoryProjection>;

impl AsOfProjectionStore for InMemoryProjection {
    type AsOfProjection = InMemoryProjection;

    fn reconstruct_as_of(
        &self,
        definition: &QueueDefinition,
        snapshot: Option<ProjectionSnapshot>,
    ) -> EngineResult<Self::AsOfProjection> {
        let mut projection = InMemoryProjection::new();
        ProjectionStore::ensure_shard(&mut projection, definition)?;
        if let Some(snapshot) = snapshot {
            let image = ProjectionImage::from_bytes(&snapshot.payload)?;
            projection.hydrate_shard(definition, image)?;
        }
        Ok(projection)
    }
}

#[cfg(test)]
mod async_axis_tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use fireweed_core::{QueueId, TenantId};
    use fireweed_engine::{AsyncLogStore, AsyncProjectionStore};

    use super::*;

    fn one_poll<F: Future>(future: F) -> F::Output {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("in-memory async axis must resolve in one poll"),
        }
    }

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    fn assert_send<T: Send>(_: T) {}

    #[test]
    fn memory_log_async_axis_is_send_one_poll_and_sync_equivalent() {
        let log = AsyncMemoryLog::new(MemoryLog::new());
        assert_send(AsyncLogStore::ensure_shard(&log, shard()));
        assert!(one_poll(AsyncLogStore::ensure_shard(&log, shard())).is_ok());
        assert_eq!(
            one_poll(AsyncLogStore::current_epoch(&log, shard())).unwrap(),
            0
        );
        assert_eq!(
            one_poll(AsyncLogStore::acquire_epoch(&log, shard())).unwrap(),
            1
        );
        assert_eq!(
            one_poll(AsyncLogStore::current_epoch(&log, shard())).unwrap(),
            1
        );
    }

    #[test]
    fn in_memory_projection_async_axis_is_send_one_poll_and_sync_equivalent() {
        let projection = AsyncInMemoryProjection::new(InMemoryProjection::new());
        assert_send(AsyncProjectionStore::recover_definitions(&projection));
        assert_eq!(
            one_poll(AsyncProjectionStore::recover_definitions(&projection)).unwrap(),
            Vec::<QueueDefinition>::new()
        );
        assert_eq!(
            AsyncProjectionStore::supports_gates(&projection),
            ProjectionStore::supports_gates(&InMemoryProjection::new())
        );
    }

    #[test]
    fn select_item_claim_honors_group_key_and_metadata_equals() {
        use fireweed_core::{
            ClientItemKey, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
            PriorityModelKind, PriorityTieBreaker, PriorityValue, RecurrencePolicy, RetryPolicy,
        };
        use fireweed_engine::{CommandChecksum, CommandId, PushCommand, QueueCommand};

        let mut projection = InMemoryProjection::new();
        let definition = QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new("queue").unwrap(),
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
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records: true,
        };
        ProjectionStore::ensure_shard(&mut projection, &definition).unwrap();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let east_match = ItemId::mint(1, 0, 0);
        let east_wrong_group = ItemId::mint(1, 0, 1);
        let west = ItemId::mint(1, 0, 2);
        let group_a = GroupKey::new("group-a").unwrap();
        let group_b = GroupKey::new("group-b").unwrap();
        let mut east = fireweed_core::Metadata::new();
        east.insert("region", MetadataValue::String("east".into()));
        let mut west_md = fireweed_core::Metadata::new();
        west_md.insert("region", MetadataValue::String("west".into()));
        let items = vec![
            PushItem {
                client_item_key: ClientItemKey::new("east-a").unwrap(),
                item_id: east_match,
                priority: Some(PriorityValue::Int64(1)),
                not_before: None,
                group_key: Some(group_a.clone()),
                max_attempts: 3,
                payload: None,
                fields: Default::default(),
                metadata: east.clone(),
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: None,
            },
            PushItem {
                client_item_key: ClientItemKey::new("east-b").unwrap(),
                item_id: east_wrong_group,
                priority: Some(PriorityValue::Int64(2)),
                not_before: None,
                group_key: Some(group_b),
                max_attempts: 3,
                payload: None,
                fields: Default::default(),
                metadata: east,
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: None,
            },
            PushItem {
                client_item_key: ClientItemKey::new("west-a").unwrap(),
                item_id: west,
                priority: Some(PriorityValue::Int64(0)),
                not_before: None,
                group_key: Some(group_a.clone()),
                max_attempts: 3,
                payload: None,
                fields: Default::default(),
                metadata: west_md,
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: None,
            },
        ];
        let envelope = CommandEnvelope {
            command_id: CommandId::new("push"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![east_match, east_wrong_group, west],
            command: QueueCommand::Push(PushCommand { items }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(10, 0).unwrap(),
        };
        ProjectionStore::apply(
            &mut projection,
            &[CommandPosition::new(shard.clone(), 1, 0)],
            &[envelope],
        )
        .unwrap();

        let compatibility = ClaimCompatibility {
            group_key: Some(group_a),
            metadata_equals: BTreeMap::from([(
                "region".to_string(),
                MetadataValue::String("east".into()),
            )]),
            ..ClaimCompatibility::default()
        };
        let selected = ProjectionStore::select_item_claim(
            &projection,
            &shard,
            &compatibility,
            UtcTimestamp::new(10, 0).unwrap(),
            10,
        )
        .unwrap();
        assert_eq!(selected, vec![east_match]);

        let via_async = AsyncInMemoryProjection::new(projection);
        let async_selected = one_poll(AsyncProjectionStore::select_item_claim(
            &via_async,
            shard,
            compatibility,
            UtcTimestamp::new(10, 0).unwrap(),
            10,
        ))
        .unwrap();
        assert_eq!(async_selected, vec![east_match]);
    }
}
