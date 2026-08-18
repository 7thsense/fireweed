//! Push planner that uses [`PlannerMap`] instead of Turso `validate_push`.

use std::sync::Arc;

use fireweed_core::QueueDefinition;

use crate::planner_map::PlannerMap;
use fireweed_engine::{
    AsyncControlPlane, AsyncLogStore, AsyncPushPlan, AsyncPushPlanner, AsyncPushRequest,
    CommandChecksum, CommandEnvelope, EngineError, EngineResult, IdGen, OwnedTask, PushCommand,
    PushFingerprint, QueueCommand, QueueCounters, RawCommitRequest, RequestOutcome,
    build_push_items,
};

pub struct MapPushPlanner<C, L, I> {
    control: Arc<C>,
    log: Arc<L>,
    ids: Arc<I>,
    counters: Arc<QueueCounters>,
    node_id: u8,
    map: PlannerMap,
}

impl<C, L, I> MapPushPlanner<C, L, I> {
    pub fn from_shared(
        control: Arc<C>,
        log: Arc<L>,
        ids: Arc<I>,
        counters: Arc<QueueCounters>,
        node_id: u8,
        map: PlannerMap,
    ) -> Self {
        Self {
            control,
            log,
            ids,
            counters,
            node_id,
            map,
        }
    }
}

impl<C, L, I> AsyncPushPlanner for MapPushPlanner<C, L, I>
where
    C: AsyncControlPlane + 'static,
    L: AsyncLogStore + 'static,
    I: IdGen + 'static,
{
    fn supports_gates(&self) -> bool {
        true
    }

    fn queue_definition(
        &self,
        shard: fireweed_engine::QueueKey,
    ) -> OwnedTask<EngineResult<QueueDefinition>> {
        let control = Arc::clone(&self.control);
        Box::pin(async move { control.queue_definition(shard).await })
    }

    fn plan_push(
        &self,
        request: AsyncPushRequest,
        definition: QueueDefinition,
        fingerprint: Option<PushFingerprint>,
    ) -> OwnedTask<EngineResult<AsyncPushPlan>> {
        let log = Arc::clone(&self.log);
        let ids = Arc::clone(&self.ids);
        let counters = Arc::clone(&self.counters);
        let node_id = self.node_id;
        let map = self.map.clone();
        Box::pin(async move {
            if let (Some(request_id), Some(fingerprint)) = (request.request_id.clone(), fingerprint)
            {
                if let Some((stored, item_ids)) = map.push_replay(&request.shard, &request_id) {
                    if stored != fingerprint.legacy_body_hash.0 {
                        return Err(EngineError::RequestIdConflict);
                    }
                    return Ok(AsyncPushPlan::replay(item_ids));
                }
            }

            if map.intake_paused(&request.shard) {
                return Err(EngineError::Paused { drain_intake: true });
            }

            let epoch = fireweed_engine::resolve_write_epoch_async(request.expected_epoch, || {
                log.current_epoch(request.shard.clone())
            })
            .await?;
            let base = counters.reserve(&request.shard, epoch, request.items.len() as u32);
            let (mut items, item_ids) = build_push_items(
                request.items,
                epoch,
                node_id,
                base,
                definition.retry_policy.max_attempts,
            );
            fireweed_engine::admit_push_items_indexes(&definition, &mut items)?;
            let reserved: Vec<_> = items
                .iter()
                .map(|item| {
                    (
                        item.client_item_key.clone(),
                        item.item_id,
                        item.group_key.clone(),
                        item.priority.clone(),
                        item.not_before,
                    )
                })
                .collect();
            let replay = request.request_id.clone().map(|request_id| {
                (
                    request_id,
                    fingerprint.map(|hash| hash.legacy_body_hash.0).unwrap_or(0),
                    item_ids.clone(),
                )
            });
            // Reservation is held until dispatch_push commits or rolls back the append.
            map.reserve_push(
                &request.shard,
                &reserved,
                replay,
                definition.max_eligible_group_size,
            )?;
            // reserved_* stays set until DerivedObjectLogTursoBackend::dispatch_push
            // calls finish_push after the object-log append.

            let envelope = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: request.request_id.clone(),
                request_fingerprint: fingerprint.map(|hash| hash.legacy_body_hash.0),
                request_outcome: request.request_id.as_ref().map(|_| RequestOutcome::Push {
                    item_ids: item_ids.clone(),
                }),
                item_ids: item_ids.clone(),
                command: QueueCommand::Push(PushCommand { items }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncPushPlan::commit(
                RawCommitRequest::new(request.shard, vec![envelope], epoch),
                item_ids,
            ))
        })
    }
}
