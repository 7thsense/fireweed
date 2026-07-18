//! Runtime-neutral asynchronous storage axes.
//!
//! These traits are the additive migration surface for the first async [`crate::ComposedBackend`]
//! conversion. They intentionally cover the initial shard, append/replay, projection-apply, basic claim,
//! and control-plane paths only. Retention, snapshots, group commit, rich relational claims, indexes, and
//! repair operations remain on the legacy axes until the slice that migrates each behavior and its tests.
//!
//! Every request value is owned. Shared receivers allow independent queue/connection work to progress
//! without requiring a process-global mutable store borrow; implementations provide their own per-queue or
//! per-connection synchronization. An implementation may borrow `self` while its future is alive, but may not
//! borrow caller-owned command buffers, identifiers, or database transactions across a suspension point.
//! The returned futures are `Send` and expose no executor type.
//!
//! # Cancellation and transaction ownership
//!
//! Dropping a future before its durable commit point must leave no effect. A backend that can suspend while
//! committing transfers the owned operation and its connection/transaction capability to backend-owned
//! execution before that suspension; dropping the caller then discards only the response. Atomic stores
//! commit append, projection apply, cursor/frontier, and replay outcome together. Eventual-apply stores may
//! make append durable first, but must preserve the response barrier and repair the projection by replay.
//!
//! Blocking implementations must offload one complete transaction below this boundary. They must not hold a
//! `std::sync::MutexGuard` or borrowed blocking transaction across an `.await`, and must not offload
//! individual statements belonging to the same transaction.

use pqueue_core::{ItemId, ItemState, QueueDefinition, QueueId, TenantId, UtcTimestamp};

use crate::{
    ClaimedItem, CommandEnvelope, CommandPage, CommandPosition, CreateQueueOutcome,
    DurabilityClass, EngineResult, QueueKey,
};

/// Native-async command-log, epoch-fence, replay, and high-water operations needed by initial composition.
pub trait AsyncLogStore: Send + Sync {
    /// Immutable after construction; implementations must not acquire an async lock here.
    fn durability_class(&self) -> DurabilityClass;

    fn ensure_shard(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    fn current_epoch(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;

    fn acquire_epoch(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;

    /// Append one owned batch under the exact expected epoch.
    ///
    /// Returning success means the positions are durable according to [`Self::durability_class`]. If the
    /// call can suspend at its commit point, its surrounding backend-owned commit task must continue after
    /// caller cancellation and retain a replay-resolvable outcome.
    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommandPosition>>> + Send;

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send;

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send;

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;
}

/// Native-async projection operations needed by initial append/apply, recovery, and item claim paths.
pub trait AsyncProjectionStore: Send + Sync {
    /// Immutable after construction; implementations must not acquire an async lock here.
    fn supports_gates(&self) -> bool {
        false
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    /// Fail-closed mutation admission immediately before the append/apply unit begins.
    fn admit_mutation(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    /// Apply a committed owned batch to the live serving image.
    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    /// Apply an owned replay batch and durably advance the projection recovery frontier with it.
    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send;

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<ItemState>>> + Send;

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send;

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send;

    /// Enumerate definitions persisted by a durable projection during recovery-on-open.
    fn recover_definitions(
        &self,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send;
}

/// Native-async queue-definition control plane. Assignment epochs remain authoritative on the log axis.
pub trait AsyncControlPlane: Send + Sync {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send;

    fn queue_definition(
        &self,
        key: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send;

    fn list_queues(
        &self,
        tenant: TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send;
}

#[cfg(test)]
mod tests {
    // The concrete `Ready` return types make these compile-time assertions fail if a future ceases to be
    // `Send`; production implementations remain free to return their own opaque future types.
    #![allow(refining_impl_trait)]

    use std::future::{Ready, ready};

    use pqueue_core::{
        EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
        PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    };

    use super::*;
    use crate::{EngineError, QueueKey};

    struct ImmediateLog;

    impl AsyncLogStore for ImmediateLog {
        fn durability_class(&self) -> DurabilityClass {
            DurabilityClass::Atomic
        }

        fn ensure_shard(&self, _shard: QueueKey) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn current_epoch(&self, _shard: QueueKey) -> Ready<EngineResult<u64>> {
            ready(Ok(0))
        }

        fn acquire_epoch(&self, _shard: QueueKey) -> Ready<EngineResult<u64>> {
            ready(Ok(1))
        }

        fn append(
            &self,
            _shard: QueueKey,
            _commands: Vec<CommandEnvelope>,
            _expected_epoch: u64,
        ) -> Ready<EngineResult<Vec<CommandPosition>>> {
            ready(Ok(Vec::new()))
        }

        fn read_from(
            &self,
            _shard: QueueKey,
            _from: Option<CommandPosition>,
            _limit: usize,
        ) -> Ready<EngineResult<CommandPage>> {
            ready(Ok(CommandPage {
                entries: Vec::new(),
                next: None,
            }))
        }

        fn high_water(&self, _shard: QueueKey) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Ok(None))
        }

        fn set_high_water(
            &self,
            _shard: QueueKey,
            _position: CommandPosition,
        ) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }
    }

    struct ImmediateProjection;

    impl AsyncProjectionStore for ImmediateProjection {
        fn ensure_shard(&self, _definition: QueueDefinition) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn admit_mutation(&self, _shard: QueueKey) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn apply_live(
            &self,
            _positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn apply_recovery(
            &self,
            _positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn eligible_candidates(
            &self,
            _shard: QueueKey,
            _now: UtcTimestamp,
            _max: usize,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            ready(Ok(Vec::new()))
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _ids: Vec<ItemId>,
        ) -> Ready<EngineResult<Vec<ClaimedItem>>> {
            ready(Ok(Vec::new()))
        }

        fn item_state(
            &self,
            _shard: QueueKey,
            _id: ItemId,
        ) -> Ready<EngineResult<Option<ItemState>>> {
            ready(Ok(None))
        }

        fn item_version(&self, _shard: QueueKey, _id: ItemId) -> Ready<EngineResult<Option<u64>>> {
            ready(Ok(None))
        }

        fn recovery_high_water(
            &self,
            _shard: QueueKey,
        ) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Ok(None))
        }

        fn recover_definitions(&self) -> Ready<EngineResult<Vec<QueueDefinition>>> {
            ready(Ok(Vec::new()))
        }
    }

    struct ImmediateControl;

    impl AsyncControlPlane for ImmediateControl {
        fn create_queue(
            &self,
            _definition: QueueDefinition,
        ) -> Ready<EngineResult<CreateQueueOutcome>> {
            ready(Err(EngineError::Unavailable))
        }

        fn queue_definition(&self, _key: QueueKey) -> Ready<EngineResult<QueueDefinition>> {
            ready(Err(EngineError::NotFound))
        }

        fn list_queues(&self, _tenant: TenantId) -> Ready<EngineResult<Vec<QueueId>>> {
            ready(Ok(Vec::new()))
        }
    }

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    fn definition() -> QueueDefinition {
        QueueDefinition {
            tenant_id: shard().tenant_id,
            queue_id: shard().queue_id,
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

    fn assert_send<T: Send>(_: T) {}

    #[test]
    fn every_log_future_is_send() {
        let log = ImmediateLog;
        assert_send(log.ensure_shard(shard()));
        assert_send(log.current_epoch(shard()));
        assert_send(log.acquire_epoch(shard()));
        assert_send(log.append(shard(), Vec::new(), 0));
        assert_send(log.read_from(shard(), None, 1));
        assert_send(log.high_water(shard()));
        assert_send(log.set_high_water(shard(), CommandPosition::new(shard(), 0, 0)));
    }

    #[test]
    fn every_projection_future_is_send() {
        let projection = ImmediateProjection;
        assert_send(projection.ensure_shard(definition()));
        assert_send(projection.admit_mutation(shard()));
        assert_send(projection.apply_live(Vec::new(), Vec::new()));
        assert_send(projection.apply_recovery(Vec::new(), Vec::new()));
        assert_send(projection.eligible_candidates(shard(), UtcTimestamp::new(0, 0).unwrap(), 1));
        let id = ItemId::new("1").unwrap();
        assert_send(projection.render_claimed(shard(), vec![id]));
        assert_send(projection.item_state(shard(), ItemId::new("1").unwrap()));
        assert_send(projection.item_version(shard(), ItemId::new("1").unwrap()));
        assert_send(projection.recovery_high_water(shard()));
        assert_send(projection.recover_definitions());
    }

    #[test]
    fn every_control_plane_future_is_send() {
        let control = ImmediateControl;
        assert_send(control.create_queue(definition()));
        assert_send(control.queue_definition(shard()));
        assert_send(control.list_queues(TenantId::new("tenant").unwrap()));
    }
}
