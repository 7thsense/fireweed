// The port impls below return `-> impl Future` (the engine's port signature) with `async move` bodies —
// the deliberate codebase pattern, not convertible to bare `async fn` without changing the trait shape.
#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueueDefinition, QueueId,
    TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed, CommandChecksum,
    CommandEnvelope, CommandId, ControlPlaneStore, CreateQueueOutcome, DurabilityClass,
    EngineError, EngineResult, FinalizeCommand, FinalizeOutcome, FinalizePort, ItemView, LeaseView,
    LiveItemView, LogRead, LogWriter, ProjectionRead, ProjectionWriter, PurgePort, PushCommand,
    PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey, QueueMetrics, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimDriver, RenewLeaseCommand, RenewLeasePort, TickReport, UpsertOutcome,
    UpsertPort, build_push_items, require_item_level_claim, validate_gate_push,
};
use pqueue_objectlog::LocalObjectLog;
use pqueue_sqlite::SqliteProjectionStore;

pub struct ObjectLogSqliteBackend {
    log: LocalObjectLog,
    projection: SqliteProjectionStore,
    queues: Mutex<HashMap<QueueKey, QueueDefinition>>,
    counters: QueueCounters,
    command_seq: AtomicU64,
    node_id: u8,
    op_lock: tokio::sync::Mutex<()>,
}

impl ObjectLogSqliteBackend {
    pub fn open(object_root: impl Into<PathBuf>, projection_path: &str) -> EngineResult<Self> {
        Ok(Self {
            log: LocalObjectLog::open(object_root)?,
            projection: SqliteProjectionStore::open(projection_path)?,
            queues: Mutex::new(HashMap::new()),
            counters: QueueCounters::default(),
            command_seq: AtomicU64::new(0),
            node_id: 0,
            op_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    fn next_envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.command_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("olsqlite-{}-{n}", self.node_id)),
            request_id: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    async fn replay_queue(&self, shard: &QueueKey) -> EngineResult<()> {
        let mut from = None;
        loop {
            let page = self.log.read_from(shard, from.clone(), 256).await?;
            for (position, envelope) in &page.entries {
                for id in &envelope.item_ids {
                    self.counters.observe(shard, *id);
                }
                self.projection.apply_committed(position, envelope)?;
            }
            match page.next {
                Some(next) => from = Some(next),
                None => return Ok(()),
            }
        }
    }

    async fn append_apply(
        &self,
        shard: &QueueKey,
        envelope: CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        let epoch = match expected_epoch {
            Some(epoch) => epoch,
            None => self.log.current_epoch(shard)?,
        };
        let positions = self
            .log
            .append(shard, std::slice::from_ref(&envelope), epoch)?;
        self.projection.apply_committed(&positions[0], &envelope)
    }

    async fn require_leased(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let renderable = self.projection.claimed_view(shard, ids).await?;
        if renderable.len() == ids.len() {
            Ok(())
        } else {
            Err(EngineError::Invalid("item is not leased"))
        }
    }
}

impl Backend for ObjectLogSqliteBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }

    fn write<R, F>(&self, _f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ControlPlaneStore for ObjectLogSqliteBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            let outcome = self.log.create_queue(definition.clone())?;
            self.projection
                .create_queue_projection(definition.clone())?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.queues
                .lock()
                .expect("object-log sqlite queues poisoned")
                .insert(key.clone(), definition);
            self.replay_queue(&key).await?;
            Ok(outcome)
        }
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .queues
            .lock()
            .expect("object-log sqlite queues poisoned")
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result = self
            .queues
            .lock()
            .expect("object-log sqlite queues poisoned")
            .keys()
            .filter(|key| key.tenant_id.as_str() == tenant.as_str())
            .map(|key| key.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(self.log.current_epoch(shard))
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(self.log.acquire_epoch(shard))
    }
}

impl PushPort for ObjectLogSqliteBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            validate_gate_push(self.supports_gates(), &items)?;
            let _guard = self.op_lock.lock().await;
            let definition = self.queue_definition(shard).await?;
            let epoch = expected_epoch.unwrap_or(self.log.current_epoch(shard)?);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) = build_push_items(
                items,
                epoch,
                self.node_id,
                counter_base,
                definition.retry_policy.max_attempts,
            );
            let envelope = self.next_envelope(
                QueueCommand::Push(PushCommand { items: push_items }),
                ids.clone(),
                now,
            );
            self.append_apply(shard, envelope, Some(epoch)).await?;
            Ok(ids)
        }
    }
}

impl ClaimPort for ObjectLogSqliteBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            if req.compatibility != ClaimCompatibility::default() {
                let definition = self.queue_definition(&req.shard).await?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, &definition)?;
            }
            let candidates = self
                .projection
                .select_eligible(&req.shard, req.now, req.max_items)
                .await?;
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let envelope = self.next_envelope(
                QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                }),
                candidates.clone(),
                req.now,
            );
            self.append_apply(&req.shard, envelope, req.expected_epoch)
                .await?;
            let items = self
                .projection
                .claimed_view(&req.shard, &candidates)
                .await?;
            Ok(Claimed {
                items,
                ..Default::default()
            })
        }
    }
}

impl FinalizePort for ObjectLogSqliteBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            let item_ids: Vec<ItemId> = outcomes.iter().map(|outcome| outcome.item_id).collect();
            self.require_leased(shard, &item_ids).await?;
            let envelope = self.next_envelope(
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                item_ids,
                now,
            );
            self.append_apply(shard, envelope, expected_epoch).await
        }
    }
}

impl UpsertPort for ObjectLogSqliteBackend {
    fn replace_if_pending(
        &self,
        _shard: &QueueKey,
        _client_item_key: &ClientItemKey,
        _priority: Option<PriorityValue>,
        _group_key: Option<GroupKey>,
        _not_before: Option<UtcTimestamp>,
        _payload: Option<Bytes>,
        _fields: BTreeMap<String, Bytes>,
        _metadata: Metadata,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl RenewLeasePort for ObjectLogSqliteBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            self.require_leased(shard, &item_ids).await?;
            let envelope = self.next_envelope(
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.append_apply(shard, envelope, expected_epoch).await
        }
    }
}

impl ReassignLeasePort for ObjectLogSqliteBackend {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            self.require_leased(shard, &item_ids).await?;
            let envelope = self.next_envelope(
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.append_apply(shard, envelope, expected_epoch).await
        }
    }
}

impl PurgePort for ObjectLogSqliteBackend {
    fn purge(
        &self,
        _shard: &QueueKey,
        _item_ids: Vec<ItemId>,
        _force: bool,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ReclaimDriver for ObjectLogSqliteBackend {
    fn tick(
        &self,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        std::future::ready(Ok(TickReport::default()))
    }
}

impl ProjectionRead for ObjectLogSqliteBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.projection.select_eligible(shard, now, limit)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        self.projection.peek(shard, limit)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection.pending(shard)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<pqueue_engine::ClaimedItem>>> + Send
    {
        self.projection.claimed_view(shard, ids)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        self.projection.live_items(shard, keys)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        self.projection.metrics(queue)
    }
}
