//! Native-async object-log then derived-projection commit capability.

use std::sync::Arc;

use pqueue_engine::{
    AsyncLogStore, AsyncProjectionStore, EngineError, EngineResult, KeyedQueueGate, OwnedTask,
    QueueKey, RawCommitFault, RawCommitOutcome, RawCommitRequest, SeparateReplayCommitter,
};

use crate::AsyncObjectLog;

/// Owns the two eventual-apply axes used by [`pqueue_engine::SeparateReplayCommit`].
///
/// The object log is the durable and fencing authority. A successful commit is returned only after the
/// projection has applied the exact positions minted by append, which is the ADR-013 response barrier.
pub struct ObjectLogProjectionCommitter<P> {
    log: AsyncObjectLog,
    projection: Arc<P>,
    gate: KeyedQueueGate<QueueKey>,
    recovery_page_size: usize,
}

impl<P> Clone for ObjectLogProjectionCommitter<P> {
    fn clone(&self) -> Self {
        Self {
            log: self.log.clone(),
            projection: Arc::clone(&self.projection),
            gate: self.gate.clone(),
            recovery_page_size: self.recovery_page_size,
        }
    }
}

pub const MAX_RECOVERY_PAGE_SIZE: usize = 4096;

impl<P> ObjectLogProjectionCommitter<P>
where
    P: AsyncProjectionStore + 'static,
{
    pub async fn open(
        log: AsyncObjectLog,
        projection: P,
        definitions: Vec<pqueue_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        Self::open_shared(
            log,
            Arc::new(projection),
            definitions,
            recovery_page_size,
            max_queued_commits,
        )
        .await
    }

    pub async fn open_shared(
        log: AsyncObjectLog,
        projection: Arc<P>,
        definitions: Vec<pqueue_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        validate_page_size(recovery_page_size)?;
        if max_queued_commits == 0 {
            return Err(EngineError::Invalid(
                "separate replay commit queue capacity must be positive",
            ));
        }
        let committer = Self {
            log,
            projection,
            gate: KeyedQueueGate::new(max_queued_commits),
            recovery_page_size,
        };
        for definition in definitions {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            committer.log.ensure_shard(shard.clone()).await?;
            committer.projection.ensure_shard(definition).await?;
            committer.recover_projection(shard).await?;
        }
        Ok(committer)
    }

    /// Convenience for migrations where a surviving projection is still available.
    /// Production composition must prefer [`Self::open`] with control-plane-owned definitions because a
    /// projection is a disposable cache and may be empty or lost.
    pub async fn open_from_surviving_projection(
        log: AsyncObjectLog,
        projection: P,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        let definitions = projection.recover_definitions().await?;
        Self::open(
            log,
            projection,
            definitions,
            recovery_page_size,
            max_queued_commits,
        )
        .await
    }

    pub fn log(&self) -> &AsyncObjectLog {
        &self.log
    }

    pub fn projection(&self) -> &Arc<P> {
        &self.projection
    }

    /// Replay every durable command after the projection's persisted frontier.
    ///
    /// This is the repair path for an append whose live apply failed or whose response was lost after the
    /// append-only fault boundary. Recovery apply, rather than live apply, intentionally accepts historical
    /// epochs while preserving ordered, page-at-a-time frontier advancement.
    pub async fn recover_projection(&self, shard: QueueKey) -> EngineResult<()> {
        let _permit = self
            .gate
            .acquire(shard.clone())
            .await
            .map_err(|_| EngineError::Unavailable)?;
        repair_tail(
            &self.log,
            self.projection.as_ref(),
            shard,
            self.recovery_page_size,
        )
        .await
    }
}

impl<P> SeparateReplayCommitter for ObjectLogProjectionCommitter<P>
where
    P: AsyncProjectionStore + 'static,
{
    type Request = RawCommitRequest;
    type Output = EngineResult<RawCommitOutcome>;

    fn commit_replayable(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        let log = self.log.clone();
        let projection = Arc::clone(&self.projection);
        let gate = self.gate.clone();
        let page_size = self.recovery_page_size;
        Box::pin(async move {
            let shard = request.shard().clone();
            let commands = request.commands().to_vec();
            let expected_epoch = request.expected_epoch();
            match request.fault() {
                RawCommitFault::BeforeAppend => {
                    return Err(EngineError::Invalid("fault-injection: kill before append"));
                }
                RawCommitFault::None | RawCommitFault::AfterAppendBeforeApply => {}
            }

            let _permit = gate
                .acquire(shard.clone())
                .await
                .map_err(|_| EngineError::Unavailable)?;
            repair_tail(&log, projection.as_ref(), shard.clone(), page_size).await?;

            let positions = log
                .append(shard.clone(), commands.clone(), expected_epoch)
                .await?;
            validate_append_footprint(&shard, &positions, commands.len(), Some(expected_epoch))?;
            if request.fault() == RawCommitFault::AfterAppendBeforeApply {
                return Ok(RawCommitOutcome::appended(positions));
            }

            projection.apply_live(positions.clone(), commands).await?;
            Ok(RawCommitOutcome::applied(positions))
        })
    }
}

/// Explicit group-commit variant. It never probes mode or calls ordinary append.
pub struct GroupCommitObjectLogProjectionCommitter<P> {
    inner: ObjectLogProjectionCommitter<P>,
}

impl<P> GroupCommitObjectLogProjectionCommitter<P>
where
    P: AsyncProjectionStore + 'static,
{
    pub async fn open(
        log: AsyncObjectLog,
        projection: P,
        definitions: Vec<pqueue_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        Ok(Self {
            inner: ObjectLogProjectionCommitter::open(
                log,
                projection,
                definitions,
                recovery_page_size,
                max_queued_commits,
            )
            .await?,
        })
    }
}

impl<P> SeparateReplayCommitter for GroupCommitObjectLogProjectionCommitter<P>
where
    P: AsyncProjectionStore + 'static,
{
    type Request = RawCommitRequest;
    type Output = EngineResult<RawCommitOutcome>;

    fn commit_replayable(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let shard = request.shard().clone();
            let commands = request.commands().to_vec();
            let expected_epoch = request.expected_epoch();
            if request.fault() == RawCommitFault::BeforeAppend {
                return Err(EngineError::Invalid("fault-injection: kill before append"));
            }
            let _permit = inner
                .gate
                .acquire(shard.clone())
                .await
                .map_err(|_| EngineError::Unavailable)?;
            repair_tail(
                &inner.log,
                inner.projection.as_ref(),
                shard.clone(),
                inner.recovery_page_size,
            )
            .await?;
            let sealed = inner
                .log
                .group_commit_enqueue_and_seal(shard.clone(), commands.clone(), expected_epoch, 0)
                .await?;
            if sealed.len() < commands.len() {
                return Err(EngineError::Storage(
                    "group commit seal omitted accepted commands".to_string(),
                ));
            }
            validate_append_footprint(&shard, &sealed, sealed.len(), Some(expected_epoch))?;
            let positions = sealed[sealed.len() - commands.len()..].to_vec();
            if request.fault() == RawCommitFault::AfterAppendBeforeApply {
                return Ok(RawCommitOutcome::appended(positions));
            }
            repair_tail(
                &inner.log,
                inner.projection.as_ref(),
                shard,
                inner.recovery_page_size,
            )
            .await?;
            Ok(RawCommitOutcome::applied(positions))
        })
    }
}

fn validate_page_size(page_size: usize) -> EngineResult<()> {
    if page_size == 0 || page_size > MAX_RECOVERY_PAGE_SIZE || page_size.checked_add(1).is_none() {
        return Err(EngineError::Invalid("invalid recovery page size"));
    }
    Ok(())
}

async fn repair_tail<P: AsyncProjectionStore + ?Sized>(
    log: &AsyncObjectLog,
    projection: &P,
    shard: QueueKey,
    page_size: usize,
) -> EngineResult<()> {
    validate_page_size(page_size)?;
    let mut cursor = projection.recovery_high_water(shard.clone()).await?;
    loop {
        let page = log
            .read_from(shard.clone(), cursor.clone(), page_size)
            .await?;
        if page.entries.is_empty() {
            return Ok(());
        }
        let next = page.next;
        let (positions, commands): (Vec<_>, Vec<_>) = page.entries.into_iter().unzip();
        validate_append_footprint(&shard, &positions, positions.len(), None)?;
        let last = positions.last().cloned();
        projection.apply_recovery(positions, commands).await?;
        cursor = next.clone().or(last);
        if next.is_none() {
            return Ok(());
        }
    }
}

fn validate_append_footprint(
    shard: &QueueKey,
    positions: &[pqueue_engine::CommandPosition],
    expected_count: usize,
    expected_epoch: Option<u64>,
) -> EngineResult<()> {
    if positions.len() != expected_count
        || positions.iter().any(|position| {
            position.queue != *shard
                || expected_epoch.is_some_and(|epoch| position.backend_epoch != epoch)
        })
        || positions.windows(2).any(|pair| !pair[0].precedes(&pair[1]))
    {
        return Err(EngineError::Storage(
            "object log returned an invalid append footprint".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use futures::channel::oneshot;
    use pqueue_conformance::{envelope, item};
    use pqueue_core::{ItemId, ItemState, QueueDefinition, QueueId, TenantId, UtcTimestamp};
    use pqueue_engine::{
        AsyncCommitStrategy, ClaimCompatibility, ClaimUnit, ClaimedItem, CommandEnvelope,
        CommandPosition, DispatchError, DurabilityClass, IdempotencyDecision, OwnedTaskDispatcher,
        OwnedTaskFactory, PushCommand, PushFingerprint, PushItem, QueueCommand, RawCommitFault,
        RichClaimSelection, SeparateReplayCommit, TaskOutcome, TaskOutcomeSender,
        task_outcome_channel,
    };

    use super::*;
    use crate::segmented::{InMemoryBlobStore, SegmentConfig};

    static NEXT_ROOT: AtomicUsize = AtomicUsize::new(0);

    fn root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "pqueue-async-replay-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    fn command(id: &str) -> CommandEnvelope {
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item(id, id, 1)],
            }),
            vec![ItemId::new(id).unwrap()],
        )
    }

    #[derive(Default)]
    struct ProjectionState {
        frontier: Option<CommandPosition>,
        live: Vec<Vec<CommandPosition>>,
        recovery: Vec<Vec<CommandPosition>>,
        fail_live: bool,
        gate: Option<oneshot::Receiver<()>>,
        started: Option<oneshot::Sender<()>>,
    }

    #[derive(Clone)]
    struct RecordingProjection {
        log: AsyncObjectLog,
        state: Arc<Mutex<ProjectionState>>,
    }

    impl RecordingProjection {
        fn new(log: AsyncObjectLog) -> Self {
            Self {
                log,
                state: Arc::new(Mutex::new(ProjectionState::default())),
            }
        }

        fn fail_next_live(&self) {
            self.state.lock().unwrap().fail_live = true;
        }

        fn gate_next_live(&self, started: oneshot::Sender<()>, release: oneshot::Receiver<()>) {
            let mut state = self.state.lock().unwrap();
            state.started = Some(started);
            state.gate = Some(release);
        }
    }

    impl AsyncProjectionStore for RecordingProjection {
        fn ensure_shard(
            &self,
            _definition: QueueDefinition,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            std::future::ready(Ok(()))
        }

        fn admit_mutation(
            &self,
            _shard: QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            std::future::ready(Ok(()))
        }

        fn validate_push(
            &self,
            _shard: QueueKey,
            _items: Vec<PushItem>,
            _now: UtcTimestamp,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            std::future::ready(Ok(()))
        }

        fn push_idempotency(
            &self,
            _shard: QueueKey,
            _request_id: pqueue_core::RequestId,
            _fingerprint: PushFingerprint,
            _now: UtcTimestamp,
        ) -> impl std::future::Future<Output = EngineResult<IdempotencyDecision<Vec<ItemId>>>> + Send
        {
            std::future::ready(Ok(IdempotencyDecision::Proceed))
        }

        fn apply_live(
            &self,
            positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            let this = self.clone();
            async move {
                let expected = positions.last().cloned();
                assert_eq!(
                    this.log
                        .high_water(positions[0].queue.clone())
                        .await
                        .unwrap(),
                    expected,
                    "the durable append must precede projection apply"
                );
                let (fail, started, gate) = {
                    let mut state = this.state.lock().unwrap();
                    (
                        std::mem::take(&mut state.fail_live),
                        state.started.take(),
                        state.gate.take(),
                    )
                };
                if let Some(started) = started {
                    let _ = started.send(());
                }
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                if fail {
                    return Err(EngineError::Storage(
                        "injected projection failure".to_string(),
                    ));
                }
                let mut state = this.state.lock().unwrap();
                state.frontier = expected;
                state.live.push(positions);
                Ok(())
            }
        }

        fn apply_recovery(
            &self,
            positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            let state = Arc::clone(&self.state);
            async move {
                let mut state = state.lock().unwrap();
                state.frontier = positions.last().cloned();
                state.recovery.push(positions);
                Ok(())
            }
        }

        fn eligible_candidates(
            &self,
            _shard: QueueKey,
            _now: UtcTimestamp,
            _max: usize,
        ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
            std::future::ready(Ok(Vec::new()))
        }

        fn select_rich_claim(
            &self,
            _shard: QueueKey,
            _unit: ClaimUnit,
            _compatibility: ClaimCompatibility,
            _now: UtcTimestamp,
            _max_items: usize,
        ) -> impl std::future::Future<Output = EngineResult<RichClaimSelection>> + Send {
            std::future::ready(Err(EngineError::Unavailable))
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _ids: Vec<ItemId>,
        ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
            std::future::ready(Ok(Vec::new()))
        }

        fn item_state(
            &self,
            _shard: QueueKey,
            _id: ItemId,
        ) -> impl std::future::Future<Output = EngineResult<Option<ItemState>>> + Send {
            std::future::ready(Ok(None))
        }

        fn item_version(
            &self,
            _shard: QueueKey,
            _id: ItemId,
        ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
            std::future::ready(Ok(None))
        }

        fn recovery_high_water(
            &self,
            _shard: QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send
        {
            let frontier = self.state.lock().unwrap().frontier.clone();
            std::future::ready(Ok(frontier))
        }

        fn recover_definitions(
            &self,
        ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
            std::future::ready(Ok(Vec::new()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn append_precedes_apply_and_outcome_crosses_response_barrier() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer =
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 16, 8)
                .await
                .unwrap();
        let strategy =
            SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer).unwrap();
        let outcome = strategy
            .commit(RawCommitRequest::new(
                shard(),
                vec![command("1"), command("2")],
                0,
            ))
            .await
            .unwrap();
        assert!(outcome.projection_applied());
        assert_eq!(outcome.positions().len(), 2);
        assert_eq!(projection.state.lock().unwrap().live.len(), 1);
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn typed_faults_and_epoch_fence_have_exact_durable_footprints() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer =
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 16, 8)
                .await
                .unwrap();
        assert!(
            committer
                .commit_replayable(
                    RawCommitRequest::new(shard(), vec![command("1")], 0)
                        .with_fault(RawCommitFault::BeforeAppend)
                )
                .await
                .is_err()
        );
        assert_eq!(log.high_water(shard()).await.unwrap(), None);
        let appended = committer
            .commit_replayable(
                RawCommitRequest::new(shard(), vec![command("2")], 0)
                    .with_fault(RawCommitFault::AfterAppendBeforeApply),
            )
            .await
            .unwrap();
        assert!(!appended.projection_applied());
        assert_eq!(projection.state.lock().unwrap().live.len(), 0);
        log.acquire_epoch(shard()).await.unwrap();
        assert!(
            committer
                .commit_replayable(RawCommitRequest::new(shard(), vec![command("3")], 0))
                .await
                .is_err()
        );
        assert_eq!(
            log.high_water(shard()).await.unwrap(),
            Some(appended.positions()[0].clone())
        );
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_live_apply_is_repaired_before_the_next_live_commit() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        projection.fail_next_live();
        let committer =
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 1, 8)
                .await
                .unwrap();
        assert!(
            committer
                .commit_replayable(RawCommitRequest::new(
                    shard(),
                    vec![command("1"), command("2")],
                    0
                ))
                .await
                .is_err()
        );
        assert!(log.high_water(shard()).await.unwrap().is_some());
        assert_eq!(projection.recovery_high_water(shard()).await.unwrap(), None);
        let later = committer
            .commit_replayable(RawCommitRequest::new(shard(), vec![command("3")], 0))
            .await
            .unwrap();
        assert!(later.projection_applied());
        {
            let state = projection.state.lock().unwrap();
            assert_eq!(state.recovery.len(), 2, "page-size one replays in order");
            assert!(state.frontier.is_some());
            assert_eq!(state.live.len(), 1, "later live apply follows tail repair");
        }
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn constructors_reject_zero_and_oversized_recovery_pages() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        assert!(
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 0, 8)
                .await
                .is_err()
        );
        assert!(
            ObjectLogProjectionCommitter::open(
                log.clone(),
                projection,
                Vec::new(),
                MAX_RECOVERY_PAGE_SIZE + 1,
                8,
            )
            .await
            .is_err()
        );
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_group_committer_seals_and_maps_each_owned_request() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            projection.clone(),
            Vec::new(),
            16,
            8,
        )
        .await
        .unwrap();
        let first = committer
            .commit_replayable(RawCommitRequest::new(shard(), vec![command("1")], 0))
            .await
            .unwrap();
        let second = committer
            .commit_replayable(RawCommitRequest::new(shard(), vec![command("2")], 0))
            .await
            .unwrap();
        assert!(first.projection_applied() && second.projection_applied());
        assert!(first.positions()[0].precedes(&second.positions()[0]));
        assert_eq!(projection.state.lock().unwrap().recovery.len(), 2);
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn atomic_group_enqueue_seal_maps_around_external_enqueues() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            projection,
            Vec::new(),
            16,
            8,
        )
        .await
        .unwrap();

        // External enqueue wins the first ordering race. The atomic committer operation seals both and
        // maps only its own suffix.
        assert!(
            log.group_commit_enqueue(shard(), vec![command("10")], 0, 0)
                .await
                .unwrap()
                .is_empty()
        );
        let own = committer
            .commit_replayable(RawCommitRequest::new(shard(), vec![command("11")], 0))
            .await
            .unwrap();
        assert_eq!(own.positions().len(), 1);
        let page = log.read_from(shard(), None, 10).await.unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(own.positions()[0], page.entries[1].0);

        // The committer wins the second race. A following external enqueue remains unsealed and cannot
        // alter the already returned request mapping.
        assert!(
            log.group_commit_enqueue(shard(), vec![command("12")], 0, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            log.high_water(shard()).await.unwrap(),
            Some(own.positions()[0].clone())
        );
        let sealed_after = log.group_commit_seal(shard(), 0, 0).await.unwrap();
        assert_eq!(sealed_after.len(), 1);
        assert!(own.positions()[0].precedes(&sealed_after[0]));
        log.close_and_drain().await.unwrap();
    }

    struct TokioTestDispatcher {
        closed: AtomicBool,
    }

    impl TokioTestDispatcher {
        fn new() -> Self {
            Self {
                closed: AtomicBool::new(false),
            }
        }
    }

    impl OwnedTaskDispatcher for TokioTestDispatcher {
        fn submit<T: Send + 'static>(
            &self,
            factory: OwnedTaskFactory<T>,
        ) -> Result<TaskOutcome<T>, DispatchError> {
            if self.closed.load(Ordering::Acquire) {
                return Err(DispatchError::Closed);
            }
            let (sender, outcome) = task_outcome_channel();
            tokio::spawn(async move { sender.send(factory().await) });
            Ok(outcome)
        }

        fn close(&self) {
            self.closed.store(true, Ordering::Release);
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::Acquire)
        }

        fn drain(&self) -> TaskOutcome<()> {
            let (sender, outcome): (TaskOutcomeSender<()>, _) = task_outcome_channel();
            sender.send(());
            outcome
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_accepted_response_does_not_cancel_commit() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        projection.gate_next_live(started_tx, release_rx);
        let committer =
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 16, 8)
                .await
                .unwrap();
        let strategy =
            SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer).unwrap();
        let backend =
            pqueue_engine::AsyncComposedBackend::new(strategy, TokioTestDispatcher::new(), 2);
        let response = tokio::spawn({
            let request = RawCommitRequest::new(shard(), vec![command("1")], 0);
            async move { backend.submit_commit(request).await }
        });
        started_rx.await.unwrap();
        response.abort();
        release_tx.send(()).unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(projection.state.lock().unwrap().live.len(), 1);
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
