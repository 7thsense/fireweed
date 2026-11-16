//! One write transaction held across one loop iteration.
//!
//! The owner plans each mutation of an iteration against this transaction, so a
//! later mutation sees the effects of the earlier ones, stages the planned entry
//! into it, publishes the iteration's single log record, and only then commits
//! or rolls back. The projection stays what it was before the iteration until
//! its record is durable: `P = fold(log[0, p))` holds at every commit.
//!
//! The batch holds the projection's writer connection for as long as it lives.
//! A batch that is dropped without `commit` or `rollback` poisons the connection
//! instead of leaving a transaction open on it.
use super::*;
use fireweed_engine::queue_log_record::OwnedQueueEntry;

/// Where a planner's read transaction runs: on its own, or inside a batch.
pub(super) enum TxHost {
    Own(TransactionBehavior),
    Batch(Connection),
}
pub(super) async fn host_transaction<W: NativeTransactionWork>(
    projection: &Arc<QueueProjection>,
    host: TxHost,
    work: &mut W,
) -> QueueResult<W::Output> {
    match host {
        TxHost::Own(behavior) => owned_native_transaction(projection, behavior, work).await,
        // The batch owns the transaction. A panic or error here leaves it to the
        // owner, which rolls the whole batch back on anything but a definite
        // planning result.
        TxHost::Batch(connection) => catch_apply_panic(work.run(&connection))
            .await
            .and_then(|value| value),
    }
}

impl QueueProjection {
    /// Run `operation` on a scratch worker, once it has its turn at the writer.
    ///
    /// The writer is one connection, and a batch holds it across its publish. An
    /// operation that blocked for it on a worker would hold that worker
    /// meanwhile; enough of them would leave the batch no worker to finish with,
    /// and nothing could proceed. So every operation waits for its turn first,
    /// holding nothing, and a batch holds the turn for its whole life and runs
    /// its own work with `in_batch`.
    pub(super) async fn run_native<T, F>(
        self: &Arc<Self>,
        in_batch: bool,
        workers: &ScratchWorkers,
        operation: F,
    ) -> QueueResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QueueResult<T> + Send + 'static,
    {
        let _turn = if in_batch {
            None
        } else {
            Some(self.turn.lock().await)
        };
        workers.run(operation).await
    }
}

pub struct QueueBatch {
    projection: Arc<QueueProjection>,
    writer: Option<tokio::sync::OwnedMutexGuard<Option<Connection>>>,
    /// The queue the staged entries belong to, once one is staged.
    queue_id: Option<i64>,
    /// Declared last so it is released after the writer.
    _turn: tokio::sync::OwnedMutexGuard<()>,
}
impl QueueProjection {
    /// Take the writer connection and open one write transaction on it.
    pub async fn begin_batch(
        self: &Arc<Self>,
        workers: &ScratchWorkers,
    ) -> QueueResult<QueueBatch> {
        let turn = Arc::clone(&self.turn).lock_owned().await;
        let mut writer = Arc::clone(&self.writer).lock_owned().await;
        let connection = writer
            .as_ref()
            .ok_or_else(|| unknown("projection connection is poisoned"))?
            .clone();
        let begun = self
            .run_native(true, workers, {
                let connection = connection.clone();
                move || {
                    Ok(crate::tx::block_on_owned_apply(async move {
                        match catch_apply_panic(connection.execute("BEGIN IMMEDIATE", ())).await {
                            Ok(Ok(_)) => Ok(()),
                            failure => {
                                drop(failure);
                                let _ = catch_apply_panic(connection.execute("ROLLBACK", ())).await;
                                Err(unknown("native batch begin failed"))
                            }
                        }
                    }))
                }
            })
            .await
            .and_then(|result| result);
        if let Err(error) = begun {
            if !matches!(connection.is_autocommit(), Ok(true)) {
                *writer = None;
            }
            return Err(error);
        }
        Ok(QueueBatch {
            projection: Arc::clone(self),
            _turn: turn,
            writer: Some(writer),
            queue_id: None,
        })
    }
}
impl QueueBatch {
    fn connection(&self) -> QueueResult<Connection> {
        self.writer
            .as_ref()
            .and_then(|writer| writer.as_ref())
            .cloned()
            .ok_or_else(|| unknown("projection connection is poisoned"))
    }
    fn host(&self) -> QueueResult<TxHost> {
        self.connection().map(TxHost::Batch)
    }
    /// Plan an insert against the batch's staged state. Same contract as
    /// `QueueProjection::prepare_insert`, which opens its own transaction.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_insert(
        &mut self,
        input: InsertReplayInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn fireweed_engine::Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<PreparedInsert>> {
        let host = self.host()?;
        self.projection
            .prepare_insert_in(host, input, epoch, max_record_bytes, clock, job, workers)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_claim(
        &mut self,
        input: ClaimReplayInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn fireweed_engine::Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<PreparedClaim>> {
        let host = self.host()?;
        self.projection
            .prepare_claim_in(host, input, epoch, max_record_bytes, clock, job, workers)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_ordinary_commit(
        &mut self,
        input: CommitPreparationInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn fireweed_engine::Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<PreparedOrdinaryCommit>> {
        let host = self.host()?;
        self.projection
            .prepare_ordinary_commit_in(host, input, epoch, max_record_bytes, clock, job, workers)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_administration(
        &mut self,
        input: AdministrativePreparationInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn fireweed_engine::Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<PreparedOrdinaryCommit>> {
        let host = self.host()?;
        self.projection
            .prepare_administration_in(host, input, epoch, max_record_bytes, clock, job, workers)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_ordinary_pack(
        &mut self,
        input: CommitPackPreparationInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn fireweed_engine::Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<OrdinaryPackPreparation> {
        let host = self.host()?;
        self.projection
            .prepare_ordinary_pack_in(host, input, epoch, max_record_bytes, clock, job, workers)
            .await
    }
    /// Classify a commit batch's groups against the staged state.
    pub async fn replay_commit_batch(
        &mut self,
        input: CommitBatchReplayInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedCommitBatchReplay> {
        let host = self.host()?;
        self.projection
            .replay_commit_batch_in(host, input, job, workers)
            .await
    }
    /// Apply one planned entry inside the batch, with the same function rebuild
    /// uses, and check the unique keys it touched. `offset` is the log offset of
    /// the record the entry will be published in. Any error leaves the batch in
    /// an unknown state: roll it back.
    pub async fn stage(
        &mut self,
        entry: &OwnedQueueEntry,
        epoch: u64,
        offset: u64,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<()> {
        let connection = self.connection()?;
        let entry = entry.clone();
        let job = Arc::clone(job);
        let offset = i64::try_from(offset).map_err(unknown)?;
        let queue_id = self
            .projection
            .run_native(true, workers, move || {
                Ok(crate::tx::block_on_owned_apply(async move {
                    catch_apply_panic(async {
                        let view = entry.view()?;
                        let frame = view.frame();
                        let queue = frame.summary().queue;
                        let _header = job.owned_buffer(
                            ((queue.0.len() + queue.1.len()) as u64)
                                .checked_mul(16)
                                .and_then(|n| n.checked_add(65536))
                                .ok_or(QueueError::TooLarge)?,
                        )?;
                        let metadata = queue_row(&connection, queue.0, queue.1)
                            .await?
                            .ok_or_else(|| unknown("publication has no projected genesis"))?;
                        let queue_id = check_record_point(&metadata, offset, epoch)?;
                        apply_entry(
                            &connection,
                            queue_id,
                            (frame, view.claim()),
                            offset,
                            &job,
                            #[cfg(test)]
                            false,
                        )
                        .await?;
                        Ok(queue_id)
                    })
                    .await
                    .and_then(|result| result)
                }))
            })
            .await??;
        self.queue_id = Some(queue_id);
        Ok(())
    }
    /// Move the applied cursor past the record, once, and commit. The record
    /// must already be durable at `offset`.
    pub async fn commit(
        mut self,
        offset: u64,
        epoch: u64,
        workers: &ScratchWorkers,
    ) -> QueueResult<()> {
        let queue_id = self
            .queue_id
            .ok_or_else(|| unknown("batch committed with no staged entry"))?;
        let connection = self.connection()?;
        let wal = self.projection.wal.clone();
        let offset = i64::try_from(offset).map_err(unknown)?;
        let committed = self
            .projection
            .run_native(true, workers, {
                let connection = connection.clone();
                move || {
                    Ok(crate::tx::block_on_owned_apply(async move {
                        catch_apply_panic(async {
                            advance_cursor(&connection, queue_id, offset, epoch).await?;
                            connection.execute("COMMIT", ()).await.map_err(unknown)?;
                            bound_wal(&connection, &wal).await?;
                            Ok(())
                        })
                        .await
                        .and_then(|result| result)
                    }))
                }
            })
            .await
            .and_then(|result| result);
        self.finish(committed, workers).await
    }
    /// Discard everything the batch staged and release the writer.
    pub async fn rollback(mut self, workers: &ScratchWorkers) {
        let _ = self.finish(Err(unknown("rolled back")), workers).await;
    }
    /// Leave the connection with no transaction on it, or take it away.
    async fn finish(
        &mut self,
        outcome: QueueResult<()>,
        workers: &ScratchWorkers,
    ) -> QueueResult<()> {
        let Some(mut writer) = self.writer.take() else {
            return outcome;
        };
        if let Some(connection) = writer.as_ref().cloned() {
            if matches!(connection.is_autocommit(), Ok(true)) {
                return outcome;
            }
            let rolled = self
                .projection
                .run_native(true, workers, {
                    let connection = connection.clone();
                    move || {
                        Ok(crate::tx::block_on_owned_apply(async move {
                            catch_apply_panic(connection.execute("ROLLBACK", ())).await
                        }))
                    }
                })
                .await
                .and_then(|result| result);
            if !matches!(rolled, Ok(Ok(_))) || !matches!(connection.is_autocommit(), Ok(true)) {
                *writer = None;
            }
        }
        outcome
    }
}
impl Drop for QueueBatch {
    fn drop(&mut self) {
        // Dropped with a transaction open: no await here, so the connection goes.
        if let Some(mut writer) = self.writer.take() {
            *writer = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{address, bootstrap, work};
    use super::*;
    use fireweed_conformance::queue_contract::TestClock;
    use fireweed_engine::{
        queue_admission::QueueAdmission,
        queue_log_record::{
            OwnedQueueLogRecord, encode_checked_entries_record, encode_checked_genesis,
        },
    };

    fn insert_input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        id: &str,
        key: &str,
        u: i64,
        point: (u64, u64),
    ) -> InsertReplayInput {
        let definition = AdmittedQueueDefinition::from_raw(definition.raw(), job).unwrap();
        InsertReplayInput {
            genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, job)
                .unwrap(),
            request: InsertRequest {
                queue: address(),
                request_id: id.into(),
                mode: InsertMode::InsertOnly,
                items: vec![work(key, u, false)],
                issued_at: 0,
            },
            limits: QueueLimits::default(),
            required_next_offset: point.0,
            required_insertion_sequence: point.1,
            required_last_record_epoch: 1,
        }
    }
    fn final_sequence(entry: &OwnedQueueEntry) -> u64 {
        entry.view().unwrap().frame().summary().final_sequence
    }
    /// Plan `id` against the batch and stage it. Returns its entry and the
    /// outcomes the plan produced.
    async fn plan_and_stage(
        batch: &mut QueueBatch,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
        (id, key, u): (&str, &str, i64),
        sequence: u64,
    ) -> (OwnedQueueEntry, Vec<InsertOutcome>) {
        let clock = Arc::new(TestClock::default());
        clock.set(100);
        let prepared = batch
            .prepare_insert(
                insert_input(definition, job, id, key, u, (1, sequence)),
                1,
                16 << 20,
                clock,
                job,
                workers,
            )
            .await
            .unwrap()
            .unwrap();
        let outcomes = prepared.outcomes().to_vec();
        let (record, provisional, head): (OwnedQueueLogRecord, _, _) = prepared.into_parts();
        drop(provisional);
        assert_eq!(head, 1);
        let entry = record.into_entry().unwrap();
        batch.stage(&entry, 1, 1, job, workers).await.unwrap();
        (entry, outcomes)
    }
    async fn replay(
        projection: &Arc<QueueProjection>,
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
        (id, key, u): (&str, &str, i64),
        point: (u64, u64),
    ) -> Option<Vec<InsertOutcome>> {
        projection
            .replay_insert(
                insert_input(definition, job, id, key, u, point),
                job,
                workers,
            )
            .await
            .unwrap()
            .map(|replay| replay.outcomes().to_vec())
    }

    #[tokio::test]
    async fn a_batch_stages_later_entries_over_earlier_ones_and_commits_or_rolls_back_whole() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("batch.db"), &job, &workers).await;
        let first = ("r1", "a", 1);
        let second = ("r2", "a", 2);

        // Roll back: nothing the batch staged survives.
        let mut batch = projection.begin_batch(&workers).await.unwrap();
        let (_, outcomes) = plan_and_stage(&mut batch, &definition, &job, &workers, first, 0).await;
        assert!(matches!(outcomes[..], [InsertOutcome::Inserted { .. }]));
        batch.rollback(&workers).await;
        assert!(
            replay(&projection, &definition, &job, &workers, first, (1, 0))
                .await
                .is_none()
        );

        // Commit: the second request sees the first one's staged row.
        let mut batch = projection.begin_batch(&workers).await.unwrap();
        let (one, outcomes) =
            plan_and_stage(&mut batch, &definition, &job, &workers, first, 0).await;
        assert!(matches!(outcomes[..], [InsertOutcome::Inserted { .. }]));
        let sequence = final_sequence(&one);
        assert!(sequence > 0);
        let (two, outcomes) =
            plan_and_stage(&mut batch, &definition, &job, &workers, second, sequence).await;
        assert!(matches!(
            outcomes[..],
            [InsertOutcome::Rejected {
                reason: Rejection::AlreadyExists,
                ..
            }]
        ));
        let sequence = final_sequence(&two);
        batch.commit(1, 1, &workers).await.unwrap();
        let live_first = replay(
            &projection,
            &definition,
            &job,
            &workers,
            first,
            (2, sequence),
        )
        .await
        .unwrap();
        let live_second = replay(
            &projection,
            &definition,
            &job,
            &workers,
            second,
            (2, sequence),
        )
        .await
        .unwrap();
        assert!(matches!(live_first[..], [InsertOutcome::Inserted { .. }]));
        assert!(matches!(
            live_second[..],
            [InsertOutcome::Rejected {
                reason: Rejection::AlreadyExists,
                ..
            }]
        ));

        // Rebuild: the same entries as one record, in one transaction, reach the
        // same state.
        let (rebuilt, _) = bootstrap(&directory.path().join("rebuilt.db"), &job, &workers).await;
        let record = encode_checked_entries_record(1, &[one, two], 16 << 20, &job).unwrap();
        assert_eq!(
            rebuilt
                .apply(record, 1, &job, &workers)
                .await
                .unwrap()
                .next_offset,
            2
        );
        assert_eq!(
            replay(&rebuilt, &definition, &job, &workers, first, (2, sequence)).await,
            Some(live_first)
        );
        assert_eq!(
            replay(&rebuilt, &definition, &job, &workers, second, (2, sequence)).await,
            Some(live_second)
        );
        workers.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn a_batch_left_open_poisons_the_connection_instead_of_leaking_a_transaction() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("dropped.db"), &job, &workers).await;
        let mut batch = projection.begin_batch(&workers).await.unwrap();
        plan_and_stage(&mut batch, &definition, &job, &workers, ("r1", "a", 1), 0).await;
        drop(batch);
        assert!(projection.begin_batch(&workers).await.is_err());
        workers.close_and_drain().await.unwrap();
    }
}
