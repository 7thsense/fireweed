//! Complete addressed workflow-state reads in a covered, owned native snapshot.
use super::*;
use fireweed_engine::queue_memory::incoming_headers;
pub struct StateReadInput {
    pub genesis: OwnedQueueLogRecord,
    pub request: StateReadRequest,
    pub limits: QueueLimits,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedStateRecords {
    records: Vec<Option<StateRecord>>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedStateRecords {
    pub fn into_owned(
        self,
    ) -> fireweed_engine::queue_contract::OwnedQueueValue<
        Vec<Option<StateRecord>>,
        Vec<OwnedQueueBufferCharge>,
    > {
        fireweed_engine::queue_contract::OwnedQueueValue::new(self.records, self._charges)
    }

    pub fn records(&self) -> &[Option<StateRecord>] {
        &self.records
    }
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: StateReadInput,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// Every state value is complete; requested order, duplicates and absent
    /// identities are preserved. Orchestration must confirm authority afterward.
    pub async fn read_state_records(
        self: &Arc<Self>,
        input: StateReadInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedStateRecords> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("state genesis belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        let headers = incoming_headers(&input.request)?;
        let containers = (input.request.keys.len() as u64)
            .checked_mul(std::mem::size_of::<Option<StateRecord>>() as u64)
            .and_then(|n| n.checked_mul(3))
            .ok_or(QueueError::TooLarge)?;
        let repeated_collection = (input.request.keys.len() as u64)
            .checked_mul(input.request.collection.len() as u64)
            .and_then(|n| n.checked_mul(4))
            .ok_or(QueueError::TooLarge)?;
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(containers))
            .and_then(|n| n.checked_add(repeated_collection))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = Worker {
            projection: self.clone(),
            input,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let records = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(AdmittedStateRecords {
                        records,
                        _charges: std::mem::take(&mut worker.charges),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for Worker {
    type Output = Vec<Option<StateRecord>>;
    async fn before_commit(&mut self) {
        #[cfg(test)]
        {
            self.projection.pause_apply(false).await;
            assert!(!self.charges.is_empty());
            if self
                .projection
                .panic_after_command
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                panic!("injected state read precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("state projection missing"))?;
        snapshot.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        self.input
            .request
            .validate(snapshot.definition.schema(), &self.input.limits)?;
        let collection = collection_number(
            snapshot.definition.schema(),
            Some(&self.input.request.collection),
        )?;
        let definition = collection_definition(snapshot.definition.schema(), collection)?;
        let mut records = Vec::new();
        let mut total = 0;
        let mut exceeded = false;
        for key in &self.input.request.keys {
            use super::state_authority::{StateAuthority, load_state_authority};
            let (id, version) =
                match load_state_authority(connection, snapshot.queue_id, collection, key).await? {
                    StateAuthority::Absent | StateAuthority::Deleted { .. } => {
                        records.push(None);
                        continue;
                    }
                    StateAuthority::Present { id, version } => (id, version),
                };
            let mut fields = Fields::new();
            let mut value = bytes::Bytes::new();
            payload::load_payload(
                connection,
                id,
                definition,
                payload::PayloadSelection {
                    fields: None,
                    include_body: true,
                    max_bytes: self.input.request.max_bytes,
                },
                &mut payload::NativePayloadBudget {
                    job: &self.job,
                    charges: &mut self.charges,
                    total: &mut total,
                    response_limit: &mut exceeded,
                },
                &mut fields,
                &mut value,
            )
            .await?;
            records.push(Some(StateRecord {
                collection: self.input.request.collection.clone(),
                key: key.clone(),
                version,
                fields,
                value,
            }));
        }
        let response = encoded_len(&records)?;
        if response > self.input.request.max_bytes {
            return Err(QueueError::TooLarge);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        address, apply_group_record, bootstrap, execute_for_test, group, seed,
    };
    use super::*;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    fn input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        keys: Vec<String>,
        max_bytes: u64,
        head: u64,
    ) -> StateReadInput {
        StateReadInput {
            genesis: encode_checked_genesis(1, &address(), definition, &[31; 32], 1 << 20, job)
                .unwrap(),
            request: StateReadRequest {
                queue: address(),
                collection: "workflow".into(),
                keys,
                max_bytes,
            },
            limits: QueueLimits::default(),
            required_next_offset: head,
            required_insertion_sequence: 2,
            required_last_record_epoch: 1,
        }
    }
    #[tokio::test]
    async fn complete_state_reads_preserve_order_duplicates_unsigned_versions_and_exact_budget() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state-read.db");
        let admission = QueueAdmission::new(2, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        let mut snapshot = seed(&projection, &definition, &job, &workers, false).await;
        let key = "state/\0λ".repeat(40);
        let mut put = group("create-state-read");
        put.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: key.clone(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: [("phase".into(), FieldValue::Text("running".into()))].into(),
                value: vec![7, 8, 9].into(),
            },
        });
        apply_group_record(
            &projection,
            &definition,
            &mut snapshot,
            &put,
            2,
            &job,
            &workers,
        )
        .await
        .unwrap();
        let keys = vec![key.clone(), "absent".into(), key.clone()];
        let result = projection
            .read_state_records(
                input(&definition, &job, keys.clone(), 65536, 3),
                &job,
                &workers,
            )
            .await
            .unwrap();
        let expected = result.records().to_vec();
        assert_eq!(expected[0], expected[2]);
        assert_eq!(expected[1], None);
        assert_eq!(expected[0].as_ref().unwrap().value.as_ref(), [7, 8, 9]);
        assert_eq!(
            expected[0].as_ref().unwrap().fields,
            [("phase".into(), FieldValue::Text("running".into()))].into()
        );
        let exact = encoded_len(&result.records()).unwrap();
        drop(result);
        let result = projection
            .read_state_records(
                input(&definition, &job, keys.clone(), exact, 3),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.records(), expected);
        drop(result);
        assert!(matches!(
            projection
                .read_state_records(
                    input(&definition, &job, keys.clone(), exact - 1, 3),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::TooLarge)
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_records SET version=?1 WHERE collection=1 AND key=?2",
            vec![blob(&u64::MAX.to_be_bytes()), blob(key.as_bytes())],
        )
        .await;
        let result = projection
            .read_state_records(
                input(&definition, &job, vec![key.clone()], 65536, 3),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.records()[0].as_ref().unwrap().version, u64::MAX);
        drop(result);
        execute_for_test(
            &projection,
            "UPDATE q7_records SET active=2 WHERE collection=1 AND key=?1",
            vec![blob(key.as_bytes())],
        )
        .await;
        assert!(matches!(
            projection
                .read_state_records(
                    input(&definition, &job, keys.clone(), 65536, 3),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        execute_for_test(
            &projection,
            "UPDATE q7_records SET active=0 WHERE collection=1 AND key=?1",
            vec![blob(key.as_bytes())],
        )
        .await;
        execute_for_test(&projection,"DELETE FROM q7_bodies WHERE record_id IN (SELECT id FROM q7_records WHERE collection=1)",vec![]).await;
        let result = projection
            .read_state_records(input(&definition, &job, keys, 65536, 3), &job, &workers)
            .await
            .unwrap();
        assert!(result.records().iter().all(Option::is_none));
        drop(result);
        drop(projection);
        workers.close_and_drain().await.unwrap();
        drop(snapshot);
        drop(definition);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
