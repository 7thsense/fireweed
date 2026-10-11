//! Current-record replay in one admitted native snapshot. No clock or selector
//! is rerun: the immutable receipt determines the complete ordered membership.
use super::*;
use fireweed_engine::{queue_claim::ClaimOwnership, queue_claim_receipt::view_claim_receipt};

pub struct ClaimReplayInput {
    pub genesis: OwnedQueueLogRecord,
    pub request: ClaimRequest,
    pub now: i64,
    pub limits: QueueLimits,
    /// Independently recovered authoritative head; this reader is not a fence.
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedClaimReplay {
    records: Vec<ClaimedWork>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedClaimReplay {
    pub fn records(&self) -> &[ClaimedWork] {
        &self.records
    }
}
struct ReplayWorker {
    projection: Arc<QueueProjection>,
    input: ClaimReplayInput,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    /// Internal snapshot reader. Callers must independently recover/fence the
    /// authoritative log; a successful snapshot does not own subsequent writes.
    pub async fn replay_claim(
        self: &Arc<Self>,
        input: ClaimReplayInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<Option<AdmittedClaimReplay>> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("replay genesis belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        let size = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = ReplayWorker {
            projection: Arc::clone(self),
            input,
            job: Arc::clone(job),
            charges: vec![job.owned_buffer(size)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = Arc::clone(&worker.projection);
                    let result = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(result.map(|records| AdmittedClaimReplay {
                        records,
                        _charges: std::mem::take(&mut worker.charges),
                    }))
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for ReplayWorker {
    type Output = Option<Vec<ClaimedWork>>;
    async fn before_commit(&mut self) {
        #[cfg(test)]
        self.projection.pause_apply(false).await;
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("replay projection missing"))?;
        let definition = snapshot.definition;
        let request = &self.input.request;
        request.validate(definition.schema(), &self.input.limits)?;
        let view = self.input.genesis.view()?;
        let address = view.address();
        if request.queue.tenant != address.tenant || request.queue.queue != address.queue {
            return Err(unknown("replay address mismatch"));
        }
        if snapshot.next_offset != self.input.required_next_offset
            || snapshot.last_record_epoch != self.input.required_last_record_epoch
            || snapshot.insertion_sequence != self.input.required_insertion_sequence
        {
            return Err(unknown("replay projection authority mismatch"));
        }
        let queue_id = snapshot.queue_id;
        let sequence = snapshot.insertion_sequence;
        let identity_hash = discovery_digest(request.request_id.as_bytes());
        let params = || {
            vec![
                Value::Integer(queue_id),
                blob(&identity_hash),
                blob(request.request_id.as_bytes()),
            ]
        };
        let metadata = one(connection,"SELECT CASE WHEN typeof(fingerprint_format)='integer' THEN fingerprint_format ELSE NULL END,CASE WHEN typeof(fingerprint)='blob' AND length(fingerprint)=32 THEN fingerprint ELSE NULL END,CASE WHEN typeof(expired)='integer' THEN expired ELSE NULL END,CASE WHEN typeof(outcome)='blob' THEN length(outcome) ELSE -1 END FROM q7_requests WHERE queue_id=?1 AND domain=1 AND identity_hash=?2 AND identity=?3",params(),4).await?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        if integer(&metadata[0])? != 3 {
            return Err(unknown("unsupported claim fingerprint format"));
        }
        let fingerprint: [u8; 32] = bytes(&metadata[1])?.try_into().map_err(unknown)?;
        if fingerprint != request.fingerprint()? {
            return Err(QueueError::RequestConflict);
        }
        match integer(&metadata[2])? {
            0 => {}
            1 => return Err(QueueError::RequestExpired),
            _ => return Err(unknown("invalid receipt expiry")),
        }
        let size = u64::try_from(integer(&metadata[3])?).map_err(unknown)?;
        self.charges.push(
            self.job.owned_buffer(
                size.checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let raw = one(connection,"SELECT outcome FROM q7_requests WHERE queue_id=?1 AND domain=1 AND identity_hash=?2 AND identity=?3",params(),1).await?.ok_or_else(||unknown("receipt vanished in snapshot"))?;
        let raw = bytes(&raw[0])?;
        if raw.len() as u64 != size {
            return Err(unknown("receipt length changed in snapshot"));
        }
        let receipt = view_claim_receipt(raw)?;
        if receipt.len() > u64::from(request.max_records) {
            return Err(unknown("receipt exceeds original claim cardinality"));
        }
        self.charges.push(
            self.job.owned_buffer(
                receipt
                    .len()
                    .checked_mul(1024)
                    .and_then(|n| n.checked_add(request.max_bytes.checked_mul(4)?))
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let mut keys = std::collections::BTreeSet::new();
        let mut records = Vec::new();
        // Every ownership check completes before any body or field fetch. This
        // makes partial settlement/expiry win over oversized remaining payload.
        for entry in receipt.entries() {
            let entry = entry?;
            if !keys.insert(entry.key) {
                return Err(unknown("duplicate claim membership"));
            }
            let key_hash = discovery_digest(entry.key.as_bytes());
            let row = one(connection,"SELECT CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(version)='blob' AND length(version)=8 THEN version ELSE NULL END,CASE WHEN typeof(active)='integer' THEN active ELSE NULL END,CASE WHEN typeof(lifecycle)='integer' THEN lifecycle ELSE NULL END,CASE WHEN typeof(priority)='integer' THEN priority ELSE NULL END,CASE WHEN typeof(available_at)='integer' THEN available_at ELSE NULL END,CASE WHEN typeof(eligible_since)='integer' THEN eligible_since ELSE NULL END,CASE WHEN typeof(attempts)='integer' THEN attempts ELSE NULL END,CASE WHEN lease_until IS NULL OR typeof(lease_until)='integer' THEN lease_until ELSE '' END,CASE WHEN lease_hash IS NULL OR (typeof(lease_hash)='blob' AND length(lease_hash)=32) THEN lease_hash ELSE x'' END,CASE WHEN typeof(insertion_sequence)='blob' AND length(insertion_sequence)=8 THEN insertion_sequence ELSE NULL END FROM q7_records WHERE queue_id=?1 AND collection=0 AND key_hash=?2 AND key=?3",vec![Value::Integer(queue_id),blob(&key_hash),blob(entry.key.as_bytes())],11).await?;
            let Some(row) = row else {
                return Err(QueueError::ClaimExpired);
            };
            match integer(&row[2])? {
                0 => return Err(QueueError::ClaimExpired),
                1 => {}
                _ => return Err(unknown("invalid active state")),
            }
            let lifecycle = match integer(&row[3])? {
                0 => Lifecycle::Pending,
                1 => Lifecycle::Leased,
                2 => Lifecycle::Completed,
                3 => Lifecycle::Failed,
                _ => return Err(unknown("invalid lifecycle")),
            };
            let expiry = match row[8] {
                Value::Null => None,
                _ => Some(integer(&row[8])?),
            };
            let hash = match &row[9] {
                Value::Null => None,
                _ => Some(bytes(&row[9])?.try_into().map_err(unknown)?),
            };
            ClaimOwnership {
                lifecycle,
                expires_at: expiry,
                token_hash: hash,
            }
            .validate(entry.token, self.input.now)?;
            let version = unsigned(&row[1])?;
            let insertion_sequence = unsigned(&row[10])?;
            if version < entry.version || insertion_sequence == 0 || insertion_sequence > sequence {
                return Err(unknown("invalid current replay version/sequence"));
            }
            records.push((
                integer(&row[0])?,
                ClaimedWork {
                    record: WorkRecord {
                        work: NewWork {
                            key: entry.key.into(),
                            priority: integer(&row[4])?,
                            available_at: integer(&row[5])?,
                            fields: Fields::new(),
                            body: bytes::Bytes::new(),
                        },
                        version,
                        insertion_sequence,
                        lifecycle,
                        eligible_since: integer(&row[6])?,
                        attempts: u32::try_from(integer(&row[7])?).map_err(unknown)?,
                        lease_expires_at: expiry,
                    },
                    lease: LeaseCredential {
                        token: entry.token.to_vec(),
                    },
                },
            ));
        }
        drop(keys);
        let mut payload_bytes = records.iter().try_fold(0u64, |size, (_, work)| {
            size.checked_add(work.record.work.key.len() as u64)
                .and_then(|n| n.checked_add(work.lease.token.len() as u64))
                .ok_or(QueueError::TooLarge)
        })?;
        for (id, work) in &mut records {
            load_replay_payload(
                connection,
                *id,
                &definition.schema().work,
                request,
                &mut ReplayPayloadBudget {
                    job: &self.job,
                    charges: &mut self.charges,
                    total: &mut payload_bytes,
                },
                &mut work.record.work,
            )
            .await?;
        }
        let records: Vec<_> = records.into_iter().map(|(_, work)| work).collect();
        if encoded_len(&records)? > request.max_bytes {
            return Err(QueueError::TooLarge);
        }
        Ok(Some(records))
    }
}
fn add_payload(total: &mut u64, length: i64, max: u64) -> QueueResult<u64> {
    let length = u64::try_from(length).map_err(unknown)?;
    *total = total.checked_add(length).ok_or(QueueError::TooLarge)?;
    if *total > max {
        return Err(QueueError::TooLarge);
    }
    Ok(length)
}
struct ReplayPayloadBudget<'a> {
    job: &'a Arc<QueueReservation>,
    charges: &'a mut Vec<OwnedQueueBufferCharge>,
    total: &'a mut u64,
}
async fn load_replay_payload(
    connection: &Connection,
    id: i64,
    definition: &CollectionDefinition,
    request: &ClaimRequest,
    budget: &mut ReplayPayloadBudget<'_>,
    work: &mut NewWork,
) -> QueueResult<()> {
    let mut names = Vec::new();
    if let Some(selected) = &request.fields {
        budget.charges.push(
            budget.job.owned_buffer(
                (selected.len() as u64)
                    .checked_mul(256)
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        names.extend(
            selected
                .iter()
                .map(|name| (std::borrow::Cow::Borrowed(name.as_str()), false)),
        );
    } else {
        budget.charges.push(
            budget.job.owned_buffer(
                (definition.fields.len() as u64)
                    .checked_mul(256)
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        names.extend(
            definition
                .fields
                .keys()
                .map(|name| (std::borrow::Cow::Borrowed(name.as_str()), false)),
        );
        let mut ordinals = connection.query("SELECT CASE WHEN typeof(field)='integer' THEN field ELSE NULL END FROM q7_fields WHERE record_id=?1", vec![Value::Integer(id)]).await.map_err(unknown)?;
        while let Some(row) = ordinals.next().await.map_err(unknown)? {
            let ordinal = integer(&row.get_value(0).map_err(unknown)?)?;
            if ordinal < 0 || ordinal as u64 >= definition.fields.len() as u64 {
                return Err(unknown("invalid stored field ordinal"));
            }
        }
        drop(ordinals);
        let mut rows = connection.query("SELECT CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(name)='blob' THEN length(name) ELSE -1 END FROM q7_additional WHERE record_id=?1 ORDER BY id",vec![Value::Integer(id)]).await.map_err(unknown)?;
        while let Some(row) = rows.next().await.map_err(unknown)? {
            let additional_id = row.get_value(0).map_err(unknown)?;
            let length = add_payload(
                budget.total,
                integer(&row.get_value(1).map_err(unknown)?)?,
                request.max_bytes,
            )?;
            budget.charges.push(
                budget.job.owned_buffer(
                    length
                        .checked_mul(8)
                        .and_then(|n| n.checked_add(512))
                        .ok_or(QueueError::TooLarge)?,
                )?,
            );
            let name = one(
                connection,
                "SELECT name FROM q7_additional WHERE id=?1 AND record_id=?2",
                vec![Value::Integer(integer(&additional_id)?), Value::Integer(id)],
                1,
            )
            .await?
            .ok_or_else(|| unknown("additional field disappeared"))?;
            let name = std::str::from_utf8(bytes(&name[0])?).map_err(unknown)?;
            if field_number(definition, name).map_err(|error| unknown(format!("{error:?}")))? != -1
            {
                return Err(unknown("invalid additional field name"));
            }
            names.push((std::borrow::Cow::Owned(name.to_owned()), true));
        }
    }
    for (name, counted_name) in names {
        if work.fields.contains_key(name.as_ref()) {
            return Err(unknown("duplicate native field"));
        }
        let no = field_number(definition, &name)?;
        let (metadata_sql, value_sql, params) = if no >= 0 {
            (
                "SELECT CASE WHEN typeof(value)='blob' THEN length(value) ELSE -1 END FROM q7_fields WHERE record_id=?1 AND field=?2",
                "SELECT value FROM q7_fields WHERE record_id=?1 AND field=?2",
                vec![Value::Integer(id), Value::Integer(i64::from(no))],
            )
        } else {
            (
                "SELECT CASE WHEN typeof(value)='blob' THEN length(value) ELSE -1 END FROM q7_additional WHERE record_id=?1 AND name_hash=?2 AND name=?3",
                "SELECT value FROM q7_additional WHERE record_id=?1 AND name_hash=?2 AND name=?3",
                vec![
                    Value::Integer(id),
                    blob(&discovery_digest(name.as_bytes())),
                    blob(name.as_bytes()),
                ],
            )
        };
        if let Some(metadata) = one(connection, metadata_sql, params.clone(), 1).await? {
            add_payload(budget.total, integer(&metadata[0])?, request.max_bytes)?;
            if !counted_name {
                add_payload(
                    budget.total,
                    i64::try_from(name.len()).map_err(unknown)?,
                    request.max_bytes,
                )?;
            }
            budget.charges.push(
                budget.job.owned_buffer(
                    (name.len() as u64)
                        .checked_mul(4)
                        .and_then(|n| n.checked_add(256))
                        .ok_or(QueueError::TooLarge)?,
                )?,
            );
            let value = one(connection, value_sql, params, 1)
                .await?
                .ok_or_else(|| unknown("field disappeared"))?;
            let raw = bytes(&value[0])?;
            if raw.len() as i64 != integer(&metadata[0])? {
                return Err(unknown("field length changed"));
            }
            let value: FieldValue = decode(raw)?;
            definition
                .validate_value(&name, &value)
                .map_err(|error| unknown(format!("{error:?}")))?;
            work.fields.insert(name.into_owned(), value);
        }
    }
    if request.include_body {
        let metadata = one(connection,"SELECT CASE WHEN typeof(value)='blob' THEN length(value) ELSE -1 END FROM q7_bodies WHERE record_id=?1",vec![Value::Integer(id)],1).await?.ok_or_else(||unknown("replay body missing"))?;
        let length = add_payload(budget.total, integer(&metadata[0])?, request.max_bytes)?;
        let body = one(
            connection,
            "SELECT value FROM q7_bodies WHERE record_id=?1",
            vec![Value::Integer(id)],
            1,
        )
        .await?
        .ok_or_else(|| unknown("body disappeared"))?;
        if bytes(&body[0])?.len() as u64 != length {
            return Err(unknown("body length changed"));
        }
        work.body = bytes::Bytes::copy_from_slice(bytes(&body[0])?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_engine::queue_admission::QueueAdmission;
    #[tokio::test]
    async fn absent_long_names_are_borrowed_and_present_names_fail_before_copy() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let projection = QueueProjection::open(&directory.path().join("names.db"), &job, &workers)
            .await
            .unwrap();
        let name = "z".repeat(81920);
        let definition = CollectionDefinition {
            fields: BTreeMap::from([(name, FieldType::Integer)]),
            additional_field_prefixes: Default::default(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
        };
        let definition_charge = job.owned_buffer(400000).unwrap();
        let task_job = Arc::clone(&job);
        let task_projection = Arc::clone(&projection);
        workers.run(move||Ok(crate::tx::block_on_owned_apply(async {
            let _definition_charge=definition_charge;
            let writer=task_projection.writer.lock().await;let connection=writer.as_ref().unwrap();
            let mut request=ClaimRequest {queue:QueueAddress {tenant:"t".into(),queue:"q".into()},request_id:"i".into(),filter:Filter::default(),order:ClaimOrder::PriorityAscending,
                eligible_at:0,lease_nanos:1_000_000,max_records:128,max_bytes:4096,fields:None,include_body:false};
            let mut charges=Vec::new();let mut total=0;
            // 128 retained records omit one absent 80-KiB schema name each.
            let mut records=Vec::new();
            for id in 1..=128 {
                let mut work=NewWork {key:id.to_string(),priority:0,available_at:0,fields:Fields::new(),body:bytes::Bytes::new()};
                load_replay_payload(connection,id,&definition,&request,&mut ReplayPayloadBudget {job:&task_job,charges:&mut charges,total:&mut total},&mut work).await?;
                records.push(work);
            }
            assert!(records.iter().all(|work|work.fields.is_empty()));assert_eq!(total,0);
            request.fields=Some(definition.fields.keys().cloned().collect());
            let selected_charge=task_job.owned_buffer(100000)?;
            let mut work=records.pop().unwrap();
            load_replay_payload(connection,128,&definition,&request,&mut ReplayPayloadBudget {job:&task_job,charges:&mut charges,total:&mut total},&mut work).await?;
            connection.execute("INSERT INTO q7_fields(record_id,queue_id,collection,field,prefix,hash,full_key,value) VALUES(128,1,0,0,x'',x'',x'',?1)",vec![blob(&encode(&FieldValue::Integer(7))?)]).await.map_err(unknown)?;
            for selected in [None,request.fields.clone()] {
                request.fields=selected;let mut total=0;
                assert_eq!(load_replay_payload(connection,128,&definition,&request,&mut ReplayPayloadBudget {job:&task_job,charges:&mut charges,total:&mut total},&mut work).await,Err(QueueError::TooLarge));
                assert!(work.fields.is_empty());
            }
            drop(work);drop(records);drop(request);drop(selected_charge);drop(charges);drop(definition);drop(_definition_charge);Ok::<(),QueueError>(())
        }))).await.unwrap().unwrap();
        drop(projection);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        workers.close_and_drain().await.unwrap();
    }
}
