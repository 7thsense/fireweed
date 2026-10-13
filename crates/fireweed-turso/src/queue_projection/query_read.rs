//! Native indexed query pages in one covered owned snapshot.
use super::*;
use fireweed_engine::{
    Clock,
    queue_cursor::{QueryCursor, make_cursor, read_cursor},
    queue_memory::incoming_headers,
};
pub struct QueryReadInput {
    pub genesis: OwnedQueueLogRecord,
    pub request: QueryRequest,
    pub limits: QueueLimits,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedQueryPage {
    page: QueryPage,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedQueryPage {
    pub fn into_owned(
        self,
    ) -> fireweed_engine::queue_contract::OwnedQueueValue<QueryPage, Vec<OwnedQueueBufferCharge>>
    {
        fireweed_engine::queue_contract::OwnedQueueValue::new(self.page, self._charges)
    }

    pub fn page(&self) -> &QueryPage {
        &self.page
    }
}
struct CursorAttempt {
    cursor: Vec<u8>,
    charge: OwnedQueueBufferCharge,
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: QueryReadInput,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    pub async fn read_query_page(
        self: &Arc<Self>,
        input: QueryReadInput,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedQueryPage> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("query genesis belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        let headers = incoming_headers(&input.request)?;
        let probes = super::filter_sql::filter_build_charge(&input.request.filter)?;
        let containers = u64::from(input.request.max_records)
            .checked_add(1)
            .and_then(|n| n.checked_mul(1024))
            .ok_or(QueueError::TooLarge)?;
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(containers))
            .and_then(|n| n.checked_add(probes))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = Worker {
            projection: self.clone(),
            input,
            clock,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let page = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(AdmittedQueryPage {
                        page,
                        _charges: std::mem::take(&mut worker.charges),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for Worker {
    type Output = QueryPage;
    async fn before_commit(&mut self) {
        #[cfg(test)]
        {
            self.projection.pause_apply(false).await;
            if self
                .projection
                .panic_after_command
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                panic!("injected query read precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<QueryPage> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("query projection missing"))?;
        snapshot.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        let request = &self.input.request;
        let schema = snapshot.definition.schema();
        request.validate(schema, &self.input.limits)?;
        let name = match &request.source {
            RecordSource::Work => None,
            RecordSource::State(name) => Some(name.as_str()),
        };
        let collection = collection_number(schema, name)?;
        let definition = collection_definition(schema, collection)?;
        let scope = request.cursor_fingerprint(schema)?;
        let after = request
            .cursor
            .as_ref()
            .map(|raw| read_cursor(raw, &snapshot.cursor_secret, scope))
            .transpose()?;
        let now = timestamp_nanos(self.clock.now())?;
        let (sql, params) = query_sql(
            request,
            definition,
            snapshot.queue_id,
            collection,
            now,
            after.as_ref(),
        )?;
        let mut rows = connection.query(&sql, params).await.map_err(unknown)?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next().await.map_err(unknown)? {
            let id = integer(&row.get_value(0).map_err(unknown)?)?;
            let key_len =
                u64::try_from(integer(&row.get_value(1).map_err(unknown)?)?).map_err(unknown)?;
            let sort_len =
                u64::try_from(integer(&row.get_value(2).map_err(unknown)?)?).map_err(unknown)?;
            if id <= 0 {
                return Err(unknown("invalid query record id"));
            }
            candidates.push((id, key_len, sort_len));
        }
        drop(rows);
        let mut more = candidates.len() > request.max_records as usize;
        candidates.truncate(request.max_records as usize);
        let mut records = Vec::new();
        let mut cursors = Vec::new();
        let mut total = 0u64;
        let mut exceeded = false;
        for (id, key_len, sort_len) in candidates {
            let identity_bytes = key_len
                .checked_add(name.map_or(0, |name| name.len() as u64))
                .ok_or(QueueError::TooLarge)?;
            let next = total
                .checked_add(identity_bytes)
                .ok_or(QueueError::TooLarge)?;
            if next > request.max_bytes {
                if records.is_empty() {
                    return Err(QueueError::TooLarge);
                }
                more = true;
                break;
            }
            total = next;
            self.charges.push(
                self.job.owned_buffer(
                    identity_bytes
                        .checked_mul(16)
                        .and_then(|n| n.checked_add(4096))
                        .ok_or(QueueError::TooLarge)?,
                )?,
            );
            let row=one(connection,"SELECT key FROM q7_records WHERE id=?1 AND queue_id=?2 AND collection=?3 AND active=1",vec![Value::Integer(id),Value::Integer(snapshot.queue_id),Value::Integer(i64::from(collection))],1).await?.ok_or_else(||unknown("selected query identity disappeared"))?;
            let key = bytes(&row[0])?;
            if key.len() as u64 != key_len {
                return Err(unknown("selected query identity length changed"));
            }
            let key = std::str::from_utf8(key).map_err(unknown)?;
            if key.trim().is_empty() {
                return Err(unknown("invalid selected query key"));
            }
            let mut record = if let Some(name) = name {
                let authority = super::state_authority::load_state_authority(
                    connection,
                    snapshot.queue_id,
                    collection,
                    key,
                )
                .await?;
                let super::state_authority::StateAuthority::Present {
                    id: actual,
                    version,
                } = authority
                else {
                    return Err(unknown("selected query state absent"));
                };
                if actual != id {
                    return Err(unknown("selected state identity differs"));
                }
                QueryRecord::State(StateRecord {
                    collection: name.into(),
                    key: key.into(),
                    version,
                    fields: Fields::new(),
                    value: bytes::Bytes::new(),
                })
            } else {
                let Some((actual, mut work)) = super::work::load_current_work(
                    connection,
                    snapshot.queue_id,
                    key,
                    self.input.required_insertion_sequence,
                )
                .await?
                else {
                    return Err(unknown("selected query work absent"));
                };
                if actual != id {
                    return Err(unknown("selected work identity differs"));
                }
                if work.record.lifecycle == Lifecycle::Leased
                    && work
                        .record
                        .lease_expires_at
                        .is_some_and(|expiry| expiry <= now)
                {
                    work.record.lifecycle = Lifecycle::Pending;
                    work.record.lease_expires_at = None;
                }
                QueryRecord::Work(work.record)
            };
            let mut fields = Fields::new();
            let mut body = bytes::Bytes::new();
            let payload = payload::load_payload(
                connection,
                id,
                definition,
                payload::PayloadSelection {
                    fields: None,
                    include_body: true,
                    max_bytes: request.max_bytes,
                },
                &mut payload::NativePayloadBudget {
                    job: &self.job,
                    charges: &mut self.charges,
                    total: &mut total,
                    response_limit: &mut exceeded,
                },
                &mut fields,
                &mut body,
            )
            .await;
            if let Err(error) = payload {
                if error == QueueError::TooLarge && exceeded && !records.is_empty() {
                    more = true;
                    break;
                }
                return Err(error);
            }
            match &mut record {
                QueryRecord::Work(work) => {
                    work.work.fields = fields;
                    work.work.body = body;
                }
                QueryRecord::State(state) => {
                    state.fields = fields;
                    state.value = body;
                }
            }
            records.push(record);
            cursors.push((id, key_len, sort_len));
        }
        let mut page = QueryPage {
            records,
            cursor: None,
        };
        loop {
            let attempt = if more && !page.records.is_empty() {
                let (id, key_len, sort_len) = cursors[page.records.len() - 1];
                let raw = key_len.checked_add(sort_len).ok_or(QueueError::TooLarge)?;
                if raw > request.max_bytes {
                    page.records.pop();
                    more = true;
                    if page.records.is_empty() {
                        return Err(QueueError::TooLarge);
                    }
                    continue;
                }
                let charge = self.job.owned_buffer(
                    raw.checked_mul(16)
                        .and_then(|n| n.checked_add(4096))
                        .ok_or(QueueError::TooLarge)?,
                )?;
                let (join, sort) = query_sort(request, definition)?;
                let row=one(connection,&format!("SELECT {sort},r.key FROM q7_records r{join} WHERE r.id=?1 AND r.queue_id=?2 AND r.collection={collection} AND r.active=1"),vec![Value::Integer(id),Value::Integer(snapshot.queue_id)],2).await?.ok_or_else(||unknown("query cursor identity disappeared"))?;
                let sort = bytes(&row[0])?;
                let key = bytes(&row[1])?;
                if sort.len() as u64 != sort_len || key.len() as u64 != key_len {
                    return Err(unknown("query cursor identity length changed"));
                }
                let cursor = make_cursor(
                    QueryCursor {
                        scope,
                        sort: sort.to_vec(),
                        key: key.to_vec(),
                    },
                    &snapshot.cursor_secret,
                )?;
                Some(CursorAttempt { cursor, charge })
            } else {
                None
            };
            // Postcard encodes this borrowed pair identically to QueryPage's
            // two fields. A rejected attempt owns payload before its scratch
            // guard and drops both before another suffix/cursor attempt.
            if encoded_len(&(&page.records, attempt.as_ref().map(|value| &value.cursor)))?
                <= request.max_bytes
            {
                if let Some(attempt) = attempt {
                    page.cursor = Some(attempt.cursor);
                    self.charges.push(attempt.charge);
                }
                return Ok(page);
            }
            drop(attempt);
            page.records.pop();
            more = true;
            if page.records.is_empty() {
                return Err(QueueError::TooLarge);
            }
        }
    }
}
fn query_sort(
    request: &QueryRequest,
    definition: &CollectionDefinition,
) -> QueueResult<(String, &'static str)> {
    if request.index == "$key" {
        Ok((String::new(), "r.key"))
    } else {
        let number = index_number(definition, &request.index)?;
        Ok((
            format!(
                " JOIN q7_indexes i ON i.record_id=r.id AND i.queue_id=r.queue_id AND i.collection=r.collection AND i.index_no={number}"
            ),
            "i.full_key",
        ))
    }
}
fn query_sql(
    request: &QueryRequest,
    definition: &CollectionDefinition,
    queue_id: i64,
    collection: i32,
    now: i64,
    after: Option<&QueryCursor>,
) -> QueueResult<(String, Vec<Value>)> {
    let mut pool = super::filter_sql::BytePool(Vec::new());
    let (with, mut filter) = super::filter_sql::filter_sql(
        &request.filter,
        definition,
        queue_id,
        collection,
        &mut pool,
    )?;
    if let Some(lifecycle) = request.filter.lifecycle {
        filter.push_str(&format!(" AND (CASE WHEN r.lifecycle=1 AND r.lease_until<={now} THEN 0 ELSE r.lifecycle END)={}",encode_lifecycle(lifecycle)));
    }
    let (join, sort) = query_sort(request, definition)?;
    if let Some(after) = after {
        let sp = pool.bytes(&after.sort);
        let kp = pool.bytes(&after.key);
        let op = if request.descending { "<" } else { ">" };
        filter.push_str(&format!(" AND ({sort},r.key){op}({sp},{kp})"));
    }
    let direction = if request.descending { "DESC" } else { "ASC" };
    let sql = format!(
        "{with}SELECT CASE WHEN typeof(r.id)='integer' THEN r.id ELSE NULL END,CASE WHEN typeof(r.key)='blob' THEN length(r.key) ELSE -1 END,CASE WHEN typeof({sort})='blob' THEN length({sort}) ELSE -1 END FROM q7_records r{join} WHERE r.queue_id={queue_id} AND r.collection={collection} AND r.active=1{filter} ORDER BY {sort} {direction},r.key {direction} LIMIT {}",
        u64::from(request.max_records) + 1
    );
    Ok((
        sql,
        if pool.0.is_empty() {
            vec![]
        } else {
            vec![Value::Blob(pool.0)]
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{address, bootstrap, seed};
    use super::*;
    use fireweed_conformance::queue_contract::TestClock;
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    };
    fn input(
        definition: &AdmittedQueueDefinition,
        job: &Arc<QueueReservation>,
        request: QueryRequest,
    ) -> QueryReadInput {
        let definition = AdmittedQueueDefinition::from_raw(definition.raw(), job).unwrap();
        QueryReadInput {
            genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 1 << 20, job)
                .unwrap(),
            request,
            limits: QueueLimits::default(),
            required_next_offset: 2,
            required_insertion_sequence: 2,
            required_last_record_epoch: 1,
        }
    }
    fn request() -> QueryRequest {
        QueryRequest {
            queue: address(),
            source: RecordSource::Work,
            index: "u".into(),
            filter: Filter::default(),
            descending: false,
            cursor: None,
            max_records: 1,
            max_bytes: 65536,
        }
    }
    fn key(record: &QueryRecord) -> &str {
        match record {
            QueryRecord::Work(work) => &work.work.key,
            QueryRecord::State(state) => &state.key,
        }
    }
    #[tokio::test]
    async fn indexed_query_pages_filter_before_limit_and_bind_cursor_to_original_scope() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("query.db");
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        seed(&projection, &definition, &job, &workers, false).await;
        let clock = Arc::new(TestClock::default());
        clock.set(100);
        let first = projection
            .read_query_page(
                input(&definition, &job, request()),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(first.page().records.len(), 1);
        assert_eq!(key(&first.page().records[0]), "a");
        assert!(first.page().cursor.is_some());
        let mut next = request();
        next.cursor = first.page().cursor.clone();
        drop(first);
        let second = projection
            .read_query_page(
                input(&definition, &job, next.clone()),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(key(&second.page().records[0]), "b");
        assert_eq!(second.page().cursor, None);
        drop(second);
        let mut changed = next.clone();
        changed.descending = true;
        assert!(matches!(
            projection
                .read_query_page(
                    input(&definition, &job, changed),
                    clock.clone(),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::Invalid(_))
        ));
        let mut corrupted = next;
        corrupted.cursor.as_mut().unwrap()[0] ^= 1;
        assert!(matches!(
            projection
                .read_query_page(
                    input(&definition, &job, corrupted),
                    clock.clone(),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::Invalid(_))
        ));
        let mut filtered = request();
        filtered
            .filter
            .fields
            .insert("u".into(), Predicate::Equal(FieldValue::Integer(2)));
        let result = projection
            .read_query_page(
                input(&definition, &job, filtered.clone()),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.page().records.len(), 1);
        assert_eq!(key(&result.page().records[0]), "b");
        assert_eq!(result.page().cursor, None);
        let exact = encoded_len(result.page()).unwrap();
        drop(result);
        filtered.max_bytes = exact;
        let result = projection
            .read_query_page(
                input(&definition, &job, filtered.clone()),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(key(&result.page().records[0]), "b");
        drop(result);
        filtered.max_bytes = exact - 1;
        assert!(matches!(
            projection
                .read_query_page(
                    input(&definition, &job, filtered),
                    clock.clone(),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::TooLarge)
        ));
        let mut descending = request();
        descending.descending = true;
        let result = projection
            .read_query_page(input(&definition, &job, descending), clock, &job, &workers)
            .await
            .unwrap();
        assert_eq!(key(&result.page().records[0]), "b");
        drop(result);
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[tokio::test]
    async fn compact_wide_query_filters_are_admitted_before_native_dispatch() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wide-query.db");
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        seed(&projection, &definition, &job, &workers, false).await;
        let mut request = request();
        request.filter.fields.insert(
            "u".into(),
            Predicate::In(vec![FieldValue::Integer(1); 5001]),
        );
        let result = projection
            .read_query_page(
                input(&definition, &job, request.clone()),
                Arc::new(TestClock::default()),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(key(&result.page().records[0]), "a");
        drop(result);
        let old = encoded_len(&request).unwrap() * 16
            + incoming_headers(&request).unwrap()
            + 2048
            + 65536;
        let probes = super::super::filter_sql::filter_build_charge(&request.filter).unwrap();
        let tight = QueueAdmission::new(1, old + probes / 2).unwrap();
        let tiny = Arc::new(tight.reserve(0).unwrap());
        let clock = Arc::new(TestClock::default());
        let held = projection.writer.lock().await;
        let rejected = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            projection.read_query_page(input(&definition, &tiny, request), clock, &tiny, &workers),
        )
        .await
        .expect("filter expansion rejection precedes native dispatch");
        assert!(
            matches!(rejected,Err(QueueError::OutcomeUnknown(message)) if message=="admitted buffer capacity exhausted; resolve owned operation")
        );
        drop(held);
        drop(tiny);
        assert_eq!(tight.usage().unwrap(), (0, 0));
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
    }
    #[tokio::test]
    async fn query_byte_prefix_omits_unmatched_opaque_payload_and_never_skips_first_nonfit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("query-prefix.db");
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        seed(&projection, &definition, &job, &workers, true).await;
        let clock = Arc::new(TestClock::default());
        clock.set(100);
        let mut filtered = request();
        filtered
            .filter
            .fields
            .insert("u".into(), Predicate::Equal(FieldValue::Integer(2)));
        let result = projection
            .read_query_page(
                input(&definition, &job, filtered),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(key(&result.page().records[0]), "b");
        drop(result);
        assert!(matches!(
            projection
                .read_query_page(
                    input(&definition, &job, request()),
                    clock.clone(),
                    &job,
                    &workers
                )
                .await,
            Err(QueueError::TooLarge)
        ));
        let mut descending = request();
        descending.descending = true;
        descending.max_records = 2;
        let result = projection
            .read_query_page(
                input(&definition, &job, descending.clone()),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(result.page().records.len(), 1);
        assert_eq!(key(&result.page().records[0]), "b");
        assert!(result.page().cursor.is_some());
        descending.cursor = result.page().cursor.clone();
        descending.max_bytes = 4 << 20;
        drop(result);
        let result = projection
            .read_query_page(input(&definition, &job, descending), clock, &job, &workers)
            .await
            .unwrap();
        assert_eq!(key(&result.page().records[0]), "a");
        assert_eq!(result.page().cursor, None);
        assert!(
            matches!(&result.page().records[0],QueryRecord::Work(work) if work.work.body.len()==1<<20&&matches!(work.work.fields.get("opaque"),Some(FieldValue::Bytes(value)) if value.len()==1<<20))
        );
        drop(result);
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    async fn seed_custom(
        path: &std::path::Path,
        schema: QueueSchema,
        items: Vec<NewWork>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> (Arc<QueueProjection>, AdmittedQueueDefinition) {
        let definition = AdmittedQueueDefinition::from_raw(
            &fireweed_engine::queue_schema_codec::encode_definition(&schema).unwrap(),
            job,
        )
        .unwrap();
        let projection = QueueProjection::open(path, job, workers).await.unwrap();
        let genesis =
            encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, job).unwrap();
        projection.apply(genesis, 0, job, workers).await.unwrap();
        let prepared = projection
            .prepare_insert(
                InsertReplayInput {
                    genesis: encode_checked_genesis(
                        1,
                        &address(),
                        &definition,
                        &[31; 32],
                        16 << 20,
                        job,
                    )
                    .unwrap(),
                    request: InsertRequest {
                        queue: address(),
                        request_id: "query-seed-many".into(),
                        mode: InsertMode::InsertOnly,
                        items,
                    },
                    limits: QueueLimits::default(),
                    required_next_offset: 1,
                    required_insertion_sequence: 0,
                    required_last_record_epoch: 1,
                },
                1,
                16 << 20,
                Arc::new(TestClock::default()),
                job,
                workers,
            )
            .await
            .unwrap()
            .unwrap();
        let (record, response, head) = prepared.into_parts();
        projection.apply(record, head, job, workers).await.unwrap();
        drop(response);
        (projection, definition)
    }
    #[tokio::test]
    async fn long_query_identities_stop_before_hydration_exhausts_a_valid_page_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 1 << 30).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let items = (0..512)
            .map(|n| NewWork {
                key: format!("{n:04}{}", "k".repeat(2048)),
                priority: 0,
                available_at: 0,
                fields: [("u".into(), FieldValue::Integer(n))].into(),
                body: bytes::Bytes::new(),
            })
            .collect();
        let (projection, definition) = seed_custom(
            &directory.path().join("long-keys.db"),
            super::super::tests::schema(),
            items,
            &job,
            &workers,
        )
        .await;
        let tiny_admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let tiny = Arc::new(tiny_admission.reserve(0).unwrap());
        let mut request = request();
        request.index = "$key".into();
        request.max_records = 4096;
        request.max_bytes = 20000;
        let mut query = input(&definition, &tiny, request);
        query.required_insertion_sequence = 512;
        let result = projection
            .read_query_page(query, Arc::new(TestClock::default()), &tiny, &workers)
            .await
            .unwrap();
        assert!(!result.page().records.is_empty());
        assert!(result.page().records.len() < 10);
        assert!(result.page().cursor.is_some());
        assert!(encoded_len(result.page()).unwrap() <= 20000);
        assert_eq!(
            key(&result.page().records[0]),
            format!("0000{}", "k".repeat(2048))
        );
        drop(result);
        drop(tiny);
        assert_eq!(tiny_admission.usage().unwrap(), (0, 0));
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
    }
    #[tokio::test]
    async fn composite_sort_values_trim_suffixes_with_one_cursor_scratch_owner() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let mut schema = super::super::tests::schema();
        let columns = (0..8).map(|n| format!("c{n}")).collect::<Vec<_>>();
        schema.work = CollectionDefinition {
            fields: columns
                .iter()
                .map(|name| (name.clone(), FieldType::Bytes))
                .collect(),
            indexes: [("wide".into(), columns.clone())].into(),
            unique_indexes: Default::default(),
            additional_field_prefixes: Default::default(),
        };
        let items = (0..4)
            .map(|n| NewWork {
                key: format!("k{n}"),
                priority: 0,
                available_at: 0,
                fields: columns
                    .iter()
                    .map(|name| (name.clone(), FieldValue::Bytes(vec![0; 800])))
                    .collect(),
                body: bytes::Bytes::new(),
            })
            .collect();
        let (projection, definition) = seed_custom(
            &directory.path().join("wide-sort.db"),
            schema,
            items,
            &job,
            &workers,
        )
        .await;
        let tiny_admission = QueueAdmission::new(1, 768 << 10).unwrap();
        let tiny = Arc::new(tiny_admission.reserve(0).unwrap());
        let mut request = request();
        request.index = "wide".into();
        request.max_records = 3;
        request.max_bytes = 20000;
        let mut query = input(&definition, &tiny, request.clone());
        query.required_insertion_sequence = 4;
        let result = projection
            .read_query_page(query, Arc::new(TestClock::default()), &tiny, &workers)
            .await
            .unwrap();
        assert!(!result.page().records.is_empty());
        assert!(
            result.page().records.len() == 1,
            "escaped composite sort cursor must trim two suffix records"
        );
        assert!(result.page().cursor.is_some());
        assert!(encoded_len(result.page()).unwrap() <= 20000);
        let keys = result
            .page()
            .records
            .iter()
            .map(|record| key(record).to_owned())
            .collect::<Vec<_>>();
        let cursor = result.page().cursor.clone();
        let exact = encoded_len(result.page()).unwrap();
        drop(result);
        request.max_bytes = exact;
        let mut query = input(&definition, &tiny, request.clone());
        query.required_insertion_sequence = 4;
        let result = projection
            .read_query_page(query, Arc::new(TestClock::default()), &tiny, &workers)
            .await
            .unwrap();
        assert_eq!(
            result
                .page()
                .records
                .iter()
                .map(|record| key(record).to_owned())
                .collect::<Vec<_>>(),
            keys
        );
        assert_eq!(result.page().cursor, cursor);
        drop(result);
        drop(tiny);
        assert_eq!(tiny_admission.usage().unwrap(), (0, 0));
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
    }
    #[tokio::test]
    async fn state_queries_are_complete_and_work_queries_use_effective_expired_leases() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("query-state-lease.db");
        let admission = QueueAdmission::new(1, 128 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(&path, &job, &workers).await;
        seed(&projection, &definition, &job, &workers, false).await;
        let clock = Arc::new(TestClock::default());
        clock.set(100);
        let mut state = super::super::tests::group("query-state-put");
        state.state_updates.push(StateUpdate {
            collection: "workflow".into(),
            key: "run/\0λ".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: [("phase".into(), FieldValue::Text("running".into()))].into(),
                value: vec![7, 8].into(),
            },
        });
        let prepared = projection
            .prepare_ordinary_commit(
                CommitPreparationInput {
                    original: None,
                    genesis: encode_checked_genesis(
                        1,
                        &address(),
                        &definition,
                        &[31; 32],
                        16 << 20,
                        &job,
                    )
                    .unwrap(),
                    queue: address(),
                    group: state,
                    limits: QueueLimits::default(),
                    required_next_offset: 2,
                    required_insertion_sequence: 2,
                    required_last_record_epoch: 1,
                },
                1,
                16 << 20,
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(response);
        let mut state_query = request();
        state_query.source = RecordSource::State("workflow".into());
        state_query.index = "$key".into();
        state_query.filter.fields.insert(
            "phase".into(),
            Predicate::Equal(FieldValue::Text("running".into())),
        );
        let mut query = input(&definition, &job, state_query);
        query.required_next_offset = 3;
        let result = projection
            .read_query_page(query, clock.clone(), &job, &workers)
            .await
            .unwrap();
        assert!(
            matches!(&result.page().records[..],[QueryRecord::State(state)] if state.key=="run/\0λ"&&state.version==1&&state.value.as_ref()==[7,8]&&state.fields.get("phase")==Some(&FieldValue::Text("running".into())))
        );
        drop(result);
        let claim = ClaimRequest {
            queue: address(),
            request_id: "query-claim".into(),
            filter: Filter {
                keys: Some(["a".into()].into()),
                ..Default::default()
            },
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 1,
            max_bytes: 65536,
            fields: None,
            include_body: true,
        };
        let prepared = projection
            .prepare_claim(
                ClaimReplayInput {
                    genesis: encode_checked_genesis(
                        1,
                        &address(),
                        &definition,
                        &[31; 32],
                        16 << 20,
                        &job,
                    )
                    .unwrap(),
                    request: claim,
                    now: 0,
                    limits: QueueLimits::default(),
                    required_next_offset: 3,
                    required_insertion_sequence: 2,
                    required_last_record_epoch: 1,
                },
                1,
                16 << 20,
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap()
            .unwrap();
        let (record, response, head) = prepared.into_parts();
        projection
            .apply(record, head, &job, &workers)
            .await
            .unwrap();
        drop(response);
        let mut leased = request();
        leased.filter.lifecycle = Some(Lifecycle::Leased);
        let mut query = input(&definition, &job, leased.clone());
        query.required_next_offset = 4;
        let result = projection
            .read_query_page(query, clock.clone(), &job, &workers)
            .await
            .unwrap();
        assert!(
            matches!(&result.page().records[..],[QueryRecord::Work(work)] if work.work.key=="a"&&work.lifecycle==Lifecycle::Leased&&work.lease_expires_at==Some(1_000_100))
        );
        drop(result);
        clock.set(1_000_100);
        let mut query = input(&definition, &job, leased);
        query.required_next_offset = 4;
        let result = projection
            .read_query_page(query, clock.clone(), &job, &workers)
            .await
            .unwrap();
        assert!(result.page().records.is_empty());
        drop(result);
        let mut pending = request();
        pending.filter.lifecycle = Some(Lifecycle::Pending);
        pending.max_records = 2;
        let mut query = input(&definition, &job, pending);
        query.required_next_offset = 4;
        let result = projection
            .read_query_page(query, clock.clone(), &job, &workers)
            .await
            .unwrap();
        assert_eq!(result.page().records.len(), 2);
        assert!(result.page().records.iter().all(|record|matches!(record,QueryRecord::Work(work) if work.lifecycle==Lifecycle::Pending&&work.lease_expires_at.is_none())));
        drop(result);
        // Known inconsistent lease authority fails before exposing a work DTO.
        super::super::tests::execute_for_test(
            &projection,
            "UPDATE q7_records SET lease_hash=NULL WHERE key=?1",
            vec![blob(b"a")],
        )
        .await;
        let mut query = input(&definition, &job, request());
        query.required_next_offset = 4;
        assert!(matches!(
            projection
                .read_query_page(query, clock, &job, &workers)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        drop(projection);
        drop(definition);
        workers.close_and_drain().await.unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
