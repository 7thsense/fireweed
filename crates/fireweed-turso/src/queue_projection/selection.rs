//! Native filtered ordered candidates. This never leases a broad queue and
//! applies a consumer-side filter. Durable claim publication is a separate step.
use super::*;

pub struct AdmittedClaimSelection {
    keys: Vec<String>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedClaimSelection {
    pub fn keys(&self) -> &[String] {
        &self.keys
    }
}
struct SelectionWorker {
    projection: Arc<QueueProjection>,
    input: ClaimReplayInput,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
pub(super) fn selection_charge(input: &ClaimReplayInput) -> QueueResult<u64> {
    let probes = super::filter_sql::filter_build_charge(&input.request.filter)?;
    let charge = encoded_len(&input.request)?
        .checked_mul(16)
        .and_then(|n| n.checked_add(probes))
        .and_then(|n| n.checked_add(u64::from(input.request.max_records).checked_mul(256)?))
        .and_then(|n| n.checked_add(input.request.max_bytes.checked_mul(4)?))
        .and_then(|n| n.checked_add(65536))
        .ok_or(QueueError::TooLarge)?;
    Ok(charge)
}
impl QueueProjection {
    /// Internal candidate snapshot. The caller must retain/recheck recovered
    /// authority and conditionally publish the final resolved lease transitions.
    pub async fn select_claim_candidates(
        self: &Arc<Self>,
        input: ClaimReplayInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedClaimSelection> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("selection belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        let charge = selection_charge(&input)?;
        let mut worker = SelectionWorker {
            projection: Arc::clone(self),
            input,
            job: Arc::clone(job),
            charges: vec![job.owned_buffer(charge)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = Arc::clone(&worker.projection);
                    let keys = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(AdmittedClaimSelection {
                        keys,
                        _charges: std::mem::take(&mut worker.charges),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for SelectionWorker {
    type Output = Vec<String>;
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let snapshot = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("selection projection missing"))?;
        snapshot.validate_claim_input(&self.input)?;
        let request = &self.input.request;
        let (sql, pool) = candidate_sql(
            request,
            snapshot.definition.schema(),
            snapshot.queue_id,
            self.input.now,
        )?;
        let params = if pool.is_empty() {
            Vec::new()
        } else {
            vec![Value::Blob(pool)]
        };
        let mut rows = connection.query(&sql, params).await.map_err(unknown)?;
        let mut keys = Vec::new();
        let mut size = 0u64;
        while let Some(row) = rows.next().await.map_err(unknown)? {
            let id = integer(&row.get_value(0).map_err(unknown)?)?;
            let length =
                u64::try_from(integer(&row.get_value(1).map_err(unknown)?)?).map_err(unknown)?;
            let next = size.checked_add(length).ok_or(QueueError::TooLarge)?;
            if next > request.max_bytes {
                if keys.is_empty() {
                    return Err(QueueError::TooLarge);
                }
                break;
            }
            let key = one(
                connection,
                "SELECT key FROM q7_records WHERE id=?1 AND queue_id=?2",
                vec![Value::Integer(id), Value::Integer(snapshot.queue_id)],
                1,
            )
            .await?
            .ok_or_else(|| unknown("selected record disappeared"))?;
            let key = bytes(&key[0])?;
            if key.len() as u64 != length {
                return Err(unknown("selected key length changed"));
            }
            let key = std::str::from_utf8(key).map_err(unknown)?;
            if key.trim().is_empty() {
                return Err(unknown("invalid selected key"));
            }
            keys.push(key.to_owned());
            size = next;
        }
        Ok(keys)
    }
}
pub(super) fn candidate_sql(
    request: &ClaimRequest,
    schema: &QueueSchema,
    queue_id: i64,
    now: i64,
) -> QueueResult<(String, Vec<u8>)> {
    let mut pool = super::filter_sql::BytePool(Vec::new());
    let (with, filter) =
        super::filter_sql::filter_sql(&request.filter, &schema.work, queue_id, 0, &mut pool)?;
    // Match the established PostgreSQL progress policy with exact i128 arithmetic.
    let threshold = i128::from(now) - i128::from(schema.progress_bound_nanos / 4 * 3);
    let overdue = match i64::try_from(threshold) {
        Ok(threshold) => format!("r.eligible_since<={threshold}"),
        Err(_) => "0".into(),
    };
    let order = match request.order {
        ClaimOrder::PriorityAscending => "r.priority ASC",
        ClaimOrder::PriorityDescending => "r.priority DESC",
        ClaimOrder::ScheduledAscending => "r.available_at ASC",
    };
    Ok((
        format!(
            "{with}SELECT CASE WHEN typeof(r.id)='integer' THEN r.id ELSE NULL END,CASE WHEN typeof(r.key)='blob' THEN length(r.key) ELSE -1 END FROM q7_records r WHERE r.queue_id={queue_id} AND r.collection=0 AND r.active=1 AND (r.lifecycle=0 OR (r.lifecycle=1 AND r.lease_until<={now})) AND r.available_at<={}{filter} ORDER BY CASE WHEN {overdue} THEN 0 ELSE 1 END,CASE WHEN {overdue} THEN r.eligible_since ELSE 0 END,{order},r.insertion_sequence LIMIT {}",
            request.eligible_at, request.max_records
        ),
        pool.0,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_engine::queue_admission::QueueAdmission;
    #[tokio::test]
    async fn native_ordering_uses_schedule_age_and_exact_unsigned_sequence() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let projection =
            QueueProjection::open(&directory.path().join("ordering.db"), &job, &workers)
                .await
                .unwrap();
        let task = Arc::clone(&projection);
        workers.run(move||Ok(crate::tx::block_on_owned_apply(async {
            let writer=task.writer.lock().await;let connection=writer.as_ref().unwrap();
            for (id,key,sequence) in [(1,b"a",u64::MAX),(2,b"b",2)] {
                connection.execute("INSERT INTO q7_records(id,queue_id,collection,key_hash,key,version,active,lifecycle,priority,available_at,eligible_since,attempts,insertion_sequence,last_change_offset,prior_highwater) VALUES(?1,42,0,?2,?3,?4,1,0,0,0,0,0,?5,1,?4)",vec![Value::Integer(id),blob(&discovery_digest(key)),blob(key),number(1),number(sequence)]).await.map_err(unknown)?;
            }
            let mut request=ClaimRequest {queue:QueueAddress {tenant:"t".into(),queue:"q".into()},request_id:"r".into(),filter:Filter::default(),order:ClaimOrder::PriorityAscending,eligible_at:0,lease_nanos:1_000_000,max_records:2,max_bytes:4096,fields:Some(Default::default()),include_body:false};
            let mut schema=QueueSchema {work:CollectionDefinition {fields:Default::default(),indexes:Default::default(),unique_indexes:Default::default(),additional_field_prefixes:Default::default()},state:Default::default(),progress_bound_nanos:u64::MAX,max_attempts:3};
            async fn order(connection:&Connection,request:&ClaimRequest,schema:&QueueSchema,now:i64)->QueueResult<Vec<i64>> {
                let (sql,pool)=candidate_sql(request,schema,42,now)?;assert!(pool.is_empty());let mut rows=connection.query(&sql,()).await.map_err(unknown)?;let mut out=Vec::new();while let Some(row)=rows.next().await.map_err(unknown)? {out.push(integer(&row.get_value(0).map_err(unknown)?)?);}Ok(out)
            }
            assert_eq!(order(connection,&request,&schema,0).await?,[2,1]);
            connection.execute("UPDATE q7_records SET priority=CASE id WHEN 1 THEN 8 ELSE -9 END,available_at=CASE id WHEN 1 THEN -1000 ELSE 0 END",()).await.map_err(unknown)?;
            request.order=ClaimOrder::ScheduledAscending;assert_eq!(order(connection,&request,&schema,0).await?,[1,2]);
            request.order=ClaimOrder::PriorityAscending;
            assert_eq!(order(connection,&request,&schema,i64::MIN).await?,[2,1]);
            schema.progress_bound_nanos=100;
            connection.execute("UPDATE q7_records SET eligible_since=CASE id WHEN 1 THEN -100 ELSE 100 END",()).await.map_err(unknown)?;
            assert_eq!(order(connection,&request,&schema,0).await?,[1,2]);
            connection.execute("UPDATE q7_records SET eligible_since=CASE id WHEN 1 THEN -100 ELSE -200 END,priority=CASE id WHEN 1 THEN -9 ELSE 8 END",()).await.map_err(unknown)?;
            assert_eq!(order(connection,&request,&schema,0).await?,[2,1]);Ok::<(),QueueError>(())
        }))).await.unwrap().unwrap();
        drop(projection);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        workers.close_and_drain().await.unwrap();
    }
}
