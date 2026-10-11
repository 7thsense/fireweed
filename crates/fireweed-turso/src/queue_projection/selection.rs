//! Native filtered ordered candidates. This never leases a broad queue and
//! applies a consumer-side filter. Durable claim publication is a separate step.
use super::*;
use fireweed_engine::queue_index::scalar_type_prefix;

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
    let entries = input.request.filter.fields.values().try_fold(
        input
            .request
            .filter
            .keys
            .as_ref()
            .map_or(0, |keys| keys.len()) as u64,
        |n, predicate| {
            n.checked_add(match predicate {
                Predicate::In(values) => values.len() as u64,
                _ => 2,
            })
            .ok_or(QueueError::TooLarge)
        },
    )?;
    let charge = encoded_len(&input.request)?
        .checked_mul(16)
        .and_then(|n| n.checked_add(entries.checked_mul(2048)?))
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
/// One bound byte pool keeps key/IN sets independent of native parameter caps.
/// Only numeric slices and fixed SQL vocabulary enter statement text.
struct BytePool(Vec<u8>);
impl BytePool {
    fn bytes(&mut self, value: &[u8]) -> String {
        let start = self.0.len() + 1;
        self.0.extend_from_slice(value);
        format!("substr(?1,{start},{})", value.len())
    }
    fn list(&mut self, values: impl IntoIterator<Item = Vec<u8>>) -> String {
        let expressions = values
            .into_iter()
            .map(|value| format!("({})", self.bytes(&value)))
            .collect::<Vec<_>>();
        if expressions.is_empty() {
            "SELECT NULL WHERE 0".into()
        } else {
            format!("SELECT column1 FROM (VALUES {})", expressions.join(","))
        }
    }
}
fn probe_rows(
    filter: &Filter,
    definition: &CollectionDefinition,
    pool: &mut BytePool,
) -> QueueResult<Vec<String>> {
    let mut rows = Vec::new();
    for (id, (name, predicate)) in filter.fields.iter().enumerate() {
        let no = field_number(definition, name)?;
        let (name_hash, full_name) = if no >= 0 {
            ("NULL".into(), "NULL".into())
        } else {
            (
                pool.bytes(&discovery_digest(name.as_bytes())),
                pool.bytes(name.as_bytes()),
            )
        };
        let identity = format!("{id},{no},{name_hash},{full_name}");
        let mut equality = |value: &FieldValue| -> QueueResult<()> {
            let key = ordered_key(&[Some(value)])?;
            let hash = pool.bytes(&discovery_digest(&key));
            let key = pool.bytes(&key);
            rows.push(format!(
                "({identity},0,{hash},{key},NULL,NULL,NULL,NULL,0,NULL,NULL,0)"
            ));
            Ok(())
        };
        match predicate {
            Predicate::Equal(value) => equality(value)?,
            Predicate::In(values) => {
                for value in values {
                    equality(value)?;
                }
            }
            Predicate::Range { lower, upper } => {
                let first = lower
                    .as_ref()
                    .or(upper.as_ref())
                    .ok_or_else(|| QueueError::Invalid("empty range".into()))?;
                let value = match first {
                    Bound::Included(value) | Bound::Excluded(value) => value,
                };
                let floor = scalar_type_prefix(value.field_type());
                let mut ceiling = floor;
                ceiling[2] += 1;
                let floor = pool.bytes(&floor);
                let ceiling = pool.bytes(&ceiling);
                let mut bound = |bound: &Option<Bound>| -> QueueResult<String> {
                    Ok(match bound {
                        None => "NULL,NULL,0".into(),
                        Some(bound) => {
                            let (value, mode) = match bound {
                                Bound::Included(value) => (value, 1),
                                Bound::Excluded(value) => (value, 2),
                            };
                            let key = ordered_key(&[Some(value)])?;
                            let prefix = pool.bytes(&discovery_prefix(&key));
                            let key = pool.bytes(&key);
                            format!("{prefix},{key},{mode}")
                        }
                    })
                };
                let lower = bound(lower)?;
                let upper = bound(upper)?;
                rows.push(format!(
                    "({identity},1,NULL,NULL,{floor},{ceiling},{lower},{upper})"
                ));
            }
        }
    }
    Ok(rows)
}
fn indexed_field_intersection(rows: &[String], count: usize, queue_id: i64) -> (String, String) {
    if rows.is_empty() {
        return (String::new(), String::new());
    }
    let range = "f.prefix>=p.floor AND f.prefix<p.ceiling AND (p.lower_mode=0 OR (f.prefix>=p.lower_prefix AND ((p.lower_mode=1 AND f.full_key>=p.lower_key) OR (p.lower_mode=2 AND f.full_key>p.lower_key)))) AND (p.upper_mode=0 OR (f.prefix<=p.upper_prefix AND ((p.upper_mode=1 AND f.full_key<=p.upper_key) OR (p.upper_mode=2 AND f.full_key<p.upper_key))))";
    let mut branches = Vec::new();
    for (table, identity, is_declared) in [
        ("q7_fields", "f.field=p.field", true),
        (
            "q7_additional",
            "f.name_hash=p.name_hash AND f.name=p.name",
            false,
        ),
    ] {
        let no = if is_declared {
            "p.field>=0"
        } else {
            "p.field<0"
        };
        for (kind, predicate) in [(0, "f.hash=p.hash AND f.full_key=p.full_key"), (1, range)] {
            let index = match (is_declared, kind) {
                (true, 0) => "q7_field_equal",
                (true, _) => "q7_field_range",
                (false, 0) => "q7_additional_equal",
                (false, _) => "q7_additional_range",
            };
            branches.push(format!("SELECT f.record_id,p.n FROM probes p CROSS JOIN {table} f INDEXED BY {index} WHERE f.queue_id={queue_id} AND f.collection=0 AND {identity} AND {predicate} AND {no} AND p.kind={kind}"));
        }
    }
    (
        format!(
            "WITH probes(n,field,name_hash,name,kind,hash,full_key,floor,ceiling,lower_prefix,lower_key,lower_mode,upper_prefix,upper_key,upper_mode) AS (VALUES {}),matches AS ({}) ",
            rows.join(","),
            branches.join(" UNION ALL ")
        ),
        format!(
            "r.id IN (SELECT record_id FROM matches GROUP BY record_id HAVING COUNT(DISTINCT n)={count})"
        ),
    )
}
pub(super) fn candidate_sql(
    request: &ClaimRequest,
    schema: &QueueSchema,
    queue_id: i64,
    now: i64,
) -> QueueResult<(String, Vec<u8>)> {
    let mut pool = BytePool(Vec::new());
    let mut conjuncts = Vec::new();
    if let Some(keys) = &request.filter.keys {
        let hashes = pool.list(
            keys.iter()
                .map(|key| discovery_digest(key.as_bytes()).to_vec()),
        );
        let keys = pool.list(keys.iter().map(|key| key.as_bytes().to_vec()));
        conjuncts.push(format!("r.key_hash IN ({hashes}) AND r.key IN ({keys})"));
    }
    let rows = probe_rows(&request.filter, &schema.work, &mut pool)?;
    let (with, fields) = indexed_field_intersection(&rows, request.filter.fields.len(), queue_id);
    if !fields.is_empty() {
        conjuncts.push(fields);
    }
    let filter = format!(
        " AND {}",
        fireweed_engine::queue_sql::balanced_boolean(conjuncts, true)
    );
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
