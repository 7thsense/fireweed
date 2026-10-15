//! Native grouped counts use a flat dimension table, avoiding one join per
//! dimension. Only bounded representative field values cross the SDK boundary.
use super::*;
pub(super) async fn load_groups(
    connection: &Connection,
    snapshot: &super::snapshot::NativeQueueSnapshot,
    request: &StatsRequest,
    now: i64,
    limits: &QueueLimits,
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
) -> QueueResult<Vec<StatsGroup>> {
    if request.group_by.is_empty() {
        return Ok(Vec::new());
    }
    let definition = &snapshot.definition.schema().work;
    let mut pool = super::filter_sql::BytePool::default();
    let (with, mut filter) = super::filter_sql::filter_sql(
        &request.filter,
        definition,
        snapshot.queue_id,
        0,
        &mut pool,
    )?;
    let effective = super::stats_read::effective_lifecycle(now);
    if let Some(lifecycle) = request.filter.lifecycle {
        filter.push_str(&format!(
            " AND ({effective})={}",
            encode_lifecycle(lifecycle)
        ));
    }
    let dimensions = pool.dimensions(request.group_by.iter().map(String::as_str), definition)?;
    let prefix = if with.is_empty() {
        "WITH ".into()
    } else {
        format!("{},", with.trim_end())
    };
    let ctes = format!(
        "{prefix}{},filtered AS (SELECT r.id,r.lifecycle,r.lease_until,r.lease_hash,r.eligible_since,r.available_at FROM q7_records r WHERE r.queue_id={} AND r.collection=0 AND r.active=1{filter}),group_values AS (SELECT r.id AS record_id,p.n,f.full_key FROM filtered r CROSS JOIN group_dims p LEFT JOIN q7_fields f ON f.record_id=r.id AND f.queue_id={} AND f.collection=0 AND f.field=p.field WHERE p.field>=0 UNION ALL SELECT r.id AS record_id,p.n,f.full_key FROM filtered r CROSS JOIN group_dims p LEFT JOIN q7_additional f ON f.record_id=r.id AND f.queue_id={} AND f.collection=0 AND f.name_hash=p.name_hash AND f.name=p.name WHERE p.field<0)",
        dimensions, snapshot.queue_id, snapshot.queue_id, snapshot.queue_id
    );
    let params = || pool.params();
    // Bound one composed grouping key before native sorting/aggregation. The
    // metadata pass never fetches a field value or work body.
    let width=one(connection,&format!("{ctes} SELECT CASE WHEN MIN(bytes)<0 THEN -1 ELSE COALESCE(MAX(bytes),0) END FROM (SELECT record_id,CASE WHEN MIN(CASE WHEN full_key IS NULL OR typeof(full_key)='blob' THEN 1 ELSE 0 END)=0 THEN -1 ELSE COALESCE(SUM(length(full_key)),0) END AS bytes FROM group_values GROUP BY record_id)"),params()?,1).await?.ok_or_else(||unknown("group width metadata missing"))?;
    let width = u64::try_from(integer(&width[0])?).map_err(unknown)?;
    let key = width
        .checked_mul(2)
        .and_then(|n| n.checked_add((request.group_by.len() as u64).checked_mul(3)?))
        .ok_or(QueueError::TooLarge)?;
    charges.push(
        job.owned_buffer(
            key.checked_mul(16)
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?,
    );
    let aggregate = super::stats_read::aggregate(&effective, request.eligible_at, "1");
    let sql = format!(
        "{ctes},tuples AS (SELECT r.id AS record_id,(SELECT group_concat(cell,'') FROM (SELECT CASE WHEN full_key IS NULL THEN '0;' ELSE '1'||hex(full_key)||';' END AS cell FROM group_values v WHERE v.record_id=r.id ORDER BY v.n LIMIT -1)) AS group_key FROM filtered r) SELECT min(r.id),{aggregate} FROM filtered r JOIN tuples t ON t.record_id=r.id GROUP BY t.group_key ORDER BY t.group_key LIMIT {}",
        u64::from(limits.max_records) + 1
    );
    let mut rows = connection.query(&sql, params()?).await.map_err(unknown)?;
    let mut representatives = Vec::new();
    while let Some(row) = rows.next().await.map_err(unknown)? {
        if representatives.len() >= limits.max_records as usize {
            return Err(QueueError::TooLarge);
        }
        let mut values = Vec::new();
        for n in 0..6 {
            values.push(row.get_value(n).map_err(unknown)?);
        }
        let id = integer(&values[0])?;
        if id <= 0 {
            return Err(unknown("invalid group representative"));
        }
        charges.push(job.owned_buffer(512)?);
        representatives.push((id, super::stats_read::counts(&values, 1)?));
    }
    drop(rows);
    let mut groups = Vec::new();
    let mut total = 0;
    let mut exceeded = false;
    for (id, counts) in representatives {
        charges.push(
            job.owned_buffer(
                (request.group_by.len() as u64)
                    .checked_mul(std::mem::size_of::<Option<FieldValue>>() as u64)
                    .and_then(|n| n.checked_mul(3))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let mut values = Vec::new();
        for name in &request.group_by {
            values.push(
                payload::load_field_value(
                    connection,
                    id,
                    definition,
                    name,
                    limits.max_response_bytes,
                    None,
                    &mut payload::NativePayloadBudget {
                        job,
                        charges,
                        total: &mut total,
                        response_limit: &mut exceeded,
                    },
                )
                .await?,
            );
        }
        groups.push(StatsGroup { values, counts });
    }
    Ok(groups)
}
