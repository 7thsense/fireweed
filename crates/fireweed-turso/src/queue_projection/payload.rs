//! One metadata-first field/body hydration boundary for projection consumers and
//! planners. Resident values are admitted before SDK fetch and owned decoding.
use super::*;
#[derive(Clone, Copy)]
pub(super) struct PayloadSelection<'a> {
    pub fields: Option<&'a std::collections::BTreeSet<String>>,
    pub include_body: bool,
    pub max_bytes: u64,
}
fn add_payload(
    total: &mut u64,
    length: i64,
    max: u64,
    response_limit: &mut bool,
) -> QueueResult<u64> {
    let length = u64::try_from(length).map_err(unknown)?;
    *total = total.checked_add(length).ok_or(QueueError::TooLarge)?;
    if *total > max {
        *response_limit = true;
        return Err(QueueError::TooLarge);
    }
    Ok(length)
}
pub(super) struct NativePayloadBudget<'a> {
    pub(super) job: &'a Arc<QueueReservation>,
    pub(super) charges: &'a mut Vec<OwnedQueueBufferCharge>,
    pub(super) total: &'a mut u64,
    pub(super) response_limit: &'a mut bool,
}
pub(super) async fn load_payload(
    connection: &Connection,
    id: i64,
    definition: &CollectionDefinition,
    selection: PayloadSelection<'_>,
    budget: &mut NativePayloadBudget<'_>,
    fields: &mut Fields,
    body: &mut bytes::Bytes,
) -> QueueResult<()> {
    let mut names = Vec::new();
    if let Some(selected) = selection.fields {
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
                selection.max_bytes,
                budget.response_limit,
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
        if fields.contains_key(name.as_ref()) {
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
            add_payload(
                budget.total,
                integer(&metadata[0])?,
                selection.max_bytes,
                budget.response_limit,
            )?;
            if !counted_name {
                add_payload(
                    budget.total,
                    i64::try_from(name.len()).map_err(unknown)?,
                    selection.max_bytes,
                    budget.response_limit,
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
            let raw_length = u64::try_from(integer(&metadata[0])?).map_err(unknown)?;
            budget.charges.push(
                budget.job.owned_buffer(
                    raw_length
                        .checked_mul(4)
                        .and_then(|n| n.checked_add(4096))
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
            fields.insert(name.into_owned(), value);
        }
    }
    if selection.include_body {
        let metadata = one(connection,"SELECT CASE WHEN typeof(value)='blob' THEN length(value) ELSE -1 END FROM q7_bodies WHERE record_id=?1",vec![Value::Integer(id)],1).await?.ok_or_else(||unknown("replay body missing"))?;
        let length = add_payload(
            budget.total,
            integer(&metadata[0])?,
            selection.max_bytes,
            budget.response_limit,
        )?;
        budget.charges.push(
            budget.job.owned_buffer(
                length
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let raw_body = one(
            connection,
            "SELECT value FROM q7_bodies WHERE record_id=?1",
            vec![Value::Integer(id)],
            1,
        )
        .await?
        .ok_or_else(|| unknown("body disappeared"))?;
        if bytes(&raw_body[0])?.len() as u64 != length {
            return Err(unknown("body length changed"));
        }
        *body = bytes::Bytes::copy_from_slice(bytes(&raw_body[0])?);
    }
    Ok(())
}
