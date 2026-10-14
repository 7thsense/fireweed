//! Shared source/cache checks, separated by the payload knowledge an operation needs.
use super::state_authority::StateAuthority;
use super::*;
use fireweed_engine::queue_state_authority::StateDescriptor;
pub(super) fn validate_presence(
    source: StateDescriptor,
    cached: &StateAuthority,
) -> QueueResult<()> {
    match (source, cached) {
        (StateDescriptor::NeverSeen, StateAuthority::Absent) => Ok(()),
        (StateDescriptor::Deleted { highwater }, StateAuthority::Deleted { version })
        | (StateDescriptor::Present { highwater, .. }, StateAuthority::Present { version, .. })
            if highwater == *version =>
        {
            Ok(())
        }
        _ => Err(unknown(
            "state source/cache presence or highwater disagreement",
        )),
    }
}
/// Fixed-size SQL metadata precedes SDK payload materialization and response limits.
pub(super) async fn validate_payload_metadata(
    connection: &Connection,
    id: i64,
    source: fireweed_engine::queue_state_authority::StateDescriptor,
) -> QueueResult<()> {
    use fireweed_engine::queue_state_authority::StateDescriptor;
    let StateDescriptor::Present { value_bytes, .. } = source else {
        return Err(unknown("payload metadata lacks live source state"));
    };
    let body = one(connection, "SELECT CASE WHEN typeof(value)='blob' THEN length(value) ELSE -1 END FROM q7_bodies WHERE record_id=?1", vec![Value::Integer(id)], 1).await?
        .ok_or_else(|| unknown("source state body missing"))?;
    if u64::try_from(integer(&body[0])?).ok() != Some(value_bytes) {
        return Err(unknown("state body length differs from source"));
    }
    validate_fields_metadata(connection, id, source).await
}
pub(super) async fn validate_fields_metadata(
    connection: &Connection,
    id: i64,
    source: StateDescriptor,
) -> QueueResult<()> {
    let StateDescriptor::Present {
        field_count,
        fields,
        ..
    } = source
    else {
        return Err(unknown("field metadata lacks live source state"));
    };
    let mut count = 0u64;
    let mut bytes = 0u64;
    for sql in [
        "SELECT count(*),coalesce(sum(CASE WHEN typeof(value)='blob' THEN length(value) ELSE 0 END),0),coalesce(sum(CASE WHEN typeof(value)='blob' AND typeof(field)='integer' THEN 0 ELSE 1 END),0) FROM q7_fields WHERE record_id=?1",
        "SELECT count(*),coalesce(sum(CASE WHEN typeof(value)='blob' AND typeof(name)='blob' THEN length(value)+length(name) ELSE 0 END),0),coalesce(sum(CASE WHEN typeof(value)='blob' AND typeof(name)='blob' THEN 0 ELSE 1 END),0) FROM q7_additional WHERE record_id=?1",
    ] {
        let row = one(connection, sql, vec![Value::Integer(id)], 3)
            .await?
            .ok_or_else(|| unknown("state field metadata missing"))?;
        if integer(&row[2])? != 0 {
            return Err(unknown("state field metadata type invalid"));
        }
        count = count
            .checked_add(u64::try_from(integer(&row[0])?).map_err(unknown)?)
            .ok_or_else(|| unknown("state field count overflow"))?;
        bytes = bytes
            .checked_add(u64::try_from(integer(&row[1])?).map_err(unknown)?)
            .ok_or_else(|| unknown("state field bytes overflow"))?;
    }
    if count != field_count || bytes > fields.encoded_bytes {
        return Err(unknown(
            "state field count or bytes differ from source bounds",
        ));
    }
    Ok(())
}
