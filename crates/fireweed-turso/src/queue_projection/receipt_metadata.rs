//! Fixed-size immutable request metadata shared by all receipt domains.
use super::*;
#[derive(Clone, Copy)]
pub(super) enum ReceiptDomain {
    Insert = 0,
    Claim = 1,
    Commit = 2,
}
pub(super) enum ReceiptMetadata {
    Absent,
    Expired,
    Present { format: u8, size: u64 },
}
pub(super) async fn read_receipt_metadata(
    connection: &Connection,
    queue_id: i64,
    domain: ReceiptDomain,
    identity: &str,
    expected: Option<(u8, [u8; 32])>,
) -> QueueResult<ReceiptMetadata> {
    let hash = discovery_digest(identity.as_bytes());
    let row=one(connection,"SELECT CASE WHEN typeof(fingerprint_format)='integer' THEN fingerprint_format ELSE NULL END,CASE WHEN typeof(fingerprint)='blob' AND length(fingerprint)=32 THEN fingerprint ELSE NULL END,CASE WHEN typeof(expired)='integer' THEN expired ELSE NULL END,CASE WHEN typeof(outcome)='blob' THEN length(outcome) ELSE -1 END FROM q7_requests WHERE queue_id=?1 AND domain=?2 AND identity_hash=?3 AND identity=?4",vec![Value::Integer(queue_id),Value::Integer(domain as i64),blob(&hash),blob(identity.as_bytes())],4).await?;
    let Some(row) = row else {
        return Ok(ReceiptMetadata::Absent);
    };
    let format = u8::try_from(integer(&row[0])?).map_err(unknown)?;
    if format != 3 && !(matches!(domain, ReceiptDomain::Commit) && format == 4) {
        return Err(unknown(
            "unsupported stored receipt fingerprint format/domain",
        ));
    }
    let fingerprint: [u8; 32] = bytes(&row[1])?.try_into().map_err(unknown)?;
    // A valid changed format/body conflicts before expiry or payload decoding.
    if expected
        .is_some_and(|(wanted_format, wanted)| format != wanted_format || fingerprint != wanted)
    {
        return Err(QueueError::RequestConflict);
    }
    match integer(&row[2])? {
        0 => {}
        1 => return Ok(ReceiptMetadata::Expired),
        _ => return Err(unknown("invalid stored receipt expiry")),
    }
    Ok(ReceiptMetadata::Present {
        format,
        size: u64::try_from(integer(&row[3])?).map_err(unknown)?,
    })
}
