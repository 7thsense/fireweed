//! Shared immutable receipt metadata and admitted raw outcome load.
use super::*;
pub(super) async fn load_raw(
    connection: &Connection,
    queue_id: i64,
    identity: &str,
    expected: Option<(u8, [u8; 32])>,
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
) -> QueueResult<Option<(u8, Option<Vec<u8>>)>> {
    let hash = discovery_digest(identity.as_bytes());
    let params = || {
        vec![
            Value::Integer(queue_id),
            blob(&hash),
            blob(identity.as_bytes()),
        ]
    };
    use super::receipt_metadata::{ReceiptDomain, ReceiptMetadata, read_receipt_metadata};
    let (format, size) = match read_receipt_metadata(
        connection,
        queue_id,
        ReceiptDomain::Commit,
        identity,
        expected,
    )
    .await?
    {
        ReceiptMetadata::Absent => {
            return Ok(None);
        }
        ReceiptMetadata::Expired => {
            return Ok(Some((3, None)));
        }
        ReceiptMetadata::Present { format, size } => (format, size),
    };
    charges.push(
        job.owned_buffer(
            size.checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?,
    );
    let row=one(connection,"SELECT outcome FROM q7_requests WHERE queue_id=?1 AND domain=2 AND identity_hash=?2 AND identity=?3",params(),1).await?.ok_or_else(||unknown("receipt vanished in snapshot"))?;
    let bytes = bytes(&row[0])?;
    if bytes.len() as u64 != size {
        return Err(unknown("receipt length changed in snapshot"));
    }
    Ok(Some((format, Some(bytes.to_vec()))))
}

/// Compare authoritative fixed descriptors before caller classification or opaque
/// payload allocation. Cached expiry has no current durable grammar witness.
pub(super) async fn load_raw_authoritative(
    connection: &Connection,
    queue_id: i64,
    identity: &str,
    expected: (u8, [u8; 32]),
    authority: Option<fireweed_engine::queue_original_authority::OriginalDescriptor>,
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
) -> QueueResult<Option<(u8, Option<Vec<u8>>)>> {
    use super::receipt_metadata::{ReceiptMetadata, read_original_metadata};
    let (format, size) =
        match read_original_metadata(connection, queue_id, identity, expected, authority).await? {
            ReceiptMetadata::Absent => return Ok(None),
            ReceiptMetadata::Present { format, size } => (format, size),
            ReceiptMetadata::Expired => return Err(unknown("authoritative expiry unavailable")),
        };
    let authority = authority.ok_or_else(|| unknown("original source descriptor missing"))?;
    charges.push(
        job.owned_buffer(
            size.checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?,
    );
    let hash = discovery_digest(identity.as_bytes());
    let row = one(connection,"SELECT outcome FROM q7_requests WHERE queue_id=?1 AND domain=2 AND identity_hash=?2 AND identity=?3",vec![Value::Integer(queue_id),blob(&hash),blob(identity.as_bytes())],1).await?.ok_or_else(||unknown("original vanished in native snapshot"))?;
    let raw = bytes(&row[0])?;
    authority.verify_outcome(raw)?;
    Ok(Some((format, Some(raw.to_vec()))))
}
