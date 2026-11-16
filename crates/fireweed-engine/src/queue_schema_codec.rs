//! Versioned queue definition persistence. Only the current framed definition
//! is accepted; any other bytes are refused.
use fireweed_core::queue_contract::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
// Ten continuation bytes cannot begin a valid postcard usize/u64 collection
// length, so the frame cannot be mistaken for an unframed postcard value.
const MAGIC: &[u8; 15] = b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xffFWQD4";
fn unknown(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(error.to_string())
}
fn take<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> QueueResult<T> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(unknown)?;
    if !rest.is_empty() {
        return Err(unknown("trailing queue definition bytes"));
    }
    Ok(value)
}
/// Exact persisted-definition identity for resolved publications. Hash the
/// authority's original bytes, including framing.
/// This is an identity check, not schema validation or allocation admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionPin([u8; 32]);
impl DefinitionPin {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub fn from_persisted_bytes(raw: &[u8]) -> Self {
        Self(Sha256::digest(raw).into())
    }
    pub fn verify(self, raw: &[u8]) -> QueueResult<()> {
        if self == Self::from_persisted_bytes(raw) {
            Ok(())
        } else {
            Err(unknown("resolved publication definition pin mismatch"))
        }
    }
}

/// Returns the payload after the definition frame; measure allocation headers
/// before calling decode_definition.
pub fn definition_payload(raw: &[u8]) -> QueueResult<&[u8]> {
    raw.strip_prefix(MAGIC.as_slice())
        .ok_or_else(|| unknown("unsupported queue definition frame"))
}
pub fn encode_definition(schema: &QueueSchema) -> QueueResult<Vec<u8>> {
    schema.validate()?;
    postcard::to_extend(schema, MAGIC.to_vec()).map_err(unknown)
}
pub fn decode_definition(raw: &[u8]) -> QueueResult<QueueSchema> {
    let schema: QueueSchema = take(definition_payload(raw)?)?;
    schema
        .validate()
        .map_err(|error| unknown(format!("invalid persisted queue schema: {error:?}")))?;
    Ok(schema)
}
pub fn definition_headers(raw: &[u8]) -> QueueResult<u64> {
    crate::queue_memory::persisted_schema_headers(definition_payload(raw)?)
}
/// Complete semantic validation of exact persisted bytes under an owned job.
/// Never reconstruct the pin from an equivalent decoded/re-encoded schema.
pub struct AdmittedQueueDefinition {
    raw: bytes::Bytes,
    schema: QueueSchema,
    pin: DefinitionPin,
    _charge: crate::queue_admission::OwnedQueueBufferCharge,
}
impl AdmittedQueueDefinition {
    pub fn from_raw(
        raw: &[u8],
        job: &std::sync::Arc<crate::queue_admission::QueueReservation>,
    ) -> QueueResult<Self> {
        let headers = definition_headers(raw)?;
        let charge = job.owned_buffer(
            (raw.len() as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add(headers.checked_mul(4)?))
                .and_then(|n| n.checked_add(1024))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let schema = decode_definition(raw)?;
        let pin = DefinitionPin::from_persisted_bytes(raw);
        Ok(Self {
            raw: bytes::Bytes::copy_from_slice(raw),
            schema,
            pin,
            _charge: charge,
        })
    }
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }
    pub fn schema(&self) -> &QueueSchema {
        &self.schema
    }
    pub fn pin(&self) -> DefinitionPin {
        self.pin
    }
    pub fn belongs_to(&self, job: &crate::queue_admission::QueueReservation) -> bool {
        std::ptr::eq(self._charge.reservation(), job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    fn schema() -> QueueSchema {
        QueueSchema {
            work: CollectionDefinition {
                fields: BTreeMap::from([("kind".into(), FieldType::Text)]),
                indexes: BTreeMap::from([("kind-index".into(), vec!["kind".into()])]),
                unique_indexes: BTreeSet::new(),
                additional_field_prefixes: BTreeSet::new(),
            },
            state: BTreeMap::new(),
            progress_bound_nanos: 1,
            max_attempts: 3,
            retention: QueueRetention {
                request_id_nanos: 86_400_000_000_000,
                deleted_keys: DeletedKeyPolicy::Reusable,
            },
        }
    }
    #[test]
    fn framed_definition_round_trips_with_exact_identity() {
        let schema = schema();
        let encoded = encode_definition(&schema).unwrap();
        assert_eq!(decode_definition(&encoded).unwrap(), schema);
        assert_eq!(
            definition_headers(&encoded).unwrap(),
            crate::queue_memory::schema_headers(&schema).unwrap()
        );
        let pin = DefinitionPin::from_persisted_bytes(&encoded);
        pin.verify(&encoded).unwrap();
        assert!(pin.verify(&encoded[1..]).is_err());
        let wire_pin = postcard::to_allocvec(&pin).unwrap();
        assert_eq!(wire_pin.len(), 32);
        assert_eq!(take::<DefinitionPin>(&wire_pin).unwrap(), pin);
        assert_eq!(
            definition_payload(&encoded).unwrap(),
            &encoded[MAGIC.len()..]
        );
        let mut extra = schema;
        extra
            .work
            .additional_field_prefixes
            .insert("/payload/".into());
        assert_eq!(
            decode_definition(&encode_definition(&extra).unwrap()).unwrap(),
            extra
        );
        extra.state.insert(
            "instances".into(),
            CollectionDefinition {
                fields: BTreeMap::new(),
                indexes: BTreeMap::new(),
                unique_indexes: BTreeSet::new(),
                additional_field_prefixes: BTreeSet::from(["/meta/".into()]),
            },
        );
        assert_eq!(
            definition_headers(&encode_definition(&extra).unwrap()).unwrap(),
            crate::queue_memory::schema_headers(&extra).unwrap()
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_definition(&trailing).is_err());
        let mut unsupported = MAGIC.to_vec();
        unsupported[14] = b'5';
        assert!(decode_definition(&unsupported).is_err());
    }
    #[test]
    fn unframed_definition_is_refused() {
        let unframed = postcard::to_allocvec(&schema()).unwrap();
        assert!(decode_definition(&unframed).is_err());
        assert!(definition_headers(&unframed).is_err());
        assert!(definition_payload(&unframed).is_err());
        assert!(
            AdmittedQueueDefinition::from_raw(
                &unframed,
                &std::sync::Arc::new(
                    crate::queue_admission::QueueAdmission::new(1, 1 << 20)
                        .unwrap()
                        .reserve(0)
                        .unwrap()
                )
            )
            .is_err()
        );
    }
}
